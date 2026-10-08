//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <mutex> // NOLINT
#include <optional>
#include <shared_mutex>
#include <vector>

#include "buffer/arc_replacer.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
public:
  /**
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k)
      : replacer_size_(num_frames), state_(num_frames, k) {}

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
   * is furthest in the past.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @return the frame ID if a frame is successfully evicted, or `std::nullopt` if no frames can be
   * evicted.
   */
  auto Evict() -> std::optional<frame_id_t> {
    std::unique_lock lock(state_.rw_lock);

    std::optional<frame_id_t> victim;
    for (size_t fid = 0; fid < state_.data.size(); fid++)
      if (state_.data[fid].is_evictable())
        if (!victim.has_value() || state_.data[fid].evicts_before(state_.data[*victim]))
          victim = static_cast<frame_id_t>(fid);

    if (victim.has_value()) {
      state_.data[*victim].clear();
      state_.curr_size--;
    }
    return victim;
  }

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   * @param access_type type of access that was received. This parameter is only needed for
   * leaderboard tests.
   */
  void RecordAccess(frame_id_t frame_id,
                    [[maybe_unused]] AccessType access_type = AccessType::Unknown) {
    std::unique_lock lock(state_.rw_lock);
    CheckFrameId(frame_id);
    state_.data[frame_id].Access(state_.current_timestamp);
    state_.current_timestamp++;
  }

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::unique_lock lock(state_.rw_lock);
    CheckFrameId(frame_id);

    FrameLRUData &frame = state_.data[frame_id];
    if (!frame.is_tracked() || frame.is_evictable() == set_evictable) {
      return;
    }
    frame.set_evictable(set_evictable);
    if (set_evictable) {
      state_.curr_size++;
    } else {
      state_.curr_size--;
    }
  }

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id) {
    std::unique_lock lock(state_.rw_lock);
    CheckFrameId(frame_id);

    FrameLRUData &frame = state_.data[frame_id];
    if (!frame.is_tracked()) {
      return;
    }
    if (!frame.is_evictable()) {
      throw Exception(ExceptionType::INVALID,
                      "LRUKReplacer::Remove called on a non-evictable frame");
    }
    frame.clear();
    state_.curr_size--;
  }

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t {
    std::shared_lock lock(state_.rw_lock);
    return state_.curr_size;
  }

private:
  /** Per-frame access history: a ring buffer of the frame's last k access timestamps. */
  class FrameLRUData {
  public:
    explicit FrameLRUData(size_t k) : ring_(k) {}

    void Access(uint64_t ts) {
      ring_[cur_ptr_] = ts;
      cur_ptr_ = (cur_ptr_ + 1) % ring_.size();
      num_accesses_++;
    }

    auto get_backwards_k() const -> std::optional<uint64_t> {
      if (num_accesses_ < ring_.size()) {
        return std::nullopt;
      }
      return ring_[cur_ptr_];
    }

    auto get_lru() const -> uint64_t {
      if (num_accesses_ >= ring_.size()) {
        return ring_[cur_ptr_];
      }
      return ring_[0];
    }

    /** True if this frame should be evicted before `other`. */
    auto evicts_before(const FrameLRUData &other) const -> bool {
      const bool this_inf = !get_backwards_k().has_value();
      const bool other_inf = !other.get_backwards_k().has_value();
      if (this_inf != other_inf) {
        return this_inf; // +inf backward k-distance always goes first
      }
      return get_lru() < other.get_lru(); // otherwise, the older timestamp goes first
    }

    auto is_tracked() const -> bool { return num_accesses_ > 0; }

    auto is_evictable() const -> bool { return evictable_; }

    void set_evictable(bool evictable) { evictable_ = evictable; }

    /** Forget all access history. ring_ keeps its size k; old values are never read once
     * num_accesses_ is 0. */
    void clear() {
      num_accesses_ = 0;
      cur_ptr_ = 0;
      evictable_ = false;
    }

  private:
    std::vector<uint64_t> ring_;
    uint64_t num_accesses_{0};
    size_t cur_ptr_{0}; // next slot to write
    bool evictable_{false};
  };

  void CheckFrameId(frame_id_t frame_id) const {
    if (frame_id < 0 || static_cast<size_t>(frame_id) >= replacer_size_) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "LRUKReplacer: invalid frame id");
    }
  }

  /** Fixed at construction and never written again, so it is safe to read without the lock. */
  size_t replacer_size_;

  /** Everything that changes after construction, together with the lock that guards it. */
  struct State {
    State(size_t num_frames, size_t k) : data(num_frames, FrameLRUData(k)) {}

    std::shared_mutex rw_lock;
    size_t current_timestamp{0};
    size_t curr_size{0};
    std::vector<FrameLRUData> data;
  };
  State state_;
};

} // namespace bustub
