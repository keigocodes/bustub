//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.h
//
// Identification: src/include/storage/page/page_guard.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <utility>
#include <vector>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"

namespace bustub {

class BufferPoolManager;
class ReadPageGuard;
class WritePageGuard;

/**
 * @brief A helper class for `BufferPoolManager` that manages a frame of memory and related
 * metadata.
 *
 * This class represents headers for frames of memory that the `BufferPoolManager` stores pages of
 * data into. Note that the actual frames of memory are not stored directly inside a `FrameHeader`,
 * rather the `FrameHeader`s store pointer to the frames and are stored separately them.
 *
 * ---
 *
 * Something that may (or may not) be of interest to you is why the field `data_` is stored as a
 * vector that is allocated on the fly instead of as a direct pointer to some pre-allocated chunk of
 * memory.
 *
 * In a traditional production buffer pool manager, all memory that the buffer pool is intended to
 * manage is allocated in one large contiguous array (think of a very large `malloc` call that
 * allocates several gigabytes of memory up front). This large contiguous block of memory is then
 * divided into contiguous frames. In other words, frames are defined by an offset from the base of
 * the array in page-sized (4 KB) intervals.
 *
 * In BusTub, we instead allocate each frame on its own (via a `std::vector<char>`) in order to
 * easily detect buffer overflow with address sanitizer. Since C++ has no notion of memory safety,
 * it would be very easy to cast a page's data pointer into some large data type and start
 * overwriting other pages of data if they were all contiguous.
 *
 * If you would like to attempt to use more efficient data structures for your buffer pool manager,
 * you are free to do so. However, you will likely benefit significantly from detecting buffer
 * overflow in future projects (especially project 2).
 */
class FrameHeader {
  friend class BufferPoolManager;
  friend class ReadPageGuard;
  friend class WritePageGuard;

public:
  /**
   * @brief The constructor for a `FrameHeader` that initializes all fields to default values.
   *
   * See the documentation for `FrameHeader` in "buffer/buffer_pool_manager.h" for more information.
   *
   * @param frame_id The frame ID / index of the frame we are creating a header for.
   */
  FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

private:
  /**
   * @brief Get a raw const pointer to the frame's data.
   *
   * @return const char* A pointer to immutable data that the frame stores.
   */
  auto GetData() const -> const char * { return data_.data(); }
  /**
   * @brief Get a raw mutable pointer to the frame's data.
   *
   * @return char* A pointer to mutable data that the frame stores.
   */
  auto GetDataMut() -> char * { return data_.data(); }
  /**
   * @brief Resets a `FrameHeader`'s member fields.
   */
  void Reset() {
    std::fill(data_.begin(), data_.end(), 0);
    pin_count_.store(0);
    is_dirty_ = false;
  }

  /** @brief The frame ID / index of the frame this header represents. */
  [[maybe_unused]] const frame_id_t frame_id_; // Read by the guards in Drop().

  /** @brief The readers / writer latch for this frame. */
  std::shared_mutex rwlatch_;

  /** @brief The number of pins on this frame keeping the page in memory. */
  std::atomic<size_t> pin_count_;

  /** @brief The dirty flag. */
  bool is_dirty_;

  /**
   * @brief A pointer to the data of the page that this frame holds.
   *
   * If the frame does not hold any page data, the frame contains all null bytes.
   */
  std::vector<char> data_;

  /**
   * TODO(P1): You may add any fields or helper functions under here that you think are necessary.
   *
   * One potential optimization you could make is storing an optional page ID of the page that the
   * `FrameHeader` is currently storing. This might allow you to skip searching for the
   * corresponding (page ID, frame ID) pair somewhere else in the buffer pool manager...
   */
};

/**
 * @brief An RAII object that grants thread-safe read access to a page of data.
 *
 * The _only_ way that the BusTub system should interact with the buffer pool's page data is via
 * page guards. Since `ReadPageGuard` is an RAII object, the system never has to manually lock and
 * unlock a page's latch.
 *
 * With `ReadPageGuard`s, there can be multiple threads that share read access to a page's data.
 * However, the existence of any `ReadPageGuard` on a page implies that no thread can be mutating
 * the page's data.
 */
class ReadPageGuard {
  /** @brief Only the buffer pool manager is allowed to construct a valid `ReadPageGuard.` */
  friend class BufferPoolManager;

public:
  /**
   * @brief The default constructor for a `ReadPageGuard`.
   *
   * Note that we do not EVER want use a guard that has only been default constructed. The only
   * reason we even define this default constructor is to enable placing an "uninitialized" guard on
   * the stack that we can later move assign via `=`.
   *
   * **Use of an uninitialized page guard is undefined behavior.**
   *
   * In other words, the only way to get a valid `ReadPageGuard` is through the buffer pool manager.
   */
  ReadPageGuard() = default;

  ReadPageGuard(const ReadPageGuard &) = delete;
  auto operator=(const ReadPageGuard &) -> ReadPageGuard & = delete;

  /**
   * @brief The move constructor for `ReadPageGuard`.
   *
   * ### Implementation
   *
   * If you are unfamiliar with move semantics, please familiarize yourself with learning materials
   * online. There are many great resources (including articles, Microsoft tutorials, YouTube
   * videos) that explain this in depth.
   *
   * Make sure you invalidate the other guard; otherwise, you might run into double free problems!
   * For both objects, you need to update _at least_ 5 fields each.
   *
   * TODO(P1): Add implementation.
   *
   * @param that The other page guard.
   */
  ReadPageGuard(ReadPageGuard &&that) noexcept
      : page_id_(that.page_id_), frame_(std::move(that.frame_)),
        replacer_(std::move(that.replacer_)), bpm_latch_(std::move(that.bpm_latch_)),
        disk_scheduler_(std::move(that.disk_scheduler_)),
        is_valid_(std::exchange(that.is_valid_, false)) {}

  /**
   * @brief The move assignment operator for `ReadPageGuard`.
   *
   * ### Implementation
   *
   * If you are unfamiliar with move semantics, please familiarize yourself with learning materials
   * online. There are many great resources (including articles, Microsoft tutorials, YouTube
   * videos) that explain this in depth.
   *
   * Make sure you invalidate the other guard; otherwise, you might run into double free problems!
   * For both objects, you need to update _at least_ 5 fields each, and for the current object, make
   * sure you release any resources it might be holding on to.
   *
   * TODO(P1): Add implementation.
   *
   * @param that The other page guard.
   * @return ReadPageGuard& The newly valid `ReadPageGuard`.
   */
  auto operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
    if (this != &that) {
      Drop();
      page_id_ = that.page_id_;
      frame_ = std::move(that.frame_);
      replacer_ = std::move(that.replacer_);
      bpm_latch_ = std::move(that.bpm_latch_);
      disk_scheduler_ = std::move(that.disk_scheduler_);
      is_valid_ = std::exchange(that.is_valid_, false);
    }
    return *this;
  }

  /** @brief Gets the page ID of the page this guard is protecting. */
  auto GetPageId() const -> page_id_t {
    BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
    return page_id_;
  }

  /** @brief Gets a `const` pointer to the page of data this guard is protecting. */
  auto GetData() const -> const char * {
    BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
    return frame_->GetData();
  }

  template <class T>
  auto As() const -> const T * {
    return reinterpret_cast<const T *>(GetData());
  }

  /** @brief Returns whether the page is dirty (modified but not flushed to the disk). */
  auto IsDirty() const -> bool {
    BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
    return frame_->is_dirty_;
  }

  /**
   * @brief Flushes this page's data safely to disk.
   *
   * TODO(P1): Add implementation.
   */
  void Flush() { UNIMPLEMENTED("TODO(P1): Add implementation."); }

  /**
   * @brief Manually drops a valid `ReadPageGuard`'s data. If this guard is invalid, this function
   * does nothing.
   *
   * ### Implementation
   *
   * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources
   * you own and the order in which you release those resources. If you get the ordering wrong, you
   * will very likely fail one of the later Gradescope tests. You may also want to take the buffer
   * pool manager's latch in a very specific scenario...
   *
   * TODO(P1): Add implementation.
   */
  void Drop() {
    if (!is_valid_)
      return;

    is_valid_ = false;

    // Release the page latch before taking the BPM latch. Other code takes the BPM latch and then a
    // frame latch, so holding them in the opposite order here could deadlock. The pin we still hold
    // keeps the frame from being evicted in between.
    frame_->rwlatch_.unlock_shared();

    // Unpin under the BPM latch, so that a concurrent re-pin can't slip in between the count
    // hitting zero and marking the frame evictable.
    {
      std::scoped_lock latch(*bpm_latch_);
      if (frame_->pin_count_.fetch_sub(1) == 1) {
        replacer_->SetEvictable(frame_->frame_id_, true);
      }
    }

    frame_.reset();
    replacer_.reset();
    bpm_latch_.reset();
    disk_scheduler_.reset();
  }

  /** @brief The destructor for `ReadPageGuard`. This destructor simply calls `Drop()`. */
  ~ReadPageGuard() { Drop(); }

private:
  /**
   * @brief The only constructor for an RAII `ReadPageGuard` that creates a valid guard.
   *
   * Only the buffer pool manager is allowed to call this constructor.
   *
   * TODO(P1): Add implementation.
   *
   * @param page_id The page ID of the page we want to read.
   * @param frame A shared pointer to the frame that holds the page we want to protect.
   * @param replacer A shared pointer to the buffer pool manager's replacer.
   * @param bpm_latch A shared pointer to the buffer pool manager's latch.
   * @param disk_scheduler A shared pointer to the buffer pool manager's disk scheduler.
   */
  explicit ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                         std::shared_ptr<LRUKReplacer> replacer,
                         std::shared_ptr<std::mutex> bpm_latch,
                         std::shared_ptr<DiskScheduler> disk_scheduler)
      : page_id_(page_id), frame_(std::move(frame)), replacer_(std::move(replacer)),
        bpm_latch_(std::move(bpm_latch)), disk_scheduler_(std::move(disk_scheduler)) {
    frame_->rwlatch_.lock_shared();
    is_valid_ = true;
  }

  /** @brief The page ID of the page we are guarding. */
  page_id_t page_id_;

  /**
   * @brief The frame that holds the page this guard is protecting.
   *
   * Almost all operations of this page guard should be done via this shared pointer to a
   * `FrameHeader`.
   */
  std::shared_ptr<FrameHeader> frame_;

  /**
   * @brief A shared pointer to the buffer pool's replacer.
   *
   * Since the buffer pool cannot know when this `ReadPageGuard` gets destructed, we maintain a
   * pointer to the buffer pool's replacer in order to set the frame as evictable on destruction.
   */
  std::shared_ptr<LRUKReplacer> replacer_;

  /**
   * @brief A shared pointer to the buffer pool's latch.
   *
   * Since the buffer pool cannot know when this `ReadPageGuard` gets destructed, we maintain a
   * pointer to the buffer pool's latch for when we need to update the frame's eviction state in the
   * buffer pool replacer.
   */
  std::shared_ptr<std::mutex> bpm_latch_;

  /**
   * @brief A shared pointer to the buffer pool's disk scheduler.
   *
   * Used when flushing pages to disk.
   */
  std::shared_ptr<DiskScheduler> disk_scheduler_;

  /**
   * @brief The validity flag for this `ReadPageGuard`.
   *
   * Since we must allow for the construction of invalid page guards (see the documentation above),
   * we must maintain some sort of state that tells us if this page guard is valid or not. Note that
   * the default constructor will automatically set this field to `false`.
   *
   * If we did not maintain this flag, then the move constructor / move assignment operators could
   * attempt to destruct or `Drop()` invalid members, causing a segmentation fault.
   */
  bool is_valid_{false};

  /**
   * TODO(P1): You may add any fields under here that you think are necessary.
   *
   * If you want extra (nonexistent) style points, and you want to be extra fancy, then you can look
   * into the `std::shared_lock` type and use that for the latching mechanism instead of manually
   * calling `lock` and `unlock`.
   */
};

/**
 * @brief An RAII object that grants thread-safe write access to a page of data.
 *
 * The _only_ way that the BusTub system should interact with the buffer pool's page data is via
 * page guards. Since `WritePageGuard` is an RAII object, the system never has to manually lock and
 * unlock a page's latch.
 *
 * With a `WritePageGuard`, there can be only be one thread that has exclusive ownership over the
 * page's data. This means that the owner of the `WritePageGuard` can mutate the page's data as much
 * as they want. However, the existence of a `WritePageGuard` implies that no other `WritePageGuard`
 * or any `ReadPageGuard`s for the same page can exist at the same time.
 */
class WritePageGuard {
  /** @brief Only the buffer pool manager is allowed to construct a valid `WritePageGuard.` */
  friend class BufferPoolManager;

public:
  /**
   * @brief The default constructor for a `WritePageGuard`.
   *
   * Note that we do not EVER want use a guard that has only been default constructed. The only
   * reason we even define this default constructor is to enable placing an "uninitialized" guard on
   * the stack that we can later move assign via `=`.
   *
   * **Use of an uninitialized page guard is undefined behavior.**
   *
   * In other words, the only way to get a valid `WritePageGuard` is through the buffer pool
   * manager.
   */
  WritePageGuard() = default;

  WritePageGuard(const WritePageGuard &) = delete;
  auto operator=(const WritePageGuard &) -> WritePageGuard & = delete;

  /**
   * @brief The move constructor for `WritePageGuard`.
   *
   * ### Implementation
   *
   * If you are unfamiliar with move semantics, please familiarize yourself with learning materials
   * online. There are many great resources (including articles, Microsoft tutorials, YouTube
   * videos) that explain this in depth.
   *
   * Make sure you invalidate the other guard; otherwise, you might run into double free problems!
   * For both objects, you need to update _at least_ 5 fields each.
   *
   * TODO(P1): Add implementation.
   *
   * @param that The other page guard.
   */
  WritePageGuard(WritePageGuard &&that) noexcept
      : page_id_(that.page_id_), frame_(std::move(that.frame_)),
        replacer_(std::move(that.replacer_)), bpm_latch_(std::move(that.bpm_latch_)),
        disk_scheduler_(std::move(that.disk_scheduler_)),
        is_valid_(std::exchange(that.is_valid_, false)) {}

  /**
   * @brief The move assignment operator for `WritePageGuard`.
   *
   * ### Implementation
   *
   * If you are unfamiliar with move semantics, please familiarize yourself with learning materials
   * online. There are many great resources (including articles, Microsoft tutorials, YouTube
   * videos) that explain this in depth.
   *
   * Make sure you invalidate the other guard; otherwise, you might run into double free problems!
   * For both objects, you need to update _at least_ 5 fields each, and for the current object, make
   * sure you release any resources it might be holding on to.
   *
   * TODO(P1): Add implementation.
   *
   * @param that The other page guard.
   * @return WritePageGuard& The newly valid `WritePageGuard`.
   */
  auto operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
    if (this != &that) {
      Drop();
      page_id_ = that.page_id_;
      frame_ = std::move(that.frame_);
      replacer_ = std::move(that.replacer_);
      bpm_latch_ = std::move(that.bpm_latch_);
      disk_scheduler_ = std::move(that.disk_scheduler_);
      is_valid_ = std::exchange(that.is_valid_, false);
    }
    return *this;
  }

  /** @brief Gets the page ID of the page this guard is protecting. */
  auto GetPageId() const -> page_id_t {
    BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
    return page_id_;
  }

  /** @brief Gets a `const` pointer to the page of data this guard is protecting. */
  auto GetData() const -> const char * {
    BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
    return frame_->GetData();
  }

  template <class T>
  auto As() const -> const T * {
    return reinterpret_cast<const T *>(GetData());
  }

  /** @brief Gets a mutable pointer to the page of data this guard is protecting. */
  auto GetDataMut() -> char * {
    BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
    frame_->is_dirty_ = true;
    return frame_->GetDataMut();
  }

  template <class T>
  auto AsMut() -> T * {
    return reinterpret_cast<T *>(GetDataMut());
  }

  /** @brief Returns whether the page is dirty (modified but not flushed to the disk). */
  auto IsDirty() const -> bool {
    BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
    return frame_->is_dirty_;
  }

  /**
   * @brief Flushes this page's data safely to disk.
   *
   * TODO(P1): Add implementation.
   */
  void Flush() { UNIMPLEMENTED("TODO(P1): Add implementation."); }

  /**
   * @brief Manually drops a valid `WritePageGuard`'s data. If this guard is invalid, this function
   * does nothing.
   *
   * ### Implementation
   *
   * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources
   * you own and the order in which you release those resources. If you get the ordering wrong, you
   * will very likely fail one of the later Gradescope tests. You may also want to take the buffer
   * pool manager's latch in a very specific scenario...
   *
   * TODO(P1): Add implementation.
   */
  void Drop() {
    if (!is_valid_)
      return;

    is_valid_ = false;

    // Release the page latch before taking the BPM latch. Other code takes the BPM latch and then a
    // frame latch, so holding them in the opposite order here could deadlock. The pin we still hold
    // keeps the frame from being evicted in between.
    frame_->rwlatch_.unlock();

    // Unpin under the BPM latch, so that a concurrent re-pin can't slip in between the count
    // hitting zero and marking the frame evictable.
    {
      std::scoped_lock latch(*bpm_latch_);
      if (frame_->pin_count_.fetch_sub(1) == 1) {
        replacer_->SetEvictable(frame_->frame_id_, true);
      }
    }

    frame_.reset();
    replacer_.reset();
    bpm_latch_.reset();
    disk_scheduler_.reset();
  }

  /** @brief The destructor for `WritePageGuard`. This destructor simply calls `Drop()`. */
  ~WritePageGuard() { Drop(); }

private:
  /**
   * @brief The only constructor for an RAII `WritePageGuard` that creates a valid guard.
   *
   * Only the buffer pool manager is allowed to call this constructor.
   *
   * TODO(P1): Add implementation.
   *
   * @param page_id The page ID of the page we want to write to.
   * @param frame A shared pointer to the frame that holds the page we want to protect.
   * @param replacer A shared pointer to the buffer pool manager's replacer.
   * @param bpm_latch A shared pointer to the buffer pool manager's latch.
   * @param disk_scheduler A shared pointer to the buffer pool manager's disk scheduler.
   */
  explicit WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                          std::shared_ptr<LRUKReplacer> replacer,
                          std::shared_ptr<std::mutex> bpm_latch,
                          std::shared_ptr<DiskScheduler> disk_scheduler)
      : page_id_(page_id), frame_(std::move(frame)), replacer_(std::move(replacer)),
        bpm_latch_(std::move(bpm_latch)), disk_scheduler_(std::move(disk_scheduler)) {
    frame_->rwlatch_.lock();
    is_valid_ = true;
  }

  /** @brief The page ID of the page we are guarding. */
  page_id_t page_id_;

  /**
   * @brief The frame that holds the page this guard is protecting.
   *
   * Almost all operations of this page guard should be done via this shared pointer to a
   * `FrameHeader`.
   */
  std::shared_ptr<FrameHeader> frame_;

  /**
   * @brief A shared pointer to the buffer pool's replacer.
   *
   * Since the buffer pool cannot know when this `WritePageGuard` gets destructed, we maintain a
   * pointer to the buffer pool's replacer in order to set the frame as evictable on destruction.
   */
  std::shared_ptr<LRUKReplacer> replacer_;

  /**
   * @brief A shared pointer to the buffer pool's latch.
   *
   * Since the buffer pool cannot know when this `WritePageGuard` gets destructed, we maintain a
   * pointer to the buffer pool's latch for when we need to update the frame's eviction state in the
   * buffer pool replacer.
   */
  std::shared_ptr<std::mutex> bpm_latch_;

  /**
   * @brief A shared pointer to the buffer pool's disk scheduler.
   *
   * Used when flushing pages to disk.
   */
  std::shared_ptr<DiskScheduler> disk_scheduler_;

  /**
   * @brief The validity flag for this `WritePageGuard`.
   *
   * Since we must allow for the construction of invalid page guards (see the documentation above),
   * we must maintain some sort of state that tells us if this page guard is valid or not. Note that
   * the default constructor will automatically set this field to `false`.
   *
   * If we did not maintain this flag, then the move constructor / move assignment operators could
   * attempt to destruct or `Drop()` invalid members, causing a segmentation fault.
   */
  bool is_valid_{false};

  /**
   * TODO(P1): You may add any fields under here that you think are necessary.
   *
   * If you want extra (nonexistent) style points, and you want to be extra fancy, then you can look
   * into the `std::unique_lock` type and use that for the latching mechanism instead of manually
   * calling `lock` and `unlock`.
   */
};

} // namespace bustub

// Included last: BufferPoolManager's inline methods need the complete page guard types above.
#include "buffer/buffer_pool_manager.h" // NOLINT
