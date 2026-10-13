//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <atomic>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "common/macros.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

class BufferPoolManager;
class ReadPageGuard;
class WritePageGuard;

// `FrameHeader` is defined in "storage/page/page_guard.h", since the page guards need it complete.

/**
 * @brief The declaration of the `BufferPoolManager` class.
 *
 * As stated in the writeup, the buffer pool is responsible for moving physical pages of data back
 * and forth from buffers in main memory to persistent storage. It also behaves as a cache, keeping
 * frequently used pages in memory for faster access, and evicting unused or cold pages back out to
 * storage.
 *
 * Make sure you read the writeup in its entirety before attempting to implement the buffer pool
 * manager. You also need to have completed the implementation of both the `LRUKReplacer` and
 * `DiskManager` classes.
 */
class BufferPoolManager {
public:
  /**
   * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
   *
   * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more
   * information.
   *
   * ### Implementation
   *
   * We have implemented the constructor for you in a way that makes sense with our reference
   * solution. You are free to change anything you would like here if it doesn't fit with you
   * implementation.
   *
   * Be warned, though! If you stray too far away from our guidance, it will be much harder for us
   * to help you. Our recommendation would be to first implement the buffer pool manager using the
   * stepping stones we have provided.
   *
   * Once you have a fully working solution (all Gradescope test cases pass), then you can try more
   * interesting things!
   *
   * @param num_frames The size of the buffer pool.
   * @param disk_manager The disk manager.
   * @param log_manager The log manager. Please ignore this for P1.
   */
  BufferPoolManager(size_t num_frames, DiskManager *disk_manager, LogManager *log_manager = nullptr)
      : num_frames_(num_frames), next_page_id_(0),
        replacer_(std::make_shared<LRUKReplacer>(num_frames, LRUK_REPLACER_K)),
        disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
    // Not strictly necessary...
    std::scoped_lock latch(*state.bpm_latch_);

    // Initialize the monotonically increasing counter at 0.
    next_page_id_.store(0);

    // Allocate all of the in-memory frames up front.
    state.frames_.reserve(num_frames_);

    // The page table should have exactly `num_frames_` slots, corresponding to exactly
    // `num_frames_` frames.
    state.page_table_.reserve(num_frames_);

    // Initialize all of the frame headers, and fill the free frame list with all possible frame IDs
    // (since all frames are initially free).
    for (size_t i = 0; i < num_frames_; i++) {
      state.frames_.push_back(std::make_shared<FrameHeader>(i));
      state.free_frames_.push_back(static_cast<int>(i));
    }
  }
  /**
   * @brief Destroys the `BufferPoolManager`, freeing up all memory that the buffer pool was using.
   */
  ~BufferPoolManager() = default;

  /**
   * @brief Returns the number of frames that this buffer pool manages.
   */
  auto Size() const -> size_t { return num_frames_; }
  /**
   * @brief Allocates a new page on disk.
   *
   * ### Implementation
   *
   * You will maintain a thread-safe, monotonically increasing counter in the form of a
   * `std::atomic<page_id_t>`. See the documentation on
   * [atomics](https://en.cppreference.com/w/cpp/atomic/atomic) for more information.
   *
   * TODO(P1): Add implementation.
   *
   * @return The page ID of the newly allocated page.
   */
  auto NewPage() -> page_id_t { return next_page_id_.fetch_add(1); }
  /**
   * @brief Removes a page from the database, both on disk and in memory.
   *
   * If the page is pinned in the buffer pool, this function does nothing and returns `false`.
   * Otherwise, this function removes the page from both disk and memory (if it is still in the
   * buffer pool), returning `true`.
   *
   * ### Implementation
   *
   * Think about all of the places that a page or a page's metadata could be, and use that to guide
   * you on implementing this function. You will probably want to implement this function _after_
   * you have implemented `CheckedReadPage` and `CheckedWritePage`.
   *
   * You should call `DeallocatePage` in the disk scheduler to make the space available for new
   * pages.
   *
   * TODO(P1): Add implementation.
   *
   * @param page_id The page ID of the page we want to delete.
   * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or
   * deletion succeeded.
   */
  auto DeletePage(page_id_t page_id) -> bool { UNIMPLEMENTED("TODO(P1): Add implementation."); }
  /**
   * @brief Acquires an optional write-locked guard over a page of data. The user can specify an
   * `AccessType` if needed.
   *
   * If it is not possible to bring the page of data into memory, this function will return a
   * `std::nullopt`.
   *
   * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are
   * expected to acquire either a `ReadPageGuard` or a `WritePageGuard` depending on the mode in
   * which they would like to access the data, which ensures that any access of data is thread-safe.
   *
   * There can only be 1 `WritePageGuard` reading/writing a page at a time. This allows data access
   * to be both immutable and mutable, meaning the thread that owns the `WritePageGuard` is allowed
   * to manipulate the page's data however they want. If a user wants to have multiple threads
   * reading the page at the same time, they must acquire a `ReadPageGuard` with `CheckedReadPage`
   * instead.
   *
   * ### Implementation
   *
   * There are three main cases that you will have to implement. The first two are relatively
   * simple: one is when there is plenty of available memory, and the other is when we don't
   * actually need to perform any additional I/O. Think about what exactly these two cases entail.
   *
   * The third case is the trickiest, and it is when we do not have any _easily_ available memory at
   * our disposal. The buffer pool is tasked with finding memory that it can use to bring in a page
   * of memory, using the replacement algorithm you implemented previously to find candidate frames
   * for eviction.
   *
   * Once the buffer pool has identified a frame for eviction, several I/O operations may be
   * necessary to bring in the page of data we want into the frame.
   *
   * There is likely going to be a lot of shared code with `CheckedReadPage`, so you may find
   * creating helper functions useful.
   *
   * These two functions are the crux of this project, so we won't give you more hints than this.
   * Good luck!
   *
   * TODO(P1): Add implementation.
   *
   * @param page_id The ID of the page we want to write to.
   * @param access_type The type of page access.
   * @return std::optional<WritePageGuard> An optional latch guard where if there are no more free
   * frames (out of memory) returns `std::nullopt`; otherwise, returns a `WritePageGuard` ensuring
   * exclusive and mutable access to a page's data.
   */
  auto CheckedWritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown)
      -> std::optional<WritePageGuard> {
    std::unique_lock lock(*state.bpm_latch_);

    auto frame_id = FindResidentFrame(page_id);
    if (!frame_id.has_value())
      frame_id = LoadPageIntoNewFrame(page_id);
    if (!frame_id.has_value())
      return std::nullopt;

    PinFrame(*frame_id, access_type);

    lock.unlock();
    return WritePageGuard(page_id, state.frames_[*frame_id], replacer_, state.bpm_latch_,
                          disk_scheduler_);
  }
  /**
   * @brief Acquires an optional read-locked guard over a page of data. The user can specify an
   * `AccessType` if needed.
   *
   * If it is not possible to bring the page of data into memory, this function will return a
   * `std::nullopt`.
   *
   * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are
   * expected to acquire either a `ReadPageGuard` or a `WritePageGuard` depending on the mode in
   * which they would like to access the data, which ensures that any access of data is thread-safe.
   *
   * There can be any number of `ReadPageGuard`s reading the same page of data at a time across
   * different threads. However, all data access must be immutable. If a user wants to mutate the
   * page's data, they must acquire a `WritePageGuard` with `CheckedWritePage` instead.
   *
   * ### Implementation
   *
   * See the implementation details of `CheckedWritePage`.
   *
   * TODO(P1): Add implementation.
   *
   * @param page_id The ID of the page we want to read.
   * @param access_type The type of page access.
   * @return std::optional<ReadPageGuard> An optional latch guard where if there are no more free
   * frames (out of memory) returns `std::nullopt`; otherwise, returns a `ReadPageGuard` ensuring
   * shared and read-only access to a page's data.
   */
  auto CheckedReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown)
      -> std::optional<ReadPageGuard> {
    std::unique_lock lock(*state.bpm_latch_);

    auto frame_id = FindResidentFrame(page_id);
    if (!frame_id.has_value())
      frame_id = LoadPageIntoNewFrame(page_id);
    if (!frame_id.has_value())
      return std::nullopt;

    PinFrame(*frame_id, access_type);

    // Release the BPM latch before the guard takes the frame latch, so we never wait on a frame
    // latch while holding the BPM latch. The pin keeps the frame from being evicted in between.
    lock.unlock();
    return ReadPageGuard(page_id, state.frames_[*frame_id], replacer_, state.bpm_latch_,
                         disk_scheduler_);
  }
  /**
   * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
   *
   * If `CheckedWritePage` returns a `std::nullopt`, **this function aborts the entire process.**
   *
   * This function should **only** be used for testing and ergonomic's sake. If it is at all
   * possible that the buffer pool manager might run out of memory, then use `CheckedPageWrite` to
   * allow you to handle that case.
   *
   * See the documentation for `CheckedPageWrite` for more information about implementation.
   *
   * @param page_id The ID of the page we want to read.
   * @param access_type The type of page access.
   * @return WritePageGuard A page guard ensuring exclusive and mutable access to a page's data.
   */
  auto WritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown)
      -> WritePageGuard {
    auto guard_opt = CheckedWritePage(page_id, access_type);

    if (!guard_opt.has_value()) {
      fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
      std::abort();
    }

    return std::move(guard_opt).value();
  }
  /**
   * @brief A wrapper around `CheckedReadPage` that unwraps the inner value if it exists.
   *
   * If `CheckedReadPage` returns a `std::nullopt`, **this function aborts the entire process.**
   *
   * This function should **only** be used for testing and ergonomic's sake. If it is at all
   * possible that the buffer pool manager might run out of memory, then use `CheckedPageWrite` to
   * allow you to handle that case.
   *
   * See the documentation for `CheckedPageRead` for more information about implementation.
   *
   * @param page_id The ID of the page we want to read.
   * @param access_type The type of page access.
   * @return ReadPageGuard A page guard ensuring shared and read-only access to a page's data.
   */
  auto ReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> ReadPageGuard {
    auto guard_opt = CheckedReadPage(page_id, access_type);

    if (!guard_opt.has_value()) {
      fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
      std::abort();
    }

    return std::move(guard_opt).value();
  }
  /**
   * @brief Flushes a page's data out to disk unsafely.
   *
   * This function will write out a page's data to disk if it has been modified. If the given page
   * is not in memory, this function will return `false`.
   *
   * You should not take a lock on the page in this function.
   * This means that you should carefully consider when to toggle the `is_dirty_` bit.
   *
   * ### Implementation
   *
   * You should probably leave implementing this function until after you have completed
   * `CheckedReadPage` and `CheckedWritePage`, as it will likely be much easier to understand what
   * to do.
   *
   * TODO(P1): Add implementation
   *
   * @param page_id The page ID of the page to be flushed.
   * @return `false` if the page could not be found in the page table; otherwise, `true`.
   */
  auto FlushPageUnsafe(page_id_t page_id) -> bool {
    UNIMPLEMENTED("TODO(P1): Add implementation.");
  }
  /**
   * @brief Flushes a page's data out to disk safely.
   *
   * This function will write out a page's data to disk if it has been modified. If the given page
   * is not in memory, this function will return `false`.
   *
   * You should take a lock on the page in this function to ensure that a consistent state is
   * flushed to disk.
   *
   * ### Implementation
   *
   * You should probably leave implementing this function until after you have completed
   * `CheckedReadPage`, `CheckedWritePage`, and `Flush` in the page guards, as it will likely be
   * much easier to understand what to do.
   *
   * TODO(P1): Add implementation
   *
   * @param page_id The page ID of the page to be flushed.
   * @return `false` if the page could not be found in the page table; otherwise, `true`.
   */
  auto FlushPage(page_id_t page_id) -> bool { UNIMPLEMENTED("TODO(P1): Add implementation."); }
  /**
   * @brief Flushes all page data that is in memory to disk unsafely.
   *
   * You should not take locks on the pages in this function.
   * This means that you should carefully consider when to toggle the `is_dirty_` bit.
   *
   * ### Implementation
   *
   * You should probably leave implementing this function until after you have completed
   * `CheckedReadPage`, `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to
   * understand what to do.
   *
   * TODO(P1): Add implementation
   */
  void FlushAllPagesUnsafe() { UNIMPLEMENTED("TODO(P1): Add implementation."); }
  /**
   * @brief Flushes all page data that is in memory to disk safely.
   *
   * You should take locks on the pages in this function to ensure that a consistent state is
   * flushed to disk.
   *
   * ### Implementation
   *
   * You should probably leave implementing this function until after you have completed
   * `CheckedReadPage`, `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to
   * understand what to do.
   *
   * TODO(P1): Add implementation
   */
  void FlushAllPages() { UNIMPLEMENTED("TODO(P1): Add implementation."); }
  /**
   * @brief Retrieves the pin count of a page. If the page does not exist in memory, return
   * `std::nullopt`.
   *
   * This function is thread safe. Callers may invoke this function in a multi-threaded environment
   * where multiple threads access the same page.
   *
   * This function is intended for testing purposes. If this function is implemented incorrectly, it
   * will definitely cause problems with the test suite and autograder.
   *
   * # Implementation
   *
   * We will use this function to test if your buffer pool manager is managing pin counts correctly.
   * Since the `pin_count_` field in `FrameHeader` is an atomic type, you do not need to take the
   * latch on the frame that holds the page we want to look at. Instead, you can simply use an
   * atomic `load` to safely load the value stored. You will still need to take the buffer pool
   * latch, however.
   *
   * Again, if you are unfamiliar with atomic types, see the official C++ docs
   * [here](https://en.cppreference.com/w/cpp/atomic/atomic).
   *
   * TODO(P1): Add implementation
   *
   * @param page_id The page ID of the page we want to get the pin count of.
   * @return std::optional<size_t> The pin count if the page exists; otherwise, `std::nullopt`.
   */
  auto GetPinCount(page_id_t page_id) -> std::optional<size_t> {
    std::scoped_lock lock(*state.bpm_latch_);

    if (!state.page_table_.count(page_id))
      return std::nullopt;

    const auto &frame_id = state.page_table_[page_id];
    const auto &frame_header = state.frames_[frame_id];

    return frame_header->pin_count_.load();
  }

private:
  /** @brief The number of frames in the buffer pool. */
  const size_t num_frames_;

  /** @brief The next page ID to be allocated.  */
  std::atomic<page_id_t> next_page_id_;

  /**
   * @brief The latch protecting the buffer pool's inner data structures.
   *
   * TODO(P1) We recommend replacing this comment with details about what this latch actually
   * protects.
   */
  struct State {
    std::shared_ptr<std::mutex> bpm_latch_{std::make_shared<std::mutex>()};

    /** @brief The frame headers of the frames that this buffer pool manages. */
    std::vector<std::shared_ptr<FrameHeader>> frames_;

    /** @brief The page table that keeps track of the mapping between pages and buffer pool frames.
     */
    std::unordered_map<page_id_t, frame_id_t> page_table_;

    /** @brief A list of free frames that do not hold any page's data. */
    std::list<frame_id_t> free_frames_;
  };

  State state;

  // The helpers below must be called with the BPM latch held.

  /** @brief Returns the frame holding `page_id`, or `std::nullopt` if the page isn't in memory. */
  auto FindResidentFrame(page_id_t page_id) -> std::optional<frame_id_t> {
    if (!state.page_table_.count(page_id))
      return std::nullopt;
    return state.page_table_[page_id];
  }

  /**
   * @brief Reads `page_id` from disk into a free or evicted frame. Does not pin the frame.
   *
   * @return The frame's ID, or `std::nullopt` if every frame is pinned.
   */
  auto LoadPageIntoNewFrame(page_id_t page_id) -> std::optional<frame_id_t> {
    const auto frame_id = AcquireFrame();
    if (frame_id.has_value())
      LoadPageIntoFrame(page_id, *frame_id);
    return frame_id;
  }

  /** @brief Pins a frame so the replacer can't evict it, and records the access. */
  void PinFrame(frame_id_t frame_id, AccessType access_type) {
    state.frames_[frame_id]->pin_count_++;
    replacer_->RecordAccess(frame_id, access_type);
    replacer_->SetEvictable(frame_id, false);
  }

  /**
   * @brief Returns an empty frame, taking one from the free list or else evicting one.
   *
   * @return The frame's ID, or `std::nullopt` if every frame is pinned.
   */
  auto AcquireFrame() -> std::optional<frame_id_t> {
    if (!state.free_frames_.empty()) {
      const auto frame_id = state.free_frames_.front();
      state.free_frames_.pop_front();
      return frame_id;
    }

    const auto victim = replacer_->Evict();
    if (victim.has_value())
      EvictPageFromFrame(*victim);
    return victim;
  }

  /** @brief Writes back the frame's page if it's dirty and removes it from the page table. */
  void EvictPageFromFrame(frame_id_t frame_id) {
    const auto old = std::find_if(state.page_table_.begin(), state.page_table_.end(),
                                  [&](const auto &entry) { return entry.second == frame_id; });
    if (old == state.page_table_.end())
      return;

    if (state.frames_[frame_id]->is_dirty_)
      DoDiskIo(true, old->first, state.frames_[frame_id]->GetDataMut());
    state.page_table_.erase(old);
  }

  /** @brief Reads `page_id` from disk into a frame and maps it in the page table. */
  void LoadPageIntoFrame(page_id_t page_id, frame_id_t frame_id) {
    const auto &frame = state.frames_[frame_id];
    frame->Reset();
    DoDiskIo(false, page_id, frame->GetDataMut());
    state.page_table_[page_id] = frame_id;
  }

  /** @brief Synchronously reads or writes one page through the disk scheduler. */
  void DoDiskIo(bool is_write, page_id_t page_id, char *data) {
    disk_scheduler_->ScheduleAndWait(is_write, page_id, data);
  }

  /** @brief The replacer to find unpinned / candidate pages for eviction. */
  std::shared_ptr<LRUKReplacer> replacer_;

  /** @brief A pointer to the disk scheduler. Shared with the page guards for flushing. */
  std::shared_ptr<DiskScheduler> disk_scheduler_;

  /**
   * @brief A pointer to the log manager.
   *
   * Note: Please ignore this for P1.
   */
  LogManager *log_manager_ __attribute__((__unused__));

  /**
   * TODO(P1): You may add additional private members and helper functions if you find them
   * necessary.
   *
   * There will likely be a lot of code duplication between the different modes of accessing a page.
   *
   * We would recommend implementing a helper function that returns the ID of a frame that is free
   * and has nothing stored inside of it. Additionally, you may also want to implement a helper
   * function that returns either a shared pointer to a `FrameHeader` that already has a page's data
   * stored inside of it, or an index to said `FrameHeader`.
   */
};
} // namespace bustub
