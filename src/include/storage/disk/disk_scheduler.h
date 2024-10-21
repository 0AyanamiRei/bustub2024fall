//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.h
//
// Identification: src/include/storage/disk/disk_scheduler.h
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <future>  // NOLINT
#include <optional>
#include <thread>  // NOLINT

#include "common/channel.h"
#include "storage/disk/disk_manager.h"

#define bk_threads 1024

namespace bustub {

/**
 * @brief Represents a Write or Read request for the DiskManager to execute.
 */
struct DiskRequest {
  bool is_write_;
  char *data_;
  page_id_t page_id_;
  std::promise<bool> callback_;
};

/**
 * @brief The DiskScheduler schedules disk read and write operations.
 *
 * A request is scheduled by calling DiskScheduler::Schedule() with an appropriate DiskRequest object. The scheduler
 * maintains a background worker thread that processes the scheduled requests using the disk manager. The background
 * thread is created in the DiskScheduler constructor and joined in its destructor.
 */
class DiskScheduler {
 public:
  explicit DiskScheduler(DiskManager *disk_manager);
  ~DiskScheduler();

  void Schedule(DiskRequest r);
  void StartWorkerThread();

  using DiskSchedulerPromise = std::promise<bool>;
  auto CreatePromise() -> DiskSchedulerPromise { return {}; };

  /**
   * @brief Increases the size of the database file to fit the specified number of pages.
   *
   * This function works like a dynamic array, where the capacity is doubled until all pages can fit.
   *
   * @param pages The number of pages the caller wants the file used for storage to support.
   */
  void IncreaseDiskSpace(size_t pages) { disk_manager_->IncreaseDiskSpace(pages); }

  /**
   * @brief Deallocates a page on disk.
   *
   * Note: You should look at the documentation for `DeletePage` in `BufferPoolManager` before using this method.
   * Also note: This is a no-op without a more complex data structure to track deallocated pages.
   *
   * @param page_id The page ID of the page to deallocate from disk.
   */
  void DeallocatePage(page_id_t page_id) {}

 private:
  // std::mutex lock_;
  DiskManager *disk_manager_ __attribute__((__unused__));
  Channel<std::optional<DiskRequest>> request_queue_[bk_threads];
  std::optional<std::thread> background_thread_;
};
}  // namespace bustub