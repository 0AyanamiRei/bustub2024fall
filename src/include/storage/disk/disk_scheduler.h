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

#define bk_threads 6400

namespace bustub {


struct DiskRequest {
  bool is_write_;
  char *data_;
  page_id_t page_id_;
  std::promise<bool> callback_;
};


class DiskScheduler {
 public:
  explicit DiskScheduler(DiskManager *disk_manager, int num_frames = 64);
  ~DiskScheduler();

  void Schedule(DiskRequest r, frame_id_t frame_id);

  using DiskSchedulerPromise = std::promise<bool>;
  auto CreatePromise() -> DiskSchedulerPromise { return {}; };

  // This function works like a dynamic array, where the capacity is doubled until all pages can fit.
  void IncreaseDiskSpace(size_t pages) { disk_manager_->IncreaseDiskSpace(pages); }

  // You should look at the documentation for `DeletePage` in `BufferPoolManager` before using this method.
  // Note: This is a no-op without a more complex data structure to track deallocated pages.
  void DeallocatePage(page_id_t page_id) { disk_manager_->DeletePage(page_id); }

 private: 
  DiskManager *disk_manager_ __attribute__((__unused__));
  std::vector<Channel<std::optional<DiskRequest>>> request_queues_;
  std::vector<std::thread> background_threads_;
  bool _close{false};
};
}  // namespace bustub