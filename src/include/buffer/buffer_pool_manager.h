
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <condition_variable>  // NOLINT(build/c++11)
#include <list>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

#include "common/logger.h"

namespace bustub {

class BufferPoolManager;
class ReadPageGuard;
class WritePageGuard;

/**
 * @brief A helper class for `BufferPoolManager` that manages a frame of memory and related metadata.
 *
 * This class represents headers for frames of memory that the `BufferPoolManager` stores pages of data into. Note that
 * the actual frames of memory are not stored directly inside a `FrameHeader`, rather the `FrameHeader`s store pointer
 * to the frames and are stored separately them.
 *
 * ---
 *
 * Something that may (or may not) be of interest to you is why the field `data_` is stored as a vector that is
 * allocated on the fly instead of as a direct pointer to some pre-allocated chunk of memory.
 *
 * In a traditional production buffer pool manager, all memory that the buffer pool is intended to manage is allocated
 * in one large contiguous array (think of a very large `malloc` call that allocates several gigabytes of memory up
 * front). This large contiguous block of memory is then divided into contiguous frames. In other words, frames are
 * defined by an offset from the base of the array in page-sized (4 KB) intervals.
 *
 * In BusTub, we instead allocate each frame on its own (via a `std::vector<char>`) in order to easily detect buffer
 * overflow with address sanitizer. Since C++ has no notion of memory safety, it would be very easy to cast a page's
 * data pointer into some large data type and start overwriting other pages of data if they were all contiguous.
 *
 * If you would like to attempt to use more efficient data structures for your buffer pool manager, you are free to do
 * so. However, you will likely benefit significantly from detecting buffer overflow in future projects (especially
 * project 2).
 */
class FrameHeader {
  friend class BufferPoolManager;
  friend class ReadPageGuard;
  friend class WritePageGuard;

 public:
  explicit FrameHeader(frame_id_t frame_id);

 private:
  auto GetData() const -> const char *;
  auto GetDataMut() -> char *;
  void Reset();

  bool is_dirty_;
  const frame_id_t frame_id_;
  page_id_t page_id_;
  std::shared_mutex rwlatch_;
  std::atomic<size_t> pin_count_;
  std::vector<char> data_;
  std::mutex io_lock_;
  std::atomic<bool> io_done_;
  std::condition_variable cv_;

  void WLatch() { rwlatch_.lock(); }
  void WUnLatch() { rwlatch_.unlock(); }
  void RLatch() { rwlatch_.lock_shared(); }
  void RUnLatch() { rwlatch_.unlock_shared(); }
};

/**
 * @brief 缓冲池组件类, 负责在主存中的缓冲区和持久存储之间来回移动数据
 *
 * - `LRUKReplacer`提供淘汰策略, 将未使用或者cold-page逐出缓冲区
 *
 * - `DiskManager`提供持久化存储, 即读写磁盘的服务
 *
 * - `LogManager`提供容错, 故障恢复等服务
 *
 * @todo 探索淘汰策略, 加入淘汰优先级, 淘汰模式选项等功能
 * @todo 加入预取(Prefetch), 旁路缓冲(Bypass)等常规优化技术
 * @todo 探索更多工作负载下的缓冲池性能
 */
class BufferPoolManager {
 public:
  BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist = LRUK_REPLACER_K,
                    LogManager *log_manager = nullptr);
  ~BufferPoolManager();

  auto Size() const -> size_t;
  auto NewPage() -> page_id_t;
  auto DeletePage(page_id_t page_id) -> bool;
  auto CheckedWritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown)
      -> std::optional<WritePageGuard>;
  auto CheckedReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> std::optional<ReadPageGuard>;
  auto WritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> WritePageGuard;
  auto ReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> ReadPageGuard;
  auto FlushPage(page_id_t page_id) -> bool;
  void FlushAllPages();
  auto GetPinCount(page_id_t page_id) -> std::optional<size_t>;
  // 统计缓存命中率
  std::atomic<uint64_t> bpm_hint_;
  std::atomic<uint64_t> access_cnt_;

 private:
  const size_t num_frames_;
  std::atomic<page_id_t> next_page_id_;
  std::shared_ptr<std::mutex> bpm_latch_;
  std::vector<std::shared_ptr<FrameHeader>> frames_;
  std::unordered_map<page_id_t, frame_id_t> page_table_;
  std::list<frame_id_t> free_frames_;
  std::shared_ptr<LRUKReplacer> replacer_;
  std::unique_ptr<DiskScheduler> disk_scheduler_;
  LogManager *log_manager_ __attribute__((__unused__));

  void ReadDisk(std::shared_ptr<FrameHeader> &frame);
  auto WriteDisk(char *data, page_id_t page_id, frame_id_t frame_id) -> std::optional<std::future<bool>>;
  void SetFrame(std::shared_ptr<FrameHeader> &frame, frame_id_t &frame_id, page_id_t &page_id, AccessType &access_type);
};
}  // namespace bustub

//
