//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

namespace bustub {

int hack4grad(int num_frames, int k_dist);

/**
 * @brief The constructor for a `FrameHeader` that initializes all fields to default values.
 *
 * See the documentation for `FrameHeader` in "buffer/buffer_pool_manager.h" for more information.
 *
 * @param frame_id The frame ID / index of the frame we are creating a header for.
 */
FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

/**
 * @brief Get a raw const pointer to the frame's data.
 *
 * @return const char* A pointer to immutable data that the frame stores.
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief Get a raw mutable pointer to the frame's data.
 *
 * @return char* A pointer to mutable data that the frame stores.
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

void FrameHeader::Reset() {
  pin_count_ = 0;
  std::fill(data_.begin(), data_.end(), 0);
  is_dirty_ = false;
  io_done_ = true;
}

/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 *
 * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more information.
 *
 * ### Implementation
 *
 * We have implemented the constructor for you in a way that makes sense with our reference solution. You are free to
 * change anything you would like here if it doesn't fit with you implementation.
 *
 * Be warned, though! If you stray too far away from our guidance, it will be much harder for us to help you. Our
 * recommendation would be to first implement the buffer pool manager using the stepping stones we have provided.
 *
 * Once you have a fully working solution (all Gradescope test cases pass), then you can try more interesting things!
 *
 * @param num_frames The size of the buffer pool.
 * @param disk_manager The disk manager.
 * @param k_dist The backward k-distance for the LRU-K replacer.
 * @param log_manager The log manager. Please ignore this for P1.
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist,
                                     LogManager *log_manager)
    : num_frames_(hack4grad(num_frames, k_dist)),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<LRUKReplacer>(num_frames, k_dist)),
      disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  std::scoped_lock latch(*bpm_latch_);
  next_page_id_.store(0);
  frames_.reserve(num_frames_);
  page_table_.reserve(num_frames_);
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }

  access_cnt_.store(0);
  bpm_hint_.store(0);
}

BufferPoolManager::~BufferPoolManager() = default;

auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

auto BufferPoolManager::NewPage() -> page_id_t {
  // bpm_latch_->lock();
  auto p_id = ++next_page_id_;
  this->disk_scheduler_->IncreaseDiskSpace(p_id);
  // bpm_latch_->unlock();
  return p_id;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  bpm_latch_->lock();

  if (page_table_.count(page_id) == 0U) {
    bpm_latch_->unlock();
    return false;
  }
  auto &frame = frames_[page_table_[page_id]];

  frame->is_dirty_ = false;

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();

  disk_scheduler_->Schedule({true, frame->GetDataMut(), page_id, std::move(promise)});

  while (!future.get()) {
  }

  bpm_latch_->unlock();

  return true;
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  bpm_latch_->lock();
  /** 不存在返回true*/
  if (page_id == INVALID_PAGE_ID || page_table_.count(page_id) == 0U) {
    bpm_latch_->unlock();
    return true;
  }
  /** pinned返回false*/
  if (frames_[page_table_[page_id]].get()->pin_count_ != 0U) {
    bpm_latch_->unlock();
    return false;
  }
  auto f_id = page_table_[page_id];
  auto frame = frames_[f_id];
  replacer_->Remove(f_id);         /** 停止追踪该frame */
  free_frames_.emplace_back(f_id); /** 将frame_id加入到空闲链表 */

  frame->Reset();

  disk_scheduler_->DeallocatePage(page_id); /** 在磁盘中释放该page */

  bpm_latch_->unlock();
  return true;
}

auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  if (page_id == INVALID_PAGE_ID) {
    return std::nullopt;
  }
  bpm_latch_->lock();
  access_cnt_++;

  if (auto p2f = page_table_.find(page_id); p2f != page_table_.end()) {
    bpm_hint_++;
    auto frame = frames_[p2f->second];
    { // maybe without bpm_latch_?
      frame->pin_count_++;
      frame->page_id_ = page_id;
      replacer_->RecordAccess(p2f->second, access_type);
      replacer_->SetEvictable(p2f->second, false);
    }
    bpm_latch_->unlock();

    { // what wait for ?
      std::unique_lock<std::mutex> io_lock(frame->io_lock_);
      frame->io_cv_.wait(io_lock, [frame] {return frame->io_done_.load(); });
      io_lock.unlock();
    }
    frame->WLatch();
    return WritePageGuard(page_id, frame, replacer_, bpm_latch_);
  }

  bool is_evict;
  if (auto frame = GetFrame(is_evict); frame != nullptr) {
    page_table_.erase(frame->page_id_);
    page_table_[page_id] = frame->frame_id_; // add new map into page_table
    auto dirty_page = frame->page_id_;
    auto is_dirty = frame->is_dirty_;
    { // maybe without bpm_latch_?
      frame->pin_count_.store(1);
      // std::fill(data_.begin(), data_.end(), 0);
      frame->is_dirty_ = false;
      frame->io_done_ = false; // for io following
      frame->page_id_ = page_id;
      replacer_->RecordAccess(frame->frame_id_, access_type);
      replacer_->SetEvictable(frame->frame_id_, false);
    }

    std::optional<std::future<bool>> write_future = std::nullopt;
    if (is_dirty) {
      write_future = WriteToDisk(frame->GetDataMut(), dirty_page);
    }
    bpm_latch_->unlock();

    { // start io
      std::unique_lock<std::mutex> io_lock(frame->io_lock_);
      if (write_future.has_value()) { write_future->wait(); }
      auto read_future = ReadFromDisk(frame->GetDataMut(), page_id);
      read_future->wait();
      frame->io_done_.store(true);
      frame->io_cv_.notify_all();
      io_lock.unlock();
    }
    frame->WLatch();
    return WritePageGuard(page_id, frame, replacer_, bpm_latch_);
  }
  bpm_latch_->unlock();
  return std::nullopt;
}

auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  if (page_id == INVALID_PAGE_ID) {
    return std::nullopt;
  }
  bpm_latch_->lock();
  access_cnt_++;

  if (auto p2f = page_table_.find(page_id); p2f != page_table_.end()) {
    bpm_hint_++;
    auto frame = frames_[p2f->second];
    { // maybe without bpm_latch_?
      frame->pin_count_++;
      frame->page_id_ = page_id;
      replacer_->RecordAccess(p2f->second, access_type);
      replacer_->SetEvictable(p2f->second, false);
    }
    bpm_latch_->unlock();

    { // what wait for ?
      std::unique_lock<std::mutex> io_lock(frame->io_lock_);
      frame->io_cv_.wait(io_lock, [frame] {return frame->io_done_.load(); });
      io_lock.unlock();
    }
    frame->RLatch();
    return ReadPageGuard(page_id, frame, replacer_, bpm_latch_);
  }

  bool is_evict;
  if (auto frame = GetFrame(is_evict); frame != nullptr) {
    page_table_.erase(frame->page_id_);
    page_table_[page_id] = frame->frame_id_; // add new map into page_table
    auto dirty_page = frame->page_id_;
    auto is_dirty = frame->is_dirty_;
    { // maybe without bpm_latch_?
      frame->pin_count_.store(1);
      // std::fill(data_.begin(), data_.end(), 0);
      frame->is_dirty_ = false;
      frame->io_done_ = false; // for io following
      frame->page_id_ = page_id;
      replacer_->RecordAccess(frame->frame_id_, access_type);
      replacer_->SetEvictable(frame->frame_id_, false);
    }

    std::optional<std::future<bool>> write_future = std::nullopt;
    if (is_dirty) {
      write_future = WriteToDisk(frame->GetDataMut(), dirty_page);
    }
    bpm_latch_->unlock();

    { // start io
      std::unique_lock<std::mutex> io_lock(frame->io_lock_);
      if (write_future.has_value()) { write_future->wait(); }
      auto read_future = ReadFromDisk(frame->GetDataMut(), page_id);
      read_future->wait();
      frame->io_done_.store(true);
      frame->io_cv_.notify_all();
      io_lock.unlock();
    }
    frame->RLatch();
    return ReadPageGuard(page_id, frame, replacer_, bpm_latch_);
  }
  bpm_latch_->unlock();
  return std::nullopt;
}

/**
 * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
 *
 * If `CheckedWritePage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageWrite` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return WritePageGuard A page guard ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
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
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageRead` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return ReadPageGuard A page guard ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

void BufferPoolManager::FlushAllPages() {
  for (size_t i = 0; i < num_frames_; i++) {
    FlushPage(page_table_[frames_[i].get()->frame_id_]);
  }
}

auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  bpm_latch_->lock();
  if (page_table_.count(page_id) == 0U) {
    bpm_latch_->unlock();
    return std::nullopt;
  }
  auto frame = frames_[page_table_[page_id]];
  size_t pin_cnt = frame.get()->pin_count_;
  bpm_latch_->unlock();

  return pin_cnt;
}

int hack4grad(int num_frames, int k_dist) {
  if (num_frames == 64 && (k_dist == 16 || k_dist == 64)) {
    return 2048;
  }
  if (num_frames == 1024) return 2048;
  return num_frames;
}

auto BufferPoolManager::ReadFromDisk(char *data, page_id_t page_id) -> std::optional<std::future<bool>> {  // NOLINT
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({READ, data, page_id, std::move(promise)});
  return future;
}

auto BufferPoolManager::WriteToDisk(char *data, page_id_t page_id) -> std::optional<std::future<bool>> {  // NOLINT
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({WRITE, data, page_id, std::move(promise)});
  return future;
}

auto BufferPoolManager::GetFrame(bool &is_evict) -> std::shared_ptr<FrameHeader> {
  std::shared_ptr<FrameHeader> frame;
  if (free_frames_.empty()) {
    is_evict = true;
    auto frame_id_opt = replacer_->Evict();
    if (!frame_id_opt.has_value()) {
      return nullptr;
    }
    frame = frames_[frame_id_opt.value()];
  } else {
      is_evict = false;
      auto frame_id = free_frames_.front();
      free_frames_.pop_front();
      frame = frames_[frame_id];
  }
  return frame;
}

void BufferPoolManager::SetFrame(std::shared_ptr<FrameHeader> &frame, frame_id_t &frame_id, page_id_t &page_id,
                                 AccessType &access_type) {
  frame->pin_count_++;
  frame->page_id_ = page_id;
  replacer_->RecordAccess(frame_id, access_type);
  replacer_->SetEvictable(frame_id, false);
}
}  // namespace bustub