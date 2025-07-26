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
    : num_frames_(num_frames),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<LRUKReplacer>(num_frames, k_dist)),
      disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  page_table_.reserve(num_frames_);
  frames_.reserve(num_frames_);
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_front(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() = default;

auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

auto BufferPoolManager::NewPage() -> page_id_t {
  auto p_id = ++next_page_id_;
  this->disk_scheduler_->IncreaseDiskSpace(p_id);
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

  disk_scheduler_->Schedule({page_id, frame->data_.size(), std::move(promise), WRITE, frame->GetDataMut()}, frame->frame_id_);

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
  free_frames_.push_front(f_id); /** 将frame_id加入到空闲链表 */

  frame->Reset();

  disk_scheduler_->DeallocatePage(page_id); /** 在磁盘中释放该page */

  bpm_latch_->unlock();
  return true;
}

auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  if (page_id == INVALID_PAGE_ID) {
    return std::nullopt;
  }

  std::lock_guard<std::mutex> lock(*bpm_latch_);
  access_cnt_++;
  
  // check cached?
  if (auto p2f = page_table_.find(page_id); p2f != page_table_.end()) {
    // maybe we not need to check dirty if the page in frame
    auto frame = this->frames_[p2f->second];
    frame->pin_count_++;
    replacer_->RecordAccess(frame->frame_id_, access_type);
    // replacer_->SetEvictable(frame->frame_id_, false); 语义上应该包含在RecordAccess
    return WritePageGuard(page_id, frame, replacer_, bpm_latch_);
  } else { // not cached

  }


  // if (page_table_.count(page_id) != 0U) {
  //   bpm_hint_++;
  //   auto frame_id = page_table_[page_id];
  //   auto &frame_cached = frames_[frame_id];
  //   SetFrame(std::ref(frame_cached), frame_id, page_id, access_type);
  //   bpm_latch_->unlock();
  //   std::unique_lock<std::mutex> lk(frame_cached->io_lock_);
  //   frame_cached->cv_.wait(lk, [frame_cached] { return frame_cached->io_done_.load(); });
  //   lk.unlock();
  //   frame_cached->WLatch();
  //   WritePageGuard page_guard(page_id, frame_cached, replacer_, bpm_latch_);
  //   return page_guard;  // 隐式调用移动构造创建了std::optional<T>对象 详情见RVO优化
  // }

  // /** case2: 分配新的frames_ */
  // if (!free_frames_.empty()) {
  //   auto frame_id = free_frames_.back();
  //   free_frames_.pop_back();
  //   auto frame_new = frames_[frame_id];
  //   page_table_[page_id] = frame_id;
  //   frame_new->Reset();
  //   SetFrame(std::ref(frame_new), frame_id, page_id, access_type);
  //   frame_new->io_done_.store(false); /** 设置IO标志:开始 */
  //   bpm_latch_->unlock();
  //   std::unique_lock<std::mutex> lock(frame_new->io_lock_);
  //   ReadDisk(std::ref(frame_new));
  //   frame_new->io_done_.store(true); /** 设置IO标志:结束 */
  //   frame_new->cv_.notify_all();     /** 唤醒 */
  //   lock.unlock();
  //   frame_new->WLatch(); /** 带写锁IO */
  //   WritePageGuard page_guard(page_id, frame_new, replacer_, bpm_latch_);
  //   return page_guard;
  // }

  // /** case3: 驱逐frames_ */
  // std::optional<frame_id_t> frame_id_opt = replacer_->Evict();
  // if (frame_id_opt.has_value()) {
  //   auto frame_id = frame_id_opt.value();
  //   auto frame = frames_[frame_id];
  //   page_table_.erase(frame->page_id_);
  //   page_table_[page_id] = frame_id;
  //   page_id_t dirty_page_id = frame->page_id_;
  //   bool dirty_old = frame->is_dirty_;
  //   frame->pin_count_.store(0);  // Reset()
  //   SetFrame(std::ref(frame), frame_id, page_id, access_type);
  //   frame->io_done_.store(false); /** 设置IO标志:开始 */
  //   std::optional<std::future<bool>> future_opt = std::nullopt;
  //   if (dirty_old) {             // 避免另外一个线程分配frame从磁盘读page
  //     frame->is_dirty_ = false;  // Reset()
  //     future_opt = WriteDisk(frame->GetDataMut(), dirty_page_id);
  //   }
  //   bpm_latch_->unlock();

  //   std::unique_lock<std::mutex> lock(frame->io_lock_);
  //   if (dirty_old) {
  //     while (!future_opt.value().get()) {
  //     }
  //   }
  //   std::fill(frame->data_.begin(), frame->data_.end(), 0);  // Reset()
  //   ReadDisk(std::ref(frame));
  //   frame->io_done_.store(true); /** 设置IO标志:结束 */
  //   frame->cv_.notify_all();     /** 唤醒 */
  //   lock.unlock();
  //   frame->WLatch(); /** 带写锁IO */
  //   WritePageGuard page_guard(page_id, frame, replacer_, bpm_latch_);
  //   return page_guard;
  // }
  // bpm_latch_->unlock();
  // return std::nullopt;
}

auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  if (page_id == INVALID_PAGE_ID) {
    return std::nullopt;
  }

  std::unique_lock<std::mutex> lock(*bpm_latch_);
  access_cnt_++;

  // check cached?
  if (auto p2f = page_table_.find(page_id); p2f != page_table_.end()) {
    // maybe we not need to check dirty if the page in frame
    auto frame = frames_[p2f->second];
    if (page_id == frame->page_id_) {
      frame->RLatch();
      lock.unlock();
      frame->pin_count_++;
      replacer_->RecordAccess(frame->frame_id_, access_type);
      // replacer_->SetEvictable(frame->frame_id_, false); 语义上应该包含在RecordAccess
      return ReadPageGuard(page_id, frame, replacer_, bpm_latch_);
    }
  }
  
  // not cached
  bool is_evict;
  auto frame = GetFrame(is_evict);
  //**********可能不用急着改, 在新的page load之前, 该frame上的page都是可读的***************// 
  //******换句话说,对于frame的数据, 读的时候是在往磁盘写, 写的时候是在读入磁盘数据***********// 
  // page_table_.erase(frame->page_id_); 我们应该是允许多个page_id 映射同一个frame的
  page_table_[page_id] = frame->frame_id_;
  frame->RLatch();
  lock.unlock();
  // check dirty
  std::future<bool> write_future;
  std::thread write_thread;

  //****************************************//
  // 现在的问题是, 额外开的线程能拿着写锁, 



  // /** 驱逐frames_ */
  // std::optional<frame_id_t> frame_id_opt = replacer_->Evict();
  // if (frame_id_opt.has_value()) {
  //   auto frame_id = frame_id_opt.value();
  //   auto frame = frames_[frame_id];
  //   page_table_.erase(frame->page_id_);
  //   page_table_[page_id] = frame_id;
  //   page_id_t dirty_page_id = frame->page_id_;
  //   bool dirty_old = frame->is_dirty_;
  //   frame->pin_count_.store(0);  // Reset()
  //   SetFrame(std::ref(frame), frame_id, page_id, access_type);
  //   frame->io_done_.store(false); /** 设置IO标志:开始 */
  //   std::optional<std::future<bool>> future_opt = std::nullopt;
  //   if (dirty_old) {             // 避免另外一个线程分配frame从磁盘读page
  //     frame->is_dirty_ = false;  // Reset()
  //     future_opt = WriteDisk(frame->GetDataMut(), dirty_page_id);
  //   }
  //   bpm_latch_->unlock();
  //   /** 插队risk */
  //   std::unique_lock<std::mutex> lock(frame->io_lock_);
  //   if (dirty_old) {
  //     while (!future_opt.value().get()) {
  //     }
  //   }
  //   std::fill(frame->data_.begin(), frame->data_.end(), 0);  // Reset()
  //   ReadDisk(std::ref(frame));
  //   frame->io_done_.store(true); /** 设置IO标志:结束 */
  //   frame->cv_.notify_all();     /** 唤醒 */
  //   lock.unlock();
  //   frame->RLatch(); /** 带读锁IO */
  //   ReadPageGuard page_guard(page_id, frame, replacer_, bpm_latch_);
  //   return page_guard;
  // }
  // bpm_latch_->unlock();
  // return std::nullopt;
}

auto BufferPoolManager::SyncGetWritePage(page_id_t page_id, AccessType access_type)
    -> std::optional<WritePageGuard> {
  if (page_id == INVALID_PAGE_ID) {
    return std::nullopt;
  }

  std::lock_guard<std::mutex> lock(*bpm_latch_);
  access_cnt_++;

  if (auto p2f = page_table_.find(page_id); p2f != page_table_.end()) {
    auto frame = this->frames_[p2f->second];
    frame->pin_count_++;
    replacer_->RecordAccess(frame->frame_id_, access_type);
    replacer_->SetEvictable(frame->frame_id_, false); // 语义上应该包含在RecordAccess
    return WritePageGuard(page_id, frame, replacer_, bpm_latch_);
  }

  bool is_evict;
  auto frame = GetFrame(is_evict);
  // 修改replacer_端
  {
    replacer_->RecordAccess(frame->frame_id_, access_type);
    replacer_->SetEvictable(frame->frame_id_, false); // 语义上应该包含在RecordAccess
  }

  {
    page_table_[page_id] = frame->frame_id_;
    page_table_.erase(frame->page_id_);
  }
  // check dirty
  if (frame->is_dirty_) {
    WriteToDisk(frame->GetDataMut(), frame->data_.size(), frame->page_id_, frame->frame_id_);
  }
  // read任务自动reset page
  auto read_future = ReadFromDisk(frame->GetDataMut(), frame->data_.size() , frame->page_id_, frame->frame_id_);
  
  // 同一个frame的读写排队 先添加WriteToDisk, 再添加ReadFromDisk 只需要等待read
  if (read_future.has_value()) {
    read_future->wait();
  }

  frame->pin_count_++;
  frame->is_dirty_ = false;
  frame->page_id_ = page_id;

  return WritePageGuard(page_id, frame, replacer_, bpm_latch_);
}

auto BufferPoolManager::SyncGetReadPage(page_id_t page_id, AccessType access_type)
    -> std::optional<ReadPageGuard> {
  if (page_id == INVALID_PAGE_ID) {
    return std::nullopt;
  }

  std::lock_guard<std::mutex> lock(*bpm_latch_);
  access_cnt_++;

  if (auto p2f = page_table_.find(page_id); p2f != page_table_.end()) {
    auto frame = this->frames_[p2f->second];
    frame->pin_count_++;
    replacer_->RecordAccess(frame->frame_id_, access_type);
    replacer_->SetEvictable(frame->frame_id_, false); // 语义上应该包含在RecordAccess
    return ReadPageGuard(page_id, frame, replacer_, bpm_latch_);
  }

  bool is_evict;
  auto frame = GetFrame(is_evict);
  // 修改replacer_端
  {
    replacer_->RecordAccess(frame->frame_id_, access_type);
    replacer_->SetEvictable(frame->frame_id_, false); // 语义上应该包含在RecordAccess
  }

  {
    page_table_[page_id] = frame->frame_id_;
    page_table_.erase(frame->page_id_);
  }
  // check dirty
  if (frame->is_dirty_) {
    WriteToDisk(frame->GetDataMut(), frame->data_.size(), frame->page_id_, frame->frame_id_);
  }
  // read任务自动reset page
  auto read_future = ReadFromDisk(frame->GetDataMut(), frame->data_.size(), frame->page_id_, frame->frame_id_);
  
  // 同一个frame的读写排队 先添加WriteToDisk, 再添加ReadFromDisk 只需要等待read
  if (read_future.has_value()) {
    read_future->wait();
  }

  frame->pin_count_++;
  frame->is_dirty_ = false;
  frame->page_id_ = page_id;

  return ReadPageGuard(page_id, frame, replacer_, bpm_latch_);
}

auto BufferPoolManager::AyncGetWritePage(page_id_t page_id, AccessType access_type)
    -> std::optional<WritePageGuard> {

    }
auto BufferPoolManager::AyncGetReadPage(page_id_t page_id, AccessType access_type)
    -> std::optional<ReadPageGuard> {

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
  auto guard_opt = SyncGetWritePage(page_id, access_type);

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
  auto guard_opt = SyncGetReadPage(page_id, access_type);

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


auto BufferPoolManager::GetFrame(bool &is_evict) -> std::shared_ptr<FrameHeader> {
  std::shared_ptr<FrameHeader> frame;
  if (is_evict = !frames_.empty(); is_evict) {
    auto frame_id_opt = replacer_->Evict();
    BUSTUB_ASSERT(frame_id_opt.has_value(), "can not evict any frame");
    frame = frames_[frame_id_opt.value()];
  } else {
      auto frame_id = free_frames_.front();
      free_frames_.pop_front();
      frame = frames_[frame_id];
  }
}

auto BufferPoolManager::ReadFromDisk(char *data, size_t data_sz, page_id_t page_id, frame_id_t frame_id)
    -> std::optional<std::future<bool>> {
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({page_id, data_sz, std::move(promise), READ, data}, frame_id);
  return future;
}

auto BufferPoolManager::WriteToDisk(char *data, size_t data_sz, page_id_t page_id, frame_id_t frame_id)
    -> std::optional<std::future<bool>> {  // NOLINT
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({page_id, data_sz, std::move(promise), WRITE, data}, frame_id);
  return future;
}

void BufferPoolManager::SetFrame(std::shared_ptr<FrameHeader> &frame, frame_id_t &frame_id, page_id_t &page_id,
                                 AccessType &access_type) {
  frame->pin_count_++;
  frame->page_id_ = page_id;
  replacer_->RecordAccess(frame_id, access_type);
  replacer_->SetEvictable(frame_id, false);
}
}  // namespace bustub
