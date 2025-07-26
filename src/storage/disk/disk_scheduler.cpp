
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

/**
 * @brief 创建后台线程
 *
 * @note - lambda表达式: 创建一个匿名函数,这个函数可以访问并可能修改其外部作用域中的所有变量,
 *         并且当这个函数被调用时,会执行`StartWorkerThread()`函数.
 * @note - `std::optional::emplace`, 成员函数, 它在`background_thread_`中直接构造了一个新的
 *         `std::thread`对象
 */
DiskScheduler::DiskScheduler(DiskManager *disk_manager, int num_frames_) : disk_manager_(disk_manager) {
  for (int i = 0; i < num_frames_; i ++) {
    request_queues_.emplace_back(Channel<std::optional<DiskRequest>>());
    background_threads_.emplace_back([i, this, request_queues = &request_queues_[i]] () {
      while(!_close) {
        auto r = request_queues->Get();
        if (r.has_value()) {
          auto page_id = r->page_id_;
          auto data = r->data_;
          auto is_write = r->is_write_;
          if (is_write) {
            disk_manager_->WritePage(page_id, data);
          } else {
            disk_manager_->ReadPage(page_id, data);
          }

          r->callback_.set_value(true); // 通知等待request的线程
        }
        return;
      }
    });
  }
}

DiskScheduler::~DiskScheduler() {
  for (auto &r : request_queues_) {
    r.Put(std::nullopt);
  }
  for (auto &t : background_threads_) {
    t.join();
  }
}

/**
 * @brief 处理上层请求
 *
 * @note 调用`Put()`将上层请求加入到请求队列`request_queue_`中
 *
 * @warning 注意`std::move`, 这里采用了移动语义, 因为`r`后续不会被使用
 */
void DiskScheduler::Schedule(DiskRequest r, frame_id_t frame_id) {
  request_queues_[frame_id].Put(std::move(r));
}

}  // namespace bustub
