
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
DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  for (auto &r : request_queue_) {
    r.Put(std::nullopt);
  }

  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

/**
 * @brief 处理上层请求
 *
 * @note 调用`Put()`将上层请求加入到请求队列`request_queue_`中
 *
 * @warning 注意`std::move`, 这里采用了移动语义, 因为`r`后续不会被使用
 */
void DiskScheduler::Schedule(DiskRequest r) {
  if (r.page_id_ < 0 || r.data_ == nullptr) {
    return;
  }

  auto index = r.page_id_ % bk_threads;
  request_queue_[index].Put(std::move(r));
}

/**
 * @brief 实际处理磁盘请求的"后台线程"
 *
 * @note - 永不退出, 直到该对象调用了析构函数
 * @note - 调用`Get()`从请求队列中`request_queue_`获取请求并处理, 这里没有对每个请求开线程处理
 *
 * @warning - 需要在任务结束后使用回调机制:`callback_`
 */
void DiskScheduler::StartWorkerThread() {
  std::vector<std::thread> threads;

  for (auto &request : request_queue_) {
    threads.emplace_back([&request, this] {
      for (;;) {
        // printf("request_queie size : %d\n", request.Size());
        auto r = request.Get();
        if (r == std::nullopt) {
          return;
        }
        auto page_id = r.value().page_id_;
        auto data = r.value().data_;
        auto is_write = r.value().is_write_;
        if (is_write) {
          disk_manager_->WritePage(page_id, data);
        } else {
          disk_manager_->ReadPage(page_id, data);
        }

        r.value().callback_.set_value(true);
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }
}

}  // namespace bustub