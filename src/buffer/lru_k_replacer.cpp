//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/*
  锁策略是对外接口: Evict(), RecordAccess()这些一个函数整个过程都持latch_操作

  内部使用的函数默认是带锁的情况下使用, 也就是被上面这些接口函数调用;
*/

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode()
    : fid_(-1),
      next_(nullptr),
      prev_(nullptr),
      accses_k_(-1),
      is_evictable_(false),
      last_access_(AccessType::Unknown) {}

LRUKNode::LRUKNode(frame_id_t fid, [[maybe_unused]] AccessType access_type)
    : next_(nullptr), prev_(nullptr), accses_k_(1), is_evictable_(false), last_access_(access_type) {
  fid_ = fid;
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  BUSTUB_ASSERT(num_frames > 0, "param to LRUKReplacer: num_frames <= 0");
  BUSTUB_ASSERT(k > 0, "param to LRUKReplacer: k <= 0");

  head_ = new LRUKNode();
  tail_ = new LRUKNode();
  scan_ = new LRUKNode();
  kmid_ = new LRUKNode();

  head_->next_ = kmid_;
  kmid_->next_ = tail_;
  tail_->next_ = scan_;
  scan_->prev_ = tail_;
  tail_->prev_ = kmid_;
  kmid_->prev_ = head_;
}

LRUKReplacer::~LRUKReplacer() {
  LRUKNode *temp = head_;
  while (temp != scan_) {
    LRUKNode *next_node = temp->next_;
    delete temp;
    temp = next_node;
  }
  delete scan_;
}

void LRUKReplacer::Add2head(LRUKNode *temp) {
  temp->next_ = head_->next_;
  head_->next_->prev_ = temp;
  head_->next_ = temp;
  temp->prev_ = head_;
}

void LRUKReplacer::Add2tail(LRUKNode *temp) {
  if (k_ == 1) { /** k_ = 1, LRU-K就等价于LRU */
    Add2head(temp);
    return;
  }
  tail_->prev_->next_ = temp;
  temp->prev_ = tail_->prev_;
  temp->next_ = tail_;
  tail_->prev_ = temp;
}

void LRUKReplacer::AddAfterkmid(LRUKNode *temp) {
  temp->next_ = kmid_->next_;
  kmid_->next_ = temp;
  temp->prev_ = kmid_;
  temp->next_->prev_ = temp;
}

void LRUKReplacer::AddAftertail(LRUKNode *temp) {
  temp->next_ = tail_->next_;
  tail_->next_ = temp;
  temp->prev_ = tail_;
  temp->next_->prev_ = temp;
}

auto LRUKReplacer::MyRemove(LRUKNode *temp) -> LRUKNode * {
  temp->prev_->next_ = temp->next_;
  temp->next_->prev_ = temp->prev_;
  return temp;
}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  LRUKNode *temp = nullptr;
  frame_id_t fid;
  latch_.lock();
  if (tail_->next_ != scan_) { /** 第三部分链表 按LIFO 从tail_->scan_ */
    temp = tail_->next_;
    while (temp != scan_) {
      if (temp->is_evictable_) { /** 第三部分驱逐 */
        fid = temp->fid_;
        lruk_map_.erase(fid); /** 从哈希表中删除 */
        MyRemove(temp);       /** 从链表中删除 */
        delete temp;          /** 释放内存 */
        curr_size_--;         /** 修改curr_size_的大小 */
        latch_.unlock();      /** 退出前释放锁 */
        return fid;
      }
      temp = temp->next_;
    }
  }

  if (kmid_->next_ != tail_) { /** 第二部分链表 按FIFO 从kmid_->tail_ */
    temp = kmid_->next_;
    while (temp != tail_) {
      if (temp->is_evictable_) { /** 第二部分驱逐 */
        fid = temp->fid_;
        lruk_map_.erase(fid); /** 从哈希表中删除 */
        MyRemove(temp);       /** 从链表中删除 */
        delete temp;          /** 释放内存 */
        curr_size_--;         /** 修改curr_size_的大小 */
        latch_.unlock();      /** 退出前释放锁 */
        return fid;
      }
      temp = temp->next_;
    }
  }

  if (kmid_->prev_ != head_) { /** 第一部分链表 按LRU从kmid_->head_ */
    temp = kmid_->prev_;

    while (temp != head_) {
      if (temp->is_evictable_) { /** 第一部分驱逐 */
        fid = temp->fid_;
        lruk_map_.erase(fid); /** 从哈希表中删除 */
        MyRemove(temp);       /** 从链表中删除 */
        delete temp;          /** 释放内存 */
        curr_size_--;         /** 修改curr_size_的大小 */
        latch_.unlock();      /** 退出前释放锁 */
        return fid;
      }
      temp = temp->prev_;
    }
  }
  latch_.unlock(); /** 退出前释放锁 */
  return std::nullopt;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  latch_.lock();

  LRUKNode *temp = nullptr;
  if (access_type == AccessType::Scan) {   /** 以scan类型第一次访问 */
    if (lruk_map_.count(frame_id) != 0U) { /** 再次访问 */
      temp = lruk_map_[frame_id];
      temp->last_access_ = AccessType::Scan;
      temp->accses_k_ = 1;
      AddAftertail(MyRemove(temp));
      BUSTUB_ASSERT(temp->last_access_ == AccessType::Scan, "连续两次以Scan方式访问");
    } else {
      temp = new LRUKNode(frame_id, AccessType::Scan);
      AddAftertail(temp);
      lruk_map_[frame_id] = temp;
    }
    latch_.unlock();
    return;
  }

  if (lruk_map_.count(frame_id) != 0U) { /** 再次访问 */
    temp = lruk_map_[frame_id];
    if (temp->last_access_ == AccessType::Scan) { /** 在scan队列中 */
      temp->last_access_ = AccessType::Unknown;
      Add2head(MyRemove(temp));        /** 处理为第一次访问, 即加入第一部分链表 */
    } else {                           /** 其余访问方式的 */
      if (++(temp->accses_k_) >= k_) { /** 满足k_, 跨越kmid_ */
        Add2head(MyRemove(temp));      /** 按照LRU方式加入第一部分链表 */
      }
      /** 对于以及小于k次的不动即可, 按FIFO处理 */
    }
  } else {                         /** 新frame */
    temp = new LRUKNode(frame_id); /** 开辟内存空间 */
    Add2tail(temp);                /** 加入cold list */
    lruk_map_[frame_id] = temp;    /** 加入哈希表 */
  }
  latch_.unlock(); /** 退出前释放锁 */
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) <= replacer_size_ && frame_id >= 0, "frame_id error\n");
  if (lruk_map_.count(frame_id) == 0U) {
    latch_.unlock(); /** 退出前释放锁 */
    return;
  }

  LRUKNode *temp = lruk_map_[frame_id];

  curr_size_ += (temp->is_evictable_ ^ set_evictable) != 0 ? (set_evictable ? 1 : -1) : 0; /** 维护可用frame */

  temp->is_evictable_ = set_evictable; /** 修改该frame状态 */
  latch_.unlock();                     /** 退出前释放锁 */
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  if (lruk_map_.count(frame_id) == 0U) {
    latch_.unlock(); /** 退出前释放锁 */
    return;
  }

  LRUKNode *temp = MyRemove(lruk_map_[frame_id]); /** 从链表中移除 */
  lruk_map_.erase(frame_id);                      /** 从哈希表中移除 */
  delete temp;                                    /** 释放内存空间 */
  curr_size_--;                                   /** 维护可用frame*/
  latch_.unlock();                                /** 退出前释放锁 */
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

// void LRUKReplacer::debug_show(){
//   latch_.lock();
//   LRUKNode *temp = head_->next_;
//   while(temp != tail_){
//     if(temp != kmid_) {
//       cout << "fid=" << temp->fid_ << " k=" << temp->accses_k_ << " " << temp->is_evictable_ << endl;
//     }
//     temp = temp->next_;
//   }
//   latch_.unlock();       /** 退出前释放锁 */
// }

}  // namespace bustub