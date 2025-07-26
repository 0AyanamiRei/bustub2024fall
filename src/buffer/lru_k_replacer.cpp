#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode() 
    : fid_(-1), 
      next_(nullptr), 
      prev_(nullptr), 
      access_count_(0), 
      is_evictable_(false), 
      last_access_(AccessType::Unknown) {}

LRUKNode::LRUKNode(frame_id_t fid, AccessType access_type)
    : fid_(fid), 
      next_(nullptr), 
      prev_(nullptr), 
      access_count_(1), 
      is_evictable_(false), 
      last_access_(access_type) {}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  BUSTUB_ASSERT(num_frames > 0, "num_frames must be positive");
  BUSTUB_ASSERT(k > 0, "k must be positive");

  // 初始化热队列
  hot_head_ = new LRUKNode();
  hot_tail_ = new LRUKNode();
  hot_head_->next_ = hot_tail_;
  hot_tail_->prev_ = hot_head_;

  // 初始化冷队列
  cold_head_ = new LRUKNode();
  cold_tail_ = new LRUKNode();
  cold_head_->next_ = cold_tail_;
  cold_tail_->prev_ = cold_head_;

  // 初始化扫描队列
  scan_head_ = new LRUKNode();
  scan_tail_ = new LRUKNode();
  scan_head_->next_ = scan_tail_;
  scan_tail_->prev_ = scan_head_;
}

LRUKReplacer::~LRUKReplacer() {
  // 清理热队列
  LRUKNode *temp = hot_head_;
  while (temp != hot_tail_) {
    LRUKNode *next = temp->next_;
    delete temp;
    temp = next;
  }
  delete hot_tail_;

  // 清理冷队列
  temp = cold_head_;
  while (temp != cold_tail_) {
    LRUKNode *next = temp->next_;
    delete temp;
    temp = next;
  }
  delete cold_tail_;

  // 清理扫描队列
  temp = scan_head_;
  while (temp != scan_tail_) {
    LRUKNode *next = temp->next_;
    delete temp;
    temp = next;
  }
  delete scan_tail_;
}

void LRUKReplacer::AddToHotHead(LRUKNode *node) {
  node->next_ = hot_head_->next_;
  node->prev_ = hot_head_;
  hot_head_->next_->prev_ = node;
  hot_head_->next_ = node;
}

void LRUKReplacer::AddToColdTail(LRUKNode *node) {
  node->next_ = cold_tail_;
  node->prev_ = cold_tail_->prev_;
  cold_tail_->prev_->next_ = node;
  cold_tail_->prev_ = node;
}

void LRUKReplacer::AddToScanTail(LRUKNode *node) {
  node->next_ = scan_tail_;
  node->prev_ = scan_tail_->prev_;
  scan_tail_->prev_->next_ = node;
  scan_tail_->prev_ = node;
}

LRUKNode* LRUKReplacer::RemoveNode(LRUKNode *node) {
  node->prev_->next_ = node->next_;
  node->next_->prev_ = node->prev_;
  return node;
}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> lock(latch_);

  // 优先从扫描队列驱逐（LIFO，从尾部）
  LRUKNode *temp = scan_tail_->prev_;
  while (temp != scan_head_) {
    if (temp->is_evictable_) {
      frame_id_t fid = temp->fid_;
      lruk_map_.erase(fid);
      RemoveNode(temp);
      delete temp;
      curr_size_--;
      return fid;
    }
    temp = temp->prev_;
  }

  // 从冷队列驱逐（FIFO，从头部）
  temp = cold_head_->next_;
  while (temp != cold_tail_) {
    if (temp->is_evictable_) {
      frame_id_t fid = temp->fid_;
      lruk_map_.erase(fid);
      RemoveNode(temp);
      delete temp;
      curr_size_--;
      return fid;
    }
    temp = temp->next_;
  }

  // 从热队列驱逐（LRU，从尾部）
  temp = hot_tail_->prev_;
  while (temp != hot_head_) {
    if (temp->is_evictable_) {
      frame_id_t fid = temp->fid_;
      lruk_map_.erase(fid);
      RemoveNode(temp);
      delete temp;
      curr_size_--;
      return fid;
    }
    temp = temp->prev_;
  }

  return std::nullopt;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);

  if (access_type == AccessType::Scan) {
    if (lruk_map_.count(frame_id) != 0U) {
      LRUKNode *node = lruk_map_[frame_id];
      RemoveNode(node);
      node->access_count_ = 1;
      node->last_access_ = AccessType::Scan;
      AddToScanTail(node);
    } else {
      LRUKNode *node = new LRUKNode(frame_id, AccessType::Scan);
      AddToScanTail(node);
      lruk_map_[frame_id] = node;
    }
    return;
  }

  // 非扫描访问
  if (lruk_map_.count(frame_id) != 0U) {
    LRUKNode *node = lruk_map_[frame_id];
    if (node->last_access_ == AccessType::Scan) {
      RemoveNode(node);
      node->last_access_ = access_type;
      node->access_count_ = 1;
      AddToColdTail(node);
    } else {
      node->access_count_++;
      if (node->access_count_ >= k_) {
        RemoveNode(node);
        AddToHotHead(node);
      }
    }
  } else {
    LRUKNode *node = new LRUKNode(frame_id, access_type);
    AddToColdTail(node);
    lruk_map_[frame_id] = node;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) <= replacer_size_ && frame_id >= 0, "Invalid frame_id");
  if (lruk_map_.count(frame_id) == 0U) {
    return;
  }

  LRUKNode *node = lruk_map_[frame_id];
  if (node->is_evictable_ != set_evictable) {
    curr_size_ += set_evictable ? 1 : -1;
    node->is_evictable_ = set_evictable;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (lruk_map_.count(frame_id) == 0U) {
    return;
  }

  LRUKNode *node = lruk_map_[frame_id];
  BUSTUB_ASSERT(node->is_evictable_, "Cannot remove non-evictable frame");
  RemoveNode(node);
  lruk_map_.erase(frame_id);
  delete node;
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub