//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUNode::LRUNode() : fid_(-1), next_(nullptr), prev_(nullptr), is_evictable_(true) {}
LRUNode::LRUNode(frame_id_t fid, bool is_evictable) : fid_(fid), next_(nullptr), prev_(nullptr), is_evictable_(is_evictable) {}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : size_(0), capacity_(num_frames) {}

LRUKReplacer::~LRUKReplacer() = default;

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> lock(mutex_);
  if (node_list_.empty()) {
    return std::nullopt;
  }
  for (auto it = node_list_.rbegin(); it != node_list_.rend(); ++it) {
    if (it->is_evictable_) {
      frame_id_t frame_id = it->fid_;
      node_map_.erase(frame_id);
      node_list_.erase(std::next(it).base());
      --size_;
      return frame_id;
    }
  }
  return std::nullopt;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = node_map_.find(frame_id);
  if (it != node_map_.end()) {
    // Move existing node to front (most recently used)
    node_list_.splice(node_list_.begin(), node_list_, it->second);
  } else if (size_ < capacity_) {
    // Add new node to front if capacity allows
    node_list_.emplace_front(frame_id, true);
    node_map_[frame_id] = node_list_.begin();
    ++size_;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = node_map_.find(frame_id);
  if (it != node_map_.end()) {
    if (it->second->is_evictable_ != set_evictable) {
      it->second->is_evictable_ = set_evictable;
      size_ += set_evictable ? 1 : -1;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = node_map_.find(frame_id);
  if (it != node_map_.end() && it->second->is_evictable_) {
    node_list_.erase(it->second);
    node_map_.erase(it);
    --size_;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(mutex_);
  return size_;
}

}  // namespace bustub