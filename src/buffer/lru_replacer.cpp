//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUNode::LRUNode() : fid_(-1), next_(nullptr), prev_(nullptr), is_evictable_(true) {}
LRUNode::LRUNode(frame_id_t fid, bool is_evictable) : fid_(fid), next_(nullptr), prev_(nullptr), is_evictable_(is_evictable) {}

LRUReplacer::LRUReplacer(size_t num_pages) : size_(0), capacity_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(mutex_);
  if (node_list_.empty()) {
    return false;
  }
  for (auto it = node_list_.rbegin(); it != node_list_.rend(); ++it) {
    if (it->is_evictable_) {
      *frame_id = it->fid_;
      node_map_.erase(it->fid_);
      node_list_.erase(std::next(it).base());
      --size_;
      return true;
    }
  }
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = node_map_.find(frame_id);
  if (it != node_map_.end()) {
    if (it->second->is_evictable_) {
      it->second->is_evictable_ = false;
      --size_;
    }
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = node_map_.find(frame_id);
  if (it != node_map_.end()) {
    auto node_it = it->second;
    if (!node_it->is_evictable_) {
      node_it->is_evictable_ = true;
      ++size_;
      // Move to front since it was not evictable before
      node_list_.splice(node_list_.begin(), node_list_, node_it);
    }
    // If already evictable, do nothing
  } else if (size_ < capacity_) {
    node_list_.emplace_front(frame_id, true);
    node_map_[frame_id] = node_list_.begin();
    ++size_;
  }
}

auto LRUReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(mutex_);
  return size_;
}

}  // namespace bustub