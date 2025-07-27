//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>
#include <unordered_map>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

class LRUNode {
 public:
  LRUNode();
  explicit LRUNode(frame_id_t fid, bool is_evictable = true);
  frame_id_t fid_;
  LRUNode *next_, *prev_;
  bool is_evictable_;
};

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  auto Victim(frame_id_t *frame_id) -> bool override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  auto Size() -> size_t override;

 private:
  std::list<LRUNode> node_list_;  // Doubly linked list to track LRU order
  std::unordered_map<frame_id_t, std::list<LRUNode>::iterator> node_map_;  // Hash map for O(1) node lookup
  std::mutex mutex_;  // Mutex for thread safety
  size_t size_;  // Number of evictable pages
  size_t capacity_;  // Maximum number of pages
};

}  // namespace bustub