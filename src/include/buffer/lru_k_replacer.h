#pragma once

#include <limits>
#include <list>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

class LRUKNode {
 public:
  LRUKNode();
  explicit LRUKNode(frame_id_t fid, AccessType access_type = AccessType::Unknown);

  frame_id_t fid_;
  LRUKNode *next_, *prev_;
  size_t access_count_;
  bool is_evictable_;
  AccessType last_access_;
};

class LRUKReplacer {
 public:
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  ~LRUKReplacer();

  auto Evict() -> std::optional<frame_id_t>;

  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  void Remove(frame_id_t frame_id);

  auto Size() -> size_t;

 private:
  void AddToHotHead(LRUKNode *node);
  void AddToColdTail(LRUKNode *node);
  void AddToScanTail(LRUKNode *node);
  LRUKNode* RemoveNode(LRUKNode *node);

  std::unordered_map<frame_id_t, LRUKNode*> lruk_map_;
  size_t curr_size_{0};
  size_t replacer_size_;
  size_t k_;
  std::mutex latch_;

  LRUKNode *hot_head_, *hot_tail_;
  LRUKNode *cold_head_, *cold_tail_;
  LRUKNode *scan_head_, *scan_tail_;
};

}  // namespace bustub