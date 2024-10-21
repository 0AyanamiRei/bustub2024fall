//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <mutex>  // NOLINT
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
  size_t accses_k_;
  bool is_evictable_;
  AccessType last_access_;
};

class LRUKReplacer {
 public:
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  ~LRUKReplacer();

  /**
   * @brief 找到具有largest backward k-distance的frame, 然后驱逐该frame, 对于
   *        被访问次数小于k的frame, 在我的设计中就是在`kmid_`之后, 如果存在多个:
   *        then evict frame with earliest timestamp based on LRU.
   *
   * @note Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history. 当前的策略是先在第一部分链表从`kmid_->head_`寻找可驱逐的frame, 如果不存在则在第二部分链表
   * 从`kmid_->tail_`寻找可驱逐的frame, 如果整个链表都不存在则返回std::nullopt
   *
   * @param[out] frame_id id of frame that is evicted.
   *
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   *
   * @warning 只有被标记为`evictable`的帧才允许被驱逐
   */
  auto Evict() -> std::optional<frame_id_t>;

  /**
   * @brief 在当前timestamp, 给定frame_id的page被访问
   *
   * @note - 如果该page没有被访问过, 那么为其创建一个新条目
   * @note - 如果该page已被记录, 增加其`accses_k_`, 可能需要调整链表位置, 跨越`kmid_`
   *
   * @param[in] frame_id id of frame that received a new access.
   * @param[in] access_type type of access that was received. This parameter is only needed for
   *            leaderboard tests.
   */
  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  /**
   * @brief 切换一个frame的可驱逐状态, 同时维护`curr_size_`, 应该和可驱逐
   *        frame的数量一致(区别frame和page)
   *
   * @warning 如果frame无效, 抛出一个异常或者终止进程
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief 移除一个指定`frame_id`的可驱逐的frame, 同时需要维护`curr_size_`
   *
   * @warning - 如果被调用在一个不可驱逐的frame上, 抛出一个异常或者终止进程
   * @warning - 如果指定的frame没有找到, 立刻返回
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

 private:
  /** @brief 在链表头添加 */
  void Add2head(LRUKNode *temp);

  /** @brief 在链表尾添加
   * @warning 需要处理k=1的时候, lru-k此时等价于lru, 调用`add_beforeKmid()`
   */
  void Add2tail(LRUKNode *temp);

  void AddAfterkmid(LRUKNode *temp);

  void AddAftertail(LRUKNode *temp);

  /** @brief 从链表中移除, 并返回 */
  auto MyRemove(LRUKNode *temp) -> LRUKNode *;

  void DebugShow();

  std::unordered_map<frame_id_t, LRUKNode *> lruk_map_;
  size_t curr_size_{0};
  size_t replacer_size_;
  size_t k_;
  std::mutex latch_;

  LRUKNode *head_, *kmid_, *tail_, *scan_;
};

}  // namespace bustub