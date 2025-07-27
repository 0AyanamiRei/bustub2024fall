/**
 * b_plus_tree.h
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
#pragma once

#include <algorithm>
#include <deque>
#include <filesystem>
#include <iostream>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

struct PrintableBPlusTree;

/**
 * @brief Definition of the Context class.
 *
 * Hint: This class is designed to help you keep track of the pages
 * that you're modifying or accessing.
 */
class Context {
 public:
  // When you insert into / remove from the B+ tree, store the write guard of header page here.
  // Remember to drop the header page guard and set it to nullopt when you want to unlock all.
  std::optional<WritePageGuard> header_page_{std::nullopt};

  // Save the root page id here so that it's easier to know if the current page is the root page.
  page_id_t root_page_id_{INVALID_PAGE_ID};

  // Store the write guards of the pages that you're modifying here.
  std::deque<WritePageGuard> write_set_;

  // You may want to use this when getting value, but not necessary.
  std::deque<ReadPageGuard> read_set_;

  auto IsRootPage(page_id_t page_id) -> bool { return page_id == root_page_id_; }
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

// Main class providing the API for the Interactive B+ Tree.
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator, int leaf_max_size = LEAF_PAGE_SLOT_CNT,
                     int internal_max_size = INTERNAL_PAGE_SLOT_CNT);

  // 判断树是否为空, 即检查`header_page_id`指向page记录的`root_page_id_`是否有效
  auto IsEmpty() const -> bool;
  // 向B+树中插入(key, value)对
  auto Insert(const KeyType &key, const ValueType &value) -> bool;
  // 删除指定key
  void Remove(const KeyType &key);
  // 获取指定key对应的value
  auto GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool;
  // 获取根节点
  auto GetRootPageId() -> page_id_t;
  auto Begin() -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;

  void Print(BufferPoolManager *bpm);
  void Draw(BufferPoolManager *bpm, const std::filesystem::path &outf);
  auto DrawBPlusTree() -> std::string;
  void InsertFromFile(const std::filesystem::path &file_name);
  void RemoveFromFile(const std::filesystem::path &file_name);
  void BatchOpsFromFile(const std::filesystem::path &file_name);

  void BTreeMetricsBegin() {
    split_cnt_ = 0;
    merge_cnt_ = 0;
    redistribute_cnt_ = 0;
    insert_cnt_ = 0;
    remove_cnt_ = 0;
  }

  void BTreeMetricsReport() {
    LOG_INFO("split:%ld", split_cnt_.load());
    LOG_INFO("merge:%ld", merge_cnt_.load());
    LOG_INFO("redistribute:%ld", redistribute_cnt_.load());
    LOG_INFO("insert:%ld", insert_cnt_.load());
    LOG_INFO("remove:%ld", remove_cnt_.load());
  }

  std::atomic<int64_t> split_cnt_;
  std::atomic<int64_t> merge_cnt_;
  std::atomic<int64_t> redistribute_cnt_;
  std::atomic<int64_t> insert_cnt_;
  std::atomic<int64_t> remove_cnt_;

 private:
  void ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out);
  void PrintTree(page_id_t page_id, const BPlusTreePage *page);
  auto ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree;

  // Help Function
  auto Getkey(const KeyType &key) -> int64_t;
  void GetLeafWritePageGuard(const KeyType &key, Context &ctx, BtreeAccessType access_type);
  auto GetLeafReadPageGuard(const KeyType &key) -> ReadPageGuard;
  // 递归插入newkv
  void RecInsert2Inner(Context &ctx, WritePageGuard &&left_guard, WritePageGuard &&right_guard, KeyType &key);
  // 处理节点数量过少的叶子节点
  void FixUnderflowLeaf(Context &ctx, LeafPage *leaf_page, WritePageGuard &&leafpage_guard, const KeyType &key);
  // 递归删除内部节点
  void RemoveInternalPage(Context &ctx, InternalPage *update_page, WritePageGuard &&update_pageguard, int idx);

  // member variable
  std::string index_name_;
  BufferPoolManager *bpm_;
  KeyComparator comparator_;
  std::vector<std::string> log;  // NOLINT
  int leaf_max_size_;
  int internal_max_size_;
  page_id_t header_page_id_;
};

/**
 * @brief for test only. PrintableBPlusTree is a printable B+ tree.
 * We first convert B+ tree into a printable B+ tree and the print it.
 */
struct PrintableBPlusTree {
  int size_;
  std::string keys_;
  std::vector<PrintableBPlusTree> children_;

  /**
   * @brief BFS traverse a printable B+ tree and print it into
   * into out_buf
   *
   * @param out_buf
   */
  void Print(std::ostream &out_buf) {
    std::vector<PrintableBPlusTree *> que = {this};
    while (!que.empty()) {
      std::vector<PrintableBPlusTree *> new_que;

      for (auto &t : que) {
        int padding = (t->size_ - t->keys_.size()) / 2;
        out_buf << std::string(padding, ' ');
        out_buf << t->keys_;
        out_buf << std::string(padding, ' ');

        for (auto &c : t->children_) {
          new_que.push_back(&c);
        }
      }
      out_buf << "\n";
      que = new_que;
    }
  }
};

}  // namespace bustub
