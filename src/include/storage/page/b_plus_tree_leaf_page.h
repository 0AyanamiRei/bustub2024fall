//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2018-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <string>
#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE 16
#define LEAF_PAGE_SLOT_CNT ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (sizeof(KeyType) + sizeof(ValueType)))

/**
 * Store indexed key and record id (record id = page id combined with slot id,
 * see `include/common/rid.h` for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ---------
 * | HEADER |
 *  ---------
 *  ---------------------------------
 * | KEY(1) | KEY(2) | ... | KEY(n) |
 *  ---------------------------------
 *  ---------------------------------
 * | RID(1) | RID(2) | ... | RID(n) |
 *  ---------------------------------
 *
 *  Header format (size in byte, 16 bytes in total):
 *  -----------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |
 *  -----------------------------------------------
 *  -----------------
 * | NextPageId (4) |
 *  -----------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  BPlusTreeLeafPage() = delete;
  BPlusTreeLeafPage(const BPlusTreeLeafPage &other) = delete;

  void Init(int max_size = LEAF_PAGE_SLOT_CNT);

  // Helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyAt(int index) const -> KeyType;

  // My helper func
  auto IsFull() const -> bool;
  auto RemoveKey(const KeyType &key, const KeyComparator &comparator) -> bool;
  auto Insert2Leaf(const KeyType &key, const ValueType &rid, const KeyComparator &comparator) -> bool;  // NOLINT
  void SetKVByIndex(const MappingType &kv, int index);
  auto GetIdxByKey(const KeyType &key, const KeyComparator &comparator) const -> std::optional<int>;
  auto GetValByKey(const KeyType &key, const KeyComparator &comparator) const -> std::optional<RID>;
  auto GetKeyByIndex(int index) const -> KeyType;
  auto GetKVByIndex(int index) const -> const MappingType &;
  auto GetArray() -> MappingType *;

  void push_back(const MappingType &kv);   // NOLINT
  void push_front(const MappingType &kv);  // NOLINT
  auto pop_front() -> MappingType;         // NOLINT
  auto pop_back() -> MappingType;          // NOLINT

  auto SizeInvariantCheck(int change) -> bool;
  void MergeA2this(WritePageGuard &&A);
  auto SplitLeaf(WritePageGuard &right_guard, MappingType newkv, const KeyComparator &comparator)
      -> std::optional<KeyType>;

  // keys[idx] <= key < keys[idx+1]
  auto BinarySearch(const KeyType &key, const KeyComparator &comparator) -> int;

  auto Getkey(const KeyType &key) -> int64_t;
  void Debug();

  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    for (int i = 0; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");

    return kstr;
  }

 private:
  page_id_t next_page_id_;
  // Array members for page data.
  MappingType array_[LEAF_PAGE_SLOT_CNT];
  // (Fall 2024) Feel free to add more fields and helper functions below if needed
};

}  // namespace bustub
