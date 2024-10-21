//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/index/generic_key.h"

namespace bustub {
enum class BtreeAccessType { Search, Insert, Delete };
enum class BtreeAccessType { Search, Insert, Delete };

#define MappingType std::pair<KeyType, ValueType>

#define INDEX_TEMPLATE_ARGUMENTS template <typename KeyType, typename ValueType, typename KeyComparator>

// define page type enum
enum class IndexPageType { INVALID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE };

/**
 * Both internal and leaf page are inherited from this page.
 *
 * It actually serves as a header part for each B+ tree page and
 * contains information shared by both leaf page and internal page.
 *
 * Header format (size in byte, 12 bytes in total):
 * ---------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |  ...   |
 * ---------------------------------------------------------
 */
class BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreePage() = delete;
  BPlusTreePage(const BPlusTreePage &other) = delete;
  ~BPlusTreePage() = delete;

  auto IsLeafPage() const -> bool;
  auto IsSafe(BtreeAccessType access_type) const -> bool;
  auto GetSize() const -> int;
  auto GetMaxSize() const -> int;
  auto GetMinSize() const -> int;

  void SetPageType(IndexPageType page_type);
  void SetSize(int size);
  void IncreaseSize(int amount);
  void IncreaseSize(int amount);
  void ChangeSizeBy(int amount);
  void SetMaxSize(int max_size);

 // private:
  // Member variables, attributes that both internal and leaf page share
  IndexPageType page_type_ __attribute__((__unused__));
  // Number of key & value pairs in a page
  int size_ __attribute__((__unused__));
  // Max number of key & value pairs in a page
  int max_size_ __attribute__((__unused__));
};

}  // namespace bustub
