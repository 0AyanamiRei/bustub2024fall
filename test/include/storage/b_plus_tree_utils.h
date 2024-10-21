//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_utils.h
//
// Identification: test/include/storage/b_plus_tree_utils.h
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

using std::cout;
using std::endl;

template <typename KeyType, typename KeyValue, typename KeyComparator>
bool TreeValuesMatch(BPlusTree<KeyType, KeyValue, KeyComparator> &tree, std::vector<int64_t> &inserted,
                     std::vector<int64_t> &deleted) {
  std::vector<KeyValue> rids;
  KeyType index_key;
  for (auto &key : inserted) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    if (rids.size() != 1) {
      cout << key << "没找到" << endl;
      return false;
    }
  }
  for (auto &key : deleted) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    if (!rids.empty()) {
      cout << key << "不应该找到" << endl;
      return false;
    }
  }
  return true;
}

}  // namespace bustub
