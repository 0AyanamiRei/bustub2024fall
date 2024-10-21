//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * 创建一个新的`internal page`之后的初始化函数:
 * 1. set page type
 * 2. set current size to zero
 * 3. set next page id
 * 4. set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IsFull() const -> bool { return GetSize() + 1 >= GetMaxSize(); }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  // int l = 0, r = GetSize() - 1, mid;
  // while(l < r) {
  //   mid = (l + r) >> 1;
  //   auto res = KeyComparator(array_[mid].second, value);
  //   if(res == 0) return mid;
  //   if(res == -1) l = mid + 1;
  //   if(res == 1) r = mid;
  // }
  return -1;
}

/** @brief 要求该节点不满 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert2Inner(const KeyType &key, const ValueType &value, KeyComparator &comparator)
    -> bool {
  int n = GetSize();
  int l = 1, r = n + 1, mid;
  while (l < r) {
    mid = l + (r - l) / 2;
    auto res = comparator(array_[mid].first, key);
    if (res == 1) {
      r = mid;
    }  // >
    else if (res == -1) {
      l = mid + 1;
    }  // <
    else {
      return false;
    }  // =
  }

  for (int i = GetSize() + 1; i > l; --i) {
    array_[i] = array_[i - 1];
  }

  array_[l] = {key, value};
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetValByKey(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  // 最左边的区间
  if (comparator(key, array_[1].first) == -1) {
    return array_[0].second;
  }
  // 二分寻找
  int l = 1, r = GetSize() + 1, mid;
  while (l < r) {
    mid = l + (r - l) / 2;
    auto res = comparator(array_[mid].first, key);
    if (res == 1) {
      r = mid;
    }  // >
    else {
      l = mid + 1;
    }  // <=
  }
  // K[l-1] <= key < K[l]
  return array_[l - 1].second;
}

// 返回的l满足: key[l]  <= key < key[l+1], 既key属于P[l]指向的leaf node
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetIndexByKey(const KeyType &key, const KeyComparator &comparator) const -> int {
  // key < array[1].key
  if (comparator(key, array_[1].first) == -1) {
    return 0;
  }

  int l = 1, r = GetSize(), mid;

  while (l < r) {
    mid = (l + r + 1) / 2;
    auto res = comparator(key, array_[mid].first);
    if (res == -1) {  // <
      r = mid - 1;
    } else {  // >=
      l = mid;
    }
  }
  // k[l] <= key < k[l+1]
  return l;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetValByIndex(const int idx) const -> ValueType {
  if(idx > GetSize()) {
    return INVALID_PAGE_ID;
  }
  return array_[idx].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetKVbyIndex(int index) const -> MappingType {
  if (index == -1) {
    return array_[GetSize()];
  }
  return array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetArray() -> MappingType * { return array_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKVbyIndex(const MappingType &newkv, int index) { array_[index] = newkv; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::push_back(const MappingType &kv) {
  array_[GetSize()] = kv;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::push_front(const MappingType &kv) {
  for (int i = GetSize(); i > 0; i--) {
    array_[i] = array_[i - 1];
  }
  array_[0] = kv;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::pop_front() -> MappingType {
  MappingType ret = array_[0];
  for (int i = 0, n = GetSize(); i < n; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::pop_back() -> MappingType {
  MappingType ret = array_[GetSize()];
  IncreaseSize(-1);
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert2InnerByIdx(const MappingType newkv, int index) {
  int n = GetSize();
  for (int i = index; i < n; i++) {
    array_[i + 1] = array_[i];
  }
  array_[index] = newkv;
  IncreaseSize(1);
}

/** @brief 一般用来设置第一个的val */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValByIndex(const ValueType &val, int index) {
  BUSTUB_ASSERT(index == 0, " 错误使用\n");
  array_[index].second = val;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Debug() {
  std::cout << " key:  ";
  for (int i = 1; i <= GetSize(); i++) {
    std::cout << getkey(array_[i].first) << " ";
  }
  std::cout << std::endl;
  std::cout << " val: ";
  for (int i = 0; i <= GetSize(); i++) {
    std::cout << array_[i].second << " ";
  }
  std::cout << std::endl;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::getkey(const KeyType &key) -> int64_t {
  int64_t kval;
  memcpy(&kval, key.data_, sizeof(int64_t));
  return kval;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteKVByIdx(int index) {
  int n = GetSize();
  for (int i = index; i < n; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

/** 内部节点的Size是按照指针的数量来算的 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SizeInvariantCheck(const int change) -> bool {
  return GetSize() + change + 1 >= GetMinSize();
}

/** 内部节点的分裂, this.array_ 就是left_guard保护的page内容 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SplitInner(WritePageGuard &right_guard, const MappingType &newkv,
                                                const KeyComparator &comparator) -> std::optional<KeyType> {
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *right_page = right_guard.AsMut<B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
  int max_size = GetMaxSize();
  int left_nums = (max_size + 1) / 2;
  this->SetSize(left_nums - 1);
  right_page->Init(max_size);

  // l=[1~max_size]
  int l = 1, r = max_size, mid;
  while (l < r) {
    mid = l + (r - l) / 2;
    auto res = comparator(array_[mid].first, newkv.first);
    if (res == 1) {
      r = mid;
    }  // >
    else if (res == -1) {
      l = mid + 1;
    }  // <
    else {
      return std::nullopt;
    }  // = 错误处理
  }


  MappingType *right_array = right_page->GetArray();
  (void) right_array;
  int sz = 0;

  if (left_nums <= l) {
    // 左节点不变化

    // 第一部分右节点
    for (int i = left_nums; i < l; i++) {
      right_array[sz++] = array_[i];
    }
    // 新插入kv
    right_array[sz++] = newkv;
    // 第二部分右节点
    for (int i = l; i < max_size; i++) {
      right_array[sz++] = array_[i];
    }
  } else {
    // 完整的copy右节点
    for (int i = left_nums - 1; i < max_size; i++) {
      right_array[sz++] = array_[i];
    }
    // 左节点等价于新插入kv
    for (int i = left_nums; i > l; i--) {
      array_[i] = array_[i-1];
    }
    array_[l] = newkv;
  }

  right_page->SetSize(sz-1);

  // 往上传的是正中间的key
  return right_page->KeyAt(0);
}

/** 根据找到key找到array_[idx], 如果存在, 返回其左兄弟节点的page_id */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SearchSiblingByKey(const KeyType &key, const KeyComparator &comparator) const
    -> ValueType {
  int l = 1, r = GetSize() + 1, mid;
  while (l < r) {
    mid = l + (r - l) / 2;
    auto res = comparator(array_[mid].first, key);
    if (res == 1) {
      r = mid;
    }  // >
    else if (res == -1) {
      l = mid + 1;
    }  // <
    else {
      break;
    }  // =
  }

  if (comparator(array_[mid].first, key) == 0) {
    return array_[mid - 1].second;
  }
  return INVALID_PAGE_ID;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub