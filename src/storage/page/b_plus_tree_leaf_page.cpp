//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * 创建一个新的`leaf page`之后的初始化函数:
 * 1. set page type
 * 2. set current size to zero
 * 3. set next page id
 * 4. set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);  // 设置page类型为叶子节点
  SetMaxSize(max_size);                   // 设置最大的size
  SetSize(0);                             // 设置当前size为0
  SetNextPageId(INVALID_PAGE_ID);         // 设置next page id
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IsFull() const -> bool { return GetSize() >= GetMaxSize(); }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return this->next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { this->next_page_id_ = next_page_id; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

/** 已进行查重, 如果已存在则返回false */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert2Leaf(const KeyType &key, const ValueType &value,
                                             const KeyComparator &comparator) -> bool {
  // 二分找到应该插入的位置
  int pos = binarySearch(key, comparator);
  if(pos < GetSize() && comparator(array_[pos].first, key) == 0U) {
    return false;
  }
  // 将array_[l~]往后移一位
  for (int i = GetSize(); i > pos; --i) {
    array_[i] = array_[i - 1];
  }

  array_[pos] = {key, value};
  IncreaseSize(1);
  // LOG_INFO("插入array_[%d]叶子节点%ld", pos, getkey(key));
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKVByIndex(const MappingType &kv, const int index) { array_[index] = kv; }

/** 因为可能不存在,所以用std::optional<T>包一层*/
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetValByKey(const KeyType &key, const KeyComparator &comparator) const
    -> std::optional<RID> {
  int n = GetSize();
  int l = 0, r = n, mid;
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
      return array_[mid].second;
    }  // =
  }

  return std::nullopt;
}

/** 返回指定位置的key, -1表示队尾key */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetKeyByIndex(const int index) const -> KeyType {
  if (index == -1) {
    return array_[GetSize() - 1].first;
  }
  return array_[index].first;
}

/** 返回指定位置的kv, -1表示队尾kv */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetKVByIndex(int index) const -> const MappingType & {
  if (index == -1) {
    return this->array_[GetSize()];
  }
  return this->array_[index];
}

/** 仅仅是返回该key在array_中的下标, 暂时不会删除 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetIdxByKey(const KeyType &key, const KeyComparator &comparator) const
    -> std::optional<int> {
  int n = GetSize();
  int l = 0, r = n, mid;
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
  // 没有找到
  if (comparator(array_[mid].first, key)) {
    return std::nullopt;
  }
  return mid;
}

/** 返回array_数组, 以便整个移动数组, 此后该node会被销毁 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetArray() -> MappingType * { return this->array_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::push_front(const MappingType &kv) {
  for (int i = GetSize(); i > 0; --i) {
    array_[i] = array_[i - 1];
  }
  array_[0] = kv;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::push_back(const MappingType &kv) {
  array_[GetSize()] = kv;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::pop_back() -> MappingType {
  IncreaseSize(-1);
  return array_[GetSize()];  // GetSize()=old_size-1
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::pop_front() -> MappingType {
  MappingType ret = array_[0];
  auto l = GetSize();
  for (int i = 0; i < l - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
  return ret;
}

/** 叶子节点的分裂, this.array_ 就是left_guard保护的page内容 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SplitLeaf(WritePageGuard &right_guard, MappingType newkv,
                                           const KeyComparator &comparator) -> std::optional<KeyType> {
  B_PLUS_TREE_LEAF_PAGE_TYPE *right_page = right_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();

  int max_size = GetSize();
  int left_nums = (GetMaxSize() + 1) / 2;
  // pos=[0~max_size]
  int pos = binarySearch(newkv.first, comparator);
  if(pos < GetSize() && comparator(array_[pos].first, newkv.first) == 0U) { 
    return std::nullopt;
  }

  right_page->Init(GetMaxSize());
  MappingType *right_array = right_page->GetArray();
  int sz = 0;

  if (left_nums <= pos) {
    // 左节点不变化

    // 第一部分右节点
    for (int i = left_nums; i < pos; i++) {
      right_array[sz++] = array_[i];
    }
    // 新插入kv
    right_array[sz++] = newkv;
    // 第二部分右节点
    for (int i = pos; i < max_size; i++) {
      right_array[sz++] = array_[i];
    }
  } else {
    // 完整的copy右节点
    for (int i = left_nums - 1; i < max_size; i++) {
      right_array[sz++] = array_[i];
    }
    // 左节点等价于新插入kv
    for (int i = left_nums; i > pos; --i) {
      array_[i] = array_[i - 1];
    }
    array_[pos] = newkv;
  }

  this->SetSize(left_nums);
  right_page->SetSize(sz);
  right_page->SetNextPageId(this->GetNextPageId());
  this->SetNextPageId(right_guard.GetPageId());

  // 往上传的是右边第一个key
  return right_page->KeyAt(0);
}

/** 将A的array按次序添加到自己的队尾 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MergeA2this(WritePageGuard &A) {
  B_PLUS_TREE_LEAF_PAGE_TYPE *A_page = A.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  auto A_size = A_page->GetSize();
  MappingType *A_array = A_page->GetArray();
  SetNextPageId(A_page->GetNextPageId());
  /** 将A的array加入到自己队尾 */
  for (int i = 0; i < A_size; i++) {
    this->push_back(A_array[i]);
  }
}

/** 删除指定key */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveKey(const KeyType &key, const KeyComparator &comparator) -> bool {
  int n = GetSize();
  int l = 0, r = n, mid;
  while (l < r) {
    mid = l + (r - l) / 2;
    auto res = comparator(array_[mid].first, key);
    if (res == 1) {  // >
      r = mid;
    } else if (res == -1) {  // <
      l = mid + 1;
    } else {  // =
      break;
    }
  }
  if (comparator(array_[mid].first, key)) {  // 不存在
    return false;
  }
  for (int i = mid; i < n - 1; i++) {  // 删除指定key
    array_[i] = array_[i + 1];
  }
  this->IncreaseSize(-1);
  return true;
}

/** 返回节点状态, 是否处于underflow condition */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SizeInvariantCheck(const int change) -> bool {
  return GetSize() + change >= GetMinSize();
}


/** 在keys中寻找大于等于key的最小下标*/
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::binarySearch(const KeyType &key, const KeyComparator &comparator) -> int {
  int l = 0, r = GetSize(), mid;
  while (l < r) {
    mid = l + (r - l) / 2;
    auto res = comparator(array_[mid].first, key);
    if (res < 0) {
      l = mid + 1;
    }  // >
    else {
      r = mid;
    }  // <=
  }
  return l;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::getkey(const KeyType &key) -> int64_t {
  int64_t kval;
  memcpy(&kval, key.data_, sizeof(int64_t));
  return kval;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Debug() {
  std::cout << "key: ";
  for (int i = 0; i < GetSize(); i++) {
    std::cout << array_[i].second.GetSlotNum() << " ";
  }
  std::cout << "\n";
  std::cout << "val: ";
  for (int i = 0; i < GetSize(); i++) {
    std::cout << getkey(array_[i].first) << " ";
  }
  std::cout << "\n";
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub