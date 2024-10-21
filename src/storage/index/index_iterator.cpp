/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(ReadPageGuard &&leaf_guard, int idx, BufferPoolManager *bpm)
    : leaf_guard_(std::move(leaf_guard)), idx_(idx), bpm_(bpm) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  auto leaf_page = leaf_guard_.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  return leaf_page->GetNextPageId() == INVALID_PAGE_ID && idx_ >= leaf_page->GetSize() + 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  const B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = leaf_guard_.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  BUSTUB_ASSERT(idx_ < leaf_page->GetSize(), "超出上限索引");
  return leaf_page->GetKVByIndex(idx_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  idx_++;
  auto leaf_page = leaf_guard_.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  if (idx_ >= leaf_page->GetSize()) {
    if (leaf_page->GetNextPageId() == INVALID_PAGE_ID) {
      idx_ = leaf_page->GetSize() + 1;
    } else {
      leaf_guard_ = bpm_->ReadPage(leaf_page->GetNextPageId());
      idx_ = 0;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub