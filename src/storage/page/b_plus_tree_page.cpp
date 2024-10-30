//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

auto BPlusTreePage::IsLeafPage() const -> bool { return this->page_type_ == IndexPageType::LEAF_PAGE; }
auto BPlusTreePage::IsSafe(BtreeAccessType access_type) const -> bool {
  if (access_type == BtreeAccessType::Delete) {
    /** 是否允许删一个 */
    return size_ - 1 + (page_type_ == IndexPageType::INTERNAL_PAGE) >= (max_size_ + 1) / 2;  // NOLINT
  } else if (access_type == BtreeAccessType::Insert) {                                       // NOLINT
    /** 是否允许插入一个 */
    return size_ + (page_type_ == IndexPageType::INTERNAL_PAGE) < max_size_;  // NOLINT
  } else {
    return false;
  }
}
void BPlusTreePage::SetPageType(IndexPageType page_type) { this->page_type_ = page_type; }

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
auto BPlusTreePage::GetSize() const -> int { return this->size_; }
void BPlusTreePage::SetSize(int size) { this->size_ = size; }
void BPlusTreePage::IncreaseSize(int amount) {
  if (amount + size_ > max_size_) {
    LOG_DEBUG("%d+%d <= %d 错误", amount, size_, max_size_);
  }
  BUSTUB_ASSERT(amount + size_ <= max_size_, "PAGE: 超过最大数量\n");
  this->size_ += amount;
}
void BPlusTreePage::ChangeSizeBy(int amount) {
  if (amount + size_ > max_size_) {
    LOG_DEBUG("%d+%d <= %d 错误", amount, size_, max_size_);
  }
  BUSTUB_ASSERT(amount + size_ <= max_size_, "PAGE: 超过最大数量\n");
  this->size_ += amount;
}

/*
 * Helper methods to get/set max size (capacity) of the page
 */
auto BPlusTreePage::GetMaxSize() const -> int { return max_size_; }
void BPlusTreePage::SetMaxSize(int size) { max_size_ = size; }
auto BPlusTreePage::GetMinSize() const -> int { return (max_size_ + 1) / 2; }
}  // namespace bustub
