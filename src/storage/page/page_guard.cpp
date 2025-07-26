//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// Identification: src/storage/page/page_guard.cpp
//
// Copyright (c) 2024-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"

namespace bustub {

/** @brief The only constructor for an RAII `ReadPageGuard` that creates a valid guard. */
ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch)
    : page_id_(page_id), frame_(std::move(frame)), replacer_(std::move(replacer)), bpm_latch_(std::move(bpm_latch)) {
  frame_->RLatch();
  is_valid_ = true;
}

/**
 * @brief The move constructor for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  if (frame_) { /** 避免掉因为RVO导致调用的移动构造 */
    Drop();
  }

  this->bpm_latch_ = that.bpm_latch_;
  this->frame_ = that.frame_;
  this->is_valid_ = that.is_valid_;
  this->page_id_ = that.page_id_;
  this->replacer_ = that.replacer_;

  that.is_valid_ = false;
}

/**
 * @brief The move assignment operator for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return ReadPageGuard& The newly valid `ReadPageGuard`.
 */
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (&that == this) {
    return *this;
  }

  if (frame_) { /** 避免掉因为RVO导致调用的移动构造 */
    Drop();
  }

  this->bpm_latch_ = that.bpm_latch_;
  this->frame_ = that.frame_;
  this->is_valid_ = that.is_valid_;
  this->page_id_ = that.page_id_;
  this->replacer_ = that.replacer_;

  that.is_valid_ = false;
  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->is_dirty_;
}

void ReadPageGuard::Drop() {
  if (!is_valid_) {
    return;
  }
  is_valid_ = false;
  frame_->RUnLatch();

  bpm_latch_->lock();
  frame_->pin_count_--;
  if (frame_->pin_count_ <= 0) {
    replacer_->SetEvictable(frame_->frame_id_, true);
  }
  bpm_latch_->unlock();
}

/** @brief The destructor for `ReadPageGuard`. This destructor simply calls `Drop()`. */
ReadPageGuard::~ReadPageGuard() { Drop(); }

void ReadPageGuard::RLatch() { this->frame_->RLatch(); }

void ReadPageGuard::RUnLatch() { this->frame_->RUnLatch(); }

/**********************************************************************************************************************/
/**********************************************************************************************************************/
/**********************************************************************************************************************/

/**
 * @brief The only constructor for an RAII `WritePageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to write to.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 */
WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch)
    : page_id_(page_id), frame_(std::move(frame)), replacer_(std::move(replacer)), bpm_latch_(std::move(bpm_latch)) {
  frame_->WLatch();
  frame_->is_dirty_ = true;
  is_valid_ = true;
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  if (frame_) { /** 避免掉因为RVO导致调用的移动构造 */
    Drop();
  }
  this->bpm_latch_ = that.bpm_latch_;
  this->frame_ = that.frame_;
  this->is_valid_ = that.is_valid_;
  this->page_id_ = that.page_id_;
  this->replacer_ = that.replacer_;
  that.is_valid_ = false;
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (&that == this) {
    return *this;
  }

  if (frame_) { /** 避免掉因为RVO导致调用的移动构造 */
    Drop();
  }
  this->bpm_latch_ = that.bpm_latch_;
  this->frame_ = that.frame_;
  this->is_valid_ = that.is_valid_;
  this->page_id_ = that.page_id_;
  this->replacer_ = that.replacer_;

  that.is_valid_ = false;
  return *this;
}

auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_;
}

auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetData();
}

auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetDataMut();
}

auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->is_dirty_;
}

void WritePageGuard::Drop() {
  if (!is_valid_) {
    return;
  }
  is_valid_ = false;
  frame_->WUnLatch();
  bpm_latch_->lock();
  frame_->pin_count_--;
  if (frame_->pin_count_ <= 0) {
    replacer_->SetEvictable(frame_->frame_id_, true);
  }
  bpm_latch_->unlock();
}

WritePageGuard::~WritePageGuard() { Drop(); }

void WritePageGuard::WLatch() { this->frame_->WLatch(); }

void WritePageGuard::WUnLatch() { this->frame_->WUnLatch(); }

}  // namespace bustub
