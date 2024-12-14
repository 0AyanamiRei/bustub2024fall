//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.h
//
// Identification: src/include/execution/executors/external_merge_sort_executor.h
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

// NOLINTBEGIN

#pragma once

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

/**
 * (TODO) 目前的局限是:
 * 1. 仅实现2-way merge sort
 * 2. 排序的数据也假设为定长的, 即不包含`VARCHAR` attributes
 */

namespace bustub {

#define SORT_PAGE_HEADER 16
#define VALUE_SIZE 24  // 不考虑变长, 那么一个Value的size就是24
#define RID_SIZE 8
#define SORT_ENTRY_SIZE(KEY_VAL_NUMS, FIX_SIZE) (KEY_VAL_NUMS * VALUE_SIZE + RID_SIZE + FIX_SIZE)
#define SORT_PAGE_CNT(KEY_VAL_NUMS, FIX_SIZE) \
  ((BUSTUB_PAGE_SIZE - SORT_PAGE_HEADER) / (KEY_VAL_NUMS * VALUE_SIZE + RID_SIZE + FIX_SIZE))

/**
 * Page to hold the intermediate data for external merge sort.
 *
 * Only fixed-length data will be supported in Fall 2024.
 *
 * sort page format :
 * +--------+----------------------+-----+----------------------+
 * | HEADER | SortKey(1), Tuple(1) | ... | SortKey(n), Tuple(n) |
 * +--------+----------------------+-----+----------------------+
 *
 * SortKey format:
 * +----------+----------+----------------+
 * | Value(1) | Value(2) | ... | Value(n) |
 * +----------+----------+----------------+
 *
 * Value format
 *     8      4       1       4      = 8 + 5(3) + 4(4) = 24bytes
 * +-------+-------+------+--------+
 * | union | union | bool | TypeId |
 * +-------+-------+------+--------+
 *
 * Tuple format:
 *     8         n
 * +-------+---------------+
 * |  RID  | fix_size data |
 * +-------+---------------+
 *
 */
class SortPage {
 public:
  SortPage() = default;
  void Init(uint32_t keys_val_nums, u_int32_t fix_size);
  auto IsFull() -> bool { return size_ >= max_size_; }
  auto IsEmpty() -> bool { return size_ == 0; }
  void Append(const SortEntry &sort_entry);
  auto GetSortEntry(uint32_t idx) const -> std::optional<SortEntry>;
  // void Clear() { std::fill(data_.begin(), data_.end(), 0); }
  void Clear() { std::memset(data_, 0, SORT_ENTRY_SIZE(keys_val_nums_, fix_size_) * max_size_); }
  auto GetMaxSize() const -> uint32_t { return max_size_; }
  auto GetSize() const -> uint32_t { return size_; }

 private:
  uint32_t size_;
  uint32_t max_size_;
  uint32_t keys_val_nums_;
  uint32_t fix_size_;
  char data_[];
  // C++ style std::vector<char> data_;
};

/**
 * A data structure that holds the sorted tuples as a run during external merge sort.
 * Tuples might be stored in multiple pages, and tuples are ordered both within one page
 * and across pages.
 *
 * (3,1) (6,2) (9,4) (8,7) (5,10)
 *
 * (1,3) (2,6) (4,9) (7,8) (5,10) 1-page页内有序
 *     (1,2)      (4,7)    (5,10)
 *     (3,6)      (8,9)           2-page跨页有序
 *
 *          (1,2)          (5,10) 4-page跨页有序
 *          (3,4)
 *          (6,7)
 *          (8,9)
 *
 *                 (1,2)          8-page跨页有序  4<=5<=8 排序完毕
 *                 (3,4)
 *                 (5,6)
 *                 (7,8)
 *                 (9,10)
 */
class MergeSortRun {
 public:
  MergeSortRun() = default;
  MergeSortRun(std::vector<page_id_t> pages, BufferPoolManager *bpm) : pages_(std::move(pages)), bpm_(bpm) {}
  MergeSortRun(MergeSortRun &&other) noexcept : pages_(std::move(other.pages_)), bpm_(other.bpm_) {
    other.bpm_ = nullptr;
  }
  MergeSortRun &operator=(MergeSortRun &&other) noexcept {
    if (this != &other) {
      pages_ = std::move(other.pages_);
      bpm_ = other.bpm_;
      other.bpm_ = nullptr;
    }
    return *this;
  }
  MergeSortRun(const MergeSortRun &) = delete;
  MergeSortRun &operator=(const MergeSortRun &) = delete;

  auto GetPageCount() -> size_t { return pages_.size(); }

  /** Iterator for iterating on the sorted tuples in one run. */
  class Iterator {
    friend class MergeSortRun;

   public:
    Iterator() = default;
    Iterator(uint32_t invalid, const MergeSortRun *run) : page_id_at_(invalid), cursor_(-1), run_(run){};
    Iterator(Iterator &&other) noexcept : page_(std::move(other.page_)) {
      page_id_at_ = other.page_id_at_;
      cursor_ = other.cursor_;
      run_ = other.run_;
      other.run_ = nullptr;
    }
    Iterator &operator=(Iterator &&other) noexcept {
      if (this != &other) {
        page_ = std::move(other.page_);
        page_id_at_ = other.page_id_at_;
        cursor_ = other.cursor_;
        run_ = other.run_;
        other.run_ = nullptr;
      }
      return *this;
    }

    void Init(const MergeSortRun *run);
    auto operator++() -> Iterator &;
    auto operator*() -> SortEntry;
    auto operator==(const Iterator &other) const -> bool;
    auto operator!=(const Iterator &other) const -> bool;

   private:
    explicit Iterator(const MergeSortRun *run);
    ReadPageGuard page_;
    uint32_t page_id_at_;
    uint32_t cursor_;
    [[maybe_unused]] const MergeSortRun *run_;
  };

  /**< Get an iterator pointing to the beginning of the sorted run, i.e. the first tuple. */
  auto Begin() -> Iterator { return Iterator(this); }

  /**< Get an iterator pointing to the end of the sorted run, i.e. the position after the last tuple. */
  auto End() -> Iterator { return {static_cast<uint32_t>(pages_.size()), this}; }

  Iterator iter_;

 private:
  std::vector<page_id_t> pages_;
  [[maybe_unused]] BufferPoolManager *bpm_;
};

/**
 * ExternalMergeSortExecutor executes an external merge sort.
 *
 * In Fall 2024, only 2-way external merge sort is required.
 */
template <size_t K>
class ExternalMergeSortExecutor : public AbstractExecutor {
 public:
  ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the external merge sort */
  void Init() override;

  /**
   * Yield the next tuple from the external merge sort.
   * @param[out] tuple The next tuple produced by the external merge sort.
   * @param[out] rid The next tuple RID produced by the external merge sort.
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the external merge sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** Help Functions */
  auto CreateSortedRuns() -> std::vector<page_id_t>;
  auto MergeRuns(MergeSortRun &a, MergeSortRun &b) -> std::vector<page_id_t>;

  const SortPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  TupleComparator cmp_;  // 根据order_bys的内容来比较两个tuples
  page_id_t sort_page_id_;
  MergeSortRun runs_;
};

}  // namespace bustub

// NOLINTEND