//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include <iostream>
#include <optional>
#include <vector>
#include "common/config.h"
#include "execution/plans/sort_plan.h"

namespace bustub {
using std::cout, std::endl;

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan), 
      child_executor_(std::move(child_executor)),
      cmp_(plan->GetOrderBy()) {
  std::vector<MergeSortRun> runs;
  auto bpm = exec_ctx_->GetBufferPoolManager();
  auto sorted_runs = std::move(CreateSortedRuns());
  for(auto &page_id : sorted_runs) {
    runs.emplace_back(MergeSortRun({page_id}, bpm));
  }
  /**< 现已得到M个排好序的run, 下一步该k-way merge (k=2) */
  size_t order_page_nums = 1;
  while(order_page_nums <= sorted_runs.size()) {
    std::vector<MergeSortRun> runs2;
    size_t i;
    for(i = 0; i + 1 < runs.size(); ) {
      runs2.emplace_back(MergeSortRun({MergeRuns(runs[i], runs[i+1]), bpm}));
      i+=2;
    }
    if(i < runs.size()) {
      runs2.emplace_back(std::move(runs[i]));
    }
    runs = std::move(runs2);
    order_page_nums *= 2;
  }
  
  // now we got a ordered
  if(runs.size() == 1) {
    runs_ = std::move(runs[0]);
    runs_.iter_ = runs_.Begin();
  } else {
    runs_.iter_ = runs_.End();
  }
}

template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  if(runs_.GetPageCount()) {
    runs_.iter_.Init(&runs_);
  }
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(Tuple *tuple, RID *rid) -> bool {
  while(runs_.iter_ != runs_.End()) {
    *tuple = (*runs_.iter_).second;
    ++runs_.iter_;
    return true;
  }
  return false;
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::CreateSortedRuns()
    -> std::vector<page_id_t> {
  std::vector<page_id_t> page_ids;
  std::vector<SortEntry> sort_entries;
  Tuple tuple{};
  RID rid{};
  auto sort_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
  auto sort_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(sort_page_id);
  auto *sort_page = sort_page_guard.AsMut<SortPage>();
  sort_page->Init(plan_->order_bys_.size(), child_executor_->GetOutputSchema().GetInlinedStorageSize());

  while(child_executor_->Next(&tuple, &rid)) {
    /**< make a `RUN` */
    if(sort_entries.size() == sort_page->GetMaxSize()) {
      std::sort(sort_entries.begin(), sort_entries.end(), cmp_);
      for(const auto &entry : sort_entries) {
        sort_page->Append(entry);
      }
      page_ids.push_back(sort_page_id);
      sort_entries.clear();
      sort_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
      sort_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(sort_page_id);
      sort_page = sort_page_guard.AsMut<SortPage>();
      sort_page->Init(plan_->order_bys_.size(), child_executor_->GetOutputSchema().GetInlinedStorageSize());
    }
    auto sort_entry = std::make_pair(GenerateSortKey(tuple, plan_->order_bys_, child_executor_->GetOutputSchema()), tuple);
    sort_entries.push_back(sort_entry);
  }
  if(!sort_entries.empty()) {
    std::sort(sort_entries.begin(), sort_entries.end(), cmp_);
    for(const auto &entry : sort_entries) {
      sort_page->Append(entry);
    }
    page_ids.push_back(sort_page_id);
  }
  return page_ids;
}

/**
 * a,b两个runs内部页有序, 跨页无序, 现在合并成跨页有序的page_ids
*/
template <size_t K>
auto ExternalMergeSortExecutor<K>::MergeRuns(MergeSortRun &a, MergeSortRun &b) -> std::vector<page_id_t> {
  std::vector<page_id_t> page_ids;
  auto output_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
  auto output_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(output_page_id);
  auto *output_page = output_page_guard.AsMut<SortPage>();
  output_page->Init(plan_->order_bys_.size(), child_executor_->GetOutputSchema().GetInlinedStorageSize());
  // now you got a SortPage
  auto iterA = a.Begin(); // 假设A有36个
  auto iterB = b.Begin(); // 假设B有20个
  // and now you got two Iterator to travel a,b runs
  while(iterA != a.End() && iterB != b.End()) {
    auto entryA = *iterA;
    auto entryB = *iterB;
    if(cmp_(entryA, entryB)) {
      output_page->Append(entryA);
      ++iterA;
    } else {
      output_page->Append(entryB);
      ++iterB;
    }
    
    if(output_page->IsFull()) {
      page_ids.push_back(output_page_id);
      output_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
      output_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(output_page_id);
      output_page = output_page_guard.AsMut<SortPage>();
      output_page->Init(plan_->order_bys_.size(), child_executor_->GetOutputSchema().GetInlinedStorageSize());
    }
  }

  // add left sort entry to output page
  while(iterA != a.End()) { // 现在是B读了20个,A读了3个, A剩下33个, 这里就溢出了
    output_page->Append(*iterA);
    ++iterA;
    if(output_page->IsFull()) {
      page_ids.push_back(output_page_id);
      output_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
      output_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(output_page_id);
      output_page = output_page_guard.AsMut<SortPage>();
      output_page->Init(plan_->order_bys_.size(), child_executor_->GetOutputSchema().GetInlinedStorageSize());
    }
  }
  while(iterB != b.End()) {
    output_page->Append(*iterB);
    ++iterB;
    if(output_page->IsFull()) {
      page_ids.push_back(output_page_id);
      output_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
      output_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(output_page_id);
      output_page = output_page_guard.AsMut<SortPage>();
      output_page->Init(plan_->order_bys_.size(), child_executor_->GetOutputSchema().GetInlinedStorageSize());
    }
  }

  if(!output_page->IsEmpty()) {
    page_ids.push_back(output_page_id);
  }
  return page_ids;
}

void SortPage::Init(uint32_t keys_val_nums, u_int32_t fix_size) {
  size_ = 0;
  max_size_ = SORT_PAGE_CNT(keys_val_nums, fix_size);
  keys_val_nums_ = keys_val_nums;
  fix_size_ = fix_size;
  // data_.resize(SORT_ENTRY_SIZE(keys_val_nums, fix_size) * max_size_);
}

void SortPage::Append(const SortEntry &sort_entry) {
  if(IsFull()) {
    LOG_INFO("if full");
    throw Exception("is full");
  }
  /**< 偏移量 */
  size_t offset = (size_++) * SORT_ENTRY_SIZE(keys_val_nums_, fix_size_);
  /**< copy SortKey中所有的Value */
  for(const auto &value : sort_entry.first) {
    std::memcpy(&this->data_[offset], &value, VALUE_SIZE); // WRITE of size 24 at 0x6210000aa100
    offset += VALUE_SIZE;
  }
  /**< copy Tuple::rid_ */
  RID rid{sort_entry.second.GetRid()};
  std::memcpy(&data_[offset], &rid, RID_SIZE);
  offset += RID_SIZE;
  /**< copy Tuple::data_ */
  std::memcpy(&data_[offset], sort_entry.second.GetData(), fix_size_);
  offset += fix_size_;
}

auto SortPage::GetSortEntry(uint32_t idx) const -> std::optional<SortEntry> {
  if(idx >= size_) {
    return std::nullopt;
  }
  SortKey keys;
  size_t offset = idx * SORT_ENTRY_SIZE(keys_val_nums_, fix_size_);
  /**< 提取SortKey */
  for(size_t i = 0; i < keys_val_nums_; ++i) {
    Value key;
    std::memcpy(&key, &data_[offset], VALUE_SIZE);
    keys.emplace_back(std::move(key));
    offset += VALUE_SIZE;
  }
  /**< 提取Tuple::rid_ */
  RID rid;
  std::memcpy(&rid, &data_[offset], RID_SIZE);
  offset += RID_SIZE;
  /**< 提取Tuple::data_ */
  Tuple tuple(rid, &data_[offset], fix_size_);

  return std::optional<SortEntry>({std::move(keys), std::move(tuple)});
}

MergeSortRun::Iterator::Iterator(const MergeSortRun *run)
          : page_{run->bpm_->ReadPage(run->pages_[0])}, 
            page_id_at_(0),
            cursor_(0),
            run_(run) {
}

void MergeSortRun::Iterator::Init(const MergeSortRun *run) {
  page_id_at_ = 0;
  cursor_ = 0;
  cursor_ = 0;
}

auto MergeSortRun::Iterator::operator++() -> Iterator & {
  const auto *sort_page = page_.As<SortPage>();
  cursor_++;
  if(cursor_ >= sort_page->GetSize()) {
    // 需要换页了
    page_id_at_++;
    if(page_id_at_ < run_->pages_.size()) {
      page_ = run_->bpm_->ReadPage(run_->pages_[page_id_at_]);
      cursor_ = 0;
    } else {
      cursor_ = -1;
      // LOG_INFO("END");
    }
  }
  return *this;
}

auto MergeSortRun::Iterator::operator*() -> SortEntry {
  const auto *sort_page = page_.As<SortPage>();
  auto sort_entry = sort_page->GetSortEntry(cursor_);
  if(sort_entry.has_value()) {
    return sort_entry.value();
  } else {
    throw Exception("get SortEntry failed.");
  }
}

auto MergeSortRun::Iterator::operator==(const Iterator &other) const -> bool {
  return (other.run_ == run_) && (other.page_id_at_ == page_id_at_) && (other.cursor_ == cursor_);
}

auto MergeSortRun::Iterator::operator!=(const Iterator &other) const -> bool {
  return (other.cursor_ != cursor_) || (other.page_id_at_ != page_id_at_) || (other.run_ != run_);
}

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub
