//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor{exec_ctx},
      plan_{plan},
      iter_(std::make_unique<TableIterator>(exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->MakeIterator())) {}

void SeqScanExecutor::Init() {
  iter_ = std::make_unique<TableIterator>(exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while(!iter_->IsEnd()) {
    auto [meta, data] = iter_->GetTuple();
    /**< 跳过 */
    if(meta.is_deleted_) {
      ++(*iter_.get());
      continue;
    }
    *tuple = data;
    *rid = iter_->GetRID();
    /**< 应用谓词 */
    if(plan_->filter_predicate_) {
      auto value = plan_->filter_predicate_->Evaluate(tuple, GetOutputSchema());
      if (value.IsNull() || !value.GetAs<bool>()) {
        ++(*iter_.get());
        continue;
      }
    }
    /**< 返回 */
    ++(*iter_.get());
    return true;
  }
  return false;
}

}  // namespace bustub
