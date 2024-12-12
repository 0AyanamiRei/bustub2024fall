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
      iter_(std::make_unique<TableIterator>(
          exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->MakeIterator())) {}

void SeqScanExecutor::Init() {
  iter_ = std::make_unique<TableIterator>(exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!iter_->IsEnd()) {
    // Get the readable version of the tuple
    auto latest_tuple = GetReadableTuple(&GetOutputSchema(), iter_->GetRID(), exec_ctx_->GetTransaction(),
                                         exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_.get(),
                                         exec_ctx_->GetTransactionManager());
    ++(*iter_.get());
    if (latest_tuple.has_value()) {
      // Otherwise, latest_tuple is std::nullopt means the tuple did not exist at this time
      *tuple = *latest_tuple;
      *rid = tuple->GetRid();
      if (plan_->filter_predicate_) {
        auto value = plan_->filter_predicate_->Evaluate(tuple, GetOutputSchema());
        if (value.IsNull() || !value.GetAs<bool>()) {
          // This tuple was filtered out
          continue;
        }
      }
      // Now return true
      return true;
    }
  }
  return false;
}

}  // namespace bustub
