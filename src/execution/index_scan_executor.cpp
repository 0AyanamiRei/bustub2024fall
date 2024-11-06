//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor{exec_ctx},
      plan_{plan}, 
      index_(exec_ctx->GetCatalog()->GetIndex(plan->index_oid_)->index_.get()) {}

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::vector<RID> result;
  for(auto &expr : plan_->pred_keys_) {
    std::vector<Value> values{};
    values.reserve(GetOutputSchema().GetColumnCount());
    values.push_back(expr->Evaluate(tuple, GetOutputSchema()));
    *tuple = Tuple{values, &GetOutputSchema()};
    index_->ScanKey(*tuple, &result, exec_ctx_->GetTransaction());
    
  }

}

}  // namespace bustub
