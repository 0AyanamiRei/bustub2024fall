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
#include "execution/expressions/constant_value_expression.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor{exec_ctx},
      plan_{plan} {}

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::vector<RID> result;
  std::vector<Value> values{};

  if(plan_->pred_keys_.empty()) {
    return false;
  }

  auto &expr = plan_->pred_keys_.back();
  auto &index_ = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)->index_;

  values.reserve(index_->GetMetadata()->GetIndexColumnCount()); /**< 构建key的列数量 (1) */
  BUSTUB_ASSERT(index_->GetMetadata()->GetIndexColumnCount() == 1, "error IndexColumnCount");
  values.push_back(expr->Evaluate(nullptr, *index_->GetKeySchema()));
  index_->ScanKey({values, index_->GetKeySchema()}, &result, exec_ctx_->GetTransaction());

  if(!result.empty()) { /**< 默认只有一个 */
    auto [meta, data] = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->GetTuple(result[0]);
    if(!meta.is_deleted_) {
      *tuple = data;
      *rid = result[0];
      auto non_const_plan = const_cast<IndexScanPlanNode *>(plan_);
      non_const_plan->pred_keys_.pop_back();
      return true;
    }
  }

  return false;
}

}  // namespace bustub
