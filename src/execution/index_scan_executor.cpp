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
      plan_{plan}, 
      pred_keys_at_{0} {
  if(plan_->pred_keys_.empty()) {
    InitIterator();
  }
}

void IndexScanExecutor::Init() {
  pred_keys_at_ = 0;
  if(plan_->pred_keys_.empty()) {
    InitIterator();
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::vector<RID> result;
  std::vector<Value> values{};

  if(plan_->pred_keys_.empty()) {
    return NextScan(tuple, rid);
  }

  while(pred_keys_at_ < plan_->pred_keys_.size()) {
    result.clear();
    values.clear();
    auto &expr = plan_->pred_keys_[pred_keys_at_++];
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
        // const_cast<IndexScanPlanNode *>(plan_)->pred_keys_.pop_back(); BAD Implement!!!
        return true;
      }
    }
  }
  return false;
}

auto IndexScanExecutor::NextScan(Tuple *tuple, RID *rid) -> bool {
  while(!iter_.IsEnd()) {
    auto [_, x] = (*iter_);
    auto [meta, data] = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->GetTuple(x);
    if(meta.is_deleted_) { /**< pass */
      ++iter_;
      continue;
    }
    *tuple = data;
    *rid = x;
    ++iter_;
    return true;
  }
  return false;
}

void IndexScanExecutor::InitIterator () {
  BUSTUB_ENSURE(plan_->pred_keys_.empty(), "Can't use iter!!!");
  auto index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  auto tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
  switch (index_info_->index_type_)
  {
    case IndexType::BPlusTreeIndex:
    {
      iter_ = tree_->GetBeginIterator();
      break;
    }
    case IndexType::HashTableIndex:
    {
      throw NotImplementedException("HashTableIndex is not implemented");
      break;
    }
    case IndexType::STLOrderedIndex:
    {
      throw NotImplementedException("STLOrderedIndex is not implemented");
      break;
    }
    case IndexType::STLUnorderedIndex:
    {
      throw NotImplementedException("STLUnorderedIndex is not implemented");
      break;
    }
    default:
    {
      throw ExecutionException("Unknown Index type");
    }
  }
}
}  // namespace bustub
