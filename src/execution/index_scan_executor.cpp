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
    : AbstractExecutor{exec_ctx}, plan_{plan}, pred_keys_at_{0} {
  // (TODO) 目测是优化器的锅, 不应该用indexScan
  if (plan_->pred_keys_.empty()) {
    InitIterator();
  }
}

void IndexScanExecutor::Init() {
  pred_keys_at_ = 0;
  if (plan_->pred_keys_.empty()) {
    InitIterator();
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (plan_->pred_keys_.empty()) {
    return NextScan(tuple, rid);
  }

  std::vector<RID> result{};
  auto &index_ = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)->index_;
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();

  while (pred_keys_at_ < plan_->pred_keys_.size()) {
    result.clear();
    auto &expr = plan_->pred_keys_[pred_keys_at_++];
    std::vector<Value> values(index_->GetMetadata()->GetIndexColumnCount());
    // 暂时只用一列作为key
    BUSTUB_ASSERT(index_->GetMetadata()->GetIndexColumnCount() == 1, "error IndexColumnCount");
    values[0] = expr->Evaluate(nullptr, *index_->GetKeySchema());
    index_->ScanKey({values, index_->GetKeySchema()}, &result, txn);

    if (!result.empty()) {
      auto &table_heap = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_;
      for (auto &base_rid : result) {
        auto [base_meta, base_tuple] = table_heap->GetTuple(base_rid);
        auto latest_tuple = GetReadableTuple(&GetOutputSchema(), base_tuple, base_meta, txn, txn_mgr);
        if (latest_tuple.has_value()) {
          *tuple = *latest_tuple;
          *rid = tuple->GetRid();
          return true;
        }
      }
    }
  }
  return false;
}

auto IndexScanExecutor::NextScan(Tuple *tuple, RID *rid) -> bool {
  while (!iter_.IsEnd()) {
    auto [_, x] = (*iter_);
    auto [base_meta, base_tuple] = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->GetTuple(x);
    // Get the readable version of the tuple
    auto latest_tuple = GetReadableTuple(&GetOutputSchema(), base_tuple, base_meta, exec_ctx_->GetTransaction(),
                                         exec_ctx_->GetTransactionManager());
    ++iter_;
    if (latest_tuple.has_value()) {
      *tuple = *latest_tuple;
      *rid = tuple->GetRid();
      return true;
    }
  }
  return false;
}

void IndexScanExecutor::InitIterator() {
  BUSTUB_ENSURE(plan_->pred_keys_.empty(), "Can't use iter!!!");
  auto index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  auto tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
  switch (index_info_->index_type_) {
    case IndexType::BPlusTreeIndex: {
      iter_ = tree_->GetBeginIterator();
      break;
    }
    case IndexType::HashTableIndex: {
      throw NotImplementedException("HashTableIndex is not implemented");
      break;
    }
    case IndexType::STLOrderedIndex: {
      throw NotImplementedException("STLOrderedIndex is not implemented");
      break;
    }
    case IndexType::STLUnorderedIndex: {
      throw NotImplementedException("STLUnorderedIndex is not implemented");
      break;
    }
    default: {
      throw ExecutionException("Unknown Index type");
    }
  }
}
}  // namespace bustub
