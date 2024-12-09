//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor{exec_ctx},
      plan_{plan},
      table_info_{exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_).get()},
      child_executor_{std::move(child_executor)},
      first_use_{true} {}

void UpdateExecutor::Init() {
  // throw NotImplementedException("Update Node没有父节点, 不需要往下重启算子");
}

/** (FIX ?)update中的undo_log应该加入 */
auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  int32_t cnt = 0;
  Tuple base_tuple;
  auto index_info_vec_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  auto &schema = child_executor_->GetOutputSchema();

  while (child_executor_->Next(&base_tuple, rid)) {
    // Check if needed to be aborted
    auto base_ts = table_info_->table_->GetTupleMeta(base_tuple.GetRid()).ts_;
    if (IsConflict(txn, base_ts)) {
      txn->SetTainted();
      throw ExecutionException("Write conflict during updating");
    }

    // delete (k,v) from index
    for (auto &index_info_ : index_info_vec_) {
      auto &index_ = index_info_->index_;
      index_->DeleteEntry(base_tuple.KeyFromTuple(table_info_->schema_, *index_->GetKeySchema(), index_->GetKeyAttrs()),
                          *rid, txn);
    }

    // Created a new tuple from base_tuple and exprs
    std::vector<Value> values{};
    values.reserve(schema.GetColumnCount());
    for (auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&base_tuple, schema));
    }
    *tuple = Tuple(values, &schema);
    tuple->SetRid(*rid);

    // Insert new (k,v) into index
    for (auto &index_info_ : index_info_vec_) {
      auto &index_ = index_info_->index_;
      index_->InsertEntry(tuple->KeyFromTuple(table_info_->schema_, *index_->GetKeySchema(), index_->GetKeyAttrs()),
                          *rid, txn);
    }

    // Updated tuple & updated version chain
    auto write_set = txn->GetWriteSets();
    auto iter = write_set[plan_->table_oid_].find(*rid);
    auto prev_link = txn_mgr->GetUndoLink(*rid);
    if (iter == write_set[plan_->table_oid_].end()) {
      // Txn first update the tuple
      txn->AppendWriteSet(plan_->table_oid_, *rid);
      auto undo_log =
          GenerateNewUndoLog(&schema, &base_tuple, tuple, base_ts, prev_link.has_value() ? *prev_link : UndoLink{});
      auto undo_link = txn->AppendUndoLog(undo_log);
      UpdateTupleAndUndoLink(txn_mgr, *rid, undo_link, table_info_->table_.get(), txn,
                             {txn->GetTransactionTempTs(), false}, *tuple);
    } else {
      // Txn isn't first write the tuple
      if (prev_link.has_value()) {
        auto undo_log = GenerateUpdatedUndoLog(&schema, &base_tuple, tuple, txn_mgr->GetUndoLog(*prev_link));
        txn->ModifyUndoLog(prev_link->prev_log_idx_, undo_log);
      }
      // The tuple inserted by self, not need to create undo_log
      table_info_->table_->UpdateTupleInPlace({txn->GetTransactionTempTs(), false}, *tuple, *rid);
    }

    // count
    cnt++;
  }

  // Now all tuple had been updated
  *tuple = Tuple({{TypeId::INTEGER, cnt}}, &GetOutputSchema());

  // travel handle
  if (first_use_) {
    first_use_ = false;
    return true;
  } else {
    return false;
  }
}

}  // namespace bustub
