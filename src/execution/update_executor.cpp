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
      child_executor_{std::move(child_executor)} {
  Tuple tuple{};
  RID rid{};
  while (child_executor_->Next(&tuple, &rid)) {
    tuple.SetRid(rid);
    tuple_buffer.emplace_back(tuple);
  }
}

void UpdateExecutor::Init() {}

/** (FIX ?)update中的undo_log应该加入 */
auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuple_buffer.empty()) {
    return false;
  }

  Tuple base_tuple;
  auto index_info_vec = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  auto &schema = child_executor_->GetOutputSchema();
  auto pkey_index_iter = std::find_if(index_info_vec.begin(), index_info_vec.end(), IsPkeyIndex);
  std::shared_ptr<bustub::IndexInfo> pkey_index_info =
      (pkey_index_iter != index_info_vec.end()) ? *pkey_index_iter : nullptr;
  auto &pkey_index = pkey_index_info->index_;
  // If pkey_index_iter is not nullptr, pkey_index will be the Primary Key Index.

  bool is_update_pkey = false;
  {
    // 如何判断update字段是否包含Primary Key?
    if (pkey_index_info != nullptr) {
      for (auto attr : pkey_index->GetKeyAttrs()) {
        auto &expr_base = plan_->target_expressions_[attr];

        // 如果expr_base不是`ConstantValueExpression`类, 说明pkey的组成col需要更新
        // 即pkey需要更新
        if (const auto *expr = dynamic_cast<const ColumnValueExpression *>(expr_base.get()); expr == nullptr) {
          is_update_pkey = true;
          break;
        }
      }
    }
  }

  for (auto &base_tuple : tuple_buffer) {
    // Check conflict in update executor
    // @explain
    // Here, if CheckConflict_1() return true shows
    // the base_tuple was modified by other txn,
    // no matter it commits or not.
    if (CheckConflict_1(txn, table_info_->table_->GetTupleMeta(base_tuple.GetRid()).ts_)) {
      txn->SetTainted();
      throw ExecutionException("Write conflict during updating");
    }

    // if possible, delete (k,v) from primary key index
    if (is_update_pkey) {
      pkey_index->DeleteEntry(
          base_tuple.KeyFromTuple(table_info_->schema_, *pkey_index->GetKeySchema(), pkey_index->GetKeyAttrs()),
          base_tuple.GetRid(), txn);
    }
  }

  for (auto &base_tuple : tuple_buffer) {
    // Created a new tuple from base_tuple and exprs
    std::vector<Value> values{};
    values.reserve(schema.GetColumnCount());
    for (auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&base_tuple, schema));
    }
    *tuple = Tuple(values, &schema);
    tuple->SetRid(base_tuple.GetRid());

    // If possible, Insert new (k,v) primary key index
    if (is_update_pkey) {
      pkey_index->InsertEntry(
          tuple->KeyFromTuple(table_info_->schema_, *pkey_index->GetKeySchema(), pkey_index->GetKeyAttrs()),
          base_tuple.GetRid(), txn);
    }

    // Updated tuple & updated version chain
    auto write_set = txn->GetWriteSets();
    auto iter = write_set[plan_->table_oid_].find(base_tuple.GetRid());
    auto prev_link = txn_mgr->GetUndoLink(base_tuple.GetRid());
    if (iter == write_set[plan_->table_oid_].end()) {
      // Txn first update the tuple
      txn->AppendWriteSet(plan_->table_oid_, base_tuple.GetRid());
      auto undo_log =
          GenerateNewUndoLog(&schema, &base_tuple, tuple, table_info_->table_->GetTupleMeta(base_tuple.GetRid()).ts_, prev_link.has_value() ? *prev_link : UndoLink{});
      auto undo_link = txn->AppendUndoLog(undo_log);
      UpdateTupleAndUndoLink(txn_mgr, base_tuple.GetRid(), undo_link, table_info_->table_.get(), txn,
                             {txn->GetTransactionTempTs(), false}, *tuple);
    } else {
      // Txn isn't first write the tuple
      if (prev_link.has_value()) {
        auto undo_log = GenerateUpdatedUndoLog(&schema, &base_tuple, tuple, txn_mgr->GetUndoLog(*prev_link));
        txn->ModifyUndoLog(prev_link->prev_log_idx_, undo_log);
      }
      // The tuple inserted by self, not need to create undo_log
      table_info_->table_->UpdateTupleInPlace({txn->GetTransactionTempTs(), false}, *tuple, base_tuple.GetRid());
    }
  }

  // Now all tuple had been updated
  *tuple = Tuple({{TypeId::INTEGER, static_cast<int32_t>(tuple_buffer.size())}}, &GetOutputSchema());
  tuple_buffer.clear();
  return true;
}

}  // namespace bustub
