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

  //! Check if we are updating the Primary Key.
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

  if (is_update_pkey) {
    //! First, delete all the tuple
    for (auto &old_tuple : tuple_buffer) {
      //! The tuples in the tuple_buffer maybe out of data
      //! what's more, we need the undo_link, too, so...
      auto [base_meta, base_tuple, prev_link] =
          GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), old_tuple.GetRid());
      //! Check conflict, the tuple maybe modified by other txn
      //! (TODO) 如果从某处开始, 该txn标记为TAINTED然后抛出异常, 那么table_heap中
      //! 会永久存在持有temp-ts标记的tuple, 导致后面的txn修改它们时也检测到冲突
      //! 所以需要考虑回退前面修改过的内容, 而即使有了回滚的行为, 它也不是原子性的
      //! 仍然会出现上述情况导致其他txn误认为发生冲突, 从而导致更多级联冲突
      if (CheckConflict_1(txn, base_meta.ts_)) {
        txn->SetTainted();
        throw ExecutionException("Write conflict during updating");
      }

      //! Delete and tag it with temp ts from the table_heap
      //! but won't change the index.
      auto write_set = txn->GetWriteSets();
      auto iter = write_set[plan_->table_oid_].find(old_tuple.GetRid());
      if (iter == write_set[plan_->table_oid_].end()) {
        //! Txn first update the tuple
        txn->AppendWriteSet(plan_->table_oid_, old_tuple.GetRid());
        auto undo_log =
            UndoLog{false, std::vector<bool>(schema.GetColumnCount(), true), base_tuple, base_meta.ts_, *prev_link};
        auto undo_link = txn->AppendUndoLog(undo_log);
        if (!UpdateTupleAndUndoLink(txn_mgr, old_tuple.GetRid(), undo_link, table_info_->table_.get(), txn,
                                    {txn->GetTransactionTempTs(), true}, base_tuple, CheckConflict_2)) {
          txn->SetTainted();
          throw ExecutionException("Write conflict during updating");
        }
      } else {
        //! Txn has modified the tuple by self, the undo_log we don't need to modify.
        //! What's more, the tuple has also been tagged, below is concurrently safe.
        table_info_->table_->UpdateTupleMeta({txn->GetTransactionTempTs(), true}, old_tuple.GetRid());
      }
    }

    //! Second, insert new tuple
    for (auto &base_tuple : tuple_buffer) {
      //! Created a new tuple from base_tuple and exprs
      {
        std::vector<Value> values{};
        values.reserve(schema.GetColumnCount());
        for (auto &expr : plan_->target_expressions_) {
          values.push_back(expr->Evaluate(&base_tuple, schema));
        }
        *tuple = Tuple(values, &schema);
        tuple->SetRid(base_tuple.GetRid());
      }

      //! If update the pkey, now all tuple will be updated are
      //! tagged with txn's temp ts, so, the following areas are concurrently safe
      std::vector<RID> result{};
      pkey_index->ScanKey(
          tuple->KeyFromTuple(table_info_->schema_, *pkey_index->GetKeySchema(), pkey_index->GetKeyAttrs()), &result,
          txn);
      if (result.empty()) {
        //! It shows that we need to create a new tuple and insert it.
        *rid = *table_info_->table_->InsertTuple({txn->GetTransactionTempTs(), false}, *tuple, nullptr, nullptr,
                                                 plan_->table_oid_);
        txn->AppendWriteSet(table_info_->oid_, *rid);
        pkey_index->InsertEntry(
            tuple->KeyFromTuple(table_info_->schema_, *pkey_index->GetKeySchema(), pkey_index->GetKeyAttrs()), *rid,
            txn);
      } else {
        //! Otherwise, we just need to update the tuple in the heap,
        //! Carefully, we need to update the rigth place with the result[0].
        tuple->SetRid(result[0]);
        table_info_->table_->UpdateTupleInPlace({txn->GetTransactionTempTs(), false}, *tuple, tuple->GetRid());
      }
    }
  } else {
    for (auto &old_tuple : tuple_buffer) {
      auto [base_meta, base_tuple, prev_link] =
          GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), old_tuple.GetRid());
      if (CheckConflict_1(txn, base_meta.ts_)) {
        txn->SetTainted();
        throw ExecutionException("Write conflict during updating");
      }

      //! Created a new tuple from base_tuple and exprs
      {
        std::vector<Value> values{};
        values.reserve(schema.GetColumnCount());
        for (auto &expr : plan_->target_expressions_) {
          values.push_back(expr->Evaluate(&base_tuple, schema));
        }
        *tuple = Tuple(values, &schema);
        tuple->SetRid(old_tuple.GetRid());
      }

      // Updated tuple & updated version chain
      auto write_set = txn->GetWriteSets();
      auto iter = write_set[plan_->table_oid_].find(tuple->GetRid());
      if (iter == write_set[plan_->table_oid_].end()) {
        // Txn first update the tuple
        txn->AppendWriteSet(plan_->table_oid_, tuple->GetRid());
        auto undo_log = GenerateNewUndoLog(&schema, &base_tuple, tuple, base_meta.ts_, *prev_link);
        auto undo_link = txn->AppendUndoLog(undo_log);
        if (!UpdateTupleAndUndoLink(txn_mgr, tuple->GetRid(), undo_link, table_info_->table_.get(), txn,
                                    {txn->GetTransactionTempTs(), false}, *tuple, CheckConflict_2)) {
          txn->SetTainted();
          throw ExecutionException("Write conflict during updating");
        }
      } else {
        // Txn isn't first write the tuple
        if (prev_link.has_value()) {
          auto undo_log = GenerateUpdatedUndoLog(&schema, &base_tuple, tuple, txn_mgr->GetUndoLog(*prev_link));
          txn->ModifyUndoLog(prev_link->prev_log_idx_, undo_log);
        }
        // The tuple inserted by self, not need to create undo_log
        table_info_->table_->UpdateTupleInPlace({txn->GetTransactionTempTs(), false}, *tuple, tuple->GetRid());
      }
    }
  }

  // Now all tuple had been updated
  *tuple = Tuple({{TypeId::INTEGER, static_cast<int32_t>(tuple_buffer.size())}}, &GetOutputSchema());
  tuple_buffer.clear();
  return true;
}

}  // namespace bustub
