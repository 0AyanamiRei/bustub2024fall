//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

// NOLINTBEGIN

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor{exec_ctx}, plan_{plan}, child_executor_{std::move(child_executor)}, first_use_(true) {}

void DeleteExecutor::Init() {}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  int32_t cnt = 0;
  Tuple base_tuple;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  auto index_info_vec = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  auto &schema = child_executor_->GetOutputSchema();

  while (child_executor_->Next(&base_tuple, rid)) {
    // Check conflict in update executor
    // @explain
    // Here, if CheckConflict1() return true shows
    // the base_tuple was deleted by other txn,
    // no matter it commits or not.
    auto base_ts = table_info->table_->GetTupleMeta(base_tuple.GetRid()).ts_;
    if (CheckConflict1(txn, base_ts)) {
      txn->SetTainted();
      throw ExecutionException("Write Conflict1 during deleting");
    }

    // Updated tuple & updated version chain
    auto write_set = txn->GetWriteSets();
    auto iter = write_set[plan_->table_oid_].find(*rid);
    auto prev_link = txn_mgr->GetUndoLink(*rid);
    if (iter == write_set[plan_->table_oid_].end()) {
      // Txn first update the tuple
      txn->AppendWriteSet(plan_->table_oid_, *rid);
      auto undo_log =
          GenerateNewUndoLog(&schema, &base_tuple, nullptr, base_ts, prev_link.has_value() ? *prev_link : UndoLink{});
      auto undo_link = txn->AppendUndoLog(undo_log);
      // here pass the base_tuple just ok, we just need update the metada.is_deleted_ to true
      // auto boundCheck = std::bind(&TransactionManager::Check, txn_mgr, std::placeholders::_1, std::placeholders::_2,
      //                             std::placeholders::_3, std::placeholders::_4);
      UpdateTupleAndUndoLink(txn_mgr, *rid, undo_link, table_info->table_.get(), txn,
                             {txn->GetTransactionTempTs(), true}, base_tuple);
    } else {
      // Txn delete the tuple inserted by self
      table_info->table_->UpdateTupleMeta({txn->GetTransactionTempTs(), true}, *rid);
    }
    cnt++;
  }
  *tuple = Tuple({{TypeId::INTEGER, cnt}}, &GetOutputSchema());
  if (first_use_) {
    first_use_ = false;
    return true;
  } else {
    return false;
  }
}

}  // namespace bustub

// NOLINTEND