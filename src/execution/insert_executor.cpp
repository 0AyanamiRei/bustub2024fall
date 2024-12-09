//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor{exec_ctx}, plan_{plan}, first_use_(true), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {}

// (TODO) 如果中途插入失败, 考虑回滚已插入行
// (FIXED) insert的子节点为空, 需要返回一个值为0的tuple 添加字段:`first_use_`
//         后来我们干脆实现为Insert算子一次性插入即可
auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  int32_t cnt = 0;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  auto index_info_vec = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  auto txn = exec_ctx_->GetTransaction();
  auto pkey_index_iter =
      std::find_if(index_info_vec.begin(), index_info_vec.end(),
                   [](const std::shared_ptr<bustub::IndexInfo> &index_info) { return index_info->is_primary_key_; });
  std::shared_ptr<bustub::IndexInfo> pkey_index_info =
      (pkey_index_iter != index_info_vec.end()) ? *pkey_index_iter : nullptr;
  auto &pkey_index = pkey_index_info->index_;

  while (child_executor_->Next(tuple, rid)) {
    // primary key check
    if (pkey_index_info != nullptr) {
      if (std::vector<bustub::RID> result{};
          pkey_index->ScanKey(
              tuple->KeyFromTuple(table_info->schema_, *pkey_index->GetKeySchema(), pkey_index->GetKeyAttrs()), &result,
              txn),
          !result.empty()) {
        txn->SetTainted();
        throw ExecutionException("Repeated insertion of the same primary key");
      }
    }

    // 插入tuple
    *rid = *table_info->table_->InsertTuple({txn->GetTransactionTempTs(), false}, *tuple, nullptr, nullptr,
                                            plan_->table_oid_);
    tuple->SetRid(*rid);

    // 更新mvcc相关ds
    txn->AppendWriteSet(table_info->oid_, *rid);

    // 修改相关Index
    for (auto &index_info : index_info_vec) {
      if (auto &index = index_info->index_;
          !index->InsertEntry(tuple->KeyFromTuple(table_info->schema_, *index->GetKeySchema(), index->GetKeyAttrs()),
                              tuple->GetRid(), exec_ctx_->GetTransaction()) &&
          index_info->is_primary_key_) {
        // Maybe useful, before throw exception
        {
          auto write_set = txn->GetWriteSets();
          for (auto &rid_dirty : write_set[table_info->oid_]) {
            table_info->table_->UpdateTupleMeta({txn->GetTransactionTempTs(), true}, rid_dirty);
          }
        }
        // 
        txn->SetTainted();
        throw ExecutionException("Repeated insertion of the same primary key");
      }
    }

    cnt++;
  }

  // 返回插入rows的数量
  *tuple = Tuple({{TypeId::INTEGER, cnt}}, &GetOutputSchema());

  // special case (TODO 更优雅的处理这个case)
  if (first_use_) {
    first_use_ = false;
    return true;
  } else {
    return false;
  }
}

}  // namespace bustub
