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

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor{exec_ctx}, 
      plan_{plan},
      child_executor_{std::move(child_executor)},
      first_use_(true) {}

void DeleteExecutor::Init() {}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  int32_t cnt = 0;
  auto table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  // auto index_info_vec_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  while(child_executor_->Next(tuple, rid)) {
    table_info_->table_->UpdateTupleMeta({0, true}, *rid);
    cnt++;
  }
  *tuple = Tuple({{TypeId::INTEGER, cnt}}, &GetOutputSchema()); /**< 返回删除rows的数量 */
  if(first_use_) {
    first_use_ = false;
    return true;
  } else {
    return false;
  }
}

}  // namespace bustub
