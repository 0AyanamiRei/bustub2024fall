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
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_).get()),
      child_executor_{std::move(child_executor)},
      first_use_(true) {}

void UpdateExecutor::Init() {}

// 同样需要返回更新条数
// 删除tuple->本地修改tuple->插入tuple
auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  int32_t cnt = 0;
  //  auto table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  auto index_info_vec_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  while(child_executor_->Next(tuple, rid)) {
    /**< 逻辑删除 (TODO) 考虑物理删除的时机 */
    table_info_->table_->UpdateTupleMeta({0, true}, *rid);
    /**< 本地修改tuple */
    std::vector<Value> values{};
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    for(auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    *tuple = Tuple{values, &child_executor_->GetOutputSchema()};
    /**< 插入tuple */
    tuple->SetRid(table_info_->table_->InsertTuple({0, false}, *tuple, nullptr, nullptr, plan_->table_oid_).value());
    for(auto &index_info_ : index_info_vec_) {
      auto &index_ = index_info_->index_;
      // std::cout << index_->GetMetadata()->ToString() << std::endl;
      index_->InsertEntry(tuple->KeyFromTuple(table_info_->schema_, *index_->GetKeySchema(), index_->GetKeyAttrs()),
                          tuple->GetRid(), exec_ctx_->GetTransaction());
    }
    /**< 更新rows计数 */
    cnt++;
  }
  *tuple = Tuple({{TypeId::INTEGER, cnt}}, &GetOutputSchema()); /**< 返回更新rows的数量 */
  if(first_use_) {
    first_use_ = false;
    return true;
  } else {
    return false;
  }
}

}  // namespace bustub
