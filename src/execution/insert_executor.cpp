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
    : AbstractExecutor{exec_ctx},
      plan_{plan},
      first_use_(true),
      child_executor_(std::move(child_executor)) {}


void InsertExecutor::Init() {}


// (TODO) 如果中途插入失败, 考虑回滚已插入行
// (FIXED) insert的子节点为空, 需要返回一个值为0的tuple 添加字段:`first_use_`
//         后来我们干脆实现为Insert算子一次性插入即可
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  int32_t cnt = 0;
  auto table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  auto index_info_vec_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  while(child_executor_->Next(tuple, rid)) {
    /**< 插入tuple */
    tuple->SetRid(table_info_->table_->InsertTuple({0, false}, *tuple, nullptr, nullptr, plan_->table_oid_).value());
    /**< 修改相关Index */
    for(auto &index_info_ : index_info_vec_) {
      auto &index_ = index_info_->index_;
      // std::cout << index_->GetMetadata()->ToString() << std::endl;
      index_->InsertEntry(tuple->KeyFromTuple(table_info_->schema_, *index_->GetKeySchema(), index_->GetKeyAttrs()),
                          tuple->GetRid(), exec_ctx_->GetTransaction());
    }
    /**< 插入rows计数 */
    cnt++;
  }
  *tuple = Tuple({{TypeId::INTEGER, cnt}}, &GetOutputSchema()); /**< 返回插入rows的数量 */
  if(first_use_) {
    first_use_ = false;
    return true;
  } else {
    return false;
  }
}

}  // namespace bustub
