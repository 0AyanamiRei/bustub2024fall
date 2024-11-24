//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor{exec_ctx},
      plan_{plan},
      left_executor_{std::move(child_executor)} {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  left_executor_->Init();
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // Tuple r_tuple{};
  Tuple l_tuple{};
  // RID r_rid{};
  RID l_rid{};
  auto &l_Schema = left_executor_->GetOutputSchema();
  auto &r_Schema = plan_->inner_table_schema_;
  uint32_t l_colms = l_Schema.GetColumnCount();
  uint32_t r_colms = r_Schema->GetColumnCount();
  auto &index = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)->index_;

  while(left_executor_->Next(&l_tuple, &l_rid)) {
    /**< 构建查询key */
    std::vector<RID> result;
    std::vector<Value> values{};
    values.push_back(plan_->key_predicate_->Evaluate(&l_tuple, l_Schema));
    /**< 查询内表 */
    index->ScanKey({values, index->GetKeySchema()}, &result, exec_ctx_->GetTransaction());
    /**< 创建返回tuple */
    if(!result.empty()) {
      auto [meta, r_tuple] = exec_ctx_->GetCatalog()->GetTable(plan_->inner_table_oid_)->table_->GetTuple(result[0]);
      if(!meta.is_deleted_) {
        values.clear();
        values.reserve(l_colms + r_colms);
        for(uint32_t i = 0; i < l_colms; i ++) {
          values.push_back(l_tuple.GetValue(&l_Schema, i));
        }
        for(uint32_t i = 0; i < r_colms; i ++) {
          values.push_back(r_tuple.GetValue(r_Schema.get(), i));
        }
        *tuple = Tuple(values, &GetOutputSchema());
        return true;
      }
    } else {
      switch (plan_->GetJoinType()) {
      case JoinType::LEFT: /**< 右节点全部加入NULL */
        values.clear();
        values.reserve(l_colms + r_colms);
        for(uint32_t i = 0; i < l_colms; i ++) {
          values.push_back(l_tuple.GetValue(&l_Schema, i));
        }
        for(uint32_t i = 0; i < r_colms; i ++) {
          values.emplace_back(ValueFactory::GetNullValueByType(r_Schema->GetColumn(i).GetType()));
        }
        *tuple = Tuple(values, &GetOutputSchema());
        return true;
      case JoinType::RIGHT:
        throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan_->GetJoinType()));
      case JoinType::OUTER:
        throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan_->GetJoinType()));
      case JoinType::INNER:
        continue;
      case JoinType::INVALID:
        throw bustub::ExecutionException(fmt::format("join type {} shoud not be here", plan_->GetJoinType()));
      }
    }
  }

  return false;
}

}  // namespace bustub
