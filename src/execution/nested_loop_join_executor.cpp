//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor{exec_ctx},
      plan_{plan},
      left_executor_{std::move(left_executor)}, 
      right_executor_{std::move(right_executor)} {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple r_tuple{};
  Tuple l_tuple{};
  RID r_rid{};
  RID l_rid{};
  auto &l_Schema = left_executor_->GetOutputSchema();
  auto &r_Schema = right_executor_->GetOutputSchema();

  while(left_executor_->Next(&l_tuple, &l_rid)) {
    Value value;
    for(right_executor_->Init(); right_executor_->Next(&r_tuple, &r_rid); ) {
      value = plan_->predicate_->EvaluateJoin(&l_tuple, l_Schema, &r_tuple, r_Schema);
      if (value.IsNull() || !value.GetAs<bool>()) {
        continue;
      } else {
        break;
      }
    }

    if (plan_->GetJoinType() == JoinType::INNER && (value.IsNull() || !value.GetAs<bool>())) {
      continue;
    }

    std::vector<Value> values{};
    uint32_t l_colms = l_Schema.GetColumnCount();
    uint32_t r_colms = r_Schema.GetColumnCount();
    values.reserve(l_colms + r_colms);

    for(uint32_t i = 0; i < l_colms; i ++) {
      values.push_back(l_tuple.GetValue(&l_Schema, i));
    }

    if(!value.IsNull() && value.GetAs<bool>()) {
      for(uint32_t i = 0; i < r_colms; i ++) {
        values.push_back(r_tuple.GetValue(&r_Schema, i));
      }
    } else {
      switch (plan_->GetJoinType()) {
      case JoinType::LEFT: /**< 右节点全部加入NULL */
        for(uint32_t i = 0; i < r_colms; i ++) {
          values.emplace_back(ValueFactory::GetNullValueByType(r_Schema.GetColumn(i).GetType()));
        }
        break;
      case JoinType::RIGHT:
        throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan_->GetJoinType()));
      case JoinType::OUTER:
        throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan_->GetJoinType()));
      case JoinType::INNER:
        throw bustub::ExecutionException(fmt::format("join type {} shoud not be here", plan_->GetJoinType()));
      case JoinType::INVALID:
        throw bustub::ExecutionException(fmt::format("join type {} shoud not be here", plan_->GetJoinType()));
      }
    }

    *tuple = Tuple(values, &GetOutputSchema());
    return true;
  }
  return false;
}

}  // namespace bustub
