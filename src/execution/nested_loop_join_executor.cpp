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

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(TryNext(tuple, rid)) {
    return true;
  }

  Tuple r_tuple{};
  Tuple l_tuple{};
  auto &l_Schema = left_executor_->GetOutputSchema();
  auto &r_Schema = right_executor_->GetOutputSchema();

  while(left_executor_->Next(&l_tuple, rid)) {
    Value value;
    uint32_t l_colms = l_Schema.GetColumnCount();
    uint32_t r_colms = r_Schema.GetColumnCount();
    for(right_executor_->Init(); right_executor_->Next(&r_tuple, rid); ) {
      value = plan_->predicate_->EvaluateJoin(&l_tuple, l_Schema, &r_tuple, r_Schema);
      if (!value.IsNull() && value.GetAs<bool>()) {
        std::vector<Value> values{};
        values.reserve(l_colms + r_colms);
        for(uint32_t i = 0; i < l_colms; i ++) {
          values.push_back(l_tuple.GetValue(&l_Schema, i));
        }
        for(uint32_t i = 0; i < r_colms; i ++) {
          values.push_back(r_tuple.GetValue(&r_Schema, i));
        }
        tuples_.emplace_back(Tuple{values, &GetOutputSchema()});
      }
    }

    if(TryNext(tuple, rid)) {
      return true;
    } else {
        switch (plan_->GetJoinType()) {
        case JoinType::LEFT: /**< 右节点全部加入NULL */
        {
          std::vector<Value> values{};
          values.reserve(l_colms + r_colms);
          for(uint32_t i = 0; i < l_colms; i ++) {
            values.push_back(l_tuple.GetValue(&l_Schema, i));
          }
          for(uint32_t i = 0; i < r_colms; i ++) {
            values.emplace_back(ValueFactory::GetNullValueByType(r_Schema.GetColumn(i).GetType()));
          }
          *tuple = Tuple{values, &GetOutputSchema()};
          return true;
        }
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

  return TryNext(tuple, rid);
}

auto inline NestedLoopJoinExecutor::TryNext(Tuple *tuple, RID *rid) -> bool{
  if(!tuples_.empty()) {
    *tuple = tuples_.front();
    tuples_.pop_front();
    return true;
  }
  return false;
}

}  // namespace bustub
