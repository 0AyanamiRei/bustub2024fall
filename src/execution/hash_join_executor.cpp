//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

using std::cout, std::endl;

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor{exec_ctx},
      plan_{plan},
      left_child_{std::move(left_child)}, 
      right_child_{std::move(right_child)} {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Fall 2024: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  RID rid{};
  Tuple tuple{};
  while(right_child_->Next(&tuple, &rid)) {
    auto keys = GetRightJoinKey(&tuple);
    if(!ht_.count(keys)) { /**< not hash collisions  */
      ht_[keys] = {tuple};
    } else { /**< hash collisions  */
      ht_[keys].push_back(tuple);
    }
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!tuples_.empty()) {
    *tuple = tuples_.back();
    tuples_.pop_back();
    return true;
  }

  Tuple l_tuple{};
  auto &l_Schema = left_child_->GetOutputSchema();
  auto &r_Schema = right_child_->GetOutputSchema();

  while(left_child_->Next(&l_tuple, rid)) {
    Value value;
    uint32_t l_colms = l_Schema.GetColumnCount();
    uint32_t r_colms = r_Schema.GetColumnCount();
    auto keys = GetLeftJoinKey(&l_tuple);
    /**< 不在哈希表中 */
    if(!ht_.count(keys)) {
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
    } else {
      for (auto &r_tuple : ht_[keys]) {
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

    if (!tuples_.empty()) {
      *tuple = tuples_.back();
      tuples_.pop_back();
      return true;
    }
  }
  if (!tuples_.empty()) {
    *tuple = tuples_.back();
    tuples_.pop_back();
    return true;
  } else {
    return false;
  }
}


auto HashJoinExecutor::GetLeftJoinKey (const Tuple *tuple) -> JoinKey {
  std::vector<Value> keys;
  for (const auto &expr : plan_->left_key_expressions_) {
    keys.emplace_back(expr->Evaluate(tuple, left_child_->GetOutputSchema()));
  }
  return {keys};
}

auto HashJoinExecutor::GetRightJoinKey (const Tuple *tuple) -> JoinKey {
  std::vector<Value> keys;
  for (const auto &expr : plan_->right_key_expressions_) {
    keys.emplace_back(expr->Evaluate(tuple, left_child_->GetOutputSchema()));
  }
  return {keys};
}

}  // namespace bustub
