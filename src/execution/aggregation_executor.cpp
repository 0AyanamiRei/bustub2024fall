//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor{exec_ctx}, 
      plan_{plan}, 
      child_executor_{std::move(child_executor)}, 
      aht_{plan->aggregates_, plan->agg_types_}, 
      aht_iterator_{aht_.Begin()}, 
      first_{true} {
  RID rid{};
  Tuple tuple{};
  while (child_executor_->Next(&tuple, &rid)) {
    auto keys = MakeAggregateKey(&tuple);
    auto values = MakeAggregateValue(&tuple);
    aht_.InsertCombine(keys, values);
  }
  aht_iterator_ = {aht_.Begin()};
}

void AggregationExecutor::Init() {}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());

  if(aht_.Begin() == aht_.End()) {
    if(plan_->GetGroupBys().size() != 0U) {
      return  false;
    }
  }

  if(first_) {
    if(aht_iterator_ == aht_.End()) {
      for (const auto &expr : plan_->GetGroupBys()) {
        values.emplace_back(ValueFactory::GetNullValueByType(expr->GetReturnType().GetType()));
      }
      for(uint32_t i = 0; i < plan_->agg_types_.size(); i ++) {
        if(plan_->agg_types_[i] == AggregationType::CountStarAggregate) {
          values.emplace_back(ValueFactory::GetZeroValueByType(plan_->aggregates_[i]->GetReturnType().GetType()));
        } else {
          values.emplace_back(ValueFactory::GetNullValueByType(plan_->aggregates_[i]->GetReturnType().GetType()));
        }
      }
      *tuple = Tuple(values, &GetOutputSchema());
      first_ = false;
      return true;
    } else {
      auto keys = aht_iterator_.Key().group_bys_;
      auto vals = aht_iterator_.Val().aggregates_;
      for(uint32_t i = 0; i < keys.size(); i ++) {
        values.push_back(keys[i]);
      }
      for(uint32_t i = 0; i < vals.size(); i ++) {
        values.push_back(vals[i]);
      }
      if(++aht_iterator_ == aht_.End()) { /**< 下次返回false */
        first_ = false;
      }
      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }
  } else {
    return false;
  }
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub

/**************************
 * @example
explain (o,p) select v1, v2, count(*) as a, count(v1) as b, sum(v4) as c, min(v3) as d, max(v2) as e
              from t1 group by v1, v2;

+-------+-------+-------+-------+
| t1.v1 | t1.v2 | t1.v3 | t1.v4 |
+-------+-------+-------+-------+
| 1     | 10    | 201   | 101   |
| 1     | 10    | 202   | 102   |
| 3     | 20    | 203   | 103   |
| 3     | 20    | 204   | 104   |
| 5     | 20    | 205   | 105   |
+-------+-------+-------+-------+

每一轮的结果如下:
keys = {1, 10}; values ={201, 101}
keys = {1, 10}; values ={202, 102}
keys = {3, 20}; values ={203, 103}
keys = {3, 10}; values ={204, 104}
keys = {5, 20}; values ={205, 105}

以第一轮调用InsertCombine(keys, values)为例:
agg_types_: count_star, count, sum, min, max
agg_exprs_:      *    ,  v1  , v4,  v3,  v2


**************************/