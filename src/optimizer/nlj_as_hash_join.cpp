#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

using std::cout, std::endl;

void parser_expr_4_hash_join (const AbstractExpressionRef &expr,
            std::vector<std::vector<AbstractExpressionRef>> &key_exprs, bool &fail) {
  if(fail) {
    return;
  }
  if (const auto *expr_cmpr = dynamic_cast<const ComparisonExpression *>(expr.get()); expr_cmpr != nullptr) {
    switch (expr_cmpr->comp_type_)
    {
      case ComparisonType::Equal:
      {
        BUSTUB_ENSURE(expr_cmpr->children_.size() == 2, "ComparisonExpression should have exactly 2 children.");
        if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr_cmpr->children_[0].get()); 
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr_cmpr->children_[1].get());
              right_expr != nullptr) {
            // Now it's in form of <column_expr> = <column_expr>
            key_exprs[left_expr->GetTupleIdx()].emplace_back(std::make_shared<ColumnValueExpression>
                                                            (0, left_expr->GetColIdx(), left_expr->GetReturnType()));
            key_exprs[right_expr->GetTupleIdx()].emplace_back(std::make_shared<ColumnValueExpression>
                                                            (0, right_expr->GetColIdx(), right_expr->GetReturnType()));
            return;
          }     
        }
        fail = true;
        return;
      }
      case ComparisonType::GreaterThan:
      case ComparisonType::GreaterThanOrEqual:
      case ComparisonType::LessThan:
      case ComparisonType::LessThanOrEqual:
      case ComparisonType::NotEqual:
      default:
      {
        fail = true;
        return;
      }
    }
    if (expr_cmpr->comp_type_ == ComparisonType::Equal) {
    }
    // it's not the form of <column_expr> = <column_expr>.
    fail = true;
    return;
  } else if (const auto *expr_logic = dynamic_cast<const LogicExpression *>(expr.get()); expr_logic != nullptr) {
    if (expr_logic->logic_type_ == LogicType::And) { /**< Now it's in form of (...) AND (...)*/
      BUSTUB_ENSURE(expr_logic->children_.size() == 2, "LogicExpression should have exactly 2 children.");
      parser_expr_4_hash_join(expr->GetChildAt(0), key_exprs, fail);
      parser_expr_4_hash_join(expr->GetChildAt(1), key_exprs, fail);
    } else { /**< Now it's in form of (...) OR (...)*/
      fail = true;
      return;
    }
  } else { /**< other expr type */
    fail = true;
    return;
  }
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if(optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    BUSTUB_ASSERT(nlj_plan.GetChildren().size() == 2, "NLJ should have exactly 2 children");
    std::vector<std::vector<AbstractExpressionRef>> key_exprs(2);
    bool fail = false;
    parser_expr_4_hash_join(nlj_plan.Predicate(), key_exprs, std::ref(fail));
    if(!fail) {
      return std::make_shared<HashJoinPlanNode>(std::move(nlj_plan.output_schema_), std::move(nlj_plan.children_[0]),
                  std::move(nlj_plan.children_[1]), std::move(key_exprs[0]), std::move(key_exprs[1]), nlj_plan.join_type_);
    }
  }
  
  return optimized_plan;
}
}  // namespace bustub


/***********
 * EXPLAIN (o) SELECT * FROM t1, t2 WHERE v1 = v3 AND v2 = v4;
 *  优化前=> NestedLoopJoin { type=Inner, predicate=((#0.0=#1.0)and(#0.1=#1.1)) }
 *  优化后=> HashJoin { type=Inner, left_key=["#0.0", "#0.1"], right_key=["#0.0", "#0.1"] }
 * 
 * 
 * Cpp知识: key_exprs(2), key_exprs[2];
 * 
 * 
*************/
