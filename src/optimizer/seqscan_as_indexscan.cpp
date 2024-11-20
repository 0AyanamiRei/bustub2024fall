#include <vector>
#include <unordered_map>
#include "optimizer/optimizer.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/logic_expression.h"

namespace bustub {

using std::cout, std::endl;

/**< 假设有表:
 * +-----+-----+-----+
 * | v_0 | v_1 | v_2 |
 * +-----+-----+-----+
 * ((v1=a) or (v2<=b)) or (c=v0)
 *          or
 *      /       \
 *     or       =
 *    /   \    /  \
 *   =     <= c   v0
 *  / \   /  \
 * v1  a v2   b
 * 最终要得到的内容:
 * col_id    = {    1,           2,           0}
 * pred_keys = {{"=", 'a'}, {"<=", 'b'}, {"=", 'c'}}
 * 
 * 需要处理的case:
 * 
*/
void parser_expr_4_indexscan(const AbstractExpressionRef &expr, std::vector<uint32_t> &col_id, 
                             std::vector<AbstractExpressionRef> &pred_keys, bool &fail) {
  if(!fail) {
    if (const auto *expr_cmpr = dynamic_cast<const ComparisonExpression *>(expr.get()); expr_cmpr != nullptr) {
      switch (expr_cmpr->comp_type_)
      {
        case ComparisonType::Equal:
        {
          int colunm = 0, constant = 1;
          if (const auto *_ = dynamic_cast<const ConstantValueExpression *>(expr_cmpr->GetChildAt(1).get()); _ == nullptr) {
            /**< 右儿子不是常值*/
            constant = 0, colunm = 1;
          }
          if (const auto *colV = dynamic_cast<const ColumnValueExpression *>(expr_cmpr->GetChildAt(colunm).get()); colV != nullptr) {
            /**< Now it's in form of "child[colunm] = child[constant]"  <=> "<column_expr> = <const_expr>" */
            col_id.push_back(colV->GetColIdx());
            pred_keys.push_back(expr_cmpr->GetChildAt(constant));
            return;
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
    } else if (const auto *expr_logic = dynamic_cast<const LogicExpression *>(expr.get()); expr_logic != nullptr) {
      if (expr_logic->logic_type_ == LogicType::Or) { /**< Now it's in form of (...) AND (...)*/
        BUSTUB_ENSURE(expr_logic->children_.size() == 2, "LogicExpression should have exactly 2 children.");
        parser_expr_4_indexscan(expr_logic->GetChildAt(0), col_id, pred_keys, fail);
        parser_expr_4_indexscan(expr_logic->GetChildAt(1), col_id, pred_keys, fail);
      } else { /**< Now it's in form of (...) AND (...)*/
        fail = true;
        return;
      }
    } else {
      fail = true;
      return;
    }
  }
}

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if(plan->GetType() == PlanType::SeqScan) {
    const auto &seqscan_plan = dynamic_cast<const SeqScanPlanNode &>(*plan);
    const auto &index_vec_ = catalog_.GetTableIndexes(seqscan_plan.table_name_);
    if(index_vec_.size()) {
      std::vector<uint32_t> col_id;
      std::vector<AbstractExpressionRef> pred_keys;
      bool fail = false;
      parser_expr_4_indexscan(seqscan_plan.filter_predicate_, col_id, pred_keys, fail);
      if(!fail) {
        /**< 目前索引的key可以假设只有一列 */
        index_oid_t index_oid;
        auto it = std::find_if(index_vec_.begin(), index_vec_.end(), [&](const auto &index_info) {
          return index_info->index_->GetKeyAttrs()[0] == col_id[0]; });
        if (it != index_vec_.end()) {
          index_oid = (*it)->index_oid_;
          /**< col_id中的值必须保持两两相同 */
          if (std::adjacent_find(col_id.begin(), col_id.end(), std::not_equal_to<>()) == col_id.end()) {
            /**< 去重pred_keys (TODO) 为Value提供一个哈希函数以使用std::unordered_set来优雅的去重 */
            std::vector<Value> seen_values;
            auto it = pred_keys.begin();
            while (it != pred_keys.end()) {
                auto conV = dynamic_cast<ConstantValueExpression*>(it->get());
                bool found = false;
                for (auto &v : seen_values) {
                    if (conV->val_.CompareEquals(v) == bustub::CmpBool::CmpTrue) {
                        found = true;
                        break;
                    }
                }
                if (found) {
                    it = pred_keys.erase(it);
                } else {
                    seen_values.push_back(conV->val_);
                    ++it;
                }
            }
            for(uint32_t i = 0; i + 1 < pred_keys.size(); i ++) {
              col_id.pop_back();
            }
            return std::make_shared<IndexScanPlanNode>(seqscan_plan.output_schema_, seqscan_plan.table_oid_, 
                                    index_oid, seqscan_plan.filter_predicate_, pred_keys);
          }
        }
      }
    }
  }

  return optimized_plan;
}

/******************************************
 * @example 假设v1上构建了索引, v2上没有构建索引
 * 实验中并非所有情况都需要构建索引
 * 1. WHERE v1 = 1;
 * 2. WHERE 1 = v1;
 * 3. WHERE v1 = 1 OR v1 = 4;
 * 而下面的这些情况就不需要构建索引, 仍然使用顺序扫描
 * 1. WHERE v1 = 1 AND v2 = 2;
 * 2. WHERE v1 = 1 OR v2 = 2; (即使v1和v2都拥有索引)
 * 
 * 例子, 第一行结果是v1~v3都未构建索引, 第二行在v1构建索引
 * explain (o) select * from t1 where v1 = 1 and/or v2 = 1;
 * => SeqScan {table=t1, filter=((#0.0=1) and/or (#0.1=1))}
 * => SeqScan {table=t1, filter=((#0.0=1) and/or (#0.1=1))}
 * 
 * explain (o) select * from t1 where v1 = 1 or v1 = 2;
 * => SeqScan {index_oid=0, filter=((#0.0=1)or(#0.0=2))}
 * => IndexScan {index_oid=0, filter=((#0.0=1)or(#0.0=2))}
 * 
 * "seq_scan"留下的问题: 分析filter_predicate_->Evaluate(...)
 * 例子: select * from t1 where v1 = 1 or v1 = 2 or v1 = 3;
 *       => filter=(((#0.0=1)or(#0.0=2))or(#0.0=3))
 *           or
 *        /       \
 *       or        =
 *     /    \     / \
 *    =      =   v1  3
 *   / \    / \ 
 *  v1  1  v1  2
 *  
 * 我们用Lg表示二元运算符, Vl表示值, 再来看一下这个AST:
 *           Lg
 *        /      \
 *       Lg        Lg
 *     /    \     / \
 *    Lg     Lg   Vl  Vl
 *   / \    / \ 
 *  Vl Vl  Vl  Vl
 ******************************************/

}  // namespace bustub
