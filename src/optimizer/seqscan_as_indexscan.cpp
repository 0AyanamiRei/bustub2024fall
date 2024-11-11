#include <vector>
#include <unordered_map>
#include "optimizer/optimizer.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"

namespace bustub {

using std::cout, std::endl;

/**< 如果是v1 = 1 or v1 = 2 or v1 = 3这种形式下一个谓词节点形式如下:
 *       Logic
 *     /      \
 *    colV   conV
 * 第一个根节点就代表v1, 第二个根节点就代表常数1, 2, 3
*/
void Show_child(const AbstractExpressionRef &expr, std::vector<uint32_t> &col_id,
                std::vector<AbstractExpressionRef> &pred_keys) {
  if(!(expr->children_[0]->children_.size() + expr->children_[1]->children_.size())) {
    int colunm = 0, constant = 1;
    if(expr->GetChildAt(colunm)->GetReturnType().GetName() == "<val>") {
      colunm = 1, constant = 0;
    }
    auto colV = dynamic_cast<const ColumnValueExpression *>(expr->GetChildAt(colunm).get()); // ColumnValueExpression
    // auto conV = dynamic_cast<const ConstantValueExpression *>(expr->GetChildAt(constant).get()); // maybe ConstantValueExpression
    col_id.push_back(colV->GetColIdx());
    /**< TODO 目前暂时不会处理v1 = v2 这种情况, 只处理了v1 = 1 */
    pred_keys.push_back(expr->GetChildAt(constant));
    return;
  }
  for(std::vector<AbstractExpressionRef>::size_type i = 0; i < expr->children_.size(); i++) {
    Show_child(expr->children_[i], col_id, pred_keys);
  }
}

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule

  /**< 将当前节点的儿子移动到`children`中 */
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeMergeFilterScan(child));
  }
  /**< 将被优化的算子节点 */
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  /**< 将SeqScan优化为IndexScan */
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seqscan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    auto index_vec_ = catalog_.GetTableIndexes(seqscan_plan.table_name_);
    if(!index_vec_.size()) { /**< 没有索引 */
      return optimized_plan;
    }
    if(!seqscan_plan.filter_predicate_) { /**< 没有过滤 */
      return optimized_plan;
    }
    std::vector<uint32_t> col_id;
    std::vector<AbstractExpressionRef> pred_keys;
    Show_child(seqscan_plan.filter_predicate_, col_id, pred_keys);

    /**< 目前索引的key可以假设只有一列 */
    index_oid_t index_oid;
    std::vector<std::shared_ptr<bustub::IndexInfo>>::size_type i;
    for(i = 0; i < index_vec_.size(); i ++) { /**< 寻找索引的oid */
      if (index_vec_[i]->index_->GetKeyAttrs()[0] == col_id[0]) {
        index_oid = index_vec_[i]->index_oid_;
        break;
      }
    }

    if(i == index_vec_.size()) {
      return optimized_plan;
    }

    for(std::vector<uint32_t>::size_type i = 0; i + 1 < col_id.size(); i ++) { /**< col_id中的值必须保持两两相同 */
      if(col_id[i] != col_id[i + 1]) {
        return optimized_plan;
      }
    }

    /**< 去重pred_keys */
    // 暂时性假设全部为ConstantValueExpression类
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

    for(int i = 0; i + 1< static_cast<int>(pred_keys.size()); i ++) {
      col_id.pop_back();
    }
    return std::make_shared<IndexScanPlanNode>(seqscan_plan.output_schema_, seqscan_plan.table_oid_, 
                           index_oid, seqscan_plan.filter_predicate_, pred_keys);
  }

  return plan;
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
