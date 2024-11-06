#include "optimizer/optimizer.h"

namespace bustub {

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

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    
  }

  return plan;
}

/******************************************
 * @example
 * 实验中并非所有情况都需要构建索引
 * 1. WHERE v1 = 1;
 * 2. WHERE 1 = v1;
 * 3. WHERE v1 = 1 OR v1 = 4;
 * 而下面的这些情况就不需要构建索引, 仍然使用顺序扫描
 * 1. WHERE v1 = 1 AND v2 = 2;
 ******************************************/

}  // namespace bustub
