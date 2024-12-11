//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.h
//
// Identification: src/include/execution/executors/update_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/execution_common.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/update_plan.h"

#include "storage/table/tuple.h"
#include "type/value_factory.h"

/******************************************
 * @attention
 * p4-3.4 Update & Delete Executor 提示
 * Your update executor should be implemented as a pipeline breaker.
 *
 * Pipeline Breaker:
 * first store all tuples from the child executor to a local
 * buffer before writing any updates.
 *
 * (TODO)
 * 目前的实现暂时没有在本地存储所有tuples再更新, 但依旧要等待所有tuples
 * 更新完后才会结束一轮Next()
 *
 * 目前的做法存在Halloween problem
 ******************************************/

namespace bustub {

/**
 * UpdateExecutor executes an update on a table.
 * Updated values are always pulled from a child.
 */
class UpdateExecutor : public AbstractExecutor {
  friend class UpdatePlanNode;

 public:
  /**
   * Construct a new UpdateExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The update plan to be executed
   * @param child_executor The child executor that feeds the update
   */
  UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the update */
  void Init() override;

  /**
   * Yield the next tuple from the update.
   * @param[out] tuple The next tuple produced by the update
   * @param[out] rid The next tuple RID produced by the update (ignore this)
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   *
   * NOTE: UpdateExecutor::Next() does not use the `rid` out-parameter.
   */
  auto Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the update */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  const UpdatePlanNode *plan_;                       /** The update plan node to be executed */
  const TableInfo *table_info_;                      /** Metadata identifying the table that should be updated */
  std::unique_ptr<AbstractExecutor> child_executor_; /** The child executor to obtain value from */
  std::vector<Tuple> tuple_buffer;
};
}  // namespace bustub
