//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new index scan executor.
   * @param exec_ctx the executor context
   * @param plan the index scan plan to be executed
   */
    IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

    auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

    void Init() override;

    auto Next(Tuple *tuple, RID *rid) -> bool override;

    using Iterator = IndexIterator<IntegerKeyType, IntegerValueType, IntegerComparatorType>;
    using BplusTreeIterator = BPlusTreeIndex<IntegerKeyType, IntegerValueType, IntegerComparatorType>;

   private:
    auto NextScan(Tuple *tuple, RID *rid) -> bool;
    void InitIterator ();
    /** The index scan plan node to be executed. */
    const IndexScanPlanNode *plan_;
    uint32_t pred_keys_at_;
    Iterator iter_;
};
}  // namespace bustub


/********************
 * @bug 在不使用迭代器的时候请不要构造迭代器
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 * 
**********************/