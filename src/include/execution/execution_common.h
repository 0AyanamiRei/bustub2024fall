//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_common.h
//
// Identification: src/include/execution/execution_common.h
//
// Copyright (c) 2014-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <string>
#include <utility>
#include <vector>

#include "binder/bound_order_by.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "storage/table/tuple.h"

namespace bustub {

/** The SortKey defines a list of values that sort is based on */
using SortKey = std::vector<Value>;
/** The SortEntry defines a key-value pairs for sorting tuples and corresponding RIDs */
using SortEntry = std::pair<SortKey, Tuple>;

/** The Tuple Comparator provides a comparison function for SortEntry */
class TupleComparator {
 public:
  explicit TupleComparator(std::vector<OrderBy> order_bys);

  /** 根据`order_bys_`比较`entry_a`和`entry_b` */
  auto operator()(const SortEntry &entry_a, const SortEntry &entry_b) const -> bool;

 private:
 /**< OrderBy = pair<OrderByType, AbstractExpressionRef> */
  std::vector<OrderBy> order_bys_;
};

/**
 * Generate sort key for a tuple based on the order by expressions.
 */
auto GenerateSortKey(const Tuple &tuple, const std::vector<OrderBy> &order_bys, const Schema &schema) -> SortKey;

/**
 * Above are all you need for P3.
 * You can ignore the remaining part of this file until P4.
 */

/** apply all modifications provided from `undo_logs` without checking the timestamp
 * @warning 即该函数不会检查undo到指定版本, 需要在传入前裁剪undo_logs
*/
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple>;

auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                     Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>>;

/** 获取当前事务可见的tuple版本 */
auto GetReadableTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta, Transaction *txn,
                      TransactionManager *txn_mgr) -> std::optional<Tuple>;

auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog;

auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog;

// Help functions 

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap);

auto GetUndoLogSchema(const Schema *schema, const UndoLog &log, std::vector<uint32_t> *attrs) -> Schema;

auto UndoLogToString(const Schema *schema, const UndoLog &log) -> std::string;

inline auto IsConflict(Transaction *txn, timestamp_t ts) -> bool {
  return !(txn->GetReadTs() >= ts || txn->GetTransactionTempTs() == ts);
}

// TODO(P4): Add new functions as needed... You are likely need to define some more functions.
//
// To give you a sense of what can be shared across executors / transaction manager, here are the
// list of helper function names that we defined in the reference solution. You should come up with
// your own when you go through the process.
// * WalkUndoLogs
// * Modify
// * IsWriteWriteConflict
// * GenerateNullTupleForSchema
// * GetUndoLogSchema
//
// We do not provide the signatures for these functions because it depends on the your implementation
// of other parts of the system. You do not need to define the same set of helper functions in
// your implementation. Please add your own ones as necessary so that you do not need to write
// the same code everywhere.

}  // namespace bustub
