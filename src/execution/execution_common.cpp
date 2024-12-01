//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_common.cpp
//
// Identification: src/execution/execution_common.cpp
//
// Copyright (c) 2024-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/execution_common.h"

#include "catalog/catalog.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value_factory.h"

namespace bustub {

// Inner Help function

inline auto GetReadableTs(timestamp_t ts) -> timestamp_t { return ts >= TXN_START_ID ? ts ^ TXN_START_ID : ts; }

TupleComparator::TupleComparator(std::vector<OrderBy> order_bys) : order_bys_(std::move(order_bys)) {}

auto TupleComparator::operator()(const SortEntry &entry_a, const SortEntry &entry_b) const -> bool {
  auto &key_a = entry_a.first;
  auto &key_b = entry_b.first;
  for(uint32_t i = 0; i < key_a.size(); i ++) {
    switch (order_bys_[i].first)
    {
    case OrderByType::INVALID:
      throw Exception("invalid order_by type");
    case OrderByType::DEFAULT: /**< 默认按ASC */
    case OrderByType::ASC:  /**< 升序 */
      if(key_a[i].CompareLessThan(key_b[i]) == CmpBool::CmpTrue) { // a < b
        return true;
      } else if (key_a[i].CompareGreaterThan(key_b[i]) == CmpBool::CmpTrue) { // a > b
        return false;
      } else { // a = b
        continue;
      }
    case OrderByType::DESC: /**< 降序 */
      if(key_a[i].CompareLessThan(key_b[i]) == CmpBool::CmpTrue) { // a < b
        return false;
      } else if (key_a[i].CompareGreaterThan(key_b[i]) == CmpBool::CmpTrue) { // a > b
        return true;
      } else { // a = b
        continue;
      }
    }
  }
  return true;
}

auto GenerateSortKey(const Tuple &tuple, const std::vector<OrderBy> &order_bys, const Schema &schema) -> SortKey {
  SortKey keys{};
  for(auto &e : order_bys) {
    keys.emplace_back(e.second->Evaluate(&tuple, schema));
  }
  return keys;
}

/**
 * Above are all you need for P3.
 * You can ignore the remaining part of this file until P4.
 */

/**
 * @brief Reconstruct a tuple by applying the provided undo logs from the base tuple. All logs in the undo_logs are
 * applied regardless of the timestamp
 *
 * @param schema The schema of the base tuple and the returned tuple.
 * @param base_tuple The base tuple to start the reconstruction from.
 * @param base_meta The metadata of the base tuple.
 * @param undo_logs The list of undo logs to apply during the reconstruction, the front is applied first.
 * @return An optional tuple that represents the reconstructed tuple. If the tuple is deleted as the result, returns
 * std::nullopt.
 */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  // txn_scan_test-case add: base_tuple in marked `is_deleted` and redo_logs is empty
  if (base_meta.is_deleted_ && undo_logs.size() == 0U) {
    return std::nullopt;
  }
  // txn_scan_test-case add: if last undo_log is delete, return nullopt
  if (!undo_logs.empty() && undo_logs.back().is_deleted_) {
    return std::nullopt;
  }
  std::vector<Value> values;
  for(size_t i = 0, n = schema->GetColumnCount(); i < n ; i ++) {
    values.push_back(base_tuple.GetValue(schema, i));
  }
  BUSTUB_ENSURE(values.size() == schema->GetColumnCount(), "error size of values");
  // Now we got the copy of base_tuple's values (latest tuple)
  for (auto &log : undo_logs) {
    if (log.is_deleted_) { // 清空values
      for(size_t i = 0, n = schema->GetColumnCount(); i < n ; i ++) {
        values[i] = ValueFactory::GetNullValueByType(values[i].GetTypeId());
      }
    } else {
      std::vector<uint32_t> attrs;
      const auto schema_log = GetUndoLogSchema(schema, log, &attrs);
      // use attrs and schema_log to rebuild the tuple
      for (uint32_t i = 0, len = attrs.size(); i < len; ++i) {
        values[attrs[i]] = log.tuple_.GetValue(&schema_log, i); 
      }
    }
  }
  return Tuple{values, schema};
}

/**
 * @brief Collects the undo logs sufficient to reconstruct the tuple w.r.t. the txn.
 *
 * @param rid The RID of the tuple.
 * @param base_meta The metadata of the base tuple.
 * @param base_tuple The base tuple.
 * @param undo_link The undo link to the latest undo log.
 * @param txn The transaction.
 * @param txn_mgr The transaction manager.
 * @return An optional vector of undo logs to pass to ReconstructTuple(). std::nullopt if the tuple did not exist at the
 * time.
 */
auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                     Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>> {
  auto read_ts = txn->GetReadTs();
  // The base_tuple just we can read, so return empty vector
  if ((txn->GetTransactionTempTs() == base_meta.ts_ 
       && base_meta.ts_ >= TXN_START_ID) ||
      read_ts >= base_meta.ts_) {
    return std::vector<UndoLog>{};
  }
  // We need to collect enough undo_log to reconstruct the tuple
  if (undo_link.has_value()) {
    std::vector<UndoLog> logs;
    auto undo_log = txn_mgr->GetUndoLogOptional(*undo_link);
    while (undo_log.has_value()) {
      logs.emplace_back(std::move(*undo_log));
      if (read_ts >= logs.back().ts_) {
        return logs;
      }
      undo_log = txn_mgr->GetUndoLogOptional(undo_log->prev_version_);
    }
  }
  return std::nullopt;
}

// equivalent to `ReconstructTuple`+`CollectUndoLogs`
auto GetReadableTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta, Transaction *txn,
                      TransactionManager *txn_mgr) -> std::optional<Tuple>{
  auto undo_link = txn_mgr->GetUndoLink(base_tuple.GetRid());
  auto undo_logs = undo_link.has_value()
                       ? CollectUndoLogs(base_tuple.GetRid(), base_meta, base_tuple, undo_link, txn, txn_mgr)
                       : std::nullopt;
  auto tuple = undo_logs.has_value() ? ReconstructTuple(schema, base_tuple, base_meta, *undo_logs) : std::nullopt;
  return tuple;
}

/**
 * @brief Generates a new undo log as the transaction tries to modify this tuple at the first time.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param ts The timestamp of the base tuple.
 * @param prev_version The undo link to the latest undo log of this tuple.
 * @return The generated undo log.
 */
auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog {
  UNIMPLEMENTED("not implemented");
}

/**
 * @brief Generate the updated undo log to replace the old one, whereas the tuple is already modified by this txn once.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param log The original undo log.
 * @return The updated undo log.
 */
auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog {
  UNIMPLEMENTED("not implemented");
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  std::stringstream ss;
  fmt::println(stderr, "debug_hook: {}", info);
  auto iter = table_heap->MakeIterator();
  while (!iter.IsEnd()) {
    auto rid = iter.GetRID();
    auto [metadata, tuple] = iter.GetTuple();
    ss << fmt::format("RID={}/{}", rid.GetPageId(), rid.GetSlotNum());
    if (metadata.ts_ & TXN_START_ID) {
      ss << fmt::format(" ts={}* ", metadata.ts_ ^ TXN_START_ID);
    } else {
      ss << fmt::format(" ts={}  ", metadata.ts_);
    }
    if (metadata.is_deleted_) {
      ss << "<del>\n";
    } else {
      ss << tuple.ToString(&table_info->schema_) << "\n";
    }
    auto undo_link = txn_mgr->GetUndoLink(rid);
    if (undo_link.has_value()) {
      auto undo_log = txn_mgr->GetUndoLogOptional(*undo_link);
      while (undo_log.has_value()) {
        const auto schema_log = GetUndoLogSchema(&table_info->schema_, *undo_log, nullptr);
        ss << fmt::format("  txn{}@{} ", undo_link->prev_txn_ ^ TXN_START_ID, undo_link->prev_log_idx_)
           <<  UndoLogToString(&table_info->schema_, *undo_log)
           << "\n";
        undo_link = undo_log->prev_version_;
        undo_log = txn_mgr->GetUndoLogOptional(undo_log->prev_version_);
      }
    }
    ss << "----------------------------------------------\n";
    ++iter;
  }
  fmt::println("{}", ss.str());

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

auto GetUndoLogSchema(const Schema *schema, const UndoLog &log, std::vector<uint32_t> *attrs) -> Schema {
  std::vector<uint32_t> local_attrs;
  if (attrs == nullptr) {
    attrs = &local_attrs;
  }
  for (uint32_t i = 0, len = log.modified_fields_.size(); i < len; ++i) {
    if (log.modified_fields_[i]) {
      attrs->push_back(i);
    }
  }
  return Schema::CopySchema(schema, *attrs);
}

auto UndoLogToString(const Schema *schema, const UndoLog &log) -> std::string {
  std::stringstream ss;
  ss << fmt::format("ts={} ", log.ts_);
  if (log.is_deleted_) {
    ss << "<del>";
  } else {
    const auto schema_log = GetUndoLogSchema(schema, log, nullptr);
    ss << "(";
    for (uint32_t i = 0, j = 0, n = schema->GetColumnCount(); i < n ; ++i) {
        if (i != 0U) {
          ss << " ";
        }
        if (log.modified_fields_[i]) {
          auto val = log.tuple_.GetValue(&schema_log, j++);
          if (val.IsNull()) {
            ss << "<NULL>";
          } else {
            ss << fmt::format("{}", val.ToString());
          }
        } else {
          ss << fmt::format("_");
        }
        if (i+1 < n) {
          ss << ",";
        }
    }
    ss << ")";
  }
  return ss.str();
}


}  // namespace bustub
