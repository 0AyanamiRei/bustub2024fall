//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_ = last_commit_ts_.load();

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  auto commit_ts = commit_ts_.load() + 1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  for (auto &[tid, rids] : txn->GetWriteSets()) {
    auto &table = catalog_->GetTable(tid)->table_;
    for (auto &rid : rids) {
      // Update the tuple's `ts_` in tuple heap
      auto is_deleted = table->GetTupleMeta(rid).is_deleted_;
      table->UpdateTupleMeta(TupleMeta{commit_ts, is_deleted}, rid);
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  txn->commit_ts_ = commit_ts;
  commit_ts_.fetch_add(1);
  last_commit_ts_.fetch_add(1);
  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(commit_ts);
  running_txns_.RemoveTxn(txn->read_ts_);
  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

/**
 * @brief this is a Stop-the-world Garbage Collection function
 *
 * 删除对所有事务不可见的undo_log, 即在read-ts=water_mark的txn视角下不可见
 * 具体来说是同时满足以下条件:
 * 1. ts < water_mark
 * 2. 不是version中最后一个undo_log (遍历version chain时的终止条件是找到第一个read-ts>=ts)
 *
 */
void TransactionManager::GarbageCollection() {
  auto water_mark = running_txns_.GetWatermark();
  std::unordered_map<txn_id_t, uint32_t> delete_count;
  // Traverse the table heap and the version chain
  // std::unique_lock<std::shared_mutex> l(version_info_mutex_); // don't need to
  for (auto &[page_id, prevs] : version_info_) {
    for (auto &[slot, undo_link] : prevs->prev_link_) {
      // RID rid{page_id, slot};
      auto undo_log_opt = GetUndoLogOptional(undo_link);
      int log_num = 0;
      while (undo_log_opt.has_value()) {
        log_num++;
        if (water_mark > undo_log_opt->ts_ && log_num > 1) {
          // we delete the undo_log from here: --(undo_link)--> undo_log
          WalkUndoLogAndClear(undo_link, delete_count);
          break;
        }
        undo_link = undo_log_opt->prev_version_;
        undo_log_opt = GetUndoLogOptional(undo_log_opt->prev_version_);
      }
    }
  }

  // std::unique_lock<std::shared_mutex> l(txn_map_mutex_); // don't need to
  for (auto &[txn_id, cnt] : delete_count) {
    auto txn = txn_map_[txn_id];
    if (txn->GetTransactionState() != TransactionState::RUNNING) {
      if (delete_count[txn_id] == txn->undo_logs_.size()) {
        LOG_INFO("delete txn%ld", txn->GetTransactionIdHumanReadable());
        txn_map_.erase(txn_id);
      }
    }
  }
}

void TransactionManager::WalkUndoLogAndClear(UndoLink undo_link, std::unordered_map<txn_id_t, uint32_t> &delete_count) {
  while (undo_link.IsValid()) {
    auto txn = txn_map_[undo_link.prev_txn_];
    auto undo_log = txn->undo_logs_[undo_link.prev_log_idx_];
    if (delete_count.count(undo_link.prev_txn_)) {
      delete_count[undo_link.prev_txn_]++;
    } else {
      delete_count[undo_link.prev_txn_] = 1;
    }
    undo_link = undo_log.prev_version_;
    // txn->undo_logs_[undo_link.prev_log_idx_] = {};
  }
}

}  // namespace bustub
