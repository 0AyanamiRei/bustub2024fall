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

// NOLINTBEGIN

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
  for (auto rid : txn->GetWriteSets()) {
    
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

///! this is a Stop-the-world Garbage Collection function
///! so you don't need to get any locks
void TransactionManager::GarbageCollection() {
  auto water_mark = running_txns_.GetWatermark();

  for (auto iter = txn_map_.begin(); iter != txn_map_.end();) {
    auto &[txn_id, txn] = *iter;
    auto state = txn->GetTransactionState();
    if (state == TransactionState::ABORTED || state == TransactionState::COMMITTED) {
      if (txn->undo_logs_.empty() || water_mark > txn->commit_ts_.load()) {
        iter = txn_map_.erase(iter);
        continue;
      }
    }
    ++iter;
  }
}

void TransactionManager::WalkChain(UndoLink undo_link, std::unordered_map<txn_id_t, uint32_t> &delete_count) {
  while (undo_link.IsValid()) {
    auto txn = txn_map_[undo_link.prev_txn_];
    auto undo_log = txn->undo_logs_[undo_link.prev_log_idx_];
    if (delete_count.count(undo_link.prev_txn_) != 0U) {
      delete_count[undo_link.prev_txn_]++;
    } else {
      delete_count[undo_link.prev_txn_] = 1;
    }
    undo_link = undo_log.prev_version_;
  }
}

void TransactionManager::GarbageCollection(BufferPoolManager *bpm_) {
  throw NotImplementedException("Not implement this function, need to complete");
  auto water_mark = running_txns_.GetWatermark();
  std::unordered_map<txn_id_t, uint32_t> delete_count;

  // Traverse the version chain and clear useless undo logs
  for (auto &[page_id, prevs] : version_info_) {
    auto page_guard = bpm_->ReadPage(page_id);
    auto page = page_guard.As<TablePage>();
    for (auto iter = prevs->prev_link_.begin(); iter != prevs->prev_link_.end();) {
      auto [slot, undo_link] = *iter;
      RID rid{page_id, static_cast<uint32_t>(slot)};
      auto meta = page->GetTupleMeta(rid);

      // delete all version chain
      if (meta.ts_ <= water_mark) {
        iter = prevs->prev_link_.erase(iter);
        continue;
      }

      // cut off part of the version chain
      int log_num = 0;
      auto undo_log_opt = GetUndoLogOptional(undo_link);
      while (undo_log_opt.has_value()) {
        log_num++;
        if (water_mark > undo_log_opt->ts_ && log_num > 1) {
          // WalkChainAndClear(undo_link, delete_count);
          break;
        }
        undo_link = undo_log_opt->prev_version_;
        undo_log_opt = GetUndoLogOptional(undo_log_opt->prev_version_);
      }

      ++iter;
    }
  }

  // Traverse the txn_map_, remove it if its undo_logs is empty
  for (auto iter = txn_map_.begin(); iter != txn_map_.end();) {
    auto &[txn_id, txn] = *iter;
    auto state = txn->GetTransactionState();
    if (state == TransactionState::ABORTED || state == TransactionState::COMMITTED) {
      if (txn->undo_logs_.empty() || txn->undo_logs_.size() == delete_count[txn_id]) {
        iter = txn_map_.erase(iter);
        continue;
      }
    }
    ++iter;
  }
}

void TransactionManager::WalkChainAndClear(RID rid, std::unordered_map<txn_id_t, uint32_t> &delete_count) {
  throw NotImplementedException("Not implement this function");
}

///! Only used if the Tuple was modified between `GetTupleAndUndoLink` and `UpdateTupleAndUndoLink`.
auto TransactionManager::Check(const TupleMeta &meta, const Tuple &tuple, RID rid, std::optional<UndoLink> undo_link)
    -> bool {
  if (undo_link.has_value() && undo_link->IsValid()) {
    std::unique_lock<std::shared_mutex> l1(txn_map_mutex_);
    auto txn = txn_map_[undo_link->prev_txn_];
    return txn->read_ts_ >= meta.ts_;
  }
  return false;
}

}  // namespace bustub

// NOLINTEND

/**
 * @details about GC
 *
 * gc需要删除对所有事务不可见的undo_logs, 系统维护一个最低read-ts的水准线: `water_mark`
 * bustub中的gc仅仅将txn从txn_mgr的`txn_map_`中移除, 不需要相应地修改undo_logs.
 *
 * bustub每个事务
 *
 *
 * 1. ts < water_mark
 * 2. 不是version中最后一个undo_log (遍历version chain时的终止条件是找到第一个read-ts>=ts)
 *
 * (FIX) case : `water_mark > tuple's ts` (which in the table_heap) drop all undo_logs
 * if `water_mark > txn->commit_ts_.load()` we drop this txn can cover the case
 */
