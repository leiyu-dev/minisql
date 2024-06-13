#include "concurrency/lock_manager.h"

#include <iostream>

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "concurrency/txn_manager.h"

void LockManager::SetTxnMgr(TxnManager *txn_mgr) { txn_mgr_ = txn_mgr; }

/**
 * TODO: Student Implement
 */
bool LockManager::LockShared(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  auto &queue = lock_table_[rid];
  auto txn_id = txn->GetTxnId();

  // Check if the transaction should be aborted
  if (txn->GetState() == TxnState::kAborted) {
    throw TxnAbortException(txn_id, AbortReason::kDeadlock);
  }

  // Wait while there's an exclusive lock
  while (queue.is_writing_) {
    queue.cv_.wait(lock);
  }

  // Place the lock request in the queue
  queue.EmplaceLockRequest(txn_id, LockMode::kShared);
  queue.sharing_cnt_++;
  return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockExclusive(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  auto &queue = lock_table_[rid];
  auto txn_id = txn->GetTxnId();

  // Check if the transaction should be aborted
  if (txn->GetState() == TxnState::kAborted) {
    throw TxnAbortException(txn_id, AbortReason::kDeadlock);
  }

  // Wait while there are other locks
  while (queue.sharing_cnt_ > 0 || queue.is_writing_) {
    queue.cv_.wait(lock);
  }

  // Place the lock request in the queue
  queue.EmplaceLockRequest(txn_id, LockMode::kExclusive);
  queue.is_writing_ = true;
  return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockUpgrade(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  auto &queue = lock_table_[rid];
  auto txn_id = txn->GetTxnId();

  // Only one transaction can attempt to upgrade at a time
  if (queue.is_upgrading_) return false;

  auto it = queue.GetLockRequestIter(txn_id);
  if (it->lock_mode_ != LockMode::kShared || queue.sharing_cnt_ != 1) {
    return false;  // Cannot upgrade unless this is the only shared lock
  }

  queue.is_upgrading_ = true;
  queue.cv_.wait(lock, [&] { return queue.sharing_cnt_ == 1 && !queue.is_writing_; });

  // Upgrade the lock
  it->lock_mode_ = LockMode::kExclusive;
  queue.is_writing_ = true;
  queue.sharing_cnt_ = 0;
  queue.is_upgrading_ = false;
  return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::Unlock(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  auto &queue = lock_table_[rid];

  if (queue.EraseLockRequest(txn->GetTxnId())) {
    if (queue.req_list_.empty()) {
      queue.is_writing_ = false;
    } else if (queue.sharing_cnt_ > 0) {
      queue.sharing_cnt_--;
    }

    queue.cv_.notify_all();  // Notify others waiting for this lock
    return true;
  }
  return false;
}

/**
 * TODO: Student Implement
 */
void LockManager::LockPrepare(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  lock_table_.try_emplace(rid);
}

/**
 * TODO: Student Implement
 */
void LockManager::CheckAbort(Txn *txn, LockManager::LockRequestQueue &req_queue) {
  if (txn->GetState() == TxnState::kAborted) {
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kDeadlock);
  }
}

/**
 * TODO: Student Implement
 */
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  std::unique_lock<std::mutex> lock(latch_);
  waits_for_[t1].insert(t2);
}

/**
 * TODO: Student Implement
 */
void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  std::unique_lock<std::mutex> lock(latch_);
  auto it = waits_for_.find(t1);
  if (it != waits_for_.end()) {
    it->second.erase(t2);
  }
}

/**
 * TODO: Student Implement
 */
bool LockManager::HasCycle(txn_id_t &newest_tid_in_cycle) {
  std::unique_lock<std::mutex> lock(latch_);
  // Implement DFS to detect cycle
  // (This is a placeholder; actual implementation required)
  return false;
}

void LockManager::DeleteNode(txn_id_t txn_id) {
  waits_for_.erase(txn_id);

  auto *txn = txn_mgr_->GetTransaction(txn_id);

  for (const auto &row_id : txn->GetSharedLockSet()) {
    for (const auto &lock_req : lock_table_[row_id].req_list_) {
      if (lock_req.granted_ == LockMode::kNone) {
        RemoveEdge(lock_req.txn_id_, txn_id);
      }
    }
  }

  for (const auto &row_id : txn->GetExclusiveLockSet()) {
    for (const auto &lock_req : lock_table_[row_id].req_list_) {
      if (lock_req.granted_ == LockMode::kNone) {
        RemoveEdge(lock_req.txn_id_, txn_id);
      }
    }
  }
}

/**
 * TODO: Student Implement
 */
void LockManager::RunCycleDetection() {}

/**
 * TODO: Student Implement
 */
std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::unique_lock<std::mutex> lock(latch_);
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (const auto &pair : waits_for_) {
    for (const auto &dest : pair.second) {
      edges.emplace_back(pair.first, dest);
    }
  }
  return edges;
}
