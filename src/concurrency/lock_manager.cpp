//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

#include <utility>
#include <vector>

namespace bustub {

std::list<LockManager::LockRequest>::iterator LockManager::GetFirstWaitRqt(const RID &rid)
{
  LockRequestQueue* lock_queue = &lock_table_[rid];
  // 找到request  
  auto iter = lock_queue->request_queue_.begin();
  for (; iter != lock_queue->request_queue_.end(); ++iter) {
    if (!iter->granted_) {
      break;
    }
  }
  return iter;
}  

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  // 事务是未提交读级别则读不会加锁，终止事务抛出异常
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }

  // 如果事务是在shrink阶段则不应该出现此操作，终止事务抛出异常
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  // 将request插入到对应的list
  LockRequestQueue* lock_queue = &lock_table_[rid];
  lock_queue->request_queue_.push_back(LockRequest(txn->GetTransactionId(), LockMode::SHARED));

  // 此事务在request list的首项等待者 && lock_queue上没有排他锁被授予或正在更新锁 && 此事务被中止状态则跳出
  while (txn->GetState() != TransactionState::ABORTED && (GetFirstWaitRqt(rid)->txn_id_ != txn->GetTransactionId()
          || lock_queue->writing_ || lock_queue->upgrading_)) {
    lock_queue->cv_.wait(lock);
  }

  // 找到request  
  auto iter = lock_queue->request_queue_.begin();
  for (; iter != lock_queue->request_queue_.end(); ++iter) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      break;
    }
  }

  // 如果事务被终止则删除request中对应的项
  if (txn->GetState() == TransactionState::ABORTED) {
    lock_queue->request_queue_.erase(iter);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }

  iter->granted_ = true;
  lock_queue->shared_count_++;

  txn->GetSharedLockSet()->emplace(rid);

  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  if (txn->GetState() == TransactionState::SHRINKING){
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  // 将request插入到对应的list
  LockRequestQueue* lock_queue = &lock_table_[rid];
  lock_queue->request_queue_.push_back(LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE));

  // 此事务是request list的首项 && lock_queue上没有排他锁被授予 && 此事务被中止状态则跳出
  while (txn->GetState() != TransactionState::ABORTED && lock_queue->request_queue_.front().txn_id_ != txn->GetTransactionId()) {
    lock_queue->cv_.wait(lock);
  }
  // 找到request  
  auto iter = lock_queue->request_queue_.begin();
  for (; iter != lock_queue->request_queue_.end(); ++iter) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      break;
    }
  }

  // 如果事务被终止则删除request中对应的项
  if (txn->GetState() == TransactionState::ABORTED) {
    lock_queue->request_queue_.erase(iter);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }

  iter->granted_ = true;
  lock_queue->writing_ = true;

  txn->GetExclusiveLockSet()->emplace(rid);

  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  if (txn->GetState() == TransactionState::SHRINKING){
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  // upgrade在未获取共享锁
  if (!txn->IsSharedLocked(rid)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_ON_UNSHARED);
    return false;
  }

  // 获取对应的list
  LockRequestQueue* lock_queue = &lock_table_[rid];

  if (lock_queue->upgrading_){
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    return false;
  }

  lock_queue->upgrading_ = true;
  // 此事务是request list的首项 && lock_queue上没有排他锁被授予 && 此事务被中止状态则跳出
  while (txn->GetState() != TransactionState::ABORTED && (lock_queue->request_queue_.front().txn_id_ != txn->GetTransactionId()
        || lock_queue->shared_count_ > 1 || lock_queue->writing_ )) {
    lock_queue->cv_.wait(lock);
  }
  // 找到request  
  auto iter = lock_queue->request_queue_.begin();
  for (; iter != lock_queue->request_queue_.end(); ++iter) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      break;
    }
  }

  // 如果事务被终止则删除request中对应的项
  if (txn->GetState() == TransactionState::ABORTED) {
    lock_queue->request_queue_.erase(iter);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }

  iter->granted_ = true;
  iter->lock_mode_ = LockMode::EXCLUSIVE;

  lock_queue->upgrading_ = false;
  lock_queue->writing_ = true;
  lock_queue->shared_count_--;

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  LockRequestQueue* lock_queue = &lock_table_[rid];
  
  // 如果在rid上不存在锁
  if (!txn->IsSharedLocked(rid) && !txn->IsExclusiveLocked(rid)) {
    return false;
  }

  // 找到request  
  auto iter = lock_queue->request_queue_.begin();
  for (; iter != lock_queue->request_queue_.end(); ++iter) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      break;
    }
  }

  LockMode mode = iter->lock_mode_;
  lock_queue->request_queue_.erase(iter);

  // 在read commited级别，S锁的unlock不进入缩小阶段
  if (!(mode == LockMode::SHARED && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED)
      && txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  if (mode == LockMode::SHARED) {
    txn->GetSharedLockSet()->erase(rid);
    if (--lock_queue->shared_count_ == 0) {
      lock_queue->cv_.notify_all();
    }
  } else { 
    txn->GetExclusiveLockSet()->erase(rid);
    lock_queue->writing_ = false;
    lock_queue->cv_.notify_all();
  }

  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto iter = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t1);
  if (iter != waits_for_[t1].end()){
    waits_for_[t1].erase(iter);
  }
}
/*
bool LockManager::HasCycle(txn_id_t *txn_id) {

}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() { return {}; }

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here
      continue;
    }
  }
}*/
void LockManager::DeleteNode(txn_id_t txn_id) {
  waits_for_.erase(txn_id);
  Transaction* txn = TransactionManager::GetTransaction(txn_id);

  for (RID const &lock_rid : *(txn->GetSharedLockSet())){
    for (LockRequest const &lock_request : lock_table_[lock_rid].request_queue_){
      if (!lock_request.granted_){
        RemoveEdge(lock_request.txn_id_, txn_id);
      }
    }
  }

  for (RID const &lock_rid : *(txn->GetExclusiveLockSet())){
    for (LockRequest const &lock_request : lock_table_[lock_rid].request_queue_){
      if (!lock_request.granted_){
        RemoveEdge(lock_request.txn_id_, txn_id);
      }
    }
  }

}

bool LockManager::dfs(txn_id_t txn_id) {
  if (safe_set_.find(txn_id) != safe_set_.end()){
    return false;
  }
  active_set_.insert(txn_id);

  std::vector<txn_id_t> &next_node_vector = waits_for_[txn_id];
  std::sort(next_node_vector.begin(), next_node_vector.end());
  for (txn_id_t const next_node : next_node_vector){
    if (active_set_.find(next_node) != active_set_.end()){
      return true;
    }
    if (dfs(next_node)){
      return true;
    }
  }

  active_set_.erase(txn_id);
  safe_set_.insert(txn_id);
  return false;
}

bool LockManager::HasCycle(txn_id_t *txn_id) {
  for (auto const &start_txn_id : txn_set_){
    if (dfs(start_txn_id)){
      *txn_id = *active_set_.begin();
      for (auto const &active_txn_id : active_set_){
        *txn_id = std::max(*txn_id, active_txn_id);
      }
      active_set_.clear();
      return true;
    }

    active_set_.clear();
    txn_set_.erase(start_txn_id);
  }
  return false;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> result;
  for (auto const &pair : waits_for_){
    auto t1 = pair.first;
    for (auto const &t2 : pair.second){
      result.emplace_back(t1, t2);
    }
  }
  return result;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      // TODO(student): remove the continue and add your cycle detection and abort code here
      // First build the entire graph
      std::unique_lock<std::mutex> l(latch_);
      for (auto const &pair : lock_table_){
        for (auto const &lock_request_waiting : pair.second.request_queue_){
          txn_set_.insert(lock_request_waiting.txn_id_);
          // If it is waiting, add a edge to each lock-granted txn
          if (lock_request_waiting.granted_) { continue; }
          require_record_[lock_request_waiting.txn_id_] = pair.first;
          for (auto const &lock_request_granted : pair.second.request_queue_){
            if (!lock_request_granted.granted_) { continue; }
            AddEdge(lock_request_waiting.txn_id_, lock_request_granted.txn_id_);
          }
        }
      }

      // Break every cycle
      txn_id_t txn_id;
      while (HasCycle(&txn_id)){
        Transaction* txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);
        DeleteNode(txn_id);
        lock_table_[require_record_[txn_id]].cv_.notify_all();
      }

      // clear internal data, next time we will construct them on-the-fly!
      waits_for_.clear();
      safe_set_.clear();
      txn_set_.clear();
      require_record_.clear();

    }
  }
}

}  // namespace bustub
