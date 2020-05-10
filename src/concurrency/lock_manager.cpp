/**
 * lock_manager.cpp
 */

#include "concurrency/lock_manager.h"

namespace cmudb {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
    if (!TxnStateValidForLock(txn)) {
        return false;
    }

    txn_id_t txn_id = txn->GetTransactionId();
    std::unique_lock<std::mutex> lock(mutex_);

    // tuple is locked in exlusive mode
    if (lock_table_.find(rid) != lock_table_.end()) {
        auto grantedTxns = lock_table_[rid];
        assert(!grantedTxns->granted_set_.empty());
        if (grantedTxns->lockType_ == LockType::EXCLUSIVE) {
            // tuple locked in exclusive mode, txn is yonger, abort
            if (txn_id >= *grantedTxns->granted_set_.begin()) {
                txn->SetState(TransactionState::ABORTED);
                return false;
            }
            // tuple locked in exlusive mode, txn is older, wait
            if (cv_table_.find(rid) == cv_table_.end()) {
                cv_table_.emplace(rid, std::make_shared<std::condition_variable>());
            }
            // increase shared_ptr reference, used by unlock to remove cv from cv table
            auto cv = cv_table_[rid];
            cv->wait(lock, [&] { return lock_table_.find(rid) == lock_table_.end()
                                 || lock_table_[rid]->lockType_ == LockType::SHARED; });
        }
    }

    // tuple is not locked by other txn, this check is needed as we might
    // just come out of wait and lock table becomes empty due to unlock
    if (lock_table_.find(rid) == lock_table_.end()) {
        lock_table_.emplace(rid, std::make_shared<GrantedTxns>(LockType::SHARED, txn_id));
        txn->GetSharedLockSet()->emplace(rid);
        return true;
    }

    // tuple is locked in shared mode
    auto grantedTxns = lock_table_[rid];
    assert(!grantedTxns->granted_set_.empty());
    if (grantedTxns->lockType_ == LockType::SHARED) {
        grantedTxns->granted_set_.emplace(txn_id);
        txn->GetSharedLockSet()->emplace(rid);
        return true;
    }

    // this should not happen as we already waited for exclusive lock to be unlocked
    assert(false);
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
    if (!TxnStateValidForLock(txn)) {
        return false;
    }

    txn_id_t txn_id = txn->GetTransactionId();
    std::unique_lock<std::mutex> lock(mutex_);

    // tuple is not locked by other txn
    if (lock_table_.find(rid) == lock_table_.end()) {
        lock_table_.emplace(rid, std::make_shared<GrantedTxns>(LockType::EXCLUSIVE, txn_id));
        txn->GetExclusiveLockSet()->emplace(rid);
        return true;
    }

    // tuple locked, txn is yonger, abort
    auto grantedTxns = lock_table_[rid];
    assert(!grantedTxns->granted_set_.empty());
    if (txn_id >= *grantedTxns->granted_set_.begin()) {
        txn->SetState(TransactionState::ABORTED);
        return false;
    }

    // tuple locked, txn is older, wait
    if (cv_table_.find(rid) == cv_table_.end()) {
        cv_table_.emplace(rid, std::make_shared<std::condition_variable>());
    }

    // increase shared_ptr reference, used by unlock to remove cv from cv table
    auto cv = cv_table_[rid];
    cv->wait(lock, [&] { return lock_table_.find(rid) == lock_table_.end(); });
    lock_table_.emplace(rid, std::make_shared<GrantedTxns>(LockType::EXCLUSIVE, txn_id));
    txn->GetExclusiveLockSet()->emplace(rid);

    return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
    if (!TxnStateValidForLock(txn)) {
        return false;
    }

    std::unique_lock<std::mutex> lock(mutex_);

    // lock upgrade requires tuple to be locked before
    if (lock_table_.find(rid) == lock_table_.end()) {
        txn->SetState(TransactionState::ABORTED);
        return false;
    }

    // lock upgrade requires tuple to be locked in shared mode
    auto grantedTxns = lock_table_[rid];
    auto txn_id = txn->GetTransactionId();
    if (grantedTxns->lockType_ != LockType::SHARED
        || grantedTxns->granted_set_.count(txn_id) == 0) {
        txn->SetState(TransactionState::ABORTED);
        return false;
    }

    // remove this txn from the lock table to release shared lock
    // if no other txn is holding the lock, upgrade to exclusive lock
    txn->GetSharedLockSet()->erase(rid);
    grantedTxns->granted_set_.erase(txn_id);
    if (grantedTxns->granted_set_.empty()) {
        lock_table_[rid] = std::make_shared<GrantedTxns>(LockType::EXCLUSIVE, txn_id);
        txn->GetExclusiveLockSet()->emplace(rid);
        return true;
    }

    // other txns also hold this shared lock, txn is yonger, abort
    // need to make sure this txn is already removed from lock table
    if (txn_id >= *grantedTxns->granted_set_.begin()) {
        txn->SetState(TransactionState::ABORTED);
        return false;
    }

    // other txns also hold this shared lock, txn is older, wait
    if (cv_table_.find(rid) == cv_table_.end()) {
        cv_table_.emplace(rid, std::make_shared<std::condition_variable>());
    }

    // increase shared_ptr reference, used by unlock to remove cv from cv table
    auto cv = cv_table_[rid];
    cv->wait(lock, [&] { return lock_table_.find(rid) == lock_table_.end(); });
    lock_table_.emplace(rid, std::make_shared<GrantedTxns>(LockType::EXCLUSIVE, txn_id));
    txn->GetExclusiveLockSet()->emplace(rid);

    return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
    // strick 2PL can only be unlocked after committed or aborted
    if (strict_2PL_ && !(txn->GetState() == TransactionState::COMMITTED
        || txn->GetState() == TransactionState::ABORTED)) {
        txn->SetState(TransactionState::ABORTED);
        return false;
    }

    std::unique_lock<std::mutex> lock(mutex_);

    // tuple has not been locked
    if (lock_table_.find(rid) == lock_table_.end()) {
        txn->SetState(TransactionState::ABORTED);
        return false;
    }

    // remove txn from granted set
    auto grantedTxns = lock_table_[rid];
    auto txn_id = txn->GetTransactionId();
    if (grantedTxns->granted_set_.erase(txn_id) == 0) {
        txn->SetState(TransactionState::ABORTED);
        return false;
    }

    // update txn state to SHRINKING if current state is GROWING
    if (!strict_2PL_ && txn->GetState() == TransactionState::GROWING) {
        txn->SetState(TransactionState::SHRINKING);
    }

    // remove tuple from txn lock sets
    if (grantedTxns->lockType_ == LockType::SHARED) {
        txn->GetSharedLockSet()->erase(rid);
    } else {
        txn->GetExclusiveLockSet()->erase(rid);
    }

    // notify all waiting txns if no txn is holding this tuple
    // and remove rid from lock table and cv table if possible
    if (grantedTxns->granted_set_.empty()) {
        lock_table_.erase(rid);
        if (cv_table_.find(rid) != cv_table_.end()) {
            cv_table_[rid]->notify_all();
            // if no one is waiting on cv, can be removed from table
            if (cv_table_[rid].unique()) {
                cv_table_.erase(rid);
            }
        }
    }

    return true;
}

bool LockManager::TxnStateValidForLock(Transaction *txn) {
    if (txn->GetState() != TransactionState::GROWING) {
        txn->SetState(TransactionState::ABORTED);
        return false;
    }

    return true;
}

} // namespace cmudb
