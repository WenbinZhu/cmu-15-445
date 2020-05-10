/**
 * lock_manager.h
 *
 * Tuple level lock manager, use wait-die to prevent deadlocks
 */

#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <set>
#include <unordered_map>

#include "common/rid.h"
#include "concurrency/transaction.h"

namespace cmudb {

enum class LockType { SHARED, EXCLUSIVE };

class LockManager {

public:
    LockManager(bool strict_2PL) : strict_2PL_(strict_2PL) {};

    /*** below are APIs need to implement ***/
    // lock:
    // return false if transaction is aborted
    // it should be blocked on waiting and should return true when granted
    // note the behavior of trying to lock locked rids by same txn is undefined
    // it is transaction's job to keep track of its current locks
    bool LockShared(Transaction *txn, const RID &rid);
    bool LockExclusive(Transaction *txn, const RID &rid);
    bool LockUpgrade(Transaction *txn, const RID &rid);

    // unlock:
    // release the lock hold by the txn
    bool Unlock(Transaction *txn, const RID &rid);
    /*** END OF APIs ***/

private:
    // check if transaction state is valid in order to acquire lock
    bool TxnStateValidForLock(Transaction *txn);

    // txns that were granted a lock, used for lock table
    class GrantedTxns {
    public:
        GrantedTxns(LockType lockType, txn_id_t txn_id)
            : lockType_(lockType), granted_set_({txn_id}) {};

        // type of the lock granted
        LockType lockType_;
        // a set of txns that we granted this lock
        std::set<txn_id_t> granted_set_;
    };

    // whether to use strict 2PL
    bool strict_2PL_;
    // mutex to guard lock operations
    std::mutex mutex_;
    // lock table to record txns that were granted lock
    std::unordered_map<RID, std::shared_ptr<GrantedTxns>> lock_table_;
    // table of condition variables to wait and notify waiting txns
    std::unordered_map<RID, std::shared_ptr<std::condition_variable>> cv_table_;
};

} // namespace cmudb
