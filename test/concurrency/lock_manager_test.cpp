/**
 * lock_manager_test.cpp
 */

#include <thread>
#include <future>
#include <mutex>
#include <string>
#include <iostream>

#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

namespace cmudb {


/*
 * This test is only a sanity check. Please do not rely on this test
 * to check the correctness.
 */
TEST(LockManagerTest, BasicTest) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  std::thread t0([&] {
    Transaction txn(0);
    bool res = lock_mgr.LockShared(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
  });

  std::thread t1([&] {
    Transaction txn(1);
    bool res = lock_mgr.LockShared(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
  });

  t0.join();
  t1.join();
}

TEST(LockManagerTest, ReadWriteTest) {
  LockManager lock_mgr{true};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  std::promise<void> exclusive_lock_promise;
  std::shared_future<void> exclusive_lock_future = exclusive_lock_promise.get_future().share();

  std::promise<void> shared_lock_promise_1;
  std::shared_future<void> shared_lock_future_1 = shared_lock_promise_1.get_future().share();

  std::promise<void> shared_lock_promise_2;
  std::shared_future<void> shared_lock_future_2 = shared_lock_promise_2.get_future().share();

  std::mutex mutex;
  std::string result;

  auto write_to_result = [&](std::string str) {
    mutex.lock();
    result += str;
    mutex.unlock();
  };

  std::thread t0([&] {
    Transaction txn(3);
    bool res = lock_mgr.LockExclusive(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    write_to_result("E");
    exclusive_lock_promise.set_value();

    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
  });

  std::thread t1([&] {
    Transaction txn(2);

    exclusive_lock_future.wait();

    bool res = lock_mgr.LockShared(&txn, rid);
    // if t3 gets lock first, t1 and t2 may need to abort
    if (!res) {
      shared_lock_promise_1.set_value();
      txn_mgr.Abort(&txn);
      return;
    }
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    write_to_result("S");
    // force both shared locks to be acquired before anyone realses
    shared_lock_promise_1.set_value();
    shared_lock_future_2.wait();

    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
  });

  std::thread t2([&] {
    Transaction txn(1);

    exclusive_lock_future.wait();

    bool res = lock_mgr.LockShared(&txn, rid);
    // if t3 gets lock first, t1 and t2 may need to abort
    if (!res) {
      shared_lock_promise_2.set_value();
      txn_mgr.Abort(&txn);
      return;
    }
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    write_to_result("S");
    // force both shared locks to be acquired before anyone realses
    shared_lock_promise_2.set_value();
    shared_lock_future_1.wait();

    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
  });

  std::thread t3([&] {
    Transaction txn(0);

    exclusive_lock_future.wait();

    bool res = lock_mgr.LockExclusive(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    write_to_result("E");

    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
  });

  t0.join();
  t1.join();
  t2.join();
  t3.join();

  std::cout << "Lock sequence: " << result << std::endl;
  EXPECT_TRUE(result == "ESSE" || result == "EESS" || result == "EE" || result == "EES");
}

} // namespace cmudb
