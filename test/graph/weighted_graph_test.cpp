/**
 * @file weighted_graph_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-27
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "graph/weighted_graph.h"
#include "util/bthread_util.h"
#include <gtest/gtest.h>

namespace arcanedb {
namespace graph {

class WeightedGraphDBTest : public ::testing::Test {
public:
  void SetUp() {
    auto s = WeightedGraphDB::Open("test_db", &db_);
    EXPECT_TRUE(s.ok());
  }

  void TearDown() {}

  std::unique_ptr<WeightedGraphDB> db_;
  Options opts_;
};

TEST_F(WeightedGraphDBTest, VertexTest) {
  for (int i = 0; i < 100; i++) {
    auto txn = db_->BeginRwTxn(opts_);
    EXPECT_TRUE(txn->InsertVertex(i, std::to_string(i)).ok());
    EXPECT_TRUE(txn->Commit().IsCommit());
  }
  for (int i = 0; i < 100; i++) {
    auto txn = db_->BeginRoTxn(opts_);
    std::string value;
    EXPECT_TRUE(txn->GetVertex(i, &value).ok());
    EXPECT_EQ(value, std::to_string(i));
    EXPECT_TRUE(txn->Commit().IsCommit());
  }
  for (int i = 0; i < 100; i++) {
    auto txn = db_->BeginRwTxn(opts_);
    EXPECT_TRUE(txn->DeleteVertex(i).ok());
    EXPECT_TRUE(txn->Commit().IsCommit());
  }
  for (int i = 0; i < 100; i++) {
    auto txn = db_->BeginRoTxn(opts_);
    std::string value;
    EXPECT_TRUE(txn->GetVertex(i, &value).IsNotFound());
    EXPECT_TRUE(txn->Commit().IsCommit());
  }
}

TEST_F(WeightedGraphDBTest, EdgeTest) {
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      auto txn = db_->BeginRwTxn(opts_);
      EXPECT_TRUE(txn->InsertEdge(i, j, std::to_string(i + j)).ok());
      EXPECT_TRUE(txn->Commit().IsCommit());
    }
  }
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      auto txn = db_->BeginRoTxn(opts_);
      std::string value;
      EXPECT_TRUE(txn->GetEdge(i, j, &value).ok());
      EXPECT_EQ(value, std::to_string(i + j));
      EXPECT_TRUE(txn->Commit().IsCommit());
    }
  }
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      auto txn = db_->BeginRwTxn(opts_);
      EXPECT_TRUE(txn->DeleteEdge(i, j).ok());
      EXPECT_TRUE(txn->Commit().IsCommit());
    }
  }
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      auto txn = db_->BeginRoTxn(opts_);
      std::string value;
      EXPECT_TRUE(txn->GetEdge(i, j, &value).IsNotFound());
      EXPECT_TRUE(txn->Commit().IsCommit());
    }
  }
}

TEST_F(WeightedGraphDBTest, ConcurrentTest) {
  // 10 worker insert vertex
  // 10 worker insert edge
  util::WaitGroup wg(20);
  for (int i = 0; i < 10; i++) {
    util::LaunchAsync([&, index = i]() {
      for (int j = 0; j < 10; j++) {
        auto id = index * 10 + j;
        auto txn = db_->BeginRwTxn(opts_);
        EXPECT_TRUE(txn->InsertVertex(id, std::to_string(id)).ok());
        EXPECT_TRUE(txn->Commit().IsCommit());
      }
      for (int j = 0; j < 10; j++) {
        auto id = index * 10 + j;
        auto txn = db_->BeginRoTxn(opts_);
        std::string value;
        EXPECT_TRUE(txn->GetVertex(id, &value).ok());
        EXPECT_EQ(value, std::to_string(id));
        EXPECT_TRUE(txn->Commit().IsCommit());
      }
      wg.Done();
    });
  }
  for (int i = 0; i < 10; i++) {
    util::LaunchAsync([&, index = i]() {
      for (int j = 0; j < 10; j++) {
        auto txn = db_->BeginRwTxn(opts_);
        EXPECT_TRUE(txn->InsertEdge(index, j, std::to_string(index + j)).ok());
        EXPECT_TRUE(txn->Commit().IsCommit());
      }
      for (int j = 0; j < 10; j++) {
        auto txn = db_->BeginRoTxn(opts_);
        std::string value;
        EXPECT_TRUE(txn->GetEdge(index, j, &value).ok());
        EXPECT_EQ(value, std::to_string(index + j));
        EXPECT_TRUE(txn->Commit().IsCommit());
      }
      wg.Done();
    });
  }
  wg.Wait();
}

} // namespace graph
} // namespace arcanedb