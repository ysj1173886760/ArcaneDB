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
#include <gtest/gtest.h>

namespace arcanedb {
namespace graph {

class WeightedGraphDBTest : public ::testing::Test {
public:
  void SetUp() {
    auto s = WeightedGraphDB::Open(&db_);
    EXPECT_TRUE(s.ok());
  }

  void TearDown() {}

  std::unique_ptr<WeightedGraphDB> db_;
};

TEST_F(WeightedGraphDBTest, VertexTest) {
  for (int i = 0; i < 100; i++) {
    auto txn = db_->BeginRwTxn();
    EXPECT_TRUE(txn->InsertVertex(i, std::to_string(i)).ok());
    EXPECT_TRUE(txn->Commit().IsCommit());
  }
  for (int i = 0; i < 100; i++) {
    auto txn = db_->BeginRoTxn();
    std::string value;
    EXPECT_TRUE(txn->GetVertex(i, &value).ok());
    EXPECT_EQ(value, std::to_string(i));
    EXPECT_TRUE(txn->Commit().IsCommit());
  }
  for (int i = 0; i < 100; i++) {
    auto txn = db_->BeginRwTxn();
    EXPECT_TRUE(txn->DeleteVertex(i).ok());
    EXPECT_TRUE(txn->Commit().IsCommit());
  }
  for (int i = 0; i < 100; i++) {
    auto txn = db_->BeginRoTxn();
    std::string value;
    EXPECT_TRUE(txn->GetVertex(i, &value).IsNotFound());
    EXPECT_TRUE(txn->Commit().IsCommit());
  }
}

TEST_F(WeightedGraphDBTest, EdgeTest) {
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      auto txn = db_->BeginRwTxn();
      EXPECT_TRUE(txn->InsertEdge(i, j, std::to_string(i + j)).ok());
      EXPECT_TRUE(txn->Commit().IsCommit());
    }
  }
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      auto txn = db_->BeginRoTxn();
      std::string value;
      EXPECT_TRUE(txn->GetEdge(i, j, &value).ok());
      EXPECT_EQ(value, std::to_string(i + j));
      EXPECT_TRUE(txn->Commit().IsCommit());
    }
  }
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      auto txn = db_->BeginRwTxn();
      EXPECT_TRUE(txn->DeleteEdge(i, j).ok());
      EXPECT_TRUE(txn->Commit().IsCommit());
    }
  }
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      auto txn = db_->BeginRoTxn();
      std::string value;
      EXPECT_TRUE(txn->GetEdge(i, j, &value).IsNotFound());
      EXPECT_TRUE(txn->Commit().IsCommit());
    }
  }
}

} // namespace graph
} // namespace arcanedb