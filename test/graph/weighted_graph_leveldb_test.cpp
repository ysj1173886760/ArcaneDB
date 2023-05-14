/**
 * @file weighted_graph_leveldb_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-05-14
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "graph/weighted_graph_leveldb.h"
#include <gtest/gtest.h>

namespace arcanedb {
namespace graph {

class WeightedGraphLevelDBTest : public ::testing::Test {
public:
  void SetUp() {
    LevelDBBasedWeightedGraph::Destroy("test_db");
    LevelDBBasedWeightedGraphOptions opts;
    auto s = LevelDBBasedWeightedGraph::Open("test_db", opts, &db_);
    EXPECT_TRUE(s.ok());
  }

  void TearDown() {}

  std::unique_ptr<LevelDBBasedWeightedGraph> db_;
};

TEST_F(WeightedGraphLevelDBTest, VertexTest) {
  for (int i = 0; i < 100; i++) {
    EXPECT_TRUE(db_->InsertVertex(i, std::to_string(i)).ok());
  }
  for (int i = 0; i < 100; i++) {
    std::string value;
    EXPECT_TRUE(db_->GetVertex(i, &value).ok());
    EXPECT_EQ(value, std::to_string(i));
  }
  for (int i = 0; i < 100; i++) {
    EXPECT_TRUE(db_->DeleteVertex(i).ok());
  }
  for (int i = 0; i < 100; i++) {
    std::string value;
    EXPECT_TRUE(db_->GetVertex(i, &value).IsNotFound());
  }
}

TEST_F(WeightedGraphLevelDBTest, EdgeTest) {
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      EXPECT_TRUE(db_->InsertEdge(i, j, std::to_string(i + j)).ok());
    }
  }
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      std::string value;
      EXPECT_TRUE(db_->GetEdge(i, j, &value).ok());
      EXPECT_EQ(value, std::to_string(i + j));
    }
  }
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      EXPECT_TRUE(db_->DeleteEdge(i, j).ok());
    }
  }
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      std::string value;
      EXPECT_TRUE(db_->GetEdge(i, j, &value).IsNotFound());
    }
  }
}

TEST_F(WeightedGraphLevelDBTest, EdgeIteratorTest) {
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      EXPECT_TRUE(db_->InsertEdge(i, j, std::to_string(i + j)).ok());
    }
  }
  for (int i = 0; i < 10; i++) {
    auto iterator = db_->GetEdgeIterator(i);
    for (int j = 0; j < 10; j++) {
      EXPECT_TRUE(iterator.Valid());
      EXPECT_EQ(iterator.EdgeValue(), std::to_string(i + j));
      EXPECT_EQ(iterator.OutVertexId(), j);
      iterator.Next();
    }
    EXPECT_FALSE(iterator.Valid());
  }
}

} // namespace graph
} // namespace arcanedb