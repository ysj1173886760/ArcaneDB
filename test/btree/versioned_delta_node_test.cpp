/**
 * @file versioned_delta_node_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-23
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "btree/page/versioned_delta_node.h"
#include "property/schema.h"
#include <gtest/gtest.h>
#include <memory>

namespace arcanedb {
namespace btree {

class VersionedDeltaNodeTest : public ::testing::Test {
public:
  property::Schema MakeTestSchema() noexcept {
    property::Column column1{
        .column_id = 0, .name = "int64", .type = property::ValueType::Int64};
    property::Column column2{
        .column_id = 1, .name = "int32", .type = property::ValueType::Int32};
    property::Column column3{
        .column_id = 2, .name = "string", .type = property::ValueType::String};
    property::RawSchema schema{.columns = {column1, column2, column3},
                               .schema_id = 0,
                               .sort_key_count = 2};
    return property::Schema(schema);
  }

  struct ValueStruct {
    int64_t point_id;
    int32_t point_type;
    std::string value;
  };

  std::vector<ValueStruct> GenerateValueList(size_t size) noexcept {
    std::vector<ValueStruct> value;
    for (int i = 0; i < size; i++) {
      value.push_back(ValueStruct{
          .point_id = i, .point_type = 0, .value = std::to_string(i)});
    }
    return value;
  }

  std::shared_ptr<VersionedDeltaNode> MakeDelta(const ValueStruct &value,
                                                bool is_deleted,
                                                TxnTs write_ts) noexcept {
    property::ValueRefVec vec;
    vec.push_back(value.point_id);
    vec.push_back(value.point_type);
    vec.push_back(value.value);
    util::BufWriter writer;
    EXPECT_TRUE(property::Row::Serialize(vec, &writer, &schema_).ok());
    auto str = writer.Detach();
    property::Row row(str.data());
    if (is_deleted) {
      return std::make_shared<VersionedDeltaNode>(row.GetSortKeys(), write_ts);
    }
    return std::make_shared<VersionedDeltaNode>(row, write_ts);
  }

  void TestRead(RowView *view, const ValueStruct &value) {
    {
      property::ValueResult res;
      EXPECT_TRUE(view->at(0).GetProp(0, &res, &schema_).ok());
      EXPECT_EQ(std::get<int64_t>(res.value), value.point_id);
    }
    {
      property::ValueResult res;
      EXPECT_TRUE(view->at(0).GetProp(1, &res, &schema_).ok());
      EXPECT_EQ(std::get<int32_t>(res.value), value.point_type);
    }
    {
      property::ValueResult res;
      EXPECT_TRUE(view->at(0).GetProp(2, &res, &schema_).ok());
      EXPECT_EQ(std::get<std::string_view>(res.value), value.value);
    }
  }

  void SetUp() { schema_ = MakeTestSchema(); }

  void TearDown() {}

  const int32_t type_ = 0;
  property::Schema schema_;
};

TEST_F(VersionedDeltaNodeTest, BasicTest) {
  // read single node write and read
  TxnTs ts = 1;
  {
    ValueStruct value{.point_id = 0, .point_type = 0, .value = "hello"};
    auto delta = MakeDelta(value, false, ts);
    auto sk = property::SortKeys({value.point_id, value.point_type});
    RowView view;
    EXPECT_TRUE(delta->GetRow(sk.as_ref(), ts, &view).ok());
    TestRead(&view, value);
  }
  {
    ValueStruct value{.point_id = 0, .point_type = 0, .value = "hello"};
    auto delta = MakeDelta(value, true, ts);
    auto sk = property::SortKeys({value.point_id, value.point_type});
    RowView view;
    auto s = delta->GetRow(sk.as_ref(), ts, &view);
    EXPECT_EQ(s, Status::Deleted()) << s.ToString();
  }
}

TEST_F(VersionedDeltaNodeTest, CompactionTest) {
  VersionedDeltaNodeBuilder builder;
  auto value_list = GenerateValueList(100);
  std::vector<std::shared_ptr<VersionedDeltaNode>> deltas;
  TxnTs ts = 1;
  for (const auto &value : value_list) {
    auto node = MakeDelta(value, false, ts);
    deltas.push_back(node);
    builder.AddDeltaNode(node.get());
  }
  auto compacted = builder.GenerateDeltaNode();
  for (const auto &value : value_list) {
    auto sk = property::SortKeys({value.point_id, value.point_type});
    RowView view;
    auto s = compacted->GetRow(sk.as_ref(), ts, &view);
    EXPECT_TRUE(s.ok());
    TestRead(&view, value);
  }
}

TEST_F(VersionedDeltaNodeTest, MultiVersionTest) {
  VersionedDeltaNodeBuilder builder;
  std::vector<ValueStruct> value_list(100);
  std::vector<std::shared_ptr<VersionedDeltaNode>> deltas;
  for (int i = 99; i >= 0; i--) {
    value_list[i] =
        ValueStruct{.point_id = 0, .point_type = 0, .value = std::to_string(i)};
    auto node = MakeDelta(value_list[i], false, i + 1);
    deltas.push_back(node);
    builder.AddDeltaNode(node.get());
  }
  auto compacted = builder.GenerateDeltaNode();
  auto sk =
      property::SortKeys({value_list[0].point_id, value_list[0].point_type});
  for (int i = 0; i < 100; i++) {
    RowView view;
    auto s = compacted->GetRow(sk.as_ref(), i + 1, &view);
    ASSERT_TRUE(s.ok()) << i << s.ToString();
    TestRead(&view, value_list[i]);
  }
}

TEST_F(VersionedDeltaNodeTest, PointReadTest) {
  VersionedDeltaNodeBuilder builder;
  auto value_list = GenerateValueList(100);
  std::vector<std::shared_ptr<VersionedDeltaNode>> deltas;
  TxnTs ts = 1;
  for (const auto &value : value_list) {
    std::shared_ptr<VersionedDeltaNode> node;
    if (value.point_id % 2 == 0) {
      node = MakeDelta(value, false, ts);
    } else {
      node = MakeDelta(value, true, ts);
    }
    deltas.push_back(node);
    builder.AddDeltaNode(node.get());
  }
  auto compacted = builder.GenerateDeltaNode();
  for (const auto &value : value_list) {
    auto sk = property::SortKeys({value.point_id, value.point_type});
    RowView view;
    auto s = compacted->GetRow(sk.as_ref(), ts, &view);
    if (value.point_id % 2 == 0) {
      ASSERT_TRUE(s.ok()) << s.ToString();
      TestRead(&view, value);
    } else {
      EXPECT_TRUE(s.IsDeleted());
    }
  }
}

TEST_F(VersionedDeltaNodeTest, LockTest) {
  TxnTs ts = 1;
  ts = MarkLocked(ts);
  ValueStruct value{.point_id = 0, .point_type = 0, .value = "hello"};
  auto delta = MakeDelta(value, false, ts);
  auto sk = property::SortKeys({value.point_id, value.point_type});
  RowView view;
  EXPECT_TRUE(delta->GetRow(sk.as_ref(), ts, &view).IsRetry());
  EXPECT_TRUE(delta->SetTs(sk.as_ref(), 1).ok());
  EXPECT_TRUE(delta->GetRow(sk.as_ref(), 1, &view).ok());
  TestRead(&view, value);
}

TEST_F(VersionedDeltaNodeTest, CompactionAbortedVersionTest) {
  VersionedDeltaNodeBuilder builder;
  auto value_list = GenerateValueList(100);
  std::vector<std::shared_ptr<VersionedDeltaNode>> deltas;
  TxnTs ts = 1;
  for (const auto &value : value_list) {
    auto node = MakeDelta(value, false, 0);
    deltas.push_back(node);
    builder.AddDeltaNode(node.get());
  }
  auto compacted = builder.GenerateDeltaNode();
  for (const auto &value : value_list) {
    auto sk = property::SortKeys({value.point_id, value.point_type});
    RowView view;
    auto s = compacted->GetRow(sk.as_ref(), ts, &view);
    EXPECT_TRUE(s.IsNotFound());
  }
}

} // namespace btree
} // namespace arcanedb