/**
 * @file delta_node_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-19
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "btree/page/delta_node.h"
#include "property/schema.h"
#include <gtest/gtest.h>
#include <memory>

namespace arcanedb {
namespace btree {

class DeltaNodeTest : public ::testing::Test {
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

  std::shared_ptr<DeltaNode> MakeDelta(const ValueStruct &value,
                                       bool is_deleted) noexcept {
    property::ValueRefVec vec;
    vec.push_back(value.point_id);
    vec.push_back(value.point_type);
    vec.push_back(value.value);
    util::BufWriter writer;
    EXPECT_TRUE(property::Row::Serialize(vec, &writer, &schema_).ok());
    auto str = writer.Detach();
    property::Row row(str.data());
    if (is_deleted) {
      return std::make_shared<DeltaNode>(row.GetSortKeys());
    }
    return std::make_shared<DeltaNode>(row);
  }

  void TestRead(const property::Row &row, const ValueStruct &value,
                bool is_deleted) {
    {
      property::ValueResult res;
      EXPECT_TRUE(row.GetProp(0, &res, &schema_).ok());
      EXPECT_EQ(std::get<int64_t>(res.value), value.point_id);
    }
    {
      property::ValueResult res;
      EXPECT_TRUE(row.GetProp(1, &res, &schema_).ok());
      EXPECT_EQ(std::get<int32_t>(res.value), value.point_type);
    }
    if (!is_deleted) {
      property::ValueResult res;
      EXPECT_TRUE(row.GetProp(2, &res, &schema_).ok());
      EXPECT_EQ(std::get<std::string_view>(res.value), value.value);
    }
  }

  void SetUp() { schema_ = MakeTestSchema(); }

  void TearDown() {}

  const int32_t type_ = 0;
  property::Schema schema_;
};

TEST_F(DeltaNodeTest, BasicTest) {
  // read single node write and read
  {
    ValueStruct value{.point_id = 0, .point_type = 0, .value = "hello"};
    auto delta = MakeDelta(value, false);
    delta->Traverse([&](const property::Row &row, bool is_deleted) {
      TestRead(row, value, false);
    });
  }
  {
    ValueStruct value{.point_id = 0, .point_type = 0, .value = "hello"};
    auto delta = MakeDelta(value, true);
    delta->Traverse([&](const property::Row &row, bool is_deleted) {
      TestRead(row, value, true);
    });
  }
}

TEST_F(DeltaNodeTest, CompactionTest) {
  DeltaNodeBuilder builder;
  auto value_list = GenerateValueList(100);
  std::vector<std::shared_ptr<DeltaNode>> deltas;
  for (const auto &value : value_list) {
    auto node = MakeDelta(value, false);
    deltas.push_back(node);
    builder.AddDeltaNode(node.get());
  }
  auto compacted = builder.GenerateDeltaNode();
  int index = 0;
  property::SortKeysRef sk;
  compacted->Traverse([&](const property::Row &row, bool is_deleted) {
    if (index != 0) {
      EXPECT_GT(row.GetSortKeys(), sk);
    }
    sk = row.GetSortKeys();
    TestRead(row, value_list[index], false);
    index++;
  });
}

TEST_F(DeltaNodeTest, PointReadTest) {
  DeltaNodeBuilder builder;
  auto value_list = GenerateValueList(100);
  std::vector<std::shared_ptr<DeltaNode>> deltas;
  for (const auto &value : value_list) {
    std::shared_ptr<DeltaNode> node;
    if (value.point_id % 2 == 0) {
      node = MakeDelta(value, false);
    } else {
      node = MakeDelta(value, true);
    }
    deltas.push_back(node);
    builder.AddDeltaNode(node.get());
  }
  auto compacted = builder.GenerateDeltaNode();
  for (const auto &value : value_list) {
    auto sk = property::SortKeys({value.point_id, value.point_type});
    RowView view;
    auto s = compacted->GetRow(sk.as_ref(), &view);
    if (value.point_id % 2 == 0) {
      EXPECT_TRUE(s.ok());
      TestRead(view.at(0), value, false);
    } else {
      EXPECT_TRUE(s.IsDeleted());
    }
  }
}

} // namespace btree
} // namespace arcanedb