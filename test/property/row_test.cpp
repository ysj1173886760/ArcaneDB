#include "property/row/row.h"
#include <gtest/gtest.h>

namespace arcanedb {
namespace property {

Schema MakeTestSchema() noexcept {
  Column column1{.column_id = 0, .name = "int64", .type = ValueType::Int64};
  Column column2{.column_id = 1, .name = "int32", .type = ValueType::Int32};
  Column column3{.column_id = 2, .name = "float", .type = ValueType::Float};
  Column column4{.column_id = 3, .name = "double", .type = ValueType::Double};
  Column column5{.column_id = 4, .name = "string", .type = ValueType::String};
  Column column6{.column_id = 5, .name = "bool", .type = ValueType::Bool};
  Column column7{.column_id = 6, .name = "string2", .type = ValueType::String};
  RawSchema schema{.columns = {column1, column2, column3, column4, column5,
                               column6, column7},
                   .schema_id = 0,
                   .sort_key_count = 5};
  return Schema(schema);
}

TEST(RowTest, BasicTest) {
  auto schema = MakeTestSchema();
  util::BufWriter writer;
  {
    ValueRefVec vec;
    vec.push_back(static_cast<int64_t>(1));
    vec.push_back(static_cast<int32_t>(2));
    vec.push_back(static_cast<float>(2.1));
    vec.push_back(static_cast<double>(2.2));
    vec.push_back("arcanedb");
    vec.push_back(true);
    vec.push_back("graph");
    EXPECT_TRUE(Row::Serialize(vec, &writer, &schema).ok());
  }
  auto binary = writer.Detach();
  Row row(binary.data());
  {
    ValueResult val;
    EXPECT_TRUE(row.GetProp(0, &val, &schema).ok());
    EXPECT_EQ(std::get<int64_t>(val.value), 1);
  }
  {
    ValueResult val;
    EXPECT_TRUE(row.GetProp(1, &val, &schema).ok());
    EXPECT_EQ(std::get<int32_t>(val.value), 2);
  }
  {
    ValueResult val;
    EXPECT_TRUE(row.GetProp(2, &val, &schema).ok());
    EXPECT_EQ(std::get<float>(val.value), static_cast<float>(2.1));
  }
  {
    ValueResult val;
    EXPECT_TRUE(row.GetProp(3, &val, &schema).ok());
    EXPECT_EQ(std::get<double>(val.value), 2.2);
  }
  {
    ValueResult val;
    EXPECT_TRUE(row.GetProp(4, &val, &schema).ok());
    EXPECT_EQ(std::get<std::string_view>(val.value), "arcanedb");
  }
  {
    ValueResult val;
    EXPECT_TRUE(row.GetProp(5, &val, &schema).ok());
    EXPECT_EQ(std::get<bool>(val.value), true);
  }
  {
    ValueResult val;
    EXPECT_TRUE(row.GetProp(6, &val, &schema).ok());
    EXPECT_EQ(std::get<std::string_view>(val.value), "graph");
  }
}

TEST(RowTest, CompareTest) {
  auto schema = MakeTestSchema();
  util::BufWriter writer;
  {
    ValueRefVec vec;
    vec.push_back(static_cast<int64_t>(1));
    vec.push_back(static_cast<int32_t>(2));
    vec.push_back(static_cast<float>(2.1));
    vec.push_back(static_cast<double>(2.2));
    vec.push_back("arcanedb");
    vec.push_back(true);
    vec.push_back("graph");
    EXPECT_TRUE(Row::Serialize(vec, &writer, &schema).ok());
  }
  auto binary = writer.Detach();
  Row row1(binary.data());
  {
    ValueRefVec vec;
    vec.push_back(static_cast<int64_t>(1));
    vec.push_back(static_cast<int32_t>(2));
    vec.push_back(static_cast<float>(2.1));
    vec.push_back(static_cast<double>(2.2));
    vec.push_back("boost");
    vec.push_back(true);
    vec.push_back("graph");
    EXPECT_TRUE(Row::Serialize(vec, &writer, &schema).ok());
  }
  auto binary2 = writer.Detach();
  Row row2(binary2.data());
  auto sk1 = row1.GetSortKeys();
  auto sk2 = row2.GetSortKeys();
  EXPECT_LE(row1.GetSortKeys(), row2.GetSortKeys());
}

} // namespace property
} // namespace arcanedb