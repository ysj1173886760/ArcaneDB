/**
 * @file simple_row_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-08
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "property/property_type.h"
#include "property/row/simple_row.h"
#include "util/codec/buf_writer.h"
#include <gtest/gtest.h>
#include <string_view>

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
                   .schema_id = 0};
  return Schema(schema);
}

TEST(SimpleRowTest, BasicTest) {
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
    EXPECT_TRUE(SimpleRow::Serialize(vec, &writer, &schema).ok());
  }
  auto binary = writer.Detach();
  SimpleRow row(binary.data());
  {
    Value val;
    EXPECT_TRUE(row.GetProp(0, &val, &schema).ok());
    EXPECT_EQ(std::get<int64_t>(val), 1);
  }
  {
    Value val;
    EXPECT_TRUE(row.GetProp(1, &val, &schema).ok());
    EXPECT_EQ(std::get<int32_t>(val), 2);
  }
  {
    Value val;
    EXPECT_TRUE(row.GetProp(2, &val, &schema).ok());
    EXPECT_EQ(std::get<float>(val), static_cast<float>(2.1));
  }
  {
    Value val;
    EXPECT_TRUE(row.GetProp(3, &val, &schema).ok());
    EXPECT_EQ(std::get<double>(val), 2.2);
  }
  {
    Value val;
    EXPECT_TRUE(row.GetProp(4, &val, &schema).ok());
    EXPECT_EQ(std::get<std::string_view>(val), "arcanedb");
  }
  {
    Value val;
    EXPECT_TRUE(row.GetProp(5, &val, &schema).ok());
    EXPECT_EQ(std::get<bool>(val), true);
  }
  {
    Value val;
    EXPECT_TRUE(row.GetProp(6, &val, &schema).ok());
    EXPECT_EQ(std::get<std::string_view>(val), "graph");
  }
}

} // namespace property
} // namespace arcanedb