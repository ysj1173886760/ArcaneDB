/**
 * @file schema_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-07
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "property/schema_manager.h"
#include <gtest/gtest.h>
#include <memory>

namespace arcanedb {
namespace property {

TEST(SchemaTest, BasicTest) {
  SchemaManager schema_manager;
  {
    Column column1{
        .column_id = 0, .name = "point_id", .type = ValueType::Int64};
    Column column2{
        .column_id = 1, .name = "point_type", .type = ValueType::Int32};
    Column column3{.column_id = 2, .name = "txn_id", .type = ValueType::Int64};
    RawSchema schema{.columns = {column1, column2, column3}, .schema_id = 0};
    schema_manager.AddSchema(std::make_unique<Schema>(schema));
  }
  {
    Column column1{.column_id = 0, .name = "name", .type = ValueType::String};
    Column column2{.column_id = 1, .name = "phone", .type = ValueType::String};
    RawSchema schema{.columns = {column1, column2}, .schema_id = 1};
    schema_manager.AddSchema(std::make_unique<Schema>(schema));
  }
  {
    auto schema = schema_manager.GetSchema(10);
    EXPECT_EQ(schema, nullptr);
  }
  {
    auto schema = schema_manager.GetSchema(0);
    EXPECT_NE(schema, nullptr);
    EXPECT_EQ(schema->GetSchemaId(), 0);
    EXPECT_EQ(schema->GetColumnNum(), 3);
    EXPECT_EQ(schema->GetColumnRefByIndex(0)->column_id, 0);
    EXPECT_EQ(schema->GetColumnRefByIndex(0)->name, "point_id");
    EXPECT_EQ(schema->GetColumnRefByIndex(0)->type, ValueType::Int64);
    EXPECT_EQ(schema->GetColumnRefByIndex(1)->column_id, 1);
    EXPECT_EQ(schema->GetColumnRefByIndex(1)->name, "point_type");
    EXPECT_EQ(schema->GetColumnRefByIndex(1)->type, ValueType::Int32);
    EXPECT_EQ(schema->GetColumnRefByIndex(2)->column_id, 2);
    EXPECT_EQ(schema->GetColumnRefByIndex(2)->name, "txn_id");
    EXPECT_EQ(schema->GetColumnRefByIndex(2)->type, ValueType::Int64);

    EXPECT_EQ(schema->GetColumnRefById(0)->column_id, 0);
    EXPECT_EQ(schema->GetColumnRefById(1)->column_id, 1);
    EXPECT_EQ(schema->GetColumnRefById(2)->column_id, 2);
  }
  {
    auto schema = schema_manager.GetSchema(1);
    EXPECT_NE(schema, nullptr);
    EXPECT_EQ(schema->GetSchemaId(), 1);
    EXPECT_EQ(schema->GetColumnNum(), 2);
    EXPECT_EQ(schema->GetColumnRefByIndex(0)->column_id, 0);
    EXPECT_EQ(schema->GetColumnRefByIndex(0)->name, "name");
    EXPECT_EQ(schema->GetColumnRefByIndex(0)->type, ValueType::String);
    EXPECT_EQ(schema->GetColumnRefByIndex(1)->column_id, 1);
    EXPECT_EQ(schema->GetColumnRefByIndex(1)->name, "phone");
    EXPECT_EQ(schema->GetColumnRefByIndex(1)->type, ValueType::String);

    EXPECT_EQ(schema->GetColumnRefById(0)->column_id, 0);
    EXPECT_EQ(schema->GetColumnRefById(1)->column_id, 1);
  }
}

} // namespace property
} // namespace arcanedb