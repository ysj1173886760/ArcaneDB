/**
 * @file bwtree_page_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-20
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "btree/page/bwtree_page.h"
#include <gtest/gtest.h>

namespace arcanedb {
namespace btree {

class BwTreePageTest : public ::testing::Test {
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

  template <typename Func>
  Status WriteHelper(const ValueStruct &value, Func func) noexcept {
    property::ValueRefVec vec;
    vec.push_back(value.point_id);
    vec.push_back(value.point_type);
    vec.push_back(value.value);
    util::BufWriter writer;
    EXPECT_TRUE(property::Row::Serialize(vec, &writer, &schema_).ok());
    auto str = writer.Detach();
    property::Row row(str.data());
    return func(row);
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

  void SetUp() {
    schema_ = MakeTestSchema();
    opts_.schema = &schema_;
  }

  void TearDown() {}

  Options opts_;
  const int32_t type_ = 0;
  property::Schema schema_;
};

TEST_F(BwTreePageTest, BasicTest) {
  BwTreePage page;
  {
    ValueStruct value{.point_id = 0, .point_type = 0, .value = "hello"};
    auto s = WriteHelper(value, [&](const property::Row &row) {
      return page.InsertRow(row, opts_);
    });
    EXPECT_TRUE(s.ok());
    property::Row row;
    auto sk = property::SortKeys({value.point_id, value.point_type});
    EXPECT_TRUE(page.GetRow(sk.as_ref(), opts_, &row).ok());
    TestRead(row, value, false);
  }
}

} // namespace btree
} // namespace arcanedb