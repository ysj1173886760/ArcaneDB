/**
 * @file sub_table_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-25
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "btree/sub_table.h"
#include "util/bthread_util.h"
#include <gtest/gtest.h>

namespace arcanedb {
namespace btree {

class SubTableTest : public ::testing::Test {
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
    Status s;
    s = func(row);
    return s;
  }

  void TestRead(SubTable *sub_table, const ValueStruct &value, TxnTs read_ts,
                bool is_deleted) {
    auto sk = property::SortKeys({value.point_id, value.point_type});
    RowView view;
    Status s;
    s = sub_table->GetRow(sk.as_ref(), read_ts, opts_, &view);
    if (is_deleted) {
      EXPECT_TRUE(s.IsNotFound());
      return;
    } else {
      EXPECT_TRUE(s.ok());
    }
    {
      property::ValueResult res;
      EXPECT_TRUE(view.at(0).GetProp(0, &res, &schema_).ok());
      EXPECT_EQ(std::get<int64_t>(res.value), value.point_id);
    }
    {
      property::ValueResult res;
      EXPECT_TRUE(view.at(0).GetProp(1, &res, &schema_).ok());
      EXPECT_EQ(std::get<int32_t>(res.value), value.point_type);
    }
    {
      property::ValueResult res;
      EXPECT_TRUE(view.at(0).GetProp(2, &res, &schema_).ok());
      EXPECT_EQ(std::get<std::string_view>(res.value), value.value);
    }
  }

  void SetUp() {
    schema_ = MakeTestSchema();
    opts_.schema = &schema_;
    bpm_ = std::make_unique<cache::BufferPool>(nullptr);
    opts_.buffer_pool = bpm_.get();
  }

  void TearDown() {}

  Options opts_;
  const int32_t type_ = 0;
  std::unique_ptr<cache::BufferPool> bpm_;
  property::Schema schema_;
  std::string table_key_ = "test_table";
};

TEST_F(SubTableTest, BasicTest) {
  std::unique_ptr<SubTable> sub_table;
  ASSERT_TRUE(SubTable::OpenSubTable(table_key_, opts_, &sub_table).ok());

  WriteInfo info;
  auto value_list = GenerateValueList(100);
  TxnTs ts = 1;
  for (const auto &value : value_list) {
    EXPECT_TRUE(WriteHelper(value, [&](const property::Row &row) {
                  return sub_table->SetRow(row, ts, opts_, &info);
                }).ok());
  }
  for (const auto &value : value_list) {
    EXPECT_TRUE(WriteHelper(value, [&](const property::Row &row) {
                  return sub_table->DeleteRow(row.GetSortKeys(), ts + 1, opts_,
                                              &info);
                }).ok());
  }
  for (const auto &value : value_list) {
    SCOPED_TRACE("");
    TestRead(sub_table.get(), value, ts, false);
  }
  for (const auto &value : value_list) {
    SCOPED_TRACE("");
    TestRead(sub_table.get(), value, ts + 1, true);
  }
  // reopen the table, could still get the same result
  std::unique_ptr<SubTable> table2;
  ASSERT_TRUE(SubTable::OpenSubTable(table_key_, opts_, &table2).ok());
  for (const auto &value : value_list) {
    SCOPED_TRACE("");
    TestRead(table2.get(), value, ts, false);
  }
  for (const auto &value : value_list) {
    SCOPED_TRACE("");
    TestRead(table2.get(), value, ts + 1, true);
  }
}

TEST_F(SubTableTest, ConcurrentTest) {
  int worker_count = 100;
  int epoch_cnt = 10;
  util::WaitGroup wg(worker_count);
  TxnTs ts = 1;
  for (int i = 0; i < worker_count; i++) {
    util::LaunchAsync([&, index = i]() {
      std::unique_ptr<SubTable> sub_table;
      ASSERT_TRUE(SubTable::OpenSubTable(table_key_, opts_, &sub_table).ok());
      ValueStruct value{
          .point_id = index, .point_type = 0, .value = std::to_string(index)};
      for (int j = 0; j < epoch_cnt; j++) {
        WriteInfo info;
        EXPECT_TRUE(WriteHelper(value, [&](const property::Row &row) {
                      return sub_table->SetRow(row, ts, opts_, &info);
                    }).ok());
        {
          SCOPED_TRACE("");
          TestRead(sub_table.get(), value, ts, false);
        }
      }
      wg.Done();
    });
  }
  wg.Wait();
}

} // namespace btree
} // namespace arcanedb