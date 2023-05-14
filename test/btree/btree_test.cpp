/**
 * @file btree_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-22
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "btree/btree.h"
#include "property/schema.h"
#include "util/bthread_util.h"
#include "util/wait_group.h"
#include <gtest/gtest.h>

namespace arcanedb {
namespace btree {

class BtreeTest : public ::testing::Test {
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
    {
      util::Timer tc;
      s = func(row);
      (*write_latency_) << tc.GetElapsed();
    }
    return s;
  }

  void TestRead(const ValueStruct &value, bool is_deleted) {
    auto sk = property::SortKeys({value.point_id, value.point_type});
    RowView view;
    Status s;
    {
      util::Timer tc;
      s = btree_.GetRow(sk.as_ref(), opts_, &view);
      (*read_latency_) << tc.GetElapsed();
    }
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

  void PrintLatency() noexcept {
    ARCANEDB_INFO("read avg latency: {}, max latency: {}",
                  read_latency_->latency(), read_latency_->max_latency());
    ARCANEDB_INFO("write latency avg: {}, max latency: {}",
                  write_latency_->latency(), write_latency_->max_latency());
  }

  void SetUp() {
    schema_ = MakeTestSchema();
    opts_.schema = &schema_;
    root_page_ = std::make_unique<BtreePage>();
    btree_ = Btree(root_page_.get());
    write_latency_ = std::make_unique<bvar::LatencyRecorder>();
    read_latency_ = std::make_unique<bvar::LatencyRecorder>();
  }

  void TearDown() {}

  Options opts_;
  const int32_t type_ = 0;
  property::Schema schema_;
  std::unique_ptr<BtreePage> root_page_;
  Btree btree_{nullptr};
  std::unique_ptr<bvar::LatencyRecorder> write_latency_;
  std::unique_ptr<bvar::LatencyRecorder> read_latency_;
};

TEST_F(BtreeTest, BasicTest) {
  auto value_list = GenerateValueList(100);
  for (const auto &value : value_list) {
    EXPECT_TRUE(WriteHelper(value, [&](const property::Row &row) {
                  return btree_.SetRow(row, opts_);
                }).ok());
  }
  for (const auto &value : value_list) {
    TestRead(value, false);
  }
  for (const auto &value : value_list) {
    EXPECT_TRUE(WriteHelper(value, [&](const property::Row &row) {
                  return btree_.DeleteRow(row.GetSortKeys(), opts_);
                }).ok());
  }
  for (const auto &value : value_list) {
    TestRead(value, true);
  }
}

TEST_F(BtreeTest, ConcurrentTest) {
  int worker_count = 100;
  int epoch_cnt = 100;
  util::WaitGroup wg(worker_count);
  for (int i = 0; i < worker_count; i++) {
    util::LaunchAsync([&, index = i]() {
      ValueStruct value{
          .point_id = index, .point_type = 0, .value = std::to_string(index)};
      for (int j = 0; j < epoch_cnt; j++) {
        EXPECT_TRUE(WriteHelper(value, [&](const property::Row &row) {
                      return btree_.SetRow(row, opts_);
                    }).ok());
        TestRead(value, false);
        EXPECT_TRUE(WriteHelper(value, [&](const property::Row &row) {
                      return btree_.DeleteRow(row.GetSortKeys(), opts_);
                    }).ok());
      }
      wg.Done();
    });
  }
  wg.Wait();
  PrintLatency();
}

} // namespace btree
} // namespace arcanedb