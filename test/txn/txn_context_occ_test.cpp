/**
 * @file txn_context_occ_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-26
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "log_store/posix_log_store/posix_log_store.h"
#include "txn/txn_context_occ.h"
#include "txn/txn_manager_occ.h"
#include "util/bthread_util.h"
#include <gtest/gtest.h>

namespace arcanedb {
namespace txn {

class TxnContextOCCTest : public ::testing::Test {
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

  void TestRead(TxnContext *context, const std::string &k1,
                const ValueStruct &value, bool is_deleted) {
    auto sk = property::SortKeys({value.point_id, value.point_type});
    btree::RowView view;
    Status s;
    Options opts;
    if (context->GetTxnType() == TxnType::ReadOnlyTxn) {
      opts = opts_ro_;
    } else {
      opts = opts_;
    }
    s = context->GetRow(k1, sk.as_ref(), opts_, &view);
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

  void TestConsistentRead(TxnContext *context, const std::string &k1,
                          const ValueStruct &value1,
                          const ValueStruct &value2) {
    auto sk1 = property::SortKeys({value1.point_id, value1.point_type});
    auto sk2 = property::SortKeys({value2.point_id, value2.point_type});
    btree::RowView view1;
    Status s1 = context->GetRow(k1, sk1.as_ref(), opts_, &view1);
    btree::RowView view2;
    Status s2 = context->GetRow(k1, sk2.as_ref(), opts_, &view2);
    ASSERT_EQ(s1, s2) << s1.ToString() << " " << s2.ToString() << "\n"
                      << context->GetReadTs() << "\n"
                      << DumpHelper(k1) << "\n"
                      << "View1: " << DumpHelper(view1) << "\n"
                      << "View2: " << DumpHelper(view2) << "\n";
    if (s1.IsNotFound()) {
      return;
    }
    {
      property::ValueResult res1;
      EXPECT_TRUE(view1.at(0).GetProp(2, &res1, &schema_).ok());
      property::ValueResult res2;
      EXPECT_TRUE(view2.at(0).GetProp(2, &res2, &schema_).ok());
      ASSERT_EQ(std::get<std::string_view>(res1.value),
                std::get<std::string_view>(res2.value))
          // << txn_manager_->GetSnapshotManager()->TEST_DumpState() << "\n"
          << context->GetReadTs() << " " << view1.at(0).GetTs() << " "
          << view2.at(0).GetTs() << "\n"
          << DumpHelper(k1) << "\n"
          << "View1: " << DumpHelper(view1) << "\n"
          << "View2: " << DumpHelper(view2) << "\n";
    }
  }

  std::string DumpHelper(const std::string &k1) {
    btree::VersionedBtreePage *page;
    EXPECT_TRUE(bpm_->GetPage<btree::VersionedBtreePage>(k1, &page).ok());
    auto s = page->TEST_DumpPage();
    page->Unref();
    return s;
  }

  std::string DumpHelper(btree::RowView &view) {
    std::string res;
    for (const auto &ptr : view.GetContainer()) {
      auto delta_ptr =
          reinterpret_cast<const btree::VersionedDeltaNode *>(ptr.get());
      res += delta_ptr->TEST_DumpChain() + "\n";
    }
    return res;
  }

  void TestTsAsending(const std::string &k1) {
    btree::VersionedBtreePage *page;
    EXPECT_TRUE(bpm_->GetPage<btree::VersionedBtreePage>(k1, &page).ok());
    EXPECT_TRUE(page->TEST_TsDesending()) << DumpHelper(k1);
    page->Unref();
  }

  void SetUp() {
    schema_ = MakeTestSchema();
    opts_.schema = &schema_;
    bpm_ = std::make_unique<cache::BufferPool>();
    opts_.buffer_pool = bpm_.get();
    txn_manager_ = std::make_unique<TxnManagerOCC>();
    opts_ro_ = opts_;
    opts_ro_.ignore_lock = true;
  }

  void TearDown() {}

  Options opts_;
  Options opts_ro_;
  const int32_t type_ = 0;
  std::unique_ptr<cache::BufferPool> bpm_;
  property::Schema schema_;
  std::string table_key_ = "test_table";
  std::unique_ptr<TxnManagerOCC> txn_manager_;
};

TEST_F(TxnContextOCCTest, BasicTest) {
  auto value_list = GenerateValueList(100);
  TxnTs ts;
  {
    auto context = txn_manager_->BeginRwTxn();
    for (const auto &value : value_list) {
      EXPECT_TRUE(WriteHelper(value,
                              [&](const property::Row &row) {
                                return context->SetRow(table_key_, row, opts_);
                              })
                      .ok());
    }
    ts = context->GetWriteTs();
    EXPECT_TRUE(context->CommitOrAbort(opts_).IsCommit());
  }
  {
    auto context = txn_manager_->BeginRoTxnWithTs(ts);
    for (const auto &value : value_list) {
      TestRead(context.get(), table_key_, value, false);
    }
    EXPECT_TRUE(context->CommitOrAbort(opts_).IsCommit());
  }
  {
    auto context = txn_manager_->BeginRwTxn();
    for (const auto &value : value_list) {
      EXPECT_TRUE(WriteHelper(value,
                              [&](const property::Row &row) {
                                return context->DeleteRow(
                                    table_key_, row.GetSortKeys(), opts_);
                              })
                      .ok());
    }
    ts = context->GetWriteTs();
    EXPECT_TRUE(context->CommitOrAbort(opts_).IsCommit());
  }
  {
    auto context = txn_manager_->BeginRoTxnWithTs(ts);
    for (const auto &value : value_list) {
      TestRead(context.get(), table_key_, value, true);
    }
  }
}

TEST_F(TxnContextOCCTest, AbortTest) {
  auto value = ValueStruct{.point_id = 0, .point_type = 0, .value = "hello"};
  {
    auto context = txn_manager_->BeginRwTxn();
    EXPECT_TRUE(WriteHelper(value,
                            [&](const property::Row &row) {
                              return context->SetRow(table_key_, row, opts_);
                            })
                    .ok());
    EXPECT_TRUE(context->CommitOrAbort(opts_).IsCommit());
  }
  // scenario:
  // 1. txn a read a value
  // 2. txn b read the value, then modify it
  // 3. txn b commit
  // 4. txn a failed to commit
  auto sk = property::SortKeys({value.point_id, value.point_type});
  auto txn1 = txn_manager_->BeginRwTxn();
  auto txn2 = txn_manager_->BeginRwTxn();
  {
    btree::RowView view;
    EXPECT_TRUE(txn1->GetRow(table_key_, sk.as_ref(), opts_, &view).ok());
  }
  {
    btree::RowView view;
    EXPECT_TRUE(txn2->GetRow(table_key_, sk.as_ref(), opts_, &view).ok());
  }
  {
    auto new_value =
        ValueStruct{.point_id = 0, .point_type = 0, .value = "world"};
    EXPECT_TRUE(WriteHelper(new_value,
                            [&](const property::Row &row) {
                              return txn2->SetRow(table_key_, row, opts_);
                            })
                    .ok());
    EXPECT_TRUE(txn2->CommitOrAbort(opts_).IsCommit());
  }
  { EXPECT_TRUE(txn1->CommitOrAbort(opts_).IsAbort()); }
}

TEST_F(TxnContextOCCTest, ConcurrentTest) {
  std::vector<std::string> table_list;
  for (int i = 0; i < 10; i++) {
    table_list.push_back("test_table" + std::to_string(i));
  }
  // 10 table, 10 worker each table
  // 3 writer, 7 reader
  int worker_count = 100;
  int epoch_cnt = 20;
  util::WaitGroup wg(worker_count);
  // launch writer
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 3; j++) {
      util::LaunchAsync([&, table_index = i]() {
        for (int k = 0; k < epoch_cnt; k++) {
          auto context = txn_manager_->BeginRwTxn();
          ValueStruct value1{
              .point_id = 0, .point_type = 0, .value = std::to_string(k)};
          ValueStruct value2{
              .point_id = 1, .point_type = 0, .value = std::to_string(k)};
          EXPECT_TRUE(WriteHelper(value1,
                                  [&](const property::Row &row) {
                                    return context->SetRow(
                                        table_list[table_index], row, opts_);
                                  })
                          .ok());
          EXPECT_TRUE(WriteHelper(value2,
                                  [&](const property::Row &row) {
                                    return context->SetRow(
                                        table_list[table_index], row, opts_);
                                  })
                          .ok());
          EXPECT_TRUE(context->CommitOrAbort(opts_).IsCommit());
        }
        wg.Done();
      });
    }
  }
  // launch reader
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 7; j++) {
      util::LaunchAsync([&, table_index = i]() {
        for (int k = 0; k < epoch_cnt; k++) {
          auto context = txn_manager_->BeginRoTxn();
          ValueStruct value1{
              .point_id = 0, .point_type = 0, .value = std::to_string(k)};
          ValueStruct value2{
              .point_id = 1, .point_type = 0, .value = std::to_string(k)};
          TestConsistentRead(context.get(), table_list[table_index], value1,
                             value2);
          context->CommitOrAbort(opts_);
        }
        wg.Done();
      });
    }
  }
  wg.Wait();
  for (int i = 0; i < 10; i++) {
    TestTsAsending(table_list[i]);
  }
}

std::shared_ptr<log_store::LogStore> GenerateLogStore() {
  auto log_store_name = "txn_context_occ_log_store";
  std::shared_ptr<log_store::LogStore> store;
  log_store::Options options;
  auto s = log_store::PosixLogStore::Destory(log_store_name);
  EXPECT_EQ(s, Status::Ok());
  s = log_store::PosixLogStore::Open(log_store_name, options, &store);
  EXPECT_EQ(s, Status::Ok());
  return store;
}

TEST_F(TxnContextOCCTest, ConcurrentTestWithLog) {
  Options opts = opts_;
  std::shared_ptr<log_store::LogStore> log_store = GenerateLogStore();
  opts.log_store = log_store.get();

  std::vector<std::string> table_list;
  for (int i = 0; i < 10; i++) {
    table_list.push_back("test_table" + std::to_string(i));
  }
  // 10 table, 10 worker each table
  // 3 writer, 7 reader
  int worker_count = 100;
  int epoch_cnt = 20;
  util::WaitGroup wg(worker_count);
  // launch writer
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 3; j++) {
      util::LaunchAsync([&, table_index = i]() {
        for (int k = 0; k < epoch_cnt; k++) {
          auto context = txn_manager_->BeginRwTxn();
          ValueStruct value1{
              .point_id = 0, .point_type = 0, .value = std::to_string(k)};
          ValueStruct value2{
              .point_id = 1, .point_type = 0, .value = std::to_string(k)};
          EXPECT_TRUE(WriteHelper(value1,
                                  [&](const property::Row &row) {
                                    return context->SetRow(
                                        table_list[table_index], row, opts_);
                                  })
                          .ok());
          EXPECT_TRUE(WriteHelper(value2,
                                  [&](const property::Row &row) {
                                    return context->SetRow(
                                        table_list[table_index], row, opts_);
                                  })
                          .ok());
          EXPECT_TRUE(context->CommitOrAbort(opts_).IsCommit());
        }
        wg.Done();
      });
    }
  }
  // launch reader
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 7; j++) {
      util::LaunchAsync([&, table_index = i]() {
        for (int k = 0; k < epoch_cnt; k++) {
          auto context = txn_manager_->BeginRoTxn();
          ValueStruct value1{
              .point_id = 0, .point_type = 0, .value = std::to_string(k)};
          ValueStruct value2{
              .point_id = 1, .point_type = 0, .value = std::to_string(k)};
          TestConsistentRead(context.get(), table_list[table_index], value1,
                             value2);
          context->CommitOrAbort(opts_);
        }
        wg.Done();
      });
    }
  }
  wg.Wait();
  for (int i = 0; i < 10; i++) {
    TestTsAsending(table_list[i]);
  }
}

} // namespace txn
} // namespace arcanedb