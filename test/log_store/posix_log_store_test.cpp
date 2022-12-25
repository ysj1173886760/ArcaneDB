/**
 * @file posix_log_store_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-25
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "log_store/options.h"
#include "log_store/posix_log_store/log_record.h"
#include "log_store/posix_log_store/log_segment.h"
#include "log_store/posix_log_store/posix_log_store.h"
#include "util/backoff.h"
#include "util/time.h"
#include <gtest/gtest.h>

namespace arcanedb {
namespace log_store {

TEST(PosixLogStoreTest, LogSegmentControlBitTest) {
  size_t control_bit = 0;
  EXPECT_EQ(LogSegment::GetLsn_(control_bit), 0);
  EXPECT_EQ(LogSegment::GetWriterNum_(control_bit), 0);
  EXPECT_EQ(LogSegment::IsSealed_(control_bit), false);

  // bump lsn
  control_bit = LogSegment::BumpLsn_(control_bit, 20);
  EXPECT_EQ(LogSegment::GetLsn_(control_bit), 20);
  // incr writer
  control_bit = LogSegment::IncrWriterNum_(control_bit);
  EXPECT_EQ(LogSegment::GetWriterNum_(control_bit), 1);
  // bump lsn again
  control_bit = LogSegment::BumpLsn_(control_bit, 20);
  EXPECT_EQ(LogSegment::GetLsn_(control_bit), 40);
  // incr writer
  control_bit = LogSegment::IncrWriterNum_(control_bit);
  EXPECT_EQ(LogSegment::GetWriterNum_(control_bit), 2);
  // seal
  control_bit = LogSegment::MarkSealed_(control_bit);
  EXPECT_EQ(LogSegment::IsSealed_(control_bit), true);
  // decr writer
  control_bit = LogSegment::DecrWriterNum_(control_bit);
  EXPECT_EQ(LogSegment::GetWriterNum_(control_bit), 1);
  control_bit = LogSegment::DecrWriterNum_(control_bit);
  EXPECT_EQ(LogSegment::GetWriterNum_(control_bit), 0);
}

std::shared_ptr<LogStore> GenerateLogStore(size_t segment_size = 4096) {
  auto log_store_name = "test_log_store";
  std::shared_ptr<LogStore> store;
  Options options;
  auto s = PosixLogStore::Destory(log_store_name);
  EXPECT_EQ(s, Status::Ok());
  options.segment_size = segment_size;
  s = PosixLogStore::Open(log_store_name, options, &store);
  EXPECT_EQ(s, Status::Ok());
  return store;
}

std::unique_ptr<LogReader> GetLogReader(std::shared_ptr<LogStore> store) {
  std::unique_ptr<LogReader> reader;
  auto s = store->GetLogReader(&reader);
  EXPECT_EQ(s, Status::Ok());
  return reader;
}

void WaitLsn(std::shared_ptr<LogStore> store, LsnType lsn) noexcept {
  util::BackOff bo;
  while (store->GetPersistentLsn() < lsn) {
    bo.Sleep(1 * util::MillSec);
  }
}

TEST(PosixLogStoreTest, BasicTest) {
  auto store = GenerateLogStore();
  std::vector<std::string> log_records = {"123", "456", "789"};
  std::vector<LsnRange> result;
  auto s = store->AppendLogRecord(log_records, &result);
  EXPECT_EQ(s, Status::Ok());
  EXPECT_EQ(result.size(), log_records.size());
  auto lsn = 0;
  {
    EXPECT_EQ(result[0].start_lsn, lsn);
    lsn += LogRecord::kHeaderSize + log_records[0].size();
    EXPECT_EQ(result[0].end_lsn, lsn);
  }
  {
    EXPECT_EQ(result[1].start_lsn, lsn);
    lsn += LogRecord::kHeaderSize + log_records[1].size();
    EXPECT_EQ(result[1].end_lsn, lsn);
  }
  {
    EXPECT_EQ(result[2].start_lsn, lsn);
    lsn += LogRecord::kHeaderSize + log_records[2].size();
    EXPECT_EQ(result[2].end_lsn, lsn);
  }
}

TEST(PosixLogStoreTest, LogReaderTest) {
  auto store = GenerateLogStore();
  std::vector<std::string> log_records = {"123", "456", "789"};
  std::vector<LsnRange> result;
  auto s = store->AppendLogRecord(log_records, &result);
  EXPECT_EQ(s, Status::Ok());

  // wait log records to be persisted
  WaitLsn(store, result.back().end_lsn);

  // test log reader
  auto log_reader = GetLogReader(store);
  for (int i = 0; i < 3; i++) {
    EXPECT_TRUE(log_reader->HasNext());
    std::string bytes;
    log_reader->GetNextLogRecord(&bytes);
    EXPECT_EQ(bytes, log_records[i]);
  }
  EXPECT_EQ(log_reader->HasNext(), false);
}

TEST(PosixLogStoreTest, SwitchLogSegmentTest) {
  auto store = GenerateLogStore(32);
  std::vector<std::string> log_records = {
      std::string(15, 'a'), std::string(15, 'b'), std::string(15, 'c')};
  LsnType lsn = 0;
  for (int i = 0; i < 3; i++) {
    std::vector<std::string> log(1);
    log[0] = log_records[i];
    std::vector<LsnRange> result;
    auto s = store->AppendLogRecord(log, &result);
    EXPECT_EQ(s, Status::Ok());
    lsn = std::max(lsn, result.back().end_lsn);
  }

  // wait log records to be persisted
  WaitLsn(store, lsn);

  // test log reader
  auto log_reader = GetLogReader(store);
  for (int i = 0; i < 3; i++) {
    EXPECT_TRUE(log_reader->HasNext());
    std::string bytes;
    auto lsn = log_reader->GetNextLogRecord(&bytes);
    EXPECT_EQ(bytes, log_records[i]);
  }
  EXPECT_EQ(log_reader->HasNext(), false);
}

} // namespace log_store
} // namespace arcanedb