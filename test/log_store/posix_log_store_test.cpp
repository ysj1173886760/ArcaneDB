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

TEST(PosixLogStoreTest, BasicTest) {
  auto log_store_name = "test_log_store";
  std::shared_ptr<LogStore> store;
  Options options;
  auto s = PosixLogStore::Destory(log_store_name);
  EXPECT_EQ(s, Status::Ok());
  s = PosixLogStore::Open(log_store_name, options, &store);
  EXPECT_EQ(s, Status::Ok());

  std::vector<std::string> log_records = {"123", "456", "789"};
  std::vector<LsnRange> result;
  s = store->AppendLogRecord(log_records, &result);
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

} // namespace log_store
} // namespace arcanedb