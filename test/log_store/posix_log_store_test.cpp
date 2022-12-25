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

} // namespace log_store
} // namespace arcanedb