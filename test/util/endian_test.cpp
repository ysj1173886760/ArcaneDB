#include "util/codec/endian.h"
#include <gtest/gtest.h>

TEST(EndianTest, BasicTest) {
  char buffer[4];
  int32_t value = 0x01020304;
  endian::write_le(value, buffer);
  EXPECT_EQ(buffer[0], 0x04);
  EXPECT_EQ(buffer[1], 0x03);
  EXPECT_EQ(buffer[2], 0x02);
  EXPECT_EQ(buffer[3], 0x01);
  int32_t res;
  res = endian::read_le<int32_t>(buffer);
  EXPECT_EQ(res, value);

  // big endian
  endian::write_be(value, buffer);
  EXPECT_EQ(buffer[0], 0x01);
  EXPECT_EQ(buffer[1], 0x02);
  EXPECT_EQ(buffer[2], 0x03);
  EXPECT_EQ(buffer[3], 0x04);
  res = endian::read_be<int32_t>(buffer);
  EXPECT_EQ(res, value);
}