/**
 * @file buf_reader_writer_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-24
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "butil/strings/string_piece.h"
#include "util/codec/buf_reader.h"
#include "util/codec/buf_writer.h"
#include <gtest/gtest.h>
#include <string_view>

namespace arcanedb {
namespace util {

TEST(BufReaderWriterTest, BasicTest) {
  std::string s = "arcanedb";
  BufWriter writer;
  // write some pods, then string
  EXPECT_EQ(writer.Offset(), 0);
  writer.WriteBytes(static_cast<uint16_t>(10));
  EXPECT_EQ(writer.Offset(), 2);
  writer.WriteBytes(static_cast<int32_t>(-1));
  EXPECT_EQ(writer.Offset(), 6);
  writer.WriteBytes(static_cast<int64_t>(123456789));
  EXPECT_EQ(writer.Offset(), 14);
  writer.WriteBytes(static_cast<uint32_t>(s.size()));
  EXPECT_EQ(writer.Offset(), 18);
  writer.WriteBytes(s);
  EXPECT_EQ(writer.Offset(), 18 + s.size());
  auto bytes = writer.Detach();

  BufReader reader(bytes);
  {
    uint16_t val;
    EXPECT_TRUE(reader.ReadBytes(&val));
    EXPECT_EQ(val, 10);
  }
  {
    int32_t val;
    EXPECT_TRUE(reader.ReadBytes(&val));
    EXPECT_EQ(val, -1);
  }
  {
    int64_t val;
    EXPECT_TRUE(reader.ReadBytes(&val));
    EXPECT_EQ(val, 123456789);
  }
  {
    uint32_t val;
    EXPECT_TRUE(reader.ReadBytes(&val));
    EXPECT_EQ(val, s.size());
    std::string_view res;
    EXPECT_TRUE(reader.ReadPiece(&res, val));
    EXPECT_EQ(res, s);
  }
}

} // namespace util
} // namespace arcanedb