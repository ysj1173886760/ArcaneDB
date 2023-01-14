#include <gtest/gtest.h>

#include "property/sort_key/sort_key_encoding.h"

namespace arcanedb {
namespace property {

template <typename T> void WriteHelper(std::string *s, const T &value) {
  if constexpr (std::is_same_v<T, int32_t>) {
    PutComparableFixed32(s, value);
  } else if constexpr (std::is_same_v<T, int64_t>) {
    PutComparableFixed64(s, value);
  } else if constexpr (std::is_same_v<T, float>) {
    PutComparableFixedF32(s, value);
  } else if constexpr (std::is_same_v<T, double>) {
    PutComparableFixedF64(s, value);
  } else if constexpr (std::is_same_v<T, uint16_t>) {
    PutComparableFixedU16(s, value);
  } else if constexpr (std::is_same_v<T, uint32_t>) {
    PutComparableFixedU32(s, value);
  } else if constexpr (std::is_same_v<T, uint64_t>) {
    PutComparableFixedU64(s, value);
  } else if constexpr (std::is_same_v<T, uint8_t>) {
    PutComparableFixedU8(s, value);
  } else if constexpr (std::is_same_v<T, bool>) {
    PutComparableBoolAsFixedU8(s, value);
  } else if constexpr (std::is_same_v<T, std::string>) {
    PutComparableString(s, value);
  }
}

template <typename T> T ReadHelper(std::string_view *sp) {
  T tmp;
  if constexpr (std::is_same_v<T, int32_t>) {
    GetComparableFixed32(sp, &tmp);
  } else if constexpr (std::is_same_v<T, int64_t>) {
    GetComparableFixed64(sp, &tmp);
  } else if constexpr (std::is_same_v<T, float>) {
    GetComparableFixedF32(sp, &tmp);
  } else if constexpr (std::is_same_v<T, double>) {
    GetComparableFixedF64(sp, &tmp);
  } else if constexpr (std::is_same_v<T, uint16_t>) {
    GetComparableFixedU16(sp, &tmp);
  } else if constexpr (std::is_same_v<T, uint32_t>) {
    GetComparableFixedU32(sp, &tmp);
  } else if constexpr (std::is_same_v<T, uint64_t>) {
    GetComparableFixedU64(sp, &tmp);
  } else if constexpr (std::is_same_v<T, uint8_t>) {
    GetComparableFixedU8(sp, &tmp);
  } else if constexpr (std::is_same_v<T, bool>) {
    GetComparableFixedU8AsBool(sp, &tmp);
  } else if constexpr (std::is_same_v<T, std::string>) {
    GetComparableString(sp, &tmp);
  }
  return tmp;
}

// test read, write, then memcmp
template <typename T> void TestHelper(const T &lhs, const T &rhs) {
  std::string s1;
  std::string s2;
  WriteHelper(&s1, lhs);
  WriteHelper(&s2, rhs);
  EXPECT_LE(s1, s2);
  std::string_view view1(s1);
  std::string_view view2(s2);
  auto t1 = ReadHelper<T>(&view1);
  auto t2 = ReadHelper<T>(&view2);
  EXPECT_EQ(t1, lhs);
  EXPECT_EQ(t2, rhs);
}

TEST(SortOrderEncodingTest, Int32Test) { TestHelper<int32_t>(10, 20); }
TEST(SortOrderEncodingTest, Int64Test) { TestHelper<int64_t>(10, 20); }
TEST(SortOrderEncodingTest, FloatTest) { TestHelper<float>(1.1, 2.2); }
TEST(SortOrderEncodingTest, DoubleTest) { TestHelper<double>(3.3, 4.4); }
TEST(SortOrderEncodingTest, Uint16Test) { TestHelper<uint16_t>(10, 20); }
TEST(SortOrderEncodingTest, Uint32Test) { TestHelper<uint32_t>(10, 20); }
TEST(SortOrderEncodingTest, Uint64Test) { TestHelper<uint64_t>(10, 20); }
TEST(SortOrderEncodingTest, BoolTest) { TestHelper<bool>(false, true); }
TEST(SortOrderEncodingTest, StringTest) {
  TestHelper<std::string>("1123", "1223");
}

} // namespace property
} // namespace arcanedb