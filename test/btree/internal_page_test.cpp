/**
 * @file internal_page_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-22
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "btree/page/internal_page.h"
#include "util/bthread_util.h"
#include "util/wait_group.h"
#include <gtest/gtest.h>

namespace arcanedb {
namespace btree {

TEST(InternalPageTest, InternalRowTest) {
  InternalRows rows;
  Options opts;
  {
    // first initialization
    std::vector<InternalRow> internal_rows;
    internal_rows.push_back(
        InternalRow{.sort_key = property::SortKeys({1, 2}).GetMinSortKeys(),
                    .page_id = "a"});
    internal_rows.push_back(InternalRow{
        .sort_key = property::SortKeys({123, 456}), .page_id = "b"});
    internal_rows.push_back(InternalRow{
        .sort_key = property::SortKeys({456, 789}), .page_id = "c"});
    EXPECT_TRUE(
        rows.Split(opts, property::SortKeysRef(""), std::move(internal_rows))
            .ok());
    EXPECT_TRUE(rows.TEST_SortKeyAscending());
  }
  {
    // test read
    PageIdView view;
    EXPECT_TRUE(
        rows.GetPageId(opts, property::SortKeys({12, 34}).as_ref(), &view)
            .ok());
    EXPECT_EQ(view, "a");
    EXPECT_TRUE(
        rows.GetPageId(opts, property::SortKeys({123, 456}).as_ref(), &view)
            .ok());
    EXPECT_EQ(view, "b");
    EXPECT_TRUE(
        rows.GetPageId(opts, property::SortKeys({456, 788}).as_ref(), &view)
            .ok());
    EXPECT_EQ(view, "b");
    EXPECT_TRUE(
        rows.GetPageId(opts, property::SortKeys({456, 789}).as_ref(), &view)
            .ok());
    EXPECT_EQ(view, "c");
    EXPECT_TRUE(
        rows.GetPageId(opts, property::SortKeys({666, 789}).as_ref(), &view)
            .ok());
    EXPECT_EQ(view, "c");
  }
  {
    // test split
    std::vector<InternalRow> internal_rows;
    internal_rows.push_back(
        InternalRow{.sort_key = property::SortKeys({1, 2}), .page_id = "a0"});
    internal_rows.push_back(InternalRow{
        .sort_key = property::SortKeys({111, 111}), .page_id = "a1"});
    internal_rows.push_back(InternalRow{
        .sort_key = property::SortKeys({111, 222}), .page_id = "a2"});
    EXPECT_TRUE(rows.Split(opts,
                           property::SortKeys({1, 2}).GetMinSortKeys().as_ref(),
                           std::move(internal_rows))
                    .ok());
    EXPECT_TRUE(rows.TEST_SortKeyAscending());
  }
  {
    // test read
    PageIdView view;
    EXPECT_TRUE(
        rows.GetPageId(opts, property::SortKeys({100, 34}).as_ref(), &view)
            .ok());
    EXPECT_EQ(view, "a0");
    EXPECT_TRUE(
        rows.GetPageId(opts, property::SortKeys({111, 111}).as_ref(), &view)
            .ok());
    EXPECT_EQ(view, "a1");
    EXPECT_TRUE(
        rows.GetPageId(opts, property::SortKeys({111, 112}).as_ref(), &view)
            .ok());
    EXPECT_EQ(view, "a1");
    EXPECT_TRUE(
        rows.GetPageId(opts, property::SortKeys({111, 222}).as_ref(), &view)
            .ok());
    EXPECT_EQ(view, "a2");
    EXPECT_TRUE(
        rows.GetPageId(opts, property::SortKeys({111, 223}).as_ref(), &view)
            .ok());
    EXPECT_EQ(view, "a2");
    EXPECT_TRUE(
        rows.GetPageId(opts, property::SortKeys({123, 456}).as_ref(), &view)
            .ok());
    EXPECT_EQ(view, "b");
    EXPECT_TRUE(
        rows.GetPageId(opts, property::SortKeys({456, 788}).as_ref(), &view)
            .ok());
    EXPECT_EQ(view, "b");
    EXPECT_TRUE(
        rows.GetPageId(opts, property::SortKeys({456, 789}).as_ref(), &view)
            .ok());
    EXPECT_EQ(view, "c");
    EXPECT_TRUE(
        rows.GetPageId(opts, property::SortKeys({666, 789}).as_ref(), &view)
            .ok());
    EXPECT_EQ(view, "c");
  }
}

TEST(InternalPageTest, ConcurrentTest) {
  InternalPage page;
  Options opts;
  {
    // first initialization
    std::vector<InternalRow> internal_rows;
    internal_rows.push_back(
        InternalRow{.sort_key = property::SortKeys({1, 2}).GetMinSortKeys(),
                    .page_id = "a"});
    internal_rows.push_back(InternalRow{
        .sort_key = property::SortKeys({123, 456}), .page_id = "a"});
    internal_rows.push_back(InternalRow{
        .sort_key = property::SortKeys({456, 789}), .page_id = "a"});
    EXPECT_TRUE(
        page.Split(opts, property::SortKeysRef(""), std::move(internal_rows))
            .ok());
    EXPECT_TRUE(page.TEST_SortKeyAscending());
  }
  util::WaitGroup wg(11);
  util::LaunchAsync([&]() {
    for (int i = 10; i >= 0; i--) {
      std::vector<InternalRow> internal_rows;
      internal_rows.push_back(
          InternalRow{.sort_key = property::SortKeys({1, 2}).GetMinSortKeys(),
                      .page_id = "a"});
      internal_rows.push_back(
          InternalRow{.sort_key = property::SortKeys({i, i}), .page_id = "a"});
      EXPECT_TRUE(
          page.Split(opts, property::SortKeys({1, 2}).GetMinSortKeys().as_ref(),
                     std::move(internal_rows))
              .ok());
    }
    wg.Done();
  });
  for (int i = 0; i < 10; i++) {
    util::LaunchAsync([&]() {
      for (int j = 0; j < 10; j++) {
        EXPECT_TRUE(page.TEST_SortKeyAscending());
      }
      wg.Done();
    });
  }
  wg.Wait();
}

} // namespace btree
} // namespace arcanedb