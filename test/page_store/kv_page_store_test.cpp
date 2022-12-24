/**
 * @file kv_page_store_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-24
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "page_store/kv_page_store/index_page.h"
#include "util/codec/buf_reader.h"
#include "util/codec/buf_writer.h"
#include <gtest/gtest.h>

namespace arcanedb {
namespace page_store {

TEST(KvPageStoreTest, IndexPageSerializationTest) {
  IndexPage index_page("test_page");
  index_page.UpdateReplacement();
  index_page.UpdateDelta();
  index_page.UpdateDelta();
  util::BufWriter writer;
  index_page.SerializationTo(&writer);

  {
    auto bytes = writer.Detach();
    util::BufReader reader(bytes);
    IndexPage new_page("test_page");
    EXPECT_TRUE(new_page.DeserializationFrom(&reader).ok());
    EXPECT_EQ(new_page, index_page);
  }

  // test page id not match
  {
    auto bytes = writer.Detach();
    util::BufReader reader(bytes);
    IndexPage new_page("error_page");
    EXPECT_FALSE(new_page.DeserializationFrom(&reader).ok());
  }
}

TEST(KvPageStoreTest, IndexPageUpdateTest) {
  IndexPage index_page("test_page");
  index_page.UpdateDelta();
  index_page.UpdateDelta();
  index_page.UpdateDelta();
  for (int i = 0; i < 3; i++) {
    EXPECT_EQ(index_page.pages_[i].type, PageStore::PageType::DeltaPage);
  }
  EXPECT_EQ(index_page.pages_.size(), 3);
  index_page.UpdateReplacement();
  EXPECT_EQ(index_page.pages_.size(), 1);
  EXPECT_EQ(index_page.pages_[0].type, PageStore::PageType::BasePage);
  index_page.UpdateDelta();
  EXPECT_EQ(index_page.pages_.size(), 2);
  EXPECT_EQ(index_page.pages_[1].type, PageStore::PageType::DeltaPage);
}

} // namespace page_store
} // namespace arcanedb
