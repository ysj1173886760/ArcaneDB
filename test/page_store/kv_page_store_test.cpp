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

#include "common/logger.h"
#include "page_store/kv_page_store/index_page.h"
#include "page_store/kv_page_store/kv_page_store.h"
#include "page_store/options.h"
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

TEST(kvPageStoreTest, BasicTest) {
  std::shared_ptr<PageStore> store;
  OpenOptions options;
  std::string store_name = "test_store";
  KvPageStore::Destory(store_name);
  auto s = KvPageStore::Open(store_name, options, &store);
  ASSERT_TRUE(s.ok());
  std::string page_id = "test_page001";
  WriteOptions write_options;
  ReadOptions read_options;

  {
    std::vector<std::string> binarys = {"12345", "arcanedb", "graph database"};
    for (int i = 0; i < 3; i++) {
      auto s = store->UpdateDelta(page_id, write_options, binarys[i]);
      EXPECT_TRUE(s.ok());
    }
    // read page
    std::vector<PageStore::RawPage> pages;
    auto s = store->ReadPage(page_id, read_options, &pages);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(pages.size(), 3);
    for (int i = 0; i < 3; i++) {
      EXPECT_EQ(pages[i].type, PageStore::PageType::DeltaPage);
      EXPECT_EQ(pages[i].binary, binarys[i]);
    }
  }
  {
    std::vector<std::string> binarys = {"base", "delta"};
    auto s = store->UpdateReplacement(page_id, write_options, binarys[0]);
    EXPECT_TRUE(s.ok());
    s = store->UpdateDelta(page_id, write_options, binarys[1]);
    EXPECT_TRUE(s.ok());
    std::vector<PageStore::RawPage> pages;
    s = store->ReadPage(page_id, read_options, &pages);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(pages.size(), 2);
    EXPECT_EQ(pages[0].type, PageStore::PageType::BasePage);
    EXPECT_EQ(pages[0].binary, binarys[0]);
    EXPECT_EQ(pages[1].type, PageStore::PageType::DeltaPage);
    EXPECT_EQ(pages[1].binary, binarys[1]);
  }
  {
    auto s = store->DeletePage(page_id, write_options);
    EXPECT_TRUE(s.ok());
    // read the page, should be not found
    std::vector<PageStore::RawPage> pages;
    s = store->ReadPage(page_id, read_options, &pages);
    EXPECT_EQ(s, Status::NotFound());
  }
  store.reset();
  KvPageStore::Destory(store_name);
}

} // namespace page_store
} // namespace arcanedb
