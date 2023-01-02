/**
 * @file cache_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-02
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "cache/cache.h"
#include "util/bthread_util.h"
#include "util/wait_group.h"
#include <gtest/gtest.h>

namespace arcanedb {
namespace cache {

TEST(CacheTest, LruMapTest) {
  auto lru_map = LruMap();
  auto alloc = [](const std::string_view &key,
                  std::unique_ptr<CacheEntry> *entry) {
    *entry = std::make_unique<CacheEntry>(key);
    return Status::Ok();
  };
  auto res = lru_map.GetEntry("k1", alloc);
  EXPECT_TRUE(res.ok());
  auto value = res.GetValue();
  EXPECT_EQ(value->GetKey(), "k1");
  EXPECT_EQ(value->IsDirty(), false);
  value->SetDirty();
  EXPECT_EQ(value->IsDirty(), true);
  EXPECT_FALSE(lru_map.TEST_CheckAllIsUndirty());
  EXPECT_FALSE(lru_map.TEST_CheckAllRefCnt());
  value->UnsetDirty();
  value->Unref();
  EXPECT_TRUE(lru_map.TEST_CheckAllIsUndirty());
  EXPECT_TRUE(lru_map.TEST_CheckAllRefCnt());

  auto s = lru_map.GetEntry("k2", nullptr);
  EXPECT_TRUE(s.IsNotFound());
  EXPECT_TRUE(lru_map.TEST_CheckAllIsUndirty());
  EXPECT_TRUE(lru_map.TEST_CheckAllRefCnt());
}

TEST(CacheTest, ConcurrentLruMapTest) {
  auto lru_map = LruMap();
  bthread::Mutex mu;
  size_t counter{0};
  auto alloc = [&](const std::string_view &key,
                   std::unique_ptr<CacheEntry> *entry) {
    *entry = std::make_unique<CacheEntry>(key);
    mu.lock();
    counter += 1;
    mu.unlock();
    return Status::Ok();
  };
  util::WaitGroup wg(10 * 100);
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 100; j++) {
      util::LaunchAsync([&, index = i]() {
        auto res = lru_map.GetEntry(std::to_string(index), alloc);
        EXPECT_TRUE(res.ok());
        auto value = res.GetValue();
        EXPECT_EQ(value->GetKey(), std::to_string(index));
        value->SetDirty();
        value->UnsetDirty();
        value->Unref();
        wg.Done();
      });
    }
  }
  wg.Wait();
  EXPECT_TRUE(lru_map.TEST_CheckAllIsUndirty());
  EXPECT_TRUE(lru_map.TEST_CheckAllRefCnt());
  EXPECT_EQ(counter, 10);
}

} // namespace cache
} // namespace arcanedb