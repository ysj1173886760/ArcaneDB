/**
 * @file cache.cc
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-02
 *
 * @copyright Copyright (c) 2023
 *
 */
#include "cache/cache.h"
#include "bthread/mutex.h"

namespace arcanedb {
namespace cache {

Result<CacheEntry *> LruMap::GetEntry(const std::string &key,
                                      AllocFunc alloc) noexcept {
  auto cache_entry = GetFromMap_(key);
  if (!cache_entry) {
    // cache miss
    if (alloc == nullptr) {
      return Status::NotFound();
    }
    // TODO: check memory and return OOM when necessary
    // TODO: swap out old pages when OOM

    // perform IO
    auto loader = [&](const std::string_view &key, CacheEntry **entry) {
      return AllocEntry_(key, entry, alloc);
    };
    auto s = single_flight_.Do(key, &cache_entry, loader);
    if (!s.ok()) {
      return s;
    }
  }
  // increase the ref cnt.
  cache_entry->Ref();
  // TODO: implement old yong lru list
  RefreshEntry(cache_entry);
  return cache_entry;
}

Status LruMap::AllocEntry_(const std::string_view &key, CacheEntry **entry,
                           const AllocFunc &alloc) noexcept {
  {
    // double check first
    auto cache_entry = GetFromMap_(key);
    if (cache_entry) {
      *entry = cache_entry;
      return Status::Ok();
    }
  }

  std::unique_ptr<CacheEntry> res{nullptr};
  auto st = alloc(key, &res);
  if (!st.ok()) {
    return st;
  }

  auto ptr = res.get();
  InsertIntoMap_(std::move(res));
  InsertIntoList_(ptr);

  *entry = ptr;
  return Status::Ok();
}

CacheEntry *LruMap::GetFromMap_(const std::string_view &key) const noexcept {
  std::unique_lock<bthread::Mutex> lock_guard(map_mu_);
  auto it = map_.find(key);
  if (it == map_.end()) {
    return nullptr;
  }
  return it->second.get();
}

void LruMap::InsertIntoMap_(std::unique_ptr<CacheEntry> entry) noexcept {
  std::unique_lock<bthread::Mutex> lock_guard(map_mu_);
  auto [it, succeed] = map_.emplace(entry->GetKey(), std::move(entry));
  DCHECK(succeed);
}

void LruMap::InsertIntoList_(CacheEntry *entry) noexcept {
  std::unique_lock<bthread::Mutex> lock_guard(list_mu_);
  std::unique_lock<bthread::Mutex> entry_guard(entry->mu_);
  entry->iter_ = list_.insert(list_.end(), entry);
  entry->in_cache_ = true;
}

bool LruMap::DelFromMap_(const CacheEntry &entry) noexcept { NOTIMPLEMENTED(); }

bool LruMap::DelFromList_(const CacheEntry &entry) noexcept {
  NOTIMPLEMENTED();
}

void LruMap::RefreshEntry(CacheEntry *entry) noexcept {
  // always lock list first, then lock entry
  std::lock_guard<bthread::Mutex> lock_guard(list_mu_);
  std::lock_guard<bthread::Mutex> entry_guard(entry->mu_);
  if (!entry->in_cache_) {
    // havn't inserted into list yet.
    // the problem is that inserting into map and list is not atomic.
    return;
  }
  list_.erase(entry->iter_);
  entry->iter_ = list_.insert(list_.end(), entry);
}

bool LruMap::TEST_CheckAllRefCnt() noexcept {
  for (const auto &[key, entry] : map_) {
    if (entry->ref_cnt_ != 1) {
      return false;
    }
  }
  return true;
}

bool LruMap::TEST_CheckAllIsUndirty() noexcept {
  for (const auto &[key, entry] : map_) {
    if (entry->is_dirty_) {
      return false;
    }
  }
  return true;
}

Result<CacheEntry *> ShardedLRU::GetEntry(const std::string &key,
                                          AllocFunc alloc) noexcept {
  auto &shard = shards_[absl::Hash<std::string>()(key) % shard_num_];
  return shard.GetEntry(key, alloc);
}

} // namespace cache
} // namespace arcanedb