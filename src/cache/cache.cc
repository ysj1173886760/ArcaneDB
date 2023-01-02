#include "cache/cache.h"
#include "bthread/mutex.h"

namespace arcanedb {
namespace cache {

Status LruMap::GetOrAllocEntry(const std::string &key,
                        std::shared_ptr<CacheEntry> *entry,
                        AllocFunc alloc) noexcept {
  auto cache_entry = GetFromMap_(key);
  if (!cache_entry) {
    // cache miss
    if (alloc == nullptr) {
      return Status::NotFound();
    }
    // TODO: check memory and return OOM when necessary

  }
}

Status LruMap::GetEntry(const std::string &key,
                std::shared_ptr<CacheEntry> *entry) noexcept {

}


Status LruMap::AllocEntry_(const std::string &key, std::shared_ptr<CacheEntry> *entry,
                    const AllocFunc &alloc) noexcept {
  auto cache_entry = GetFromMap_(key);
  // already loaded by others
  if (cache_entry) {
    *entry = cache_entry;
    return Status::Ok();
  }

  auto st = alloc(key, &cache_entry);
  if (!st.ok()) {
    return st;
  }
  
}

std::shared_ptr<CacheEntry> LruMap::GetFromMap_(const std::string &key) const noexcept {
  std::unique_lock<bthread::Mutex> lock_guard(map_mu_);
  auto it = map_.find(key);
  if (it == map_.end()) {
    return nullptr;
  }
  return it->second;
}

void LruMap::InsertIntoMap_(const std::shared_ptr<CacheEntry> &entry) noexcept {
  std::unique_lock<bthread::Mutex> lock_guard(map_mu_);
  auto [it, succeed] = map_.emplace(entry->GetKey(), entry);
  DCHECK(succeed);
}

void LruMap::InsertIntoList_(const std::shared_ptr<CacheEntry> &entry) noexcept {
  std::unique_lock<bthread::Mutex> lock_guard(list_mu_);
  std::unique_lock<bthread::Mutex> entry_guard(entry->mu_);
  entry->iter_ = list_.insert(list_.end(), entry);
}

bool LruMap::DelFromMap_(const std::shared_ptr<CacheEntry> &entry) noexcept {

}

bool LruMap::DelFromList_(const std::shared_ptr<CacheEntry> &entry) noexcept {

}

}
}