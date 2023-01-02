/**
 * @file singleflight.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief port from singleflight from go
 * @version 0.1
 * @date 2023-01-02
 *
 * @copyright Copyright (c) 2023
 *
 */
#pragma once

#include <string>

#include "absl/container/flat_hash_map.h"
#include "bthread/mutex.h"
#include "butil/macros.h"
#include "common/status.h"
#include "util/wait_group.h"

namespace arcanedb {
namespace util {

template <typename T, typename Key = std::string_view> class SingleFlight {
public:
  SingleFlight() = default;
  ~SingleFlight() = default;

  using LoadFunc = std::function<Status(const Key &key, T **entry)>;

  /**
   * @brief
   * SingleFlight only guarantte that loader won't get called concurrently.
   * It won't guarantee that loader will only get called once.
   * So user should make sure that loader has some double check logic to achieve
   * "exactly once" semantic.
   * @param key
   * @param entry
   * @param loader
   * @return Status
   */
  Status Do(const Key &key, T **entry, LoadFunc loader) noexcept {
    mu_.lock();
    auto it = map_.find(key);
    if (it != map_.end()) {
      auto call = it->second;
      mu_.unlock();
      call->wg.Wait();

      *entry = call->entry;
      return call->st;
    }

    // initialize control struct
    auto call = std::make_shared<Call>();
    map_.emplace(key, call);
    mu_.unlock();

    // call loader
    call->st = loader(key, &call->entry);
    call->wg.Done();

    // remove it from map
    mu_.lock();
    map_.erase(key);
    mu_.unlock();

    *entry = call->entry;
    return call->st;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(SingleFlight);

  struct Call {
    WaitGroup wg{1};
    T *entry;
    Status st;
  };

  bthread::Mutex mu_;
  absl::flat_hash_map<Key, std::shared_ptr<Call>> map_; // guarded by mu_
};

} // namespace util
} // namespace arcanedb