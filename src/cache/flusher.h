#pragma once

#include "bthread/condition_variable.h"
#include "bthread/mutex.h"
#include "cache/buffer_pool.h"
#include "util/wait_group.h"

namespace arcanedb {
namespace cache {

class Flusher {
public:
  void Start() noexcept;

  void Stop() noexcept;

  void InsertDirtyPage(const BufferPool::PageHolder &page) noexcept;

private:
  std::deque<BufferPool::PageHolder> deque_;
  bthread::ConditionVariable cv_;
  bthread::Mutex mu_;

  util::WaitGroup wg_;
  std::atomic_bool stop_{false};
};

} // namespace cache
} // namespace arcanedb