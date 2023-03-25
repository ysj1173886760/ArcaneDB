/**
 * @file flusher.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-03-25
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "bthread/condition_variable.h"
#include "bthread/mutex.h"
#include "cache/buffer_pool.h"
#include "page_store/page_store.h"
#include "util/wait_group.h"

namespace arcanedb {
namespace cache {

class FlusherShard {
public:
  FlusherShard(std::shared_ptr<page_store::PageStore> page_store) noexcept
      : page_store_(std::move(page_store)) {}

  void Start() noexcept;

  bool Stop() noexcept;

  void InsertDirtyPage(BufferPool::PageHolder page_holder) noexcept;

  void ForceFlushAllPages() noexcept;

private:
  void LoopWork_() noexcept;

  bool PopDirtyPage(BufferPool::PageHolder *page_holder) noexcept;

  void FlushPage(BufferPool::PageHolder *page_holder) noexcept;

  std::deque<BufferPool::PageHolder> deque_;
  bthread::ConditionVariable cv_;
  bthread::Mutex mu_;

  util::WaitGroup wg_;
  std::atomic_bool stop_{true};

  std::shared_ptr<page_store::PageStore> page_store_;
};

class Flusher {
public:
  Flusher(size_t shard_num,
          std::shared_ptr<page_store::PageStore> page_store) noexcept {
    shards_.reserve(shard_num);
    for (int i = 0; i < shard_num; i++) {
      shards_.emplace_back(std::make_unique<FlusherShard>(page_store));
    }
  }

  void Start() noexcept;

  void Stop() noexcept;

  void TryInsertDirtyPage(const BufferPool::PageHolder &page_holder) noexcept;

  void ForceFlushAllPages() noexcept;

private:
  std::vector<std::unique_ptr<FlusherShard>> shards_;
};

} // namespace cache
} // namespace arcanedb