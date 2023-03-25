/**
 * @file flusher.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-03-25
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "cache/flusher.h"
#include "util/bthread_util.h"

namespace arcanedb {
namespace cache {

void Flusher::Start() noexcept {
  for (int i = 0; i < shards_.size(); i++) {
    shards_[i].Start();
  }
}

void Flusher::Stop() noexcept {
  for (int i = 0; i < shards_.size(); i++) {
    shards_[i].Stop();
  }
}

void Flusher::TryInsertDirtyPage(
    const BufferPool::PageHolder &page_holder) noexcept {
  if (!page_holder->TryMarkInFlusher()) {
    return;
  }
  auto shard = absl::Hash<std::string_view>()(page_holder->GetPageKey()) &
               shards_.size();
  shards_[shard].InsertDirtyPage(page_holder);
}

void FlusherShard::Start() noexcept {
  bool stop = true;
  if (!stop_.compare_exchange_strong(stop, false)) {
    return;
  }

  wg_.Add(1);
  util::LaunchAsync([this]() {
    this->LoopWork_();
    wg_.Done();
  });
}

void FlusherShard::Stop() noexcept {
  bool stop = false;
  if (!stop_.compare_exchange_strong(stop, true)) {
    return;
  }

  wg_.Wait();
}

void FlusherShard::LoopWork_() noexcept {
  while (!stop_.load(std::memory_order_relaxed)) {
    BufferPool::PageHolder page_holder;
    if (PopDirtyPage(&page_holder)) {
      FlushPage(&page_holder);
    }
  }
}

void FlusherShard::FlushPage(BufferPool::PageHolder *page_holder) noexcept {
  auto snapshot = (*page_holder)->GetPageSnapshot();
  auto lsn = snapshot->GetLSN();
  auto binary = snapshot->Serialize();
  // TODO(yangshijiao): wait for log to be persisted according to WAL protocol.
  page_store::WriteOptions opts;
  auto s = page_store_->UpdateReplacement((*page_holder)->GetPageKeyRef(), opts,
                                          binary);
  bool need_flush = (*page_holder)->FinishFlush(s, lsn);
  if (need_flush) {
    InsertDirtyPage(std::move(*page_holder));
  }
}

bool FlusherShard::PopDirtyPage(BufferPool::PageHolder *page_holder) noexcept {
  std::unique_lock<decltype(mu_)> lock(mu_);
  while (deque_.empty() && !stop_) {
    cv_.wait(lock);
  }
  if (stop_) {
    return false;
  }
  *page_holder = std::move(deque_.front());
  deque_.pop_front();
  return true;
}

void FlusherShard::InsertDirtyPage(
    BufferPool::PageHolder page_holder) noexcept {
  std::lock_guard<decltype(mu_)> guard(mu_);
  deque_.emplace_back(std::move(page_holder));
}

} // namespace cache
} // namespace arcanedb