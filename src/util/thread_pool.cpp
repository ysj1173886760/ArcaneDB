/**
 * @file thread_pool.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-11
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "util/thread_pool.h"
#include "common/logger.h"
#include <atomic>

namespace arcanedb {
namespace util {

ThreadPool::ThreadPool(std::size_t size) {
  for (size_t i = 0; i < size; i++) {
    auto ptr = std::make_unique<std::thread>(&ThreadPool::WorkerJob_, this);
    threads_.emplace_back(std::move(ptr));
  }
}

ThreadPool::~ThreadPool() {
  stop_.store(true, std::memory_order_relaxed);
  // dispatch empty task, one instance is ok.
  EmptyTask task;
  for (size_t i = 0; i < threads_.size(); i++) {
    queue_.enqueue(&task);
  }
  for (auto &thread : threads_) {
    thread->join();
  }
}

void ThreadPool::AddTask(TaskIf *task) noexcept { queue_.enqueue(task); }

void ThreadPool::WorkerJob_() noexcept {
  while (!stop_.load(std::memory_order_relaxed)) {
    TaskIf *task = nullptr;
    queue_.wait_dequeue(task);
    if (task != nullptr) {
      task->Run();
    }
  }
}

} // namespace util
} // namespace arcanedb