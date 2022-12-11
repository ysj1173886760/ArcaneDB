/**
 * @file thread_pool.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief simple thread pool
 * @version 0.1
 * @date 2022-12-11
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "util/concurrent_queue/blockingconcurrentqueue.h"

namespace arcanedb {
namespace util {

class TaskIf {
public:
  TaskIf() = default;
  virtual ~TaskIf() = default;
  virtual void Run() = 0;
};

/**
 * @brief
 * since we are using blocking queue inside thread pool. we need some kind of
 * mechanism to wake workers up.
 * EmptyTask is used to notify worker that we are entering the destructing
 * phase.
 */
class EmptyTask : public TaskIf {
public:
  void Run() override {}
};

class ThreadPool {
public:
  ThreadPool(std::size_t size);
  ~ThreadPool();

  void AddTask(TaskIf *task) noexcept;

private:
  void WorkerJob_() noexcept;

  std::vector<std::unique_ptr<std::thread>> threads_;
  std::atomic_bool stop_{false};
  moodycamel::BlockingConcurrentQueue<TaskIf *> queue_;
};

} // namespace util
} // namespace arcanedb