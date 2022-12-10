/**
 * @file bthread_util.h
 * @author sheep
 * @brief utils that wraps bthread primitive.
 * @version 0.1
 * @date 2022-12-10
 *
 * @copyright Copyright (c) 2022
 *
 */
#pragma once

#include <exception>
#include <type_traits>
#include <utility>

#include "bthread/bthread.h"
#include "bthread/condition_variable.h"
#include "butil/object_pool.h"

namespace arcanedb {
namespace util {

class BthreadFutureBase {
public:

  void Wait() noexcept {
    std::unique_lock<bthread::Mutex> lock(mu_);
    while (state_ == State::Init) {
      cv_.wait(lock);
    }
  }

protected:
  // {Init} -> FunctionRun -> {Done} -> GetReturnValue -> {Invalid}
  enum class State {
    Init,
    Done,
    Invalid,
  };

  mutable bthread::Mutex mu_;
  mutable bthread::ConditionVariable cv_;
  State state_{State::Init};
};

template <typename ReturnType> class BthreadFuture : public BthreadFutureBase {

public:
  BthreadFuture() = default;
  ReturnType Get() {
    Wait();
    std::lock_guard<bthread::Mutex> lock(mu_);
    state_ = State::Invalid;
    return std::move(res_);
  }

  void Set(ReturnType &&res) {
    std::lock_guard<bthread::Mutex> lock(mu_);
    res_ = std::forward<decltype(res)>(res);
    state_ = State::Done;
    cv_.notify_all();
  }

private:
  ReturnType res_;
};

// wtf, foolish implementation.
// TODO: refer to std implementation of future and amend code here.
template <> class BthreadFuture<void> : public BthreadFutureBase {
public:
  BthreadFuture() = default;
  void Get() {
    Wait();
    std::lock_guard<bthread::Mutex> lock(mu_);
    state_ = State::Invalid;
  }

  void Set() {
    std::lock_guard<bthread::Mutex> lock(mu_);
    state_ = State::Done;
    cv_.notify_all();
  }
};

template <typename Func> class FunctionTask {
public:
  using ReturnType = std::result_of_t<Func()>;
  FunctionTask() = default;

  static FunctionTask *New(Func &&func) noexcept {
    FunctionTask *task = butil::get_object<FunctionTask>();
    task->func_ = std::forward<Func>(func);
    task->future_ = std::make_shared<BthreadFuture<ReturnType>>();
    return task;
  }

  void Run() noexcept {
    CallFuncHelper_();
    // clear task state
    future_ = nullptr;
    func_ = nullptr;
    butil::return_object(this);
  }

  std::shared_ptr<BthreadFuture<ReturnType>> GetFuture() noexcept {
    return future_;
  }

  static void *BthreadWrapper(void *args) {
    auto *task = static_cast<FunctionTask *>(args);
    task->Run();
    return nullptr;
  }

private:
  void CallFuncHelper_() {
    if constexpr (std::is_void_v<ReturnType>) {
      func_();
      future_->Set();
    } else {
      future_->Set(func_());
    }
  }

  std::function<ReturnType(void)> func_{nullptr};
  std::shared_ptr<BthreadFuture<ReturnType>> future_{nullptr};
};

template <typename Func, typename ReturnType = std::result_of_t<Func()>>
std::shared_ptr<BthreadFuture<ReturnType>> LaunchAsync(Func &&func) noexcept {
  static_assert(
      std::is_same_v<ReturnType, typename FunctionTask<Func>::ReturnType>);
  auto *task = FunctionTask<Func>::New(std::forward<decltype(func)>(func));
  auto future = task->GetFuture();
  bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  bthread_t id;
  int ret = bthread_start_background(&id, &attr,
                                     FunctionTask<Func>::BthreadWrapper, task);
  assert(ret == 0);
  return future;
}

} // namespace util
} // namespace arcanedb