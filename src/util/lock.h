/**
 * @file lock.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-18
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "butil/macros.h"
#include "bvar/latency_recorder.h"
#include "common/logger.h"
#include "util/time.h"
#include <cassert>
#include <string>

namespace arcanedb {
namespace util {

class NamedMutexHelper {
public:
  explicit NamedMutexHelper(const std::string &name)
      : name_(name), wait_latency_name_(name + ".wait_latency"),
        hold_latency_name_(name + ".hold_latency") {}

  ~NamedMutexHelper() = default;

protected:
  const std::string name_;
  const std::string wait_latency_name_;
  const std::string hold_latency_name_;
};

template <typename Mutex> class DebugAssertionHelper {
public:
  void lock() noexcept {
    Real()->lock_();
    locked_ = true;
  }

  void unlock() noexcept {
    locked_ = false;
    Real()->unlock_();
  }

  void AssertHeld() noexcept { assert(locked_); }

protected:
  bool locked_{false};
  Mutex *Real() { return static_cast<Mutex *>(this); }
};

template <typename Mutex>
class ArcaneMutex : public NamedMutexHelper,
                    public DebugAssertionHelper<ArcaneMutex<Mutex>> {
public:
  explicit ArcaneMutex(const std::string &name) : NamedMutexHelper(name) {}

  ~ArcaneMutex() noexcept {
    // ARCANEDB_INFO("{} avg latency: {}", wait_latency_name_,
    // wait_latency_.latency()); ARCANEDB_INFO("{} max latency: {}",
    // wait_latency_name_, wait_latency_.latency()); ARCANEDB_INFO("{} avg
    // latency: {}", hold_latency_name_, hold_latency_.latency());
    // ARCANEDB_INFO("{} max latency: {}", hold_latency_name_,
    // hold_latency_.latency());
  }

  void lock_() noexcept { mu_.lock(); }

  void unlock_() noexcept { mu_.unlock(); }

  void SetHoldLatency(int64_t time) noexcept { hold_latency_ << time; }

  void SetWaitLatency(int64_t time) noexcept { wait_latency_ << time; }

  DISALLOW_COPY_AND_ASSIGN(ArcaneMutex);

private:
  Mutex mu_;
  bvar::LatencyRecorder wait_latency_;
  bvar::LatencyRecorder hold_latency_;
};

// TODO(sheep): emit the metric
template <typename Mutex> class InstrumentedLockGuard {
public:
  explicit InstrumentedLockGuard(Mutex &mu) noexcept : mu_(mu) {
    mu_.lock();
    mu_.SetWaitLatency(timer_.GetElapsed());
    timer_.Reset();
  }

  ~InstrumentedLockGuard() noexcept {
    mu_.unlock();
    mu_.SetHoldLatency(timer_.GetElapsed());
  }

  DISALLOW_COPY_AND_ASSIGN(InstrumentedLockGuard);

private:
  util::Timer timer_{};
  Mutex &mu_;
};

} // namespace util
} // namespace arcanedb