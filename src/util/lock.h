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

#include "butil/macros.h"
#include "bvar/latency_recorder.h"
#include "common/logger.h"
#include <cassert>
#include <string>

namespace arcanedb {
namespace util {

class NamedMutexHelper {
public:
  explicit NamedMutexHelper(const std::string &name)
      : name_(name), wait_latency_(name + ".wait_latency"),
        throughput_(name + ".throughput") {}

  ~NamedMutexHelper() = default;

private:
  const std::string name_;
  const std::string wait_latency_;
  const std::string throughput_;
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

private:
  bool locked_{false};
  Mutex *Real() { return static_cast<Mutex *>(this); }
};

template <typename Mutex>
class ArcaneMutex : public NamedMutexHelper,
                    public DebugAssertionHelper<ArcaneMutex<Mutex>> {
public:
  explicit ArcaneMutex(const std::string &name) : NamedMutexHelper(name) {}

  void lock_() noexcept { mu_.lock(); }

  void unlock_() noexcept { mu_.unlock(); }

  DISALLOW_COPY_AND_ASSIGN(ArcaneMutex);

private:
  Mutex mu_;
};

// TODO(sheep): emit the metric
template <typename Mutex> class InstrumentedLockGuard {
public:
  explicit InstrumentedLockGuard(Mutex &mu) noexcept : mu_(mu) {
    mu_.lock();
    bvar::LatencyRecorder recorder;
  }

  ~InstrumentedLockGuard() noexcept { mu_.unlock(); }

  DISALLOW_COPY_AND_ASSIGN(InstrumentedLockGuard);

private:
  Mutex &mu_;
};

} // namespace util
} // namespace arcanedb