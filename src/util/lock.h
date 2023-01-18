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
#include <string>

namespace arcanedb {
namespace util {

template <typename Mutex> class NamedMutex {
public:
  explicit NamedMutex(const std::string &name)
      : name_(name), wait_latency_(name + ".wait_latency"),
        throughput_(name + ".throughput") {}

  ~NamedMutex() = default;

  void lock() noexcept { mu_.lock(); }

  void unlock() noexcept { mu_.unlock(); }

  DISALLOW_COPY_AND_ASSIGN(NamedMutex);

private:
  Mutex mu_;
  const std::string name_;
  const std::string wait_latency_;
  const std::string throughput_;
};

// TODO(sheep): emit the metric
template <typename NamedMutex> class InstrumentedLockGuard {
public:
  explicit InstrumentedLockGuard(NamedMutex &mu) noexcept : mu_(mu) {
    mu_.lock();
    bvar::LatencyRecorder recorder;
  }

  ~InstrumentedLockGuard() noexcept { mu_.unlock(); }

  DISALLOW_COPY_AND_ASSIGN(InstrumentedLockGuard);

private:
  NamedMutex &mu_;
};

} // namespace util
} // namespace arcanedb