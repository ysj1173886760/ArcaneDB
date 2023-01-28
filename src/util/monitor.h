/**
 * @file monitor.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-24
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "bvar/bvar.h"
#include "common/logger.h"
#include <functional>
#include <unordered_map>

namespace arcanedb {
namespace util {

#define ARCANEDB_MONITOR_LIST                                                  \
  ARCANEDB_X(AppendLog)                                                        \
  ARCANEDB_X(ReserveLogBuffer)                                                 \
  ARCANEDB_X(SerializeLog)                                                     \
  ARCANEDB_X(LogStoreRetryCnt)                                                 \
  ARCANEDB_X(SealAndOpen)

class Monitor {
public:
  Monitor() = default;

  static Monitor *GetInstance() noexcept {
    static Monitor monitor;
    return &monitor;
  }

#define ARCANEDB_X(Metric)                                                     \
  void Record##Metric##Latency(int64_t latency) noexcept {                     \
    Metric << latency;                                                         \
  }                                                                            \
  void Print##Metric##Latency() noexcept {                                     \
    ARCANEDB_INFO("{} Latency: avg latency {}, p50 latency {}, p99 latency "   \
                  "{}, max latency {}, qps {}",                                \
                  #Metric, Metric.latency(), Metric.latency_percentile(0.50),  \
                  Metric.latency_percentile(0.99), Metric.max_latency(),       \
                  Metric.qps());                                               \
  }
  ARCANEDB_MONITOR_LIST
#undef ARCANEDB_X

private:
#define ARCANEDB_X(Metric) bvar::LatencyRecorder Metric{};
  ARCANEDB_MONITOR_LIST
#undef ARCANEDB_X
};

#undef ARCANEDB_MONITOR_LIST

} // namespace util
} // namespace arcanedb