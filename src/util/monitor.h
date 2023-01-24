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
#include <unordered_map>

namespace arcanedb {
namespace util {

class Monitor {
public:
  Monitor() = default;

  Monitor *GetInstance() noexcept {
    static Monitor monitor;
    return &monitor;
  }

  void Reset() noexcept { map_.clear(); }

  void Record(const std::string &metric_name, int64_t latency) noexcept {
    map_[metric_name] << latency;
  }

  void Print() const noexcept {
    for (const auto &[k, v] : map_) {
      ARCANEDB_INFO("{}: avg latency {}, max latency {}", k, v.latency(),
                    v.max_latency());
    }
  }

private:
  std::unordered_map<std::string, bvar::LatencyRecorder> map_;
};

}
}