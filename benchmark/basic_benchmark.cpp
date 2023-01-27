/**
 * @file basic_benchmark.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief 
 * @version 0.1
 * @date 2023-01-27
 * 
 * @copyright Copyright (c) 2023
 * 
 */

#include <cstdint>
#include <cstddef>
#include <limits>
#include <random>

#include "util/bthread_util.h"
#include "graph/weighted_graph.h"
#include "bvar/bvar.h"

static constexpr size_t kConcurrency = 16;
static constexpr size_t kPointPerThread = 100000;
static constexpr size_t kEdgePerPoint = 100;

static bvar::LatencyRecorder latency_recorder;

inline int64_t GetRandom(int64_t min, int64_t max) noexcept {
  static thread_local std::random_device rd;
  static thread_local std::mt19937 generator(rd());
  std::uniform_int_distribution<int64_t> distribution(min, max);
  return distribution(generator);
}

void Work() {
  std::unique_ptr<arcanedb::graph::WeightedGraphDB> db;
  auto s = arcanedb::graph::WeightedGraphDB::Open(&db);
  if (!s.ok()) {
    ARCANEDB_INFO("Failed to open db");
    return;
  }
  auto min = std::numeric_limits<int64_t>::min();
  auto max = std::numeric_limits<int64_t>::max();
  auto value = "arcane";
  arcanedb::Options opts;
  for (int i = 0; i < kPointPerThread; i++) {
    auto vertex_id = GetRandom(min, max);
    for (int j = 0; j < kEdgePerPoint; j++) {
      auto target_id = GetRandom(min, max);
      arcanedb::util::Timer timer;
      auto context = db->BeginRwTxn(opts);
      auto s = context->InsertEdge(vertex_id, target_id, value);
      if (!s.ok()) {
        ARCANEDB_INFO("Failed to insert edge");
      }
      s = context->Commit();
      if (!s.IsCommit()) {
        ARCANEDB_INFO("Failed to commit");
      }
      latency_recorder << timer.GetElapsed();
    }
  }
}

int main() {
  bthread_setconcurrency(16);
  arcanedb::util::WaitGroup wg(kConcurrency + 1);
  std::atomic<bool> stopped(false);
  for (int i = 0; i < kConcurrency; i++) {
    arcanedb::util::LaunchAsync([&]() {
      Work();
      wg.Done();
      stopped.store(true);
    });
  }
  auto thread = std::thread([&]() {
    while (!stopped.load()) {
      ARCANEDB_INFO("avg latency {}", latency_recorder.latency());
      ARCANEDB_INFO("max latency {}", latency_recorder.max_latency());
      ARCANEDB_INFO("qps {}", latency_recorder.qps());
      bthread_usleep(1 * arcanedb::util::Second);
    }
    wg.Done();
  });
  wg.Wait();
  thread.join();
  return 0;
}