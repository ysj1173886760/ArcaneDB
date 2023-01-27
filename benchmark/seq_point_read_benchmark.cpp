/**
 * @file seq_point_read_benchmark.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief 
 * @version 0.1
 * @date 2023-01-27
 * 
 * @copyright Copyright (c) 2023
 * 
 */

#include <atomic>
#include <cstdint>
#include <cstddef>
#include <limits>
#include <random>
#include <gflags/gflags.h>

#include "util/bthread_util.h"
#include "graph/weighted_graph.h"
#include "bvar/bvar.h"

DEFINE_int64(concurrency, 4, "");
DEFINE_int64(point_num, 100, "");
DEFINE_int64(edge_per_point, 100, "");
DEFINE_int64(iterations, 100000000, "");

std::unique_ptr<arcanedb::graph::WeightedGraphDB> db;

static bvar::LatencyRecorder latency_recorder;

inline int64_t GetRandom(int64_t min, int64_t max) noexcept {
  static thread_local std::random_device rd;
  static thread_local std::mt19937 generator(rd());
  std::uniform_int_distribution<int64_t> distribution(min, max);
  return distribution(generator);
}

std::atomic<bool> flag{true};

void PrepareEdge() {
  arcanedb::Options opts_normal;
  arcanedb::Options opts_last;
  opts_last.force_compaction = true;
  auto value = "arcane";
  auto insert = [&](const arcanedb::Options &opts, int i, int j) {
    auto context = db->BeginRwTxn(opts);
    auto s = context->InsertEdge(i, j, value);
    if (!s.ok()) {
      ARCANEDB_INFO("Failed to insert edge");
    }
    s = context->Commit();
    if (!s.IsCommit()) {
      ARCANEDB_INFO("Failed to commit");
    }
  };
  for (int i = 0; i < FLAGS_point_num; i++) {
    for (int j = 0; j < FLAGS_edge_per_point - 1; j++) {
      insert(opts_normal, i, j);
    }
    insert(opts_last, i, FLAGS_edge_per_point - 1);
  }
}

void Work() {
  arcanedb::Options opts;
  opts.ignore_lock = true;
  auto min = std::numeric_limits<int64_t>::min();
  auto max = std::numeric_limits<int64_t>::max();
  auto context = db->BeginRoTxn(opts);
  for (int i = 0; i < FLAGS_iterations; i++) {
    for (int j = 0; j < FLAGS_point_num; j++) {
      for (int k = 0; k < FLAGS_edge_per_point; k++) {
        std::string res;
        auto s = context->GetEdge(j, k, &res);
      }
      if (j == 0) {
        latency_recorder << 1;
      }
    }
  }
  auto s = context->Commit();
}

int main(int argc, char* argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  bthread_setconcurrency(4);
  auto s = arcanedb::graph::WeightedGraphDB::Open(&db);
  if (!s.ok()) {
    ARCANEDB_INFO("Failed to open db");
    return 0;
  }
  PrepareEdge();
  arcanedb::util::WaitGroup wg(FLAGS_concurrency + 1);
  std::atomic<bool> stopped(false);
  for (int i = 0; i < FLAGS_concurrency; i++) {
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
      ARCANEDB_INFO("qps {}", latency_recorder.qps() * FLAGS_edge_per_point * FLAGS_point_num);
      bthread_usleep(1 * arcanedb::util::Second);
    }
    wg.Done();
  });
  wg.Wait();
  thread.join();
  return 0;
}