/**
 * @file seq_scan_benchmark.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief 
 * @version 0.1
 * @date 2023-04-01
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
#include <type_traits>

#include "util/bthread_util.h"
#include "graph/weighted_graph.h"
#include "bvar/bvar.h"

DEFINE_int64(concurrency, 4, "");
DEFINE_int64(point_per_thread, 100, "");
DEFINE_int64(edge_per_point, 100, "");
DEFINE_int64(iterations, 100000000, "");
DEFINE_bool(force_compaction, false, "");
DEFINE_bool(unsorted_scan, true, "");

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
  opts_last.force_compaction = FLAGS_force_compaction;
  ARCANEDB_INFO("ForceCompaction: {}", FLAGS_force_compaction);
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
  for (int i = 0; i < FLAGS_point_per_thread * FLAGS_concurrency; i++) {
    for (int j = 0; j < FLAGS_edge_per_point - 1; j++) {
      insert(opts_normal, i, j);
    }
    insert(opts_last, i, FLAGS_edge_per_point - 1);
  }
}

extern std::aligned_storage<64> id_store[128];
std::aligned_storage<64> id_store[128];

void RangeScan(arcanedb::graph::WeightedGraphDB::Transaction *txn, int64_t vertex_id) {
  std::string_view value;
  // arcanedb::util::Timer timer;
  arcanedb::graph::WeightedGraphDB::EdgeIterator iterator;
  txn->GetEdgeIterator(vertex_id, &iterator);
  while (iterator.Valid()) {
    // value = iterator.EdgeValue();
    // *std::launder(reinterpret_cast<int64_t*>(&id_store[idx])) = id;
    iterator.Next();
  }
  // latency_recorder << timer.GetElapsed();
}

void UnsortedScan(arcanedb::graph::WeightedGraphDB::Transaction *txn, int64_t vertex_id) {
  std::string_view value;
  auto iterator = txn->GetUnsortedEdgeIterator(vertex_id);
  while (iterator.Valid()) {
    // value = iterator.EdgeValue();
    iterator.Next();
  }
}

template <bool UseUnsortedScan>
void Work(int idx) {
  arcanedb::Options opts;
  opts.ignore_lock = true;
  auto context = db->BeginRoTxn(opts);
  for (int i = 0; i < FLAGS_iterations; i++) {
    for (int j = FLAGS_point_per_thread * idx; j < FLAGS_point_per_thread * (idx + 1); j++) {
      if constexpr (UseUnsortedScan) {
        UnsortedScan(context.get(), j);
      } else {
        RangeScan(context.get(), j);
      }
    }
    latency_recorder << 1;
  }
  auto s = context->Commit();
}

int main(int argc, char* argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  ARCANEDB_INFO("worker cnt {} ", bthread_getconcurrency());
  auto s = arcanedb::graph::WeightedGraphDB::Open("seq_scan_benchmark", &db);
  if (!s.ok()) {
    ARCANEDB_INFO("Failed to open db");
    return 0;
  }
  PrepareEdge();
  arcanedb::util::WaitGroup wg(FLAGS_concurrency + 1);
  std::atomic<bool> stopped(false);
  for (int i = 0; i < FLAGS_concurrency; i++) {
    arcanedb::util::LaunchAsync([&, idx = i]() {
      if (FLAGS_unsorted_scan) {
        Work<true>(idx);
      } else {
        Work<false>(idx);
      }
      wg.Done();
      stopped.store(true);
    });
  }
  auto thread = std::thread([&]() {
    while (!stopped.load()) {
      ARCANEDB_INFO("avg latency {}", latency_recorder.latency());
      ARCANEDB_INFO("max latency {}", latency_recorder.max_latency());
      ARCANEDB_INFO("qps {}", latency_recorder.qps() * FLAGS_point_per_thread);
      bthread_usleep(1 * arcanedb::util::Second);
    }
    wg.Done();
  });
  wg.Wait();
  thread.join();
  return 0;
}