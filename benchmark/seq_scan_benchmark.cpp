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
#include "graph/weighted_graph_leveldb.h"

DEFINE_int64(concurrency, 4, "");
DEFINE_int64(point_per_thread, 100, "");
DEFINE_int64(edge_per_point, 100, "");
DEFINE_int64(iterations, 100000000, "");
DEFINE_bool(force_compaction, false, "");
DEFINE_bool(unsorted_scan, true, "");
DEFINE_int64(pthread_concurrency, 32, "");
DEFINE_string(db_type, "ArcaneDB", "ArcaneDB or LevelDB");

std::unique_ptr<arcanedb::graph::WeightedGraphDB> db;
std::unique_ptr<arcanedb::graph::LevelDBBasedWeightedGraph> leveldb_graph;

static bvar::LatencyRecorder latency_recorder;

inline int64_t GetRandom(int64_t min, int64_t max) noexcept {
  static thread_local std::random_device rd;
  static thread_local std::mt19937 generator(rd());
  std::uniform_int_distribution<int64_t> distribution(min, max);
  return distribution(generator);
}

std::atomic<bool> flag{true};

void ArcaneDBPrepareEdge() {
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

void LevelDBPrepareEdge() {
  auto value = "arcane";
  for (int i = 0; i < FLAGS_point_per_thread * FLAGS_concurrency; i++) {
    for (int j = 0; j < FLAGS_edge_per_point; j++) {
      auto s = leveldb_graph->InsertEdge(i, j, value);
      if (!s.ok()) {
        ARCANEDB_INFO("Failed to insert edge: {}", s.ToString());
      }
    }
  }
}

void RangeScan(arcanedb::graph::WeightedGraphDB::Transaction *txn, int64_t vertex_id) {
  arcanedb::graph::WeightedGraphDB::VertexId dst_id;
  arcanedb::graph::WeightedGraphDB::EdgeIterator iterator;
  txn->GetEdgeIterator(vertex_id, &iterator);
  while (iterator.Valid()) {
    // dst_id = iterator.OutVertexId();
    iterator.Next();
  }
}

void UnsortedScan(arcanedb::graph::WeightedGraphDB::Transaction *txn, int64_t vertex_id) {
  arcanedb::graph::WeightedGraphDB::VertexId dst_id;
  auto iterator = txn->GetUnsortedEdgeIterator(vertex_id);
  while (iterator.Valid()) {
    // dst_id = iterator.OutVertexId();
    iterator.Next();
  }
}

template <bool UseUnsortedScan>
void ArcaneDBWork(int idx) {
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

void LevelDBWork(int idx) noexcept {
  arcanedb::Options opts;
  opts.ignore_lock = true;
  for (int i = 0; i < FLAGS_iterations; i++) {
    for (int j = FLAGS_point_per_thread * idx; j < FLAGS_point_per_thread * (idx + 1); j++) {
      auto it = leveldb_graph->GetEdgeIterator(j);
      while (it.Valid()) {
        it.Next();
      }
    }
    latency_recorder << 1;
  }
}

void TestArcaneDB() noexcept {
  bthread_setconcurrency(FLAGS_pthread_concurrency);
  ARCANEDB_INFO("worker cnt {} ", bthread_getconcurrency());
  arcanedb::graph::WeightedGraphDB::Destroy("seq_scan_benchmark");
  auto s = arcanedb::graph::WeightedGraphDB::Open("seq_scan_benchmark", &db);
  if (!s.ok()) {
    ARCANEDB_INFO("Failed to open db");
    return;
  }
  ArcaneDBPrepareEdge();
  arcanedb::util::WaitGroup wg(FLAGS_concurrency + 1);
  std::atomic<bool> stopped(false);
  for (int i = 0; i < FLAGS_concurrency; i++) {
    arcanedb::util::LaunchAsync([&, idx = i]() {
      if (FLAGS_unsorted_scan) {
        ArcaneDBWork<true>(idx);
      } else {
        ArcaneDBWork<false>(idx);
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
      usleep(1 * arcanedb::util::Second);
    }
    wg.Done();
  });
  wg.Wait();
  thread.join();
}

void TestLevelDB() noexcept {
  arcanedb::graph::LevelDBBasedWeightedGraph::Destroy("seq_scan_benchmark");
  arcanedb::graph::LevelDBBasedWeightedGraphOptions opts;
  // 16G cache
  opts.block_cache_size = 16u << 30;
  auto s = arcanedb::graph::LevelDBBasedWeightedGraph::Open("seq_scan_benchmark", opts, &leveldb_graph);
  if (!s.ok()) {
    ARCANEDB_INFO("Failed to open db");
    return;
  }
  LevelDBPrepareEdge();
  arcanedb::util::WaitGroup wg(FLAGS_concurrency + 1);
  std::atomic<bool> stopped(false);
  for (int i = 0; i < FLAGS_concurrency; i++) {
    std::thread([&, idx = i]() {
      LevelDBWork(idx);
      wg.Done();
      stopped.store(true);
    }).detach();
  }
  auto thread = std::thread([&]() {
    while (!stopped.load()) {
      ARCANEDB_INFO("avg latency {}", latency_recorder.latency());
      ARCANEDB_INFO("max latency {}", latency_recorder.max_latency());
      ARCANEDB_INFO("qps {}", latency_recorder.qps() * FLAGS_point_per_thread);
      usleep(1 * arcanedb::util::Second);
    }
    wg.Done();
  });
  wg.Wait();
  thread.join();
}

int main(int argc, char* argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  ARCANEDB_INFO("db_type: {}", FLAGS_db_type);
  if (FLAGS_db_type == "ArcaneDB") {
    TestArcaneDB();
  } else if (FLAGS_db_type == "LevelDB") {
    TestLevelDB();
  }
  return 0;
}