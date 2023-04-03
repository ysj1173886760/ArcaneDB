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
#include <gflags/gflags.h>

#include "util/bthread_util.h"
#include "graph/weighted_graph.h"
#include "bvar/bvar.h"
#include "log_store/posix_log_store/posix_log_store.h"
#include "util/monitor.h"

DEFINE_int64(concurrency, 4, "");
DEFINE_int64(pthread_concurrency, 4, "");
DEFINE_int64(point_per_thread, 1000000, "");
DEFINE_int64(edge_per_point, 100, "");
DEFINE_bool(enable_wal, false, "");
DEFINE_bool(enable_flush, false, "");
DEFINE_bool(decentralized_lock, false, "");
DEFINE_bool(inlined_lock, false, "");
DEFINE_bool(sync_commit, false, "");
DEFINE_bool(sync_log, true, "");

static bvar::LatencyRecorder latency_recorder;

inline int64_t GetRandom(int64_t min, int64_t max) noexcept {
  static thread_local std::random_device rd;
  static thread_local std::mt19937 generator(rd());
  std::uniform_int_distribution<int64_t> distribution(min, max);
  return distribution(generator);
}

static const std::string db_name = "random_write_benchmark";

void Work(arcanedb::graph::WeightedGraphDB *db) {
  auto min = std::numeric_limits<int64_t>::min();
  auto max = std::numeric_limits<int64_t>::max();
  auto value = "arcane";
  arcanedb::Options opts;
  opts.sync_commit = FLAGS_sync_commit;
  for (int i = 0; i < FLAGS_point_per_thread; i++) {
    auto vertex_id = GetRandom(0, max);
    for (int j = 0; j < FLAGS_edge_per_point; j++) {
      auto target_id = GetRandom(0, max);
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

int main(int argc, char* argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  bthread_setconcurrency(FLAGS_pthread_concurrency);
  ARCANEDB_INFO("worker cnt {} ", bthread_getconcurrency());
  arcanedb::util::WaitGroup wg(FLAGS_concurrency + 1);
  std::atomic<bool> stopped(false);
  std::unique_ptr<arcanedb::graph::WeightedGraphDB> db;
  {
    arcanedb::graph::WeightedGraphDB::Destroy(db_name);
    arcanedb::graph::WeightedGraphOptions opts;
    if (FLAGS_inlined_lock) {
      opts.lock_manager_type = arcanedb::txn::LockManagerType::kInlined;
    } else if (FLAGS_decentralized_lock) {
      opts.lock_manager_type = arcanedb::txn::LockManagerType::kDecentralized;
    } else {
      opts.lock_manager_type = arcanedb::txn::LockManagerType::kCentralized;
    }
    opts.enable_wal = FLAGS_enable_wal;
    opts.enable_flush = FLAGS_enable_flush;
    opts.sync_log = FLAGS_sync_log;
    auto s = arcanedb::graph::WeightedGraphDB::Open(db_name, &db, opts);
    if (!s.ok()) {
      ARCANEDB_INFO("Failed to open db");
      return 0;
    }
  }
  for (int i = 0; i < FLAGS_concurrency; i++) {
    arcanedb::util::LaunchAsync([&]() {
      Work(db.get());
      wg.Done();
      stopped.store(true);
    });
  }
  auto thread = std::thread([&]() {
    while (!stopped.load()) {
      ARCANEDB_INFO("avg latency {}", latency_recorder.latency());
      ARCANEDB_INFO("qps {}", latency_recorder.qps());
      arcanedb::util::Monitor::GetInstance()->PrintAppendLogLatency();
      // arcanedb::util::Monitor::GetInstance()->PrintReserveLogBufferLatency();
      // arcanedb::util::Monitor::GetInstance()->PrintSerializeLogLatency();
      // arcanedb::util::Monitor::GetInstance()->PrintLogStoreRetryCntLatency();
      // arcanedb::util::Monitor::GetInstance()->PrintSealAndOpenLatency();
      // arcanedb::util::Monitor::GetInstance()->PrintSealByIoThreadLatency();
      // arcanedb::util::Monitor::GetInstance()->PrintIoLatencyLatency();
      arcanedb::util::Monitor::GetInstance()->PrintWaitCommitLatencyLatency();
      // arcanedb::util::Monitor::GetInstance()->PrintWritePageCacheLatency();
      // arcanedb::util::Monitor::GetInstance()->PrintFsyncLatency();

      bthread_usleep(1 * arcanedb::util::Second);
    }
    wg.Done();
  });
  wg.Wait();
  thread.join();
  return 0;
}