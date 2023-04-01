/**
 * @file posix_log_store.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-24
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "log_store/posix_log_store/posix_log_store.h"
#include "absl/cleanup/cleanup.h"
#include "common/config.h"
#include "log_store/log_store.h"
#include "log_store/posix_log_store/log_record.h"
#include "log_store/posix_log_store/log_segment.h"
#include "util/backoff.h"
#include "util/bthread_util.h"
#include "util/codec/buf_reader.h"
#include "util/codec/buf_writer.h"
#include "util/monitor.h"
#include "util/time.h"
#include <atomic>
#include <chrono>
#include <memory>
#include <ratio>
#include <string>

namespace arcanedb {
namespace log_store {

Status PosixLogStore::Open(const std::string &name, const Options &options,
                           std::shared_ptr<LogStore> *log_store) noexcept {
  auto store = std::make_shared<PosixLogStore>();
  store->env_ = leveldb::Env::Default();
  store->name_ = name;
  // create directory
  auto s = store->env_->CreateDir(name);
  if (!s.ok()) {
    ARCANEDB_WARN("Failed to create dir, error: {}", s.ToString());
    return Status::Err();
  }

  // create log file
  s = store->env_->NewWritableFile(MakeLogFileName_(name), &store->log_file_);
  if (!s.ok()) {
    ARCANEDB_WARN("Failed to create writable file, error: {}", s.ToString());
    return Status::Err();
  }

  // initialize log segment
  store->segment_num_ = options.segment_num;
  store->segments_ = std::make_unique<LogSegment[]>(options.segment_num);
  for (size_t i = 0; i < options.segment_num; i++) {
    store->segments_[i].Init(options.segment_size, i);
  }

  // set first log segment as open
  store->GetCurrentLogSegment_()->OpenLogSegment(0);

  // generate mfence here.
  std::atomic_thread_fence(std::memory_order_seq_cst);

  // start background thread
  store->StartBackgroundThread_();

  *log_store = store;
  return Status::Ok();
}

Status PosixLogStore::Destory(const std::string &store_name) noexcept {
  auto *env = leveldb::Env::Default();
  std::vector<std::string> filenames;
  auto s = env->GetChildren(store_name, &filenames);
  if (!s.ok()) {
    return Status::Ok();
  }
  for (const auto &name : filenames) {
    if (name != "LOG") {
      continue;
    }
    s = env->DeleteFile(store_name + '/' + name);
    if (!s.ok()) {
      ARCANEDB_WARN("Failed to remove file, status: {}", s.ToString());
      return Status::Err();
    }
  }
  s = env->DeleteDir(store_name);
  if (!s.ok()) {
    ARCANEDB_WARN("Failed to remove dir, status: {}", s.ToString());
    return Status::Err();
  }
  return Status::Ok();
}

void PosixLogStore::AppendLogRecord(const LogRecordContainer &log_records,
                                    LogResultContainer *result) noexcept {
  // util::HighResolutionTimer append_log_timer;
  // first calc the size we need to occupy
  size_t total_size = LogRecord::kHeaderSize * log_records.size();
  for (const auto &record : log_records) {
    total_size += record.size();
  }

  int cnt = 0;
  util::BackOff bo;
  do {
    // first acquire the guard
    auto *segment = GetCurrentLogSegment_();

    // util::HighResolutionTimer reserve_log_buffer_timer;
    auto [succeed, should_seal, raw_lsn] =
        segment->TryReserveLogBuffer(total_size);
    // util::Monitor::GetInstance()->RecordReserveLogBufferLatency(
    //     reserve_log_buffer_timer.GetElapsed());

    if (succeed) {
      // util::HighResolutionTimer serialize_log_timer;

      auto guard = ControlGuard(segment);
      result->clear();
      result->reserve(log_records.size());
      auto writer = segment->GetBufWriter(raw_lsn, total_size);
      // acquire succeed, performing serialization
      auto current_lsn = segment->GetRealLsn(raw_lsn);
      for (const auto &record : log_records) {
        LogRecord log_record(current_lsn, record);
        auto start_lsn = current_lsn;
        current_lsn += log_record.GetSerializeSize();
        log_record.SerializeTo(&writer);
        result->emplace_back(
            LsnRange{.start_lsn = start_lsn, .end_lsn = current_lsn});
      }

      // util::Monitor::GetInstance()->RecordSerializeLogLatency(
      //     serialize_log_timer.GetElapsed());
      // util::Monitor::GetInstance()->RecordLogStoreRetryCntLatency(cnt);
      // util::Monitor::GetInstance()->RecordAppendLogLatency(
      //     append_log_timer.GetElapsed());
      return;
    }

    // util::HighResolutionTimer seal_and_open_timer;
    // check whether we should seal
    if (should_seal && SealAndOpen(segment)) {
      // start next round immediately.
      // util::Monitor::GetInstance()->RecordSealAndOpenLatency(
      //     seal_and_open_timer.GetElapsed());
      continue;
    }
    // util::Monitor::GetInstance()->RecordSealAndOpenLatency(
    //     seal_and_open_timer.GetElapsed());

    // innodb will sleep 20 microseconds, so do we.
    bo.Sleep(20 * util::MicroSec, 1 * util::MillSec);
    cnt += 1;
  } while (true);

  return;
}

void ControlGuard::OnExit_() noexcept { segment_->OnWriterExit(); }

void PosixLogStore::ThreadJob_() noexcept {
  size_t current_io_segment = 0;
  while (!stopped_.load(std::memory_order_relaxed)) {
    auto *log_segment = GetLogSegment_(current_io_segment);
    if (log_segment->IsIo()) {
      util::Timer timer;
      auto start_lsn = log_segment->start_lsn_;
      auto data = log_segment->GetIoData();
      if (data.empty()) {
        ARCANEDB_WARN("Unhealthy data size. segment idx: {}",
                      log_segment->GetIndex());
      }

      util::Timer write_page_cache_timer;
      auto s = log_file_->Append(leveldb::Slice(data.data(), data.size()));
      util::Monitor::GetInstance()->RecordWritePageCacheLatency(
          write_page_cache_timer.GetElapsed());

      if (!s.ok()) {
        FATAL("io failed, status: {}", s.ToString());
      }

      util::Timer fsync_timer;
      s = log_file_->Sync();
      util::Monitor::GetInstance()->RecordFsyncLatency(
          fsync_timer.GetElapsed());

      if (!s.ok()) {
        FATAL("sync failed, status: {}", s.ToString());
      }
      log_segment->FreeSegment();
      // increment index
      current_io_segment = (current_io_segment + 1) % segment_num_;
      // update persistent lsn
      auto next_lsn = GetLogSegment_(current_io_segment)->start_lsn_;
      persistent_lsn_.store(next_lsn, std::memory_order_relaxed);
      if (next_lsn - start_lsn != data.size()) {
        ARCANEDB_WARN("Logical error might happens. {} {}",
                      next_lsn - start_lsn, data.size());
      }
      util::Monitor::GetInstance()->RecordIoLatencyLatency(timer.GetElapsed());
      continue;
    }
    // otherwise, we wait
    log_segment->waiter_.Wait(common::Config::kLogStoreFlushInterval);
    // recheck state
    if (!log_segment->IsIo()) {
      util::Monitor::GetInstance()->RecordSealByIoThreadLatency(1);
      SealAndOpen(log_segment);
    }
  }
}

bool PosixLogStore::SealAndOpen(LogSegment *log_segment) noexcept {
  // try to seal the segment.
  auto lsn = log_segment->TrySealLogSegment();
  if (!lsn.has_value()) {
    return false;
  }
  // open a new segment
  OpenNewLogSegment_(lsn.value());
  return true;
}

void PosixLogReader::PeekNext_() noexcept {
  auto s = file_->Read(LogRecord::kHeaderSize, &header_slice_,
                       header_buffer_.data());
  if (!s.ok()) {
    ARCANEDB_WARN("Failed to read file, status: {}", s.ToString());
    has_next_ = false;
    return;
  }
  if (header_slice_.size() < LogRecord::kHeaderSize) {
    has_next_ = false;
    return;
  }
  // parse header
  auto reader = util::BufReader(
      std::string_view(header_slice_.data(), header_slice_.size()));
  if (!reader.ReadBytes(&current_lsn_)) {
    has_next_ = false;
    return;
  }
  // there is no empty log.
  if (!reader.ReadBytes(&data_size_) || data_size_ == 0) {
    has_next_ = false;
    return;
  }
  if (data_size_ > data_buffer_.size()) {
    data_buffer_.resize(data_size_);
  }
  // parse data
  s = file_->Read(data_size_, &data_slice_, data_buffer_.data());
  if (!s.ok()) {
    ARCANEDB_WARN("Failed to read file, status: {}", s.ToString());
    has_next_ = false;
    return;
  }
  if (data_slice_.size() < data_size_) {
    has_next_ = false;
    return;
  }
  has_next_ = true;
}

bool PosixLogReader::HasNext() noexcept { return has_next_; }

LsnType PosixLogReader::GetNextLogRecord(std::string *bytes) noexcept {
  CHECK(has_next_);
  // copy data out
  *bytes = data_slice_.ToString();
  LsnType lsn = current_lsn_;
  // fetch next
  PeekNext_();
  return lsn;
}

Status
PosixLogStore::GetLogReader(std::unique_ptr<LogReader> *log_reader) noexcept {
  auto reader = std::make_unique<PosixLogReader>();
  auto s = env_->NewSequentialFile(MakeLogFileName_(name_), &reader->file_);
  if (!s.ok()) {
    ARCANEDB_WARN("Failed to open file, status: {}", s.ToString());
    return Status::Err();
  }
  reader->PeekNext_();
  *log_reader = std::move(reader);
  return Status::Ok();
}

} // namespace log_store
} // namespace arcanedb