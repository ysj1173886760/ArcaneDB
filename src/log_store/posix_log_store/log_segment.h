/**
 * @file log_segment.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-25
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

#include "common/logger.h"
#include "log_store/log_store.h"
#include "util/codec/buf_writer.h"
#include "util/simple_waiter.h"
#include "util/thread_pool.h"
#include <atomic>
#include <optional>

namespace arcanedb {
namespace log_store {

class LogSegment;

class ControlGuard {
public:
  ControlGuard(LogSegment *segment) : segment_(segment) {}
  ~ControlGuard() { OnExit_(); }

private:
  /**
   * @brief
   * 1. decr the writer num.
   * 2. when writer num == 0 and log segment is sealed, schedule an io task.
   */
  void OnExit_() noexcept;

  LogSegment *segment_{nullptr};
};

class LogSegment {
public:
  LogSegment() = default;

  void Init(size_t size) noexcept {
    size_ = size;
    writer_.Reset(size);
  }

  /**
   * @brief
   * return nullopt indicates writer should wait.
   * return ControlGuard indicates writer is ok to writing log records.
   * second returned value indicates whether writer should seal current log
   * segment.
   * @param length length of this batch of log record
   * @return std::pair<std::optional<ControlGuard>, bool>
   */
  std::pair<std::optional<ControlGuard>, bool>
  AcquireControlGuard(size_t length) {
    uint64_t current_control_bits =
        control_bits_.load(std::memory_order_acquire);
    uint64_t new_control_bits;
    do {
      auto current_lsn = GetLsn_(current_control_bits);
      if (length > size_) {
        LOG_WARN("LogLength: %lld is greater than total size: %lld, resize is "
                 "needed",
                 static_cast<int64_t>(length), static_cast<int64_t>(size_));
      }
      if (current_lsn + length > size_) {
        // writer should seal current log segment and open new one.
        return {std::nullopt, true};
      }
      auto current_writers = GetWriterNum_(current_control_bits);
      if (current_writers + 1 > kMaximumWriterNum) {
        // too much writers
        return {std::nullopt, false};
      }
      // CAS the new control bits
      new_control_bits = IncrWriterNum_(current_control_bits);
      new_control_bits = BumpLsn_(new_control_bits, length);
    } while (control_bits_.compare_exchange_weak(
        current_control_bits, new_control_bits, std::memory_order_acq_rel));
    return {ControlGuard(this), false};
  }

  /**
   * @brief
   * decr the writer number.
   * And notify io threads when log segment has sealed and we are the last
   * writer.
   */
  void OnWriterExit() noexcept {
    uint64_t current_control_bits =
        control_bits_.load(std::memory_order_acquire);
    uint64_t new_control_bits;
    bool should_schedule_io_task = false;
    do {
      bool is_sealed = IsSealed_(current_control_bits);
      new_control_bits = DecrWriterNum_(current_control_bits);
      bool is_last_writer = GetWriterNum_(new_control_bits);
      if (is_last_writer && is_sealed) {
        should_schedule_io_task = true;
      }
    } while (control_bits_.compare_exchange_weak(
        current_control_bits, new_control_bits, std::memory_order_acq_rel));
    if (should_schedule_io_task) {
      state_.store(LogSegmentState::kIo, std::memory_order_relaxed);
      // notify io thread
      waiter_.NotifyAll();
    }
  }

  /**
   * @brief
   * Free the log segment.
   * this function should get called by IO thread.
   */
  void FreeSegment() noexcept {
    // reset buffer
    writer_.Reset(size_);
    // set state to kfree
    state_.store(LogSegment::LogSegmentState::kFree, std::memory_order_release);
  }

  /**
   * @brief
   * open the segment
   * @param start_lsn
   */
  void OpenLogSegment(LsnType start_lsn) noexcept {
    CHECK(state_.load(std::memory_order_relaxed) == LogSegmentState::kFree);
    start_lsn_ = start_lsn;
    control_bits_.store(0, std::memory_order_release);
    state_.store(LogSegmentState::kOpen, std::memory_order_release);
    // writers must observe the state being kOpen before it can proceed to
    // appending log records.
  }

  /**
   * @brief
   * try to seal the log segment.
   * And notify io thread when seal has succeed and there is no concurrent
   * writers.
   * @return std::optional<LsnType> return nullopt indicates the seal has
   * failed. Otherwise, returns the end lsn of current log segment
   */
  std::optional<LsnType> TrySealLogSegment() noexcept {
    uint64_t current_control_bits =
        control_bits_.load(std::memory_order_acquire);
    uint64_t new_control_bits;
    LsnType new_lsn;
    bool should_schedule_io_task = false;
    do {
      if (IsSealed_(current_control_bits)) {
        return std::nullopt;
      }
      if (GetLsn_(current_control_bits) == 0) {
        // don't seal when segment is empty
        return std::nullopt;
      }
      if (GetWriterNum_(current_control_bits) == 0) {
        should_schedule_io_task = true;
      }
      new_control_bits = MarkSealed_(current_control_bits);
      new_lsn = static_cast<LsnType>(GetLsn_(new_control_bits));
    } while (control_bits_.compare_exchange_weak(
        current_control_bits, new_control_bits, std::memory_order_acq_rel));
    if (should_schedule_io_task) {
      state_.store(LogSegmentState::kIo, std::memory_order_relaxed);
      // notify io thread
      waiter_.NotifyAll();
    }
    return new_lsn + start_lsn_;
  }

private:
  FRIEND_TEST(PosixLogStoreTest, LogSegmentControlBitTest);
  friend class PosixLogStore;

  static bool IsSealed_(size_t control_bits) noexcept {
    return (control_bits >> kIsSealedOffset) & kIsSealedMaskbit;
  }

  static size_t MarkSealed_(size_t control_bits) noexcept {
    return control_bits | (1ul << kIsSealedOffset);
  }

  static size_t GetWriterNum_(size_t control_bits) noexcept {
    return (control_bits >> kWriterNumOffset) & kWriterNumMaskbit;
  }

  static size_t IncrWriterNum_(size_t control_bits) noexcept {
    return control_bits + (1ul << kWriterNumOffset);
  }

  static size_t DecrWriterNum_(size_t control_bits) noexcept {
    return control_bits - (1ul << kWriterNumOffset);
  }

  static size_t GetLsn_(size_t control_bits) noexcept {
    return control_bits & kLsnMaskbit;
  }

  static size_t BumpLsn_(size_t control_bits, size_t length) noexcept {
    return control_bits + length;
  }

  static constexpr size_t kIsSealedOffset = 63;
  static constexpr size_t kIsSealedMaskbit = 1;
  static constexpr size_t kWriterNumOffset = 48;
  static constexpr size_t kWriterNumMaskbit = 0x7FFF;
  static constexpr size_t kLsnOffset = 0;
  static constexpr size_t kLsnMaskbit = (1ul << 48) - 1;
  static constexpr size_t kMaximumWriterNum = kWriterNumMaskbit;

  /**
   * @brief
   * State change:
   * Initial state is kFree.
   * First segment is kOpen.
   * When segment is not capable of writing new logs, one of the foreground
   * threads is responsible to seal the segment. (the one who has complete the
   * CAS operation). The one who sealed the previous segment, is responsible to
   * open next log segment. note that background worker could also seal the
   * segment, preventing the latency of persisting log record being to high. The
   * last writer of sealed segment is responsible to change segment state from
   * kSeal to kIo. and schedule an io job.
   * At last, after io worker has finish it's job, it will change kIo to kFree,
   * indicating this segment is reusable again.
   */
  enum class LogSegmentState : uint8_t {
    kFree,
    kOpen,
    // kSeal, Seal state is guided by control bits.
    kIo,
  };

  std::atomic<LogSegmentState> state_{LogSegmentState::kFree};
  size_t size_{};
  LsnType start_lsn_{};
  util::BufWriter writer_;
  /**
   * @brief
   * Control bits format:
   * | IsSealed 1bit | Writer num 15bit | LsnOffset 48bit |
   */
  // TODO: there might be a more efficient way to implement lock-free WAL.
  std::atomic<uint64_t> control_bits_{0};
  util::SimpleWaiter waiter_;
};

} // namespace log_store
} // namespace arcanedb