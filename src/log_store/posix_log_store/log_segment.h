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

  void Init(size_t size, size_t index) noexcept {
    size_ = size;
    buffer_.resize(size);
    index_ = index;
  }

  /**
   * @brief
   * first returned value indicates that reservation has succeed.
   * second returned value indicates whether writer should seal current log
   * segment.
   * third returned value indicates the start lsn.
   * @param length length of this batch of log record
   * @return std::tuple<bool, bool, LsnType>
   */
  std::tuple<bool, bool, LsnType> TryReserveLogBuffer(size_t length) {
    uint64_t current_control_bits =
        control_bits_.load(std::memory_order_acquire);
    uint64_t new_control_bits;
    LsnType lsn;
    do {
      if (!IsOpen_(current_control_bits)) {
        return {false, false, kInvalidLsn};
      }
      auto current_lsn = GetLsn_(current_control_bits);
      if (length > size_) {
        LOG_WARN("LogLength: %lld is greater than total size: %lld, resize is "
                 "needed",
                 static_cast<int64_t>(length), static_cast<int64_t>(size_));
      }
      if (current_lsn + length > size_) {
        // writer should seal current log segment and open new one.
        return {false, true, kInvalidLsn};
      }
      auto current_writers = GetWriterNum_(current_control_bits);
      if (current_writers + 1 > kMaximumWriterNum) {
        // too much writers
        return {false, false, kInvalidLsn};
      }
      lsn = GetLsn_(current_control_bits);
      // CAS the new control bits
      new_control_bits = IncrWriterNum_(current_control_bits);
      new_control_bits = BumpLsn_(new_control_bits, length);
    } while (!control_bits_.compare_exchange_weak(
        current_control_bits, new_control_bits, std::memory_order_acq_rel));

    return {true, false, lsn};
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
    bool is_sealed;
    bool is_last_writer;
    do {
      is_sealed = IsSeal_(current_control_bits);
      new_control_bits = DecrWriterNum_(current_control_bits);
      is_last_writer = GetWriterNum_(new_control_bits) == 0;
    } while (!control_bits_.compare_exchange_weak(
        current_control_bits, new_control_bits, std::memory_order_acq_rel));
    if (is_last_writer && is_sealed) {
      CasState_(LogSegmentState::kSeal, LogSegmentState::kIo);
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
    buffer_.clear();
    buffer_.resize(size_);
    // set state to kfree
    CasState_(LogSegmentState::kIo, LogSegmentState::kFree);
  }

  /**
   * @brief
   * open the segment
   * @param start_lsn
   */
  void OpenLogSegment(LsnType start_lsn) noexcept {
    start_lsn_ = start_lsn;
    control_bits_.store(0, std::memory_order_release);
    CasState_(LogSegmentState::kFree, LogSegmentState::kOpen);
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
      if (!IsOpen_(current_control_bits)) {
        return std::nullopt;
      }
      if (GetLsn_(current_control_bits) == 0) {
        // don't seal when segment is empty
        return std::nullopt;
      }
      if (GetWriterNum_(current_control_bits) == 0) {
        should_schedule_io_task = true;
      }
      new_control_bits =
          SetState_(current_control_bits, LogSegmentState::kSeal);
      new_lsn = static_cast<LsnType>(GetLsn_(new_control_bits));
    } while (!control_bits_.compare_exchange_weak(
        current_control_bits, new_control_bits, std::memory_order_acq_rel));
    if (should_schedule_io_task) {
      CasState_(LogSegmentState::kSeal, LogSegmentState::kIo);
      // notify io thread
      waiter_.NotifyAll();
    }
    // resize to fit
    buffer_.resize(new_lsn);
    return new_lsn + start_lsn_;
  }

  util::NonOwnershipBufWriter GetBufWriter(LsnType start_lsn,
                                           size_t size) noexcept {
    return util::NonOwnershipBufWriter(&buffer_[start_lsn], size);
  }

  LsnType GetRealLsn(LsnType offset) noexcept { return offset + start_lsn_; }

  size_t GetIndex() const noexcept { return index_; }

  bool IsOpen() const noexcept {
    return IsOpen_(control_bits_.load(std::memory_order_acquire));
  }

  bool IsFree() const noexcept {
    return IsFree_(control_bits_.load(std::memory_order_acquire));
  }

  bool IsSeal() const noexcept {
    return IsSeal_(control_bits_.load(std::memory_order_acquire));
  }

  bool IsIo() const noexcept {
    return IsIo_(control_bits_.load(std::memory_order_acquire));
  }

private:
  FRIEND_TEST(PosixLogStoreTest, LogSegmentControlBitTest);
  friend class PosixLogStore;

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
    kFree = 0,
    kOpen = 1,
    kSeal = 2,
    kIo = 3,
  };

  static bool IsSeal_(size_t control_bits) noexcept {
    return GetState_(control_bits) == LogSegmentState::kSeal;
  }

  static bool IsFree_(size_t control_bits) noexcept {
    return GetState_(control_bits) == LogSegmentState::kFree;
  }

  static bool IsOpen_(size_t control_bits) noexcept {
    return GetState_(control_bits) == LogSegmentState::kOpen;
  }

  static bool IsIo_(size_t control_bits) noexcept {
    return GetState_(control_bits) == LogSegmentState::kIo;
  }

  static LogSegmentState GetState_(size_t control_bits) noexcept {
    return static_cast<LogSegmentState>((control_bits >> kStateOffset) &
                                        kStateMaskbit);
  }

  // could be templated to avoid one calculation.
  static size_t SetState_(size_t control_bits, LogSegmentState state) noexcept {
    auto clear_state = control_bits & (~(kStateMaskbit << kStateOffset));
    return clear_state |
           ((static_cast<uint8_t>(state) & kStateMaskbit) << kStateOffset);
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

  void CasState_(LogSegmentState current_state,
                 LogSegmentState expected_state) noexcept {
    uint64_t current_control_bits =
        control_bits_.load(std::memory_order_acquire);
    uint64_t new_control_bits;
    do {
      if (GetState_(current_control_bits) != current_state) {
        LOG_ERROR("expect state %d, get %d", current_state,
                  GetState_(current_control_bits));
        return;
      }
      new_control_bits = SetState_(current_control_bits, expected_state);
    } while (!control_bits_.compare_exchange_weak(
        current_control_bits, new_control_bits, std::memory_order_acq_rel));
  }

  static constexpr size_t kStateOffset = 62;
  static constexpr size_t kStateMaskbit = 0b11;
  static constexpr size_t kWriterNumOffset = 48;
  static constexpr size_t kWriterNumMaskbit = 0x3FFF;
  static constexpr size_t kLsnOffset = 0;
  static constexpr size_t kLsnMaskbit = (1ul << 48) - 1;
  static constexpr size_t kMaximumWriterNum = kWriterNumMaskbit;

  size_t size_{};
  LsnType start_lsn_{};
  std::string buffer_;
  /**
   * @brief
   * Control bits format:
   * | State 2bit | Writer num 14bit | LsnOffset 48bit |
   */
  // TODO: there might be a more efficient way to implement lock-free WAL.
  std::atomic<uint64_t> control_bits_{0};
  util::SimpleWaiter waiter_;
  size_t index_;
};

} // namespace log_store
} // namespace arcanedb