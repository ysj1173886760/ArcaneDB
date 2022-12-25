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
#include "util/thread_pool.h"
#include <atomic>
#include <optional>

namespace arcanedb {
namespace log_store {

class LogSegment;

class ControlGuard {
public:
  ControlGuard(LogSegment *segment) : segment_(segment) {}

private:
  LogSegment *segment_{nullptr};
};

class LogSegment {
public:
  LogSegment(size_t size, std::shared_ptr<util::ThreadPool> thread_pool)
      : size_(size), writer_(size), thread_pool_(std::move(thread_pool)) {}

  LogSegment(LogSegment &&rhs) noexcept
      : state_(rhs.state_), size_(rhs.size_), start_lsn_(rhs.start_lsn_),
        writer_(size_),
        control_bits_(rhs.control_bits_.load(std::memory_order_relaxed)),
        thread_pool_(rhs.thread_pool_) {}

  /**
   * @brief
   * return nullopt indicates writer should wait.
   * return ControlGuard indicates writer is ok to writing log records.
   * @param length length of this batch of log record
   * @return std::optional<ControlGuard>
   */
  std::optional<ControlGuard> AcquireControlGuard(size_t length) {
    auto current_control_bits = control_bits_.load(std::memory_order_acquire);
    return std::nullopt;
  }

private:
  FRIEND_TEST(PosixLogStoreTest, LogSegmentControlBitTest);
  friend class ControlGuard;

  static bool IsSealed(size_t control_bits) noexcept {
    return (control_bits >> kIsSealedOffset) & kIsSealedMaskbit;
  }

  static size_t MarkSealed(size_t control_bits) noexcept {
    return control_bits | (1ul << kIsSealedOffset);
  }

  static size_t GetWriterNum(size_t control_bits) noexcept {
    return (control_bits >> kWriterNumOffset) & kWriterNumMaskbit;
  }

  static size_t IncrWriterNum(size_t control_bits) noexcept {
    return control_bits + (1ul << kWriterNumOffset);
  }

  static size_t DecrWriterNum(size_t control_bits) noexcept {
    return control_bits - (1ul << kWriterNumOffset);
  }

  static size_t GetLsn(size_t control_bits) noexcept {
    return control_bits & kLsnMaskbit;
  }

  static size_t BumpLsn(size_t control_bits, size_t length) noexcept {
    return control_bits + length;
  }

  static constexpr size_t kIsSealedOffset = 63;
  static constexpr size_t kIsSealedMaskbit = 1;
  static constexpr size_t kWriterNumOffset = 48;
  static constexpr size_t kWriterNumMaskbit = 0x7FFF;
  static constexpr size_t kLsnOffset = 0;
  static constexpr size_t kLsnMaskbit = (1ul << 48) - 1;

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
  enum class LogSegmentState {
    kFree,
    kOpen,
    kSeal,
    kIo,
  };

  LogSegmentState state_{LogSegmentState::kFree};
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
  std::shared_ptr<util::ThreadPool> thread_pool_{nullptr};
};

} // namespace log_store
} // namespace arcanedb