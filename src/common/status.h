/**
 * @file status.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-10
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

#include "butil/strings/string_piece.h"
#include "common/macros.h"
#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>

#define ARCANEDB_STATUS_LIST                                                   \
  ARCANEDB_X(Ok)                                                               \
  ARCANEDB_X(Err)                                                              \
  ARCANEDB_X(NotFound)                                                         \
  ARCANEDB_X(EndOfBuf)                                                         \
  ARCANEDB_X(PageIdNotMatch)

#define STATUS_ERROR_FUNC(name)                                                \
  static Status name() { return Status(ErrorCode::k##name); }                  \
  static Status name(std::string msg) {                                        \
    return Status(ErrorCode::k##name, std::move(msg));                         \
  }                                                                            \
  bool Is##name() const { return code_ == ErrorCode::k##name; }

namespace arcanedb {

class Status {
public:
  enum ErrorCode : uint32_t {
#define ARCANEDB_X(error_code) k##error_code,
    ARCANEDB_STATUS_LIST
#undef ARCANEDB_X
  };

  // default is succeed
  Status() = default;
  ~Status() = default;

  Status(const Status &rhs) : code_(rhs.code_) {
    if (rhs.msg_) {
      msg_ = std::make_unique<std::string>(*rhs.msg_);
    }
  }
  Status &operator=(const Status &rhs) noexcept {
    if (this == &rhs) {
      return *this;
    }
    if (rhs.msg_) {
      msg_ = std::make_unique<std::string>(*rhs.msg_);
    }
    code_ = rhs.code_;
    return *this;
  }

  Status(Status &&s) = default;
  Status &operator=(Status &&s) noexcept = default;

  bool operator==(const Status &rhs) const noexcept {
    return (code_ == rhs.code_);
  }

  bool operator!=(const Status &rhs) const noexcept { return !(*this == rhs); }

  bool ok() const noexcept { return code_ == ErrorCode::kOk; }

  butil::StringPiece GetMsg() const noexcept {
    if (msg_ == nullptr) {
      return butil::StringPiece();
    }
    return butil::StringPiece(*msg_);
  }

#define ARCANEDB_X(error_code) STATUS_ERROR_FUNC(error_code)
  ARCANEDB_STATUS_LIST
#undef ARCANEDB_X

  std::string ToString() const noexcept {
#define STATUS_ERROR_STRING(name)                                              \
  case ErrorCode::k##name:                                                     \
    type = #name ": ";                                                         \
    break;

    char tmp[30];
    const char *type;
    switch (code_) {
#define ARCANEDB_X(error_code) STATUS_ERROR_STRING(error_code)
      ARCANEDB_STATUS_LIST
#undef ARCANEDB_X
    default:
      FATAL("unknown status");
    }
    std::string result(type);
    if (msg_ != nullptr) {
      result.append(*msg_);
    }
    return result;
  }

private:
  Status(ErrorCode code) : code_(code) {}
  Status(ErrorCode code, std::string msg)
      : code_(code), msg_(std::make_unique<std::string>(std::move(msg))) {}

  ErrorCode code_{ErrorCode::kOk};
  std::unique_ptr<std::string> msg_{nullptr};
};

#define RESULT_ERROR_FUNC(name)                                                \
  static Result name() { return Result(Status::ErrorCode::k##name); }          \
  bool Is##name() const { return code_ == Status::ErrorCode::k##name; }

/**
 * @brief
 * Equivalent to Result<Status, T> in rust.
 * FIXME: Havn't support embedding error msg yet.
 * @tparam T
 */
template <typename T> class Result {
  static_assert(!std::is_reference_v<T>, "value shouldn't be a reference");
  static_assert(!std::is_const_v<T>, "value shouldn't be const qualified");

public:
  Result(T &&value)
      : code_(Status::ErrorCode::kOk), value_(std::forward<T>(value)) {}

  // TODO: support inplace constructing value

  Result(const Result &) = delete;
  Result &operator=(const Result &) = delete;

  Result(Result &&) = default;
  Result &operator=(Result &&) = default;

#define ARCANEDB_X(error_code) RESULT_ERROR_FUNC(error_code)
  ARCANEDB_STATUS_LIST
#undef ARCANEDB_X

  bool ok() const noexcept { return IsOk(); }

  const T &GetValue() const & {
    DCHECK(ok());
    return value_;
  }

  T GetValue() && {
    DCHECK(ok());
    return value_;
  }

  const T &ValueOr(const T &value) const & {
    if (ok()) {
      return value_;
    }
    return value;
  }

  T ValueOr(T &&value) && {
    if (ok()) {
      return std::move(value_);
    }
    return std::forward<T>(value);
  }

private:
  Result(Status::ErrorCode code) : code_(code) {}

  Status::ErrorCode code_;
  T value_;
};

} // namespace arcanedb