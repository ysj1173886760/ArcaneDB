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

#define ARCANEDB_STATUS_LIST                                                   \
  ARCANEDB_X(Ok)                                                               \
  ARCANEDB_X(Err)

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
  Status &operator=(Status rhs) noexcept {
    std::swap(code_, rhs.code_);
    std::swap(msg_, rhs.msg_);
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

} // namespace arcanedb