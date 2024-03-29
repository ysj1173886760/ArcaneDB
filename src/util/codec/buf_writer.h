/**
 * @file buf_writer.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-24
 *
 * @copyright Copyright (c) 2022
 *
 */
#pragma once

#include "common/macros.h"
#include <cstring>
#include <string>
#include <type_traits>
#include <variant>

namespace arcanedb {
namespace util {

class NonOwnershipBufWriter {
public:
  NonOwnershipBufWriter(char *begin, size_t size)
      : ptr_(begin), end_(ptr_ + size) {}

  template <typename... Args>
  void WriteVariant(const std::variant<Args...> &val) {
    std::visit(
        [this](auto &&arg) {
          this->WriteBytes(std::forward<decltype(arg)>(arg));
        },
        val);
  }

  // TODO(sheep): more elegant way to implement template specification.
  void WriteBytes(const std::string &val) noexcept {
    size_t s = val.size();
    CHECK(ptr_ + s <= end_);
    memcpy(ptr_, val.data(), s);
    ptr_ += s;
    offset_ += s;
  }

  void WriteBytes(const std::string_view &val) noexcept {
    size_t s = val.size();
    CHECK(ptr_ + s <= end_);
    memcpy(ptr_, val.data(), s);
    ptr_ += s;
    offset_ += s;
  }

  template <typename T> void WriteBytes(T val) noexcept {
    static_assert(std::is_pod_v<T>, "expect POD");
    size_t s = sizeof(T);
    CHECK(ptr_ + s <= end_);
    memcpy(ptr_, &val, s);
    ptr_ += s;
    offset_ += s;
  }

  size_t Offset() const noexcept { return offset_; }

private:
  char *ptr_;
  char *end_;
  size_t offset_{0};
};

class BufWriter {
  const size_t kInitialSize = 16;

public:
  BufWriter() = default;
  BufWriter(size_t initial_size) noexcept { buffer_.resize(initial_size); }

  void Reset(size_t initial_size) noexcept {
    buffer_.clear();
    buffer_.resize(initial_size);
    write_offset_ = 0;
  }

  std::string Detach() noexcept {
    buffer_.resize(write_offset_);
    write_offset_ = 0;
    auto res = std::move(buffer_);
    buffer_.clear();
    return res;
  }

  size_t Offset() const noexcept { return write_offset_; }

  template <typename... Args>
  void WriteVariant(const std::variant<Args...> &val) {
    std::visit(
        [this](auto &&arg) {
          this->WriteBytes(std::forward<decltype(arg)>(arg));
        },
        val);
  }

  void WriteBytes(const std::string &val) noexcept {
    size_t s = val.size();
    if (write_offset_ + s > buffer_.size()) {
      ResizeHelper_(write_offset_ + s);
    }
    memcpy(&buffer_[write_offset_], val.data(), s);
    write_offset_ += s;
  }

  void WriteBytes(const std::string_view &val) noexcept {
    size_t s = val.size();
    if (write_offset_ + s > buffer_.size()) {
      ResizeHelper_(write_offset_ + s);
    }
    memcpy(&buffer_[write_offset_], val.data(), s);
    write_offset_ += s;
  }

  template <typename T> void WriteBytes(T val) noexcept {
    static_assert(std::is_pod_v<T>, "expect POD");
    if (write_offset_ + sizeof(T) > buffer_.size()) {
      ResizeHelper_(write_offset_ + sizeof(T));
    }

    memcpy(&buffer_[write_offset_], &val, sizeof(T));
    write_offset_ += sizeof(T);
  }
  template <typename T> void WriteBytesAtPos(size_t pos, T val) noexcept {
    static_assert(std::is_pod_v<T>, "expect POD");
    auto size = sizeof(T);
    CHECK(pos + size < write_offset_);
    memcpy(&buffer_[pos], &val, size);
  }

  void Reserve(size_t size) noexcept {
    if (write_offset_ + size > buffer_.size()) {
      ResizeHelper_(write_offset_ + size);
    }
    write_offset_ += size;
  }

private:
  void ResizeHelper_(size_t dest_size) noexcept {
    do {
      if (buffer_.empty()) {
        buffer_.resize(kInitialSize);
      } else {
        buffer_.resize(buffer_.size() * 2);
      }
    } while (dest_size > buffer_.size());
  }

  std::string buffer_{};
  size_t write_offset_{0};
};

} // namespace util
} // namespace arcanedb