#pragma once

#include <cstddef>
#include <string_view>

namespace arcanedb {
namespace util {

// TODO: distinguish big endian and small endian buf reader.
/**
 * @brief
 * buf reader based on machine arch
 */
class BufReader {
public:
  BufReader(std::string_view buffer)
      : ptr_(buffer.data()), begin_(ptr_), end_(begin_ + buffer.size()) {}

  size_t Remaining() const noexcept { return end_ - ptr_; }
  size_t Offset() const noexcept { return ptr_ - begin_; }

  bool Skip(size_t len) noexcept {
    if (ptr_ + len > end_) {
      return false;
    }
    ptr_ += len;
    return true;
  }

  template <typename T> bool ReadBytes(T *value) noexcept {
    static_assert(std::is_pod_v<T>, "not a pod type");
    static_assert(!std::is_same_v<T, bool>, "unsafe to read a bool");
    return ReadHelper_(value);
  }

  bool ReadPiece(std::string_view *out, size_t len) noexcept {
    if (ptr_ + len > end_) {
      return false;
    }
    *out = std::string_view(ptr_, len);
    ptr_ += len;
    return true;
  }

private:
  template <typename T> bool ReadHelper_(T *v) noexcept {
    if (ptr_ + sizeof(T) > end_) {
      return false;
    }
    memcpy(reinterpret_cast<char *>(v), ptr_, sizeof(T));
    ptr_ += sizeof(T);
    return true;
  }

  const char *ptr_;
  const char *begin_;
  const char *end_;
};

} // namespace util
} // namespace arcanedb