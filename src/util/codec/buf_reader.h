#pragma once

#include <cstddef>
#include <string_view>

namespace arcanedb {
namespace util {

/**
 * @brief
 * Simple memcpy helper.
 * We are assuming data is written by memcpy.
 * @tparam T
 * @param buf
 * @param out
 */
template <typename T> inline void ReadBuf(const char buf[], T *out) noexcept {
  memcpy(reinterpret_cast<char *>(out), buf, sizeof(T));
}

// TODO: distinguish big endian and small endian buf reader.

class BufReaderBase {
public:
  BufReaderBase(std::string_view buffer)
      : ptr_(buffer.data()), begin_(ptr_), end_(begin_ + buffer.size()) {}

  size_t Remaining() const noexcept { return end_ - ptr_; }

  size_t Offset() const noexcept { return ptr_ - begin_; }

  std::string_view as_slice() const noexcept { return {ptr_, Remaining()}; }

  bool Skip(size_t len) noexcept {
    if (ptr_ + len > end_) {
      return false;
    }
    ptr_ += len;
    return true;
  }

protected:
  const char *ptr_;
  const char *begin_;
  const char *end_;
};

/**
 * @brief
 * buf reader based on machine arch
 */
class BufReader : public BufReaderBase {
public:
  BufReader(std::string_view buffer) : BufReaderBase(buffer) {}

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
};

} // namespace util
} // namespace arcanedb