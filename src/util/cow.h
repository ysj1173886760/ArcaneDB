/**
 * @file cow.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-22
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "butil/containers/doubly_buffered_data.h"
#include "butil/macros.h"
#include "common/logger.h"
#include <memory>
#include <type_traits>

namespace arcanedb {
namespace util {

/**
 * @brief
 * Single writer, multi reader cow
 * util based on butil::DoublyBufferedData.
 * @tparam T Cow requires T is copy constructible.
 */
template <typename T> class Cow {
  static_assert(std::is_copy_constructible_v<T>,
                "Cow requires T is copy constructible");
  using Ptr = std::shared_ptr<T>;
  using DoublyBufferedData = butil::DoublyBufferedData<Ptr>;
  using ScopedPtr = typename DoublyBufferedData::ScopedPtr;

public:
  explicit Cow(Ptr ptr) noexcept {
    immutable_ptr_.Modify(SetPtr_, ptr);
    mutable_ptr_.reset();
  }

  // requires external synchronization
  void Promote() noexcept {
    immutable_ptr_.Modify(SetPtr_, mutable_ptr_);
    mutable_ptr_.reset();
  }

  // requires external synchronization
  Ptr GetMutablePtr() noexcept {
    if (mutable_ptr_) {
      return mutable_ptr_;
    }
    auto immutable_ptr = GetImmutablePtr();
    mutable_ptr_.reset(new T(*immutable_ptr));
    return mutable_ptr_;
  }

  Ptr GetImmutablePtr() const noexcept {
    ScopedPtr ptr;
    {
      int ret = immutable_ptr_.Read(&ptr);
      CHECK(ret == 0);
    }
    return *ptr;
  }

  DISALLOW_COPY_AND_ASSIGN(Cow);

private:
  static size_t SetPtr_(Ptr &data, const std::shared_ptr<T> &ptr) noexcept {
    // syntax here is reall annoying
    data = ptr;
    return 1;
  }

  mutable DoublyBufferedData immutable_ptr_;
  Ptr mutable_ptr_;
};

} // namespace util
} // namespace arcanedb