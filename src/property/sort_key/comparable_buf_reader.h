/**
 * @file comparable_buf_reader.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-15
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "common/macros.h"
#include "property/property_type.h"
#include "property/sort_key/sort_key_encoding.h"
#include "util/codec/buf_reader.h"
#include <type_traits>

namespace arcanedb {
namespace property {

class ComparableBufReader : public util::BufReaderBase {
public:
  ComparableBufReader(std::string_view buffer) : BufReaderBase(buffer) {}

  void ReadValue(OwnedValue *value) noexcept {
    auto t = ReadType_();
    auto type = static_cast<ValueType>(t);

#define READ_POD(typename)                                                     \
  typename v;                                                                  \
  ReadPod_(&v);                                                                \
  *value = v;                                                                  \
  break;

    switch (type) {
    case ValueType::Bool: {
      READ_POD(bool);
    }
    case ValueType::Int32: {
      READ_POD(int32_t);
    }
    case ValueType::Int64: {
      READ_POD(int64_t);
    }
    case ValueType::Double: {
      READ_POD(double);
    }
    case ValueType::Float: {
      READ_POD(float);
    }
    case ValueType::String: {
      std::string s;
      ReadComparableStringHelper_(&s);
      *value = std::move(s);
      break;
    }
    default:
      UNREACHABLE();
      break;
    }
#undef READ_POD
  }

  void SkipK(int k) noexcept {
    for (int i = 0; i < k; i++) {
      CHECK(Remaining() > 0);
      auto type = static_cast<ValueType>(ReadType_());
      switch (type) {
      case ValueType::Bool:
        Skip(1);
        break;
      case ValueType::Int32:
      case ValueType::Float:
        Skip(4);
        break;
      case ValueType::Int64:
      case ValueType::Double:
        Skip(8);
        break;
      case ValueType::String: {
        std::string dummy;
        ReadComparableStringHelper_(&dummy);
        break;
      }
      default:
        UNREACHABLE();
      }
    }
  }

private:
  size_t ReadType_() noexcept {
    uint8_t type;
    auto view = as_slice();
    GetComparableFixedU8(&view, &type);
    auto res = Skip(1);
    assert(res);
    return type;
  }

  template <typename T> void ReadPod_(T *value) noexcept {
    static_assert(std::is_pod_v<T>, "Expected pod type");
    auto view = as_slice();
    bool res;
    if constexpr (std::is_same_v<T, bool>) {
      res = Skip(1);
    } else {
      res = Skip(sizeof(T));
    }
    assert(res);
    if constexpr (std::is_same_v<bool, T>) {
      GetComparableFixedU8AsBool(&view, value);
    } else if constexpr (std::is_same_v<int32_t, T>) {
      GetComparableFixed32(&view, value);
    } else if constexpr (std::is_same_v<int64_t, T>) {
      GetComparableFixed64(&view, value);
    } else if constexpr (std::is_same_v<float, T>) {
      GetComparableFixedF32(&view, value);
    } else if constexpr (std::is_same_v<double, T>) {
      GetComparableFixedF64(&view, value);
    }
  }

  void ReadComparableStringHelper_(std::string *value) noexcept {
    auto view = as_slice();
    size_t remains = view.size();
    GetComparableString(&view, value);
    bool res = Skip(remains - view.size());
    assert(res);
  }
};

} // namespace property
} // namespace arcanedb