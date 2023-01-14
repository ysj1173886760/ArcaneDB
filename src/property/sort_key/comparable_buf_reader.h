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
    auto type = static_cast<ValueType>(ReadType_());

#define READ_POD(typename)                                                     \
  typename v;                                                                  \
  ReadPod(&v);                                                                 \
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

private:
  size_t ReadType_() noexcept {
    uint8_t type;
    auto view = as_slice();
    GetComparableFixedU8(&view, &type);
    assert(Skip(1));
    return type;
  }

  template <typename T> void ReadPod(T *value) noexcept {
    static_assert(std::is_pod_v<T>, "Expected pod type");
    auto view = as_slice();
    if constexpr (std::is_same_v<T, bool>) {
      assert(Skip(1));
    } else {
      assert(Skip(sizeof(T)));
    }
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
    assert(Skip(remains - view.size()));
  }
};

} // namespace property
} // namespace arcanedb