/**
 * @file sort_key.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-15
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "absl/container/inlined_vector.h"
#include "absl/hash/hash.h"
#include "common/macros.h"
#include "property/property_type.h"
#include "property/sort_key/comparable_buf_reader.h"
#include "property/sort_key/comparable_buf_writer.h"
#include <limits>
#include <type_traits>
#include <vector>

namespace arcanedb {
namespace property {

class SortKeysRef;
class SortKeys;

template <typename T> class SortKeyCRTP {
public:
  std::string_view as_slice() const noexcept {
    return static_cast<const T *>(this)->as_slice();
  }

  bool empty() const noexcept { return as_slice().empty(); }

  SortKeys GetMinSortKeys() const noexcept;

  std::string ToString() const noexcept {
    std::string result = "(";
    TraverseSK_([&](OwnedValue value) {
      if (result.size() != 1) {
        result += ", ";
      }
      switch (static_cast<ValueType>(value.index())) {
      case ValueType::Int32:
        result += "[i32]";
        break;
      case ValueType::Int64:
        result += "[i64]";
        break;
      case ValueType::Float:
        result += "[float]";
        break;
      case ValueType::Double:
        result += "[double]";
        break;
      case ValueType::String:
        result += "[str]";
        break;
      case ValueType::Bool:
        result += "[bool]";
      default:
        UNREACHABLE();
      }
      std::visit(
          [&](const auto &arg) {
            using Type = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<Type, std::string>) {
              result += arg;
            } else {
              result += std::to_string(arg);
            }
          },
          value);
    });

    return result;
  }

private:
  template <typename Visitor> void TraverseSK_(const Visitor &visitor) const {
    ComparableBufReader reader(as_slice());
    while (true) {
      if (reader.Remaining() > 0) {
        OwnedValue value;
        reader.ReadValue(&value);
        visitor(std::move(value));
      } else {
        break;
      }
    }
  }
};

#define OP_HELPER(op)                                                          \
  template <typename T1, typename T2>                                          \
  inline bool operator op(const SortKeyCRTP<T1> &left,                         \
                          const SortKeyCRTP<T2> &right) {                      \
    return left.as_slice() op right.as_slice();                                \
  }

OP_HELPER(<);
OP_HELPER(<=);
OP_HELPER(==);
OP_HELPER(>);
OP_HELPER(>=);
OP_HELPER(!=);

#undef OP_HELPER

class SortKeys : public SortKeyCRTP<SortKeys> {
public:
  SortKeys(const ValueRefVec &value, size_t sort_key_cnt) noexcept {
    ComparableBufWriter writer;
    for (int i = 0; i < sort_key_cnt; i++) {
      writer.WriteValue(value[i]);
    }
    bytes_ = writer.Detach();
  }

  explicit SortKeys(const std::vector<Value> &value) noexcept {
    ComparableBufWriter writer;
    for (int i = 0; i < value.size(); i++) {
      writer.WriteValue(value[i]);
    }
    bytes_ = writer.Detach();
  }

  explicit SortKeys(std::string_view ref) : bytes_(ref.data(), ref.size()) {}

  std::string_view as_slice() const noexcept { return bytes_; }

  SortKeysRef as_ref() const noexcept;

private:
  std::string bytes_;
};

class SortKeysRef : public SortKeyCRTP<SortKeysRef> {
public:
  explicit SortKeysRef(std::string_view ref) : bytes_ref_(ref) {}

  SortKeys deref() const noexcept { return SortKeys(bytes_ref_); }

  std::string_view as_slice() const noexcept { return bytes_ref_; }

  SortKeysRef() = default;

private:
  std::string_view bytes_ref_;
};

struct SortKeysHash {
  size_t operator()(const SortKeys &sk) const noexcept {
    return absl::Hash<std::string_view>()(sk.as_slice());
  }
};

template <typename T> inline Value GetSortKeysLimitMin() noexcept {
  return Value(std::numeric_limits<std::decay_t<T>>::min());
}

template <> inline Value GetSortKeysLimitMin<std::string>() noexcept {
  static std::string str(1, static_cast<char>(0));
  return Value(std::string_view(str));
}

template <typename T> SortKeys SortKeyCRTP<T>::GetMinSortKeys() const noexcept {
  std::vector<Value> values;
  TraverseSK_([&](OwnedValue value) {
    std::visit(
        [&](const auto &arg) {
          using Type = std::decay_t<decltype(arg)>;
          auto min = GetSortKeysLimitMin<Type>();
          values.push_back(min);
        },
        value);
  });
  return SortKeys{values};
}

} // namespace property
} // namespace arcanedb