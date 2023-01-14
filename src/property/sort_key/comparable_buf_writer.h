#pragma once

#include "property/property_type.h"
#include "property/sort_key/sort_key_encoding.h"
#include <string>
namespace arcanedb {
namespace property {

class ComparableBufWriter {
public:
  ComparableBufWriter() = default;

  void WriteValue(const Value &value) noexcept {
    PutComparableFixedU8(&buf_, static_cast<uint8_t>(value.index()));
    std::visit(
        [&](auto &&arg) {
          using Type = std::decay_t<decltype(arg)>;
          if constexpr (std::is_same_v<Type, int32_t>) {
            PutComparableFixed32(&buf_, arg);
          } else if constexpr (std::is_same_v<Type, int64_t>) {
            PutComparableFixed64(&buf_, arg);
          } else if constexpr (std::is_same_v<Type, float>) {
            PutComparableFixedF32(&buf_, arg);
          } else if constexpr (std::is_same_v<Type, double>) {
            PutComparableFixedF64(&buf_, arg);
          } else if constexpr (std::is_same_v<Type, bool>) {
            PutComparableBoolAsFixedU8(&buf_, arg);
          } else if constexpr (std::is_same_v<Type, std::string_view>) {
            PutComparableString(&buf_, arg);
          }
        },
        value);
  }

  std::string Detach() noexcept { return std::move(buf_); }

private:
  std::string buf_;
};

} // namespace property
} // namespace arcanedb