#include "property/sort_key/sort_key.h"

namespace arcanedb {
namespace property {

SortKeysRef SortKeys::as_ref() const noexcept { return SortKeysRef(bytes_); }

} // namespace property
} // namespace arcanedb