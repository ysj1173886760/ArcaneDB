/**
 * @file options.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-19
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "property/schema.h"

namespace arcanedb {
namespace btree {

/**
 * @brief
 * Options for reading and writing btree.
 */
struct Options {
  const property::Schema *schema{};
  bool disable_compaction{true};
};

} // namespace btree
} // namespace arcanedb