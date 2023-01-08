/**
 * @file logical_tuple.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-08
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "property/property_type.h"
namespace arcanedb {
namespace handler {

/**
 * @brief
 * Tuple that is generated from user.
 * It's different from Row in arcanedb, since user doesn't need to aware Row's
 * format. You can regard LogicalTuple as dtuple_t in innodb, and *Row* as row_t
 * in innodb.
 */
class LogicalTuple {
private:
  property::ValueRefVec value_ref_;
};

} // namespace handler
} // namespace arcanedb