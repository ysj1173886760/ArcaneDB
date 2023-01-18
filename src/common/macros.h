/**
 * @file macros.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-10
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

#include "butil/logging.h"
#include "common/logger.h"

namespace arcanedb {

#define FATAL(fmt, ...)                                                        \
  do {                                                                         \
    ARCANEDB_ERROR(fmt, ##__VA_ARGS__);                                        \
    CHECK(false);                                                              \
  } while (false)

#define FRIEND_TEST(test_case_name, test_name)                                 \
  friend class test_case_name##_##test_name##_Test

#define UNREACHABLE() std::abort();

} // namespace arcanedb