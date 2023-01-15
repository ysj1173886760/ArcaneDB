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

// brpc has these macros
// so i commentted it out here.
// #define DISALLOW_COPY(cname) \
//   cname(const cname &) = delete; \ cname &operator=(const cname &) = delete

// #define DISALLOW_MOVE(cname)                                                   \
//   cname(cname &&) = delete;                                                    \
//   cname &operator=(cname &&) = delete

// #define DISALLOW_COPY_AND_MOVE(cname)                                          \
//   DISALLOW_COPY(cname);                                                        \
//   DISALLOW_MOVE(cname)

#define FATAL(fmt, ...)                                                        \
  do {                                                                         \
    ARCANEDB_ERROR(fmt, ##__VA_ARGS__);                                        \
    CHECK(false);                                                              \
  } while (false)

#define FRIEND_TEST(test_case_name, test_name)                                 \
  friend class test_case_name##_##test_name##_Test

#define UNREACHABLE() CHECK(false)

} // namespace arcanedb