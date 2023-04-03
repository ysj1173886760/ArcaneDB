/**
 * @file options.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-24
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

#include "common/config.h"
#include "util/thread_pool.h"
namespace arcanedb {
namespace log_store {

struct Options {
  size_t segment_num{common::Config::kLogSegmentDefaultNum};
  size_t segment_size{common::Config::kLogSegmentDefaultSize};
  bool should_sync_file{true};
};

} // namespace log_store
} // namespace arcanedb