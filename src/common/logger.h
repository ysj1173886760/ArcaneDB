/**
 * @file logger.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief simple logger
 * @version 0.1
 * @date 2022-12-10
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

#include "spdlog/spdlog.h"
#include <cstdint>

namespace arcanedb {

enum class LogLevel : uint8_t {
  kLevelDebug = 1,
  kLevelInfo,
  kLevelWarn,
  kLevelError,
  kLevelCritical,
  kLevelOff,
};

static_assert(static_cast<uint8_t>(LogLevel::kLevelOff) ==
                  static_cast<uint8_t>(spdlog::level::level_enum::off),
              "LogLevelCheck failed");

// TODO(sheep) introduce compile time log level

#define ARCANEDB_DEBUG(...) spdlog::debug(__VA_ARGS__);

#define ARCANEDB_INFO(...) spdlog::info(__VA_ARGS__);

#define ARCANEDB_WARN(...) spdlog::warn(__VA_ARGS__);

#define ARCANEDB_ERROR(...) spdlog::error(__VA_ARGS__);

} // namespace arcanedb