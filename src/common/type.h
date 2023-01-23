/**
 * @file type.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief type alias
 * @version 0.1
 * @date 2022-12-11
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

#include "bthread/mutex.h"
#include "util/lock.h"
#include <string>

namespace arcanedb {

using PageIdType = std::string;
using PageIdView = std::string_view;

using ArcanedbLock = util::ArcaneMutex<bthread::Mutex>;

using TxnID = int32_t;
using TxnTs = int32_t;

} // namespace arcanedb