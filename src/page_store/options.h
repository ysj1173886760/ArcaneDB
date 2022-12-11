/**
 * @file options.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-11
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

namespace arcanedb {
namespace page_store {

struct OpenOptions {
  enum class PageStoreType {
    LeveldbPageStore,
  };

  PageStoreType type{PageStoreType::LeveldbPageStore};
};

struct WriteOptions {};

struct ReadOptions {};

} // namespace page_store
} // namespace arcanedb