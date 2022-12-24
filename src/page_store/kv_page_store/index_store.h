/**
 * @file index_store.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief helper functions for index store. e.g. serialization utils.
 * @version 0.1
 * @date 2022-12-24
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

#include "page_store/page_store.h"

namespace arcanedb {
namespace page_store {

class IndexPage {
public:
  struct IndexEntry {
    PageStore::PageType type;
    std::string page_id;
  };

  IndexPage() = default;

private:
  std::vector<IndexEntry> pages_;
};

} // namespace page_store
} // namespace arcanedb