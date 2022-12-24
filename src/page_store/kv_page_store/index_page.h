/**
 * @file index_page.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief helper functions for index store. e.g. serialization utils.
 * @version 0.1
 * @date 2022-12-24
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

#include "common/macros.h"
#include "common/type.h"
#include "page_store/kv_page_store/kv_page_store.h"
#include "page_store/page_store.h"
#include "util/codec/buf_reader.h"
#include "util/codec/buf_writer.h"

namespace arcanedb {
namespace page_store {

class IndexPage {
public:
  struct IndexEntry {
    PageStore::PageType type;
    PageIdType page_id;

    bool operator==(const IndexEntry &rhs) const noexcept {
      return type == rhs.type && page_id == rhs.page_id;
    }
  };

  IndexPage() = default;

  Status DeserializationFrom(util::BufReader *reader) noexcept;

  void SerializationTo(util::BufWriter *writer) noexcept;

  void UpdateDelta(const PageIdType &page_id) noexcept;

  void UpdateReplacement(const PageIdType &page_id) noexcept;

  bool operator==(const IndexPage &rhs) const noexcept {
    return pages_ == rhs.pages_;
  }

private:
  FRIEND_TEST(KvPageStoreTest, IndexPageSerializationTest);
  FRIEND_TEST(KvPageStoreTest, IndexPageUpdateTest);

  void SerializeIndexEntry_(const IndexEntry &entry,
                            util::BufWriter *writer) noexcept;
  Status DeserializeIndexEntry_(IndexEntry *entry,
                                util::BufReader *reader) noexcept;

  std::vector<IndexEntry> pages_;
};

} // namespace page_store
} // namespace arcanedb