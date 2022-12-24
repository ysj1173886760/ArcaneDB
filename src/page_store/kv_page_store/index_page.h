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
#include "page_store/page_store.h"
#include "util/codec/buf_reader.h"
#include "util/codec/buf_writer.h"

namespace arcanedb {
namespace page_store {

class IndexPage {
public:
  struct IndexEntry {
    PageStore::PageType type;

    bool operator==(const IndexEntry &rhs) const noexcept {
      return type == rhs.type;
    }
  };

  IndexPage() = default;
  IndexPage(const PageIdType &page_id) : page_id_(page_id) {}

  Status DeserializationFrom(util::BufReader *reader) noexcept;

  void SerializationTo(util::BufWriter *writer) const noexcept;

  /**
   * @brief
   *
   * @return PageIdType new page id for physical page.
   */
  PageIdType UpdateDelta() noexcept;

  /**
   * @brief
   *
   * @return PageIdType new page id for physical page.
   */
  PageIdType UpdateReplacement() noexcept;

  std::vector<PageStore::PageIdAndType> ListAllPhysicalPages() const noexcept;

  bool operator==(const IndexPage &rhs) const noexcept {
    return pages_ == rhs.pages_;
  }

private:
  FRIEND_TEST(KvPageStoreTest, IndexPageSerializationTest);
  FRIEND_TEST(KvPageStoreTest, IndexPageUpdateTest);

  void SerializeIndexEntry_(const IndexEntry &entry,
                            util::BufWriter *writer) const noexcept;
  Status DeserializeIndexEntry_(IndexEntry *entry,
                                util::BufReader *reader) noexcept;

  static PageIdType AppendIndexOnPageId_(const PageIdType &page_id,
                                         size_t index) noexcept;

  PageIdType page_id_;
  std::vector<IndexEntry> pages_;
};

} // namespace page_store
} // namespace arcanedb