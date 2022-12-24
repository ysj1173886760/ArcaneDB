/**
 * @file index_page.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-24
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "page_store/kv_page_store/index_page.h"
#include "butil/strings/string_piece.h"
#include "common/macros.h"
#include "page_store/page_store.h"
#include <cassert>
#include <limits>

namespace arcanedb {
namespace page_store {

// Page Format:
// | EntryNum 2byte | Entry0 | Entry1 | Entry2 | ...
// Entry Format:
// | Type 1byte | PageId Length 1byte | PageId |

Status IndexPage::DeserializationFrom(util::BufReader *reader) noexcept {
  pages_.clear();
  uint16_t entry_num;
#define READ_OR_RETURN_END_OF_BUF(value)                                       \
  if (!reader->ReadBytes(&value)) {                                            \
    return Status::EndOfBuf();                                                 \
  }

  READ_OR_RETURN_END_OF_BUF(entry_num);
  pages_.reserve(entry_num);
  for (size_t i = 0; i < entry_num; i++) {
    IndexEntry entry;
    auto s = DeserializeIndexEntry_(&entry, reader);
    if (!s.ok()) {
      return s;
    }
    pages_.emplace_back(std::move(entry));
  }
  return Status::Ok();
}

Status IndexPage::DeserializeIndexEntry_(IndexEntry *entry,
                                         util::BufReader *reader) noexcept {
  uint8_t type;
  READ_OR_RETURN_END_OF_BUF(type);
  uint8_t page_id_length;
  READ_OR_RETURN_END_OF_BUF(page_id_length);
  butil::StringPiece buffer;
  if (!reader->ReadPiece(&buffer, page_id_length)) {
    return Status::EndOfBuf();
  }
  entry->type = static_cast<PageStore::PageType>(type);
  entry->page_id = buffer.as_string();
  return Status::Ok();
}
#undef READ_OR_RETURN_END_OF_BUF

void IndexPage::SerializationTo(util::BufWriter *writer) noexcept {
  assert(!pages_.empty());
  CHECK(pages_.size() < std::numeric_limits<uint16_t>::max());
  writer->WriteBytes(static_cast<uint16_t>(pages_.size()));
  for (const auto &entry : pages_) {
    SerializeIndexEntry_(entry, writer);
  }
}

void IndexPage::SerializeIndexEntry_(const IndexEntry &entry,
                                     util::BufWriter *writer) noexcept {
  writer->WriteBytes(static_cast<uint8_t>(entry.type));
  CHECK(entry.page_id.size() < std::numeric_limits<uint8_t>::max());
  writer->WriteBytes(static_cast<uint8_t>(entry.page_id.size()));
  writer->WriteBytes(entry.page_id);
}

void IndexPage::UpdateDelta(const PageIdType &page_id) noexcept {
  pages_.emplace_back(
      IndexEntry{.type = PageStore::PageType::DeltaPage, .page_id = page_id});
}

void IndexPage::UpdateReplacement(const PageIdType &page_id) noexcept {
  pages_.clear();
  pages_.emplace_back(
      IndexEntry{.type = PageStore::PageType::BasePage, .page_id = page_id});
}

} // namespace page_store
} // namespace arcanedb