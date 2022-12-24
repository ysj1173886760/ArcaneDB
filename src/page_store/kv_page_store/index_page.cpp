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
#include "common/type.h"
#include "page_store/page_store.h"
#include <cassert>
#include <limits>
#include <string>

namespace arcanedb {
namespace page_store {

// Page Format:
// | EntryNum 2byte | PageId Length 1byte | PageId Bytes | Entry0 | Entry1 |...
// Entry Format:
// | Type 1byte |

Status IndexPage::DeserializationFrom(util::BufReader *reader) noexcept {
  CHECK(!page_id_.empty());
  pages_.clear();
  uint16_t entry_num;
#define READ_OR_RETURN_END_OF_BUF(value)                                       \
  if (!reader->ReadBytes(&value)) {                                            \
    return Status::EndOfBuf();                                                 \
  }

  READ_OR_RETURN_END_OF_BUF(entry_num);
  pages_.reserve(entry_num);

  uint8_t page_id_length;
  READ_OR_RETURN_END_OF_BUF(page_id_length);
  butil::StringPiece buffer;
  if (!reader->ReadPiece(&buffer, page_id_length)) {
    return Status::EndOfBuf();
  }
  if (buffer != page_id_) {
    return Status::PageIdNotMatch();
  }

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
  entry->type = static_cast<PageStore::PageType>(type);
  return Status::Ok();
}
#undef READ_OR_RETURN_END_OF_BUF

void IndexPage::SerializationTo(util::BufWriter *writer) const noexcept {
  // serialize entry num
  assert(!pages_.empty());
  CHECK(pages_.size() < std::numeric_limits<uint16_t>::max());
  writer->WriteBytes(static_cast<uint16_t>(pages_.size()));

  // serialize page id
  CHECK(page_id_.size() < std::numeric_limits<uint8_t>::max());
  writer->WriteBytes(static_cast<uint8_t>(page_id_.size()));
  writer->WriteBytes(page_id_);

  // serialize entries
  for (const auto &entry : pages_) {
    SerializeIndexEntry_(entry, writer);
  }
}

void IndexPage::SerializeIndexEntry_(const IndexEntry &entry,
                                     util::BufWriter *writer) const noexcept {
  writer->WriteBytes(static_cast<uint8_t>(entry.type));
}

PageIdType IndexPage::UpdateDelta() noexcept {
  pages_.emplace_back(IndexEntry{.type = PageStore::PageType::DeltaPage});
  return AppendIndexOnPageId_(page_id_, pages_.size() - 1);
}

PageIdType IndexPage::UpdateReplacement() noexcept {
  pages_.clear();
  pages_.emplace_back(IndexEntry{.type = PageStore::PageType::BasePage});
  return AppendIndexOnPageId_(page_id_, pages_.size() - 1);
}

std::vector<PageStore::PageIdAndType>
IndexPage::ListAllPhysicalPages() const noexcept {
  std::vector<PageStore::PageIdAndType> res;
  res.reserve(pages_.size());
  for (int i = 0; i < pages_.size(); i++) {
    res.emplace_back(PageStore::PageIdAndType{
        .page_id = AppendIndexOnPageId_(page_id_, i), .type = pages_[i].type});
  }
  return res;
}

PageIdType IndexPage::AppendIndexOnPageId_(const PageIdType &page_id,
                                           size_t index) noexcept {
  return page_id + "|" + std::to_string(index);
}

} // namespace page_store
} // namespace arcanedb