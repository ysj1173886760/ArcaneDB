/**
 * @file internal_page.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-22
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "btree/page/internal_page.h"
#include "butil/macros.h"
#include <algorithm>
#include <iterator>

namespace arcanedb {
namespace btree {

Status InternalRows::GetPageId(const Options &opts,
                               property::SortKeysRef sort_key,
                               PageIdView *page_id) const noexcept {
  auto iter =
      std::upper_bound(rows_.begin(), rows_.end(), sort_key,
                       [](property::SortKeysRef sk, const InternalRow &row) {
                         return sk < row.sort_key;
                       });
  // check due to the fact that rows_.begin() is smallest sort key
  CHECK(iter != rows_.begin());
  iter--;
  *page_id = iter->page_id;
  return Status::Ok();
}

Status
InternalRows::Split(const Options &opts, property::SortKeysRef old_sort_key,
                    std::vector<InternalRow> new_internal_rows) noexcept {
  if (old_sort_key.empty()) {
    // overwrite
    CHECK(rows_.empty());
    rows_ = std::move(new_internal_rows);
    return Status::Ok();
  }
  CHECK(new_internal_rows.size() > 1);
  // first find old
  auto it =
      std::lower_bound(rows_.begin(), rows_.end(), old_sort_key,
                       [](const InternalRow &row, property::SortKeysRef sk) {
                         return row.sort_key < sk;
                       });
  if (it == rows_.end() || it->sort_key != old_sort_key) {
    return Status::NotFound();
  }
  // overwrite
  // user the original lower bound
  new_internal_rows[0].sort_key = it->sort_key;
  *it = std::move(new_internal_rows[0]);
  it++;
  rows_.insert(it, std::make_move_iterator(new_internal_rows.begin() + 1),
               std::make_move_iterator(new_internal_rows.end()));
  return Status::Ok();
}

bool InternalRows::TEST_SortKeyAscending() const noexcept {
  if (rows_.size() == 1) {
    return true;
  }
  auto sk = rows_[0].sort_key.as_ref();
  for (int i = 1; i < rows_.size(); i++) {
    auto new_sk = rows_[i].sort_key.as_ref();
    if (sk > new_sk) {
      ARCANEDB_ERROR("sk {}, new sk {}", sk.ToString(), new_sk.ToString());
      return false;
    }
    sk = new_sk;
  }
  return true;
}

} // namespace btree
} // namespace arcanedb