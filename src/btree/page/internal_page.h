/**
 * @file internal_page.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-22
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "btree/options.h"
#include "common/status.h"
#include "common/type.h"
#include "property/sort_key/sort_key.h"
#include "util/cow.h"

namespace arcanedb {
namespace btree {

struct InternalRow {
  property::SortKeys sort_key;
  PageIdType page_id;

  InternalRow(InternalRow &&) = default;
  InternalRow &operator=(InternalRow &&) = default;
  InternalRow(const InternalRow &) = default;
  InternalRow &operator=(const InternalRow &) = default;
};

class InternalRows {
public:
  InternalRows() = default;

  Status GetPageId(const Options &opts, property::SortKeysRef sort_key,
                   PageIdView *page_id) const noexcept;

  Status Split(const Options &opts, property::SortKeysRef old_sort_key,
               std::vector<InternalRow> new_internal_rows) noexcept;

  bool TEST_SortKeyAscending() const noexcept;

private:
  std::vector<InternalRow> rows_;
};

class InternalPage {
public:
  InternalPage() noexcept : data_(std::make_shared<InternalRows>()) {}

  /**
   * @brief
   * Get page id though sort_key
   * @param opts
   * @param sort_key
   * @param page_id
   * @return Status
   */
  Status GetPageId(const Options &opts, property::SortKeysRef sort_key,
                   PageIdView *page_id) const noexcept;

  /**
   * @brief
   * Inserting internal rows into internal page, corresponding to btree split
   * operation. If old_sort_key is not empty, we will use new_internal_rows to
   * replace the entry which has same sort_key with old_sort_key. otherwise, we
   * will use new_internal_rows to overwrite the entire internal page.
   * @param opts
   * @param old_sort_key
   * @param new_internal_rows
   * @return Status
   */
  Status Split(const Options &opts, property::SortKeysRef old_sort_key,
               std::vector<InternalRow> new_internal_rows) noexcept;

  bool TEST_SortKeyAscending() noexcept;

private:
  util::Cow<InternalRows> data_;
};

} // namespace btree
} // namespace arcanedb