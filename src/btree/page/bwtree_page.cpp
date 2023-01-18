/**
 * @file bwtree_page.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-08
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "btree/page/bwtree_page.h"
#include "butil/logging.h"

namespace arcanedb {
namespace btree {

Status BwTreePage::InsertRow(const property::Row &row,
                  const property::Schema *schema) noexcept {
  // generate delta node
  auto delta = std::make_shared<DeltaNode>(row, false);
  util::InstrumentedLockGuard<ArcanedbLock> guard(ptr_mu_);
}

Status BwTreePage::UpdateRow(const property::Row &row,
                  const property::Schema *schema) noexcept {
  auto delta = std::make_shared<DeltaNode>(row, false);
  util::InstrumentedLockGuard<ArcanedbLock> guard(ptr_mu_);
}

Status BwTreePage::DeleteRow(property::SortKeysRef sort_key,
                  const property::Schema *schema) noexcept {
  util::InstrumentedLockGuard<ArcanedbLock> guard(ptr_mu_);
}

} // namespace btree
} // namespace arcanedb