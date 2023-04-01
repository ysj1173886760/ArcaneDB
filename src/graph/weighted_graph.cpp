/**
 * @file weighted_graph.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-27
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "graph/weighted_graph.h"
#include "cache/buffer_pool.h"
#include "log_store/posix_log_store/posix_log_store.h"
#include "page_store/kv_page_store/kv_page_store.h"
#include "txn/txn_manager_occ.h"
#include <memory>

namespace arcanedb {
namespace graph {

static constexpr property::ColumnId kWeightedGraphVertexIdColumn = 0;
static constexpr property::ColumnId kWeightedGraphValueColumn = 1;
static const property::Column kWeightedGraphColumn1{
    .column_id = kWeightedGraphVertexIdColumn,
    .name = "vertex_id",
    .type = property::ValueType::Int64};
static const property::Column kWeightedGraphColumn2{
    .column_id = kWeightedGraphValueColumn,
    .name = "string",
    .type = property::ValueType::String};
static const property::RawSchema kWeightedGraphRawSchema{
    .columns = {kWeightedGraphColumn1, kWeightedGraphColumn2},
    .schema_id = 0,
    .sort_key_count = 1};
static const property::Schema kWeightedGraphSchema(kWeightedGraphRawSchema);

WeightedGraphDB::VertexId WeightedGraphDB::EdgeIterator::OutVertexId() const
    noexcept {
  property::ValueResult res;
  auto s =
      views.at(current_idx)
          .GetProp(kWeightedGraphVertexIdColumn, &res, &kWeightedGraphSchema);
  CHECK(s.ok());
  return std::get<int64_t>(res.value);
}

std::string_view WeightedGraphDB::EdgeIterator::EdgeValue() const noexcept {
  property::ValueResult res;
  auto s = views.at(current_idx)
               .GetProp(kWeightedGraphValueColumn, &res, &kWeightedGraphSchema);
  CHECK(s.ok());
  return std::get<std::string_view>(res.value);
}

void WeightedGraphDB::Transaction::GetEdgeIterator(
    VertexId src, EdgeIterator *iterator) noexcept {
  btree::RangeScanRowView views;
  Filter filter;
  BtreeScanOpts scan_opts;
  txn_context_->RangeFilter(EdgeEncoding(src), opts_, filter, scan_opts,
                            &views);
  iterator->current_idx = 0;
  iterator->views = std::move(views);
}

Status WeightedGraphDB::Open(const std::string &db_name,
                             std::unique_ptr<WeightedGraphDB> *db,
                             const WeightedGraphOptions &opts) noexcept {
  auto res = std::make_unique<WeightedGraphDB>();
  if (opts.enable_wal) {
    log_store::Options opts;
    auto s = log_store::PosixLogStore::Open(db_name + "_log", opts,
                                            &res->log_store_);
    if (!s.ok()) {
      return s;
    }
  }

  std::shared_ptr<page_store::PageStore> page_store;
  if (opts.enable_flush) {
    page_store::Options opts;
    auto s =
        page_store::KvPageStore::Open(db_name + "_page", opts, &page_store);
    if (!s.ok()) {
      return s;
    }
  }

  res->buffer_pool_ = std::make_unique<cache::BufferPool>(page_store);
  res->txn_manager_ =
      std::make_unique<txn::TxnManagerOCC>(opts.lock_manager_type);
  *db = std::move(res);
  return Status::Ok();
}

Status WeightedGraphDB::Destroy(const std::string &db_name) noexcept {
  log_store::PosixLogStore::Destory(db_name + "_log");
  page_store::KvPageStore::Destory(db_name + "_page");
  return Status::Ok();
}

// TODO(sheep): check duplicate
Status WeightedGraphDB::Transaction::InsertVertex(VertexId vertex_id,
                                                  Value data) noexcept {
  property::ValueRefVec vec;
  vec.push_back(vertex_id);
  vec.push_back(data);
  util::BufWriter writer;
  auto s = property::Row::Serialize(vec, &writer, &kWeightedGraphSchema);
  if (unlikely(!s.ok())) {
    return s;
  }
  auto buffer = writer.Detach();
  property::Row row(buffer.data());
  s = txn_context_->SetRow(VertexEncoding(vertex_id), row, opts_);
  return s;
}

Status WeightedGraphDB::Transaction::DeleteVertex(VertexId vertex_id) noexcept {
  property::SortKeys sk(vertex_id);
  auto s =
      txn_context_->DeleteRow(VertexEncoding(vertex_id), sk.as_ref(), opts_);
  return s;
}

Status WeightedGraphDB::Transaction::InsertEdge(VertexId src, VertexId dst,
                                                Value data) noexcept {
  property::ValueRefVec vec;
  vec.push_back(dst);
  vec.push_back(data);
  util::BufWriter writer;
  auto s = property::Row::Serialize(vec, &writer, &kWeightedGraphSchema);
  if (unlikely(!s.ok())) {
    return s;
  }
  auto buffer = writer.Detach();
  property::Row row(buffer.data());
  s = txn_context_->SetRow(EdgeEncoding(src), row, opts_);
  return s;
}

Status WeightedGraphDB::Transaction::DeleteEdge(VertexId src,
                                                VertexId dst) noexcept {
  property::SortKeys sk(dst);
  auto s = txn_context_->DeleteRow(EdgeEncoding(src), sk.as_ref(), opts_);
  return s;
}

Status WeightedGraphDB::Transaction::GetVertex(VertexId vertex_id,
                                               std::string *value) noexcept {
  property::SortKeys sk(vertex_id);
  btree::RowView view;
  auto s = txn_context_->GetRow(VertexEncoding(vertex_id), sk.as_ref(), opts_,
                                &view);
  if (!s.ok()) {
    return s;
  }
  property::ValueResult res;
  s = view.at(0).GetProp(kWeightedGraphValueColumn, &res,
                         &kWeightedGraphSchema);
  if (unlikely(!s.ok())) {
    return s;
  }
  *value = std::get<std::string_view>(res.value);
  return s;
}

Status WeightedGraphDB::Transaction::GetEdge(VertexId src, VertexId dst,
                                             std::string *value) noexcept {
  property::SortKeys sk(dst);
  btree::RowView view;
  auto s = txn_context_->GetRow(EdgeEncoding(src), sk.as_ref(), opts_, &view);
  if (!s.ok()) {
    return s;
  }
  property::ValueResult res;
  s = view.at(0).GetProp(kWeightedGraphValueColumn, &res,
                         &kWeightedGraphSchema);
  if (unlikely(!s.ok())) {
    return s;
  }
  *value = std::get<std::string_view>(res.value);
  return s;
}

Status WeightedGraphDB::Transaction::Commit() noexcept {
  return txn_context_->CommitOrAbort(opts_);
}

std::unique_ptr<WeightedGraphDB::Transaction>
WeightedGraphDB::BeginRoTxn(const Options &opts) noexcept {
  auto txn = std::make_unique<WeightedGraphDB::Transaction>();
  txn->opts_ = opts;
  txn->opts_.log_store = log_store_.get();
  txn->opts_.buffer_pool = buffer_pool_.get();
  txn->opts_.ignore_lock = true;
  txn->txn_context_ = txn_manager_->BeginRoTxn(opts);
  return txn;
}

std::unique_ptr<WeightedGraphDB::Transaction>
WeightedGraphDB::BeginRwTxn(const Options &opts) noexcept {
  auto txn = std::make_unique<WeightedGraphDB::Transaction>();
  txn->opts_ = opts;
  txn->opts_.log_store = log_store_.get();
  txn->opts_.buffer_pool = buffer_pool_.get();
  txn->txn_context_ = txn_manager_->BeginRwTxn(opts);
  return txn;
}

} // namespace graph
} // namespace arcanedb