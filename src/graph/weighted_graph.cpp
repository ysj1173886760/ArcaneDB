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

Status WeightedGraphDB::Open(std::unique_ptr<WeightedGraphDB> *db) noexcept {
  auto res = std::make_unique<WeightedGraphDB>();
  res->buffer_pool_ = std::make_unique<cache::BufferPool>();
  res->txn_manager_ = std::make_unique<txn::TxnManagerOCC>();
  *db = std::move(res);
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
  property::SortKeys sk({vertex_id});
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
  property::SortKeys sk({dst});
  auto s = txn_context_->DeleteRow(EdgeEncoding(src), sk.as_ref(), opts_);
  return s;
}

Status WeightedGraphDB::Transaction::GetVertex(VertexId vertex_id,
                                               std::string *value) noexcept {
  property::SortKeys sk({vertex_id});
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
  property::SortKeys sk({dst});
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
WeightedGraphDB::BeginRoTxn() noexcept {
  auto txn = std::make_unique<WeightedGraphDB::Transaction>();
  txn->opts_.buffer_pool = buffer_pool_.get();
  txn->opts_.ignore_lock = true;
  txn->txn_context_ = txn_manager_->BeginRoTxn();
  return txn;
}

std::unique_ptr<WeightedGraphDB::Transaction>
WeightedGraphDB::BeginRwTxn() noexcept {
  auto txn = std::make_unique<WeightedGraphDB::Transaction>();
  txn->opts_.buffer_pool = buffer_pool_.get();
  txn->txn_context_ = txn_manager_->BeginRwTxn();
  return txn;
}

} // namespace graph
} // namespace arcanedb