#include "graph/weighted_graph_leveldb.h"
#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/iterator.h"
#include "util/codec/buf_writer.h"
#include <charconv>
#include <leveldb/options.h>

namespace arcanedb {
namespace graph {

Status LevelDBBasedWeightedGraph::Destroy(const std::string &name) noexcept {
  auto status = leveldb::DestroyDB(name, leveldb::Options());
  if (!status.ok()) {
    ARCANEDB_WARN("Failed to destory db {}, error: {}", name,
                  status.ToString());
    return Status::Err("Failed to destroy");
  }
  return Status::Ok();
}

LevelDBBasedWeightedGraph::~LevelDBBasedWeightedGraph() noexcept {
  delete db_;
  delete cache_;
  delete filter_policy_;
}

Status LevelDBBasedWeightedGraph::Open(
    const std::string &db_name, const LevelDBBasedWeightedGraphOptions &opts,
    std::unique_ptr<LevelDBBasedWeightedGraph> *db) noexcept {
  leveldb::Options leveldb_options;
  leveldb_options.write_buffer_size = opts.write_buffer_size;
  leveldb_options.block_cache = leveldb::NewLRUCache(opts.block_cache_size);
  // 10 is leveldb recommend value
  leveldb_options.filter_policy = leveldb::NewBloomFilterPolicy(10);
  leveldb_options.create_if_missing = true;

  leveldb::DB *leveldb;
  leveldb::Status status =
      leveldb::DB::Open(leveldb_options, db_name, &leveldb);
  if (!status.ok()) {
    ARCANEDB_WARN("Failed to open db, name %s, error: %s", db_name.c_str(),
                  status.ToString().c_str());
    return Status::Err();
  }

  auto res = std::make_unique<LevelDBBasedWeightedGraph>();
  res->db_ = leveldb;
  res->cache_ = leveldb_options.block_cache;
  res->filter_policy_ = leveldb_options.filter_policy;
  res->opts_ = opts;
  *db = std::move(res);
  return Status::Ok();
}

Status LevelDBBasedWeightedGraph::InsertVertex(VertexId vertex_id,
                                               Value data) noexcept {
  leveldb::WriteOptions write_opts;
  write_opts.sync = opts_.sync;
  leveldb::Slice value(data.data(), data.size());
  auto s = db_->Put(write_opts, VertexEncoding(vertex_id), value);
  if (!s.ok()) {
    ARCANEDB_ERROR("Failed to insert vertex, status: {}", s.ToString());
    return Status::Err();
  }
  return Status::Ok();
}

Status LevelDBBasedWeightedGraph::DeleteVertex(VertexId vertex_id) noexcept {
  leveldb::WriteOptions write_opts;
  write_opts.sync = opts_.sync;
  auto s = db_->Delete(write_opts, VertexEncoding(vertex_id));
  if (!s.ok()) {
    ARCANEDB_ERROR("Failed to delete vertex, status: {}", s.ToString());
    return Status::Err();
  }
  return Status::Ok();
}

Status LevelDBBasedWeightedGraph::InsertEdge(VertexId src, VertexId dst,
                                             Value data) noexcept {
  leveldb::WriteOptions write_opts;
  write_opts.sync = opts_.sync;
  leveldb::Slice value(data.data(), data.size());
  auto s = db_->Put(write_opts, EdgeEncoding(src, dst), value);
  if (!s.ok()) {
    ARCANEDB_ERROR("Failed to insert edge, status: {}", s.ToString());
    return Status::Err();
  }
  return Status::Ok();
}

Status LevelDBBasedWeightedGraph::DeleteEdge(VertexId src,
                                             VertexId dst) noexcept {
  leveldb::WriteOptions write_opts;
  write_opts.sync = opts_.sync;
  auto s = db_->Delete(write_opts, EdgeEncoding(src, dst));
  if (!s.ok()) {
    ARCANEDB_ERROR("Failed to delete edge, status: {}", s.ToString());
    return Status::Err();
  }
  return Status::Ok();
}

Status LevelDBBasedWeightedGraph::GetVertex(VertexId vertex_id,
                                            std::string *value) noexcept {
  leveldb::ReadOptions read_opts;
  auto s = db_->Get(read_opts, VertexEncoding(vertex_id), value);
  if (s.IsNotFound()) {
    return Status::NotFound();
  }
  if (!s.ok()) {
    ARCANEDB_ERROR("Failed to read vertex, status: {}", s.ToString());
    return Status::Err();
  }
  return Status::Ok();
}

Status LevelDBBasedWeightedGraph::GetEdge(VertexId src, VertexId dst,
                                          std::string *value) noexcept {
  leveldb::ReadOptions read_opts;
  auto s = db_->Get(read_opts, EdgeEncoding(src, dst), value);
  if (s.IsNotFound()) {
    return Status::NotFound();
  }
  if (!s.ok()) {
    ARCANEDB_ERROR("Failed to read edge, status: {}", s.ToString());
    return Status::Err();
  }
  return Status::Ok();
}

LevelDBBasedWeightedGraph::EdgeIterator
LevelDBBasedWeightedGraph::GetEdgeIterator(VertexId src) noexcept {
  leveldb::ReadOptions read_opts;
  auto iterator = db_->NewIterator(read_opts);
  iterator->Seek(EdgePrefix(src));
  assert(iterator->Valid());
  return {iterator, src};
}

LevelDBBasedWeightedGraph::EdgeIterator::~EdgeIterator() noexcept {
  delete iterator_;
}

bool LevelDBBasedWeightedGraph::EdgeIterator::Valid() const noexcept {
  return iterator_->Valid() && iterator_->key().starts_with(EdgePrefix(src_));
}

void LevelDBBasedWeightedGraph::EdgeIterator::Next() noexcept {
  iterator_->Next();
}

LevelDBBasedWeightedGraph::VertexId
LevelDBBasedWeightedGraph::EdgeIterator::OutVertexId() const noexcept {
  auto key = iterator_->key();
  int idx = 0;
  while (idx < key.size()) {
    if (key[idx] == 'E') {
      idx += 1;
      break;
    }
    idx += 1;
  }
  key.remove_prefix(idx);
  VertexId result;
  auto [ptr, ec] = std::from_chars(key.data(), key.data() + key.size(), result);
  assert(ec == std::errc());
  return result;
}

std::string_view
LevelDBBasedWeightedGraph::EdgeIterator::EdgeValue() const noexcept {
  auto slice = iterator_->value();
  return std::string_view(slice.data(), slice.size());
}

} // namespace graph
} // namespace arcanedb