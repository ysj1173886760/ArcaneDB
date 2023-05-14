/**
 * @file weighted_graph_leveldb.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-05-14
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "common/status.h"
#include "leveldb/db.h"
#include <cstdint>
#include <memory>
#include <string>

namespace arcanedb {
namespace graph {

struct LevelDBBasedWeightedGraphOptions {
  // memtable size
  size_t write_buffer_size = 4 << 20;
  // cache size
  size_t block_cache_size = 8 << 20;

  bool sync = false;
};

class LevelDBBasedWeightedGraph {
public:
  using VertexId = int64_t;
  using Value = std::string_view;

  static Status Open(const std::string &db_name,
                     const LevelDBBasedWeightedGraphOptions &opts,
                     std::unique_ptr<LevelDBBasedWeightedGraph> *db) noexcept;

  ~LevelDBBasedWeightedGraph() noexcept;

  static Status Destroy(const std::string &db_name) noexcept;

  class EdgeIterator {
  public:
    EdgeIterator(leveldb::Iterator *iterator, VertexId src) noexcept
        : iterator_(iterator), src_(src) {}

    ~EdgeIterator() noexcept;

    bool Valid() const noexcept;

    void Next() noexcept;

    VertexId OutVertexId() const noexcept;

    std::string_view EdgeValue() const noexcept;

  private:
    leveldb::Iterator *iterator_;
    VertexId src_;
  };

  /**
   * @brief
   *
   * @param vertex_id
   * @param data
   * @return Status
   */
  Status InsertVertex(VertexId vertex_id, Value data) noexcept;

  /**
   * @brief
   *
   * @param vertex_id
   * @return Status
   */
  Status DeleteVertex(VertexId vertex_id) noexcept;

  /**
   * @brief
   *
   * @param src
   * @param dst
   * @param data
   * @return Status
   */
  Status InsertEdge(VertexId src, VertexId dst, Value data) noexcept;

  /**
   * @brief
   *
   * @param src
   * @param dst
   * @return Status
   */
  Status DeleteEdge(VertexId src, VertexId dst) noexcept;

  /**
   * @brief Get the Vertex object
   *
   * @param vertex_id
   * @param value
   * @return Status
   */
  Status GetVertex(VertexId vertex_id, std::string *value) noexcept;

  /**
   * @brief Get the Edge object
   *
   * @param src
   * @param dst
   * @param value
   * @return Status
   */
  Status GetEdge(VertexId src, VertexId dst, std::string *value) noexcept;

  /**
   * @brief Get the Edge Iterator object
   *
   * @param src
   * @param iterator
   */
  EdgeIterator GetEdgeIterator(VertexId src) noexcept;

private:
  static std::string VertexEncoding(VertexId vertex) noexcept {
    return fmt::format_int(static_cast<uint64_t>(vertex)).str() + "V";
  }

  static std::string EdgeEncoding(VertexId src, VertexId dst) noexcept {
    return fmt::format_int(static_cast<uint64_t>(src)).str() + "E" +
           fmt::format_int(static_cast<uint64_t>(dst)).str();
  }

  static std::string EdgePrefix(VertexId src) noexcept {
    return fmt::format_int(static_cast<uint64_t>(src)).str() + "E";
  }

  leveldb::DB *db_{};
  leveldb::Cache *cache_{};
  const leveldb::FilterPolicy *filter_policy_{};
  LevelDBBasedWeightedGraphOptions opts_;
};

} // namespace graph
} // namespace arcanedb