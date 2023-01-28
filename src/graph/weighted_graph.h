/**
 * @file weighted_graph.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-27
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "cache/buffer_pool.h"
#include "txn/txn_context.h"
#include "txn/txn_manager.h"

namespace arcanedb {
namespace graph {

/**
 * @brief
 * Simple weighted graph db.
 * Vertex has vertex id (uint64_t) and value (std::string)
 * Edge has src vertex(Vertex), dest vertex(Vertex), and value(std::string).
 */
class WeightedGraphDB {
public:
  using VertexId = int64_t;
  using Value = std::string_view;

  static Status Open(std::unique_ptr<WeightedGraphDB> *db) noexcept;

  class Transaction {
  public:
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
     * @brief Read a vertex
     *
     * @param vertex_id
     * @param value
     * @return Status
     */
    Status GetVertex(VertexId vertex_id, std::string *value) noexcept;

    /**
     * @brief Read a edge
     *
     * @param src
     * @param dst
     * @param value
     * @return Status
     */
    Status GetEdge(VertexId src, VertexId dst, std::string *value) noexcept;

    Status Commit() noexcept;

    TxnTs GetReadTs() const noexcept { return txn_context_->GetReadTs(); }

    TxnTs GetWriteTs() const noexcept { return txn_context_->GetWriteTs(); }

  private:
    friend class WeightedGraphDB;

    std::unique_ptr<txn::TxnContext> txn_context_;
    Options opts_;
  };

  static std::string VertexEncoding(VertexId vertex) noexcept {
    // cast to uint is more faster since it doesn't need to handle minus mark
    return fmt::format_int(static_cast<uint64_t>(vertex)).str() + "V";
  }

  static std::string EdgeEncoding(VertexId vertex) noexcept {
    return fmt::format_int(static_cast<uint64_t>(vertex)).str() + "E";
  }

  std::unique_ptr<Transaction> BeginRoTxn(const Options &opts) noexcept;

  std::unique_ptr<Transaction> BeginRwTxn(const Options &opts) noexcept;

private:
  std::unique_ptr<txn::TxnManager> txn_manager_;
  std::unique_ptr<cache::BufferPool> buffer_pool_;
};

} // namespace graph
} // namespace arcanedb