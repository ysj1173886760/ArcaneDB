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
#include "log_store/log_store.h"
#include "txn/txn_context.h"
#include "txn/txn_manager.h"

namespace arcanedb {
namespace graph {

struct WeightedGraphOptions {
  bool enable_wal{false};
  bool enable_flush{false};
  bool sync_log{true};
  txn::LockManagerType lock_manager_type{txn::LockManagerType::kCentralized};
};

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

  static Status
  Open(const std::string &db_name, std::unique_ptr<WeightedGraphDB> *db,
       const WeightedGraphOptions &opts = WeightedGraphOptions()) noexcept;

  static Status Destroy(const std::string &db_name) noexcept;

  struct EdgeIterator {
    bool Valid() const noexcept { return current_idx < views.size(); }

    void Next() noexcept { current_idx += 1; }

    VertexId OutVertexId() const noexcept;

    std::string_view EdgeValue() const noexcept;

    size_t current_idx{};
    btree::RangeScanRowView views;
  };

  struct UnsortedEdgeIterator {
    bool Valid() const noexcept { return iterator.Valid(); }

    void Next() noexcept { iterator.Next(); }

    VertexId OutVertexId() const noexcept;

    std::string_view EdgeValue() const noexcept;

    btree::RowIterator iterator;
  };

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

    /**
     * @brief Read all out edges within the same start vertex.
     *
     * @param src
     * @param iterator
     */
    void GetEdgeIterator(VertexId src, EdgeIterator *iterator) noexcept;

    /**
     * @brief Get unsorted edge iterator
     */
    UnsortedEdgeIterator GetUnsortedEdgeIterator(VertexId src) noexcept;

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
  std::shared_ptr<log_store::LogStore> log_store_;
};

} // namespace graph
} // namespace arcanedb