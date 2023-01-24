/**
 * @file schema_manager.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-07
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "property/property_type.h"
#include "property/schema.h"
#include <unordered_map>

namespace arcanedb {
namespace property {

class SchemaManager {
public:
  SchemaManager() = default;

  static SchemaManager *GetInstance() noexcept {
    static SchemaManager schema_manager;
    return &schema_manager;
  }

  void AddSchema(std::unique_ptr<Schema> schema) noexcept {
    index_[schema->GetSchemaId()] = std::move(schema);
  }

  Schema *GetSchema(SchemaId schema_id) noexcept {
    auto it = index_.find(schema_id);
    if (it == index_.end()) {
      return nullptr;
    }
    return it->second.get();
  }

private:
  // mapping from schema id to schema
  std::unordered_map<SchemaId, std::unique_ptr<Schema>> index_;
};

} // namespace property
} // namespace arcanedb