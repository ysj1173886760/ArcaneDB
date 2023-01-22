/**
 * @file view.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-22
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "absl/container/inlined_vector.h"
#include <memory>
#include <optional>
#include <unordered_set>

namespace arcanedb {
namespace util {

template <typename T> struct OwnershipHelper { using type = std::nullopt_t; };

template <typename T, typename Owner = typename OwnershipHelper<T>::type>
class Views {
  static constexpr size_t kDefaultSize = 1;
  using ContainerType = absl::InlinedVector<T, kDefaultSize>;

public:
  using iterator = typename ContainerType::iterator;
  using const_iterator = typename ContainerType::const_iterator;
  using reference = typename ContainerType::reference;
  using const_reference = typename ContainerType::const_reference;
  using size_type = typename ContainerType::size_type;
  using reverse_iterator = typename ContainerType::reverse_iterator;
  using const_reverse_iterator = typename ContainerType::const_reverse_iterator;

  const_iterator begin() const noexcept { return container_.begin(); }

  iterator begin() noexcept { return container_.begin(); }

  const_iterator end() const noexcept { return container_.end(); }

  iterator end() noexcept { return container_.end(); }

  const_reverse_iterator rbegin() const noexcept { return container_.rbegin(); }

  reverse_iterator rbegin() noexcept { return container_.rbegin(); }

  const_reverse_iterator rend() const noexcept { return container_.rend(); }

  reverse_iterator rend() noexcept { return container_.rend(); }

  const_reference at(size_type n) const noexcept { return container_.at(n); }

  reference at(size_type n) noexcept { return container_.at(n); }

  const_reference back() const noexcept { return container_.back(); }

  reference back() noexcept { return container_.back(); }

  size_type size() const noexcept { return container_.size(); }

  void reserve(size_type n) noexcept { container_.reserve(n); }

  void reverse() noexcept { container_.reverse(); }

  void clear() noexcept {
    container_.clear();
    owner_container_.clear();
  }

  bool empty() noexcept { return container_.empty(); }

  template <typename U> void PushBackRef(U &&view) noexcept {
    container_.emplace_back(std::forward<U>(view));
  }

  void AddOwnerPointer(std::shared_ptr<const Owner> owner) noexcept {
    owner_container_.insert(std::move(owner));
  }

private:
  ContainerType container_;
  std::unordered_set<std::shared_ptr<const Owner>> owner_container_;
};

} // namespace util
} // namespace arcanedb