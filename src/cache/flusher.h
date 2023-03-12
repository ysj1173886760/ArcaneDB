#pragma once

#include "bthread/mutex.h"
#include "cache/cache.h"

namespace arcanedb {
namespace cache {

class Flusher {
public:
private:
  bthread::Mutex mu_;
};

} // namespace cache
} // namespace arcanedb