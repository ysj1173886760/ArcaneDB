#pragma once

#include "bthread/mutex.h"

namespace arcanedb {
namespace cache {

class Flusher {
public:
private:
  bthread::Mutex mu_;
};

} // namespace cache
} // namespace arcanedb