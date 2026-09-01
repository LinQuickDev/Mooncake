#pragma once

#include "io_pattern/types.h"

namespace mooncake::io_pattern {

// Owns the published cache view, not the storage operations that realize it.
class CacheViewManager {
   public:
    virtual ~CacheViewManager() = default;

    virtual CacheView ComputeView() const = 0;
    virtual void PublishEvent(const CacheEvent& event) = 0;
    virtual KVMappingTable GetGlobalMapping() const = 0;
};

}  // namespace mooncake::io_pattern
