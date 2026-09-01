#pragma once

#include <cstdint>

#include "io_pattern/types.h"
#include "../types.h"

namespace mooncake::io_pattern {

// Produces an eviction plan; it does not move or delete data.
class EvictionOps {
   public:
    virtual ~EvictionOps() = default;

    virtual EvictionPlan Evaluate(const PolicyContext& context, CacheTier tier,
                                  uint64_t target_bytes) const = 0;
};

// Produces a prefetch plan; execution belongs to PrefetchExecutor.
class PrefetchOps {
   public:
    virtual ~PrefetchOps() = default;

    virtual PrefetchPlan Evaluate(const PolicyContext& context,
                                  const TraceHistory& trace) const = 0;
};

// Decides whether an object may enter a target tier.
class AdmissionOps {
   public:
    virtual ~AdmissionOps() = default;

    virtual AdmissionResult Evaluate(const ObjectRef& object,
                                     CacheTier target_tier,
                                     const PolicyContext& context) const = 0;
};

// Data movement is a client-side seam. Keeping it separate prevents a
// SubMaster-side prefetch planner from depending on a concrete storage client.
class PrefetchExecutor {
   public:
    virtual ~PrefetchExecutor() = default;

    virtual ErrorCode Execute(const PrefetchPlan& plan) = 0;
};

}  // namespace mooncake::io_pattern
