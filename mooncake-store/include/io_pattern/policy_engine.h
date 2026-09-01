#pragma once

#include <cstdint>

#include "io_pattern/types.h"

namespace mooncake::io_pattern {

class PolicyEngine {
   public:
    virtual ~PolicyEngine() = default;

    virtual EvictionPlan PlanEviction(const PolicyContext& context,
                                      CacheTier tier,
                                      uint64_t target_bytes) const = 0;
    virtual PrefetchPlan PlanPrefetch(const PolicyContext& context,
                                     const TraceHistory& trace) const = 0;
    virtual AdmissionResult DecideAdmission(
        const ObjectRef& object, CacheTier target_tier,
        const PolicyContext& context) const = 0;
};

}  // namespace mooncake::io_pattern
