#pragma once

#include <functional>
#include <vector>
#include <utility>

#include "types.h"
#include "../types.h"

namespace mooncake::io_pattern {

using EvictionHandler = std::function<ErrorCode(const EvictionPlan&)>;
using PrefetchHandler = std::function<ErrorCode(const PrefetchPlan&)>;
using AdmissionHandler = std::function<ErrorCode(const AdmissionResult&)>;

struct PolicyExecutionStatus {
    ErrorCode eviction{ErrorCode::OK};
    ErrorCode prefetch{ErrorCode::OK};
    std::vector<ErrorCode> admissions;
    bool degraded{false};
};

// Bridges policy output to storage/tier mechanisms owned by other modules.
class TierOperationExecutor final {
   public:
    TierOperationExecutor(EvictionHandler eviction,
                           PrefetchHandler prefetch,
                           AdmissionHandler admission)
        : eviction_(std::move(eviction)),
          prefetch_(std::move(prefetch)),
          admission_(std::move(admission)) {}

    PolicyExecutionStatus Execute(const PolicyResult& result) const;

   private:
    EvictionHandler eviction_;
    PrefetchHandler prefetch_;
    AdmissionHandler admission_;
};

}  // namespace mooncake::io_pattern
