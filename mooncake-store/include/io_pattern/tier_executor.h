#pragma once

#include <cstddef>
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
    // Dimensions whose handler was present but declined to act because the
    // underlying storage primitive cannot run in the current mode (promotion
    // disabled, no lower-tier source replica, or a refusal to move data into an
    // inference-runtime-owned tier). A skip is not a policy failure: the policy
    // cannot influence that outcome, so it must not drive degradation.
    size_t skipped{0};
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
