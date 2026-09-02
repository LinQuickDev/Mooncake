#include "io_pattern/tier_executor.h"

namespace mooncake::io_pattern {

PolicyExecutionStatus TierOperationExecutor::Execute(
    const PolicyResult& result) const {
    PolicyExecutionStatus status;
    if (eviction_ && (!result.eviction.candidates.empty() ||
                      result.eviction.target_bytes != 0)) {
        status.eviction = eviction_(result.eviction);
    } else if (!result.eviction.candidates.empty() ||
               result.eviction.target_bytes != 0) {
        status.eviction = ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
        status.degraded = true;
    }
    if (prefetch_ && !result.prefetch.candidates.empty()) {
        status.prefetch = prefetch_(result.prefetch);
    } else if (!result.prefetch.candidates.empty()) {
        status.prefetch = ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
        status.degraded = true;
    }
    for (const auto& admission : result.admissions) {
        if (admission_ && admission.decision == AdmissionDecision::kAdmit) {
            status.admissions.push_back(admission_(admission));
        } else if (admission.decision == AdmissionDecision::kAdmit) {
            status.admissions.push_back(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
            status.degraded = true;
        } else {
            status.admissions.push_back(ErrorCode::OK);
        }
    }
    return status;
}

}  // namespace mooncake::io_pattern
