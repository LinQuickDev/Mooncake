#include "io_pattern/tier_executor.h"

namespace mooncake::io_pattern {
namespace {

// A handler that reports UNAVAILABLE_IN_CURRENT_MODE declined to act because the
// storage primitive cannot run in this configuration. That is not a failure of
// the plan: the policy had no way to influence it, so it must not be counted as
// a policy failure by the caller.
bool IsUnavailable(ErrorCode code) {
    return code == ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}

}  // namespace

PolicyExecutionStatus TierOperationExecutor::Execute(
    const PolicyResult& result) const {
    PolicyExecutionStatus status;
    const bool has_eviction_work = !result.eviction.candidates.empty() ||
                                   result.eviction.target_bytes != 0;
    if (eviction_ && has_eviction_work) {
        status.eviction = eviction_(result.eviction);
        if (IsUnavailable(status.eviction)) ++status.skipped;
    } else if (has_eviction_work) {
        // A missing handler is a configuration error rather than a storage
        // limitation, so it keeps marking the execution degraded.
        status.eviction = ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
        status.degraded = true;
    }
    if (prefetch_ && !result.prefetch.candidates.empty()) {
        status.prefetch = prefetch_(result.prefetch);
        if (IsUnavailable(status.prefetch)) ++status.skipped;
    } else if (!result.prefetch.candidates.empty()) {
        status.prefetch = ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
        status.degraded = true;
    }
    for (const auto& admission : result.admissions) {
        if (admission.decision != AdmissionDecision::kAdmit) {
            status.admissions.push_back(ErrorCode::OK);
            continue;
        }
        if (!admission_) {
            status.admissions.push_back(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
            status.degraded = true;
            continue;
        }
        const ErrorCode code = admission_(admission);
        if (IsUnavailable(code)) ++status.skipped;
        status.admissions.push_back(code);
    }
    return status;
}

}  // namespace mooncake::io_pattern
