#include "io_pattern/degrading_policy_engine.h"

namespace mooncake::io_pattern {

void DegradingPolicyEngine::RecordFailure() {
    std::lock_guard lock(mutex_);
    ++consecutive_failures_;
    if (failure_threshold_ != 0 && consecutive_failures_ >= failure_threshold_)
        degraded_ = true;
}

void DegradingPolicyEngine::RecordSuccess() {
    std::lock_guard lock(mutex_);
    consecutive_failures_ = 0;
}

void DegradingPolicyEngine::ForceDegraded(bool value) {
    std::lock_guard lock(mutex_);
    degraded_ = value;
    if (!value) consecutive_failures_ = 0;
}

bool DegradingPolicyEngine::degraded() const {
    std::lock_guard lock(mutex_);
    return degraded_;
}

size_t DegradingPolicyEngine::consecutive_failures() const {
    std::lock_guard lock(mutex_);
    return consecutive_failures_;
}

std::shared_ptr<PolicyEngine> DegradingPolicyEngine::Active() const {
    std::lock_guard lock(mutex_);
    return degraded_ ? fallback_ : primary_;
}

EvictionPlan DegradingPolicyEngine::PlanEviction(const PolicyContext& context,
                                                 CacheTier tier,
                                                 uint64_t bytes) const {
    auto engine = Active();
    return engine ? engine->PlanEviction(context, tier, bytes)
                  : EvictionPlan{.source_tier = tier, .target_bytes = bytes};
}

PrefetchPlan DegradingPolicyEngine::PlanPrefetch(
    const PolicyContext& context, const TraceHistory& trace) const {
    auto engine = Active();
    return engine ? engine->PlanPrefetch(context, trace) : PrefetchPlan{};
}

AdmissionResult DegradingPolicyEngine::DecideAdmission(
    const ObjectRef& object, CacheTier tier,
    const PolicyContext& context) const {
    auto engine = Active();
    return engine ? engine->DecideAdmission(object, tier, context)
                  : AdmissionResult{.object = object, .target_tier = tier};
}

}  // namespace mooncake::io_pattern
