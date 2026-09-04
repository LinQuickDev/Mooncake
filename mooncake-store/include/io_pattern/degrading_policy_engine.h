#pragma once

#include <cstddef>
#include <memory>
#include <mutex>

#include "policy_engine.h"

namespace mooncake::io_pattern {

// Switches from a primary policy engine to a caller-provided fallback after
// repeated failures; recovery is explicit to avoid policy oscillation.
class DegradingPolicyEngine final : public PolicyEngine {
   public:
    DegradingPolicyEngine(std::shared_ptr<PolicyEngine> primary,
                          std::shared_ptr<PolicyEngine> fallback,
                          size_t failure_threshold = 3)
        : primary_(std::move(primary)),
          fallback_(std::move(fallback)),
          failure_threshold_(failure_threshold) {}

    void RecordFailure();
    void RecordSuccess();
    void ForceDegraded(bool degraded);
    bool degraded() const;
    size_t consecutive_failures() const;

    EvictionPlan PlanEviction(const PolicyContext&, CacheTier, uint64_t) const override;
    PrefetchPlan PlanPrefetch(const PolicyContext&, const TraceHistory&) const override;
    AdmissionResult DecideAdmission(const ObjectRef&, CacheTier,
                                    const PolicyContext&) const override;

   private:
    std::shared_ptr<PolicyEngine> Active() const;
    mutable std::mutex mutex_;
    std::shared_ptr<PolicyEngine> primary_;
    std::shared_ptr<PolicyEngine> fallback_;
    size_t failure_threshold_;
    size_t consecutive_failures_{0};
    bool degraded_{false};
};

}  // namespace mooncake::io_pattern
