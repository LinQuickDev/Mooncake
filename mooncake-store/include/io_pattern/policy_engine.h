#pragma once

#include <cstdint>
#include <memory>
#include <utility>

#include "io_pattern/ops.h"
#include "io_pattern/types.h"

namespace mooncake::io_pattern {

// Coordinates configured Ops implementations without owning data-path state.
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

// A small composition adapter that wires selected Ops instances together.
// Missing optional Ops degrade to empty plans or a deferred admission result.
class ComposedPolicyEngine final : public PolicyEngine {
   public:
    ComposedPolicyEngine(std::shared_ptr<EvictionOps> eviction,
                         std::shared_ptr<PrefetchOps> prefetch,
                         std::shared_ptr<AdmissionOps> admission)
        : eviction_(std::move(eviction)),
          prefetch_(std::move(prefetch)),
          admission_(std::move(admission)) {}

    EvictionPlan PlanEviction(const PolicyContext& context, CacheTier tier,
                              uint64_t target_bytes) const override {
        if (!eviction_) {
            return EvictionPlan{.source_tier = tier,
                                 .target_bytes = target_bytes};
        }
        return eviction_->Evaluate(context, tier, target_bytes);
    }

    PrefetchPlan PlanPrefetch(const PolicyContext& context,
                              const TraceHistory& trace) const override {
        if (!prefetch_) {
            return {};
        }
        return prefetch_->Evaluate(context, trace);
    }

    AdmissionResult DecideAdmission(const ObjectRef& object,
                                    CacheTier target_tier,
                                    const PolicyContext& context) const override {
        if (!admission_) {
            return AdmissionResult{.object = object,
                                    .target_tier = target_tier};
        }
        return admission_->Evaluate(object, target_tier, context);
    }

   private:
    std::shared_ptr<EvictionOps> eviction_;
    std::shared_ptr<PrefetchOps> prefetch_;
    std::shared_ptr<AdmissionOps> admission_;
};

}  // namespace mooncake::io_pattern
