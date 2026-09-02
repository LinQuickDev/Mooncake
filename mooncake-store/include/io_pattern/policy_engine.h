#pragma once

#include <algorithm>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <utility>
#include <unordered_map>

#include "io_pattern/ops.h"
#include "io_pattern/policy_strategies.h"
#include "io_pattern/registry.h"
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

    // Executes the three policy dimensions through one uniform result seam.
    virtual PolicyResult ExecutePolicy(const PolicyContext& context,
                                       CacheTier eviction_tier,
                                       uint64_t eviction_bytes,
                                       const TraceHistory& trace,
                                       const std::vector<ObjectRef>& admissions = {}) const {
        PolicyResult result;
        result.eviction = PlanEviction(context, eviction_tier, eviction_bytes);
        result.prefetch = PlanPrefetch(context, trace);
        for (const auto& object : admissions) {
            result.admissions.push_back(
                DecideAdmission(object, eviction_tier, context));
        }
        return result;
    }
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

// Resolves Ops implementations by registry name and composes them for one
// policy execution. Factories are consulted per call to avoid shared state.
class RegistryPolicyEngine final : public PolicyEngine {
   public:
    RegistryPolicyEngine(std::shared_ptr<PolicyOpsRegistries> registries,
                         std::string eviction_name,
                         std::string prefetch_name,
                         std::string admission_name)
        : registries_(std::move(registries)),
          eviction_name_(std::move(eviction_name)),
          prefetch_name_(std::move(prefetch_name)),
          admission_name_(std::move(admission_name)) {}

    EvictionPlan PlanEviction(const PolicyContext& context, CacheTier tier,
                              uint64_t bytes) const override {
        auto engine = Compose();
        return engine->PlanEviction(context, tier, bytes);
    }
    PrefetchPlan PlanPrefetch(const PolicyContext& context,
                              const TraceHistory& trace) const override {
        return Compose()->PlanPrefetch(context, trace);
    }
    AdmissionResult DecideAdmission(const ObjectRef& object, CacheTier tier,
                                    const PolicyContext& context) const override {
        return Compose()->DecideAdmission(object, tier, context);
    }

    PolicyResult ExecutePolicy(const PolicyContext& context,
                               CacheTier tier, uint64_t bytes,
                               const TraceHistory& trace,
                               const std::vector<ObjectRef>& admissions = {}) const override {
        auto result = PolicyEngine::ExecutePolicy(context, tier, bytes, trace,
                                                  admissions);
        std::shared_lock lock(mutex_);
        result.degraded = !registries_ ||
                          !registries_->eviction.Create(eviction_name_) ||
                          !registries_->prefetch.Create(prefetch_name_) ||
                          !registries_->admission.Create(admission_name_);
        return result;
    }

   private:
    std::shared_ptr<ComposedPolicyEngine> Compose() const {
        if (!registries_) return std::make_shared<ComposedPolicyEngine>(nullptr, nullptr, nullptr);
        return std::make_shared<ComposedPolicyEngine>(
            registries_->eviction.Create(eviction_name_),
            registries_->prefetch.Create(prefetch_name_),
            registries_->admission.Create(admission_name_));
    }

    std::shared_ptr<PolicyOpsRegistries> registries_;
    mutable std::shared_mutex mutex_;
    std::string eviction_name_;
    std::string prefetch_name_;
    std::string admission_name_;
};

// Selects the documented policy template for the current workload.
class WorkloadPolicyEngine final : public PolicyEngine {
   public:
    explicit WorkloadPolicyEngine(WorkloadType type = WorkloadType::kMixed,
                                  uint32_t transition_windows = 3)
        : transition_windows_(transition_windows), workload_type_(type),
          previous_type_(type) {
        Configure(type);
    }

    void SetWorkloadType(WorkloadType type) {
        std::unique_lock lock(mutex_);
        if (type == workload_type_) return;
        previous_type_ = workload_type_;
        previous_eviction_ = active_eviction_;
        workload_type_ = type;
        transition_progress_ = transition_windows_ == 0 ? 1.0F : 0.0F;
        Configure(type);
    }

    WorkloadType ActiveWorkload() const {
        std::shared_lock lock(mutex_);
        return workload_type_;
    }

    float TransitionProgress() const {
        std::shared_lock lock(mutex_);
        return transition_progress_;
    }

    // Advances the template transition by one completed detection window.
    void AdvanceTransitionWindow() {
        std::unique_lock lock(mutex_);
        if (transition_progress_ < 1.0F && transition_windows_ != 0) {
            transition_progress_ = std::min(
                1.0F, transition_progress_ + 1.0F /
                                  static_cast<float>(transition_windows_));
        }
    }

    void SetSessionWorkloads(const std::vector<SessionPattern>& sessions) {
        std::unique_lock lock(mutex_);
        session_engines_.clear();
        session_types_.clear();
        for (const auto& session : sessions) {
            if (!session.session_id.empty()) {
                session_engines_[session.session_id] = MakeEngine(session.workload_type);
                session_types_[session.session_id] = session.workload_type;
            }
        }
    }

    ScoreBasedEvictionConfig CurrentEvictionConfig() const {
        std::shared_lock lock(mutex_);
        return active_eviction_;
    }

    void ApplyEvictionTuning(const ScoreBasedEvictionConfig& config) {
        std::unique_lock lock(mutex_);
        tuned_eviction_ = config;
        Configure(workload_type_);
        for (auto& [session, engine] : session_engines_) {
            engine = MakeEngine(session_types_.at(session));
        }
    }

    EvictionPlan PlanEviction(const PolicyContext& context, CacheTier tier,
                              uint64_t target_bytes) const override {
        std::shared_lock lock(mutex_);
        if (context.session_id.empty() && transition_progress_ < 1.0F) {
            auto blended = MakeEngine(
                workload_type_, Blend(previous_eviction_, active_eviction_,
                                      transition_progress_));
            return blended->PlanEviction(context, tier, target_bytes);
        }
        return SelectEngine(context)->PlanEviction(context, tier, target_bytes);
    }
    PrefetchPlan PlanPrefetch(const PolicyContext& context,
                              const TraceHistory& trace) const override {
        std::shared_lock lock(mutex_);
        return SelectEngine(context)->PlanPrefetch(context, trace);
    }
    AdmissionResult DecideAdmission(const ObjectRef& object,
                                    CacheTier tier,
                                    const PolicyContext& context) const override {
        std::shared_lock lock(mutex_);
        return SelectEngine(context)->DecideAdmission(object, tier, context);
    }

   private:
    void Configure(WorkloadType type) {
        engine_ = MakeEngine(type);
        active_eviction_ = EvictionConfigFor(type);
        if (tuned_eviction_) active_eviction_ = *tuned_eviction_;
    }

    ScoreBasedEvictionConfig EvictionConfigFor(WorkloadType type) const {
        ScoreBasedEvictionConfig eviction;
        switch (type) {
            case WorkloadType::kCodeAgent:
                eviction.idle_weight = 0.8F;
                eviction.frequency_weight = 0.2F;
                eviction.prefix_weight = 0.3F;
                eviction.tier_down_mode = TierDownMode::kSkipHost;
                break;
            case WorkloadType::kGenerativeRecommendation:
                eviction.idle_weight = 0.3F;
                eviction.frequency_weight = 0.8F;
                eviction.prefix_weight = 0.2F;
                eviction.tier_down_mode = TierDownMode::kStepwise;
                break;
            case WorkloadType::kMultiTurnConversation:
                eviction.idle_weight = 0.5F;
                eviction.frequency_weight = 0.4F;
                eviction.prefix_weight = 0.6F;
                eviction.tier_down_mode = TierDownMode::kPrefixAffinity;
                break;
            case WorkloadType::kUnknown:
            case WorkloadType::kMixed:
                break;
        }
        return eviction;
    }

    static ScoreBasedEvictionConfig Blend(const ScoreBasedEvictionConfig& from,
                                          const ScoreBasedEvictionConfig& to,
                                          float progress) {
        const auto blend = [progress](float old_value, float new_value) {
            return old_value * (1.0F - progress) + new_value * progress;
        };
        ScoreBasedEvictionConfig result = to;
        result.idle_weight = blend(from.idle_weight, to.idle_weight);
        result.frequency_weight = blend(from.frequency_weight, to.frequency_weight);
        result.prefix_weight = blend(from.prefix_weight, to.prefix_weight);
        result.recompute_weight = blend(from.recompute_weight, to.recompute_weight);
        result.lower_replica_weight =
            blend(from.lower_replica_weight, to.lower_replica_weight);
        result.other_replica_weight =
            blend(from.other_replica_weight, to.other_replica_weight);
        // Tier routing is categorical, so switch at the midpoint while the
        // numerical eviction weights transition continuously.
        result.tier_down_mode = progress < 0.5F ? from.tier_down_mode
                                                : to.tier_down_mode;
        return result;
    }

    std::shared_ptr<ComposedPolicyEngine> MakeEngine(
        WorkloadType type,
        std::optional<ScoreBasedEvictionConfig> eviction_override = std::nullopt) const {
        ScoreBasedEvictionConfig eviction =
            eviction_override.value_or(EvictionConfigFor(type));
        PrefixMatchAdmissionConfig admission;
        TraceBasedPrefetchConfig prefetch;
        switch (type) {
            case WorkloadType::kCodeAgent:
                prefetch.match_length_threshold = 512;
                break;
            case WorkloadType::kGenerativeRecommendation:
                admission.frequency_threshold = 20;
                prefetch.match_length_threshold = 64;
                prefetch.strategy = PrefetchStrategy::kWaitComplete;
                break;
            case WorkloadType::kMultiTurnConversation:
                prefetch.match_length_threshold = 256;
                prefetch.strategy = PrefetchStrategy::kTimeout;
                prefetch.timeout_us = 5000;
                break;
            case WorkloadType::kUnknown:
            case WorkloadType::kMixed:
                break;
        }
        if (tuned_eviction_ && !eviction_override) eviction = *tuned_eviction_;
        return std::make_shared<ComposedPolicyEngine>(
            std::make_shared<ScoreBasedEvictionOps>(eviction),
            std::make_shared<TraceBasedPrefetchOps>(prefetch),
            std::make_shared<PrefixMatchAdmissionOps>(admission));
    }

    std::shared_ptr<ComposedPolicyEngine> SelectEngine(
        const PolicyContext& context) const {
        if (!context.session_id.empty()) {
            const auto it = session_engines_.find(context.session_id);
            if (it != session_engines_.end()) return it->second;
        }
        return engine_;
    }

    mutable std::shared_mutex mutex_;
    std::shared_ptr<ComposedPolicyEngine> engine_;
    std::unordered_map<std::string, std::shared_ptr<ComposedPolicyEngine>>
        session_engines_;
    std::unordered_map<std::string, WorkloadType> session_types_;
    WorkloadType workload_type_{WorkloadType::kUnknown};
    WorkloadType previous_type_{WorkloadType::kUnknown};
    uint32_t transition_windows_{3};
    float transition_progress_{1.0F};
    std::optional<ScoreBasedEvictionConfig> tuned_eviction_;
    ScoreBasedEvictionConfig active_eviction_;
    ScoreBasedEvictionConfig previous_eviction_;
};

}  // namespace mooncake::io_pattern
