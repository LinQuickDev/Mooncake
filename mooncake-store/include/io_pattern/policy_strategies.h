#pragma once

#include "io_pattern/ops.h"

namespace mooncake::io_pattern {

enum class TierDownMode : uint8_t {
    kStepwise,
    kSkipHost,
    kPrefixAffinity,
};

struct ScoreBasedEvictionConfig {
    float idle_weight{1.0F};
    float frequency_weight{1.0F};
    float prefix_weight{1.0F};
    float recompute_weight{1.0F};
    float lower_replica_weight{1.0F};
    float other_replica_weight{1.0F};
    TierDownMode tier_down_mode{TierDownMode::kStepwise};
    uint64_t max_candidates{0};
};

class ScoreBasedEvictionOps final : public EvictionOps {
   public:
    explicit ScoreBasedEvictionOps(ScoreBasedEvictionConfig config = {})
        : config_(config) {}

    EvictionPlan Evaluate(const PolicyContext& context, CacheTier tier,
                          uint64_t target_bytes) const override;

   private:
    ScoreBasedEvictionConfig config_;
};

struct PrefixMatchAdmissionConfig {
    uint32_t hbm_match_length{64};
    uint64_t frequency_threshold{1};
    float max_memory_used_ratio{0.90F};
};

class PrefixMatchAdmissionOps final : public AdmissionOps {
   public:
    explicit PrefixMatchAdmissionOps(PrefixMatchAdmissionConfig config = {})
        : config_(config) {}

    AdmissionResult Evaluate(const ObjectRef& object, CacheTier target_tier,
                             const PolicyContext& context) const override;

   private:
    PrefixMatchAdmissionConfig config_;
};

struct TraceBasedPrefetchConfig {
    uint32_t match_length_threshold{256};
    float minimum_confidence{0.6F};
    uint64_t max_candidates{0};
    PrefetchStrategy strategy{PrefetchStrategy::kBestEffort};
    uint64_t timeout_us{0};
};

class TraceBasedPrefetchOps final : public PrefetchOps {
   public:
    explicit TraceBasedPrefetchOps(TraceBasedPrefetchConfig config = {})
        : config_(config) {}

    PrefetchPlan Evaluate(const PolicyContext& context,
                          const TraceHistory& trace) const override;

   private:
    TraceBasedPrefetchConfig config_;
};

}  // namespace mooncake::io_pattern
