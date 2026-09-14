#pragma once

#include "io_pattern/analyzer.h"

namespace mooncake::io_pattern {

struct ThresholdAnalyzerConfig {
    uint32_t code_agent_token_count{16 * 1024};
    uint32_t code_agent_prefix_fanout{16};
    uint32_t code_agent_match_length{256};
    uint64_t recommendation_block_size{128 * 1024};
    uint32_t recommendation_frequency{20};
    uint32_t conversation_prefix_fanout{16};
    uint32_t conversation_match_length{256};
};

// Deterministic, low-latency analyzer for the documented threshold path.
// Mixed workloads are intentionally returned when no rule matches.
class ThresholdAnalyzer final : public IoPatternAnalyzer {
   public:
    explicit ThresholdAnalyzer(ThresholdAnalyzerConfig config = {})
        : config_(config) {}

    PatternResult Analyze(const IoPatternSnapshot& snapshot) const override;
    WorkloadType DetectWorkloadType(
        const IoPatternSnapshot& snapshot) const override;
    float CalculateConfidence(const ObjectRef& object,
                              const IoPatternSnapshot& snapshot) const override;

   private:
    const KeyMetrics* FindKey(const ObjectRef& object,
                              const IoPatternSnapshot& snapshot) const;
    float KeyConfidence(const KeyMetrics& key) const;

    ThresholdAnalyzerConfig config_;
};

}  // namespace mooncake::io_pattern
