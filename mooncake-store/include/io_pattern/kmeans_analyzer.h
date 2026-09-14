#pragma once

#include "threshold_analyzer.h"

namespace mooncake::io_pattern {

// Slow-path workload detector for mixed traffic. It clusters session feature
// vectors, then maps each centroid to the documented workload templates.
class KMeansWorkloadAnalyzer final : public IoPatternAnalyzer {
   public:
    struct Config {
        uint32_t iterations{8};
        ThresholdAnalyzerConfig thresholds{};
    };

    KMeansWorkloadAnalyzer() = default;
    explicit KMeansWorkloadAnalyzer(Config config) : config_(config) {}

    PatternResult Analyze(const IoPatternSnapshot& snapshot) const override;
    WorkloadType DetectWorkloadType(
        const IoPatternSnapshot& snapshot) const override;
    float CalculateConfidence(const ObjectRef& object,
                              const IoPatternSnapshot& snapshot) const override;

   private:
    Config config_{};
};

}  // namespace mooncake::io_pattern
