#pragma once

#include <cstdint>
#include <deque>
#include <mutex>

#include "threshold_analyzer.h"
#include "kmeans_analyzer.h"

namespace mooncake::io_pattern {

struct WorkloadFeatureStats {
    uint32_t token_median{0};
    uint32_t token_p90{0};
    uint32_t fanout_p90{0};
    uint64_t block_median{0};
    uint64_t block_p90{0};
    uint32_t match_p90{0};
    uint32_t frequency_median{0};
    size_t samples{0};
};

// Maintains a timestamp-bounded history of snapshots for workload detection.
class SlidingWindowAnalyzer final : public IoPatternAnalyzer {
   public:
    explicit SlidingWindowAnalyzer(uint64_t window_ns = 60'000'000'000ULL,
                                   ThresholdAnalyzerConfig config = {})
        : window_ns_(window_ns),
          analyzer_(config),
          kmeans_(KMeansWorkloadAnalyzer::Config{.thresholds = config}) {}

    PatternResult Analyze(const IoPatternSnapshot& snapshot) const override;
    WorkloadType DetectWorkloadType(
        const IoPatternSnapshot& snapshot) const override;
    float CalculateConfidence(const ObjectRef& object,
                              const IoPatternSnapshot& snapshot) const override;
    WorkloadFeatureStats FeatureStats() const;

   private:
    IoPatternSnapshot Aggregate(const IoPatternSnapshot& current) const;
    void Append(const IoPatternSnapshot& snapshot) const;

    const uint64_t window_ns_;
    mutable std::mutex mutex_;
    mutable std::deque<IoPatternSnapshot> history_;
    ThresholdAnalyzer analyzer_;
    KMeansWorkloadAnalyzer kmeans_;
};

}  // namespace mooncake::io_pattern
