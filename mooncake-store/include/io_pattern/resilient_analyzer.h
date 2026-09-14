#pragma once

#include <memory>
#include <mutex>

#include "analyzer.h"

namespace mooncake::io_pattern {

class ResilientAnalyzer final : public IoPatternAnalyzer {
   public:
    explicit ResilientAnalyzer(std::shared_ptr<IoPatternAnalyzer> primary,
                               size_t failure_threshold = 3)
        : primary_(std::move(primary)), failure_threshold_(failure_threshold) {}

    PatternResult Analyze(const IoPatternSnapshot& snapshot) const override;
    WorkloadType DetectWorkloadType(
        const IoPatternSnapshot& snapshot) const override;
    float CalculateConfidence(const ObjectRef& object,
                              const IoPatternSnapshot& snapshot) const override;
    bool degraded() const;
    size_t failures() const;
    // Returns the most recent safe answer without invoking the primary
    // analyzer. Used by a bounded caller when its analysis budget expires.
    PatternResult FallbackResult() const;

   private:
    void RecordFailure() const;
    void RecordSuccess(const PatternResult& result) const;
    PatternResult Fallback() const;

    std::shared_ptr<IoPatternAnalyzer> primary_;
    const size_t failure_threshold_;
    mutable std::mutex mutex_;
    mutable PatternResult last_result_;
    mutable size_t failures_{0};
    mutable bool degraded_{false};
};

}  // namespace mooncake::io_pattern
