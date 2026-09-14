#pragma once

#include "io_pattern/types.h"

namespace mooncake::io_pattern {

// Converts an immutable snapshot into workload and per-object features.
class IoPatternAnalyzer {
   public:
    virtual ~IoPatternAnalyzer() = default;

    virtual PatternResult Analyze(const IoPatternSnapshot& snapshot) const = 0;
    virtual WorkloadType DetectWorkloadType(
        const IoPatternSnapshot& snapshot) const = 0;
    virtual float CalculateConfidence(
        const ObjectRef& object, const IoPatternSnapshot& snapshot) const = 0;
};

}  // namespace mooncake::io_pattern
