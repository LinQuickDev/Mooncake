#pragma once

#include "io_pattern/types.h"

namespace mooncake::io_pattern {

class IoPatternAnalyzer {
   public:
    virtual ~IoPatternAnalyzer() = default;

    virtual PatternResult Analyze(const IoPatternSnapshot& snapshot) const = 0;
};

}  // namespace mooncake::io_pattern
