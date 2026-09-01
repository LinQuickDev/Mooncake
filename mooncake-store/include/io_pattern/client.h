#pragma once

#include "../types.h"
#include "io_pattern/types.h"

namespace mooncake::io_pattern {

// Adapter seam between an inference node and the remote Cache Flow Manager.
class CfmClient {
   public:
    virtual ~CfmClient() = default;

    virtual ErrorCode ReportSnapshot(const IoPatternSnapshot& snapshot) = 0;
    virtual ErrorCode ReceivePolicy(const PolicyCommand& command) = 0;
    virtual ErrorCode ExecutePrefetch(const PrefetchPlan& plan) = 0;
};

}  // namespace mooncake::io_pattern
