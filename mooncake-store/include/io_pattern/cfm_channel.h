#pragma once

#include <optional>

#include "types.h"
#include "../types.h"

namespace mooncake::io_pattern {

// Transport-neutral CFM RPC channel. Implementations own serialization,
// retries and connection lifecycle.
class CfmChannel {
   public:
    virtual ~CfmChannel() = default;
    virtual bool SendSnapshot(const IoPatternSnapshot& snapshot) = 0;
    virtual std::optional<PolicyCommand> PollPolicy() = 0;
    virtual ErrorCode ExecutePrefetch(const PrefetchPlan& plan) = 0;
};

}  // namespace mooncake::io_pattern
