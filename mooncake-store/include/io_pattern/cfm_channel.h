#pragma once

#include <optional>
#include <utility>

#include "reporter.h"
#include "types.h"
#include "../types.h"

namespace mooncake::io_pattern {

// Transport-neutral CFM reporting channel. In the embedded architecture a
// SubMaster owns CFM for the keys in its slots, so a channel delivers reports
// (snapshots and metric batches) and explicit prefetch plans to the owning
// SubMaster endpoint. There is no policy polling or delivery acknowledgement
// loop: the receiver merges reports into its local runtime and policy
// commands execute on the SubMaster that owns the reported keys.
class CfmChannel {
   public:
    virtual ~CfmChannel() = default;
    virtual bool SendSnapshot(const IoPatternSnapshot& snapshot) = 0;
    virtual bool SendMetricBatch(const MetricBatch& batch) = 0;
    virtual ErrorCode ExecutePrefetch(const PrefetchPlan& plan) = 0;
};

}  // namespace mooncake::io_pattern
