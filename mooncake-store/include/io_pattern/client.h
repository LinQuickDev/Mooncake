#pragma once

#include "../types.h"
#include "reporter.h"
#include "types.h"

namespace mooncake::io_pattern {

// Adapter seam between an inference node and the Cache Flow Manager of the
// SubMaster(s) that own the reported keys. Reports are sent over the same
// coro_rpc endpoint every other Mooncake API uses; there is no separate CFM
// Master and no credential to obtain.
class CfmClient {
   public:
    virtual ~CfmClient() = default;

    virtual ErrorCode ReportSnapshot(const IoPatternSnapshot& snapshot) = 0;
    virtual ErrorCode ReportMetricBatch(const MetricBatch& batch) = 0;
    virtual ErrorCode ExecutePrefetch(const PrefetchPlan& plan) = 0;
};

}  // namespace mooncake::io_pattern
