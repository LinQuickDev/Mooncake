#pragma once

#include <memory>
#include <utility>

#include "cfm_channel.h"
#include "client.h"

namespace mooncake::io_pattern {

// Reporting client used by inference-side connectors and integration tests.
// Network behavior is delegated to the injected channel; the client is what a
// vLLM/SGLang connector sees when it reports IO Pattern observations for keys
// owned by the addressed SubMaster.
class CfmClientImpl final : public CfmClient {
   public:
    explicit CfmClientImpl(std::shared_ptr<CfmChannel> channel)
        : channel_(std::move(channel)) {}

    ErrorCode ReportSnapshot(const IoPatternSnapshot& snapshot) override;
    ErrorCode ReportMetricBatch(const MetricBatch& batch) override;
    ErrorCode ExecutePrefetch(const PrefetchPlan& plan) override;

   private:
    std::shared_ptr<CfmChannel> channel_;
};

}  // namespace mooncake::io_pattern
