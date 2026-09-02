#pragma once

#include "rpc_transport.h"

namespace mooncake::io_pattern {

// Versioned binary wire codec for the CFM RPC methods. It deliberately owns
// every serialization detail so transports only deal in authenticated bytes.
class CfmBinaryCodec final : public CfmRpcCodec {
   public:
    std::string EncodeSnapshot(const IoPatternSnapshot& snapshot) const override;
    std::string EncodePrefetch(const PrefetchPlan& plan) const override;
    std::string EncodeMetricBatch(const MetricBatch& batch) const override;
    std::optional<PolicyCommand> DecodePolicy(const std::string& payload) const override;

    std::optional<IoPatternSnapshot> DecodeSnapshot(
        const std::string& payload) const;
    std::optional<MetricBatch> DecodeMetricBatch(const std::string& payload) const;
    std::string EncodePolicy(const PolicyCommand& command) const;
};

}  // namespace mooncake::io_pattern
