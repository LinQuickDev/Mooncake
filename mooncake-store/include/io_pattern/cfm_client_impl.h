#pragma once

#include <memory>
#include <functional>

#include "cfm_channel.h"
#include "client.h"

namespace mooncake::io_pattern {

// Production CFM client orchestration. Network behavior is delegated to the
// injected channel so this class remains independent of RPC libraries.
class CfmClientImpl final : public CfmClient {
   public:
    using PolicyCommandHandler = std::function<ErrorCode(const PolicyCommand&)>;

    explicit CfmClientImpl(std::shared_ptr<CfmChannel> channel,
                           PolicyCommandHandler policy_handler = {})
        : channel_(std::move(channel)),
          policy_handler_(std::move(policy_handler)) {}

    ErrorCode ReportSnapshot(const IoPatternSnapshot& snapshot) override;
    ErrorCode ReceivePolicy(const PolicyCommand& command) override;
    ErrorCode ExecutePrefetch(const PrefetchPlan& plan) override;

    std::optional<PolicyCommand> PollPolicy();
    ErrorCode PollAndDispatchPolicy();

   private:
    std::shared_ptr<CfmChannel> channel_;
    PolicyCommandHandler policy_handler_;
};

}  // namespace mooncake::io_pattern
