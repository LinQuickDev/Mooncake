#include "io_pattern/cfm_client_impl.h"

namespace mooncake::io_pattern {

ErrorCode CfmClientImpl::ReportSnapshot(const IoPatternSnapshot& snapshot) {
    if (!channel_) return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    return channel_->SendSnapshot(snapshot) ? ErrorCode::OK
                                             : ErrorCode::RPC_FAIL;
}

ErrorCode CfmClientImpl::ReceivePolicy(const PolicyCommand& command) {
    if (!channel_) return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    if (!policy_handler_) return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    return policy_handler_(command);
}

ErrorCode CfmClientImpl::ExecutePrefetch(const PrefetchPlan& plan) {
    if (!channel_) return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    return channel_->ExecutePrefetch(plan);
}

std::optional<PolicyCommand> CfmClientImpl::PollPolicy() {
    return channel_ ? channel_->PollPolicy() : std::nullopt;
}

ErrorCode CfmClientImpl::PollAndDispatchPolicy() {
    const auto command = PollPolicy();
    return command ? ReceivePolicy(*command) : ErrorCode::RPC_TIMEOUT;
}

}  // namespace mooncake::io_pattern
