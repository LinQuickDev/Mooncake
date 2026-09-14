#include "io_pattern/cfm_client_impl.h"

namespace mooncake::io_pattern {

ErrorCode CfmClientImpl::ReportSnapshot(const IoPatternSnapshot& snapshot) {
    if (!channel_) return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    return channel_->SendSnapshot(snapshot) ? ErrorCode::OK
                                            : ErrorCode::RPC_FAIL;
}

ErrorCode CfmClientImpl::ReportMetricBatch(const MetricBatch& batch) {
    if (!channel_) return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    return channel_->SendMetricBatch(batch) ? ErrorCode::OK
                                            : ErrorCode::RPC_FAIL;
}

ErrorCode CfmClientImpl::ExecutePrefetch(const PrefetchPlan& plan) {
    if (!channel_) return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    return channel_->ExecutePrefetch(plan);
}

}  // namespace mooncake::io_pattern
