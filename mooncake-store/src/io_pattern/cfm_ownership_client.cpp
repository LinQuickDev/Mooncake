#include "io_pattern/cfm_ownership_client.h"

#include <utility>

namespace mooncake::io_pattern {

CfmOwnershipClient::CfmOwnershipClient(SubmasterEndpointResolver resolver,
                                       std::chrono::milliseconds timeout)
    : resolver_(std::move(resolver)), timeout_(timeout) {}

std::string CfmOwnershipClient::OwnerEndpoint(const ObjectRef& object) const {
    if (!resolver_) return {};
    const auto endpoint = resolver_(object.tenant_id, object.key);
    return endpoint ? *endpoint : std::string{};
}

std::shared_ptr<CfmChannel> CfmOwnershipClient::ChannelFor(
    const std::string& endpoint) {
    if (endpoint.empty()) return nullptr;
    {
        std::lock_guard lock(channels_mutex_);
        const auto existing = channels_.find(endpoint);
        if (existing != channels_.end()) return existing->second;
    }
    auto transport = std::make_shared<CoroRpcCfmTransport>(endpoint, timeout_);
    auto channel = std::make_shared<CfmRpcChannel>(
        std::move(transport), std::make_shared<CfmBinaryCodec>(),
        CfmRpcConfig{.timeout = timeout_});
    std::lock_guard lock(channels_mutex_);
    const auto inserted = channels_.emplace(endpoint, std::move(channel));
    return inserted.first->second;
}

ErrorCode CfmOwnershipClient::ReportSnapshot(const IoPatternSnapshot& snapshot) {
    // Bucket keys by their owning SubMaster and report one snapshot per owner.
    // Storage observations are not routed by default: the SubMaster that owns
    // the storage already reports its own watermark into its local runtime.
    // When forward_storage_ is enabled (benchmark / simulation mode) every
    // owner-addressed snapshot also carries the reported storage metrics so a
    // remote run can drive the report-driven eviction dimension on each
    // owning SubMaster.
    std::unordered_map<std::string, IoPatternSnapshot> by_owner;
    for (const auto& key : snapshot.keys) {
        const auto endpoint = OwnerEndpoint(key.object);
        if (endpoint.empty()) {
            ++dropped_observations_;
            continue;
        }
        auto& owned = by_owner[endpoint];
        owned.generated_at_ns = snapshot.generated_at_ns;
        owned.keys.push_back(key);
    }
    if (forward_storage_ && !snapshot.storage.empty() && !by_owner.empty()) {
        for (auto& [endpoint, owned] : by_owner) {
            (void)endpoint;
            owned.storage = snapshot.storage;
        }
    }
    bool all_ok = true;
    for (const auto& [endpoint, owned] : by_owner) {
        auto channel = ChannelFor(endpoint);
        if (!channel || !channel->SendSnapshot(owned)) all_ok = false;
    }
    return all_ok ? ErrorCode::OK : ErrorCode::RPC_FAIL;
}

ErrorCode CfmOwnershipClient::ReportMetricBatch(const MetricBatch& batch) {
    std::unordered_map<std::string, MetricBatch> by_owner;
    for (const auto& metric : batch.inference) {
        const auto endpoint = OwnerEndpoint(metric.object);
        if (endpoint.empty()) {
            ++dropped_observations_;
            continue;
        }
        by_owner[endpoint].inference.push_back(metric);
    }
    for (const auto& access : batch.accesses) {
        const auto endpoint = OwnerEndpoint(access.object);
        if (endpoint.empty()) {
            ++dropped_observations_;
            continue;
        }
        by_owner[endpoint].accesses.push_back(access);
    }
    // batch.storage is intentionally not forwarded by default (see
    // ReportSnapshot); forward_storage_ attaches it to every owner-addressed
    // batch for benchmark / simulation runs.
    if (forward_storage_ && !batch.storage.empty() && !by_owner.empty()) {
        for (auto& [endpoint, owned] : by_owner) {
            (void)endpoint;
            owned.storage = batch.storage;
        }
    }
    bool all_ok = true;
    for (const auto& [endpoint, owned] : by_owner) {
        auto channel = ChannelFor(endpoint);
        if (!channel || !channel->SendMetricBatch(owned)) all_ok = false;
    }
    return all_ok ? ErrorCode::OK : ErrorCode::RPC_FAIL;
}

ErrorCode CfmOwnershipClient::ExecutePrefetch(const PrefetchPlan& plan) {
    if (plan.candidates.empty()) return ErrorCode::OK;
    // Address the plan to the SubMaster owning the first candidate object;
    // a plan produced by one SubMaster concerns keys it owns, so routing by
    // the primary candidate keeps a single endpoint target in practice.
    const auto endpoint = OwnerEndpoint(plan.candidates.front().object);
    if (endpoint.empty()) {
        ++dropped_observations_;
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    auto channel = ChannelFor(endpoint);
    if (!channel) return ErrorCode::RPC_FAIL;
    const auto code = channel->ExecutePrefetch(plan);
    return code == ErrorCode::OK ? ErrorCode::OK : ErrorCode::RPC_FAIL;
}

uint64_t CfmOwnershipClient::dropped_observations() const {
    return dropped_observations_.load(std::memory_order_relaxed);
}

}  // namespace mooncake::io_pattern
