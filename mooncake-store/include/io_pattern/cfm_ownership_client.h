#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "client.h"
#include "cfm_channel.h"
#include "cfm_protocol.h"
#include "cfm_service.h"
#include "reporter.h"
#include "rpc_transport.h"
#include "types.h"

namespace mooncake::io_pattern {

// Resolves the SubMaster that owns a reported object. Production callers feed
// this from the CVM key->slot->submaster mapping (cvm::KeySlot over the etcd
// /cvm/<ns> master registry) inside the Store client / connector layer;
// returns the SubMaster's regular coro_rpc endpoint ("host:port"), the same
// endpoint every other Mooncake RPC of that SubMaster uses.
using SubmasterEndpointResolver =
    std::function<std::optional<std::string>(const TenantId&, const std::string&)>;

// Ownership-addressed CFM reporting client.
//
// CFM is a component of every SubMaster; there is no standalone CFM Master
// and no auth token. A caller (inference connector / Store client) observes
// keys that may live on many SubMasters, so reports are bucketed by the CVM
// ownership resolver: observations whose key belongs to the same SubMaster are
// aggregated into one metric batch and delivered to that SubMaster's embedded
// CFM over its ordinary coro_rpc endpoint. Metrics whose owner cannot be
// resolved are dropped and counted so callers can degrade instead of guessing.
class CfmOwnershipClient final : public CfmClient {
   public:
    // `resolver` maps a reported object to the owning SubMaster endpoint. It
    // must be kept up to date with the CVM route (slot rebalance is rare).
    // `timeout` applies to every RPC.
    explicit CfmOwnershipClient(SubmasterEndpointResolver resolver,
                                std::chrono::milliseconds timeout =
                                    std::chrono::milliseconds(500));

    // Sends the snapshot, grouped by the owning SubMaster of each key.
    ErrorCode ReportSnapshot(const IoPatternSnapshot& snapshot) override;

    // Sends the metric batch, grouped by the owning SubMaster of each
    // inference/access object. Storage observations are deliberately not
    // routed by default: the SubMaster that owns the underlying storage
    // already reports its own watermark to its local runtime. Benchmark /
    // simulation callers can enable forward_storage to have the storage
    // metrics ride along with each owner-addressed report so a remote run can
    // drive the report-driven eviction dimension.
    ErrorCode ReportMetricBatch(const MetricBatch& batch) override;

    void set_forward_storage(bool forward) { forward_storage_ = forward; }

    // Sends an explicit prefetch plan to the SubMaster that owns the first
    // candidate; that SubMaster executes it through its local storage-safe
    // handlers.
    ErrorCode ExecutePrefetch(const PrefetchPlan& plan) override;

    // Observations dropped because no owner could be resolved.
    uint64_t dropped_observations() const;

   private:
    std::shared_ptr<CfmChannel> ChannelFor(const std::string& endpoint);
    std::string OwnerEndpoint(const ObjectRef& object) const;

    SubmasterEndpointResolver resolver_;
    std::chrono::milliseconds timeout_;
    std::unordered_map<std::string, std::shared_ptr<CfmChannel>> channels_;
    mutable std::mutex channels_mutex_;
    std::atomic<uint64_t> dropped_observations_{0};
    bool forward_storage_{false};
};

}  // namespace mooncake::io_pattern
