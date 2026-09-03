#pragma once

#include <cstddef>
#include <cstdint>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>

#include "cfm_ingress.h"

namespace mooncake::io_pattern {

// Authenticated server-side CFM endpoint. The RPC layer delegates to this
// class, keeping authentication, bounded policy queues and runtime dispatch
// independent of the concrete network transport.
class CfmService final {
   public:
    CfmService(std::shared_ptr<IoPatternRuntime> runtime,
               std::string auth_token, size_t policy_queue_capacity = 4096,
               std::string producer_auth_token = {});
    ~CfmService();

    bool Authenticate(std::string_view token) const;
    bool AuthenticateNode(std::string_view token) const;
    bool AuthenticateProducer(std::string_view token) const;
    bool Send(std::string_view node_id, std::string_view method,
              std::string_view payload, std::string_view token);
    std::optional<std::pair<uint64_t, std::string>> PollPolicy(
        std::string_view node_id, std::string_view token);
    bool AcknowledgePolicy(std::string_view node_id, uint64_t delivery_id,
                           bool success, std::string_view token);
    bool EnqueuePolicy(std::string node_id, std::string payload,
                       std::string_view token);

    // Returns the metrics aggregated for one CFM node. In a CVM deployment a
    // node is a stable SubMaster identity, so policy generation for one slot
    // owner must never observe keys reported by another SubMaster.
    IoPatternSnapshot SnapshotForNode(std::string_view node_id) const;

   private:
    struct NodeRuntime {
        std::shared_ptr<IoPatternRuntime> runtime;
        std::unique_ptr<CfmIngress> ingress;
    };

    bool EnqueueValidated(std::string node_id, std::string payload);
    std::shared_ptr<NodeRuntime> GetOrCreateNodeRuntime(
        std::string_view node_id);
    std::shared_ptr<NodeRuntime> FindNodeRuntime(
        std::string_view node_id) const;
    void SchedulePolicyProduction(std::string node_id, MetricBatch batch);
    void PolicyProducerWorker();
    void ProducePolicies(std::string_view node_id, const MetricBatch& batch);

    std::shared_ptr<IoPatternRuntime> runtime_;
    std::shared_ptr<CfmBinaryCodec> codec_;
    CfmIngress ingress_;
    mutable std::mutex node_runtimes_mutex_;
    std::unordered_map<std::string, std::shared_ptr<NodeRuntime>>
        node_runtimes_;
    const std::string auth_token_;
    const std::string producer_auth_token_;
    const size_t policy_queue_capacity_;
    std::mutex mutex_;
    std::unordered_map<std::string,
                       std::deque<std::pair<uint64_t, std::string>>>
        policy_queues_;
    uint64_t next_delivery_id_{1};
    size_t total_queued_policies_{0};
    std::mutex producer_mutex_;
    std::condition_variable producer_cv_;
    std::deque<std::pair<std::string, MetricBatch>> pending_metric_batches_;
    bool producer_stopping_{false};
    std::thread producer_worker_;
};

// coro_rpc-facing adapter. Keeping RPC signatures here lets both the Master
// server and integration tests register the exact production endpoints.
class CfmRpcService final {
   public:
    explicit CfmRpcService(std::shared_ptr<CfmService> service)
        : service_(std::move(service)) {}

    bool Authenticate(const std::string& auth_token);
    bool Send(const std::string& node_id, const std::string& method,
              const std::string& payload, const std::string& auth_token);
    // The boolean explicitly distinguishes a rejected request from an
    // authenticated queue that currently has no policy.
    std::pair<bool, std::optional<std::pair<uint64_t, std::string>>> Receive(
        const std::string& method, const std::string& node_id,
        const std::string& auth_token);
    bool Acknowledge(const std::string& node_id, uint64_t delivery_id,
                     bool success, const std::string& auth_token);
    bool EnqueuePolicy(const std::string& node_id, const std::string& payload,
                       const std::string& auth_token);

   private:
    std::shared_ptr<CfmService> service_;
};

}  // namespace mooncake::io_pattern
