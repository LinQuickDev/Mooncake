#include "io_pattern/cfm_service.h"

#include <algorithm>
#include <cmath>
#include <limits>

namespace mooncake::io_pattern {

CfmService::CfmService(std::shared_ptr<IoPatternRuntime> runtime,
                       std::string auth_token,
                       size_t policy_queue_capacity,
                       std::string producer_auth_token)
    : runtime_(std::move(runtime)),
      codec_(std::make_shared<CfmBinaryCodec>()),
      ingress_(runtime_, codec_),
      auth_token_(std::move(auth_token)),
      producer_auth_token_(std::move(producer_auth_token)),
      policy_queue_capacity_(policy_queue_capacity),
      producer_worker_(&CfmService::PolicyProducerWorker, this) {}

CfmService::~CfmService() {
    {
        std::lock_guard lock(producer_mutex_);
        producer_stopping_ = true;
        pending_metric_batches_.clear();
    }
    producer_cv_.notify_all();
    if (producer_worker_.joinable()) producer_worker_.join();
}

bool CfmService::Authenticate(std::string_view token) const {
    return AuthenticateNode(token) || AuthenticateProducer(token);
}

bool CfmService::AuthenticateNode(std::string_view token) const {
    return !auth_token_.empty() && token == auth_token_;
}

bool CfmService::AuthenticateProducer(std::string_view token) const {
    return !producer_auth_token_.empty() && producer_auth_token_ != auth_token_ &&
           token == producer_auth_token_;
}

bool CfmService::Send(std::string_view node_id, std::string_view method,
                      std::string_view payload, std::string_view token) {
    const bool executes_policy =
        method == "execute_policy" || method == "execute_prefetch";
    if (node_id.empty() ||
        (executes_policy ? !AuthenticateProducer(token)
                         : !AuthenticateNode(token))) {
        return false;
    }
    std::optional<MetricBatch> metric_batch;
    if (method == "report_metric_batch") {
        metric_batch = codec_->DecodeMetricBatch(std::string(payload));
        if (!metric_batch) return false;
    }
    if (!ingress_.Handle(method, payload, node_id)) return false;
    if (metric_batch) {
        SchedulePolicyProduction(std::string(node_id),
                                 std::move(*metric_batch));
    }
    return true;
}

std::optional<std::pair<uint64_t, std::string>> CfmService::PollPolicy(
    std::string_view node_id, std::string_view token) {
    if (!AuthenticateNode(token) || node_id.empty()) return std::nullopt;
    std::lock_guard lock(mutex_);
    auto it = policy_queues_.find(std::string(node_id));
    if (it == policy_queues_.end() || it->second.empty()) return std::nullopt;
    return it->second.front();
}

bool CfmService::AcknowledgePolicy(std::string_view node_id,
                                   uint64_t delivery_id, bool success,
                                   std::string_view token) {
    if (!AuthenticateNode(token) || node_id.empty() || delivery_id == 0) {
        return false;
    }
    std::lock_guard lock(mutex_);
    auto it = policy_queues_.find(std::string(node_id));
    if (it == policy_queues_.end() || it->second.empty() ||
        it->second.front().first != delivery_id) {
        return false;
    }
    if (!success) {
        if (it->second.size() > 1) {
            auto failed = std::move(it->second.front());
            it->second.pop_front();
            it->second.push_back(std::move(failed));
        }
        return true;
    }
    it->second.pop_front();
    --total_queued_policies_;
    if (it->second.empty()) policy_queues_.erase(it);
    return true;
}

bool CfmService::EnqueuePolicy(std::string node_id, std::string payload,
                               std::string_view token) {
    if (!AuthenticateProducer(token) || node_id.empty() || payload.empty() ||
        !codec_->DecodePolicy(payload)) {
        return false;
    }
    return EnqueueValidated(std::move(node_id), std::move(payload));
}

bool CfmService::EnqueueValidated(std::string node_id, std::string payload) {
    std::lock_guard lock(mutex_);
    auto& queue = policy_queues_[node_id];
    if (std::any_of(queue.begin(), queue.end(), [&](const auto& queued) {
            return queued.second == payload;
        })) {
        return true;
    }
    if (policy_queue_capacity_ == 0 ||
        total_queued_policies_ >= policy_queue_capacity_ ||
        queue.size() >= policy_queue_capacity_) {
        if (queue.empty()) policy_queues_.erase(node_id);
        return false;
    }
    if (next_delivery_id_ == 0) next_delivery_id_ = 1;
    const uint64_t delivery_id = next_delivery_id_++;
    queue.emplace_back(delivery_id, std::move(payload));
    ++total_queued_policies_;
    return true;
}

void CfmService::SchedulePolicyProduction(std::string node_id,
                                          MetricBatch batch) {
    {
        std::lock_guard lock(producer_mutex_);
        if (producer_stopping_ || policy_queue_capacity_ == 0 ||
            pending_metric_batches_.size() >= policy_queue_capacity_) {
            return;
        }
        pending_metric_batches_.emplace_back(std::move(node_id),
                                             std::move(batch));
    }
    producer_cv_.notify_one();
}

void CfmService::PolicyProducerWorker() {
    while (true) {
        std::pair<std::string, MetricBatch> pending;
        {
            std::unique_lock lock(producer_mutex_);
            producer_cv_.wait(lock, [this] {
                return producer_stopping_ || !pending_metric_batches_.empty();
            });
            if (producer_stopping_) return;
            pending = std::move(pending_metric_batches_.front());
            pending_metric_batches_.pop_front();
        }
        try {
            ProducePolicies(pending.first, pending.second);
        } catch (...) {
            // Policy production is best effort and must never terminate the
            // RPC service. The next metric batch will trigger a fresh plan.
        }
    }
}

void CfmService::ProducePolicies(std::string_view node_id,
                                 const MetricBatch& batch) {
    if (!runtime_ || node_id.empty()) return;

    std::unordered_map<ObjectRef, uint32_t, ObjectRefHash> match_lengths;
    std::string session_id;
    for (const auto& metric : batch.inference) {
        match_lengths[metric.object] = metric.match_length;
        if (session_id.empty()) session_id = metric.session_id;
    }
    TraceHistory trace;
    trace.events.reserve(batch.accesses.size());
    for (const auto& access : batch.accesses) {
        const auto match = match_lengths.find(access.object);
        trace.events.push_back(
            {.object = access.object,
             .observed_at_ns = access.observed_at_ns,
             .match_length = match == match_lengths.end() ? 0U
                                                           : match->second,
             .is_hit = access.is_hit});
    }

    bool produced_prefetch = false;
    for (const auto& storage : batch.storage) {
        if (static_cast<uint8_t>(storage.tier) >
            static_cast<uint8_t>(CacheTier::kL3NofSsd)) {
            continue;
        }
        if (!std::isfinite(storage.memory_used_ratio) ||
            storage.memory_used_ratio < 0.90F) {
            continue;
        }
        const float used_ratio =
            std::clamp(storage.memory_used_ratio, 0.0F, 1.0F);
        uint64_t target_bytes = 0;
        if (storage.capacity_bytes != 0) {
            const uint64_t low_watermark =
                storage.capacity_bytes - storage.capacity_bytes / 5;
            if (storage.used_bytes > low_watermark) {
                target_bytes = storage.used_bytes - low_watermark;
            }
        }
        if (target_bytes == 0) {
            uint64_t tier_bytes = 0;
            for (const auto& key : runtime_->Snapshot().keys) {
                if ((key.replica_tiers & CacheTierBit(storage.tier)) == 0) {
                    continue;
                }
                tier_bytes =
                    key.block_size >
                            std::numeric_limits<uint64_t>::max() - tier_bytes
                        ? std::numeric_limits<uint64_t>::max()
                        : tier_bytes + key.block_size;
            }
            const auto excess_ratio = std::max(
                0.0F, used_ratio - 0.80F);
            target_bytes = static_cast<uint64_t>(
                static_cast<long double>(tier_bytes) * excess_ratio /
                std::max(0.01F, used_ratio));
        }
        auto result = runtime_->Plan(storage.tier, target_bytes, trace, {},
                                     session_id);
        if (result.degraded) continue;
        if (!result.eviction.candidates.empty()) {
            EnqueueValidated(std::string(node_id),
                             codec_->EncodePolicy(result.eviction));
        }
        if (!produced_prefetch && !result.prefetch.candidates.empty()) {
            EnqueueValidated(std::string(node_id),
                             codec_->EncodePolicy(result.prefetch));
            produced_prefetch = true;
        }
    }

    if (!produced_prefetch && !trace.events.empty()) {
        auto result = runtime_->Plan(CacheTier::kL1Host, 0, trace, {},
                                     session_id);
        if (!result.degraded && !result.prefetch.candidates.empty()) {
            EnqueueValidated(std::string(node_id),
                             codec_->EncodePolicy(result.prefetch));
        }
    }
}

bool CfmRpcService::Authenticate(const std::string& auth_token) {
    return service_ && service_->Authenticate(auth_token);
}

bool CfmRpcService::Send(const std::string& node_id, const std::string& method,
                         const std::string& payload,
                         const std::string& auth_token) {
    return service_ && service_->Send(node_id, method, payload, auth_token);
}

std::pair<bool, std::optional<std::pair<uint64_t, std::string>>>
CfmRpcService::Receive(
    const std::string& method, const std::string& node_id,
    const std::string& auth_token) {
    if (!service_ || method != "poll_policy" ||
        !service_->AuthenticateNode(auth_token) || node_id.empty()) {
        return {false, std::nullopt};
    }
    return {true, service_->PollPolicy(node_id, auth_token)};
}

bool CfmRpcService::Acknowledge(const std::string& node_id,
                                uint64_t delivery_id, bool success,
                                const std::string& auth_token) {
    return service_ && service_->AcknowledgePolicy(
                           node_id, delivery_id, success, auth_token);
}

bool CfmRpcService::EnqueuePolicy(const std::string& node_id,
                                  const std::string& payload,
                                  const std::string& auth_token) {
    return service_ && service_->EnqueuePolicy(node_id, payload, auth_token);
}

}  // namespace mooncake::io_pattern
