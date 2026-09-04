#include "io_pattern/rpc_transport.h"

#include <async_simple/coro/SyncAwait.h>
#include <ylt/coro_io/client_pool.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

#include <unordered_map>

#include "io_pattern/cfm_service.h"
#include "store_rpc_client_io_context.h"

namespace mooncake::io_pattern {

class CoroRpcCfmTransport::Impl {
   public:
    Impl(std::string endpoint, std::string node_id,
         std::chrono::milliseconds default_timeout)
        : endpoint_(std::move(endpoint)),
          node_id_(std::move(node_id)),
          default_timeout_(default_timeout) {}

    template <auto ServiceMethod, typename ReturnType, typename... Args>
    std::optional<ReturnType> Invoke(std::chrono::milliseconds timeout,
                                     Args&&... args) {
        auto pool = GetPool(timeout.count() > 0 ? timeout : default_timeout_);
        return async_simple::coro::syncAwait(
            [&]() -> async_simple::coro::Lazy<std::optional<ReturnType>> {
                auto request = co_await pool->send_request(
                    [&](coro_io::client_reuse_hint,
                        coro_rpc::coro_rpc_client& client) {
                        return client.send_request<ServiceMethod>(
                            std::forward<Args>(args)...);
                    });
                if (!request) co_return std::nullopt;
                auto response = co_await std::move(request.value());
                if (!response) co_return std::nullopt;
                co_return response->result();
            }());
    }

    std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>> GetPool(
        std::chrono::milliseconds timeout) {
        std::lock_guard lock(mutex_);
        const auto key = timeout.count();
        const auto existing = pools_.find(key);
        if (existing != pools_.end()) return existing->second;
        coro_io::client_pool<coro_rpc::coro_rpc_client>::pool_config config;
        config.client_config.request_timeout_duration = timeout;
        config.host_alive_detect_duration = std::chrono::seconds(0);
        auto pool = coro_io::client_pool<coro_rpc::coro_rpc_client>::create(
            endpoint_, config, GetStoreRpcClientIoContextPool());
        pools_.emplace(key, pool);
        return pool;
    }

    std::string endpoint_;
    std::string node_id_;
    std::chrono::milliseconds default_timeout_;
    std::mutex mutex_;
    std::string auth_token_;
    std::unordered_map<
        int64_t,
        std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>>
        pools_;
};

CoroRpcCfmTransport::CoroRpcCfmTransport(
    std::string endpoint, std::string node_id,
    std::chrono::milliseconds default_timeout)
    : impl_(std::make_unique<Impl>(std::move(endpoint), std::move(node_id),
                                   default_timeout)) {}

CoroRpcCfmTransport::~CoroRpcCfmTransport() = default;

bool CoroRpcCfmTransport::Authenticate(std::string_view token) {
    if (!impl_ || token.empty()) return false;
    const std::string wire_token(token);
    const auto result = impl_->Invoke<&CfmRpcService::Authenticate, bool>(
        impl_->default_timeout_, wire_token);
    if (!result || !*result) return false;
    std::lock_guard lock(impl_->mutex_);
    impl_->auth_token_ = wire_token;
    return true;
}

bool CoroRpcCfmTransport::Send(std::string_view method,
                               std::string_view payload,
                               std::chrono::milliseconds timeout) {
    if (!impl_) return false;
    std::string auth_token;
    {
        std::lock_guard lock(impl_->mutex_);
        auth_token = impl_->auth_token_;
    }
    if (auth_token.empty()) return false;
    const auto result = impl_->Invoke<&CfmRpcService::Send, bool>(
        timeout, impl_->node_id_, std::string(method), std::string(payload),
        auth_token);
    return result && *result;
}

CfmReceiveResult CoroRpcCfmTransport::Receive(
    std::string_view method, std::chrono::milliseconds timeout) {
    if (!impl_) return CfmReceiveResult::Error();
    std::string auth_token;
    {
        std::lock_guard lock(impl_->mutex_);
        auth_token = impl_->auth_token_;
    }
    if (auth_token.empty()) return CfmReceiveResult::Error();
    const auto result =
        impl_->Invoke<&CfmRpcService::Receive,
                      std::pair<
                          bool,
                          std::optional<std::pair<uint64_t, std::string>>>>(
            timeout, std::string(method), impl_->node_id_, auth_token);
    if (!result || !result->first) return CfmReceiveResult::Error();
    if (!result->second) return CfmReceiveResult::Empty();
    return CfmReceiveResult::Payload(std::move(result->second->second),
                                     result->second->first);
}

bool CoroRpcCfmTransport::Acknowledge(
    uint64_t delivery_id, bool success, std::chrono::milliseconds timeout) {
    if (!impl_ || delivery_id == 0) return false;
    std::string auth_token;
    {
        std::lock_guard lock(impl_->mutex_);
        auth_token = impl_->auth_token_;
    }
    if (auth_token.empty()) return false;
    const auto result = impl_->Invoke<&CfmRpcService::Acknowledge, bool>(
        timeout, impl_->node_id_, delivery_id, success, auth_token);
    return result && *result;
}

bool CoroRpcCfmTransport::EnqueuePolicy(
    std::string_view node_id, std::string_view payload,
    std::chrono::milliseconds timeout) {
    if (!impl_) return false;
    std::string auth_token;
    {
        std::lock_guard lock(impl_->mutex_);
        auth_token = impl_->auth_token_;
    }
    if (auth_token.empty()) return false;
    const auto result =
        impl_->Invoke<&CfmRpcService::EnqueuePolicy, bool>(
            timeout, std::string(node_id), std::string(payload), auth_token);
    return result && *result;
}

bool InProcessCfmRpcTransport::Authenticate(std::string_view token) {
    std::lock_guard lock(mutex_);
    authenticated_ = token == auth_token_;
    return authenticated_;
}

bool InProcessCfmRpcTransport::Send(std::string_view method,
                                    std::string_view payload,
                                    std::chrono::milliseconds) {
    SendHandler handler;
    {
        std::lock_guard lock(mutex_);
        if (!authenticated_) return false;
        handler = send_handler_;
    }
    return !handler || handler(method, payload);
}

CfmReceiveResult InProcessCfmRpcTransport::Receive(
    std::string_view method, std::chrono::milliseconds) {
    std::lock_guard lock(mutex_);
    if (!authenticated_ || method != "poll_policy") return CfmReceiveResult::Error();
    if (policies_.empty()) return CfmReceiveResult::Empty();
    auto payload = std::move(policies_.front());
    policies_.pop();
    return CfmReceiveResult::Payload(std::move(payload));
}

void InProcessCfmRpcTransport::EnqueuePolicy(std::string payload) {
    std::lock_guard lock(mutex_);
    policies_.push(std::move(payload));
}

void InProcessCfmRpcTransport::SetSendHandler(SendHandler handler) {
    std::lock_guard lock(mutex_);
    send_handler_ = std::move(handler);
}

bool CfmRpcChannel::EnsureAuthenticated() {
    std::lock_guard lock(authentication_mutex_);
    if (authenticated_) return true;
    authenticated_ = transport_ && transport_->Authenticate(config_.auth_token);
    return authenticated_;
}

bool CfmRpcChannel::SendSnapshot(const IoPatternSnapshot& snapshot) {
    if (!transport_ || !codec_ || !EnsureAuthenticated()) return false;
    return transport_->Send("report_snapshot", codec_->EncodeSnapshot(snapshot),
                            config_.timeout);
}

CfmPollResult CfmRpcChannel::PollPolicyResult() {
    if (!transport_ || !codec_ || !EnsureAuthenticated()) {
        return CfmPollResult::Error();
    }
    auto received = transport_->Receive("poll_policy", config_.timeout);
    if (received.status == CfmReceiveResult::Status::kEmpty) {
        return CfmPollResult::Empty();
    }
    if (received.status == CfmReceiveResult::Status::kError) {
        return CfmPollResult::Error();
    }
    auto command = codec_->DecodePolicy(received.payload);
    if (!command) {
        transport_->Acknowledge(received.delivery_id, false, config_.timeout);
        return CfmPollResult::Error();
    }
    return CfmPollResult::Command(std::move(*command), received.delivery_id);
}

bool CfmRpcChannel::AcknowledgePolicy(uint64_t delivery_id, bool success) {
    return transport_ && delivery_id != 0 && EnsureAuthenticated() &&
           transport_->Acknowledge(delivery_id, success, config_.timeout);
}

ErrorCode CfmRpcChannel::ExecutePrefetch(const PrefetchPlan& plan) {
    if (!transport_ || !codec_ || !EnsureAuthenticated()) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    }
    return transport_->Send("execute_prefetch", codec_->EncodePrefetch(plan),
                            config_.timeout)
               ? ErrorCode::OK
               : ErrorCode::RPC_TIMEOUT;
}

bool CfmRpcChannel::SendMetricBatch(const MetricBatch& batch) {
    if (!transport_ || !codec_ || !EnsureAuthenticated()) return false;
    return transport_->Send("report_metric_batch", codec_->EncodeMetricBatch(batch),
                            config_.timeout);
}

std::shared_ptr<CfmChannel> CfmChannelPool::Next() const {
    if (channels_.empty()) return nullptr;
    const auto index = next_.fetch_add(1, std::memory_order_relaxed) %
                       channels_.size();
    return channels_[index];
}

bool CfmChannelPool::SendSnapshot(const IoPatternSnapshot& snapshot) {
    for (size_t attempt = 0; attempt < channels_.size(); ++attempt) {
        auto channel = Next();
        if (channel && channel->SendSnapshot(snapshot)) return true;
    }
    return false;
}

CfmPollResult CfmChannelPool::PollPolicyResult() {
    bool saw_empty = false;
    for (size_t attempt = 0; attempt < channels_.size(); ++attempt) {
        auto channel = Next();
        if (!channel) continue;
        auto result = channel->PollPolicyResult();
        if (result.status == CfmPollResult::Status::kCommand) return result;
        saw_empty = saw_empty || result.status == CfmPollResult::Status::kEmpty;
    }
    return saw_empty ? CfmPollResult::Empty() : CfmPollResult::Error();
}

bool CfmChannelPool::AcknowledgePolicy(uint64_t delivery_id, bool success) {
    for (size_t attempt = 0; attempt < channels_.size(); ++attempt) {
        auto channel = Next();
        if (channel && channel->AcknowledgePolicy(delivery_id, success)) {
            return true;
        }
    }
    return false;
}

ErrorCode CfmChannelPool::ExecutePrefetch(const PrefetchPlan& plan) {
    ErrorCode last_error = ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    for (size_t attempt = 0; attempt < channels_.size(); ++attempt) {
        auto channel = Next();
        if (!channel) continue;
        last_error = channel->ExecutePrefetch(plan);
        if (last_error == ErrorCode::OK) return last_error;
    }
    return last_error;
}

}  // namespace mooncake::io_pattern
