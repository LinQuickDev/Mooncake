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
    Impl(std::string endpoint, std::chrono::milliseconds default_timeout)
        : endpoint_(std::move(endpoint)), default_timeout_(default_timeout) {}

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
    std::chrono::milliseconds default_timeout_;
    std::mutex mutex_;
    std::unordered_map<
        int64_t,
        std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>>
        pools_;
};

CoroRpcCfmTransport::CoroRpcCfmTransport(
    std::string endpoint, std::chrono::milliseconds default_timeout)
    : impl_(std::make_unique<Impl>(std::move(endpoint), default_timeout)) {}

CoroRpcCfmTransport::~CoroRpcCfmTransport() = default;

bool CoroRpcCfmTransport::Send(std::string_view method,
                               std::string_view payload,
                               std::chrono::milliseconds timeout) {
    if (!impl_) return false;
    // Reports are addressed to the SubMaster endpoint this transport was
    // created for; there is no separate node identity or credential.
    const auto result = impl_->Invoke<&CfmRpcService::Send, bool>(
        timeout, std::string(method), std::string(payload), std::string{});
    return result && *result;
}

bool InProcessCfmRpcTransport::Send(std::string_view method,
                                    std::string_view payload,
                                    std::chrono::milliseconds) {
    SendHandler handler;
    {
        std::lock_guard lock(mutex_);
        handler = send_handler_;
    }
    return !handler || handler(method, payload);
}

void InProcessCfmRpcTransport::SetSendHandler(SendHandler handler) {
    std::lock_guard lock(mutex_);
    send_handler_ = std::move(handler);
}

bool CfmRpcChannel::SendSnapshot(const IoPatternSnapshot& snapshot) {
    if (!transport_ || !codec_) return false;
    return transport_->Send("report_snapshot", codec_->EncodeSnapshot(snapshot),
                            config_.timeout);
}

bool CfmRpcChannel::SendMetricBatch(const MetricBatch& batch) {
    if (!transport_ || !codec_) return false;
    return transport_->Send("report_metric_batch", codec_->EncodeMetricBatch(batch),
                            config_.timeout);
}

ErrorCode CfmRpcChannel::ExecutePrefetch(const PrefetchPlan& plan) {
    if (!transport_ || !codec_) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    }
    return transport_->Send("execute_prefetch", codec_->EncodePrefetch(plan),
                            config_.timeout)
               ? ErrorCode::OK
               : ErrorCode::RPC_TIMEOUT;
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

bool CfmChannelPool::SendMetricBatch(const MetricBatch& batch) {
    for (size_t attempt = 0; attempt < channels_.size(); ++attempt) {
        auto channel = Next();
        if (channel && channel->SendMetricBatch(batch)) return true;
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
