#include "io_pattern/rpc_transport.h"

namespace mooncake::io_pattern {

bool InProcessCfmRpcTransport::Authenticate(std::string_view token) {
    std::lock_guard lock(mutex_);
    authenticated_ = token == auth_token_;
    return authenticated_;
}

bool InProcessCfmRpcTransport::Send(std::string_view method,
                                    std::string_view payload,
                                    std::chrono::milliseconds) {
    std::lock_guard lock(mutex_);
    if (!authenticated_) return false;
    return !send_handler_ || send_handler_(method, payload);
}

std::optional<std::string> InProcessCfmRpcTransport::Receive(
    std::string_view method, std::chrono::milliseconds) {
    std::lock_guard lock(mutex_);
    if (!authenticated_ || method != "poll_policy" || policies_.empty()) {
        return std::nullopt;
    }
    auto payload = std::move(policies_.front());
    policies_.pop();
    return payload;
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

std::optional<PolicyCommand> CfmRpcChannel::PollPolicy() {
    if (!transport_ || !codec_ || !EnsureAuthenticated()) return std::nullopt;
    const auto payload = transport_->Receive("poll_policy", config_.timeout);
    return payload ? codec_->DecodePolicy(*payload) : std::nullopt;
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

std::optional<PolicyCommand> CfmChannelPool::PollPolicy() {
    for (size_t attempt = 0; attempt < channels_.size(); ++attempt) {
        auto channel = Next();
        if (!channel) continue;
        auto command = channel->PollPolicy();
        if (command) return command;
    }
    return std::nullopt;
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
