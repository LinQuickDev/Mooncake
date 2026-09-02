#include "io_pattern/resilient_cfm_channel.h"

namespace mooncake::io_pattern {

template <typename Operation>
bool ResilientCfmChannel::Retry(Operation&& operation) {
    if (!delegate_) {
        RecordFailure();
        return false;
    }
    for (uint32_t attempt = 0; attempt <= config_.max_retries; ++attempt) {
        if (operation()) {
            RecordSuccess();
            return true;
        }
    }
    RecordFailure();
    return false;
}

bool ResilientCfmChannel::SendSnapshot(const IoPatternSnapshot& snapshot) {
    return Retry([&] { return delegate_->SendSnapshot(snapshot); });
}

std::optional<PolicyCommand> ResilientCfmChannel::PollPolicy() {
    if (!delegate_) {
        RecordFailure();
        return std::nullopt;
    }
    for (uint32_t attempt = 0; attempt <= config_.max_retries; ++attempt) {
        auto result = delegate_->PollPolicy();
        if (result.has_value()) {
            RecordSuccess();
            return result;
        }
    }
    RecordFailure();
    return std::nullopt;
}

ErrorCode ResilientCfmChannel::ExecutePrefetch(const PrefetchPlan& plan) {
    ErrorCode result = ErrorCode::RPC_FAIL;
    if (!delegate_) {
        RecordFailure();
        return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    }
    for (uint32_t attempt = 0; attempt <= config_.max_retries; ++attempt) {
        result = delegate_->ExecutePrefetch(plan);
        if (result == ErrorCode::OK) {
            RecordSuccess();
            return result;
        }
    }
    RecordFailure();
    return result;
}

void ResilientCfmChannel::RecordSuccess() {
    std::lock_guard lock(mutex_);
    consecutive_failures_ = 0;
    degraded_ = false;
}

void ResilientCfmChannel::RecordFailure() {
    std::lock_guard lock(mutex_);
    ++consecutive_failures_;
    if (consecutive_failures_ >= config_.degrade_after_failures) degraded_ = true;
}

bool ResilientCfmChannel::degraded() const {
    std::lock_guard lock(mutex_);
    return degraded_;
}

uint64_t ResilientCfmChannel::consecutive_failures() const {
    std::lock_guard lock(mutex_);
    return consecutive_failures_;
}

}  // namespace mooncake::io_pattern
