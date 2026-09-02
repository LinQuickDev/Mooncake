#pragma once

#include <cstdint>
#include <memory>
#include <mutex>

#include "cfm_channel.h"

namespace mooncake::io_pattern {

struct CfmRetryConfig {
    uint32_t max_retries{3};
    uint32_t degrade_after_failures{3};
};

// Adds bounded retry and health tracking to any concrete CFM transport.
class ResilientCfmChannel final : public CfmChannel {
   public:
    ResilientCfmChannel(std::shared_ptr<CfmChannel> delegate,
                        CfmRetryConfig config = {})
        : delegate_(std::move(delegate)), config_(config) {}

    bool SendSnapshot(const IoPatternSnapshot& snapshot) override;
    std::optional<PolicyCommand> PollPolicy() override;
    ErrorCode ExecutePrefetch(const PrefetchPlan& plan) override;

    bool degraded() const;
    uint64_t consecutive_failures() const;

   private:
    template <typename Operation>
    bool Retry(Operation&& operation);
    void RecordSuccess();
    void RecordFailure();

    std::shared_ptr<CfmChannel> delegate_;
    CfmRetryConfig config_;
    mutable std::mutex mutex_;
    uint64_t consecutive_failures_{0};
    bool degraded_{false};
};

}  // namespace mooncake::io_pattern
