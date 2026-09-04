#pragma once

#include <memory>
#include <mutex>

#include "../eviction_strategy.h"
#include "ops.h"

namespace mooncake::io_pattern {

class LegacyEvictionOps final : public EvictionOps {
   public:
    explicit LegacyEvictionOps(std::shared_ptr<mooncake::EvictionStrategy> strategy)
        : strategy_(std::move(strategy)) {}

    EvictionPlan Evaluate(const PolicyContext& context, CacheTier tier,
                          uint64_t target_bytes) const override;

   private:
    std::shared_ptr<mooncake::EvictionStrategy> strategy_;
    mutable std::mutex mutex_;
};

}  // namespace mooncake::io_pattern
