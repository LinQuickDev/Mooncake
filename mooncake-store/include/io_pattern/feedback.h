#pragma once

#include <cstddef>
#include <deque>
#include <mutex>
#include <functional>
#include <utility>

#include "policy_strategies.h"

namespace mooncake::io_pattern {

struct PolicyFeedbackSample {
    float hit_rate_delta{0.0F};
    float eviction_churn{0.0F};
    float ttft_delta{0.0F};
    float prefetch_accuracy{0.0F};
};

struct PolicyFeedbackStats {
    float hit_rate_delta{0.0F};
    float eviction_churn{0.0F};
    float ttft_delta{0.0F};
    float prefetch_accuracy{0.0F};
    size_t samples{0};
};

class PolicyFeedbackWindow final {
   public:
    explicit PolicyFeedbackWindow(size_t capacity = 60) : capacity_(capacity) {}
    void Record(PolicyFeedbackSample sample);
    PolicyFeedbackStats Snapshot() const;

   private:
    const size_t capacity_;
    mutable std::mutex mutex_;
    std::deque<PolicyFeedbackSample> samples_;
};

// Conservative tuner: after three consecutive negative hit-rate windows,
// reduce frequency weight and increase idle weight to curb cache churn.
class AdaptivePolicyTuner final {
   public:
    explicit AdaptivePolicyTuner(size_t negative_windows = 3)
        : negative_windows_(negative_windows) {}
    bool Tune(const PolicyFeedbackStats& stats,
              ScoreBasedEvictionConfig& config);
    bool conservative() const { return conservative_; }
    void SetPersistenceCallback(std::function<void(const ScoreBasedEvictionConfig&)>
                                    callback) {
        persistence_ = std::move(callback);
    }

   private:
    const size_t negative_windows_;
    size_t negative_streak_{0};
    bool conservative_{false};
    std::function<void(const ScoreBasedEvictionConfig&)> persistence_;
};

}  // namespace mooncake::io_pattern
