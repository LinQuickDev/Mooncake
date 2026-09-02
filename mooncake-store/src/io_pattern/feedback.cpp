#include "io_pattern/feedback.h"

namespace mooncake::io_pattern {

void PolicyFeedbackWindow::Record(PolicyFeedbackSample sample) {
    std::lock_guard lock(mutex_);
    if (capacity_ == 0) return;
    if (samples_.size() == capacity_) samples_.pop_front();
    samples_.push_back(sample);
}

PolicyFeedbackStats PolicyFeedbackWindow::Snapshot() const {
    std::lock_guard lock(mutex_);
    PolicyFeedbackStats stats;
    stats.samples = samples_.size();
    for (const auto& sample : samples_) {
        stats.hit_rate_delta += sample.hit_rate_delta;
        stats.eviction_churn += sample.eviction_churn;
        stats.ttft_delta += sample.ttft_delta;
        stats.prefetch_accuracy += sample.prefetch_accuracy;
    }
    if (stats.samples != 0) {
        const float divisor = static_cast<float>(stats.samples);
        stats.hit_rate_delta /= divisor;
        stats.eviction_churn /= divisor;
        stats.ttft_delta /= divisor;
        stats.prefetch_accuracy /= divisor;
    }
    return stats;
}

bool AdaptivePolicyTuner::Tune(const PolicyFeedbackStats& stats,
                               ScoreBasedEvictionConfig& config) {
    if (stats.hit_rate_delta < 0.0F) {
        ++negative_streak_;
    } else {
        negative_streak_ = 0;
    }
    if (negative_windows_ == 0 || negative_streak_ < negative_windows_) {
        if (stats.eviction_churn > 0.5F || stats.prefetch_accuracy < 0.2F ||
            stats.ttft_delta > 0.1F) {
            conservative_ = true;
            config.prefix_weight *= 0.9F;
            config.recompute_weight *= 0.9F;
            if (persistence_) persistence_(config);
            return true;
        }
        return false;
    }
    config.frequency_weight *= 0.8F;
    config.idle_weight *= 1.1F;
    conservative_ = true;
    negative_streak_ = 0;
    if (persistence_) persistence_(config);
    return true;
}

}  // namespace mooncake::io_pattern
