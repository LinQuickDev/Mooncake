#include "io_pattern/observability.h"

#include <algorithm>

namespace mooncake::io_pattern {

void IoPatternObservability::RecordCollectLatency(uint64_t value) {
    std::lock_guard lock(mutex_);
    values_.collect_latency_us = std::max(values_.collect_latency_us, value);
}

void IoPatternObservability::RecordAnalyzeLatency(uint64_t value) {
    std::lock_guard lock(mutex_);
    values_.analyze_latency_us = std::max(values_.analyze_latency_us, value);
}

void IoPatternObservability::RecordPolicyDecision(bool strategy_hit) {
    std::lock_guard lock(mutex_);
    ++values_.policy_decisions;
    ++values_.strategy_trials;
    if (strategy_hit) ++values_.strategy_hits;
}

void IoPatternObservability::RecordFalsePositive() {
    std::lock_guard lock(mutex_);
    ++values_.false_positives;
}

void IoPatternObservability::RecordDegrade() {
    std::lock_guard lock(mutex_);
    ++values_.degrade_count;
}

void IoPatternObservability::RecordReportDrop(uint64_t count) {
    std::lock_guard lock(mutex_);
    values_.report_drop_count += count;
}

IoPatternObservabilitySnapshot IoPatternObservability::Snapshot() const {
    std::lock_guard lock(mutex_);
    auto result = values_;
    result.strategy_hit_rate = result.strategy_trials == 0
                                   ? 0.0F
                                   : static_cast<float>(result.strategy_hits) /
                                         static_cast<float>(result.strategy_trials);
    result.false_positive_rate = result.strategy_trials == 0
                                     ? 0.0F
                                     : static_cast<float>(result.false_positives) /
                                           static_cast<float>(result.strategy_trials);
    return result;
}

IoPatternObservabilitySnapshot IoPatternObservability::Snapshot(
    double window_seconds) const {
    auto result = Snapshot();
    if (window_seconds > 0.0) {
        result.policy_decision_qps =
            static_cast<float>(result.policy_decisions / window_seconds);
    }
    return result;
}

}  // namespace mooncake::io_pattern
