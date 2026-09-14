#pragma once

#include <cstdint>
#include <mutex>

namespace mooncake::io_pattern {

struct IoPatternObservabilitySnapshot {
    uint64_t collect_latency_us{0};
    uint64_t analyze_latency_us{0};
    uint64_t policy_decisions{0};
    uint64_t strategy_hits{0};
    uint64_t strategy_trials{0};
    uint64_t false_positives{0};
    uint64_t degrade_count{0};
    uint64_t report_drop_count{0};
    float strategy_hit_rate{0.0F};
    float false_positive_rate{0.0F};
    float policy_decision_qps{0.0F};
};

// Thread-safe counters for IO Pattern operational metrics.
class IoPatternObservability final {
   public:
    void RecordCollectLatency(uint64_t latency_us);
    void RecordAnalyzeLatency(uint64_t latency_us);
    void RecordPolicyDecision(bool strategy_hit);
    void RecordFalsePositive();
    void RecordDegrade();
    void RecordReportDrop(uint64_t count = 1);
    IoPatternObservabilitySnapshot Snapshot() const;
    IoPatternObservabilitySnapshot Snapshot(double window_seconds) const;

   private:
    mutable std::mutex mutex_;
    IoPatternObservabilitySnapshot values_;
};

}  // namespace mooncake::io_pattern
