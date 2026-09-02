#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "collector_impl.h"
#include "degrading_policy_engine.h"
#include "feedback.h"
#include "legacy_eviction_ops.h"
#include "observability.h"
#include "policy_engine.h"
#include "resilient_analyzer.h"
#include "sliding_window_analyzer.h"
#include "tier_executor.h"

namespace mooncake::io_pattern {

// Owns the Store-side IO Pattern pipeline. Producers only record observations;
// policy evaluation and storage operations run through this explicit runtime
// seam so collection never blocks the data path.
class IoPatternRuntime final {
   public:
    enum class LegacyFallback { kLru, kFifo };
    struct Handlers {
        EvictionHandler eviction;
        PrefetchHandler prefetch;
        AdmissionHandler admission;
    };

    struct Config {
        IoPatternCollectorImpl::Config collector;
        uint64_t analysis_window_ns{60'000'000'000ULL};
        uint64_t analysis_timeout_us{500'000};
        size_t max_analysis_keys{100'000};
        size_t feedback_window{60};
        size_t report_capacity{4096};
        size_t report_per_tenant_capacity{0};
        size_t max_pending_prefetches{4096};
        MetricBatchSink report_sink;
        LegacyFallback legacy_fallback{LegacyFallback::kLru};
    };

    explicit IoPatternRuntime(Handlers handlers, Config config = {});
    ~IoPatternRuntime();

    void ReportInferenceMetrics(const InferenceMetrics& metrics);
    void RecordAccess(const std::string& key, const AccessRecord& record);
    void RecordStorageMetric(const StorageMetric& metric);
    void MergeSnapshot(const IoPatternSnapshot& snapshot);

    PolicyExecutionStatus Execute(
        CacheTier eviction_tier, uint64_t eviction_bytes,
        const TraceHistory& trace,
        const std::vector<ObjectRef>& admissions = {},
        const std::string& session_id = {});
    // Applies a CFM-issued command through the same storage handlers as a
    // locally planned policy. This is the CFM-to-Store execution endpoint.
    ErrorCode ExecuteCommand(const PolicyCommand& command);

    void RecordFeedback(PolicyFeedbackSample sample);
    IoPatternSnapshot Snapshot() const;
    IoPatternObservabilitySnapshot ObservabilitySnapshot(
        double window_seconds = 0.0) const;
    bool degraded() const;

   private:
    PatternResult AnalyzeWithinBudget(const IoPatternSnapshot& snapshot,
                                      bool& degraded);

    Config config_;
    std::shared_ptr<IoPatternReporter> reporter_;
    std::shared_ptr<IoPatternCollectorImpl> collector_;
    std::shared_ptr<ResilientAnalyzer> analyzer_;
    std::shared_ptr<WorkloadPolicyEngine> workload_policy_;
    std::shared_ptr<DegradingPolicyEngine> policy_;
    TierOperationExecutor executor_;
    PolicyFeedbackWindow feedback_;
    AdaptivePolicyTuner tuner_;
    IoPatternObservability observability_;
    mutable std::mutex feedback_state_mutex_;
    std::unordered_set<ObjectRef, ObjectRefHash> pending_prefetches_;
    uint64_t feedback_accesses_{0};
    uint64_t feedback_hits_{0};
    float previous_hit_rate_{0.0F};
    // Shared with a timed-out detached analyzer so runtime teardown cannot
    // leave a worker holding a pointer into a destroyed runtime instance.
    std::shared_ptr<std::atomic<bool>> analysis_in_flight_{
        std::make_shared<std::atomic<bool>>(false)};
};

}  // namespace mooncake::io_pattern
