#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
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

    // Outcome of one report-driven policy cycle. The cycle aggregates the
    // merged collector snapshot, runs analysis -> decision and executes the
    // three storage-safe flows (eviction, prefetch, admission); this report
    // lets the owning process surface each execution in its own metrics.
    struct ReportDrivenCycleReport {
        uint64_t cycle_id{0};
        size_t keys_analyzed{0};
        uint64_t analysis_elapsed_us{0};
        bool degraded{false};
        // Eviction dimension (derived from merged storage watermarks).
        CacheTier eviction_tier{CacheTier::kL1Host};
        uint64_t eviction_target_bytes{0};
        size_t eviction_candidates{0};
        ErrorCode eviction_status{ErrorCode::OK};
        // True when the eviction request came from the cold-data driver
        // (analysis-selected idle keys) rather than a storage-pressure
        // watermark request.
        bool cold_eviction{false};
        // True when the eviction dimension was a demotion pass instead: the
        // candidates were copied down to LOCAL_DISK and kept their MEMORY
        // replica, so eviction_target_bytes is a demotion budget and
        // eviction_status is the demotion outcome. Nothing was reclaimed.
        bool tier_down{false};
        // Prefetch dimension (derived from merged prefix-affinity keys).
        size_t prefetch_candidates{0};
        ErrorCode prefetch_status{ErrorCode::OK};
        // Admission dimension (derived from merged lower-tier hot keys).
        size_t admission_candidates{0};
        size_t admissions_admitted{0};
        ErrorCode admission_status{ErrorCode::OK};
        // Dimensions whose storage handler declined to act because the
        // primitive cannot run in the current mode (promotion disabled, no
        // lower-tier source replica, HBM refusal). Such a skip is expected and
        // is not counted as a policy failure.
        size_t skipped_dimensions{0};
    };
    using ReportDrivenObserver =
        std::function<void(const ReportDrivenCycleReport&)>;

    struct Config {
        IoPatternCollectorImpl::Config collector;
        uint64_t analysis_window_ns{60'000'000'000ULL};
        uint64_t analysis_timeout_us{500'000};
        size_t max_analysis_keys{100'000};
        size_t feedback_window{60};
        size_t report_capacity{4096};
        size_t report_per_tenant_capacity{0};
        size_t max_pending_prefetches{4096};
        size_t max_pending_admissions{4096};
        MetricBatchSink report_sink;
        LegacyFallback legacy_fallback{LegacyFallback::kLru};
        // Report-driven execution: after each client report (snapshot or
        // metric batch) is merged, the runtime runs its own full
        // Collector -> Analyzer -> PolicyEngine -> execution cycle. The
        // eviction dimension is triggered by merged storage watermarks; the
        // prefetch and admission dimensions are derived from the merged key
        // set. Defaults keep the worker off for pure collector/reporter
        // runtimes; MasterService enables it on the SubMaster that owns the
        // reported keys.
        bool report_driven_execution{false};
        // Storage metric ratio (L1Host memory) at or above which the merged
        // snapshot is considered under pressure and an eviction cycle is
        // executed. Mirrors the master's own high-watermark trigger.
        float report_eviction_high_ratio{0.80F};
        // Storage ratio at or above which admission into the head tier is
        // refused. MasterService derives it from the same eviction high
        // watermark so admission stops before eviction starts.
        float admission_watermark_ratio{0.90F};
        // Minimum accesses in the rolling window before admission into a
        // non-HBM tier. Defaults to the ops default (2); 1 restores the previous
        // admit-on-first-sight behaviour.
        uint32_t admission_frequency_threshold{2};
        // After an eviction cycle the tier is considered relieved once this
        // ratio is reached; eviction target bytes are derived as
        // (peak_ratio - report_eviction_target_ratio) * capacity_bytes.
        float report_eviction_target_ratio{0.70F};
        // Cold-data eviction driver. When enabled, a report-driven cycle that
        // sees no storage-pressure request (merged L1 ratio below the high
        // watermark) still runs a bounded eviction of the coldest keys, so
        // eviction is driven by cold/hot analysis rather than only by memory
        // pressure. Candidate selection and handler execution are identical to
        // pressure eviction; the cycle report marks `cold_eviction=true` so
        // logs/metrics can distinguish the two drivers.
        bool report_driven_cold_eviction{false};
        // Only keys idle (idle_time_us) at least this long are eligible for a
        // cold-eviction pass. 0 disables the idle gate.
        uint64_t report_driven_cold_idle_threshold_us{0};
        // Max bytes a single cold-eviction pass may request (per drained
        // cycle). 0 disables the cold driver regardless of the enable flag.
        uint64_t report_driven_cold_eviction_bytes{0};
        // Policy-driven tier down: the below-watermark placement action. When a
        // report-driven cycle finds no reclaim request (neither storage pressure
        // nor the cold-eviction driver) it spends this budget copying the coldest
        // in-memory keys down to LOCAL_DISK while keeping their MEMORY replica,
        // so a later reclaim of those keys can discard them safely instead of
        // paying for the copy then. A demotion frees nothing, so a reclaim always
        // wins the cycle, and the cycle report marks `tier_down=true` so a
        // demotion is never counted as an eviction. There is no separate enable
        // flag: 0 keeps the driver off, so this budget is the whole control.
        uint64_t tier_down_bytes_per_cycle{0};
        // Periodic driver tick, in milliseconds. Reports only flow while the
        // workload does, but tier down and cold eviction are exactly the drivers
        // that must act on an idle cluster, so when either is configured the
        // report-driven worker also wakes on this interval. A tick cycle
        // deliberately ignores recorded storage pressure: the master's own
        // watermark thread owns pressure reclaims (and the ratio it records
        // lingers in the collector until the next breach), so a tick that also
        // reclaimed would evict twice for one breach. 0 disables the tick.
        uint64_t tick_interval_ms{10'000};
        // Optional per-cycle observer used to surface executions in process
        // metrics (e.g. MasterMetricManager). Never called from the report
        // data path; only from the background cycle worker.
        ReportDrivenObserver report_driven_observer;
    };

    explicit IoPatternRuntime(Handlers handlers);
    IoPatternRuntime(Handlers handlers, Config config);
    ~IoPatternRuntime();

    void ReportInferenceMetrics(const InferenceMetrics& metrics);
    void RecordAccess(const std::string& key, const AccessRecord& record);
    void RecordStorageMetric(const StorageMetric& metric);
    void MergeSnapshot(const IoPatternSnapshot& snapshot);
    // Applies an authoritative tier transition reported by the owner of the
    // replica metadata. See IoPatternCollectorImpl::RecordTierEvent.
    void RecordTierEvent(const CacheEvent& event);
    bool FlushReports();
    void StopReports();

    PolicyExecutionStatus Execute(
        CacheTier eviction_tier, uint64_t eviction_bytes,
        const TraceHistory& trace,
        const std::vector<ObjectRef>& admissions = {},
        const std::string& session_id = {});
    // Requests one report-driven cycle after merged report data. Coalesces:
    // reports that arrive while a cycle is pending or running only mark the
    // cycle dirty; the single background worker runs at most one full cycle
    // per drain. Non-blocking for the report path.
    void RequestReportDrivenExecution();
    // Blocks until the background report-driven worker has drained all
    // currently pending reports (no pending flag and no cycle in flight).
    // Used by benchmarks/tests that must read deterministic counters after a
    // known report burst. No-op when report-driven execution is disabled.
    void WaitForReportDrivenIdle();
    bool report_driven_execution() const {
        return config_.report_driven_execution;
    }
    // Runs Collector -> Analyzer -> PolicyEngine without invoking the local
    // storage handlers. Callers use Plan when they need the raw policy result
    // (for example the local eviction watermark path, observability or tests);
    // Store data paths continue to use Execute().
    PolicyResult Plan(CacheTier eviction_tier, uint64_t eviction_bytes,
                      const TraceHistory& trace,
                      const std::vector<ObjectRef>& admissions = {},
                      const std::string& session_id = {});
    // Applies a CFM-issued command through the same storage handlers as a
    // locally planned policy. This is the CFM-to-Store execution endpoint used
    // by the embedded SubMaster CFM receiver.
    ErrorCode ExecuteCommand(const PolicyCommand& command);
    bool ScheduleAdmission(ObjectRef object, CacheTier target_tier,
                           std::string session_id = {});

    void RecordFeedback(PolicyFeedbackSample sample);
    PolicyFeedbackStats FeedbackSnapshot() const;
    IoPatternSnapshot Snapshot() const;
    // With no explicit window, QPS is averaged over this runtime's lifetime.
    IoPatternObservabilitySnapshot ObservabilitySnapshot(
        double window_seconds = 0.0) const;
    bool degraded() const;

   private:
    PatternResult AnalyzeWithinBudget(const IoPatternSnapshot& snapshot,
                                       bool& degraded);
    struct PlannedPolicy {
        IoPatternSnapshot snapshot;
        PolicyResult result;
        bool analysis_degraded{false};
        uint64_t analysis_elapsed_us{0};
    };
    PlannedPolicy BuildPolicy(CacheTier eviction_tier,
                              uint64_t eviction_bytes,
                              const TraceHistory& trace,
                              const std::vector<ObjectRef>& admissions,
                              const std::string& session_id,
                              uint64_t min_idle_time_us = 0,
                              bool tier_down = false);
    void AdmissionWorker();
    ErrorCode ExecuteAdmission(const ObjectRef& object, CacheTier target_tier,
                               const std::string& session_id);

    // Report-driven cycle internals (single background worker).
    void ReportDrivenWorker();
    // allow_pressure=false is the periodic-tick path: the pressure request is
    // skipped so only the below-watermark drivers act.
    void RunReportDrivenCycle(bool allow_pressure);
    // Runs the executor over an already planned policy and records the shared
    // outcome bookkeeping (policy failure/success, degradation, pending
    // prefetch set and feedback). Used by both Execute() and the
    // report-driven cycle so the two paths stay semantically identical.
    PolicyExecutionStatus CommitPolicy(PlannedPolicy& planned);
    static void DeriveEvictionRequest(const IoPatternSnapshot& snapshot,
                                      float high_ratio, float target_ratio,
                                      CacheTier& eviction_tier,
                                      uint64_t& eviction_bytes);
    static TraceHistory DeriveTraceHistory(const IoPatternSnapshot& snapshot);
    static std::vector<ObjectRef> DeriveAdmissionCandidates(
        const IoPatternSnapshot& snapshot);

    struct PendingAdmission {
        ObjectRef object;
        CacheTier target_tier{CacheTier::kL1Host};
        std::string session_id;
    };

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
    const std::chrono::steady_clock::time_point started_at_{
        std::chrono::steady_clock::now()};
    mutable std::mutex feedback_state_mutex_;
    std::unordered_set<ObjectRef, ObjectRefHash> pending_prefetches_;
    uint64_t feedback_accesses_{0};
    uint64_t feedback_hits_{0};
    float previous_hit_rate_{0.0F};
    // Shared with a timed-out detached analyzer so runtime teardown cannot
    // leave a worker holding a pointer into a destroyed runtime instance.
    std::shared_ptr<std::atomic<bool>> analysis_in_flight_{
        std::make_shared<std::atomic<bool>>(false)};
    std::mutex admission_mutex_;
    std::condition_variable admission_condition_;
    std::deque<PendingAdmission> pending_admissions_;
    std::thread admission_worker_;
    bool admission_stopping_{false};

    // Report-driven cycle worker state. Guarded by report_mutex_; the worker
    // drains the pending flag and runs one cycle, then loops so reports that
    // arrived during the cycle coalesce into the next drain.
    std::mutex report_mutex_;
    std::condition_variable report_condition_;
    std::thread report_worker_;
    bool report_pending_{false};
    bool report_stopping_{false};
    bool report_worker_busy_{false};
    uint64_t report_cycle_id_{0};
};

}  // namespace mooncake::io_pattern
