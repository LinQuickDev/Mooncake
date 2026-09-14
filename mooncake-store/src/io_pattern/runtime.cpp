#include "io_pattern/runtime.h"

#include <chrono>
#include <future>
#include <limits>
#include <thread>

namespace mooncake::io_pattern {
namespace {

// A dimension that reported UNAVAILABLE_IN_CURRENT_MODE declined to act because
// the storage primitive cannot run in this configuration. The policy cannot
// influence that outcome, so it is not a policy failure.
bool IsPolicyFailure(ErrorCode code) {
    return code != ErrorCode::OK &&
           code != ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}

}  // namespace

IoPatternRuntime::IoPatternRuntime(Handlers handlers)
    : IoPatternRuntime(std::move(handlers), Config{}) {}

IoPatternRuntime::IoPatternRuntime(Handlers handlers, Config config)
    : config_(config),
      executor_(std::move(handlers.eviction), std::move(handlers.prefetch),
                std::move(handlers.admission)),
      feedback_(config.feedback_window) {
    // A runtime always has an OOM guard even when a caller omits collector
    // limits.  The same bound is used by the bounded analyzer below.
    if (config_.collector.max_total_keys == 0) {
        config_.collector.max_total_keys = config_.max_analysis_keys;
    }
    if (config_.report_sink) {
        reporter_ = std::make_shared<IoPatternReporter>(
            config_.report_capacity, config_.report_sink,
            config_.report_per_tenant_capacity);
        reporter_->Start();
    }
    collector_ = std::make_shared<IoPatternCollectorImpl>(config_.collector,
                                                           reporter_);
    auto sliding = std::make_shared<SlidingWindowAnalyzer>(
        config.analysis_window_ns);
    analyzer_ = std::make_shared<ResilientAnalyzer>(std::move(sliding));
    workload_policy_ = std::make_shared<WorkloadPolicyEngine>(
        WorkloadType::kMixed, 3, config_.admission_watermark_ratio,
        config_.admission_frequency_threshold);
    std::shared_ptr<mooncake::EvictionStrategy> legacy_strategy;
    if (config_.legacy_fallback == LegacyFallback::kFifo) {
        legacy_strategy = std::make_shared<FIFOEvictionStrategy>();
    } else {
        legacy_strategy = std::make_shared<LRUEvictionStrategy>();
    }
    auto fallback = std::make_shared<ComposedPolicyEngine>(
        std::make_shared<LegacyEvictionOps>(std::move(legacy_strategy)),
        nullptr, std::make_shared<PrefixMatchAdmissionOps>());
    policy_ = std::make_shared<DegradingPolicyEngine>(workload_policy_, fallback);
    admission_worker_ = std::thread(&IoPatternRuntime::AdmissionWorker, this);
    if (config_.report_driven_execution) {
        report_worker_ =
            std::thread(&IoPatternRuntime::ReportDrivenWorker, this);
    }
}

IoPatternRuntime::~IoPatternRuntime() {
    {
        std::lock_guard lock(admission_mutex_);
        admission_stopping_ = true;
        pending_admissions_.clear();
    }
    admission_condition_.notify_all();
    if (admission_worker_.joinable()) admission_worker_.join();
    {
        std::lock_guard lock(report_mutex_);
        report_stopping_ = true;
        report_pending_ = false;
    }
    report_condition_.notify_all();
    if (report_worker_.joinable()) report_worker_.join();
    if (reporter_) reporter_->Stop();
}

void IoPatternRuntime::ReportInferenceMetrics(const InferenceMetrics& metrics) {
    const auto start = std::chrono::steady_clock::now();
    const auto dropped_before = collector_->dropped();
    collector_->ReportInferenceMetrics(metrics);
    observability_.RecordCollectLatency(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start)
            .count());
    const auto dropped_after = collector_->dropped();
    if (dropped_after > dropped_before)
        observability_.RecordReportDrop(dropped_after - dropped_before);
}

void IoPatternRuntime::RecordAccess(const std::string& key,
                                    const AccessRecord& record) {
    const auto start = std::chrono::steady_clock::now();
    const auto dropped_before = collector_->dropped();
    collector_->RecordAccess(key, record);
    observability_.RecordCollectLatency(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start)
            .count());
    const auto dropped_after = collector_->dropped();
    if (dropped_after > dropped_before)
        observability_.RecordReportDrop(dropped_after - dropped_before);

    PolicyFeedbackSample feedback;
    bool has_feedback = false;
    {
        std::lock_guard lock(feedback_state_mutex_);
        ++feedback_accesses_;
        feedback_hits_ += record.is_hit;
        ObjectRef object = record.object;
        if (!key.empty()) object.key = key;
        if (pending_prefetches_.erase(object) != 0) {
            feedback.prefetch_accuracy = record.is_hit ? 1.0F : 0.0F;
            has_feedback = true;
            if (!record.is_hit) observability_.RecordFalsePositive();
        }
        // A completed 64-access window is a stable, bounded source of actual
        // hit-rate deltas. TTFT remains supplied by the inference bridge via
        // the public RecordFeedback API.
        if (feedback_accesses_ >= 64) {
            const auto hit_rate = static_cast<float>(feedback_hits_) /
                                  static_cast<float>(feedback_accesses_);
            feedback.hit_rate_delta = hit_rate - previous_hit_rate_;
            previous_hit_rate_ = hit_rate;
            feedback_accesses_ = 0;
            feedback_hits_ = 0;
            has_feedback = true;
        }
    }
    if (has_feedback) RecordFeedback(feedback);
}

void IoPatternRuntime::RecordStorageMetric(const StorageMetric& metric) {
    const auto start = std::chrono::steady_clock::now();
    const auto dropped_before = collector_->dropped();
    collector_->RecordStorageMetric(metric);
    observability_.RecordCollectLatency(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start)
            .count());
    const auto dropped_after = collector_->dropped();
    if (dropped_after > dropped_before)
        observability_.RecordReportDrop(dropped_after - dropped_before);
}

void IoPatternRuntime::MergeSnapshot(const IoPatternSnapshot& snapshot) {
    const auto start = std::chrono::steady_clock::now();
    const auto dropped_before = collector_->dropped();
    collector_->MergeSnapshot(snapshot);
    observability_.RecordCollectLatency(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start)
            .count());
    const auto dropped_after = collector_->dropped();
    if (dropped_after > dropped_before) {
        observability_.RecordReportDrop(dropped_after - dropped_before);
    }
}

void IoPatternRuntime::RecordTierEvent(const CacheEvent& event) {
    collector_->RecordTierEvent(event);
}

bool IoPatternRuntime::FlushReports() { return collector_->FlushReports(); }

void IoPatternRuntime::StopReports() { collector_->StopReports(); }

PatternResult IoPatternRuntime::AnalyzeWithinBudget(
    const IoPatternSnapshot& snapshot, bool& degraded) {
    degraded = config_.max_analysis_keys != 0 &&
               snapshot.keys.size() > config_.max_analysis_keys;
    if (degraded || analysis_in_flight_->exchange(true, std::memory_order_acq_rel)) {
        degraded = true;
        return analyzer_->FallbackResult();
    }

    std::promise<PatternResult> promise;
    auto result = promise.get_future();
    auto analyzer = analyzer_;
    auto in_flight = analysis_in_flight_;
    std::thread([analyzer = std::move(analyzer), snapshot,
                 promise = std::move(promise), in_flight]() mutable {
        try {
            promise.set_value(analyzer->Analyze(snapshot));
        } catch (...) {
            promise.set_value(analyzer->FallbackResult());
        }
        in_flight->store(false, std::memory_order_release);
    }).detach();

    if (result.wait_for(std::chrono::microseconds(config_.analysis_timeout_us)) ==
        std::future_status::ready) {
        return result.get();
    }
    degraded = true;
    return analyzer_->FallbackResult();
}

PolicyExecutionStatus IoPatternRuntime::Execute(
    CacheTier eviction_tier, uint64_t eviction_bytes, const TraceHistory& trace,
    const std::vector<ObjectRef>& admissions, const std::string& session_id) {
    auto planned = BuildPolicy(eviction_tier, eviction_bytes, trace, admissions,
                               session_id);
    return CommitPolicy(planned);
}

PolicyExecutionStatus IoPatternRuntime::CommitPolicy(PlannedPolicy& planned) {
    const auto& snapshot = planned.snapshot;
    const auto& result = planned.result;
    auto status = executor_.Execute(result);
    status.degraded = status.degraded || result.degraded;
    const bool failed = IsPolicyFailure(status.eviction) ||
                        IsPolicyFailure(status.prefetch) || status.degraded;
    if (failed)
        policy_->RecordFailure();
    else
        policy_->RecordSuccess();
    if (status.degraded || policy_->degraded()) observability_.RecordDegrade();
    status.degraded = status.degraded || policy_->degraded();

    PolicyFeedbackSample feedback;
    bool has_feedback = false;
    {
        std::lock_guard lock(feedback_state_mutex_);
        for (const auto& candidate : result.prefetch.candidates) {
            if (config_.max_pending_prefetches == 0 ||
                pending_prefetches_.size() < config_.max_pending_prefetches) {
                pending_prefetches_.insert(candidate.object);
            }
        }
        if (!result.prefetch.candidates.empty() &&
            status.prefetch != ErrorCode::OK) {
            feedback.prefetch_accuracy = 0.0F;
            has_feedback = true;
        }
        if (!snapshot.keys.empty() && !result.eviction.candidates.empty()) {
            feedback.eviction_churn = static_cast<float>(
                result.eviction.candidates.size()) /
                                      static_cast<float>(snapshot.keys.size());
            has_feedback = true;
        }
    }
    if (has_feedback) RecordFeedback(feedback);
    return status;
}

PolicyResult IoPatternRuntime::Plan(
    CacheTier eviction_tier, uint64_t eviction_bytes, const TraceHistory& trace,
    const std::vector<ObjectRef>& admissions, const std::string& session_id) {
    return BuildPolicy(eviction_tier, eviction_bytes, trace, admissions,
                       session_id)
        .result;
}

IoPatternRuntime::PlannedPolicy IoPatternRuntime::BuildPolicy(
    CacheTier eviction_tier, uint64_t eviction_bytes, const TraceHistory& trace,
    const std::vector<ObjectRef>& admissions, const std::string& session_id,
    uint64_t min_idle_time_us, bool tier_down) {
    PlannedPolicy planned;
    planned.snapshot = collector_->GetSnapshot();
    const auto start = std::chrono::steady_clock::now();
    const auto analysis =
        AnalyzeWithinBudget(planned.snapshot, planned.analysis_degraded);
    planned.analysis_elapsed_us = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start)
            .count());
    observability_.RecordAnalyzeLatency(planned.analysis_elapsed_us);

    workload_policy_->SetWorkloadType(analysis.workload_type);
    workload_policy_->SetSessionWorkloads(analysis.sessions);
    workload_policy_->AdvanceTransitionWindow();
    planned.result = policy_->ExecutePolicy(
        PolicyContext{.snapshot = planned.snapshot,
                      .analysis = analysis,
                      .session_id = session_id,
                      .min_idle_time_us = min_idle_time_us,
                      .tier_down = tier_down},
        eviction_tier, eviction_bytes, CacheTier::kL1Host, trace, admissions);
    planned.result.degraded =
        planned.result.degraded || collector_->degraded() ||
        planned.analysis_degraded ||
        planned.analysis_elapsed_us > config_.analysis_timeout_us;
    observability_.RecordPolicyDecision(
        !planned.result.eviction.candidates.empty() ||
        !planned.result.prefetch.candidates.empty());
    return planned;
}

ErrorCode IoPatternRuntime::ExecuteCommand(const PolicyCommand& command) {
    PolicyResult result;
    if (const auto* eviction = std::get_if<EvictionPlan>(&command)) {
        result.eviction = *eviction;
    } else if (const auto* prefetch = std::get_if<PrefetchPlan>(&command)) {
        result.prefetch = *prefetch;
    } else {
        result.admissions.push_back(std::get<AdmissionResult>(command));
    }
    const auto status = executor_.Execute(result);
    if (status.degraded) {
        observability_.RecordDegrade();
        return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    }
    if (const auto* eviction = std::get_if<EvictionPlan>(&command)) {
        return status.eviction;
    }
    if (const auto* prefetch = std::get_if<PrefetchPlan>(&command)) {
        return status.prefetch;
    }
    return status.admissions.empty() ? ErrorCode::OK : status.admissions.front();
}

void IoPatternRuntime::RequestReportDrivenExecution() {
    if (!config_.report_driven_execution) return;
    {
        std::lock_guard lock(report_mutex_);
        if (report_stopping_) return;
        report_pending_ = true;
    }
    // notify_all: a concurrent WaitForReportDrivenIdle() must not swallow the
    // worker's wakeup (predicates re-check under the mutex either way).
    report_condition_.notify_all();
}

void IoPatternRuntime::WaitForReportDrivenIdle() {
    if (!config_.report_driven_execution) return;
    std::unique_lock lock(report_mutex_);
    report_condition_.wait(lock, [this] {
        return report_stopping_ || (!report_pending_ && !report_worker_busy_);
    });
}

void IoPatternRuntime::ReportDrivenWorker() {
    // Below-watermark drivers (tier down, cold eviction) must not depend on client
    // reports: reports only flow while there is traffic, so an idle cluster would
    // never pave cold data down or reclaim it. When either driver is configured
    // the worker wakes on a timer as well; clusters that configure neither pay no
    // periodic analysis, and a tick can never fire with a zero interval.
    const bool tick_enabled = config_.report_driven_execution &&
                              config_.tick_interval_ms != 0 &&
                              (config_.tier_down_bytes_per_cycle != 0 ||
                               config_.report_driven_cold_eviction);
    const auto tick_interval =
        std::chrono::milliseconds(static_cast<int64_t>(config_.tick_interval_ms));
    while (true) {
        bool tick_triggered = false;
        {
            std::unique_lock lock(report_mutex_);
            if (tick_enabled) {
                if (!report_condition_.wait_for(lock, tick_interval, [this] {
                        return report_stopping_ || report_pending_;
                    })) {
                    tick_triggered = true;
                }
            } else {
                report_condition_.wait(lock, [this] {
                    return report_stopping_ || report_pending_;
                });
            }
            if (report_stopping_) return;
            report_pending_ = false;
            report_worker_busy_ = true;
        }
        try {
            // A tick cycle ignores any recorded storage pressure on purpose: the
            // master's own watermark thread owns pressure reclaims, and the ratio
            // it records lingers in the collector until the next breach, so
            // honouring it here would reclaim the same excess a second time.
            RunReportDrivenCycle(/*allow_pressure=*/!tick_triggered);
        } catch (...) {
            policy_->RecordFailure();
            observability_.RecordDegrade();
        }
        {
            std::lock_guard lock(report_mutex_);
            report_worker_busy_ = false;
        }
        report_condition_.notify_all();
    }
}

void IoPatternRuntime::DeriveEvictionRequest(const IoPatternSnapshot& snapshot,
                                             float high_ratio,
                                             float target_ratio,
                                             CacheTier& eviction_tier,
                                             uint64_t& eviction_bytes) {
    // The Store-side eviction handler is tenant-qualified MEMORY (L1) quota
    // eviction; L2/L3 pressure is handled by the legacy NoF/SSD paths outside
    // the IO Pattern runtime. Restrict the report-driven eviction dimension to
    // host-memory watermarks so L2/L3 reports never route lower-tier keys into
    // the memory quota eviction handler.
    eviction_tier = CacheTier::kL1Host;
    eviction_bytes = 0;
    float peak_ratio = 0.0F;
    uint64_t capacity_bytes = 0;
    for (const auto& metric : snapshot.storage) {
        if (metric.tier != CacheTier::kL1Host) continue;
        if (metric.memory_used_ratio > peak_ratio) {
            peak_ratio = metric.memory_used_ratio;
            capacity_bytes = metric.capacity_bytes;
        }
    }
    // No merged host-memory watermark, or below the high watermark: the merged
    // snapshot does not indicate pressure, so the eviction dimension produces
    // no action (the prefetch/admission dimensions are still evaluated).
    if (peak_ratio < high_ratio) return;
    // An eviction plan needs L1 keys to select candidates from. A report that
    // only carries storage watermarks (no key observations for this tier)
    // would otherwise execute an empty eviction plan through the handler on
    // every cycle.
    bool any_l1_keys = false;
    for (const auto& key : snapshot.keys) {
        if ((key.replica_tiers & CacheTierBit(CacheTier::kL1Host)) != 0) {
            any_l1_keys = true;
            break;
        }
    }
    if (!any_l1_keys) return;
    if (capacity_bytes == 0) {
        for (const auto& metric : snapshot.storage) {
            if (metric.tier == CacheTier::kL1Host &&
                metric.memory_used_ratio == peak_ratio &&
                metric.used_bytes != 0) {
                capacity_bytes = static_cast<uint64_t>(
                    static_cast<double>(metric.used_bytes) /
                    static_cast<double>(metric.memory_used_ratio));
                break;
            }
        }
    }
    const double reclaim_fraction =
        static_cast<double>(peak_ratio) - static_cast<double>(target_ratio);
    if (reclaim_fraction <= 0.0) return;
    const double target = reclaim_fraction * static_cast<double>(capacity_bytes);
    eviction_bytes = target > 0.0
                         ? static_cast<uint64_t>(target)
                         : (capacity_bytes > 0 ? capacity_bytes / 10 : 0);
}

TraceHistory IoPatternRuntime::DeriveTraceHistory(
    const IoPatternSnapshot& snapshot) {
    // Report-driven prefetch input: keys that were recently served as hits and
    // still have a lower-tier replica are the ones a promotion should bring
    // closer to the head. TraceBasedPrefetchOps re-applies its own
    // match-length / confidence gates against this candidate trace.
    TraceHistory trace;
    const auto now_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
    for (const auto& key : snapshot.keys) {
        if (!key.active || key.access_count_window == 0) continue;
        // Any replica deeper than host memory is a promotion candidate; local
        // disk is the only one the store can actually move up, but the others
        // still belong in the trace so the ops layer decides.
        const bool has_lower_replica =
            (key.replica_tiers & CacheTierBit(CacheTier::kLocalDisk)) != 0 ||
            (key.replica_tiers & CacheTierBit(CacheTier::kL2Segment)) != 0 ||
            (key.replica_tiers & CacheTierBit(CacheTier::kL3NofSsd)) != 0;
        if (!has_lower_replica) {
            continue;
        }
        trace.events.push_back(
            TraceEvent{.object = key.object,
                       .observed_at_ns = now_ns,
                       .match_length = key.match_length,
                       .is_hit = true});
    }
    return trace;
}

std::vector<ObjectRef> IoPatternRuntime::DeriveAdmissionCandidates(
    const IoPatternSnapshot& snapshot) {
    // Report-driven admission input: lower-tier objects that the merged
    // reports show as hot (frequent reads) and that are not already in the
    // head tier are promotion candidates. PrefixMatchAdmissionOps re-applies
    // its frequency/watermark gates per object before execution.
    std::vector<ObjectRef> candidates;
    for (const auto& key : snapshot.keys) {
        if (key.pinned || key.access_count_window == 0) continue;
        const bool in_head =
            (key.replica_tiers & CacheTierBit(CacheTier::kL1Host)) != 0;
        const bool lower_tier =
            (key.replica_tiers & CacheTierBit(CacheTier::kLocalDisk)) != 0 ||
            (key.replica_tiers & CacheTierBit(CacheTier::kL2Segment)) != 0 ||
            (key.replica_tiers & CacheTierBit(CacheTier::kL3NofSsd)) != 0;
        if (!in_head && lower_tier) candidates.push_back(key.object);
    }
    return candidates;
}

void IoPatternRuntime::RunReportDrivenCycle(bool allow_pressure) {
    IoPatternSnapshot snapshot = collector_->GetSnapshot();
    ReportDrivenCycleReport report;
    report.cycle_id = ++report_cycle_id_;
    report.keys_analyzed = snapshot.keys.size();
    if (snapshot.keys.empty() && snapshot.storage.empty()) {
        if (config_.report_driven_observer) {
            config_.report_driven_observer(report);
        }
        return;
    }
    CacheTier eviction_tier = CacheTier::kL1Host;
    uint64_t eviction_bytes = 0;
    // A tick-driven cycle skips the pressure request entirely, so the
    // below-watermark drivers below decide (tier down, then cold eviction). The
    // master's watermark thread already handled any real breach.
    if (allow_pressure) {
        DeriveEvictionRequest(snapshot, config_.report_eviction_high_ratio,
                              config_.report_eviction_target_ratio,
                              eviction_tier, eviction_bytes);
    }
    // Tier-down driver: the below-watermark placement action. Copy the coldest
    // in-memory keys down to LOCAL_DISK while keeping their MEMORY replica, so a
    // later reclaim of those keys can discard them safely instead of copying then.
    // A demotion frees nothing, so it must never win over a reclaim: the pressure
    // request above takes the cycle whenever the watermark is reached, and this
    // driver only fills the cycle that would otherwise do nothing. The budget is
    // the whole control (no enable flag); the driver decides the action here, and
    // the policy must never derive it from target_tier.
    bool tier_down = false;
    if (eviction_bytes == 0 && config_.tier_down_bytes_per_cycle != 0) {
        eviction_bytes = config_.tier_down_bytes_per_cycle;
        tier_down = true;
        report.tier_down = true;
    }
    // Cold-data eviction driver: when the merged storage watermark does not
    // trigger a pressure eviction but the analysis-relevant snapshot contains
    // idle L1 keys, run a bounded eviction of the coldest keys so eviction is
    // driven by cold/hot analysis and not only by memory pressure. Candidate
    // selection still goes through the policy engine (ScoreBasedEvictionOps
    // ranks the coldest first); this driver only supplies a byte target.
    //
    // It also acts below the watermark, but it reclaims instead of placing, so it
    // only takes the slot when no tier-down budget is configured: demoting a key
    // the same cycle would have discarded defeats the point of paving cold data
    // down first.
    uint64_t cold_idle_threshold_us = 0;
    if (eviction_bytes == 0 && config_.report_driven_cold_eviction &&
        config_.report_driven_cold_eviction_bytes != 0) {
        // The idle gate is applied where the policy selects victims, so the byte
        // budget and the victim set describe the same keys. Pre-summing the idle
        // keys' bytes here produced a target the selector was free to ignore.
        eviction_bytes = config_.report_driven_cold_eviction_bytes;
        cold_idle_threshold_us = config_.report_driven_cold_idle_threshold_us;
        report.cold_eviction = true;
    }
    report.eviction_tier = eviction_tier;
    report.eviction_target_bytes = eviction_bytes;
    const auto trace = DeriveTraceHistory(snapshot);
    const auto admissions = DeriveAdmissionCandidates(snapshot);
    report.admission_candidates = admissions.size();

    auto planned = BuildPolicy(eviction_tier, eviction_bytes, trace,
                               admissions, "report-driven",
                               cold_idle_threshold_us, tier_down);
    report.analysis_elapsed_us = planned.analysis_elapsed_us;
    const auto& result = planned.result;
    report.eviction_candidates = result.eviction.candidates.size();
    report.prefetch_candidates = result.prefetch.candidates.size();
    size_t admitted = 0;
    for (const auto& admission : result.admissions) {
        if (admission.decision == AdmissionDecision::kAdmit) ++admitted;
    }
    report.admissions_admitted = admitted;
    // A pressure report with no evictable L1 keys is a clean no-op for the
    // eviction dimension, not a policy failure: do not invoke the storage
    // handler with an empty candidate list (TierOperationExecutor otherwise
    // forwards any non-zero target). Keep the derived target visible in the
    // report for observability.
    if (report.eviction_candidates == 0) {
        planned.result.eviction.target_bytes = 0;
        planned.result.eviction.candidates.clear();
    }
    const auto status = CommitPolicy(planned);
    report.degraded = status.degraded || planned.result.degraded;
    report.eviction_status = status.eviction;
    report.prefetch_status = status.prefetch;
    report.skipped_dimensions = status.skipped;
    if (!status.admissions.empty()) {
        report.admission_status = status.admissions.front();
    }
    if (config_.report_driven_observer) {
        config_.report_driven_observer(report);
    }
}

bool IoPatternRuntime::ScheduleAdmission(ObjectRef object, CacheTier target_tier,
                                         std::string session_id) {
    {
        std::lock_guard lock(admission_mutex_);
        if (admission_stopping_ ||
            (config_.max_pending_admissions != 0 &&
             pending_admissions_.size() >= config_.max_pending_admissions)) {
            return false;
        }
        pending_admissions_.push_back(
            {.object = std::move(object),
             .target_tier = target_tier,
             .session_id = std::move(session_id)});
    }
    admission_condition_.notify_one();
    return true;
}

void IoPatternRuntime::AdmissionWorker() {
    while (true) {
        PendingAdmission pending;
        {
            std::unique_lock lock(admission_mutex_);
            admission_condition_.wait(lock, [this] {
                return admission_stopping_ || !pending_admissions_.empty();
            });
            if (admission_stopping_) return;
            pending = std::move(pending_admissions_.front());
            pending_admissions_.pop_front();
        }
        try {
            ExecuteAdmission(pending.object, pending.target_tier,
                             pending.session_id);
        } catch (...) {
            policy_->RecordFailure();
            observability_.RecordDegrade();
        }
    }
}

ErrorCode IoPatternRuntime::ExecuteAdmission(const ObjectRef& object,
                                             CacheTier target_tier,
                                             const std::string& session_id) {
    const auto snapshot = collector_->GetSnapshot();
    bool analysis_degraded = false;
    const auto analysis = AnalyzeWithinBudget(snapshot, analysis_degraded);
    workload_policy_->SetWorkloadType(analysis.workload_type);
    workload_policy_->SetSessionWorkloads(analysis.sessions);
    const auto admission = policy_->DecideAdmission(
        object, target_tier,
        PolicyContext{.snapshot = snapshot,
                      .analysis = analysis,
                      .session_id = session_id});
    PolicyResult result;
    result.admissions.push_back(admission);
    auto status = executor_.Execute(result);
    if (analysis_degraded) status.degraded = true;
    const auto code = status.admissions.empty() ? ErrorCode::OK
                                                 : status.admissions.front();
    if (IsPolicyFailure(code) || status.degraded)
        observability_.RecordDegrade();
    return code;
}

void IoPatternRuntime::RecordFeedback(PolicyFeedbackSample sample) {
    // Tuner state is not internally synchronized; the report-driven worker,
    // the eviction thread and the data path can all reach RecordFeedback.
    // feedback_state_mutex_ serializes them (all callers invoke this method
    // outside that lock, so there is no recursive acquisition).
    std::lock_guard lock(feedback_state_mutex_);
    feedback_.Record(sample);
    auto config = workload_policy_->CurrentEvictionConfig();
    if (tuner_.Tune(feedback_.Snapshot(), config)) {
        workload_policy_->ApplyEvictionTuning(config);
    }
}

PolicyFeedbackStats IoPatternRuntime::FeedbackSnapshot() const {
    return feedback_.Snapshot();
}

IoPatternSnapshot IoPatternRuntime::Snapshot() const {
    return collector_->GetSnapshot();
}

IoPatternObservabilitySnapshot IoPatternRuntime::ObservabilitySnapshot(
    double window_seconds) const {
    if (window_seconds <= 0.0) {
        window_seconds = std::chrono::duration<double>(
                             std::chrono::steady_clock::now() - started_at_)
                             .count();
    }
    return observability_.Snapshot(window_seconds);
}

bool IoPatternRuntime::degraded() const {
    return collector_->degraded() || analyzer_->degraded() || policy_->degraded();
}

}  // namespace mooncake::io_pattern
