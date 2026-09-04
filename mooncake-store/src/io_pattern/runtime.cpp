#include "io_pattern/runtime.h"

#include <chrono>
#include <future>
#include <thread>

namespace mooncake::io_pattern {

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
    workload_policy_ = std::make_shared<WorkloadPolicyEngine>();
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
}

IoPatternRuntime::~IoPatternRuntime() {
    {
        std::lock_guard lock(admission_mutex_);
        admission_stopping_ = true;
        pending_admissions_.clear();
    }
    admission_condition_.notify_all();
    if (admission_worker_.joinable()) admission_worker_.join();
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
    const auto& snapshot = planned.snapshot;
    const auto& result = planned.result;
    auto status = executor_.Execute(result);
    status.degraded = status.degraded || result.degraded;
    const bool failed = status.eviction != ErrorCode::OK ||
                        status.prefetch != ErrorCode::OK || status.degraded;
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
    const std::vector<ObjectRef>& admissions, const std::string& session_id) {
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
        PolicyContext{.snapshot = planned.snapshot, .analysis = analysis,
                      .session_id = session_id}, eviction_tier,
        eviction_bytes, trace, admissions);
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
    if (code != ErrorCode::OK || status.degraded)
        observability_.RecordDegrade();
    return code;
}

void IoPatternRuntime::RecordFeedback(PolicyFeedbackSample sample) {
    feedback_.Record(sample);
    auto config = workload_policy_->CurrentEvictionConfig();
    if (tuner_.Tune(feedback_.Snapshot(), config)) {
        workload_policy_->ApplyEvictionTuning(config);
    }
}

IoPatternSnapshot IoPatternRuntime::Snapshot() const {
    return collector_->GetSnapshot();
}

IoPatternObservabilitySnapshot IoPatternRuntime::ObservabilitySnapshot(
    double window_seconds) const {
    return observability_.Snapshot(window_seconds);
}

bool IoPatternRuntime::degraded() const {
    return collector_->degraded() || analyzer_->degraded() || policy_->degraded();
}

}  // namespace mooncake::io_pattern
