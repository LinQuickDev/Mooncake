#include "io_pattern/io_pattern.h"
#include "master_metric_manager.h"
#include "io_pattern/threshold_analyzer.h"
#include "io_pattern/policy_strategies.h"

#include <memory>
#include <atomic>
#include <chrono>
#include <future>
#include <mutex>
#include <condition_variable>
#include <optional>
#include <thread>
#include <type_traits>
#include <variant>
#include <vector>
#include <stdexcept>

#include <gtest/gtest.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

namespace mooncake::io_pattern {
namespace {

class TestEvictionOps final : public EvictionOps {
   public:
    EvictionPlan Evaluate(const PolicyContext&, CacheTier tier,
                          uint64_t target_bytes) const override {
        return EvictionPlan{.source_tier = tier,
                            .target_bytes = target_bytes,
                            .candidates = {}};
    }
};

class TestCollector final : public IoPatternCollector {
   public:
    void ReportInferenceMetrics(const InferenceMetrics& metrics) override {
        inference_metrics = metrics;
    }
    void RecordAccess(const std::string&, const AccessRecord& record) override {
        access_record = record;
    }
    void RecordStorageMetric(const StorageMetric& metric) override {
        storage_metric = metric;
    }
    IoPatternSnapshot GetSnapshot() const override { return snapshot; }

    InferenceMetrics inference_metrics;
    AccessRecord access_record;
    StorageMetric storage_metric;
    IoPatternSnapshot snapshot;
};

class TestAnalyzer final : public IoPatternAnalyzer {
   public:
    PatternResult Analyze(const IoPatternSnapshot&) const override {
        return result;
    }
    WorkloadType DetectWorkloadType(
        const IoPatternSnapshot&) const override {
        return result.workload_type;
    }
    float CalculateConfidence(const ObjectRef&,
                              const IoPatternSnapshot&) const override {
        return result.workload_confidence;
    }

    PatternResult result{.workload_type = WorkloadType::kMixed,
                         .workload_confidence = 0.75F};
};

class ThrowingAnalyzer final : public IoPatternAnalyzer {
   public:
    PatternResult Analyze(const IoPatternSnapshot&) const override {
        throw std::runtime_error("analysis failure");
    }
    WorkloadType DetectWorkloadType(const IoPatternSnapshot&) const override {
        throw std::runtime_error("analysis failure");
    }
    float CalculateConfidence(const ObjectRef&, const IoPatternSnapshot&) const override {
        throw std::runtime_error("analysis failure");
    }
};

class TestPrefetchOps final : public PrefetchOps {
   public:
    PrefetchPlan Evaluate(const PolicyContext&,
                          const TraceHistory&) const override {
        return plan;
    }

    PrefetchPlan plan;
};

class TestAdmissionOps final : public AdmissionOps {
   public:
    AdmissionResult Evaluate(const ObjectRef& object, CacheTier tier,
                             const PolicyContext&) const override {
        return AdmissionResult{.object = object,
                                .target_tier = tier,
                                .decision = AdmissionDecision::kAdmit};
    }
};

class TestPrefetchExecutor final : public PrefetchExecutor {
   public:
    ErrorCode Execute(const PrefetchPlan& value) override {
        plan = value;
        return ErrorCode::OK;
    }

    PrefetchPlan plan;
};

class TestCfmChannel final : public CfmChannel {
   public:
    bool SendSnapshot(const IoPatternSnapshot& value) override {
        snapshot = value;
        return send_ok;
    }
    bool SendMetricBatch(const MetricBatch& value) override {
        batch = value;
        return send_ok;
    }
    ErrorCode ExecutePrefetch(const PrefetchPlan& value) override {
        plan = value;
        return execute_code;
    }
    bool send_ok{true};
    ErrorCode execute_code{ErrorCode::OK};
    IoPatternSnapshot snapshot;
    MetricBatch batch;
    PrefetchPlan plan;
};

class FlakyCfmChannel final : public CfmChannel {
   public:
    bool SendSnapshot(const IoPatternSnapshot&) override {
        return send_failures-- <= 0;
    }
    bool SendMetricBatch(const MetricBatch&) override {
        return send_failures-- <= 0;
    }
    ErrorCode ExecutePrefetch(const PrefetchPlan&) override {
        return ErrorCode::RPC_FAIL;
    }
    int send_failures{0};
};

class TestRpcTransport final : public CfmRpcTransport {
   public:
    bool Send(std::string_view method, std::string_view payload,
              std::chrono::milliseconds timeout) override {
        last_method = std::string(method);
        last_payload = std::string(payload);
        last_timeout = timeout;
        return send_ok;
    }
    bool send_ok{true};
    std::string last_method;
    std::string last_payload;
    std::chrono::milliseconds last_timeout{0};
};

class TestRpcCodec final : public CfmRpcCodec {
   public:
    std::string EncodeSnapshot(const IoPatternSnapshot&) const override { return "snapshot"; }
    std::string EncodePrefetch(const PrefetchPlan&) const override { return "prefetch"; }
    std::string EncodeMetricBatch(const MetricBatch&) const override {
        return "batch";
    }
    std::optional<PolicyCommand> DecodePolicy(const std::string& value) const override {
        return value == "policy" ? std::optional<PolicyCommand>(PrefetchPlan{})
                                   : std::nullopt;
    }
};

TEST(IoPatternFrameworkTest, PublicSeamsRemainAbstract) {
    static_assert(std::is_abstract_v<IoPatternCollector>);
    static_assert(std::is_abstract_v<IoPatternAnalyzer>);
    static_assert(std::is_abstract_v<CfmClient>);
    static_assert(std::is_abstract_v<EvictionOps>);
    static_assert(std::is_abstract_v<PrefetchOps>);
    static_assert(std::is_abstract_v<AdmissionOps>);
    static_assert(std::is_abstract_v<PrefetchExecutor>);
    static_assert(std::is_abstract_v<PolicyEngine>);
}

TEST(IoPatternFrameworkTest, CacheTierMaskRepresentsAllTiers) {
    const CacheTierMask all_tiers =
        CacheTierBit(CacheTier::kL0Hbm) | CacheTierBit(CacheTier::kL1Host) |
        CacheTierBit(CacheTier::kL2Segment) |
        CacheTierBit(CacheTier::kL3NofSsd);

    EXPECT_EQ(all_tiers, 0x0F);
}

TEST(IoPatternFrameworkTest, MetricsKeepTenantAndLayoutIdentity) {
    InferenceMetrics metrics;
    metrics.object = {TenantId("tenant-a"), "prefix/block-1"};
    metrics.layout = CacheLayout::kHmaMultiGroup;
    metrics.layout_group = 3;
    metrics.match_length = 512;

    IoPatternSnapshot snapshot;
    KeyMetrics key_metrics;
    key_metrics.object = metrics.object;
    key_metrics.match_length = metrics.match_length;
    key_metrics.layout = metrics.layout;
    key_metrics.layout_group = metrics.layout_group;
    snapshot.keys.push_back(key_metrics);

    ASSERT_EQ(snapshot.keys.size(), 1);
    EXPECT_EQ(snapshot.keys.front().object.tenant_id.value(), "tenant-a");
    EXPECT_EQ(snapshot.keys.front().object.key, "prefix/block-1");
    EXPECT_EQ(snapshot.keys.front().layout, CacheLayout::kHmaMultiGroup);
    EXPECT_EQ(snapshot.keys.front().layout_group, 3);
    EXPECT_EQ(snapshot.keys.front().match_length, 512);
}

TEST(IoPatternFrameworkTest, CollectorAndAnalyzerExposeValueFlow) {
    TestCollector collector;
    collector.inference_metrics.object =
        {TenantId("tenant-a"), "prefix/block-1"};
    collector.snapshot.generated_at_ns = 42;
    collector.snapshot.keys.push_back(
        KeyMetrics{.object = collector.inference_metrics.object});

    collector.ReportInferenceMetrics(collector.inference_metrics);
    AccessRecord access_record;
    access_record.object = collector.inference_metrics.object;
    access_record.observed_at_ns = 43;
    access_record.is_hit = true;
    collector.RecordAccess("key", access_record);
    collector.RecordStorageMetric(StorageMetric{.source_id = "segment-1"});

    const auto snapshot = collector.GetSnapshot();
    ASSERT_EQ(snapshot.keys.size(), 1);
    EXPECT_EQ(collector.inference_metrics.object.key, "prefix/block-1");
    EXPECT_TRUE(collector.access_record.is_hit);
    EXPECT_EQ(collector.storage_metric.source_id, "segment-1");

    TestAnalyzer analyzer;
    const auto result = analyzer.Analyze(snapshot);
    EXPECT_EQ(result.workload_type, WorkloadType::kMixed);
    EXPECT_FLOAT_EQ(analyzer.CalculateConfidence({}, snapshot), 0.75F);
    EXPECT_EQ(analyzer.DetectWorkloadType(snapshot), WorkloadType::kMixed);
}

TEST(IoPatternFrameworkTest, CollectorImplAggregatesAndIsolatesTenants) {
    IoPatternCollectorImpl collector;
    AccessRecord access;
    access.object = {TenantId("tenant-a"), "ignored"};
    access.observed_at_ns = 200;
    access.block_size = 4096;
    access.tier = CacheTier::kL1Host;
    access.is_hit = true;
    collector.RecordAccess("shared-key", access);
    access.object.tenant_id = TenantId("tenant-b");
    access.observed_at_ns = 100;
    collector.RecordAccess("shared-key", access);

    const auto snapshot = collector.GetSnapshot();
    ASSERT_EQ(snapshot.keys.size(), 2);
    EXPECT_EQ(snapshot.keys[0].object.tenant_id.value(), "tenant-a");
    EXPECT_EQ(snapshot.keys[1].object.tenant_id.value(), "tenant-b");
    EXPECT_EQ(snapshot.keys[0].object.key, "shared-key");
}

TEST(IoPatternFrameworkTest, CollectorImplKeepsLatestStorageObservation) {
    IoPatternCollectorImpl collector;
    collector.RecordStorageMetric(StorageMetric{.source_id = "segment",
                                                .observed_at_ns = 20,
                                                .used_bytes = 200});
    collector.RecordStorageMetric(StorageMetric{.source_id = "segment",
                                                .observed_at_ns = 10,
                                                .used_bytes = 100});
    const auto snapshot = collector.GetSnapshot();
    ASSERT_EQ(snapshot.storage.size(), 1);
    EXPECT_EQ(snapshot.storage.front().used_bytes, 200);
}

TEST(IoPatternFrameworkTest, CollectorImplEnforcesPerTenantKeyQuota) {
    IoPatternCollectorImpl collector(
        IoPatternCollectorImpl::Config{.max_keys_per_tenant = 1});
    InferenceMetrics first;
    first.object = {TenantId("tenant-a"), "first"};
    collector.ReportInferenceMetrics(first);
    InferenceMetrics second;
    second.object = {TenantId("tenant-a"), "second"};
    collector.ReportInferenceMetrics(second);
    InferenceMetrics other_tenant;
    other_tenant.object = {TenantId("tenant-b"), "second"};
    collector.ReportInferenceMetrics(other_tenant);
    EXPECT_EQ(collector.GetSnapshot().keys.size(), 2);
    EXPECT_EQ(collector.dropped(), 1);
}

TEST(IoPatternFrameworkTest, CollectorImplDegradesAtGlobalKeyLimit) {
    IoPatternCollectorImpl collector(
        IoPatternCollectorImpl::Config{.max_total_keys = 1});
    InferenceMetrics first;
    first.object = {TenantId("tenant-a"), "first"};
    collector.ReportInferenceMetrics(first);
    InferenceMetrics second;
    second.object = {TenantId("tenant-b"), "second"};
    collector.ReportInferenceMetrics(second);
    EXPECT_TRUE(collector.degraded());
    EXPECT_EQ(collector.dropped(), 1);
    EXPECT_EQ(collector.GetSnapshot().keys.size(), 1);
    collector.RecordStorageMetric(StorageMetric{.source_id = "segment"});
    EXPECT_EQ(collector.GetSnapshot().storage.size(), 1);
}

TEST(IoPatternFrameworkTest, CollectorImplFeedsReporterWithoutInlineTransport) {
    MetricBatch received;
    auto reporter = std::make_shared<IoPatternReporter>(4, [&](const MetricBatch& batch) {
        received = batch;
        return true;
    });
    IoPatternCollectorImpl collector({}, reporter);
    collector.ReportInferenceMetrics(InferenceMetrics{});
    collector.RecordStorageMetric(StorageMetric{});
    EXPECT_EQ(reporter->pending(), 2);
    EXPECT_TRUE(collector.FlushReports());
    EXPECT_EQ(received.inference.size(), 1);
    EXPECT_EQ(received.storage.size(), 1);
}

TEST(IoPatternFrameworkTest, CollectorImplDerivesWritePathMetrics) {
    IoPatternCollectorImpl collector;
    AccessRecord write;
    write.object = {TenantId("tenant-a"), "write-key"};
    write.operation = IoOperation::kPut;
    write.block_size = 4096;
    write.write_batch_size = 32;
    collector.RecordAccess("write-key", write);
    write.overwrite = true;
    collector.RecordAccess("write-key", write);
    const auto snapshot = collector.GetSnapshot();
    const auto& key = snapshot.keys.front();
    EXPECT_EQ(key.write_frequency, 2);
    EXPECT_EQ(key.write_batch_size, 32);
    EXPECT_EQ(key.write_object_size, 4096);
    EXPECT_FLOAT_EQ(key.overwrite_ratio, 0.5F);
    EXPECT_TRUE(key.write_burst);
}

TEST(IoPatternFrameworkTest, CollectorImplBoundsAccessCountByTimeWindow) {
    uint64_t now_ns = 10;
    IoPatternCollectorImpl collector(IoPatternCollectorImpl::Config{
        .access_window_ns = 100,
        .access_bucket_ns = 1,
        .now_ns = [&] { return now_ns; }});
    AccessRecord access{.object = {TenantId("tenant-a"), "key"},
                        .observed_at_ns = 10};
    collector.RecordAccess(access.object.key, access);
    access.observed_at_ns = 50;
    collector.RecordAccess(access.object.key, access);
    EXPECT_EQ(collector.GetSnapshot().keys.front().access_count_window, 2);

    access.observed_at_ns = 111;
    now_ns = 111;
    collector.RecordAccess(access.object.key, access);
    // A true sliding window retains the event at 50 even though the first
    // event's fixed 10..110 bucket has ended.
    EXPECT_EQ(collector.GetSnapshot().keys.front().access_count_window, 2);

    now_ns = 212;
    EXPECT_EQ(collector.GetSnapshot().keys.front().access_count_window, 0);
}

TEST(IoPatternFrameworkTest, CollectorExpiresMergedAccessWindowsLocally) {
    uint64_t now_ns = 10;
    IoPatternCollectorImpl collector(IoPatternCollectorImpl::Config{
        .access_window_ns = 100,
        .access_bucket_ns = 1,
        .now_ns = [&] { return now_ns; }});
    IoPatternSnapshot remote;
    remote.keys.push_back(
        KeyMetrics{.object = {TenantId("tenant-a"), "remote"},
                   .access_count_window = 7,
                   .write_frequency = 3});

    collector.MergeSnapshot(remote);
    EXPECT_EQ(collector.GetSnapshot().keys.front().access_count_window, 7);
    now_ns = 111;
    const auto expired = collector.GetSnapshot().keys.front();
    EXPECT_EQ(expired.access_count_window, 0);
    EXPECT_EQ(expired.write_frequency, 0);
}

TEST(IoPatternFrameworkTest, CollectorHardCapsBucketsForOutOfOrderInput) {
    uint64_t now_ns = 3;
    IoPatternCollectorImpl collector(IoPatternCollectorImpl::Config{
        .access_window_ns = 1'000,
        .access_bucket_ns = 1,
        .max_access_buckets_per_key = 2,
        .now_ns = [&] { return now_ns; }});
    AccessRecord access{.object = {TenantId("tenant-a"), "key"}};
    for (uint64_t timestamp : {1ULL, 2ULL, 3ULL}) {
        access.observed_at_ns = timestamp;
        collector.RecordAccess(access.object.key, access);
    }
    EXPECT_EQ(collector.GetSnapshot().keys.front().access_count_window, 2);
}

TEST(IoPatternFrameworkTest, ThresholdAnalyzerClassifiesDocumentedWorkloads) {
    ThresholdAnalyzer analyzer;
    IoPatternSnapshot code_agent;
    KeyMetrics code_key;
    code_key.object = {TenantId("tenant-a"), "code"};
    code_key.token_count = 20 * 1024;
    code_key.prefix_fanout = 20;
    code_key.match_length = 512;
    code_agent.keys.push_back(code_key);
    EXPECT_EQ(analyzer.DetectWorkloadType(code_agent),
              WorkloadType::kCodeAgent);

    IoPatternSnapshot recommendation;
    KeyMetrics recommendation_key;
    recommendation_key.object = {TenantId("tenant-a"), "recommendation"};
    recommendation_key.block_size = 64 * 1024;
    recommendation_key.access_count_window = 30;
    recommendation.keys.push_back(recommendation_key);
    EXPECT_EQ(analyzer.DetectWorkloadType(recommendation),
              WorkloadType::kGenerativeRecommendation);
}

TEST(IoPatternFrameworkTest, ThresholdAnalyzerFallsBackToMixed) {
    ThresholdAnalyzer analyzer;
    IoPatternSnapshot snapshot;
    KeyMetrics key;
    key.object = {TenantId("tenant-a"), "unknown"};
    snapshot.keys.push_back(key);

    const auto result = analyzer.Analyze(snapshot);
    EXPECT_EQ(result.workload_type, WorkloadType::kMixed);
    EXPECT_FLOAT_EQ(result.workload_confidence, 0.0F);
    ASSERT_EQ(result.keys.size(), 1);
    EXPECT_EQ(result.keys.front().object.key, "unknown");
    EXPECT_FLOAT_EQ(analyzer.CalculateConfidence(key.object, snapshot), 0.0F);
}

TEST(IoPatternFrameworkTest, ThresholdAnalyzerReportsMixedAndPartialConfidence) {
    ThresholdAnalyzer analyzer;
    IoPatternSnapshot snapshot;
    KeyMetrics code;
    code.object = {TenantId("tenant-a"), "code"};
    code.token_count = 20 * 1024;
    code.prefix_fanout = 20;
    code.match_length = 512;
    snapshot.keys.push_back(code);
    KeyMetrics recommendation;
    recommendation.object = {TenantId("tenant-b"), "recommendation"};
    recommendation.block_size = 64 * 1024;
    recommendation.access_count_window = 30;
    snapshot.keys.push_back(recommendation);
    EXPECT_EQ(analyzer.DetectWorkloadType(snapshot), WorkloadType::kMixed);
    EXPECT_FLOAT_EQ(analyzer.CalculateConfidence(
                        {TenantId("tenant-a"), "code"}, snapshot),
                    1.0F);
    KeyMetrics partial;
    partial.object = {TenantId("tenant-c"), "partial"};
    partial.token_count = 8 * 1024;
    snapshot.keys.push_back(partial);
    EXPECT_GT(analyzer.CalculateConfidence(partial.object, snapshot), 0.0F);
}

TEST(IoPatternFrameworkTest, ScoreEvictionSelectsColdObjectsWithinBudget) {
    ScoreBasedEvictionOps eviction;
    PolicyContext context;
    KeyMetrics cold;
    cold.object = {TenantId("tenant-a"), "cold"};
    cold.block_size = 100;
    cold.replica_tiers = CacheTierBit(CacheTier::kL1Host);
    KeyMetrics hot = cold;
    hot.object.key = "hot";
    hot.block_size = 100;
    context.snapshot.keys = {cold, hot};
    context.analysis.keys = {
        {.object = cold.object, .frequency_score = 0.1F, .idle_score = 0.9F},
        {.object = hot.object, .frequency_score = 0.9F, .idle_score = 0.1F},
    };

    const auto plan = eviction.Evaluate(context, CacheTier::kL1Host, 100);
    ASSERT_EQ(plan.candidates.size(), 1);
    EXPECT_EQ(plan.candidates.front().object.key, "cold");
    EXPECT_EQ(plan.candidates.front().bytes, 100);
}

TEST(IoPatternFrameworkTest, ScoreEvictionSkipsPinnedAndZeroBudget) {
    ScoreBasedEvictionOps eviction;
    PolicyContext context;
    KeyMetrics key;
    key.object = {TenantId("tenant-a"), "pinned"};
    key.block_size = 1;
    key.pinned = true;
    key.replica_tiers = CacheTierBit(CacheTier::kL1Host);
    context.snapshot.keys.push_back(key);
    context.analysis.keys.push_back(
        KeyPattern{.object = key.object, .frequency_score = 0.0F});

    EXPECT_TRUE(eviction.Evaluate(context, CacheTier::kL1Host, 1024)
                    .candidates.empty());
    key.pinned = false;
    context.snapshot.keys.front() = key;
    EXPECT_TRUE(eviction.Evaluate(context, CacheTier::kL1Host, 0)
                    .candidates.empty());
}

TEST(IoPatternFrameworkTest, PrefixAdmissionUsesTierSpecificSignals) {
    PrefixMatchAdmissionOps admission;
    PolicyContext context;
    KeyMetrics key;
    key.object = {TenantId("tenant-a"), "prefix"};
    key.access_count_window = 10;
    key.match_length = 64;
    context.snapshot.keys.push_back(key);

    const auto hbm = admission.Evaluate(key.object, CacheTier::kL0Hbm, context);
    EXPECT_EQ(hbm.decision, AdmissionDecision::kAdmit);

    key.match_length = 1;
    context.snapshot.keys.front() = key;
    const auto rejected =
        admission.Evaluate(key.object, CacheTier::kL0Hbm, context);
    EXPECT_EQ(rejected.decision, AdmissionDecision::kRejectPrefix);

    context.snapshot.storage = {
        StorageMetric{.source_id = "ssd",
                      .tier = CacheTier::kL3NofSsd,
                      .memory_used_ratio = 0.1F},
        StorageMetric{.source_id = "host",
                      .tier = CacheTier::kL1Host,
                      .memory_used_ratio = 0.95F}};
    EXPECT_EQ(admission.Evaluate(key.object, CacheTier::kL1Host, context)
                  .decision,
              AdmissionDecision::kRejectWatermark);
}

TEST(IoPatternFrameworkTest, TracePrefetchPlansOnlyLongPrefixMatches) {
    TraceBasedPrefetchOps prefetch;
    PolicyContext context;
    KeyMetrics key;
    key.object = {TenantId("tenant-a"), "block"};
    key.block_size = 4096;
    key.replica_tiers = CacheTierBit(CacheTier::kLocalDisk);
    context.snapshot.keys.push_back(key);
    // The prefetch gate also requires analyzer confidence, so supply the key
    // pattern the production pipeline would derive for this object.
    context.analysis.keys = {
        KeyPattern{.object = key.object, .confidence = 1.0F}};

    TraceHistory trace;
    trace.events.push_back(
        TraceEvent{.object = key.object, .match_length = 512, .is_hit = true});
    trace.events.push_back(
        TraceEvent{.object = key.object, .match_length = 8, .is_hit = true});

    const auto plan = prefetch.Evaluate(context, trace);
    ASSERT_EQ(plan.candidates.size(), 1);
    EXPECT_EQ(plan.candidates.front().source_tier, CacheTier::kLocalDisk);
    EXPECT_EQ(plan.candidates.front().target_tier, CacheTier::kL1Host);
    EXPECT_EQ(plan.candidates.front().bytes, 4096);
}

TEST(IoPatternFrameworkTest, TracePrefetchDeduplicatesObjects) {
    TraceBasedPrefetchOps prefetch;
    PolicyContext context;
    KeyMetrics key;
    key.object = {TenantId("tenant-a"), "block"};
    key.block_size = 128;
    key.replica_tiers = CacheTierBit(CacheTier::kLocalDisk);
    context.snapshot.keys.push_back(key);
    // The prefetch gate also requires analyzer confidence, so supply the key
    // pattern the production pipeline would derive for this object.
    context.analysis.keys = {
        KeyPattern{.object = key.object, .confidence = 1.0F}};

    TraceHistory trace;
    trace.events.push_back(
        TraceEvent{.object = key.object, .match_length = 300, .is_hit = true});
    trace.events.push_back(
        TraceEvent{.object = key.object, .match_length = 400, .is_hit = true});

    EXPECT_EQ(prefetch.Evaluate(context, trace).candidates.size(), 1);
}

TEST(IoPatternFrameworkTest, PolicyContextCarriesRawAndDerivedViews) {
    PolicyContext context;
    context.snapshot.generated_at_ns = 123;
    context.analysis.workload_type = WorkloadType::kMixed;
    context.analysis.workload_confidence = 0.75F;

    EXPECT_EQ(context.snapshot.generated_at_ns, 123);
    EXPECT_EQ(context.analysis.workload_type, WorkloadType::kMixed);
    EXPECT_FLOAT_EQ(context.analysis.workload_confidence, 0.75F);
}

TEST(IoPatternFrameworkTest, PolicyCommandAndViewRemainValueTypes) {
    const ObjectRef object{TenantId("tenant-b"), "block"};
    PrefetchCandidate candidate;
    candidate.object = object;
    candidate.bytes = 4096;
    PrefetchPlan prefetch_plan;
    prefetch_plan.strategy = PrefetchStrategy::kTimeout;
    prefetch_plan.timeout_us = 1000;
    prefetch_plan.candidates.push_back(candidate);
    const PolicyCommand command = prefetch_plan;
    ASSERT_TRUE(std::holds_alternative<PrefetchPlan>(command));
    EXPECT_EQ(std::get<PrefetchPlan>(command).candidates.front().object,
              object);

    CacheView view;
    view.version = 7;
    view.entries.push_back({object, CacheTier::kL1Host, 4096});
    EXPECT_EQ(view.entries.front().tier, CacheTier::kL1Host);
    EXPECT_EQ(view.entries.front().bytes, 4096);
}

TEST(IoPatternFrameworkTest, RegistryCreatesTypedOpsAndRejectsDuplicates) {
    OpsRegistry<EvictionOps> registry;

    EXPECT_TRUE(registry.Register(
        "test", [] { return std::make_shared<TestEvictionOps>(); }));
    EXPECT_FALSE(registry.Register(
        "test", [] { return std::make_shared<TestEvictionOps>(); }));
    EXPECT_FALSE(registry.Register("", {}));
    EXPECT_EQ(registry.RegisteredNames(), std::vector<std::string>{"test"});

    const auto ops = registry.Create("test");
    ASSERT_NE(ops, nullptr);
    const auto plan = ops->Evaluate({}, CacheTier::kL2Segment, 4096);
    EXPECT_EQ(plan.source_tier, CacheTier::kL2Segment);
    EXPECT_EQ(plan.target_bytes, 4096);
    EXPECT_EQ(registry.Create("missing"), nullptr);
}

TEST(IoPatternFrameworkTest, ComposedEngineDelegatesAndDegradesSafely) {
    auto eviction = std::make_shared<TestEvictionOps>();
    auto prefetch = std::make_shared<TestPrefetchOps>();
    auto admission = std::make_shared<TestAdmissionOps>();
    prefetch->plan.strategy = PrefetchStrategy::kWaitComplete;
    ComposedPolicyEngine engine(eviction, prefetch, admission);

    const auto delegated =
        engine.PlanEviction({}, CacheTier::kL1Host, 2048);
    EXPECT_EQ(delegated.source_tier, CacheTier::kL1Host);
    EXPECT_EQ(delegated.target_bytes, 2048);

    const auto prefetch_plan = engine.PlanPrefetch({}, {});
    EXPECT_EQ(prefetch_plan.strategy, PrefetchStrategy::kWaitComplete);

    const ObjectRef object{TenantId("tenant-a"), "key"};
    const auto admitted = engine.DecideAdmission(
        object, CacheTier::kL2Segment, {});
    EXPECT_EQ(admitted.object, object);
    EXPECT_EQ(admitted.target_tier, CacheTier::kL2Segment);
    EXPECT_EQ(admitted.decision, AdmissionDecision::kAdmit);

    ComposedPolicyEngine degraded(nullptr, nullptr, nullptr);
    const auto deferred =
        degraded.DecideAdmission(object, CacheTier::kL2Segment, {});
    EXPECT_EQ(deferred.decision, AdmissionDecision::kDefer);

    TestPrefetchExecutor executor;
    EXPECT_EQ(executor.Execute(prefetch_plan), ErrorCode::OK);
    EXPECT_EQ(executor.plan.strategy, PrefetchStrategy::kWaitComplete);
}

TEST(IoPatternFrameworkTest, WorkloadPolicyEngineSelectsAndTransitionsTemplates) {
    WorkloadPolicyEngine engine(WorkloadType::kCodeAgent, 3);
    EXPECT_EQ(engine.ActiveWorkload(), WorkloadType::kCodeAgent);
    EXPECT_FLOAT_EQ(engine.TransitionProgress(), 1.0F);

    engine.SetWorkloadType(WorkloadType::kGenerativeRecommendation);
    EXPECT_EQ(engine.ActiveWorkload(),
              WorkloadType::kGenerativeRecommendation);
    EXPECT_FLOAT_EQ(engine.TransitionProgress(), 0.0F);
    engine.AdvanceTransitionWindow();
    EXPECT_FLOAT_EQ(engine.TransitionProgress(), 1.0F / 3.0F);
    engine.AdvanceTransitionWindow();
    engine.AdvanceTransitionWindow();
    EXPECT_FLOAT_EQ(engine.TransitionProgress(), 1.0F);

    PolicyContext context;
    KeyMetrics key;
    key.object = {TenantId("tenant-a"), "item"};
    key.block_size = 64 * 1024;
    key.access_count_window = 30;
    context.snapshot.keys.push_back(key);
    TraceHistory trace;
    trace.events.push_back(
        TraceEvent{.object = key.object, .match_length = 64, .is_hit = true});
    EXPECT_EQ(engine.PlanPrefetch(context, trace).strategy,
              PrefetchStrategy::kWaitComplete);
}

TEST(IoPatternFrameworkTest, UnifiedPolicyResultSeamDelegates) {
    IoPatternSnapshot snapshot;
    KeyMetrics key;
    key.object = {TenantId("tenant-a"), "key"};
    snapshot.keys.push_back(key);
    ComposedPolicyEngine engine(std::make_shared<ScoreBasedEvictionOps>(),
                                std::make_shared<TraceBasedPrefetchOps>(),
                                std::make_shared<PrefixMatchAdmissionOps>());
    PolicyContext context;
    context.snapshot = snapshot;
    const auto result = engine.ExecutePolicy(
        context, CacheTier::kL1Host, 1024, CacheTier::kL1Host, {}, {key.object});
    EXPECT_EQ(result.admissions.size(), 1);
    EXPECT_EQ(result.admissions.front().object, key.object);
}

TEST(IoPatternFrameworkTest, RegistryPolicyEngineResolvesNamedOps) {
    auto registries = std::make_shared<PolicyOpsRegistries>();
    ASSERT_TRUE(registries->eviction.Register(
        "score", [] { return std::make_shared<ScoreBasedEvictionOps>(); }));
    ASSERT_TRUE(registries->prefetch.Register(
        "trace", [] { return std::make_shared<TraceBasedPrefetchOps>(); }));
    ASSERT_TRUE(registries->admission.Register(
        "prefix", [] { return std::make_shared<PrefixMatchAdmissionOps>(); }));
    RegistryPolicyEngine engine(registries, "score", "trace", "prefix");
    const ObjectRef object{TenantId("tenant-a"), "key"};
    const auto result = engine.ExecutePolicy(
        {}, CacheTier::kL1Host, 1024, CacheTier::kL1Host, {}, {object});
    ASSERT_EQ(result.admissions.size(), 1);
    EXPECT_EQ(result.admissions.front().object, object);

    RegistryPolicyEngine missing(registries, "missing", "trace", "prefix");
    EXPECT_TRUE(
        missing.ExecutePolicy({}, CacheTier::kL1Host, 0, CacheTier::kL1Host, {})
            .degraded);
}

TEST(IoPatternFrameworkTest, ReporterBatchesBoundsAndCountsDrops) {
    MetricBatch received;
    IoPatternReporter reporter(2, [&](const MetricBatch& batch) {
        received = batch;
        return true;
    });
    EXPECT_TRUE(reporter.Enqueue(InferenceMetrics{}));
    EXPECT_TRUE(reporter.EnqueueStorage(StorageMetric{}));
    EXPECT_FALSE(reporter.EnqueueAccess(AccessRecord{}));
    EXPECT_EQ(reporter.dropped(), 1);
    EXPECT_EQ(reporter.pending(), 2);
    EXPECT_TRUE(reporter.Flush());
    EXPECT_EQ(reporter.pending(), 0);
    EXPECT_EQ(reporter.reported(), 2);
    EXPECT_EQ(received.inference.size(), 1);
    EXPECT_EQ(received.storage.size(), 1);
}

TEST(IoPatternFrameworkTest, ReporterAdaptsFlushIntervalToLoad) {
    IoPatternReporter reporter(4, [](const MetricBatch&) { return true; });
    reporter.UpdateLoad(0.25F, 0);
    EXPECT_EQ(reporter.RecommendedFlushInterval(),
              std::chrono::milliseconds(100));
    reporter.UpdateLoad(0.50F, 0);
    EXPECT_EQ(reporter.RecommendedFlushInterval(),
              std::chrono::milliseconds(200));
    reporter.UpdateLoad(0.80F, 0);
    EXPECT_EQ(reporter.RecommendedFlushInterval(),
              std::chrono::milliseconds(500));
    reporter.UpdateLoad(0.95F, 0);
    EXPECT_EQ(reporter.RecommendedFlushInterval(),
              std::chrono::milliseconds(1000));
    reporter.UpdateLoad(0.25F, 101'000);
    EXPECT_EQ(reporter.RecommendedFlushInterval(),
              std::chrono::milliseconds(1000));
}

TEST(IoPatternFrameworkTest, ReporterEnforcesPerTenantFairness) {
    IoPatternReporter reporter(4, [](const MetricBatch&) { return true; }, 1);
    InferenceMetrics first;
    first.object = {TenantId("tenant-a"), "a"};
    InferenceMetrics second = first;
    second.object.key = "b";
    InferenceMetrics other = first;
    other.object.tenant_id = TenantId("tenant-b");
    EXPECT_TRUE(reporter.Enqueue(first));
    EXPECT_FALSE(reporter.Enqueue(second));
    EXPECT_TRUE(reporter.Enqueue(other));
    EXPECT_EQ(reporter.dropped(), 1);
}

TEST(IoPatternFrameworkTest, CfmClientDelegatesToTransportChannel) {
    auto channel = std::make_shared<TestCfmChannel>();
    CfmClientImpl client(channel);
    IoPatternSnapshot snapshot;
    snapshot.generated_at_ns = 42;
    EXPECT_EQ(client.ReportSnapshot(snapshot), ErrorCode::OK);
    EXPECT_EQ(channel->snapshot.generated_at_ns, 42);
    EXPECT_EQ(client.ExecutePrefetch(PrefetchPlan{}), ErrorCode::OK);
    MetricBatch batch;
    batch.inference.push_back(InferenceMetrics{});
    EXPECT_EQ(client.ReportMetricBatch(batch), ErrorCode::OK);
    EXPECT_EQ(channel->batch.inference.size(), 1);
    channel->send_ok = false;
    EXPECT_EQ(client.ReportSnapshot(snapshot), ErrorCode::RPC_FAIL);
    EXPECT_EQ(client.ReportMetricBatch(batch), ErrorCode::RPC_FAIL);
    CfmClientImpl unavailable(nullptr);
    EXPECT_EQ(unavailable.ReportSnapshot(snapshot),
              ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    EXPECT_EQ(unavailable.ReportMetricBatch(batch),
              ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

TEST(IoPatternFrameworkTest, ResilientChannelRetriesAndTracksDegrade) {
    auto flaky = std::make_shared<FlakyCfmChannel>();
    flaky->send_failures = 2;
    ResilientCfmChannel channel(flaky, CfmRetryConfig{.max_retries = 2,
                                                      .degrade_after_failures = 2});
    EXPECT_TRUE(channel.SendSnapshot({}));
    EXPECT_FALSE(channel.degraded());
    EXPECT_EQ(channel.ExecutePrefetch({}), ErrorCode::RPC_FAIL);
    EXPECT_FALSE(channel.degraded());
    EXPECT_EQ(channel.consecutive_failures(), 1);
    EXPECT_EQ(channel.ExecutePrefetch({}), ErrorCode::RPC_FAIL);
    EXPECT_EQ(channel.consecutive_failures(), 2);
    EXPECT_TRUE(channel.degraded());
}

TEST(IoPatternFrameworkTest, ResilientChannelRecoversAfterSuccess) {
    auto flaky = std::make_shared<FlakyCfmChannel>();
    ResilientCfmChannel channel(flaky, CfmRetryConfig{.max_retries = 2,
                                                      .degrade_after_failures = 1});
    MetricBatch batch;
    EXPECT_TRUE(channel.SendMetricBatch(batch));
    EXPECT_FALSE(channel.degraded());
    EXPECT_EQ(channel.consecutive_failures(), 0);
}

TEST(IoPatternFrameworkTest, ResilientAnalyzerFallsBackAfterFailure) {
    ResilientAnalyzer analyzer(std::make_shared<ThrowingAnalyzer>(), 2);
    EXPECT_EQ(analyzer.DetectWorkloadType({}), WorkloadType::kMixed);
    EXPECT_FALSE(analyzer.degraded());
    EXPECT_EQ(analyzer.DetectWorkloadType({}), WorkloadType::kMixed);
    EXPECT_TRUE(analyzer.degraded());
    EXPECT_EQ(analyzer.failures(), 2);
}

TEST(IoPatternFrameworkTest, RpcChannelUsesCodecTransportAndTimeout) {
    auto transport = std::make_shared<TestRpcTransport>();
    auto codec = std::make_shared<TestRpcCodec>();
    CfmRpcChannel channel(transport, codec, CfmRpcConfig{.timeout = std::chrono::milliseconds(25)});
    EXPECT_TRUE(channel.SendSnapshot({}));
    EXPECT_EQ(transport->last_method, "report_snapshot");
    EXPECT_EQ(transport->last_payload, "snapshot");
    EXPECT_EQ(transport->last_timeout, std::chrono::milliseconds(25));
    EXPECT_EQ(channel.ExecutePrefetch({}), ErrorCode::OK);
    EXPECT_EQ(transport->last_method, "execute_prefetch");
    auto rpc_channel = std::make_shared<CfmRpcChannel>(transport, codec);
    IoPatternReporter reporter(2, MakeCfmMetricBatchSink(rpc_channel));
    reporter.Enqueue(InferenceMetrics{});
    EXPECT_TRUE(reporter.Flush());
    EXPECT_EQ(transport->last_method, "report_metric_batch");
    EXPECT_EQ(transport->last_payload, "batch");
    transport->send_ok = false;
    EXPECT_EQ(channel.ExecutePrefetch({}), ErrorCode::RPC_TIMEOUT);
    EXPECT_FALSE(channel.SendSnapshot({}));
}

TEST(IoPatternFrameworkTest, BinaryCfmCodecRoundTripsAllPolicyCommands) {
    CfmBinaryCodec codec;
    PrefetchPlan prefetch{.strategy = PrefetchStrategy::kTimeout,
                          .timeout_us = 42,
                          .candidates = {PrefetchCandidate{
                              .object = {TenantId("tenant-a"), "key"},
                              .source_tier = CacheTier::kL3NofSsd,
                              .target_tier = CacheTier::kL2Segment,
                              .bytes = 512,
                              .priority = 0.8F,
                              .confidence = 0.9F}}};
    const auto decoded_prefetch = codec.DecodePolicy(codec.EncodePolicy(prefetch));
    ASSERT_TRUE(decoded_prefetch.has_value());
    const auto& decoded_plan = std::get<PrefetchPlan>(*decoded_prefetch);
    ASSERT_EQ(decoded_plan.candidates.size(), 1);
    EXPECT_EQ(decoded_plan.candidates.front().object.key, "key");
    EXPECT_EQ(decoded_plan.timeout_us, 42);

    AdmissionResult admission{.object = {TenantId("tenant-b"), "admit"},
                              .target_tier = CacheTier::kL1Host,
                              .decision = AdmissionDecision::kAdmit,
                              .confidence = 0.75F};
    const auto decoded_admission = codec.DecodePolicy(codec.EncodePolicy(admission));
    ASSERT_TRUE(decoded_admission.has_value());
    EXPECT_EQ(std::get<AdmissionResult>(*decoded_admission).object.key, "admit");

    EvictionPlan eviction{.source_tier = CacheTier::kL1Host,
                          .target_bytes = 128,
                          .candidates = {EvictionCandidate{
                              .object = {TenantId("tenant-c"), "evict"},
                              .bytes = 128,
                              .score = 0.4F}}};
    const auto decoded_eviction = codec.DecodePolicy(codec.EncodePolicy(eviction));
    ASSERT_TRUE(decoded_eviction.has_value());
    EXPECT_EQ(std::get<EvictionPlan>(*decoded_eviction).candidates.front().object.key,
              "evict");

    IoPatternSnapshot snapshot{.generated_at_ns = 9,
                               .keys = {KeyMetrics{.object = {TenantId("tenant-d"), "full"},
                                                   .session_id = "session",
                                                   .token_count = 16,
                                                   .active = true}},
                               .storage = {StorageMetric{.source_id = "master",
                                                         .used_bytes = 42}}};
    const auto decoded_snapshot = codec.DecodeSnapshot(codec.EncodeSnapshot(snapshot));
    ASSERT_TRUE(decoded_snapshot.has_value());
    EXPECT_EQ(decoded_snapshot->keys.front().session_id, "session");
    EXPECT_EQ(decoded_snapshot->storage.front().used_bytes, 42);

    MetricBatch batch{.inference = {InferenceMetrics{.object = {TenantId("tenant"), "metric"},
                                                      .session_id = "s"}},
                      .accesses = {AccessRecord{.object = {TenantId("tenant"), "metric"},
                                                .is_hit = true}}};
    const auto decoded_batch = codec.DecodeMetricBatch(codec.EncodeMetricBatch(batch));
    ASSERT_TRUE(decoded_batch.has_value());
    EXPECT_EQ(decoded_batch->inference.front().session_id, "s");
    EXPECT_TRUE(decoded_batch->accesses.front().is_hit);
}

// An EvictionPlan crosses the CFM wire to a remote SubMaster, so the
// per-candidate action and the tier-down budget have to survive the codec:
// otherwise the remote executor receives a plan that looks like pure eviction
// and reclaims the very keys the driver chose to demote.
TEST(IoPatternFrameworkTest, BinaryCfmCodecCarriesTierDownActionAndBudget) {
    CfmBinaryCodec codec;
    EvictionPlan plan{.source_tier = CacheTier::kL1Host,
                      .target_bytes = 4096,
                      .candidates = {EvictionCandidate{
                                         .object = {TenantId("tenant"), "demote"},
                                         .bytes = 4096,
                                         .score = 0.5F,
                                         .target_tier = CacheTier::kLocalDisk,
                                         .action = EvictionAction::kTierDown},
                                     EvictionCandidate{
                                         .object = {TenantId("tenant"), "reclaim"},
                                         .bytes = 4096,
                                         .score = 0.25F,
                                         .target_tier = CacheTier::kL3NofSsd,
                                         .action = EvictionAction::kEvict}},
                      .tier_down_target_bytes = 4096};

    const auto payload = codec.EncodePolicy(plan);
    const auto decoded = codec.DecodePolicy(payload);
    ASSERT_TRUE(decoded.has_value());
    ASSERT_TRUE(std::holds_alternative<EvictionPlan>(*decoded));
    const auto& out = std::get<EvictionPlan>(*decoded);
    ASSERT_EQ(out.candidates.size(), 2);
    EXPECT_EQ(out.candidates[0].action, EvictionAction::kTierDown);
    EXPECT_EQ(out.candidates[1].action, EvictionAction::kEvict);
    EXPECT_EQ(out.tier_down_target_bytes, 4096u);
    EXPECT_EQ(out.target_bytes, 4096u);

    // A payload written before tier down existed stops after the candidate list
    // and still decodes. Both new fields then keep their pre-tier-down meaning
    // (kEvict / 0) instead of silently turning the plan into a demotion.
    const size_t trailer_bytes = sizeof(uint64_t) + out.candidates.size();
    ASSERT_GT(payload.size(), trailer_bytes);
    const auto legacy =
        codec.DecodePolicy(payload.substr(0, payload.size() - trailer_bytes));
    ASSERT_TRUE(legacy.has_value());
    ASSERT_TRUE(std::holds_alternative<EvictionPlan>(*legacy));
    const auto& old = std::get<EvictionPlan>(*legacy);
    ASSERT_EQ(old.candidates.size(), 2);
    EXPECT_EQ(old.candidates[0].action, EvictionAction::kEvict);
    EXPECT_EQ(old.candidates[1].action, EvictionAction::kEvict);
    EXPECT_EQ(old.tier_down_target_bytes, 0u);
}

TEST(IoPatternFrameworkTest, InProcessCfmTransportDispatchesReports) {
    CfmBinaryCodec codec;
    bool received_snapshot = false;
    auto transport = std::make_shared<InProcessCfmRpcTransport>(
        [&received_snapshot](std::string_view method, std::string_view) {
            received_snapshot = method == "report_snapshot";
            return received_snapshot;
        });
    CfmRpcChannel channel(transport, std::make_shared<CfmBinaryCodec>());
    EXPECT_TRUE(channel.SendSnapshot({}));
    EXPECT_TRUE(received_snapshot);

    // Without a bound handler the transport has no receiver; the embedded
    // receiver path is exercised through CfmService/CfmIngress instead.
    auto unbound = std::make_shared<InProcessCfmRpcTransport>();
    CfmRpcChannel unbound_channel(unbound, std::make_shared<CfmBinaryCodec>());
    EXPECT_TRUE(unbound_channel.SendSnapshot({}));
}

TEST(IoPatternFrameworkTest, InProcessTransportDoesNotHoldLockAcrossHandler) {
    std::promise<void> handler_entered;
    std::promise<void> release_handler;
    auto release = release_handler.get_future().share();
    // Only the first invocation blocks and signals entry: the test deliberately
    // issues a second concurrent Send, and re-satisfying the promise from that
    // invocation would throw std::future_error inside the handler and abort the
    // process instead of proving that the transport lock is not held across it.
    std::atomic<int> handler_calls{0};
    auto transport = std::make_shared<InProcessCfmRpcTransport>(
        [&](std::string_view, std::string_view) {
            if (handler_calls.fetch_add(1) == 0) {
                handler_entered.set_value();
                release.wait();
            }
            return true;
        });

    std::thread sender([&] {
        EXPECT_TRUE(transport->Send("report_snapshot", {},
                                    std::chrono::milliseconds(10)));
    });
    if (handler_entered.get_future().wait_for(std::chrono::seconds(1)) !=
        std::future_status::ready) {
        release_handler.set_value();
        sender.join();
        FAIL() << "send handler did not start";
        return;
    }
    // A concurrent Send must not deadlock on the transport mutex while the
    // first handler is still executing.
    std::thread second([&] {
        EXPECT_TRUE(transport->Send("report_snapshot", {},
                                    std::chrono::milliseconds(10)));
    });
    second.join();
    release_handler.set_value();
    sender.join();
}

TEST(IoPatternFrameworkTest, CfmIngressFeedsRuntimeFromMetricBatches) {
    auto runtime = std::make_shared<IoPatternRuntime>(
        IoPatternRuntime::Handlers{.eviction = [](const EvictionPlan&) {
                                      return ErrorCode::OK;
                                  },
                                  .prefetch = [](const PrefetchPlan&) {
                                      return ErrorCode::OK;
                                  },
                                  .admission = [](const AdmissionResult&) {
                                      return ErrorCode::OK;
                                  }});
    auto codec = std::make_shared<CfmBinaryCodec>();
    CfmIngress ingress(runtime, codec);
    MetricBatch batch{.inference = {InferenceMetrics{
                          .object = {TenantId("tenant"), "metric-key"},
                          .session_id = "session",
                          .token_count = 32}},
                      .accesses = {AccessRecord{
                          .object = {TenantId("tenant"), "metric-key"},
                          .block_size = 64,
                          .is_hit = true}}};
    EXPECT_TRUE(ingress.Handle("report_metric_batch", codec->EncodeMetricBatch(batch)));
    const auto snapshot = runtime->Snapshot();
    ASSERT_EQ(snapshot.keys.size(), 1);
    EXPECT_EQ(snapshot.keys.front().session_id, "session");
    EXPECT_EQ(snapshot.keys.front().access_count_window, 1);
}

TEST(IoPatternFrameworkTest, CfmServiceMergesReportsAndExecutesLocally) {
    int admissions = 0;
    auto runtime = std::make_shared<IoPatternRuntime>(
        IoPatternRuntime::Handlers{
            .eviction = [](const EvictionPlan&) { return ErrorCode::OK; },
            .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
            .admission = [&admissions](const AdmissionResult&) {
                ++admissions;
                return ErrorCode::OK;
            }});
    CfmService service(runtime);
    CfmBinaryCodec codec;

    // Reports are merged into the single local runtime: no per-node runtime,
    // no policy queue and no credential gate.
    MetricBatch batch;
    batch.inference.push_back(
        InferenceMetrics{.object = {TenantId("tenant"), "key"},
                         .session_id = "session",
                         .token_count = 32});
    ASSERT_TRUE(service.Send("report_metric_batch",
                             codec.EncodeMetricBatch(batch)));
    const auto merged = service.Snapshot();
    ASSERT_EQ(merged.keys.size(), 1);
    EXPECT_EQ(merged.keys.front().object.key, "key");

    // A delivered policy command executes through the local runtime handlers.
    const auto admission = codec.EncodePolicy(AdmissionResult{
        .object = {TenantId("tenant"), "key"},
        .target_tier = CacheTier::kL1Host,
        .decision = AdmissionDecision::kAdmit});
    ASSERT_TRUE(service.Send("execute_policy", admission));
    EXPECT_EQ(admissions, 1);
    EXPECT_FALSE(service.Send("execute_policy", "malformed"));
    EXPECT_EQ(admissions, 1);
}

TEST(IoPatternFrameworkTest, CfmServiceMergesAllReportsIntoLocalRuntime) {
    auto runtime = std::make_shared<IoPatternRuntime>(
        IoPatternRuntime::Handlers{
            .eviction = [](const EvictionPlan&) { return ErrorCode::OK; },
            .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
            .admission = [](const AdmissionResult&) { return ErrorCode::OK; }});
    CfmService service(runtime);
    CfmBinaryCodec codec;

    MetricBatch first;
    first.accesses.push_back(
        AccessRecord{.object = {TenantId("tenant"), "key-a"},
                     .block_size = 64,
                     .tier = CacheTier::kL1Host,
                     .is_hit = true});
    MetricBatch second;
    second.accesses.push_back(
        AccessRecord{.object = {TenantId("tenant"), "key-b"},
                     .block_size = 128,
                     .tier = CacheTier::kL1Host,
                     .is_hit = true});

    ASSERT_TRUE(service.Send("report_metric_batch",
                             codec.EncodeMetricBatch(first)));
    ASSERT_TRUE(service.Send("report_metric_batch",
                             codec.EncodeMetricBatch(second)));

    // Ownership-addressed reports all land on the receiving SubMaster, whose
    // CFM component owns a single local runtime.
    const auto snapshot = service.Snapshot();
    ASSERT_EQ(snapshot.keys.size(), 2);
    EXPECT_EQ(snapshot.keys[0].object.key, "key-a");
    EXPECT_EQ(snapshot.keys[1].object.key, "key-b");
}

TEST(IoPatternFrameworkTest, CoroRpcCfmTransportRunsTheProductionWirePath) {
    auto runtime = std::make_shared<IoPatternRuntime>(
        IoPatternRuntime::Handlers{
            .eviction = [](const EvictionPlan&) { return ErrorCode::OK; },
            .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
            .admission = [](const AdmissionResult&) { return ErrorCode::OK; }});
    auto service = std::make_shared<CfmService>(runtime);
    CfmRpcService endpoint(service);
    coro_rpc::coro_rpc_server server(1, 0, "127.0.0.1");
    server.register_handler<&CfmRpcService::Send>(&endpoint);
    ASSERT_FALSE(server.async_start().hasResult());

    CoroRpcCfmTransport transport(
        "127.0.0.1:" + std::to_string(server.port()),
        std::chrono::milliseconds(500));

    CfmBinaryCodec codec;
    MetricBatch batch;
    batch.accesses.push_back(
        AccessRecord{.object = {TenantId("tenant"), "remote-key"},
                     .is_hit = true});
    batch.storage.push_back(StorageMetric{.source_id = "reporter",
                                          .tier = CacheTier::kL1Host,
                                          .memory_used_ratio = 0.75F});
    EXPECT_TRUE(transport.Send("report_metric_batch",
                               codec.EncodeMetricBatch(batch),
                               std::chrono::milliseconds(500)));
    // No authentication: the SubMaster merges the report into its local
    // runtime and treats the transport source as the metric origin.
    const auto snapshot = service->Snapshot();
    ASSERT_EQ(snapshot.keys.size(), 1);
    ASSERT_EQ(snapshot.storage.size(), 1);
    EXPECT_EQ(snapshot.keys.front().object.key, "remote-key");
    EXPECT_EQ(snapshot.storage.front().source_id, "reporter");

    // Snapshot reports follow the same unauthenticated path.
    IoPatternSnapshot snapshot_report;
    snapshot_report.keys.push_back(
        KeyMetrics{.object = {TenantId("tenant"), "snap-key"}});
    EXPECT_TRUE(transport.Send("report_snapshot",
                               codec.EncodeSnapshot(snapshot_report),
                               std::chrono::milliseconds(500)));
    EXPECT_EQ(service->Snapshot().keys.back().object.key, "snap-key");
    server.stop();
}

TEST(IoPatternFrameworkTest, LocalCfmExecutesEvictionOnHighWatermarkSnapshot) {
    // High-watermark policy now runs in the SubMaster's own runtime rather
    // than in a separate central CFM process. Reports feed that runtime; a
    // storage observation at >= 0.90 memory ratio then triggers a local
    // eviction plan executed through the SubMaster handlers.
    auto runtime = std::make_shared<IoPatternRuntime>(
        IoPatternRuntime::Handlers{
            .eviction = [](const EvictionPlan&) { return ErrorCode::OK; },
            .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
            .admission = [](const AdmissionResult&) { return ErrorCode::OK; }});
    CfmService service(runtime);
    CfmBinaryCodec codec;
    MetricBatch batch;
    batch.accesses.push_back(
        AccessRecord{.object = {TenantId("tenant"), "cold-key"},
                     .block_size = 1024,
                     .tier = CacheTier::kL1Host,
                     .is_hit = false});
    ASSERT_TRUE(service.Send("report_metric_batch",
                             codec.EncodeMetricBatch(batch)));
    ASSERT_EQ(runtime->Snapshot().keys.size(), 1);

    // Eviction is local: Execute() plans against the merged snapshot and
    // invokes the storage handler directly (no policy delivery round trip).
    const auto status = runtime->Execute(CacheTier::kL1Host, 1024,
                                         TraceHistory{});
    EXPECT_EQ(status.eviction, ErrorCode::OK);
    EXPECT_GE(runtime->ObservabilitySnapshot().policy_decisions, 1);
}

TEST(IoPatternFrameworkTest, ReportDrivenCycleRunsAfterMergedReport) {
    // A merged client report must drive the local analysis -> decision ->
    // execution cycle on the receiving runtime (the driver that makes remote
    // SubMaster CFM execution observable), not only the local watermark
    // thread, the Put admission hook or explicit execute_* RPCs.
    std::mutex observer_mutex;
    std::condition_variable observer_condition;
    std::optional<IoPatternRuntime::ReportDrivenCycleReport> last_report;
    std::atomic<int> eviction_handled{0};
    IoPatternRuntime::Config config;
    config.report_driven_execution = true;
    // Let the detached analyzer finish so pattern selection is deterministic
    // in this test (the worker otherwise uses the 500 us bounded budget).
    config.analysis_timeout_us = 30'000'000;
    config.report_driven_observer =
        [&](const IoPatternRuntime::ReportDrivenCycleReport& report) {
            std::lock_guard lock(observer_mutex);
            last_report = report;
            observer_condition.notify_all();
        };
    auto rt = std::make_shared<IoPatternRuntime>(
        IoPatternRuntime::Handlers{
            .eviction = [&eviction_handled](const EvictionPlan&) {
                ++eviction_handled;
                return ErrorCode::OK;
            },
            .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
            .admission = [](const AdmissionResult&) { return ErrorCode::OK; }},
        std::move(config));
    CfmService service(rt);
    CfmBinaryCodec codec;
    MetricBatch batch;
    batch.accesses.push_back(
        AccessRecord{.object = {TenantId("tenant"), "hot-key"},
                     .observed_at_ns = 1,
                     .block_size = 4096,
                     .tier = CacheTier::kL1Host,
                     .operation = IoOperation::kGet,
                     .is_hit = true});
    batch.storage.push_back(
        StorageMetric{.source_id = "reporter",
                      .observed_at_ns = 1,
                      .tier = CacheTier::kL1Host,
                      .used_bytes = 1024ULL * 1024 * 1024,
                      .capacity_bytes = 1024ULL * 1024 * 1024,
                      .memory_used_ratio = 0.95F});
    ASSERT_TRUE(service.Send("report_metric_batch",
                             codec.EncodeMetricBatch(batch)));

    // Wait for the background cycle to drain the merged report.
    std::unique_lock lock(observer_mutex);
    ASSERT_TRUE(observer_condition.wait_for(lock, std::chrono::seconds(30), [&] {
        return last_report.has_value();
    }));
    ASSERT_TRUE(last_report.has_value());
    EXPECT_GE(last_report->cycle_id, 1);
    EXPECT_GE(last_report->keys_analyzed, 1);
    // 0.95 merged host-memory ratio exceeds the default 0.80 high watermark:
    // the eviction dimension derived a target and executed through the
    // storage handler.
    EXPECT_GT(last_report->eviction_target_bytes, 0);
    EXPECT_GT(eviction_handled.load(), 0);
    EXPECT_EQ(last_report->eviction_status, ErrorCode::OK);
}

TEST(IoPatternFrameworkTest, ReportDrivenCycleSkipsEvictionWithoutPressure) {
    // Below the high watermark the eviction dimension produces no action, but
    // the cycle still runs so the report-driven pipeline stays observable.
    std::mutex observer_mutex;
    std::condition_variable observer_condition;
    std::optional<IoPatternRuntime::ReportDrivenCycleReport> last_report;
    std::atomic<int> eviction_handled{0};
    IoPatternRuntime::Config config;
    config.report_driven_execution = true;
    config.report_driven_observer =
        [&](const IoPatternRuntime::ReportDrivenCycleReport& report) {
            std::lock_guard lock(observer_mutex);
            last_report = report;
            observer_condition.notify_all();
        };
    auto rt = std::make_shared<IoPatternRuntime>(
        IoPatternRuntime::Handlers{
            .eviction = [&eviction_handled](const EvictionPlan&) {
                ++eviction_handled;
                return ErrorCode::OK;
            },
            .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
            .admission = [](const AdmissionResult&) { return ErrorCode::OK; }},
        std::move(config));
    CfmService service(rt);
    CfmBinaryCodec codec;
    MetricBatch batch;
    batch.accesses.push_back(
        AccessRecord{.object = {TenantId("tenant"), "key"},
                     .observed_at_ns = 1,
                     .block_size = 1024,
                     .tier = CacheTier::kL1Host,
                     .operation = IoOperation::kGet,
                     .is_hit = true});
    batch.storage.push_back(
        StorageMetric{.source_id = "reporter",
                      .observed_at_ns = 1,
                      .tier = CacheTier::kL1Host,
                      .used_bytes = 512ULL * 1024 * 1024,
                      .capacity_bytes = 1024ULL * 1024 * 1024,
                      .memory_used_ratio = 0.50F});
    ASSERT_TRUE(service.Send("report_metric_batch",
                             codec.EncodeMetricBatch(batch)));

    std::unique_lock lock(observer_mutex);
    ASSERT_TRUE(observer_condition.wait_for(lock, std::chrono::seconds(5), [&] {
        return last_report.has_value();
    }));
    ASSERT_TRUE(last_report.has_value());
    EXPECT_EQ(last_report->eviction_target_bytes, 0);
    EXPECT_EQ(eviction_handled.load(), 0);
}

TEST(IoPatternFrameworkTest, ReportDrivenColdEvictionRunsWithoutPressure) {
    // The cold-data eviction driver must run even when no storage watermark
    // pressure is present: a report that only contains idle L1 keys still
    // yields a bounded eviction request (cold_eviction=true) so eviction is
    // driven by cold/hot analysis, not only by memory pressure.
    std::mutex observer_mutex;
    std::condition_variable observer_condition;
    std::optional<IoPatternRuntime::ReportDrivenCycleReport> last_report;
    std::atomic<int> eviction_handled{0};
    IoPatternRuntime::Config config;
    config.report_driven_execution = true;
    config.report_driven_cold_eviction = true;
    config.report_driven_cold_eviction_bytes = 128ULL * 1024 * 1024;
    config.report_driven_cold_idle_threshold_us = 0;  // any idle L1 key counts
    config.analysis_timeout_us = 30'000'000;
    config.report_driven_observer =
        [&](const IoPatternRuntime::ReportDrivenCycleReport& report) {
            std::lock_guard lock(observer_mutex);
            last_report = report;
            observer_condition.notify_all();
        };
    auto rt = std::make_shared<IoPatternRuntime>(
        IoPatternRuntime::Handlers{
            .eviction = [&eviction_handled](const EvictionPlan&) {
                ++eviction_handled;
                return ErrorCode::OK;
            },
            .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
            .admission = [](const AdmissionResult&) { return ErrorCode::OK; }},
        std::move(config));
    CfmService service(rt);
    CfmBinaryCodec codec;
    MetricBatch batch;
    batch.accesses.push_back(
        AccessRecord{.object = {TenantId("tenant"), "cold-key"},
                     .observed_at_ns = 1,
                     .block_size = 1024,
                     .tier = CacheTier::kL1Host,
                     .operation = IoOperation::kGet,
                     .is_hit = true});
    // No storage metric => no pressure path; only the cold driver can act.
    ASSERT_TRUE(service.Send("report_metric_batch",
                             codec.EncodeMetricBatch(batch)));

    std::unique_lock lock(observer_mutex);
    ASSERT_TRUE(observer_condition.wait_for(lock, std::chrono::seconds(30), [&] {
        return last_report.has_value();
    }));
    ASSERT_TRUE(last_report.has_value());
    EXPECT_TRUE(last_report->cold_eviction);
    EXPECT_GT(last_report->eviction_target_bytes, 0);
    EXPECT_GT(eviction_handled.load(), 0);
    EXPECT_EQ(last_report->eviction_status, ErrorCode::OK);
}

TEST(IoPatternFrameworkTest, ReportDrivenTierDownLabelsCandidatesWithoutPressure) {
    // The tier-down driver must make the policy label its candidates as
    // demotions: the plan it produces copies the selected keys down to
    // LOCAL_DISK and keeps their MEMORY replica, so nothing is reclaimed. It
    // fires from a report with no storage pressure at all.
    std::mutex observer_mutex;
    std::condition_variable observer_condition;
    std::optional<IoPatternRuntime::ReportDrivenCycleReport> last_report;
    std::mutex plan_mutex;
    std::optional<EvictionPlan> handled_plan;
    IoPatternRuntime::Config config;
    config.report_driven_execution = true;
    // The budget alone enables the driver: there is no enable flag.
    config.tier_down_bytes_per_cycle = 96ULL * 1024 * 1024;
    // Let the detached analyzer finish so candidate selection is deterministic.
    config.analysis_timeout_us = 30'000'000;
    config.report_driven_observer =
        [&](const IoPatternRuntime::ReportDrivenCycleReport& report) {
            std::lock_guard lock(observer_mutex);
            last_report = report;
            observer_condition.notify_all();
        };
    auto rt = std::make_shared<IoPatternRuntime>(
        IoPatternRuntime::Handlers{
            .eviction =
                [&](const EvictionPlan& plan) {
                    std::lock_guard lock(plan_mutex);
                    handled_plan = plan;
                    return ErrorCode::OK;
                },
            .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
            .admission = [](const AdmissionResult&) { return ErrorCode::OK; }},
        std::move(config));
    CfmService service(rt);
    CfmBinaryCodec codec;
    MetricBatch batch;
    batch.accesses.push_back(
        AccessRecord{.object = {TenantId("tenant"), "demote-key"},
                     .observed_at_ns = 1,
                     .block_size = 4096,
                     .tier = CacheTier::kL1Host,
                     .operation = IoOperation::kGet,
                     .is_hit = true});
    // No storage metric, no cold-eviction driver: only tier down can act.
    ASSERT_TRUE(service.Send("report_metric_batch",
                             codec.EncodeMetricBatch(batch)));

    std::unique_lock lock(observer_mutex);
    ASSERT_TRUE(observer_condition.wait_for(lock, std::chrono::seconds(30), [&] {
        return last_report.has_value();
    }));
    ASSERT_TRUE(last_report.has_value());
    EXPECT_TRUE(last_report->tier_down);
    EXPECT_FALSE(last_report->cold_eviction);
    EXPECT_EQ(last_report->eviction_target_bytes, 96ULL * 1024 * 1024);
    EXPECT_GT(last_report->eviction_candidates, 0u);
    EXPECT_EQ(last_report->eviction_status, ErrorCode::OK);

    std::lock_guard plan_lock(plan_mutex);
    ASSERT_TRUE(handled_plan.has_value());
    // The budget is carried as a demotion budget, and every candidate is
    // labelled a demotion -- the action comes from the driver, not target_tier.
    EXPECT_EQ(handled_plan->tier_down_target_bytes, 96ULL * 1024 * 1024);
    ASSERT_FALSE(handled_plan->candidates.empty());
    for (const auto& candidate : handled_plan->candidates) {
        EXPECT_EQ(candidate.action, EvictionAction::kTierDown);
    }
}

TEST(IoPatternFrameworkTest, ReportDrivenTierDownYieldsToPressureEviction) {
    // A demotion frees nothing, so a reclaim always wins the cycle: with the
    // tier-down driver enabled and host memory above the high watermark, the
    // same configuration must produce an eviction plan instead of a demotion.
    std::mutex observer_mutex;
    std::condition_variable observer_condition;
    std::optional<IoPatternRuntime::ReportDrivenCycleReport> last_report;
    std::mutex plan_mutex;
    std::optional<EvictionPlan> handled_plan;
    IoPatternRuntime::Config config;
    config.report_driven_execution = true;
    // Same budget-only enablement, with host memory above the high watermark.
    config.tier_down_bytes_per_cycle = 96ULL * 1024 * 1024;
    config.analysis_timeout_us = 30'000'000;
    config.report_driven_observer =
        [&](const IoPatternRuntime::ReportDrivenCycleReport& report) {
            std::lock_guard lock(observer_mutex);
            last_report = report;
            observer_condition.notify_all();
        };
    auto rt = std::make_shared<IoPatternRuntime>(
        IoPatternRuntime::Handlers{
            .eviction =
                [&](const EvictionPlan& plan) {
                    std::lock_guard lock(plan_mutex);
                    handled_plan = plan;
                    return ErrorCode::OK;
                },
            .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
            .admission = [](const AdmissionResult&) { return ErrorCode::OK; }},
        std::move(config));
    CfmService service(rt);
    CfmBinaryCodec codec;
    MetricBatch batch;
    batch.accesses.push_back(
        AccessRecord{.object = {TenantId("tenant"), "hot-key"},
                     .observed_at_ns = 1,
                     .block_size = 4096,
                     .tier = CacheTier::kL1Host,
                     .operation = IoOperation::kGet,
                     .is_hit = true});
    batch.storage.push_back(
        StorageMetric{.source_id = "reporter",
                      .observed_at_ns = 1,
                      .tier = CacheTier::kL1Host,
                      .used_bytes = 1024ULL * 1024 * 1024,
                      .capacity_bytes = 1024ULL * 1024 * 1024,
                      .memory_used_ratio = 0.95F});
    ASSERT_TRUE(service.Send("report_metric_batch",
                             codec.EncodeMetricBatch(batch)));

    std::unique_lock lock(observer_mutex);
    ASSERT_TRUE(observer_condition.wait_for(lock, std::chrono::seconds(30), [&] {
        return last_report.has_value();
    }));
    ASSERT_TRUE(last_report.has_value());
    EXPECT_FALSE(last_report->tier_down);
    EXPECT_FALSE(last_report->cold_eviction);
    EXPECT_GT(last_report->eviction_target_bytes, 0);
    EXPECT_EQ(last_report->eviction_status, ErrorCode::OK);

    std::lock_guard plan_lock(plan_mutex);
    ASSERT_TRUE(handled_plan.has_value());
    EXPECT_EQ(handled_plan->tier_down_target_bytes, 0u);
    ASSERT_FALSE(handled_plan->candidates.empty());
    for (const auto& candidate : handled_plan->candidates) {
        EXPECT_EQ(candidate.action, EvictionAction::kEvict);
    }
}

TEST(IoPatternFrameworkTest, ReportDrivenTierDownPavesColdDataBeforeColdEviction) {
    // Both drivers act below the watermark, but only one can own the cycle. The
    // tier-down budget takes it: demoting a key that the same cycle would have
    // discarded defeats the point of paving cold data down first, so the
    // cold-eviction driver only keeps the slot when no tier-down budget is
    // configured (see ReportDrivenColdEvictionRunsWithoutPressure).
    std::mutex observer_mutex;
    std::condition_variable observer_condition;
    std::optional<IoPatternRuntime::ReportDrivenCycleReport> last_report;
    std::mutex plan_mutex;
    std::optional<EvictionPlan> handled_plan;
    IoPatternRuntime::Config config;
    config.report_driven_execution = true;
    config.tier_down_bytes_per_cycle = 96ULL * 1024 * 1024;
    config.report_driven_cold_eviction = true;
    config.report_driven_cold_eviction_bytes = 128ULL * 1024 * 1024;
    config.report_driven_cold_idle_threshold_us = 0;
    config.analysis_timeout_us = 30'000'000;
    config.report_driven_observer =
        [&](const IoPatternRuntime::ReportDrivenCycleReport& report) {
            std::lock_guard lock(observer_mutex);
            last_report = report;
            observer_condition.notify_all();
        };
    auto rt = std::make_shared<IoPatternRuntime>(
        IoPatternRuntime::Handlers{
            .eviction =
                [&](const EvictionPlan& plan) {
                    std::lock_guard lock(plan_mutex);
                    handled_plan = plan;
                    return ErrorCode::OK;
                },
            .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
            .admission = [](const AdmissionResult&) { return ErrorCode::OK; }},
        std::move(config));
    CfmService service(rt);
    CfmBinaryCodec codec;
    MetricBatch batch;
    batch.accesses.push_back(
        AccessRecord{.object = {TenantId("tenant"), "cold-key"},
                     .observed_at_ns = 1,
                     .block_size = 4096,
                     .tier = CacheTier::kL1Host,
                     .operation = IoOperation::kGet,
                     .is_hit = true});
    ASSERT_TRUE(service.Send("report_metric_batch",
                             codec.EncodeMetricBatch(batch)));

    std::unique_lock lock(observer_mutex);
    ASSERT_TRUE(observer_condition.wait_for(lock, std::chrono::seconds(30), [&] {
        return last_report.has_value();
    }));
    ASSERT_TRUE(last_report.has_value());
    EXPECT_TRUE(last_report->tier_down);
    EXPECT_FALSE(last_report->cold_eviction);
    // The tier-down budget, not the cold-eviction budget, sized this cycle.
    EXPECT_EQ(last_report->eviction_target_bytes, 96ULL * 1024 * 1024);

    std::lock_guard plan_lock(plan_mutex);
    ASSERT_TRUE(handled_plan.has_value());
    EXPECT_EQ(handled_plan->tier_down_target_bytes, 96ULL * 1024 * 1024);
    ASSERT_FALSE(handled_plan->candidates.empty());
    for (const auto& candidate : handled_plan->candidates) {
        EXPECT_EQ(candidate.action, EvictionAction::kTierDown);
    }
}

// The below-watermark drivers must not depend on client reports: reports only
// flow while the workload does, so an idle cluster would never pave cold data
// down. The tick lets them run on their own, with no report at all.
TEST(IoPatternFrameworkTest, ReportDrivenTickRunsTierDownWithoutAnyReport) {
    std::mutex observer_mutex;
    std::condition_variable observer_condition;
    std::optional<IoPatternRuntime::ReportDrivenCycleReport> last_report;
    std::mutex plan_mutex;
    std::optional<EvictionPlan> handled_plan;
    IoPatternRuntime::Config config;
    config.report_driven_execution = true;
    config.tier_down_bytes_per_cycle = 64ULL * 1024 * 1024;
    config.tick_interval_ms = 50;
    config.analysis_timeout_us = 30'000'000;
    config.report_driven_observer =
        [&](const IoPatternRuntime::ReportDrivenCycleReport& report) {
            std::lock_guard lock(observer_mutex);
            last_report = report;
            observer_condition.notify_all();
        };
    auto rt = std::make_shared<IoPatternRuntime>(
        IoPatternRuntime::Handlers{
            .eviction =
                [&](const EvictionPlan& plan) {
                    std::lock_guard lock(plan_mutex);
                    handled_plan = plan;
                    return ErrorCode::OK;
                },
            .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
            .admission = [](const AdmissionResult&) { return ErrorCode::OK; }},
        std::move(config));

    // Recorded locally on the owning SubMaster, exactly as PutEnd/Get do; no
    // report is ever sent, so the tick is the only possible wakeup.
    rt->RecordAccess("local-key",
                     AccessRecord{.object = {TenantId("tenant"), "local-key"},
                                  .observed_at_ns = 1,
                                  .block_size = 4096,
                                  .tier = CacheTier::kL1Host,
                                  .operation = IoOperation::kGet,
                                  .is_hit = true});

    std::unique_lock lock(observer_mutex);
    ASSERT_TRUE(observer_condition.wait_for(lock, std::chrono::seconds(10), [&] {
        return last_report.has_value();
    })) << "the periodic tick never ran a cycle";
    ASSERT_TRUE(last_report.has_value());
    EXPECT_TRUE(last_report->tier_down);
    EXPECT_EQ(last_report->eviction_target_bytes, 64ULL * 1024 * 1024);

    std::lock_guard plan_lock(plan_mutex);
    ASSERT_TRUE(handled_plan.has_value());
    EXPECT_EQ(handled_plan->tier_down_target_bytes, 64ULL * 1024 * 1024);
    for (const auto& candidate : handled_plan->candidates) {
        EXPECT_EQ(candidate.action, EvictionAction::kTierDown);
    }
}

// A tick cycle must not reclaim: the master's watermark thread owns pressure, and
// the ratio it records stays in the collector until the next breach, so honouring
// it here would reclaim the same excess twice.
TEST(IoPatternFrameworkTest, ReportDrivenTickIgnoresRecordedPressure) {
    std::mutex observer_mutex;
    std::condition_variable observer_condition;
    std::optional<IoPatternRuntime::ReportDrivenCycleReport> last_report;
    std::mutex plan_mutex;
    std::optional<EvictionPlan> handled_plan;
    IoPatternRuntime::Config config;
    config.report_driven_execution = true;
    config.tier_down_bytes_per_cycle = 64ULL * 1024 * 1024;
    config.tick_interval_ms = 50;
    config.analysis_timeout_us = 30'000'000;
    config.report_driven_observer =
        [&](const IoPatternRuntime::ReportDrivenCycleReport& report) {
            std::lock_guard lock(observer_mutex);
            last_report = report;
            observer_condition.notify_all();
        };
    auto rt = std::make_shared<IoPatternRuntime>(
        IoPatternRuntime::Handlers{
            .eviction =
                [&](const EvictionPlan& plan) {
                    std::lock_guard lock(plan_mutex);
                    handled_plan = plan;
                    return ErrorCode::OK;
                },
            .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
            .admission = [](const AdmissionResult&) { return ErrorCode::OK; }},
        std::move(config));

    rt->RecordAccess("local-key",
                     AccessRecord{.object = {TenantId("tenant"), "local-key"},
                                  .observed_at_ns = 1,
                                  .block_size = 4096,
                                  .tier = CacheTier::kL1Host,
                                  .operation = IoOperation::kGet,
                                  .is_hit = true});
    // Host memory far above the high watermark: a report-triggered cycle would
    // derive a reclaim request from this, a tick-triggered one must not.
    rt->RecordStorageMetric(StorageMetric{.source_id = "master-memory",
                                          .observed_at_ns = 1,
                                          .tier = CacheTier::kL1Host,
                                          .used_bytes = 1024ULL * 1024 * 1024,
                                          .capacity_bytes =
                                              1024ULL * 1024 * 1024,
                                          .memory_used_ratio = 0.95F});

    std::unique_lock lock(observer_mutex);
    ASSERT_TRUE(observer_condition.wait_for(lock, std::chrono::seconds(10), [&] {
        return last_report.has_value();
    }));
    ASSERT_TRUE(last_report.has_value());
    EXPECT_TRUE(last_report->tier_down);
    EXPECT_FALSE(last_report->cold_eviction);
    EXPECT_EQ(last_report->eviction_target_bytes, 64ULL * 1024 * 1024);

    std::lock_guard plan_lock(plan_mutex);
    ASSERT_TRUE(handled_plan.has_value());
    EXPECT_EQ(handled_plan->tier_down_target_bytes, 64ULL * 1024 * 1024);
    ASSERT_FALSE(handled_plan->candidates.empty());
    for (const auto& candidate : handled_plan->candidates) {
        EXPECT_EQ(candidate.action, EvictionAction::kTierDown);
    }
}

// The tick is gated on a below-watermark driver being configured, so a cluster
// that only uses the watermark path pays no periodic analysis (and gets no
// surprise cycles while idle).
TEST(IoPatternFrameworkTest, ReportDrivenTickIsOffWithoutABelowWatermarkDriver) {
    std::mutex observer_mutex;
    std::condition_variable observer_condition;
    bool observed = false;
    IoPatternRuntime::Config config;
    config.report_driven_execution = true;
    config.tick_interval_ms = 50;
    config.report_driven_observer =
        [&](const IoPatternRuntime::ReportDrivenCycleReport&) {
            std::lock_guard lock(observer_mutex);
            observed = true;
            observer_condition.notify_all();
        };
    auto rt = std::make_shared<IoPatternRuntime>(
        IoPatternRuntime::Handlers{
            .eviction = [](const EvictionPlan&) { return ErrorCode::OK; },
            .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
            .admission = [](const AdmissionResult&) { return ErrorCode::OK; }},
        std::move(config));

    std::unique_lock lock(observer_mutex);
    EXPECT_FALSE(observer_condition.wait_for(lock, std::chrono::milliseconds(500),
                                             [&] { return observed; }))
        << "no driver is configured, so nothing should be running cycles";
}

TEST(IoPatternFrameworkTest, ReporterBackgroundLifecycleFlushesOnStop) {
    size_t batches = 0;
    IoPatternReporter reporter(4, [&](const MetricBatch&) {
        ++batches;
        return true;
    });
    reporter.Enqueue(InferenceMetrics{});
    reporter.Start();
    reporter.Stop();
    EXPECT_EQ(batches, 1);
}

TEST(IoPatternFrameworkTest, DegradingPolicyEngineSwitchesToFallback) {
    auto primary = std::make_shared<ComposedPolicyEngine>(
        std::make_shared<TestEvictionOps>(), nullptr, nullptr);
    auto fallback = std::make_shared<ComposedPolicyEngine>(nullptr, nullptr,
                                                            nullptr);
    DegradingPolicyEngine engine(primary, fallback, 2);
    EXPECT_FALSE(engine.degraded());
    EXPECT_EQ(engine.PlanEviction({}, CacheTier::kL1Host, 10).target_bytes, 10);
    engine.RecordFailure();
    engine.RecordFailure();
    EXPECT_TRUE(engine.degraded());
    EXPECT_TRUE(engine.PlanEviction({}, CacheTier::kL1Host, 10)
                    .candidates.empty());
    engine.ForceDegraded(false);
    EXPECT_FALSE(engine.degraded());
    EXPECT_EQ(engine.consecutive_failures(), 0);
}

TEST(IoPatternFrameworkTest, FeedbackWindowAggregatesBoundedSamples) {
    PolicyFeedbackWindow window(2);
    window.Record({.hit_rate_delta = -0.2F, .prefetch_accuracy = 0.5F});
    window.Record({.hit_rate_delta = 0.1F, .prefetch_accuracy = 0.9F});
    window.Record({.hit_rate_delta = -0.4F, .prefetch_accuracy = 0.3F});
    const auto stats = window.Snapshot();
    EXPECT_EQ(stats.samples, 2);
    // Capacity 2 keeps the two most recent samples, i.e. the second and third
    // records: (-0.4 + 0.1) / 2. The previous expectation averaged the first and
    // third records, which no bounded window can produce.
    EXPECT_FLOAT_EQ(stats.hit_rate_delta, (0.1F - 0.4F) / 2.0F);
    EXPECT_FLOAT_EQ(stats.prefetch_accuracy, (0.9F + 0.3F) / 2.0F);
}

TEST(IoPatternFrameworkTest, AdaptiveTunerChangesWeightsAfterNegativeStreak) {
    AdaptivePolicyTuner tuner(3);
    ScoreBasedEvictionConfig config;
    // prefetch_accuracy is stated explicitly: leaving it at its 0.0F default
    // would trip the tuner's conservative branch (prefetch_accuracy < 0.2F) on
    // the very first sample and the frequency/idle streak path would never run.
    const PolicyFeedbackStats negative{.hit_rate_delta = -0.1F,
                                       .prefetch_accuracy = 1.0F};
    EXPECT_FALSE(tuner.Tune(negative, config));
    EXPECT_FALSE(tuner.Tune(negative, config));
    EXPECT_TRUE(tuner.Tune(negative, config));
    EXPECT_FLOAT_EQ(config.frequency_weight, 0.8F);
    EXPECT_FLOAT_EQ(config.idle_weight, 1.1F);
    EXPECT_FALSE(tuner.Tune({.hit_rate_delta = 0.0F, .prefetch_accuracy = 1.0F},
                            config));
}

TEST(IoPatternFrameworkTest, AdaptiveTunerHandlesChurnAndPersistsChanges) {
    AdaptivePolicyTuner tuner(3);
    ScoreBasedEvictionConfig config;
    bool persisted = false;
    tuner.SetPersistenceCallback(
        [&](const ScoreBasedEvictionConfig&) { persisted = true; });
    EXPECT_TRUE(tuner.Tune({.eviction_churn = 0.8F}, config));
    EXPECT_TRUE(tuner.conservative());
    EXPECT_TRUE(persisted);
}

TEST(IoPatternFrameworkTest, ObservabilityTracksPolicyAndDegradeCounters) {
    IoPatternObservability metrics;
    metrics.RecordCollectLatency(10);
    metrics.RecordCollectLatency(3);
    metrics.RecordAnalyzeLatency(20);
    metrics.RecordPolicyDecision(true);
    metrics.RecordPolicyDecision(false);
    metrics.RecordFalsePositive();
    metrics.RecordDegrade();
    metrics.RecordReportDrop(2);
    const auto snapshot = metrics.Snapshot();
    EXPECT_EQ(snapshot.collect_latency_us, 10);
    EXPECT_EQ(snapshot.analyze_latency_us, 20);
    EXPECT_EQ(snapshot.policy_decisions, 2);
    EXPECT_EQ(snapshot.strategy_hits, 1);
    EXPECT_EQ(snapshot.strategy_trials, 2);
    EXPECT_EQ(snapshot.false_positives, 1);
    EXPECT_EQ(snapshot.degrade_count, 1);
    EXPECT_EQ(snapshot.report_drop_count, 2);
    EXPECT_FLOAT_EQ(snapshot.strategy_hit_rate, 0.5F);
    EXPECT_FLOAT_EQ(snapshot.false_positive_rate, 0.5F);
    EXPECT_FLOAT_EQ(metrics.Snapshot(2.0).policy_decision_qps, 1.0F);
}

TEST(IoPatternFrameworkTest, SlidingWindowAnalyzerComputesPercentiles) {
    SlidingWindowAnalyzer analyzer(100);
    IoPatternSnapshot first;
    first.generated_at_ns = 10;
    first.keys.push_back(KeyMetrics{.object = {TenantId("tenant"), "first"},
                                    .access_count_window = 1,
                                    .block_size = 100,
                                    .token_count = 20 * 1024,
                                    .prefix_fanout = 20,
                                    .match_length = 512});
    IoPatternSnapshot second;
    second.generated_at_ns = 50;
    second.keys.push_back(KeyMetrics{.object = {TenantId("tenant"), "second"},
                                     .access_count_window = 5,
                                     .block_size = 300,
                                     .token_count = 30,
                                     .prefix_fanout = 20,
                                     .match_length = 300});
    // Both snapshots must reach the analyzer: the aggregate has to hold two
    // samples for kMixed (one code-agent shaped key, one conversation shaped
    // key), and the percentile expectations below are computed over both. The
    // first snapshot was previously built and then never fed to the analyzer,
    // which left a single sample and made the kMixed expectation unreachable.
    analyzer.Analyze(first);
    EXPECT_EQ(analyzer.DetectWorkloadType(second), WorkloadType::kMixed);
    const auto stats = analyzer.FeatureStats();
    EXPECT_EQ(stats.samples, 2);
    // Percentile() ranks with size/2, so two samples select the upper-middle
    // element: sorted {30, 20480} at index 1.
    EXPECT_EQ(stats.token_median, 20480);
    EXPECT_EQ(stats.fanout_p90, 20);
    EXPECT_EQ(stats.block_p90, 300);
}

TEST(IoPatternFrameworkTest, SlidingWindowDeduplicatesObjectsAndBoundsHistory) {
    SlidingWindowAnalyzer analyzer(1'000, {}, 2);
    const ObjectRef object{TenantId("tenant-a"), "same-key"};
    for (uint64_t timestamp = 1; timestamp <= 3; ++timestamp) {
        IoPatternSnapshot snapshot;
        snapshot.generated_at_ns = timestamp;
        snapshot.keys.push_back(KeyMetrics{.object = object,
                                           .access_count_window = timestamp,
                                           .token_count =
                                               static_cast<uint32_t>(timestamp)});
        analyzer.Analyze(snapshot);
    }

    const auto stats = analyzer.FeatureStats();
    EXPECT_EQ(stats.samples, 1);
    EXPECT_EQ(stats.token_median, 3);
    EXPECT_EQ(stats.frequency_median, 3);
}

TEST(IoPatternFrameworkTest, SlidingWindowCapsASingleOversizedSnapshot) {
    SlidingWindowAnalyzer analyzer(1'000, {}, 2);
    IoPatternSnapshot snapshot;
    snapshot.generated_at_ns = 1;
    snapshot.keys = {
        KeyMetrics{.object = {TenantId("tenant"), "c"}},
        KeyMetrics{.object = {TenantId("tenant"), "a"}},
        KeyMetrics{.object = {TenantId("tenant"), "b"}},
    };

    analyzer.Analyze(snapshot);
    EXPECT_EQ(analyzer.FeatureStats().samples, 2);
}

TEST(IoPatternFrameworkTest, KMeansFallbackLabelsIndependentSessions) {
    SlidingWindowAnalyzer analyzer;
    IoPatternSnapshot snapshot;
    snapshot.generated_at_ns = 1;
    snapshot.keys = {
        KeyMetrics{.object = {TenantId("tenant-a"), "long"},
                   .session_id = "code-session",
                   .token_count = 20 * 1024,
                   .prefix_fanout = 20,
                   .match_length = 512},
        KeyMetrics{.object = {TenantId("tenant-b"), "small"},
                   .session_id = "recommendation-session",
                   .access_count_window = 30,
                   .block_size = 64 * 1024},
    };

    const auto result = analyzer.Analyze(snapshot);
    EXPECT_EQ(result.workload_type, WorkloadType::kMixed);
    ASSERT_EQ(result.sessions.size(), 2);
    EXPECT_NE(result.sessions[0].workload_type, result.sessions[1].workload_type);
}

TEST(IoPatternFrameworkTest, TierExecutorBridgesPolicyResults) {
    int evictions = 0;
    int prefetches = 0;
    int admissions = 0;
    TierOperationExecutor executor(
        [&](const EvictionPlan&) { ++evictions; return ErrorCode::OK; },
        [&](const PrefetchPlan&) { ++prefetches; return ErrorCode::OK; },
        [&](const AdmissionResult&) { ++admissions; return ErrorCode::OK; });
    PolicyResult result;
    result.eviction.target_bytes = 1024;
    result.prefetch.candidates.push_back(PrefetchCandidate{});
    result.admissions.push_back(
        AdmissionResult{.decision = AdmissionDecision::kAdmit});
    const auto status = executor.Execute(result);
    EXPECT_EQ(status.eviction, ErrorCode::OK);
    EXPECT_EQ(status.prefetch, ErrorCode::OK);
    ASSERT_EQ(status.admissions.size(), 1);
    EXPECT_EQ(status.admissions.front(), ErrorCode::OK);
    EXPECT_EQ(evictions, 1);
    EXPECT_EQ(prefetches, 1);
    EXPECT_EQ(admissions, 1);

    TierOperationExecutor degraded({}, {}, {});
    const auto degraded_status = degraded.Execute(result);
    EXPECT_TRUE(degraded_status.degraded);
    EXPECT_EQ(degraded_status.prefetch,
              ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

TEST(IoPatternFrameworkTest, LegacyEvictionAdapterUsesLruFallback) {
    auto lru = std::make_shared<LRUEvictionStrategy>();
    LegacyEvictionOps fallback(lru);
    PolicyContext context;
    KeyMetrics first{.object = {TenantId("tenant-a"), "first"},
                     .block_size = 10,
                     .replica_tiers = CacheTierBit(CacheTier::kL1Host)};
    KeyMetrics second{.object = {TenantId("tenant-a"), "second"},
                     .block_size = 20,
                     .replica_tiers = CacheTierBit(CacheTier::kL1Host)};
    context.snapshot.keys = {first, second};
    const auto plan = fallback.Evaluate(context, CacheTier::kL1Host, 10);
    ASSERT_EQ(plan.candidates.size(), 1);
    EXPECT_EQ(plan.candidates.front().object.key, "first");
}

TEST(IoPatternFrameworkTest, ScoreBasedEvictionMayCrossTheByteTarget) {
    PolicyContext context;
    context.snapshot.keys = {
        KeyMetrics{.object = {TenantId("tenant-a"), "large"},
                   .block_size = 64,
                   .replica_tiers = CacheTierBit(CacheTier::kL1Host)}};
    context.analysis.keys = {
        KeyPattern{.object = {TenantId("tenant-a"), "large"}}};
    ScoreBasedEvictionOps eviction;

    const auto plan = eviction.Evaluate(context, CacheTier::kL1Host, 32);
    ASSERT_EQ(plan.candidates.size(), 1);
    EXPECT_EQ(plan.candidates.front().bytes, 64);
}

TEST(IoPatternFrameworkTest, ScoreBasedEvictionUsesTierSpecificSignals) {
    PolicyContext context;
    context.snapshot.keys = {
        KeyMetrics{.object = {TenantId("tenant-a"), "small-single-copy"},
                   .block_size = 10,
                   .other_replica_count = 0,
                   .replica_tiers = CacheTierBit(CacheTier::kL3NofSsd)},
        KeyMetrics{.object = {TenantId("tenant-a"), "large-redundant"},
                   .block_size = 100,
                   .other_replica_count = 1,
                   .replica_tiers = CacheTierBit(CacheTier::kL3NofSsd)},
    };
    context.analysis.keys = {
        KeyPattern{.object = context.snapshot.keys[0].object, .idle_score = 1.0F},
        KeyPattern{.object = context.snapshot.keys[1].object, .idle_score = 1.0F},
    };

    ScoreBasedEvictionOps eviction;
    const auto plan = eviction.Evaluate(context, CacheTier::kL3NofSsd, 110);

    ASSERT_EQ(plan.candidates.size(), 2);
    EXPECT_EQ(plan.candidates.front().object.key, "large-redundant");
    EXPECT_EQ(plan.candidates.front().target_tier, CacheTier::kL3NofSsd);
}

TEST(IoPatternFrameworkTest, TierDownTemplatesChooseDocumentedTargets) {
    PolicyContext context;
    context.snapshot.keys = {KeyMetrics{
        .object = {TenantId("tenant"), "key"},
        .block_size = 64,
        .replica_tiers = CacheTierBit(CacheTier::kL0Hbm)}};
    context.analysis.keys = {KeyPattern{.object = context.snapshot.keys.front().object}};

    ScoreBasedEvictionOps code({.tier_down_mode = TierDownMode::kSkipHost});
    EXPECT_EQ(code.Evaluate(context, CacheTier::kL0Hbm, 64)
                  .candidates.front().target_tier,
              CacheTier::kL2Segment);
    ScoreBasedEvictionOps recommendation(
        {.tier_down_mode = TierDownMode::kStepwise});
    EXPECT_EQ(recommendation.Evaluate(context, CacheTier::kL0Hbm, 64)
                  .candidates.front().target_tier,
              CacheTier::kL1Host);
}

// The candidate action is decided by the driver (PolicyContext::tier_down) and
// must never be derived from target_tier: TierDownTarget() returns source+1 for
// L0/L1/L2 alike, so a derived action would label every candidate a demotion and
// eviction would stop working entirely.
TEST(IoPatternFrameworkTest, TierDownContextLabelsCandidatesAndBudget) {
    PolicyContext context;
    context.snapshot.keys = {KeyMetrics{
        .object = {TenantId("tenant"), "key"},
        .block_size = 64,
        .replica_tiers = CacheTierBit(CacheTier::kL1Host)}};
    context.analysis.keys = {
        KeyPattern{.object = context.snapshot.keys.front().object}};

    ScoreBasedEvictionOps ops;

    const auto reclaimed = ops.Evaluate(context, CacheTier::kL1Host, 64);
    ASSERT_EQ(reclaimed.candidates.size(), 1);
    EXPECT_EQ(reclaimed.candidates.front().object.key, "key");
    EXPECT_EQ(reclaimed.candidates.front().action, EvictionAction::kEvict);
    EXPECT_EQ(reclaimed.tier_down_target_bytes, 0u);

    context.tier_down = true;
    const auto demoted = ops.Evaluate(context, CacheTier::kL1Host, 64);
    ASSERT_EQ(demoted.candidates.size(), 1);
    EXPECT_EQ(demoted.candidates.front().object.key, "key");
    EXPECT_EQ(demoted.candidates.front().action, EvictionAction::kTierDown);
    EXPECT_EQ(demoted.tier_down_target_bytes, 64u);
    // Same victims either way: only the action differs.
    EXPECT_EQ(demoted.candidates.front().target_tier,
              reclaimed.candidates.front().target_tier);
}

// A demotion keeps the MEMORY replica, so a key that already has a replica below
// it has nothing left to copy down. Without that exclusion the driver spins on
// the same keys every cycle -- they stay L1 candidates and the eviction scorer
// actively prefers lower-replica-backed victims -- so the rest of the cold set is
// never paved and the SSD never grows past one budget.
TEST(IoPatternFrameworkTest, TierDownSkipsKeysThatAlreadyHaveALowerReplica) {
    PolicyContext context;
    context.snapshot.keys = {
        KeyMetrics{.object = {TenantId("tenant"), "already-paved"},
                   .block_size = 64,
                   .replica_tiers = CacheTierBit(CacheTier::kL1Host) |
                                    CacheTierBit(CacheTier::kL3NofSsd)},
        KeyMetrics{.object = {TenantId("tenant"), "not-paved"},
                   .block_size = 64,
                   .replica_tiers = CacheTierBit(CacheTier::kL1Host)},
    };
    context.analysis.keys = {
        KeyPattern{.object = context.snapshot.keys[0].object},
        KeyPattern{.object = context.snapshot.keys[1].object},
    };

    ScoreBasedEvictionOps ops;

    // Eviction keeps both keys eligible: the exclusion below is tier-down only,
    // and the scorer still ranks the lower-replica-backed key first as the safe
    // victim to reclaim.
    const auto reclaimed = ops.Evaluate(context, CacheTier::kL1Host, 128);
    ASSERT_EQ(reclaimed.candidates.size(), 2);
    EXPECT_EQ(reclaimed.candidates.front().object.key, "already-paved");
    EXPECT_EQ(reclaimed.candidates.front().action, EvictionAction::kEvict);

    // Tier down must only consider the key that still needs a disk copy.
    context.tier_down = true;
    const auto demoted = ops.Evaluate(context, CacheTier::kL1Host, 128);
    ASSERT_EQ(demoted.candidates.size(), 1);
    EXPECT_EQ(demoted.candidates.front().object.key, "not-paved");
    EXPECT_EQ(demoted.candidates.front().action, EvictionAction::kTierDown);
}

TEST(IoPatternFrameworkTest, PrefetchRequiresConfidenceAndNeverPromotesToHbm) {
    PolicyContext context;
    context.snapshot.keys = {
        KeyMetrics{.object = {TenantId("tenant-a"), "low-confidence"},
                   .block_size = 64,
                   .replica_tiers = CacheTierBit(CacheTier::kL3NofSsd)},
        KeyMetrics{.object = {TenantId("tenant-a"), "host-only"},
                   .block_size = 64,
                   .replica_tiers = CacheTierBit(CacheTier::kL1Host)},
    };
    context.analysis.keys = {
        KeyPattern{.object = context.snapshot.keys[0].object, .confidence = 0.5F},
        KeyPattern{.object = context.snapshot.keys[1].object, .confidence = 0.9F},
    };
    TraceHistory trace{.events = {
        TraceEvent{.object = context.snapshot.keys[0].object,
                   .match_length = 512,
                   .is_hit = true},
        TraceEvent{.object = context.snapshot.keys[1].object,
                   .match_length = 512,
                   .is_hit = true},
    }};

    TraceBasedPrefetchOps prefetch;
    const auto plan = prefetch.Evaluate(context, trace);

    EXPECT_TRUE(plan.candidates.empty());
}

TEST(IoPatternFrameworkTest, RuntimeConnectsCollectionAnalysisPolicyAndHandlers) {
    int evictions = 0;
    int prefetches = 0;
    int admissions = 0;
    IoPatternRuntime runtime(
        IoPatternRuntime::Handlers{
            .eviction = [&](const EvictionPlan&) {
                ++evictions;
                return ErrorCode::OK;
            },
            .prefetch = [&](const PrefetchPlan&) {
                ++prefetches;
                return ErrorCode::OK;
            },
            .admission = [&](const AdmissionResult&) {
                ++admissions;
                return ErrorCode::OK;
            },
        });

    // observed_at_ns is deliberately left unset so the collector stamps this
    // access with its own clock: the frequency window is a true rolling window,
    // so a synthetic epoch timestamp would be pruned and the admission would be
    // rejected for frequency instead of exercising the handler.
    AccessRecord access{.object = {TenantId("tenant-a"), "runtime-key"},
                        .block_size = 64,
                        .tier = CacheTier::kL2Segment,
                        .is_hit = true};
    // Two accesses: the default admission frequency gate is 2, aligned with the
    // master's promotion_admission_threshold. A single observation would be
    // rejected for frequency instead of exercising the handler.
    runtime.RecordAccess(access.object.key, access);
    runtime.RecordAccess(access.object.key, access);
    runtime.ReportInferenceMetrics(
        InferenceMetrics{.object = access.object, .match_length = 512});

    const auto status = runtime.Execute(CacheTier::kL2Segment, 64,
                                        TraceHistory{}, {access.object});

    EXPECT_EQ(status.eviction, ErrorCode::OK);
    ASSERT_EQ(status.admissions.size(), 1);
    EXPECT_EQ(status.admissions.front(), ErrorCode::OK);
    EXPECT_EQ(evictions, 1);
    EXPECT_EQ(admissions, 1);
    EXPECT_GE(runtime.Snapshot().keys.size(), 1);
}

TEST(IoPatternFrameworkTest, RuntimeExecutesCfmCommandsThroughStorageHandlers) {
    int admissions = 0;
    IoPatternRuntime runtime(
        {.eviction = [](const EvictionPlan&) { return ErrorCode::OK; },
         .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
         .admission = [&admissions](const AdmissionResult&) {
             ++admissions;
             return ErrorCode::OK;
         }});
    // In the embedded architecture the receiving SubMaster executes delivered
    // policy commands through its own runtime; there is no client-side
    // dispatch loop any more.
    EXPECT_EQ(runtime.ExecuteCommand(
                  AdmissionResult{.object = {TenantId("tenant"), "key"},
                                  .decision = AdmissionDecision::kAdmit}),
              ErrorCode::OK);
    EXPECT_EQ(admissions, 1);
}

TEST(IoPatternFrameworkTest, RuntimeSchedulesAdmissionOffTheProducerPath) {
    std::promise<AdmissionResult> handled;
    IoPatternRuntime runtime(
        {.eviction = [](const EvictionPlan&) { return ErrorCode::OK; },
         .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
         .admission = [&handled](const AdmissionResult& result) {
             handled.set_value(result);
             return ErrorCode::OK;
         }});
    AccessRecord access{.object = {TenantId("tenant"), "disk-key"},
                        .tier = CacheTier::kL3NofSsd,
                        .operation = IoOperation::kPut};
    // The default admission frequency gate is 2, so two observations are needed
    // before the admission handler is reached.
    runtime.RecordAccess(access.object.key, access);
    runtime.RecordAccess(access.object.key, access);

    EXPECT_TRUE(runtime.ScheduleAdmission(access.object, CacheTier::kL1Host));
    auto result = handled.get_future();
    ASSERT_EQ(result.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    EXPECT_EQ(result.get().object, access.object);
}

TEST(IoPatternFrameworkTest, OwnershipClientBucketsReportsByResolvedOwner) {
    auto runtime = std::make_shared<IoPatternRuntime>(
        IoPatternRuntime::Handlers{
            .eviction = [](const EvictionPlan&) { return ErrorCode::OK; },
            .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
            .admission = [](const AdmissionResult&) { return ErrorCode::OK; }});
    auto service = std::make_shared<CfmService>(runtime);
    CfmRpcService endpoint(service);
    coro_rpc::coro_rpc_server server(1, 0, "127.0.0.1");
    server.register_handler<&CfmRpcService::Send>(&endpoint);
    ASSERT_FALSE(server.async_start().hasResult());
    const std::string owner_endpoint =
        "127.0.0.1:" + std::to_string(server.port());

    // Only keys starting with "owned/" resolve to the single SubMaster under
    // test; "foreign/" and empty resolver results are dropped.
    CfmOwnershipClient client(
        [&](const TenantId&, const std::string& key)
            -> std::optional<std::string> {
            return key.rfind("owned/", 0) == 0
                       ? std::optional<std::string>(owner_endpoint)
                       : std::nullopt;
        },
        std::chrono::milliseconds(500));

    MetricBatch batch;
    batch.accesses.push_back(
        AccessRecord{.object = {TenantId("tenant"), "owned/key-a"},
                     .block_size = 64,
                     .tier = CacheTier::kL1Host,
                     .is_hit = true});
    batch.accesses.push_back(
        AccessRecord{.object = {TenantId("tenant"), "owned/key-b"},
                     .block_size = 64,
                     .tier = CacheTier::kL1Host,
                     .is_hit = true});
    batch.inference.push_back(InferenceMetrics{
        .object = {TenantId("tenant"), "foreign/key"}, .session_id = "s"});

    EXPECT_EQ(client.ReportMetricBatch(batch), ErrorCode::OK);
    EXPECT_EQ(client.dropped_observations(), 1);

    // Both owned observations were aggregated into one batch for the resolved
    // owner and merged into that SubMaster's local runtime.
    const auto snapshot = service->Snapshot();
    ASSERT_EQ(snapshot.keys.size(), 2);
    EXPECT_EQ(snapshot.keys[0].object.key, "owned/key-a");
    EXPECT_EQ(snapshot.keys[1].object.key, "owned/key-b");
    server.stop();
}

TEST(IoPatternFrameworkTest, UnavailablePrefetchCapabilityDoesNotDegradePolicy) {
    // The prefetch handler's own primitive can be structurally unavailable:
    // promotion may be disabled, or the object may have no LOCAL_DISK source
    // replica to promote from. The policy cannot influence either outcome, so
    // repeating cycles must not be counted as policy failure -- otherwise the
    // whole workload policy is permanently replaced by the legacy fallback.
    int prefetch_calls = 0;
    IoPatternRuntime runtime(IoPatternRuntime::Handlers{
        .eviction = [](const EvictionPlan&) { return ErrorCode::OK; },
        .prefetch =
            [&prefetch_calls](const PrefetchPlan&) {
                ++prefetch_calls;
                return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
            },
        .admission = [](const AdmissionResult&) { return ErrorCode::OK; }});

    AccessRecord access{.object = {TenantId("tenant-a"), "cold-key"},
                        .block_size = 1024,
                        // Local disk is the only tier the store can promote from,
                        // and the only one that yields a prefetch candidate.
                        .tier = CacheTier::kLocalDisk,
                        .operation = IoOperation::kGet,
                        .is_hit = true};
    // A recommendation-shaped key yields a definitive workload classification
    // and a confidence of 1.0, so the prefetch gate is genuinely exercised.
    for (int i = 0; i < 21; ++i) {
        runtime.RecordAccess(access.object.key, access);
    }
    runtime.ReportInferenceMetrics(
        InferenceMetrics{.object = access.object, .match_length = 300});

    TraceHistory trace;
    trace.events.push_back(
        TraceEvent{.object = access.object, .match_length = 300, .is_hit = true});

    // Three consecutive reported failures is the DegradingPolicyEngine
    // threshold used by the runtime. The short pause lets the analyzer thread
    // of the previous cycle clear its in-flight flag, which would otherwise
    // mark a cycle degraded for reasons unrelated to this test.
    for (int cycle = 0; cycle < 3; ++cycle) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        runtime.Execute(CacheTier::kL1Host, 0, trace, {});
    }

    ASSERT_GE(prefetch_calls, 1) << "the prefetch handler must be exercised";
    EXPECT_FALSE(runtime.degraded())
        << "a structurally unavailable prefetch primitive is not a policy "
           "failure";
}

TEST(IoPatternFrameworkTest, TierRemovalEventRetiresTheReplicaBit) {
    // An object evicted from host memory must stop claiming an L1 replica, so
    // it becomes promotable again instead of being treated as already resident
    // in the head tier.
    IoPatternRuntime runtime(
        {.eviction = [](const EvictionPlan&) { return ErrorCode::OK; },
         .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
         .admission = [](const AdmissionResult&) { return ErrorCode::OK; }});
    AccessRecord access{.object = {TenantId("tenant-a"), "tiered-key"},
                        .block_size = 64,
                        .tier = CacheTier::kL1Host,
                        .is_hit = true};
    runtime.RecordAccess(access.object.key, access);
    ASSERT_EQ(runtime.Snapshot().keys.size(), 1U);
    EXPECT_EQ(runtime.Snapshot().keys.front().replica_tiers,
              CacheTierBit(CacheTier::kL1Host));

    runtime.RecordTierEvent(CacheEvent{.type = CacheEventType::kRemoved,
                                       .object = access.object,
                                       .source_tier = CacheTier::kL1Host});

    EXPECT_EQ(runtime.Snapshot().keys.front().replica_tiers,
              static_cast<CacheTierMask>(0));
}

TEST(IoPatternFrameworkTest, TierChangeEventMovesTheReplicaBit) {
    // Demotion keeps the lower-tier claim without keeping the upper one; a
    // fresh insert adds a claim.
    IoPatternCollectorImpl collector;
    AccessRecord access{.object = {TenantId("tenant-a"), "moved-key"},
                        .block_size = 64,
                        .tier = CacheTier::kL1Host,
                        .is_hit = true};
    collector.RecordAccess(access.object.key, access);

    collector.RecordTierEvent(
        CacheEvent{.type = CacheEventType::kTierChanged,
                   .object = access.object,
                   .source_tier = CacheTier::kL1Host,
                   .target_tier = CacheTier::kL3NofSsd});
    EXPECT_EQ(collector.GetSnapshot().keys.front().replica_tiers,
              CacheTierBit(CacheTier::kL3NofSsd));

    collector.RecordTierEvent(CacheEvent{.type = CacheEventType::kInserted,
                                         .object = access.object,
                                         .target_tier = CacheTier::kL2Segment});
    EXPECT_EQ(collector.GetSnapshot().keys.front().replica_tiers,
              static_cast<CacheTierMask>(CacheTierBit(CacheTier::kL3NofSsd) |
                                         CacheTierBit(CacheTier::kL2Segment)));
}

TEST(IoPatternFrameworkTest, LocalDiskCountsAsALowerTierThanHostMemory) {
    // TierDepth, not declaration order, defines the storage ladder: kLocalDisk is
    // declared last so the existing tier values stay stable on the CFI wire.
    EXPECT_LT(TierDepth(CacheTier::kL1Host), TierDepth(CacheTier::kLocalDisk));
    EXPECT_LT(TierDepth(CacheTier::kLocalDisk), TierDepth(CacheTier::kL2Segment));
    EXPECT_LT(TierDepth(CacheTier::kL2Segment), TierDepth(CacheTier::kL3NofSsd));

    // A local-disk copy is what makes reclaiming the host copy safe, which is
    // exactly what the eviction score's lower-replica term rewards.
    ScoreBasedEvictionOps eviction;
    PolicyContext context;
    KeyMetrics key;
    key.object = {TenantId("tenant-a"), "offloaded"};
    key.block_size = 64;
    key.replica_tiers = CacheTierBit(CacheTier::kL1Host) |
                        CacheTierBit(CacheTier::kLocalDisk);
    context.snapshot.keys.push_back(key);
    context.analysis.keys = {KeyPattern{.object = key.object, .idle_score = 1.0F}};

    const auto plan = eviction.Evaluate(context, CacheTier::kL1Host, 64);
    ASSERT_EQ(plan.candidates.size(), 1);
    // idle_weight(1.0) * idle_score(1.0) + lower_replica_weight(1.0) * 1.0.
    EXPECT_FLOAT_EQ(plan.candidates.front().score, 2.0F);
}

TEST(IoPatternFrameworkTest, ScoreEvictionHonoursTheColdIdleGate) {
    // The cold-eviction driver gates victims by idle time. Applying that gate
    // where candidates are selected -- rather than pre-summing idle bytes into
    // the byte target -- keeps the budget and the victim set describing the same
    // keys, so a pass cannot reclaim objects the driver never considered cold.
    PolicyContext context;
    context.min_idle_time_us = 1'000'000;  // 1 s
    KeyMetrics fresh;
    fresh.object = {TenantId("tenant-a"), "fresh"};
    fresh.idle_time_us = 500'000;  // below the gate
    fresh.block_size = 64;
    fresh.replica_tiers = CacheTierBit(CacheTier::kL1Host);
    KeyMetrics cold;
    cold.object = {TenantId("tenant-a"), "cold"};
    cold.idle_time_us = 5'000'000;  // above the gate
    cold.block_size = 64;
    cold.replica_tiers = CacheTierBit(CacheTier::kL1Host);
    context.snapshot.keys = {fresh, cold};
    context.analysis.keys = {
        KeyPattern{.object = fresh.object, .idle_score = 1.0F},
        KeyPattern{.object = cold.object, .idle_score = 0.1F}};

    ScoreBasedEvictionOps eviction;
    const auto plan = eviction.Evaluate(context, CacheTier::kL1Host, 64);

    ASSERT_EQ(plan.candidates.size(), 1U);
    // Only the genuinely idle key is eligible, even though "fresh" scores higher.
    EXPECT_EQ(plan.candidates.front().object.key, "cold");
}

TEST(IoPatternFrameworkTest, AdmissionWatermarkFollowsTheConfiguredHighWatermark) {
    // Admission must stop before eviction starts, so the watermark is derived
    // from the store's eviction high watermark instead of a fixed 0.90: a store
    // configured to evict at 0.75 also refuses admission at 0.75 rather than
    // continuing to admit through the 0.75..0.90 window.
    WorkloadPolicyEngine engine(WorkloadType::kMixed, 3, 0.75F);
    PolicyContext context;
    KeyMetrics key;
    key.object = {TenantId("tenant-a"), "hot"};
    key.access_count_window = 32;
    context.snapshot.keys.push_back(key);
    context.snapshot.storage = {StorageMetric{.source_id = "host",
                                              .tier = CacheTier::kL1Host,
                                              .memory_used_ratio = 0.80F}};

    EXPECT_EQ(engine.DecideAdmission(key.object, CacheTier::kL1Host, context)
                  .decision,
              AdmissionDecision::kRejectWatermark);
}

TEST(IoPatternFrameworkTest, RuntimeMetricsExport) {
    auto& metrics = MasterMetricManager::instance();
    IoPatternRuntime::Config config;
    config.collector.max_total_keys = 1;
    auto runtime = std::make_shared<IoPatternRuntime>(
        IoPatternRuntime::Handlers{}, config);
    metrics.set_io_pattern_runtime(runtime);

    const auto value = [](const std::string& text, const std::string& name) {
        const auto offset = text.find("\n" + name + " ");
        EXPECT_NE(offset, std::string::npos) << name;
        return offset == std::string::npos
                   ? -1.0
                   : std::stod(text.substr(offset + name.size() + 2));
    };
    const auto initial = metrics.serialize_metrics();
    for (const auto* name :
         {"policy_decisions_total", "feedback_samples", "collect_latency_us",
          "analyze_latency_us", "strategy_hit_rate", "false_positive_rate",
          "degrade_count", "report_drop_count"}) {
        EXPECT_DOUBLE_EQ(
            value(initial, std::string("master_io_pattern_") + name), 0.0);
    }
    runtime->RecordFeedback({.hit_rate_delta = -0.25F,
                             .eviction_churn = 0.5F,
                             .ttft_delta = -0.125F,
                             .prefetch_accuracy = 0.75F});
    runtime->Plan(CacheTier::kL1Host, 0, {});
    const auto first = metrics.serialize_metrics();
    EXPECT_DOUBLE_EQ(value(first, "master_io_pattern_hit_rate_delta"), -0.25);
    EXPECT_DOUBLE_EQ(value(first, "master_io_pattern_eviction_churn"), 0.5);
    EXPECT_DOUBLE_EQ(value(first, "master_io_pattern_ttft_delta"), -0.125);
    EXPECT_DOUBLE_EQ(value(first, "master_io_pattern_prefetch_accuracy"), 0.75);
    EXPECT_DOUBLE_EQ(value(first, "master_io_pattern_feedback_samples"), 1.0);
    EXPECT_DOUBLE_EQ(value(first, "master_io_pattern_policy_decisions_total"),
                     1.0);
    EXPECT_GT(value(first, "master_io_pattern_policy_decision_qps"), 0.0);
    // Both the HTTP summary and periodic Master Admin Metrics log must expose
    // the same runtime and feedback values without consuming the counters.
    for (const auto& summary :
         {metrics.get_summary_string(),
          metrics.get_summary_string_and_update_snapshot()}) {
        EXPECT_NE(summary.find("IO Pattern (runtime, lifetime):"),
                  std::string::npos);
        for (const auto* field :
             {"collect_latency_max_us=", "analyze_latency_max_us=",
              "policy_decision_qps=", "policy_decisions=1",
              "strategy_hit_rate=0", "false_positive_rate=0", "degrade_count=0",
              "report_drop_count=0", "hit_rate_delta=-0.25",
              "eviction_churn=0.5", "ttft_delta=-0.125",
              "prefetch_accuracy=0.75", "feedback_samples=1"}) {
            EXPECT_NE(summary.find(field), std::string::npos) << field;
        }
    }
    // Scrapes must not increment cumulative counters or consume feedback.
    const auto second = metrics.serialize_metrics();
    EXPECT_DOUBLE_EQ(value(second, "master_io_pattern_policy_decisions_total"),
                     1.0);
    EXPECT_DOUBLE_EQ(value(second, "master_io_pattern_feedback_samples"), 1.0);
    EXPECT_NE(
        second.find("# TYPE master_io_pattern_policy_decisions_total counter"),
        std::string::npos);
    EXPECT_NE(second.find("# TYPE master_io_pattern_hit_rate_delta gauge"),
              std::string::npos);

    runtime->RecordAccess(
        "one", {.object = {TenantId::Default(), "one"}, .is_hit = true});
    runtime->RecordAccess(
        "two", {.object = {TenantId::Default(), "two"}, .is_hit = true});
    runtime->Execute(CacheTier::kL1Host, 0, {});
    const auto degraded = metrics.serialize_metrics();
    EXPECT_DOUBLE_EQ(value(degraded, "master_io_pattern_report_drop_count"),
                     1.0);
    EXPECT_GE(value(degraded, "master_io_pattern_degrade_count"), 1.0);
    EXPECT_NE(metrics.get_summary_string().find("report_drop_count=1"),
              std::string::npos);

    // The singleton must not retain a runtime (or its MasterService handlers).
    std::weak_ptr<IoPatternRuntime> weak = runtime;
    runtime.reset();
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(
        metrics.get_summary_string().find("IO Pattern (runtime, lifetime):"),
        std::string::npos);
    EXPECT_EQ(metrics.serialize_metrics().find(
                  "# TYPE master_io_pattern_hit_rate_delta "),
              std::string::npos);

    auto replacement =
        std::make_shared<IoPatternRuntime>(IoPatternRuntime::Handlers{});
    metrics.set_io_pattern_runtime(replacement);
    metrics.clear_io_pattern_runtime(nullptr);
    EXPECT_DOUBLE_EQ(value(metrics.serialize_metrics(),
                           "master_io_pattern_policy_decisions_total"),
                     0.0);
    metrics.clear_io_pattern_runtime(replacement.get());
    EXPECT_EQ(metrics.get_summary_string_and_update_snapshot().find(
                  "IO Pattern (feedback, sample window):"),
              std::string::npos);
    EXPECT_EQ(metrics.serialize_metrics().find(
                  "# TYPE master_io_pattern_hit_rate_delta "),
              std::string::npos);
}

}  // namespace
}  // namespace mooncake::io_pattern
