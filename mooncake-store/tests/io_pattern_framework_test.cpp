#include "io_pattern/io_pattern.h"
#include "io_pattern/threshold_analyzer.h"
#include "io_pattern/policy_strategies.h"

#include <memory>
#include <chrono>
#include <future>
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
    CfmPollResult PollPolicyResult() override {
        return policy ? CfmPollResult::Command(*policy, 42)
                      : CfmPollResult::Empty();
    }
    bool AcknowledgePolicy(uint64_t delivery_id, bool success) override {
        acknowledged_delivery_id = delivery_id;
        acknowledged_success = success;
        return acknowledge_ok;
    }
    ErrorCode ExecutePrefetch(const PrefetchPlan& value) override {
        plan = value;
        return execute_code;
    }
    bool send_ok{true};
    bool acknowledge_ok{true};
    bool acknowledged_success{false};
    uint64_t acknowledged_delivery_id{0};
    ErrorCode execute_code{ErrorCode::OK};
    IoPatternSnapshot snapshot;
    std::optional<PolicyCommand> policy;
    PrefetchPlan plan;
};

class FlakyCfmChannel final : public CfmChannel {
   public:
    bool SendSnapshot(const IoPatternSnapshot&) override {
        return send_failures-- <= 0;
    }
    CfmPollResult PollPolicyResult() override {
        return CfmPollResult::Command(PrefetchPlan{});
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
    CfmReceiveResult Receive(std::string_view method,
                             std::chrono::milliseconds timeout) override {
        last_method = std::string(method);
        last_timeout = timeout;
        return response ? CfmReceiveResult::Payload(*response)
                        : CfmReceiveResult::Empty();
    }
    bool Acknowledge(uint64_t delivery_id, bool success,
                     std::chrono::milliseconds timeout) override {
        acknowledged_delivery_id = delivery_id;
        acknowledged_success = success;
        last_timeout = timeout;
        return acknowledge_ok;
    }
    bool send_ok{true};
    bool acknowledge_ok{true};
    bool acknowledged_success{false};
    uint64_t acknowledged_delivery_id{0};
    std::optional<std::string> response;
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
    key.replica_tiers = CacheTierBit(CacheTier::kL3NofSsd);
    context.snapshot.keys.push_back(key);

    TraceHistory trace;
    trace.events.push_back(
        TraceEvent{.object = key.object, .match_length = 512, .is_hit = true});
    trace.events.push_back(
        TraceEvent{.object = key.object, .match_length = 8, .is_hit = true});

    const auto plan = prefetch.Evaluate(context, trace);
    ASSERT_EQ(plan.candidates.size(), 1);
    EXPECT_EQ(plan.candidates.front().source_tier, CacheTier::kL3NofSsd);
    EXPECT_EQ(plan.candidates.front().target_tier, CacheTier::kL2Segment);
    EXPECT_EQ(plan.candidates.front().bytes, 4096);
}

TEST(IoPatternFrameworkTest, TracePrefetchDeduplicatesObjects) {
    TraceBasedPrefetchOps prefetch;
    PolicyContext context;
    KeyMetrics key;
    key.object = {TenantId("tenant-a"), "block"};
    key.block_size = 128;
    key.replica_tiers = CacheTierBit(CacheTier::kL2Segment);
    context.snapshot.keys.push_back(key);

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
        context, CacheTier::kL1Host, 1024, {}, {key.object});
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
    const auto result = engine.ExecutePolicy({}, CacheTier::kL1Host, 1024, {},
                                             {object});
    ASSERT_EQ(result.admissions.size(), 1);
    EXPECT_EQ(result.admissions.front().object, object);

    RegistryPolicyEngine missing(registries, "missing", "trace", "prefix");
    EXPECT_TRUE(missing.ExecutePolicy({}, CacheTier::kL1Host, 0, {}).degraded);
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
    channel->policy = PrefetchPlan{};
    EXPECT_TRUE(client.PollPolicy().has_value());
    EXPECT_EQ(client.ExecutePrefetch(PrefetchPlan{}), ErrorCode::OK);
    channel->send_ok = false;
    EXPECT_EQ(client.ReportSnapshot(snapshot), ErrorCode::RPC_FAIL);
    CfmClientImpl unavailable(nullptr);
    EXPECT_EQ(unavailable.ReportSnapshot(snapshot),
              ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

TEST(IoPatternFrameworkTest, CfmClientDispatchesReceivedPolicyCommands) {
    auto channel = std::make_shared<TestCfmChannel>();
    int dispatched = 0;
    CfmClientImpl client(channel, [&](const PolicyCommand& command) {
        EXPECT_TRUE(std::holds_alternative<PrefetchPlan>(command));
        ++dispatched;
        return ErrorCode::OK;
    });

    EXPECT_EQ(client.ReceivePolicy(PolicyCommand{PrefetchPlan{}}), ErrorCode::OK);
    EXPECT_EQ(dispatched, 1);
    channel->policy = PolicyCommand{AdmissionResult{}};
    EXPECT_EQ(client.PollAndDispatchPolicy(), ErrorCode::OK);
    EXPECT_EQ(dispatched, 2);
    EXPECT_EQ(channel->acknowledged_delivery_id, 42);
    EXPECT_TRUE(channel->acknowledged_success);
    channel->policy.reset();
    EXPECT_EQ(client.PollAndDispatchPolicy(), ErrorCode::OK);
    EXPECT_EQ(dispatched, 2);
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

TEST(IoPatternFrameworkTest, EmptyPolicyPollKeepsChannelHealthy) {
    auto idle = std::make_shared<TestCfmChannel>();
    ResilientCfmChannel channel(
        idle, CfmRetryConfig{.max_retries = 2, .degrade_after_failures = 2});

    EXPECT_FALSE(channel.PollPolicy().has_value());
    EXPECT_FALSE(channel.PollPolicy().has_value());
    EXPECT_EQ(channel.consecutive_failures(), 0);
    EXPECT_FALSE(channel.degraded());
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
    transport->response = "policy";
    EXPECT_TRUE(channel.PollPolicy().has_value());
    EXPECT_EQ(channel.ExecutePrefetch({}), ErrorCode::OK);
    auto rpc_channel = std::make_shared<CfmRpcChannel>(transport, codec);
    IoPatternReporter reporter(2, MakeCfmMetricBatchSink(rpc_channel));
    reporter.Enqueue(InferenceMetrics{});
    EXPECT_TRUE(reporter.Flush());
    EXPECT_EQ(transport->last_method, "report_metric_batch");
    EXPECT_EQ(transport->last_payload, "batch");
    transport->send_ok = false;
    EXPECT_EQ(channel.ExecutePrefetch({}), ErrorCode::RPC_TIMEOUT);
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

TEST(IoPatternFrameworkTest, InProcessCfmTransportAuthenticatesAndDispatches) {
    CfmBinaryCodec codec;
    bool received_snapshot = false;
    auto transport = std::make_shared<InProcessCfmRpcTransport>(
        "shared-secret", [&received_snapshot](std::string_view method,
                                                 std::string_view) {
            received_snapshot = method == "report_snapshot";
            return received_snapshot;
        });
    CfmRpcChannel authorized(transport, std::make_shared<CfmBinaryCodec>(),
                             {.auth_token = "shared-secret"});
    EXPECT_TRUE(authorized.SendSnapshot({}));
    EXPECT_TRUE(received_snapshot);

    transport->EnqueuePolicy(codec.EncodePolicy(
        AdmissionResult{.object = {TenantId("tenant"), "key"},
                        .decision = AdmissionDecision::kAdmit}));
    ASSERT_TRUE(authorized.PollPolicy().has_value());

    auto rejected = std::make_shared<InProcessCfmRpcTransport>("secret");
    CfmRpcChannel unauthorized(rejected, std::make_shared<CfmBinaryCodec>(),
                               {.auth_token = "wrong"});
    EXPECT_FALSE(unauthorized.SendSnapshot({}));
}

TEST(IoPatternFrameworkTest, InProcessTransportDoesNotHoldLockAcrossHandler) {
    std::promise<void> handler_entered;
    std::promise<void> release_handler;
    auto release = release_handler.get_future().share();
    auto transport = std::make_shared<InProcessCfmRpcTransport>(
        "shared-secret", [&](std::string_view, std::string_view) {
            handler_entered.set_value();
            release.wait();
            return true;
        });
    ASSERT_TRUE(transport->Authenticate("shared-secret"));

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
    auto enqueue = std::async(std::launch::async, [&] {
        transport->EnqueuePolicy("policy");
        return true;
    });
    EXPECT_EQ(enqueue.wait_for(std::chrono::milliseconds(100)),
              std::future_status::ready);
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

TEST(IoPatternFrameworkTest, CfmServiceAuthenticatesAndBoundsPolicyQueues) {
    int admissions = 0;
    auto runtime = std::make_shared<IoPatternRuntime>(
        IoPatternRuntime::Handlers{
            .eviction = [](const EvictionPlan&) { return ErrorCode::OK; },
            .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
            .admission = [&admissions](const AdmissionResult&) {
                ++admissions;
                return ErrorCode::OK;
            }});
    CfmService service(runtime, "secret", 1, "producer");
    CfmBinaryCodec codec;

    EXPECT_FALSE(service.Authenticate("wrong"));
    EXPECT_TRUE(service.Authenticate("secret"));
    EXPECT_FALSE(service.EnqueuePolicy("node-a", "first", "secret"));
    const auto admission = codec.EncodePolicy(AdmissionResult{
        .object = {TenantId("tenant"), "key"},
        .target_tier = CacheTier::kL1Host,
        .decision = AdmissionDecision::kAdmit});
    const auto second = codec.EncodePolicy(PrefetchPlan{});
    EXPECT_FALSE(service.EnqueuePolicy("node-a", "malformed", "producer"));
    EXPECT_TRUE(service.EnqueuePolicy("node-a", admission, "producer"));
    EXPECT_FALSE(service.EnqueuePolicy("node-a", second, "producer"));
    const auto delivery = service.PollPolicy("node-a", "secret");
    ASSERT_TRUE(delivery.has_value());
    EXPECT_EQ(delivery->second, admission);
    EXPECT_TRUE(service.AcknowledgePolicy("node-a", delivery->first, false,
                                         "secret"));
    EXPECT_TRUE(service.PollPolicy("node-a", "secret").has_value());
    EXPECT_TRUE(service.AcknowledgePolicy("node-a", delivery->first, true,
                                         "secret"));
    EXPECT_FALSE(service.PollPolicy("node-a", "secret").has_value());

    EXPECT_FALSE(service.Send("", "execute_policy", admission, "secret"));
    EXPECT_FALSE(
        service.Send("node-a", "execute_policy", admission, "secret"));
    EXPECT_TRUE(
        service.Send("node-a", "execute_policy", admission, "producer"));
    EXPECT_EQ(admissions, 1);
}

TEST(IoPatternFrameworkTest, CfmAggregatesMetricsBySubmasterNode) {
    auto runtime = std::make_shared<IoPatternRuntime>(
        IoPatternRuntime::Handlers{
            .eviction = [](const EvictionPlan&) { return ErrorCode::OK; },
            .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
            .admission = [](const AdmissionResult&) { return ErrorCode::OK; }});
    CfmService service(runtime, "secret", 8, "producer");
    CfmBinaryCodec codec;

    MetricBatch submaster_a;
    submaster_a.accesses.push_back(
        AccessRecord{.object = {TenantId("tenant"), "key-a"},
                     .block_size = 64,
                     .tier = CacheTier::kL1Host,
                     .is_hit = true});
    MetricBatch submaster_b;
    submaster_b.accesses.push_back(
        AccessRecord{.object = {TenantId("tenant"), "key-b"},
                     .block_size = 128,
                     .tier = CacheTier::kL1Host,
                     .is_hit = true});

    ASSERT_TRUE(service.Send("submaster-a", "report_metric_batch",
                             codec.EncodeMetricBatch(submaster_a), "secret"));
    ASSERT_TRUE(service.Send("submaster-b", "report_metric_batch",
                             codec.EncodeMetricBatch(submaster_b), "secret"));

    const auto snapshot_a = service.SnapshotForNode("submaster-a");
    const auto snapshot_b = service.SnapshotForNode("submaster-b");
    ASSERT_EQ(snapshot_a.keys.size(), 1);
    ASSERT_EQ(snapshot_b.keys.size(), 1);
    EXPECT_EQ(snapshot_a.keys.front().object.key, "key-a");
    EXPECT_EQ(snapshot_b.keys.front().object.key, "key-b");
}

TEST(IoPatternFrameworkTest, CoroRpcCfmTransportRunsTheProductionWirePath) {
    auto runtime = std::make_shared<IoPatternRuntime>(
        IoPatternRuntime::Handlers{
            .eviction = [](const EvictionPlan&) { return ErrorCode::OK; },
            .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
            .admission = [](const AdmissionResult&) { return ErrorCode::OK; }});
    auto service =
        std::make_shared<CfmService>(runtime, "secret", 2, "producer");
    CfmRpcService endpoint(service);
    coro_rpc::coro_rpc_server server(1, 0, "127.0.0.1");
    server.register_handler<&CfmRpcService::Authenticate>(&endpoint);
    server.register_handler<&CfmRpcService::Send>(&endpoint);
    server.register_handler<&CfmRpcService::Receive>(&endpoint);
    server.register_handler<&CfmRpcService::Acknowledge>(&endpoint);
    server.register_handler<&CfmRpcService::EnqueuePolicy>(&endpoint);
    ASSERT_FALSE(server.async_start().hasResult());

    const auto rejected_poll =
        endpoint.Receive("poll_policy", "node-a", "wrong");
    EXPECT_FALSE(rejected_poll.first);
    const auto empty_poll = endpoint.Receive("poll_policy", "node-a", "secret");
    EXPECT_TRUE(empty_poll.first);
    EXPECT_FALSE(empty_poll.second.has_value());

    CoroRpcCfmTransport transport(
        "127.0.0.1:" + std::to_string(server.port()), "node-a",
        std::chrono::milliseconds(500));
    EXPECT_FALSE(transport.Authenticate("wrong"));
    ASSERT_TRUE(transport.Authenticate("secret"));

    CfmBinaryCodec codec;
    MetricBatch batch;
    batch.accesses.push_back(
        AccessRecord{.object = {TenantId("tenant"), "remote-key"},
                     .is_hit = true});
    batch.storage.push_back(StorageMetric{.source_id = "spoofed",
                                          .tier = CacheTier::kL1Host,
                                          .memory_used_ratio = 0.75F});
    EXPECT_TRUE(transport.Send("report_metric_batch",
                               codec.EncodeMetricBatch(batch),
                               std::chrono::milliseconds(500)));
    const auto snapshot = service->SnapshotForNode("node-a");
    ASSERT_EQ(snapshot.keys.size(), 1);
    ASSERT_EQ(snapshot.storage.size(), 1);
    EXPECT_EQ(snapshot.keys.front().object.key, "remote-key");
    EXPECT_EQ(snapshot.storage.front().source_id, "node-a");

    const auto policy = codec.EncodePolicy(PrefetchPlan{});
    CoroRpcCfmTransport producer(
        "127.0.0.1:" + std::to_string(server.port()), "producer",
        std::chrono::milliseconds(500));
    ASSERT_TRUE(producer.Authenticate("producer"));
    EXPECT_TRUE(producer.EnqueuePolicy("node-a", policy,
                                       std::chrono::milliseconds(500)));
    const auto received =
        transport.Receive("poll_policy", std::chrono::milliseconds(500));
    EXPECT_EQ(received.status, CfmReceiveResult::Status::kPayload);
    EXPECT_EQ(received.payload, policy);
    EXPECT_NE(received.delivery_id, 0);
    EXPECT_TRUE(transport.Acknowledge(received.delivery_id, false,
                                      std::chrono::milliseconds(500)));
    const auto redelivered =
        transport.Receive("poll_policy", std::chrono::milliseconds(500));
    EXPECT_EQ(redelivered.delivery_id, received.delivery_id);
    EXPECT_EQ(redelivered.payload, policy);
    EXPECT_TRUE(transport.Acknowledge(redelivered.delivery_id, true,
                                      std::chrono::milliseconds(500)));
    EXPECT_EQ(transport.Receive("poll_policy", std::chrono::milliseconds(500))
                  .status,
              CfmReceiveResult::Status::kEmpty);
    server.stop();
}

TEST(IoPatternFrameworkTest, CfmProducesNodePolicyFromHighWatermarkReport) {
    auto runtime = std::make_shared<IoPatternRuntime>(
        IoPatternRuntime::Handlers{
            .eviction = [](const EvictionPlan&) { return ErrorCode::OK; },
            .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
            .admission = [](const AdmissionResult&) { return ErrorCode::OK; }});
    CfmService service(runtime, "node-secret", 8, "producer-secret");
    CfmBinaryCodec codec;
    MetricBatch batch;
    batch.accesses.push_back(
        AccessRecord{.object = {TenantId("tenant"), "cold-key"},
                     .block_size = 1024,
                     .tier = CacheTier::kL1Host,
                     .is_hit = false});
    batch.storage.push_back(StorageMetric{.tier = CacheTier::kL1Host,
                                          .used_bytes = 950,
                                          .capacity_bytes = 1000,
                                          .memory_used_ratio = 0.95F});
    ASSERT_TRUE(service.Send("node-a", "report_metric_batch",
                             codec.EncodeMetricBatch(batch), "node-secret"));

    std::optional<std::pair<uint64_t, std::string>> delivery;
    for (size_t attempt = 0; attempt < 100 && !delivery; ++attempt) {
        delivery = service.PollPolicy("node-a", "node-secret");
        if (!delivery) std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(delivery.has_value());
    const auto command = codec.DecodePolicy(delivery->second);
    ASSERT_TRUE(command.has_value());
    const auto* eviction = std::get_if<EvictionPlan>(&*command);
    ASSERT_NE(eviction, nullptr);
    ASSERT_EQ(eviction->candidates.size(), 1);
    EXPECT_EQ(eviction->candidates.front().object.key, "cold-key");
    EXPECT_TRUE(service.AcknowledgePolicy("node-a", delivery->first, true,
                                         "node-secret"));
}

TEST(IoPatternFrameworkTest, CfmPolicyDoesNotCrossSubmasterKeySets) {
    auto runtime = std::make_shared<IoPatternRuntime>(
        IoPatternRuntime::Handlers{
            .eviction = [](const EvictionPlan&) { return ErrorCode::OK; },
            .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
            .admission = [](const AdmissionResult&) { return ErrorCode::OK; }});
    CfmService service(runtime, "node-secret", 8, "producer-secret");
    CfmBinaryCodec codec;

    MetricBatch submaster_b;
    submaster_b.accesses.push_back(
        AccessRecord{.object = {TenantId("tenant"), "key-b"},
                     .block_size = 4096,
                     .tier = CacheTier::kL1Host,
                     .is_hit = false});
    ASSERT_TRUE(service.Send("submaster-b", "report_metric_batch",
                             codec.EncodeMetricBatch(submaster_b),
                             "node-secret"));

    MetricBatch submaster_a;
    submaster_a.accesses.push_back(
        AccessRecord{.object = {TenantId("tenant"), "key-a"},
                     .block_size = 1024,
                     .tier = CacheTier::kL1Host,
                     .is_hit = false});
    submaster_a.storage.push_back(
        StorageMetric{.tier = CacheTier::kL1Host,
                      .used_bytes = 950,
                      .capacity_bytes = 1000,
                      .memory_used_ratio = 0.95F});
    ASSERT_TRUE(service.Send("submaster-a", "report_metric_batch",
                             codec.EncodeMetricBatch(submaster_a),
                             "node-secret"));

    std::optional<std::pair<uint64_t, std::string>> delivery;
    for (size_t attempt = 0; attempt < 100 && !delivery; ++attempt) {
        delivery = service.PollPolicy("submaster-a", "node-secret");
        if (!delivery) std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(delivery.has_value());
    const auto command = codec.DecodePolicy(delivery->second);
    ASSERT_TRUE(command.has_value());
    const auto* eviction = std::get_if<EvictionPlan>(&*command);
    ASSERT_NE(eviction, nullptr);
    ASSERT_FALSE(eviction->candidates.empty());
    for (const auto& candidate : eviction->candidates) {
        EXPECT_EQ(candidate.object.key, "key-a");
    }
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
    EXPECT_FLOAT_EQ(stats.hit_rate_delta, (-0.2F - 0.4F) / 2.0F);
    EXPECT_FLOAT_EQ(stats.prefetch_accuracy, (0.9F + 0.3F) / 2.0F);
}

TEST(IoPatternFrameworkTest, AdaptiveTunerChangesWeightsAfterNegativeStreak) {
    AdaptivePolicyTuner tuner(3);
    ScoreBasedEvictionConfig config;
    EXPECT_FALSE(tuner.Tune({.hit_rate_delta = -0.1F}, config));
    EXPECT_FALSE(tuner.Tune({.hit_rate_delta = -0.1F}, config));
    EXPECT_TRUE(tuner.Tune({.hit_rate_delta = -0.1F}, config));
    EXPECT_FLOAT_EQ(config.frequency_weight, 0.8F);
    EXPECT_FLOAT_EQ(config.idle_weight, 1.1F);
    EXPECT_FALSE(tuner.Tune({.hit_rate_delta = 0.0F}, config));
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
                                    .token_count = 20 * 1024,
                                    .prefix_fanout = 20,
                                    .match_length = 512,
                                    .block_size = 100,
                                    .access_count_window = 1});
    IoPatternSnapshot second;
    second.generated_at_ns = 50;
    second.keys.push_back(KeyMetrics{.object = {TenantId("tenant"), "second"},
                                     .token_count = 30,
                                     .prefix_fanout = 20,
                                     .match_length = 300,
                                     .block_size = 300,
                                     .access_count_window = 5});
    EXPECT_EQ(analyzer.DetectWorkloadType(second), WorkloadType::kMixed);
    const auto stats = analyzer.FeatureStats();
    EXPECT_EQ(stats.samples, 2);
    EXPECT_EQ(stats.token_median, 30);
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
                                           .token_count =
                                               static_cast<uint32_t>(timestamp),
                                           .access_count_window = timestamp});
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
                   .block_size = 64 * 1024,
                   .access_count_window = 30},
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

    AccessRecord access{.object = {TenantId("tenant-a"), "runtime-key"},
                        .observed_at_ns = 1,
                        .block_size = 64,
                        .tier = CacheTier::kL2Segment,
                        .is_hit = true};
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
    CfmClientImpl client(
        std::make_shared<TestCfmChannel>(),
        [&runtime](const PolicyCommand& command) {
            return runtime.ExecuteCommand(command);
        });
    EXPECT_EQ(client.ReceivePolicy(
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
    runtime.RecordAccess(access.object.key, access);

    EXPECT_TRUE(runtime.ScheduleAdmission(access.object, CacheTier::kL1Host));
    auto result = handled.get_future();
    ASSERT_EQ(result.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    EXPECT_EQ(result.get().object, access.object);
}

}  // namespace
}  // namespace mooncake::io_pattern
