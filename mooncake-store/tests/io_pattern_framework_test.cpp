#include "io_pattern/io_pattern.h"

#include <memory>
#include <type_traits>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

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
    void RecordAccess(const AccessRecord& record) override {
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

TEST(IoPatternFrameworkTest, PublicSeamsRemainAbstract) {
    static_assert(std::is_abstract_v<IoPatternCollector>);
    static_assert(std::is_abstract_v<IoPatternAnalyzer>);
    static_assert(std::is_abstract_v<CfmClient>);
    static_assert(std::is_abstract_v<CacheViewManager>);
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
    collector.RecordAccess(access_record);
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

}  // namespace
}  // namespace mooncake::io_pattern
