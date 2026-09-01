#include "io_pattern/io_pattern.h"

#include <memory>
#include <type_traits>

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

TEST(IoPatternFrameworkTest, PublicSeamsRemainAbstract) {
    static_assert(std::is_abstract_v<IoPatternCollector>);
    static_assert(std::is_abstract_v<IoPatternAnalyzer>);
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

}  // namespace
}  // namespace mooncake::io_pattern
