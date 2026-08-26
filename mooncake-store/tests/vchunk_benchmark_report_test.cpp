#include "vchunk_benchmark_report.h"

#include <gtest/gtest.h>

#include <vector>

namespace mooncake::benchmarks {
namespace {

VChunkBenchmarkResult Result(std::string path, double throughput,
                             double p99) {
    VChunkBenchmarkResult result;
    result.path = std::move(path);
    result.object_size = 1024U * 1024U;
    result.segment_count = 4;
    result.concurrency = 32;
    result.operations = 100;
    result.successes = 100;
    result.throughput_mib_per_second = throughput;
    result.p99_us = p99;
    return result;
}

TEST(VChunkBenchmarkReportTest, ComputesNearestRankPercentiles) {
    const std::vector<double> values{50, 10, 40, 20, 30};
    EXPECT_EQ(Percentile(values, 0.50), 30);
    EXPECT_EQ(Percentile(values, 0.99), 50);
    EXPECT_EQ(Percentile({}, 0.50), 0);
}

TEST(VChunkBenchmarkReportTest, RecommendsFullDesignWhenGainIsRepeatable) {
    std::vector<VChunkBenchmarkResult> results{
        Result("legacy", 100, 1000), Result("vchunk", 120, 1050)};
    const auto decision = AnalyzeVChunkBenchmark(results, 1.10, 1.10, true);
    EXPECT_TRUE(decision.correctness_passed);
    EXPECT_TRUE(decision.cleanup_passed);
    EXPECT_TRUE(decision.performance_gain_found);
    EXPECT_TRUE(decision.recommend_full_design);
    EXPECT_EQ(decision.priority, "metadata_partitioning");
}

TEST(VChunkBenchmarkReportTest, CleanupLeakBlocksRecommendation) {
    auto vchunk = Result("vchunk", 130, 900);
    vchunk.allocator_bytes_after_cleanup = 4096;
    std::vector<VChunkBenchmarkResult> results{Result("legacy", 100, 1000),
                                               vchunk};
    const auto decision = AnalyzeVChunkBenchmark(results, 1.10, 1.10, true);
    EXPECT_FALSE(decision.cleanup_passed);
    EXPECT_FALSE(decision.recommend_full_design);
    EXPECT_EQ(decision.priority, "resource_lifecycle");
}

TEST(VChunkBenchmarkReportTest, DataMismatchBlocksRecommendation) {
    auto vchunk = Result("vchunk", 130, 900);
    vchunk.data_mismatches = 1;
    const auto decision = AnalyzeVChunkBenchmark(
        {Result("legacy", 100, 1000), vchunk}, 1.10, 1.10, true);
    EXPECT_FALSE(decision.correctness_passed);
    EXPECT_EQ(decision.priority, "correctness_and_failure_cleanup");
}

TEST(VChunkBenchmarkReportTest, EmitsMachineReadableConfigAndDecision) {
    VChunkBenchmarkConfig config;
    config.object_sizes = {4096};
    config.segment_counts = {2};
    config.concurrencies = {8};
    config.operations_per_scenario = 10;
    auto legacy = Result("legacy", 100, 1000);
    const auto decision =
        AnalyzeVChunkBenchmark({legacy}, 1.10, 1.10, false);
    const auto report = BuildVChunkBenchmarkReport(config, {legacy}, decision);
    EXPECT_EQ(report["schema_version"].asInt(), 1);
    EXPECT_EQ(report["config"]["object_sizes"][0].asUInt64(), 4096U);
    EXPECT_EQ(report["results"][0]["path"].asString(), "legacy");
    EXPECT_TRUE(report["decision"].isObject());
    EXPECT_TRUE(report["decision"]["known_limitations"].isArray());
}

TEST(VChunkBenchmarkReportTest, SyntheticDataPlaneCannotRecommendProduction) {
    const auto decision = AnalyzeVChunkBenchmark(
        {Result("legacy", 100, 1000), Result("vchunk", 130, 900)}, 1.10,
        1.10, false);
    EXPECT_TRUE(decision.performance_gain_found);
    EXPECT_FALSE(decision.recommend_full_design);
    EXPECT_EQ(decision.priority, "production_data_plane_validation");
}

}  // namespace
}  // namespace mooncake::benchmarks
