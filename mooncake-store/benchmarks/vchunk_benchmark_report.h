#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include <json/json.h>

namespace mooncake::benchmarks {

struct VChunkBenchmarkConfig {
    std::vector<size_t> object_sizes;
    std::vector<size_t> segment_counts;
    std::vector<size_t> concurrencies;
    size_t operations_per_scenario{0};
    uint64_t seed{0};
    uint32_t failure_every_n{0};
    uint32_t transfer_delay_us{0};
};

struct VChunkBenchmarkResult {
    std::string path;
    size_t object_size{0};
    size_t segment_count{0};
    size_t concurrency{0};
    size_t operations{0};
    size_t successes{0};
    size_t errors{0};
    size_t injected_failures{0};
    double duration_seconds{0};
    double qps{0};
    double throughput_mib_per_second{0};
    double p50_us{0};
    double p99_us{0};
    double cpu_ms{0};
    uint64_t transfer_requests{0};
    uint64_t transfer_time_us{0};
    uint64_t control_time_us{0};
    uint64_t metadata_bytes{0};
    double metadata_amplification{0};
    uint64_t rollbacks{0};
    uint64_t retries{0};
    uint64_t timeouts{0};
    uint64_t active_objects_after_cleanup{0};
    uint64_t creating_objects_after_cleanup{0};
    uint64_t allocator_bytes_after_cleanup{0};
    uint64_t batch_ids_after_cleanup{0};
    uint64_t data_mismatches{0};
};

struct VChunkBenchmarkDecision {
    bool correctness_passed{false};
    bool cleanup_passed{false};
    bool performance_gain_found{false};
    bool recommend_full_design{false};
    std::string priority;
    std::vector<std::string> limitations;
    std::vector<std::string> production_cleanup;
};

double Percentile(std::vector<double> values, double percentile);
VChunkBenchmarkDecision AnalyzeVChunkBenchmark(
    const std::vector<VChunkBenchmarkResult>& results,
    double minimum_throughput_gain_ratio,
    double maximum_p99_regression_ratio);
Json::Value ToJson(const VChunkBenchmarkConfig& config);
Json::Value ToJson(const VChunkBenchmarkResult& result);
Json::Value ToJson(const VChunkBenchmarkDecision& decision);
Json::Value BuildVChunkBenchmarkReport(
    const VChunkBenchmarkConfig& config,
    const std::vector<VChunkBenchmarkResult>& results,
    const VChunkBenchmarkDecision& decision);

}  // namespace mooncake::benchmarks
