#include "vchunk_benchmark_report.h"

#include <algorithm>
#include <cmath>
#include <map>
#include <tuple>

namespace mooncake::benchmarks {

double Percentile(std::vector<double> values, double percentile) {
    if (values.empty() || percentile < 0 || percentile > 1) {
        return 0;
    }
    std::sort(values.begin(), values.end());
    const auto index = static_cast<size_t>(
        std::ceil(percentile * static_cast<double>(values.size())) - 1);
    return values[std::min(index, values.size() - 1)];
}

VChunkBenchmarkDecision AnalyzeVChunkBenchmark(
    const std::vector<VChunkBenchmarkResult>& results,
    double minimum_throughput_gain_ratio,
    double maximum_p99_regression_ratio) {
    VChunkBenchmarkDecision decision;
    decision.correctness_passed = true;
    decision.cleanup_passed = true;
    using Key = std::tuple<size_t, size_t, size_t>;
    std::map<Key, const VChunkBenchmarkResult*> legacy;
    for (const auto& result : results) {
        decision.correctness_passed &=
            result.errors == 0 && result.data_mismatches == 0 &&
            result.successes == result.operations;
        decision.cleanup_passed &=
            result.active_objects_after_cleanup == 0 &&
            result.creating_objects_after_cleanup == 0 &&
            result.allocator_bytes_after_cleanup == 0 &&
            result.batch_ids_after_cleanup == 0;
        const Key key{result.object_size, result.segment_count,
                      result.concurrency};
        if (result.path == "legacy") {
            legacy[key] = &result;
        }
    }
    for (const auto& result : results) {
        if (result.path != "vchunk") continue;
        const Key key{result.object_size, result.segment_count,
                      result.concurrency};
        const auto it = legacy.find(key);
        if (it == legacy.end() || it->second->throughput_mib_per_second <= 0) {
            continue;
        }
        const auto throughput_ratio =
            result.throughput_mib_per_second /
            it->second->throughput_mib_per_second;
        const auto p99_ratio = it->second->p99_us > 0
                                   ? result.p99_us / it->second->p99_us
                                   : 1.0;
        if (throughput_ratio >= minimum_throughput_gain_ratio &&
            p99_ratio <= maximum_p99_regression_ratio) {
            decision.performance_gain_found = true;
        }
    }
    decision.recommend_full_design = decision.correctness_passed &&
                                     decision.cleanup_passed &&
                                     decision.performance_gain_found;
    if (!decision.correctness_passed) {
        decision.priority = "correctness_and_failure_cleanup";
    } else if (!decision.cleanup_passed) {
        decision.priority = "resource_lifecycle";
    } else if (!decision.performance_gain_found) {
        decision.priority = "multi_batch_and_parallel_read";
    } else {
        decision.priority = "metadata_partitioning";
    }
    decision.limitations = {
        "memory segments and a single replica only",
        "benchmark data plane does not represent a production network",
        "restart recovery does not reconstruct remote memory contents",
        "experimental comparison metrics remain benchmark-only"};
    decision.production_cleanup = {
        "keep timeout cleanup, rollback, core metrics, and resource guards",
        "remove benchmark fault injection and synthetic transfer delay",
        "remove experimental A/B metrics from production targets",
        "remove temporary TransferEngine probes after equivalent coverage",
        "retire the legacy path only after stored-object inventory reaches zero"};
    return decision;
}

Json::Value ToJson(const VChunkBenchmarkConfig& config) {
    Json::Value value;
    for (const auto size : config.object_sizes) value["object_sizes"].append(size);
    for (const auto count : config.segment_counts)
        value["segment_counts"].append(count);
    for (const auto count : config.concurrencies)
        value["concurrencies"].append(count);
    value["operations_per_scenario"] =
        Json::UInt64(config.operations_per_scenario);
    value["seed"] = Json::UInt64(config.seed);
    value["failure_every_n"] = config.failure_every_n;
    value["transfer_delay_us"] = config.transfer_delay_us;
    return value;
}

Json::Value ToJson(const VChunkBenchmarkResult& result) {
    Json::Value value;
    value["path"] = result.path;
    value["object_size"] = Json::UInt64(result.object_size);
    value["segment_count"] = Json::UInt64(result.segment_count);
    value["concurrency"] = Json::UInt64(result.concurrency);
    value["operations"] = Json::UInt64(result.operations);
    value["successes"] = Json::UInt64(result.successes);
    value["errors"] = Json::UInt64(result.errors);
    value["injected_failures"] = Json::UInt64(result.injected_failures);
    value["duration_seconds"] = result.duration_seconds;
    value["qps"] = result.qps;
    value["throughput_mib_per_second"] = result.throughput_mib_per_second;
    value["p50_us"] = result.p50_us;
    value["p99_us"] = result.p99_us;
    value["cpu_ms"] = result.cpu_ms;
    value["transfer_requests"] = Json::UInt64(result.transfer_requests);
    value["transfer_time_us"] = Json::UInt64(result.transfer_time_us);
    value["control_time_us"] = Json::UInt64(result.control_time_us);
    value["metadata_bytes"] = Json::UInt64(result.metadata_bytes);
    value["metadata_amplification"] = result.metadata_amplification;
    value["rollbacks"] = Json::UInt64(result.rollbacks);
    value["retries"] = Json::UInt64(result.retries);
    value["timeouts"] = Json::UInt64(result.timeouts);
    value["active_objects_after_cleanup"] =
        Json::UInt64(result.active_objects_after_cleanup);
    value["creating_objects_after_cleanup"] =
        Json::UInt64(result.creating_objects_after_cleanup);
    value["allocator_bytes_after_cleanup"] =
        Json::UInt64(result.allocator_bytes_after_cleanup);
    value["batch_ids_after_cleanup"] =
        Json::UInt64(result.batch_ids_after_cleanup);
    value["data_mismatches"] = Json::UInt64(result.data_mismatches);
    return value;
}

Json::Value ToJson(const VChunkBenchmarkDecision& decision) {
    Json::Value value;
    value["correctness_passed"] = decision.correctness_passed;
    value["cleanup_passed"] = decision.cleanup_passed;
    value["performance_gain_found"] = decision.performance_gain_found;
    value["recommend_full_design"] = decision.recommend_full_design;
    value["priority"] = decision.priority;
    for (const auto& limitation : decision.limitations)
        value["known_limitations"].append(limitation);
    for (const auto& item : decision.production_cleanup)
        value["production_cleanup"].append(item);
    return value;
}

Json::Value BuildVChunkBenchmarkReport(
    const VChunkBenchmarkConfig& config,
    const std::vector<VChunkBenchmarkResult>& results,
    const VChunkBenchmarkDecision& decision) {
    Json::Value root;
    root["schema_version"] = 1;
    root["config"] = ToJson(config);
    for (const auto& result : results) root["results"].append(ToJson(result));
    root["decision"] = ToJson(decision);
    return root;
}

}  // namespace mooncake::benchmarks
