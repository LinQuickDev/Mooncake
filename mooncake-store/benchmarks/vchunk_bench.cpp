#include "vchunk_benchmark_report.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "master_service.h"
#include "common.h"
#include "vchunk_client.h"

DEFINE_string(object_sizes, "4096,65536,262144,1048576,4194304",
              "Comma-separated object sizes in bytes");
DEFINE_string(segment_counts, "1,2,4,8",
              "Comma-separated memory segment counts");
DEFINE_string(concurrencies, "1,8,32,128",
              "Comma-separated worker counts");
DEFINE_uint64(operations, 100, "Operations per path and scenario");
DEFINE_uint64(seed, 20260826, "Deterministic data seed");
DEFINE_uint32(failure_every_n, 0,
              "Fail every Nth data-plane call; zero disables injection");
DEFINE_uint32(transfer_delay_us, 0,
              "Synthetic data-plane delay per call");
DEFINE_double(min_throughput_gain, 1.05,
              "Required vchunk/legacy throughput ratio");
DEFINE_double(max_p99_regression, 1.10,
              "Maximum accepted vchunk/legacy P99 ratio");
DEFINE_string(output_json, "", "Optional JSON report output path");

namespace mooncake::benchmarks {
namespace {

using Clock = std::chrono::steady_clock;

std::vector<size_t> ParseSizes(const std::string& text) {
    std::vector<size_t> values;
    std::stringstream stream(text);
    std::string token;
    while (std::getline(stream, token, ',')) {
        const auto value = std::stoull(token);
        if (value == 0) throw std::invalid_argument("matrix values must be > 0");
        values.push_back(static_cast<size_t>(value));
    }
    if (values.empty()) throw std::invalid_argument("matrix must not be empty");
    return values;
}

Segment MakeSegment(size_t index) {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "vchunk-bench-segment-" + std::to_string(index);
    segment.base = 0x1000000000ULL + index * 0x100000000ULL;
    segment.size = 512U * 1024U * 1024U;
    segment.protocol = "tcp";
    segment.te_endpoint = segment.name;
    return segment;
}

std::vector<uint8_t> MakeData(size_t size, uint64_t seed, size_t operation) {
    std::vector<uint8_t> data(size);
    uint64_t state = seed ^ (operation * 0x9E3779B97F4A7C15ULL);
    for (auto& value : data) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        value = static_cast<uint8_t>(state);
    }
    return data;
}

class BenchmarkLegacyPath final : public VChunkLegacyPath {
   public:
    ErrorCode Put(const TenantId& tenant, const std::string& key,
                  const void* source, size_t length) override {
        if (!source || length == 0) return ErrorCode::INVALID_PARAMS;
        const auto* begin = static_cast<const uint8_t*>(source);
        std::lock_guard<std::mutex> guard(mutex_);
        objects_[tenant.MakeScopedKey(key)] =
            std::vector<uint8_t>(begin, begin + length);
        return ErrorCode::OK;
    }
    ErrorCode Get(const TenantId& tenant, const std::string& key,
                  void* destination, size_t length) override {
        std::lock_guard<std::mutex> guard(mutex_);
        const auto it = objects_.find(tenant.MakeScopedKey(key));
        if (it == objects_.end()) return ErrorCode::OBJECT_NOT_FOUND;
        if (!destination || it->second.size() != length)
            return ErrorCode::INVALID_PARAMS;
        std::copy(it->second.begin(), it->second.end(),
                  static_cast<uint8_t*>(destination));
        return ErrorCode::OK;
    }
    ErrorCode Remove(const TenantId& tenant, const std::string& key) override {
        std::lock_guard<std::mutex> guard(mutex_);
        objects_.erase(tenant.MakeScopedKey(key));
        return ErrorCode::OK;
    }
    size_t Size() const {
        std::lock_guard<std::mutex> guard(mutex_);
        return objects_.size();
    }

   private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::vector<uint8_t>> objects_;
};

class BenchmarkDataPlane final : public VChunkDataPlane {
   public:
    BenchmarkDataPlane(uint32_t failure_every_n, uint32_t delay_us)
        : failure_every_n_(failure_every_n), delay_us_(delay_us) {}

    ErrorCode Write(const VChunkMetadataRecord& record, const void* source,
                    size_t length, Clock::time_point deadline) override {
        BatchScope batch(*this);
        if (ShouldFailOrTimeout(deadline)) return ErrorCode::TRANSFER_FAIL;
        const auto* begin = static_cast<const uint8_t*>(source);
        std::lock_guard<std::mutex> guard(mutex_);
        objects_[record.vchunk_id] =
            std::vector<uint8_t>(begin, begin + length);
        transfer_requests_.fetch_add(record.slice_count);
        return ErrorCode::OK;
    }
    ErrorCode Read(const VChunkMetadataRecord& record, void* destination,
                   size_t length, Clock::time_point deadline) override {
        BatchScope batch(*this);
        if (ShouldFailOrTimeout(deadline)) return ErrorCode::TRANSFER_FAIL;
        std::lock_guard<std::mutex> guard(mutex_);
        const auto it = objects_.find(record.vchunk_id);
        if (it == objects_.end() || it->second.size() != length)
            return ErrorCode::OBJECT_NOT_FOUND;
        std::copy(it->second.begin(), it->second.end(),
                  static_cast<uint8_t*>(destination));
        transfer_requests_.fetch_add(record.slice_count);
        return ErrorCode::OK;
    }
    uint64_t TransferRequests() const { return transfer_requests_.load(); }
    uint64_t TransferTimeUs() const { return transfer_time_us_.load(); }
    uint64_t ActiveBatches() const { return active_batches_.load(); }
    size_t StoredObjects() const {
        std::lock_guard<std::mutex> guard(mutex_);
        return objects_.size();
    }
    void Clear() {
        std::lock_guard<std::mutex> guard(mutex_);
        objects_.clear();
    }

   private:
    class BatchScope {
       public:
        explicit BatchScope(BenchmarkDataPlane& owner)
            : owner_(owner), started_(Clock::now()) {
            owner_.active_batches_.fetch_add(1);
        }
        ~BatchScope() {
            owner_.active_batches_.fetch_sub(1);
            owner_.transfer_time_us_.fetch_add(
                std::chrono::duration_cast<std::chrono::microseconds>(
                    Clock::now() - started_)
                    .count());
        }

       private:
        BenchmarkDataPlane& owner_;
        Clock::time_point started_;
    };

    bool ShouldFailOrTimeout(Clock::time_point deadline) {
        if (delay_us_ > 0)
            std::this_thread::sleep_for(std::chrono::microseconds(delay_us_));
        const auto call = calls_.fetch_add(1) + 1;
        return Clock::now() >= deadline ||
               (failure_every_n_ > 0 && call % failure_every_n_ == 0);
    }

    uint32_t failure_every_n_;
    uint32_t delay_us_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::vector<uint8_t>> objects_;
    std::atomic<uint64_t> calls_{0};
    std::atomic<uint64_t> transfer_requests_{0};
    std::atomic<uint64_t> transfer_time_us_{0};
    std::atomic<uint64_t> active_batches_{0};
};

VChunkBenchmarkResult RunScenario(const std::string& path, size_t object_size,
                                  size_t segment_count, size_t concurrency,
                                  const VChunkBenchmarkConfig& config) {
    MasterServiceConfig master_config;
    master_config.memory_allocator = BufferAllocatorType::OFFSET;
    master_config.vchunk_config.enabled = true;
    master_config.vchunk_config.reaper_interval_ms = 100;
    MasterService master(master_config);
    const auto client_id = generate_uuid();
    for (size_t i = 0; i < segment_count; ++i) {
        if (!master.MountSegment(MakeSegment(i), client_id))
            throw std::runtime_error("failed to mount benchmark segment");
    }
    BenchmarkLegacyPath legacy;
    BenchmarkDataPlane data_plane(config.failure_every_n,
                                  config.transfer_delay_us);
    VChunkClient client(path == "vchunk", master, data_plane, legacy,
                        std::chrono::seconds(5),
                        [] { return getCurrentTimeInMilli(); }, 0);
    const TenantId tenant("vchunk-benchmark");
    std::vector<double> latencies(config.operations_per_scenario);
    std::atomic<size_t> next{0};
    std::atomic<size_t> successes{0};
    std::atomic<size_t> errors{0};
    std::atomic<size_t> injected_failures{0};
    std::atomic<size_t> mismatches{0};
    const auto cpu_start = std::clock();
    const auto started = Clock::now();
    std::vector<std::thread> workers;
    workers.reserve(concurrency);
    for (size_t worker = 0; worker < concurrency; ++worker) {
        workers.emplace_back([&] {
            while (true) {
                const auto operation = next.fetch_add(1);
                if (operation >= config.operations_per_scenario) break;
                const auto key = "key-" + std::to_string(operation);
                const auto source =
                    MakeData(object_size, config.seed, operation);
                std::vector<uint8_t> destination(object_size);
                const auto operation_started = Clock::now();
                const auto put = client.Put(tenant, key, source.data(),
                                            source.size());
                ErrorCode get = put;
                if (put == ErrorCode::OK)
                    get = client.Get(tenant, key, destination.data(),
                                     destination.size());
                client.Remove(tenant, key);
                latencies[operation] =
                    std::chrono::duration<double, std::micro>(
                        Clock::now() - operation_started)
                        .count();
                if (put == ErrorCode::OK && get == ErrorCode::OK &&
                    destination == source) {
                    successes.fetch_add(1);
                } else if (config.failure_every_n > 0 &&
                           (put != ErrorCode::OK || get != ErrorCode::OK)) {
                    successes.fetch_add(1);
                    injected_failures.fetch_add(1);
                } else {
                    errors.fetch_add(1);
                    if (put == ErrorCode::OK && get == ErrorCode::OK)
                        mismatches.fetch_add(1);
                }
            }
        });
    }
    for (auto& worker : workers) worker.join();
    const auto elapsed = std::chrono::duration<double>(Clock::now() - started);
    const auto cpu_ms = 1000.0 * (std::clock() - cpu_start) / CLOCKS_PER_SEC;
    const auto metrics = client.MetricsSnapshot();
    const auto master_metrics = master.GetVChunkMetrics();
    VChunkBenchmarkResult result;
    result.path = path;
    result.object_size = object_size;
    result.segment_count = segment_count;
    result.concurrency = concurrency;
    result.operations = config.operations_per_scenario;
    result.successes = successes.load();
    result.errors = errors.load();
    result.injected_failures = injected_failures.load();
    result.duration_seconds = elapsed.count();
    result.qps = result.operations / result.duration_seconds;
    result.throughput_mib_per_second =
        result.qps * object_size / (1024.0 * 1024.0);
    result.p50_us = Percentile(latencies, 0.50);
    result.p99_us = Percentile(latencies, 0.99);
    result.cpu_ms = cpu_ms;
    result.transfer_requests = data_plane.TransferRequests();
    result.transfer_time_us = data_plane.TransferTimeUs();
    const auto wall_work_us = static_cast<uint64_t>(
        elapsed.count() * 1'000'000.0 * static_cast<double>(concurrency));
    result.control_time_us =
        wall_work_us > result.transfer_time_us
            ? wall_work_us - result.transfer_time_us
            : 0;
    result.metadata_bytes = master_metrics.metadata_bytes;
    const auto payload_bytes = result.successes * object_size;
    result.metadata_amplification =
        payload_bytes > 0
            ? static_cast<double>(result.metadata_bytes) / payload_bytes
            : 0;
    result.rollbacks = metrics.rollbacks;
    result.retries = metrics.retries;
    result.timeouts = metrics.timeouts;
    result.active_objects_after_cleanup =
        master_metrics.states[static_cast<size_t>(VChunkStatus::ACTIVE)];
    result.creating_objects_after_cleanup =
        master_metrics.states[static_cast<size_t>(VChunkStatus::CREATING)];
    result.allocator_bytes_after_cleanup = master_metrics.allocated_bytes;
    result.batch_ids_after_cleanup = data_plane.ActiveBatches();
    result.data_mismatches = mismatches.load();
    if (path == "legacy" && legacy.Size() != 0)
        result.allocator_bytes_after_cleanup = legacy.Size();
    data_plane.Clear();
    return result;
}

}  // namespace
}  // namespace mooncake::benchmarks

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    using namespace mooncake::benchmarks;
    try {
        VChunkBenchmarkConfig config;
        config.object_sizes = ParseSizes(FLAGS_object_sizes);
        config.segment_counts = ParseSizes(FLAGS_segment_counts);
        config.concurrencies = ParseSizes(FLAGS_concurrencies);
        config.operations_per_scenario = FLAGS_operations;
        config.seed = FLAGS_seed;
        config.failure_every_n = FLAGS_failure_every_n;
        config.transfer_delay_us = FLAGS_transfer_delay_us;
        std::vector<VChunkBenchmarkResult> results;
        for (const auto size : config.object_sizes) {
            for (const auto segments : config.segment_counts) {
                for (const auto concurrency : config.concurrencies) {
                    results.push_back(RunScenario("legacy", size, segments,
                                                  concurrency, config));
                    results.push_back(RunScenario("vchunk", size, segments,
                                                  concurrency, config));
                }
            }
        }
        const auto decision = AnalyzeVChunkBenchmark(
            results, FLAGS_min_throughput_gain, FLAGS_max_p99_regression,
            config.production_equivalent_data_plane);
        const auto report = BuildVChunkBenchmarkReport(config, results, decision);
        Json::StreamWriterBuilder writer;
        writer["indentation"] = "  ";
        const auto output = Json::writeString(writer, report);
        std::cout << output << std::endl;
        if (!FLAGS_output_json.empty()) {
            std::ofstream file(FLAGS_output_json);
            if (!file) throw std::runtime_error("cannot open output file");
            file << output << '\n';
        }
        return decision.correctness_passed && decision.cleanup_passed ? 0 : 2;
    } catch (const std::exception& error) {
        LOG(ERROR) << "vchunk benchmark failed: " << error.what();
        return 1;
    }
}
