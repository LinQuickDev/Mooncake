// Copyright 2026 Mooncake Authors
// Licensed under the Apache License, Version 2.0.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <numa.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <numeric>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "common.h"
#include "scheduler/scheduler_core.h"
#include "scheduler/scheduler_policy.h"
#include "transfer_engine.h"

DEFINE_string(mode, "initiator", "Benchmark role: target or initiator");
DEFINE_string(protocol, "ub", "Transport protocol: ub, rdma, or tcp");
DEFINE_string(device_name, "urma0",
              "Comma-separated devices for UB/RDMA, e.g. urma0,urma1");
DEFINE_int32(numa_node, 0, "NUMA node used for the registered buffer");
DEFINE_string(target_seg_name, "", "Segment printed by the target process");
DEFINE_string(metadata_conn_string, "P2PHANDSHAKE",
              "Metadata connection string");
DEFINE_string(local_server_name, mooncake::getHostname(),
              "Local server name used for P2P discovery");
DEFINE_bool(scheduling, false, "Submit through the classic TE scheduler");
DEFINE_uint64(buffer_size, 1ULL << 30,
              "Registered initiator buffer size in bytes");
DEFINE_int32(foreground_threads, 1,
             "Worker count for each foreground request size");
DEFINE_int32(background_threads, 3,
             "Worker count for each background request size");
DEFINE_int32(warmup_seconds, 30, "Warmup duration for each repetition");
DEFINE_int32(duration_seconds, 120, "Measurement duration per repetition");
DEFINE_int32(repetitions, 5, "Number of independent repetitions");
DEFINE_uint64(foreground_deadline_us, 0,
              "Optional relative deadline for foreground requests");
DEFINE_string(output_jsonl, "scheduler-benchmark.jsonl",
              "File to which one JSON record per repetition is appended");
DEFINE_uint64(scheduler_quantum_bytes, 1ULL << 20, "Scheduler byte quantum");
DEFINE_uint64(scheduler_max_inflight_bytes, 16ULL << 20,
              "Scheduler maximum in-flight bytes");
DEFINE_uint64(scheduler_reserved_high_bytes, 1ULL << 20,
              "Scheduler bytes reserved for HIGH traffic");
DEFINE_uint32(scheduler_max_slices, 32,
              "Scheduler maximum transport slices per grant");
DEFINE_string(scheduler_class_weights, "8:4:1",
              "Scheduler class weights in HIGH:MEDIUM:LOW order");

namespace {

using Clock = std::chrono::steady_clock;
volatile std::sig_atomic_t target_running = 1;

struct TrafficClass {
    const char* name;
    size_t block_size;
    int threads;
    mooncake::TransferRequest::OpCode opcode;
    mooncake::TaskIntent intent;
    const char* tenant;
};

struct WorkerResult {
    std::vector<double> latency_us;
    uint64_t bytes{0};
    double duration_seconds{0};
};

struct LatencyMetrics {
    double average_us{0};
    double min_us{0};
    double p50_us{0};
    double p99_us{0};
    double p999_us{0};
    double p9999_us{0};
    double max_us{0};
};

void check(const mooncake::Status& status, const char* operation) {
    LOG_ASSERT(status.ok()) << operation << " failed: " << status.ToString();
}

void stopTarget(int) { target_running = 0; }

std::array<uint32_t, 3> parseClassWeights() {
    const std::string_view text = FLAGS_scheduler_class_weights;
    const size_t first_separator = text.find(':');
    const size_t second_separator =
        first_separator == std::string_view::npos
            ? std::string_view::npos
            : text.find(':', first_separator + 1);
    LOG_ASSERT(first_separator != std::string_view::npos &&
               second_separator != std::string_view::npos &&
               text.find(':', second_separator + 1) == std::string_view::npos)
        << "--scheduler_class_weights must be HIGH:MEDIUM:LOW, for example "
           "8:4:1";

    const std::array<std::string_view, 3> tokens = {
        text.substr(0, first_separator),
        text.substr(first_separator + 1,
                    second_separator - first_separator - 1),
        text.substr(second_separator + 1),
    };
    std::array<uint32_t, 3> weights{};
    for (size_t i = 0; i < tokens.size(); ++i) {
        uint64_t value = 0;
        const auto parsed = std::from_chars(
            tokens[i].data(), tokens[i].data() + tokens[i].size(), value);
        LOG_ASSERT(parsed.ec == std::errc() &&
                   parsed.ptr == tokens[i].data() + tokens[i].size() &&
                   value > 0 &&
                   value <= std::numeric_limits<uint32_t>::max())
            << "--scheduler_class_weights entries must be positive uint32 "
               "values";
        weights[i] = static_cast<uint32_t>(value);
    }
    return weights;
}

std::string topologyJson() {
    std::string devices;
    size_t begin = 0;
    while (begin < FLAGS_device_name.size()) {
        const size_t comma = FLAGS_device_name.find(',', begin);
        const std::string device =
            FLAGS_device_name.substr(begin, comma - begin);
        LOG_ASSERT(!device.empty()) << "--device_name contains an empty entry";
        if (!devices.empty()) devices += ',';
        devices += "\"" + device + "\"";
        if (comma == std::string::npos) break;
        begin = comma + 1;
    }
    LOG_ASSERT(!devices.empty())
        << "--device_name is required for " << FLAGS_protocol;
    return "{\"cpu:" + std::to_string(FLAGS_numa_node) + "\":[[" + devices +
           "],[]]}";
}

void installSelectedTransport(mooncake::TransferEngine& engine) {
    LOG_ASSERT(FLAGS_protocol == "ub" || FLAGS_protocol == "rdma" ||
               FLAGS_protocol == "tcp")
        << "--protocol must be ub, rdma, or tcp";
    if (FLAGS_protocol == "tcp") {
        LOG_ASSERT(engine.installTransport("tcp", nullptr) != nullptr)
            << "TCP Transport installation failed";
        return;
    }
    std::string topology = topologyJson();
    void* args[] = {topology.data(), nullptr};
    LOG_ASSERT(engine.installTransport(FLAGS_protocol, args) != nullptr)
        << FLAGS_protocol << " Transport installation failed";
}

double percentile(const std::vector<double>& sorted, double value) {
    if (sorted.empty()) return 0.0;
    const double rank = value / 100.0 * (sorted.size() - 1);
    const size_t lower = static_cast<size_t>(rank);
    const size_t upper = std::min(lower + 1, sorted.size() - 1);
    const double fraction = rank - lower;
    return sorted[lower] * (1.0 - fraction) + sorted[upper] * fraction;
}

LatencyMetrics summarizeLatency(std::vector<double> samples) {
    if (samples.empty()) return {};
    const double sum = std::accumulate(samples.begin(), samples.end(), 0.0);
    std::sort(samples.begin(), samples.end());
    return {
        sum / samples.size(),
        samples.front(),
        percentile(samples, 50.0),
        percentile(samples, 99.0),
        percentile(samples, 99.9),
        percentile(samples, 99.99),
        samples.back(),
    };
}

double runTransfer(mooncake::TransferEngine& engine, mooncake::SegmentID target,
                   void* local, uint64_t remote_offset,
                   const TrafficClass& traffic) {
    const auto batch = engine.allocateBatchID(1);
    mooncake::TransferRequest request;
    request.opcode = traffic.opcode;
    request.source = local;
    request.target_id = target;
    request.target_offset = remote_offset;
    request.length = traffic.block_size;

    const auto started = Clock::now();
    if (FLAGS_scheduling) {
        mooncake::SchedulingHint hint;
        hint.tenant_id = traffic.tenant;
        hint.intent = traffic.intent;
        if (FLAGS_foreground_deadline_us != 0 &&
            traffic.intent == mooncake::TaskIntent::FOREGROUND_GET) {
            hint.deadline_ns =
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    Clock::now().time_since_epoch())
                    .count() +
                FLAGS_foreground_deadline_us * 1000ULL;
        }
        check(engine.submitScheduledTransfer(batch, {{request, hint}}),
              "submitScheduledTransfer");
    } else {
        check(engine.submitTransfer(batch, {request}), "submitTransfer");
    }

    while (true) {
        mooncake::TransferStatus status;
        check(engine.getTransferStatus(batch, 0, status), "getTransferStatus");
        if (status.s == mooncake::TransferStatusEnum::COMPLETED) break;
        LOG_ASSERT(status.s != mooncake::TransferStatusEnum::FAILED &&
                   status.s != mooncake::TransferStatusEnum::TIMEOUT &&
                   status.s != mooncake::TransferStatusEnum::CANCELED)
            << "transfer reached terminal status " << status.s;
        std::this_thread::yield();
    }
    const double elapsed_us =
        std::chrono::duration<double, std::micro>(Clock::now() - started)
            .count();
    check(engine.freeBatchID(batch), "freeBatchID");
    return elapsed_us;
}

void runWorker(mooncake::TransferEngine& engine, mooncake::SegmentID target,
               uint8_t* local_base, uint64_t remote_base, size_t worker_index,
               size_t address_stride, const TrafficClass& traffic,
               WorkerResult& result) {
    void* local = local_base + worker_index * address_stride;
    const uint64_t remote = remote_base + worker_index * address_stride;
    auto until = Clock::now() + std::chrono::seconds(FLAGS_warmup_seconds);
    while (Clock::now() < until)
        runTransfer(engine, target, local, remote, traffic);

    const auto started = Clock::now();
    until = started + std::chrono::seconds(FLAGS_duration_seconds);
    while (Clock::now() < until) {
        result.latency_us.push_back(
            runTransfer(engine, target, local, remote, traffic));
        result.bytes += traffic.block_size;
    }
    result.duration_seconds =
        std::chrono::duration<double>(Clock::now() - started).count();
}

void appendRecord(const std::vector<TrafficClass>& classes,
                  const std::vector<std::vector<WorkerResult>>& results,
                  const std::array<uint32_t, 3>& class_weights,
                  int repetition) {
    std::ofstream output(FLAGS_output_jsonl, std::ios::app);
    LOG_ASSERT(output) << "cannot open " << FLAGS_output_jsonl;

    double aggregate_throughput = 0.0;
    struct ClassMetrics {
        LatencyMetrics latency;
        double throughput_gbps;
        uint64_t operations;
    };
    std::vector<ClassMetrics> metrics;
    for (size_t class_index = 0; class_index < classes.size(); ++class_index) {
        std::vector<double> latencies;
        uint64_t operations = 0;
        double throughput = 0.0;
        for (const auto& worker : results[class_index]) {
            latencies.insert(latencies.end(), worker.latency_us.begin(),
                             worker.latency_us.end());
            operations += worker.latency_us.size();
            if (worker.duration_seconds > 0)
                throughput += worker.bytes / 1e9 / worker.duration_seconds;
        }
        aggregate_throughput += throughput;
        metrics.push_back(
            {summarizeLatency(std::move(latencies)), throughput, operations});
    }

    output << std::fixed << std::setprecision(6)
           << "{\"schema_version\":2,\"scheduling\":"
           << (FLAGS_scheduling ? "true" : "false")
           << ",\"protocol\":\"" << FLAGS_protocol << "\""
           << ",\"repetition\":" << repetition
           << ",\"class_weights\":{\"high\":" << class_weights[0]
           << ",\"medium\":" << class_weights[1]
           << ",\"low\":" << class_weights[2] << '}'
           << ",\"aggregate_throughput_gbps\":" << aggregate_throughput
           << ",\"classes\":[";
    for (size_t i = 0; i < classes.size(); ++i) {
        if (i != 0) output << ',';
        output << "{\"name\":\"" << classes[i].name
               << "\",\"threads\":" << classes[i].threads
               << ",\"block_size\":" << classes[i].block_size
               << ",\"operations\":" << metrics[i].operations
               << ",\"avg_us\":" << metrics[i].latency.average_us
               << ",\"min_us\":" << metrics[i].latency.min_us
               << ",\"p50_us\":" << metrics[i].latency.p50_us
               << ",\"p99_us\":" << metrics[i].latency.p99_us
               << ",\"p999_us\":" << metrics[i].latency.p999_us
               << ",\"p9999_us\":" << metrics[i].latency.p9999_us
               << ",\"max_us\":" << metrics[i].latency.max_us
               << ",\"throughput_gbps\":" << metrics[i].throughput_gbps << '}';
    }
    output << "]}\n";
    LOG_ASSERT(output) << "failed to write " << FLAGS_output_jsonl;

    std::cout << "repetition=" << repetition << " scheduling=" << std::boolalpha
              << FLAGS_scheduling
              << " class_weights=" << class_weights[0] << ':'
              << class_weights[1] << ':' << class_weights[2]
              << " aggregate_throughput=" << aggregate_throughput << " GB/s"
              << std::endl;
    for (size_t i = 0; i < classes.size(); ++i)
        std::cout << "  " << classes[i].name
                  << ": avg=" << metrics[i].latency.average_us
                  << " us min=" << metrics[i].latency.min_us
                  << " us p50=" << metrics[i].latency.p50_us
                  << " us p99=" << metrics[i].latency.p99_us
                  << " us p999=" << metrics[i].latency.p999_us
                  << " us p9999=" << metrics[i].latency.p9999_us
                  << " us max=" << metrics[i].latency.max_us
                  << " us throughput=" << metrics[i].throughput_gbps
                  << " GB/s operations=" << metrics[i].operations << std::endl;
}

}  // namespace

int main(int argc, char** argv) {
    gflags::SetUsageMessage("Classic Transfer Engine scheduler A/B benchmark");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    LOG_ASSERT(FLAGS_mode == "target" || FLAGS_mode == "initiator")
        << "--mode must be target or initiator";
    if (FLAGS_mode == "initiator")
        LOG_ASSERT(!FLAGS_target_seg_name.empty())
            << "--target_seg_name is required in initiator mode";
    LOG_ASSERT(FLAGS_numa_node >= 0) << "--numa_node must be non-negative";
    LOG_ASSERT(FLAGS_foreground_threads > 0 && FLAGS_background_threads > 0);
    LOG_ASSERT(FLAGS_foreground_threads <= 128 &&
               FLAGS_background_threads <= 128)
        << "thread counts must not exceed 128 per traffic class";
    LOG_ASSERT(FLAGS_warmup_seconds >= 0 && FLAGS_duration_seconds > 0 &&
               FLAGS_repetitions > 0);
    const auto class_weights = parseClassWeights();

    std::vector<TrafficClass> classes = {
        {"foreground-4k", 4ULL << 10, FLAGS_foreground_threads,
         mooncake::TransferRequest::READ, mooncake::TaskIntent::FOREGROUND_GET,
         "online"},
        {"foreground-64k", 64ULL << 10, FLAGS_foreground_threads,
         mooncake::TransferRequest::READ, mooncake::TaskIntent::FOREGROUND_GET,
         "online"},
        {"migration-8m", 8ULL << 20, FLAGS_background_threads,
         mooncake::TransferRequest::WRITE, mooncake::TaskIntent::MIGRATION,
         "batch"},
        {"checkpoint-64m", 64ULL << 20, FLAGS_background_threads,
         mooncake::TransferRequest::WRITE, mooncake::TaskIntent::CHECKPOINT,
         "batch"},
    };

    size_t worker_count = 0;
    for (const auto& traffic : classes) worker_count += traffic.threads;
    const size_t address_stride = 64ULL << 20;
    const size_t required_buffer = worker_count * address_stride;
    LOG_ASSERT(FLAGS_buffer_size <= std::numeric_limits<size_t>::max() &&
               FLAGS_buffer_size % 4096 == 0)
        << "--buffer_size must fit in size_t and be a multiple of 4096";
    LOG_ASSERT(FLAGS_buffer_size >= required_buffer)
        << "--buffer_size must be at least " << required_buffer;

    auto* local_buffer = static_cast<uint8_t*>(numa_alloc_onnode(
        static_cast<size_t>(FLAGS_buffer_size), FLAGS_numa_node));
    LOG_ASSERT(local_buffer != nullptr) << "failed to allocate local buffer";

    mooncake::TransferEngine engine(false);
    LOG_ASSERT(
        engine.init(FLAGS_metadata_conn_string, FLAGS_local_server_name) == 0)
        << "TransferEngine initialization failed";
    installSelectedTransport(engine);
    const std::string location = "cpu:" + std::to_string(FLAGS_numa_node);
    LOG_ASSERT(engine.registerLocalMemory(local_buffer, FLAGS_buffer_size,
                                          location) == 0)
        << "local memory registration failed";

    if (FLAGS_mode == "target") {
        std::signal(SIGINT, stopTarget);
        std::signal(SIGTERM, stopTarget);
        std::cout << "TARGET_SEGMENT=" << engine.getLocalIpAndPort()
                  << " PROTOCOL=" << FLAGS_protocol << std::endl;
        while (target_running)
            std::this_thread::sleep_for(std::chrono::seconds(1));
        engine.unregisterLocalMemory(local_buffer);
        engine.freeEngine();
        numa_free(local_buffer, FLAGS_buffer_size);
        return 0;
    }

    if (FLAGS_scheduling) {
        mooncake::scheduling::SchedulerConfig config;
        config.quantum_bytes = FLAGS_scheduler_quantum_bytes;
        config.max_inflight_bytes = FLAGS_scheduler_max_inflight_bytes;
        config.reserved_high_bytes = FLAGS_scheduler_reserved_high_bytes;
        config.max_slices = FLAGS_scheduler_max_slices;
        config.class_weights = class_weights;
        check(engine.configureScheduling(config), "configureScheduling");
    }

    const auto target = engine.openSegment(FLAGS_target_seg_name);
    auto segment = engine.getMetadata()->getSegmentDescByID(target);
    LOG_ASSERT(segment && !segment->buffers.empty())
        << "target segment has no registered buffers";
    LOG_ASSERT(segment->protocol == FLAGS_protocol)
        << "target protocol is " << segment->protocol << ", expected "
        << FLAGS_protocol;
    const auto& remote_buffer = segment->buffers.front();
    LOG_ASSERT(remote_buffer.length >= required_buffer)
        << "target buffer must be at least " << required_buffer << " bytes";

    for (int repetition = 1; repetition <= FLAGS_repetitions; ++repetition) {
        std::vector<std::vector<WorkerResult>> results;
        results.reserve(classes.size());
        for (const auto& traffic : classes)
            results.emplace_back(traffic.threads);

        std::vector<std::thread> workers;
        size_t worker_index = 0;
        for (size_t class_index = 0; class_index < classes.size();
             ++class_index) {
            for (int class_worker = 0;
                 class_worker < classes[class_index].threads;
                 ++class_worker, ++worker_index) {
                workers.emplace_back(
                    runWorker, std::ref(engine), target, local_buffer,
                    remote_buffer.addr, worker_index, address_stride,
                    std::cref(classes[class_index]),
                    std::ref(results[class_index][class_worker]));
            }
        }
        for (auto& worker : workers) worker.join();
        appendRecord(classes, results, class_weights, repetition);
    }

    engine.closeSegment(target);
    engine.unregisterLocalMemory(local_buffer);
    engine.freeEngine();
    numa_free(local_buffer, FLAGS_buffer_size);
    return 0;
}
