#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>
#include <vector>

#include "master_client.h"
#include "vchunk_client.h"
#include "vchunk_control_plane.h"
#include "vchunk_transfer_engine.h"

DEFINE_string(master, "127.0.0.1:50051", "Master RPC address");
DEFINE_string(metadata, "http://127.0.0.1:8080/metadata",
              "TransferEngine metadata connection string");
DEFINE_string(local_server, "vchunk-bench-client:12345",
              "Unique TransferEngine client endpoint");
DEFINE_string(protocol, "tcp", "TransferEngine transport protocol");
DEFINE_bool(auto_discovery, false,
            "Enable TransferEngine device auto-discovery (normally required "
            "for RDMA without an explicit device matrix)");
DEFINE_string(tenant, "vchunk-benchmark", "Tenant id");
DEFINE_uint64(object_size, 1048576, "Object size in bytes");
DEFINE_uint64(operations, 1000, "Put/Get/Remove transactions");
DEFINE_uint32(concurrency, 8, "Concurrent workers");
DEFINE_uint32(timeout_ms, 30000, "Per-operation timeout");

namespace mooncake {
namespace {
using Clock = std::chrono::steady_clock;

class DisabledLegacyPath final : public VChunkLegacyPath {
   public:
    ErrorCode Put(const TenantId&, const std::string&, const void*, size_t)
        override {
        return ErrorCode::INVALID_PARAMS;
    }
    ErrorCode Get(const TenantId&, const std::string&, void*, size_t) override {
        return ErrorCode::INVALID_PARAMS;
    }
    ErrorCode Remove(const TenantId&, const std::string&) override {
        return ErrorCode::INVALID_PARAMS;
    }
};

double Percentile(std::vector<double> values, double percentile) {
    if (values.empty()) return 0;
    std::sort(values.begin(), values.end());
    const auto index = static_cast<size_t>(
        percentile * static_cast<double>(values.size() - 1));
    return values[index];
}
}  // namespace
}  // namespace mooncake

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    using namespace mooncake;
    if (FLAGS_object_size == 0 || FLAGS_operations == 0 ||
        FLAGS_concurrency == 0 || FLAGS_timeout_ms == 0) {
        LOG(ERROR) << "benchmark numeric arguments must be greater than zero";
        return 2;
    }

    MasterClient master(generate_uuid(), nullptr, FLAGS_tenant);
    if (const auto error = master.Connect(FLAGS_master);
        error != ErrorCode::OK) {
        LOG(ERROR) << "failed to connect master: " << toString(error);
        return 2;
    }
    auto runtime = master.GetVChunkRuntimeInfo();
    if (!runtime || !runtime->enabled || !runtime->persistent_metadata) {
        LOG(ERROR) << "master must enable vchunk with persistent ETCD metadata";
        return 2;
    }
    RpcVChunkControlPlane control_plane(master);

    TransferEngine engine(FLAGS_auto_discovery);
    if (engine.init(FLAGS_metadata, FLAGS_local_server) != 0 ||
        engine.installTransport(FLAGS_protocol, nullptr) == nullptr) {
        LOG(ERROR) << "failed to initialize TransferEngine";
        return 2;
    }
    TransferEngineVChunkDataPlane data_plane(engine);
    DisabledLegacyPath legacy;
    const TenantId tenant(FLAGS_tenant);

    std::atomic<uint64_t> next{0};
    std::atomic<uint64_t> succeeded{0};
    std::atomic<uint64_t> failed{0};
    std::vector<double> latencies(FLAGS_operations);
    std::vector<std::thread> workers;
    workers.reserve(FLAGS_concurrency);
    std::atomic<bool> setup_failed{false};
    std::barrier start_barrier(FLAGS_concurrency + 1);
    std::barrier finish_barrier(FLAGS_concurrency + 1);
    for (uint32_t worker_id = 0; worker_id < FLAGS_concurrency; ++worker_id) {
        workers.emplace_back([&, worker_id] {
            std::vector<uint8_t> source(FLAGS_object_size);
            std::vector<uint8_t> destination(FLAGS_object_size);
            const bool source_registered =
                engine.registerLocalMemory(source.data(), source.size(),
                                           "cpu:0") == 0;
            const bool destination_registered =
                engine.registerLocalMemory(destination.data(),
                                           destination.size(), "cpu:0") == 0;
            if (!source_registered || !destination_registered) {
                setup_failed.store(true);
            }
            start_barrier.arrive_and_wait();
            if (setup_failed.load()) {
                finish_barrier.arrive_and_wait();
                if (source_registered)
                    engine.unregisterLocalMemory(source.data());
                if (destination_registered)
                    engine.unregisterLocalMemory(destination.data());
                return;
            }
            VChunkClient client(
                true, control_plane, data_plane, legacy,
                std::chrono::milliseconds(FLAGS_timeout_ms),
                [] { return getCurrentTimeInMilli(); });
            while (true) {
                const auto operation = next.fetch_add(1);
                if (operation >= FLAGS_operations) break;
                for (size_t i = 0; i < source.size(); ++i) {
                    source[i] = static_cast<uint8_t>((operation + i) & 0xff);
                }
                const auto key = "distributed-" +
                                 std::to_string(worker_id) + "-" +
                                 std::to_string(operation);
                const auto operation_started = Clock::now();
                auto error = client.Put(tenant, key, source.data(),
                                        source.size());
                if (error == ErrorCode::OK) {
                    error = client.Get(tenant, key, destination.data(),
                                       destination.size());
                }
                const bool data_matches = destination == source;
                const auto remove_error = client.Remove(tenant, key);
                latencies[operation] =
                    std::chrono::duration<double, std::micro>(
                        Clock::now() - operation_started)
                        .count();
                if (error == ErrorCode::OK && data_matches &&
                    remove_error == ErrorCode::OK) {
                    succeeded.fetch_add(1);
                } else {
                    failed.fetch_add(1);
                }
            }
            finish_barrier.arrive_and_wait();
            engine.unregisterLocalMemory(source.data());
            engine.unregisterLocalMemory(destination.data());
        });
    }
    start_barrier.arrive_and_wait();
    const auto started = Clock::now();
    finish_barrier.arrive_and_wait();
    const auto finished = Clock::now();
    for (auto& worker : workers) worker.join();
    if (setup_failed.load()) {
        LOG(ERROR) << "failed to register benchmark buffers";
        return 2;
    }
    const auto seconds =
        std::chrono::duration<double>(finished - started).count();
    latencies.resize(static_cast<size_t>(next.load() > FLAGS_operations
                                             ? FLAGS_operations
                                             : next.load()));
    const double gib = static_cast<double>(succeeded.load()) *
                       static_cast<double>(FLAGS_object_size) * 2.0 /
                       (1024.0 * 1024.0 * 1024.0);
    std::cout << "{\n"
              << "  \"production_equivalent_data_plane\": true,\n"
              << "  \"master_rpc\": \"" << FLAGS_master << "\",\n"
              << "  \"transfer_protocol\": \"" << FLAGS_protocol << "\",\n"
              << "  \"auto_discovery\": "
              << (FLAGS_auto_discovery ? "true" : "false") << ",\n"
              << "  \"object_size_bytes\": " << FLAGS_object_size << ",\n"
              << "  \"concurrency\": " << FLAGS_concurrency << ",\n"
              << "  \"operations\": " << FLAGS_operations << ",\n"
              << "  \"succeeded\": " << succeeded.load() << ",\n"
              << "  \"failed\": " << failed.load() << ",\n"
              << "  \"duration_seconds\": " << seconds << ",\n"
              << "  \"transactions_per_second\": "
              << succeeded.load() / seconds << ",\n"
              << "  \"data_gib_per_second\": " << gib / seconds << ",\n"
              << "  \"latency_p50_us\": " << Percentile(latencies, 0.50)
              << ",\n"
              << "  \"latency_p99_us\": " << Percentile(latencies, 0.99)
              << "\n}\n";
    return failed.load() == 0 && succeeded.load() == FLAGS_operations ? 0 : 1;
}
