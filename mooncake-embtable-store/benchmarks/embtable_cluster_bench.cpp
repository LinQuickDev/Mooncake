#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <exception>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "emb_table_client/emb_table_dummy_client.h"
#include "utils.h"

DEFINE_string(embtable_rpc_endpoint, "127.0.0.1:50055",
              "EmbTableClient RPC endpoint in host:port form");
DEFINE_string(embtable_table_name, "", "Embedding table to benchmark");
DEFINE_string(embtable_shared_memory_size, "64 MB",
              "POSIX shared-memory size for every dummy client");
DEFINE_uint64(embtable_num_keys, 1 << 20,
              "Number of keys inserted before Find benchmarking");
DEFINE_uint64(embtable_key_start, 1, "First key used for data preparation");
DEFINE_uint64(embtable_request_keys, 64, "Number of keys in each Find request");
DEFINE_uint64(embtable_insert_batch_size, 1024,
              "Number of key/value pairs in each preparation Insert call");
DEFINE_uint64(embtable_value_size, 0,
              "Value size in bytes; 0 uses the size reported by the server");
DEFINE_uint64(embtable_threads, 1, "Number of concurrent dummy clients");
DEFINE_string(embtable_mode, "once", "Benchmark mode: once or continuous");
DEFINE_uint64(embtable_iterations, 100000, "Total Find requests in once mode");
DEFINE_uint64(embtable_duration_sec, 30,
              "Measurement duration in continuous mode");
DEFINE_uint64(embtable_report_interval_sec, 1,
              "Interval for printing continuous-mode statistics");
DEFINE_bool(embtable_prepare_data, true,
            "Insert keys and build the index before measuring Find");

namespace {

using Clock = std::chrono::steady_clock;

struct Snapshot {
    uint64_t requests = 0;
    uint64_t keys = 0;
    uint64_t bytes = 0;
    uint64_t errors = 0;
    uint64_t latency_sum_ns = 0;
    std::vector<uint64_t> latency_ns;
};

class WorkerStats {
   public:
    void Record(uint64_t key_count, uint64_t value_size, uint64_t latency_ns,
                bool success) {
        std::lock_guard<std::mutex> lock(mutex_);
        ++snapshot_.requests;
        snapshot_.keys += key_count;
        snapshot_.bytes += key_count * value_size;
        snapshot_.latency_sum_ns += latency_ns;
        snapshot_.latency_ns.push_back(latency_ns);
        if (!success) ++snapshot_.errors;
    }

    Snapshot Take() {
        std::lock_guard<std::mutex> lock(mutex_);
        Snapshot result;
        std::swap(result, snapshot_);
        return result;
    }

   private:
    std::mutex mutex_;
    Snapshot snapshot_;
};

void Merge(Snapshot& target, Snapshot source) {
    target.requests += source.requests;
    target.keys += source.keys;
    target.bytes += source.bytes;
    target.errors += source.errors;
    target.latency_sum_ns += source.latency_sum_ns;
    target.latency_ns.insert(target.latency_ns.end(),
                             std::make_move_iterator(source.latency_ns.begin()),
                             std::make_move_iterator(source.latency_ns.end()));
}

double PercentileUs(std::vector<uint64_t>& latency_ns, double percentile) {
    if (latency_ns.empty()) return 0.0;
    std::sort(latency_ns.begin(), latency_ns.end());
    const size_t rank = static_cast<size_t>(
        std::ceil(percentile * static_cast<double>(latency_ns.size())));
    const size_t index = std::min(latency_ns.size() - 1, rank - 1);
    return static_cast<double>(latency_ns[index]) / 1000.0;
}

void PrintSnapshot(const std::string& label, Snapshot snapshot,
                   double elapsed_sec, uint64_t value_size) {
    if (snapshot.requests == 0 || elapsed_sec <= 0.0) {
        std::cout << label << " requests=0\n" << std::flush;
        return;
    }

    const auto min_latency = *std::min_element(snapshot.latency_ns.begin(),
                                               snapshot.latency_ns.end());
    const auto max_latency = *std::max_element(snapshot.latency_ns.begin(),
                                               snapshot.latency_ns.end());
    const double avg_us = static_cast<double>(snapshot.latency_sum_ns) /
                          static_cast<double>(snapshot.requests) / 1000.0;
    const double throughput = static_cast<double>(snapshot.keys) / elapsed_sec;
    const double bandwidth_gbps =
        static_cast<double>(snapshot.bytes) / elapsed_sec / 1e9;

    std::cout << label << " requests=" << snapshot.requests
              << " keys_per_request=" << FLAGS_embtable_request_keys
              << " request_keys=" << snapshot.keys
              << " value_size=" << value_size << "B"
              << " latency_us{avg=" << avg_us
              << ",min=" << static_cast<double>(min_latency) / 1000.0
              << ",max=" << static_cast<double>(max_latency) / 1000.0
              << ",p99=" << PercentileUs(snapshot.latency_ns, 0.99)
              << ",p999=" << PercentileUs(snapshot.latency_ns, 0.999) << "}"
              << " throughput=" << throughput << " keys/s"
              << " bandwidth=" << bandwidth_gbps << " GB/s"
              << " errors=" << snapshot.errors << "\n"
              << std::flush;
}

bool CheckedAdd(uint64_t lhs, uint64_t rhs, uint64_t& result) {
    if (rhs > std::numeric_limits<uint64_t>::max() - lhs) return false;
    result = lhs + rhs;
    return true;
}

bool CheckedMultiply(uint64_t lhs, uint64_t rhs, uint64_t& result) {
    if (lhs != 0 && rhs > std::numeric_limits<uint64_t>::max() / lhs) {
        return false;
    }
    result = lhs * rhs;
    return true;
}

embtable::EmbTableDummyClient::Options DummyOptions() {
    embtable::EmbTableDummyClient::Options options;
    options.rpcEndpoint = FLAGS_embtable_rpc_endpoint;
    options.tableName = FLAGS_embtable_table_name;
    options.sharedMemorySize =
        mooncake::string_to_byte_size(FLAGS_embtable_shared_memory_size);
    return options;
}

bool PrepareData(uint64_t value_size) {
    embtable::EmbTableDummyClient client(DummyOptions());
    auto status = client.Init();
    if (!status.IsOk()) {
        LOG(ERROR) << "Preparation client Init failed: " << status.msg();
        return false;
    }

    std::string value(value_size, '\0');
    for (size_t i = 0; i < value.size(); ++i) {
        value[i] = static_cast<char>((i * 31 + 17) & 0xff);
    }
    const uint64_t batch_size =
        std::min(FLAGS_embtable_insert_batch_size, FLAGS_embtable_num_keys);
    std::vector<uint64_t> keys;
    std::vector<embtable::StringView> values;
    keys.reserve(batch_size);
    values.reserve(batch_size);
    for (uint64_t offset = 0; offset < FLAGS_embtable_num_keys;
         offset += batch_size) {
        const uint64_t count =
            std::min(batch_size, FLAGS_embtable_num_keys - offset);
        keys.clear();
        values.clear();
        for (uint64_t i = 0; i < count; ++i) {
            uint64_t key = 0;
            if (!CheckedAdd(FLAGS_embtable_key_start, offset + i, key)) {
                LOG(ERROR) << "key range overflows uint64_t";
                return false;
            }
            keys.push_back(key);
            values.emplace_back(value.data(), value.size());
        }
        status = client.Insert(keys, values);
        if (!status.IsOk()) {
            LOG(ERROR) << "Preparation Insert failed: " << status.msg();
            return false;
        }
    }
    status = client.BuildIndex();
    if (!status.IsOk()) {
        LOG(ERROR) << "Preparation BuildIndex failed: " << status.msg();
        return false;
    }
    return true;
}

void SetError(std::atomic<bool>& failed, std::mutex& error_mutex,
              std::string& error_message, const std::string& message) {
    bool expected = false;
    if (failed.compare_exchange_strong(expected, true)) {
        std::lock_guard<std::mutex> lock(error_mutex);
        error_message = message;
    }
}

void RunWorker(uint64_t worker_id, uint64_t request_count, bool continuous,
               uint64_t duration_sec, std::atomic<uint64_t>& ready,
               std::atomic<bool>& go, std::atomic<bool>& failed,
               std::mutex& error_mutex, std::string& error_message,
               WorkerStats& stats, uint64_t value_size) {
    bool announced_ready = false;
    const auto announce_ready = [&]() {
        if (!announced_ready) {
            ready.fetch_add(1, std::memory_order_release);
            announced_ready = true;
        }
    };
    try {
        embtable::EmbTableDummyClient client(DummyOptions());
        auto status = client.Init();
        if (!status.IsOk()) {
            announce_ready();
            SetError(failed, error_mutex, error_message,
                     "worker " + std::to_string(worker_id) +
                         " Init failed: " + status.msg());
            return;
        }
        if (client.ValueSize() != value_size) {
            announce_ready();
            SetError(failed, error_mutex, error_message,
                     "worker " + std::to_string(worker_id) +
                         " observed an unexpected value size");
            return;
        }
        announce_ready();

        std::vector<uint64_t> keys(FLAGS_embtable_request_keys);
        std::vector<embtable::StringView> buffers;
        uint64_t iteration = 0;
        while (!go.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        const auto deadline = Clock::now() + std::chrono::seconds(duration_sec);
        while (!failed.load(std::memory_order_relaxed) &&
               (continuous ? Clock::now() < deadline
                           : iteration < request_count)) {
            const uint64_t first = (iteration * FLAGS_embtable_request_keys) %
                                   FLAGS_embtable_num_keys;
            for (uint64_t i = 0; i < FLAGS_embtable_request_keys; ++i) {
                const uint64_t offset = (first + i) % FLAGS_embtable_num_keys;
                if (!CheckedAdd(FLAGS_embtable_key_start, offset, keys[i])) {
                    SetError(failed, error_mutex, error_message,
                             "query key overflows uint64_t");
                    return;
                }
            }

            const auto started = Clock::now();
            status = client.Find(keys, buffers);
            const auto latency_ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    Clock::now() - started)
                    .count());
            stats.Record(keys.size(), value_size, latency_ns, status.IsOk());
            if (!status.IsOk()) {
                SetError(failed, error_mutex, error_message,
                         "worker " + std::to_string(worker_id) +
                             " Find failed: " + status.msg());
                return;
            }
            ++iteration;
        }
    } catch (const std::exception& e) {
        announce_ready();
        SetError(
            failed, error_mutex, error_message,
            "worker " + std::to_string(worker_id) + " exception: " + e.what());
    }
}

}  // namespace

int main(int argc, char* argv[]) {
    mooncake::ResourceTracker::getInstance();
    gflags::SetUsageMessage(
        "Benchmark EmbTableDummyClient Find performance in a cluster");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    const uint64_t shared_memory_size =
        mooncake::string_to_byte_size(FLAGS_embtable_shared_memory_size);
    if (FLAGS_embtable_rpc_endpoint.empty() ||
        FLAGS_embtable_table_name.empty() || shared_memory_size == 0 ||
        FLAGS_embtable_num_keys == 0 || FLAGS_embtable_request_keys == 0 ||
        FLAGS_embtable_request_keys > FLAGS_embtable_num_keys ||
        FLAGS_embtable_insert_batch_size == 0 || FLAGS_embtable_threads == 0) {
        LOG(ERROR) << "invalid benchmark arguments";
        return 1;
    }
    if (FLAGS_embtable_mode != "once" && FLAGS_embtable_mode != "continuous") {
        LOG(ERROR) << "--embtable_mode must be once or continuous";
        return 1;
    }
    if (FLAGS_embtable_mode == "once" && FLAGS_embtable_iterations == 0) {
        LOG(ERROR) << "--embtable_iterations must be > 0 in once mode";
        return 1;
    }
    if (FLAGS_embtable_mode == "continuous" &&
        (FLAGS_embtable_duration_sec == 0 ||
         FLAGS_embtable_report_interval_sec == 0)) {
        LOG(ERROR) << "continuous mode requires positive duration and interval";
        return 1;
    }

    uint64_t key_end = 0;
    if (!CheckedAdd(FLAGS_embtable_key_start, FLAGS_embtable_num_keys - 1,
                    key_end)) {
        LOG(ERROR) << "key range overflows uint64_t";
        return 1;
    }
    (void)key_end;

    auto probe =
        std::make_unique<embtable::EmbTableDummyClient>(DummyOptions());
    auto status = probe->Init();
    if (!status.IsOk()) {
        LOG(ERROR) << "probe client Init failed: " << status.msg();
        return 1;
    }
    const uint64_t value_size = probe->ValueSize();
    if (value_size == 0 || (FLAGS_embtable_value_size != 0 &&
                            FLAGS_embtable_value_size != value_size)) {
        LOG(ERROR) << "configured value size does not match server value size";
        return 1;
    }
    uint64_t find_buffer_size = 0;
    if (value_size == std::numeric_limits<uint64_t>::max() ||
        !CheckedMultiply(FLAGS_embtable_request_keys, value_size + 1,
                         find_buffer_size) ||
        find_buffer_size > shared_memory_size) {
        LOG(ERROR) << "shared memory is too small for one Find request";
        return 1;
    }
    if (FLAGS_embtable_prepare_data) {
        uint64_t insert_buffer_size = 0;
        const uint64_t insert_batch_size =
            std::min(FLAGS_embtable_insert_batch_size, FLAGS_embtable_num_keys);
        if (!CheckedMultiply(insert_batch_size, value_size,
                             insert_buffer_size) ||
            insert_buffer_size > shared_memory_size) {
            LOG(ERROR) << "shared memory is too small for one Insert batch";
            return 1;
        }
    }
    if (FLAGS_embtable_prepare_data) {
        probe.reset();
        if (!PrepareData(value_size)) return 1;
    }

    std::vector<std::unique_ptr<WorkerStats>> stats;
    stats.reserve(FLAGS_embtable_threads);
    for (uint64_t i = 0; i < FLAGS_embtable_threads; ++i) {
        stats.push_back(std::make_unique<WorkerStats>());
    }

    const bool continuous = FLAGS_embtable_mode == "continuous";
    std::atomic<uint64_t> ready{0};
    std::atomic<bool> go{false};
    std::atomic<bool> failed{false};
    std::mutex error_mutex;
    std::string error_message;
    std::vector<std::thread> workers;
    workers.reserve(FLAGS_embtable_threads);
    for (uint64_t worker_id = 0; worker_id < FLAGS_embtable_threads;
         ++worker_id) {
        uint64_t request_count =
            FLAGS_embtable_iterations / FLAGS_embtable_threads;
        if (worker_id < FLAGS_embtable_iterations % FLAGS_embtable_threads) {
            ++request_count;
        }
        workers.emplace_back(RunWorker, worker_id, request_count, continuous,
                             FLAGS_embtable_duration_sec, std::ref(ready),
                             std::ref(go), std::ref(failed),
                             std::ref(error_mutex), std::ref(error_message),
                             std::ref(*stats[worker_id]), value_size);
    }
    while (ready.load(std::memory_order_acquire) < FLAGS_embtable_threads &&
           !failed.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (failed.load(std::memory_order_relaxed)) {
        go.store(true, std::memory_order_release);
        for (auto& worker : workers) worker.join();
        std::lock_guard<std::mutex> lock(error_mutex);
        LOG(ERROR) << error_message;
        return 1;
    }
    const auto start = Clock::now();
    go.store(true, std::memory_order_release);

    if (continuous) {
        const auto deadline =
            start + std::chrono::seconds(FLAGS_embtable_duration_sec);
        Snapshot total;
        while (!failed.load(std::memory_order_relaxed) &&
               Clock::now() < deadline) {
            std::this_thread::sleep_for(
                std::chrono::seconds(FLAGS_embtable_report_interval_sec));
            Snapshot interval;
            for (auto& worker_stats : stats)
                Merge(interval, worker_stats->Take());
            Snapshot report = interval;
            Merge(total, std::move(interval));
            PrintSnapshot(
                "interval", std::move(report),
                static_cast<double>(FLAGS_embtable_report_interval_sec),
                value_size);
        }
        for (auto& worker : workers) worker.join();
        for (auto& worker_stats : stats) Merge(total, worker_stats->Take());
        const double elapsed =
            std::chrono::duration<double>(Clock::now() - start).count();
        PrintSnapshot("total", std::move(total), elapsed, value_size);
    } else {
        for (auto& worker : workers) worker.join();
        Snapshot total;
        for (auto& worker_stats : stats) Merge(total, worker_stats->Take());
        const double elapsed =
            std::chrono::duration<double>(Clock::now() - start).count();
        PrintSnapshot("total", std::move(total), elapsed, value_size);
    }

    if (failed.load()) {
        std::lock_guard<std::mutex> lock(error_mutex);
        LOG(ERROR) << error_message;
        return 1;
    }
    return 0;
}
