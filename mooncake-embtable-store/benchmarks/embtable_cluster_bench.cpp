#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
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
DEFINE_bool(embtable_create_table_if_missing, true,
            "Create the benchmark table when GetInfo returns not found");
DEFINE_uint32(embtable_num_buckets, 16,
              "Bucket count used when creating a missing table");
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

std::string MakeBenchmarkValue(uint64_t value_size) {
    std::string value(value_size, '\0');
    for (size_t i = 0; i < value.size(); ++i) {
        value[i] = static_cast<char>((i * 31 + 17) & 0xff);
    }
    return value;
}

embtable::Status ProbePreparedData(embtable::EmbTableDummyClient& client,
                                   uint64_t value_size, bool& ready) {
    ready = false;
    std::vector<uint64_t> offsets = {0, FLAGS_embtable_num_keys / 2,
                                     FLAGS_embtable_num_keys - 1};
    std::sort(offsets.begin(), offsets.end());
    offsets.erase(std::unique(offsets.begin(), offsets.end()), offsets.end());

    std::vector<uint64_t> keys;
    keys.reserve(offsets.size());
    for (const uint64_t offset : offsets) {
        uint64_t key = 0;
        if (!CheckedAdd(FLAGS_embtable_key_start, offset, key)) {
            return embtable::Status::Error(embtable::ErrorCode::kOutOfRange,
                                           "probe key range overflows");
        }
        keys.push_back(key);
    }

    std::vector<embtable::StringView> buffers;
    auto status = client.Find(keys, buffers);
    if (!status.IsOk()) {
        if (status.code() == static_cast<int>(embtable::ErrorCode::kNotFound)) {
            return embtable::Status::OK();
        }
        return status;
    }
    if (buffers.size() != keys.size()) {
        return embtable::Status::Error(
            embtable::ErrorCode::kInternal,
            "prepared-data probe returned an unexpected result count");
    }

    const std::string expected = MakeBenchmarkValue(value_size);
    for (const auto& buffer : buffers) {
        if (buffer.size() != expected.size() ||
            std::memcmp(buffer.data(), expected.data(), expected.size()) != 0) {
            return embtable::Status::OK();
        }
    }
    ready = true;
    return embtable::Status::OK();
}

bool PrepareData(uint64_t value_size) {
    embtable::EmbTableDummyClient client(DummyOptions());
    auto status = client.Init();
    if (!status.IsOk()) {
        LOG(ERROR) << "Preparation client Init failed: " << status.msg();
        return false;
    }

    const std::string value = MakeBenchmarkValue(value_size);
    const uint64_t batch_size =
        std::min(FLAGS_embtable_insert_batch_size, FLAGS_embtable_num_keys);
    LOG(INFO) << "Preparing EmbTable data: table=" << FLAGS_embtable_table_name
              << ", total_keys=" << FLAGS_embtable_num_keys
              << ", batch_size=" << batch_size << ", value_size=" << value_size;
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

        // Preparation can be resumed after an earlier run failed while
        // flushing another bucket. Discover keys already present in this
        // batch and submit only the missing keys; ShareMap intentionally
        // rejects duplicate keys rather than silently overwriting them.
        std::vector<embtable::StringView> existing;
        status = client.Find(keys, existing);
        if (!status.IsOk() || existing.size() != keys.size()) {
            LOG(ERROR) << "Preparation existing-data probe failed: "
                       << (status.IsOk()
                               ? "unexpected result count"
                               : status.msg());
            return false;
        }
        std::vector<uint64_t> missing_keys;
        std::vector<embtable::StringView> missing_values;
        missing_keys.reserve(keys.size());
        missing_values.reserve(values.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            // valueSize is validated as non-zero, so an empty StringView is
            // an unambiguous miss and does not depend on data()'s empty-view
            // representation.
            if (existing[i].empty()) {
                missing_keys.push_back(keys[i]);
                missing_values.push_back(values[i]);
            }
        }
        if (missing_keys.empty()) {
            LOG(INFO) << "Insert batch already present; skipping: table="
                      << FLAGS_embtable_table_name << ", batch_keys="
                      << count;
            continue;
        }

        status = client.Insert(missing_keys, missing_values);
        if (!status.IsOk()) {
            if (status.code() ==
                static_cast<int>(embtable::ErrorCode::kIndexBuilt)) {
                LOG(ERROR) << "The table is already read-only but benchmark "
                              "sentinel data is missing; use a new table name "
                              "or recreate the table before preparing data";
            }
            LOG(ERROR) << "Preparation Insert failed: " << status.msg();
            return false;
        }
        LOG(INFO) << "Insert batch completed: table="
                  << FLAGS_embtable_table_name << ", batch_keys="
                  << missing_keys.size()
                  << ", inserted=" << (offset + count) << "/"
                  << FLAGS_embtable_num_keys;
    }
    LOG(INFO) << "All benchmark data inserted, building index for table="
              << FLAGS_embtable_table_name;
    status = client.BuildIndex();
    if (!status.IsOk()) {
        LOG(ERROR) << "Preparation BuildIndex failed: " << status.msg();
        return false;
    }
    LOG(INFO) << "Benchmark data preparation completed: table="
              << FLAGS_embtable_table_name;
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
    if (!status.IsOk() &&
        status.code() == static_cast<int>(embtable::ErrorCode::kNotFound) &&
        FLAGS_embtable_create_table_if_missing) {
        if (FLAGS_embtable_value_size == 0 || FLAGS_embtable_num_buckets == 0) {
            LOG(ERROR) << "creating a missing table requires "
                          "--embtable_value_size and --embtable_num_buckets "
                          "to be greater than zero";
            return 1;
        }

        auto admin_options = DummyOptions();
        admin_options.tableName.clear();
        embtable::EmbTableDummyClient admin_client(std::move(admin_options));
        status = admin_client.Init();
        if (!status.IsOk()) {
            LOG(ERROR) << "admin client Init failed: " << status.msg();
            return 1;
        }
        status = admin_client.CreateTable(FLAGS_embtable_table_name,
                                          FLAGS_embtable_num_buckets,
                                          FLAGS_embtable_value_size);
        if (!status.IsOk() &&
            status.code() !=
                static_cast<int>(embtable::ErrorCode::kAlreadyExists)) {
            LOG(ERROR) << "CreateTable failed: " << status.msg();
            return 1;
        }
        LOG(INFO) << "Benchmark table is ready: " << FLAGS_embtable_table_name
                  << ", buckets=" << FLAGS_embtable_num_buckets
                  << ", value_size=" << FLAGS_embtable_value_size;

        probe = std::make_unique<embtable::EmbTableDummyClient>(DummyOptions());
        status = probe->Init();
    }
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
    bool data_ready = false;
    status = ProbePreparedData(*probe, value_size, data_ready);
    if (!status.IsOk()) {
        LOG(ERROR) << "failed to probe benchmark data: " << status.msg();
        return 1;
    }

    if (!data_ready && FLAGS_embtable_prepare_data) {
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

    if (data_ready) {
        LOG(INFO) << "Benchmark data already exists; skipping Insert for table="
                  << FLAGS_embtable_table_name;
        status = probe->BuildIndex();
        if (!status.IsOk() &&
            status.code() !=
                static_cast<int>(embtable::ErrorCode::kIndexBuilt)) {
            LOG(ERROR) << "BuildIndex readiness check failed: " << status.msg();
            return 1;
        }
        if (status.IsOk()) {
            LOG(INFO) << "Existing data was not indexed; BuildIndex completed";
        } else {
            LOG(INFO) << "Existing table index is already built";
        }
    } else if (!FLAGS_embtable_prepare_data) {
        LOG(ERROR) << "benchmark data is missing or incompatible and "
                      "--embtable_prepare_data=false";
        return 1;
    } else {
        LOG(INFO) << "Benchmark data is missing; starting Insert preparation";
        probe.reset();
        if (!PrepareData(value_size)) return 1;

        auto validation_client =
            std::make_unique<embtable::EmbTableDummyClient>(DummyOptions());
        status = validation_client->Init();
        if (!status.IsOk()) {
            LOG(ERROR) << "validation client Init failed: " << status.msg();
            return 1;
        }
        data_ready = false;
        status = ProbePreparedData(*validation_client, value_size, data_ready);
        if (!status.IsOk() || !data_ready) {
            LOG(ERROR) << "prepared data validation failed: "
                       << (status.IsOk() ? "sentinel keys are missing"
                                         : status.msg());
            return 1;
        }
        LOG(INFO) << "Prepared data validation succeeded";
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
