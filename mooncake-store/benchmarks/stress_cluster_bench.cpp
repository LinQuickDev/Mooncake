#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <latch>
#include <memory>
#include <numeric>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "real_client.h"

namespace {
constexpr size_t KB = 1024;
constexpr size_t MB = 1024 * KB;
constexpr size_t GB = 1024 * MB;
}  // namespace

DEFINE_string(local_hostname, "localhost",
              "Local hostname (with optional port, e.g. node1:12345)");
DEFINE_string(metadata_server, "http://127.0.0.1:8080/metadata",
              "Metadata server URL");
DEFINE_string(master_server, "127.0.0.1:50051", "Master server address");
DEFINE_string(protocol, "tcp", "Transport protocol: tcp, rdma, ub");
DEFINE_string(device_name, "", "RDMA/UB device name (comma-separated)");
DEFINE_uint64(global_segment_size, 4 * GB, "Global segment size in bytes");
DEFINE_uint64(local_buffer_size, 512 * MB, "Local buffer size in bytes");
DEFINE_bool(enable_ssd_offload, false, "Enable SSD offload on this client");
DEFINE_string(ssd_offload_path, "", "SSD offload directory path");

DEFINE_string(scenario, "local_memory",
              "Benchmark scenario: local_memory, remote_memory, local_disk, "
              "remote_disk");
DEFINE_string(role, "writer",
              "Node role: writer (prefill data) or reader (benchmark reads)");
DEFINE_uint64(value_size, 4 * MB, "Size of each value in bytes");
DEFINE_uint64(num_keys, 100, "Number of keys to write/read");
DEFINE_uint64(batch_size, 1, "Batch size for put/get operations");
DEFINE_uint64(num_threads, 1, "Number of concurrent reader threads");
DEFINE_uint64(warmup_keys, 5, "Number of warmup keys (not counted in stats)");
DEFINE_uint64(wait_seconds, 5,
              "Seconds to wait before reading (for remote scenarios)");
DEFINE_bool(verify, true, "Verify data integrity after read");
DEFINE_uint64(replica_num, 1, "Number of replicas for each object");

using Clock = std::chrono::steady_clock;
using Nanos = std::chrono::nanoseconds;

inline int64_t ElapsedNanos(Clock::time_point t0, Clock::time_point t1) {
    return std::chrono::duration_cast<Nanos>(t1 - t0).count();
}

inline double NanosToUs(int64_t ns) { return static_cast<double>(ns) / 1000.0; }
inline double NanosToMs(int64_t ns) { return static_cast<double>(ns) / 1000000.0; }
inline double NanosToSec(int64_t ns) { return static_cast<double>(ns) / 1e9; }

struct ThreadResult {
    std::vector<int64_t> latencies_ns;
    size_t total_bytes = 0;
    size_t total_ops = 0;
    size_t failed_ops = 0;
};

class BenchmarkStats {
   public:
    void InitThreads(size_t n, size_t expected_per_thread) {
        thread_results_.resize(n);
        expected_per_thread_ = expected_per_thread;
    }

    ThreadResult& GetThreadResult(size_t tid) { return thread_results_[tid]; }

    void StartTimer() { start_ = Clock::now(); }
    void StopTimer() { end_ = Clock::now(); }

    double WallSeconds() const {
        return NanosToSec(ElapsedNanos(start_, end_));
    }

    void Finalize() {
        merged_latencies_ns_.clear();
        total_bytes_ = 0;
        total_ops_ = 0;
        total_failed_ = 0;

        for (auto& tr : thread_results_) {
            merged_latencies_ns_.insert(merged_latencies_ns_.end(),
                                        tr.latencies_ns.begin(),
                                        tr.latencies_ns.end());
            total_bytes_ += tr.total_bytes;
            total_ops_ += tr.total_ops;
            total_failed_ += tr.failed_ops;
        }
        std::sort(merged_latencies_ns_.begin(), merged_latencies_ns_.end());
    }

    double PercentileUs(double p) const {
        if (merged_latencies_ns_.empty()) return 0.0;
        double rank = (p / 100.0) * (merged_latencies_ns_.size() - 1);
        size_t lo = static_cast<size_t>(rank);
        size_t hi = std::min(lo + 1, merged_latencies_ns_.size() - 1);
        double frac = rank - lo;
        int64_t ns_val = static_cast<int64_t>(
            merged_latencies_ns_[lo] * (1.0 - frac) +
            merged_latencies_ns_[hi] * frac);
        return NanosToUs(ns_val);
    }

    double MeanLatencyUs() const {
        if (merged_latencies_ns_.empty()) return 0.0;
        int64_t sum = std::accumulate(merged_latencies_ns_.begin(),
                                      merged_latencies_ns_.end(), int64_t(0));
        return NanosToUs(sum / static_cast<int64_t>(merged_latencies_ns_.size()));
    }

    double ThroughputMBps() const {
        double wall = WallSeconds();
        return (wall > 0) ? (static_cast<double>(total_bytes_) / MB) / wall : 0;
    }

    double OpsPerSec() const {
        double wall = WallSeconds();
        return (wall > 0) ? static_cast<double>(total_ops_) / wall : 0;
    }

    void Print(const std::string& title) const {
        std::cout << "\n";
        std::cout << "========================================"
                  << "========================================\n";
        std::cout << "  " << title << "\n";
        std::cout << "========================================"
                  << "========================================\n";
        std::cout << std::fixed << std::setprecision(2);

        double wall = WallSeconds();
        std::cout << "  Wall time:        " << wall << " s\n";
        std::cout << "  Total ops:        " << total_ops_
                  << " (failed: " << total_failed_ << ")\n";
        std::cout << "  Total data:       " << FormatBytes(total_bytes_)
                  << "\n";
        std::cout << "  Throughput:       " << ThroughputMBps() << " MB/s";
        if (ThroughputMBps() > 1024) {
            std::cout << " (" << ThroughputMBps() / 1024 << " GB/s)";
        }
        std::cout << "\n";
        std::cout << "  Ops/sec:          " << OpsPerSec() << "\n";

        if (!merged_latencies_ns_.empty()) {
            size_t n = merged_latencies_ns_.size();
            std::cout << "\n  Latency (us)      [n=" << n << "]\n";
            std::cout << "    Min:   " << std::setw(12)
                      << NanosToUs(merged_latencies_ns_.front()) << "\n";
            std::cout << "    Mean:  " << std::setw(12) << MeanLatencyUs()
                      << "\n";
            std::cout << "    P50:   " << std::setw(12) << PercentileUs(50)
                      << "\n";
            std::cout << "    P90:   " << std::setw(12) << PercentileUs(90)
                      << "\n";
            std::cout << "    P99:   " << std::setw(12) << PercentileUs(99);
            if (n < 100) std::cout << "  (n<100)";
            std::cout << "\n";
            std::cout << "    P999:  " << std::setw(12) << PercentileUs(99.9);
            if (n < 1000) std::cout << "  (n<1000)";
            std::cout << "\n";
            std::cout << "    Max:   " << std::setw(12)
                      << NanosToUs(merged_latencies_ns_.back()) << "\n";
        }
        std::cout << "========================================"
                  << "========================================\n\n";
    }

    size_t total_bytes() const { return total_bytes_; }
    size_t total_ops() const { return total_ops_; }
    size_t total_failed() const { return total_failed_; }

   private:
    static std::string FormatBytes(size_t bytes) {
        if (bytes == 0) return "0 B";
        const char* units[] = {"B", "KB", "MB", "GB", "TB"};
        int i = static_cast<int>(std::floor(std::log2(bytes) / 10));
        if (i > 4) i = 4;
        double val = static_cast<double>(bytes) / std::pow(1024, i);
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2) << val << " " << units[i];
        return oss.str();
    }

    std::vector<ThreadResult> thread_results_;
    std::vector<int64_t> merged_latencies_ns_;
    size_t total_bytes_ = 0;
    size_t total_ops_ = 0;
    size_t total_failed_ = 0;
    size_t expected_per_thread_ = 0;
    Clock::time_point start_;
    Clock::time_point end_;
};

class StressBenchmark {
   public:
    StressBenchmark()
        : client_(mooncake::RealClient::create()),
          buffer_(nullptr),
          buffer_size_(0) {}

    ~StressBenchmark() {
        if (client_) {
            if (buffer_) {
                client_->unregister_buffer(buffer_);
                buffer_ = nullptr;
            }
            client_->tearDownAll();
        }
    }

    int Setup() {
        int ret = client_->setup_real(
            FLAGS_local_hostname, FLAGS_metadata_server,
            FLAGS_global_segment_size, FLAGS_local_buffer_size, FLAGS_protocol,
            FLAGS_device_name, FLAGS_master_server, nullptr, "",
            FLAGS_enable_ssd_offload, FLAGS_ssd_offload_path);
        if (ret != 0) {
            LOG(ERROR) << "RealClient setup_real failed, ret=" << ret;
            return ret;
        }
        LOG(INFO) << "RealClient setup succeeded";

        buffer_size_ = FLAGS_batch_size * FLAGS_value_size;
        int err = posix_memalign(reinterpret_cast<void**>(&buffer_), 4096,
                                 buffer_size_);
        if (err != 0 || !buffer_) {
            LOG(ERROR) << "Failed to allocate buffer of " << buffer_size_
                       << " bytes";
            return -1;
        }
        std::memset(buffer_, 0, buffer_size_);

        ret = client_->register_buffer(buffer_, buffer_size_);
        if (ret != 0) {
            LOG(ERROR) << "register_buffer failed, ret=" << ret;
            return ret;
        }
        LOG(INFO) << "Registered buffer of " << buffer_size_ / MB << " MB";
        return 0;
    }

    int RunWriter() {
        LOG(INFO) << "=== WRITER MODE ===";
        LOG(INFO) << "Writing " << FLAGS_num_keys << " keys, each "
                  << FLAGS_value_size / MB << " MB";

        mooncake::ReplicateConfig config;
        config.replica_num = FLAGS_replica_num;

        size_t written = 0;
        size_t failed = 0;

        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
            std::string key = MakeKey(i);
            FillBuffer(i);

            auto t0 = Clock::now();
            int ret = client_->put_from(key, buffer_, FLAGS_value_size, config);
            auto t1 = Clock::now();

            if (ret != 0) {
                LOG(ERROR) << "put_from failed for key=" << key
                           << " ret=" << ret;
                ++failed;
                continue;
            }
            ++written;

            if ((i + 1) % 10 == 0 || i == FLAGS_num_keys - 1) {
                double elapsed_us = NanosToUs(ElapsedNanos(t0, t1));
                LOG(INFO) << "  Written " << (i + 1) << "/" << FLAGS_num_keys
                          << " last_latency=" << elapsed_us << " us";
            }
        }

        LOG(INFO) << "Write complete: " << written << " succeeded, " << failed
                  << " failed";
        LOG(INFO) << "Waiting " << FLAGS_wait_seconds
                  << " seconds for reader to connect...";
        std::this_thread::sleep_for(std::chrono::seconds(FLAGS_wait_seconds));

        return (failed > 0) ? -1 : 0;
    }

    int RunReader() {
        LOG(INFO) << "=== READER MODE ===";
        LOG(INFO) << "Scenario: " << FLAGS_scenario;
        LOG(INFO) << "Reading " << FLAGS_num_keys << " keys with "
                  << FLAGS_num_threads
                  << " threads, batch_size=" << FLAGS_batch_size;

        if (FLAGS_scenario == "remote_memory" ||
            FLAGS_scenario == "remote_disk") {
            LOG(INFO) << "Waiting " << FLAGS_wait_seconds
                      << " seconds for writer to finish prefill...";
            std::this_thread::sleep_for(
                std::chrono::seconds(FLAGS_wait_seconds));
        }

        int warmup_ret = DoWarmup();
        if (warmup_ret != 0) {
            LOG(WARNING) << "Warmup had errors, continuing anyway";
        }

        BenchmarkStats stats;
        stats.InitThreads(FLAGS_num_threads,
                          FLAGS_num_keys / FLAGS_num_threads);
        stats.StartTimer();

        std::latch start_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::latch done_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::vector<std::thread> threads;

        size_t keys_per_thread = FLAGS_num_keys / FLAGS_num_threads;
        size_t remainder = FLAGS_num_keys % FLAGS_num_threads;

        for (size_t t = 0; t < FLAGS_num_threads; ++t) {
            size_t my_keys = keys_per_thread + (t < remainder ? 1 : 0);
            size_t key_offset = t * keys_per_thread + std::min(t, remainder);

            threads.emplace_back([&, t, my_keys, key_offset]() {
                ReadWorker(t, my_keys, key_offset, stats, start_latch,
                           done_latch);
            });
        }

        done_latch.wait();
        stats.StopTimer();

        for (auto& th : threads) {
            th.join();
        }

        stats.Finalize();

        std::string title = "READ BENCHMARK [" + FLAGS_scenario + "]";
        stats.Print(title);

        if (FLAGS_verify) {
            int v = VerifyData();
            if (v != 0) {
                LOG(ERROR) << "Data verification FAILED";
            } else {
                LOG(INFO) << "Data verification PASSED";
            }
        }

        return 0;
    }

    int RunLocalMemory() {
        LOG(INFO) << "=== LOCAL MEMORY BENCHMARK ===";

        mooncake::ReplicateConfig config;
        config.replica_num = FLAGS_replica_num;

        LOG(INFO) << "Phase 1: Writing " << FLAGS_num_keys << " keys...";
        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
            std::string key = MakeKey(i);
            FillBuffer(i);
            int ret = client_->put_from(key, buffer_, FLAGS_value_size, config);
            if (ret != 0) {
                LOG(ERROR) << "put_from failed for key=" << key;
                return ret;
            }
            if ((i + 1) % 50 == 0) {
                LOG(INFO) << "  Written " << (i + 1) << "/" << FLAGS_num_keys;
            }
        }
        LOG(INFO) << "Write phase complete";

        int warmup_ret = DoWarmup();
        if (warmup_ret != 0) {
            LOG(WARNING) << "Warmup had errors, continuing anyway";
        }

        LOG(INFO) << "Phase 2: Concurrent reads with " << FLAGS_num_threads
                  << " threads";

        BenchmarkStats stats;
        stats.InitThreads(FLAGS_num_threads,
                          FLAGS_num_keys / FLAGS_num_threads);
        stats.StartTimer();

        std::latch start_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::latch done_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::vector<std::thread> threads;

        size_t keys_per_thread = FLAGS_num_keys / FLAGS_num_threads;
        size_t remainder = FLAGS_num_keys % FLAGS_num_threads;

        for (size_t t = 0; t < FLAGS_num_threads; ++t) {
            size_t my_keys = keys_per_thread + (t < remainder ? 1 : 0);
            size_t key_offset = t * keys_per_thread + std::min(t, remainder);

            threads.emplace_back([&, t, my_keys, key_offset]() {
                ReadWorker(t, my_keys, key_offset, stats, start_latch,
                           done_latch);
            });
        }

        done_latch.wait();
        stats.StopTimer();

        for (auto& th : threads) {
            th.join();
        }

        stats.Finalize();
        stats.Print("LOCAL MEMORY READ BENCHMARK");

        if (FLAGS_verify) {
            int v = VerifyData();
            LOG_IF(INFO, v == 0) << "Data verification PASSED";
            LOG_IF(ERROR, v != 0) << "Data verification FAILED";
        }

        return 0;
    }

    int RunLocalDisk() {
        LOG(INFO) << "=== LOCAL DISK BENCHMARK ===";
        LOG(INFO) << "NOTE: Disk reads require Master with enable_offload=true "
                  << "and client with enable_ssd_offload=true";

        mooncake::ReplicateConfig config;
        config.replica_num = FLAGS_replica_num;

        LOG(INFO) << "Phase 1: Writing " << FLAGS_num_keys
                  << " keys (data may be offloaded to SSD)...";
        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
            std::string key = MakeKey(i);
            FillBuffer(i);
            int ret = client_->put_from(key, buffer_, FLAGS_value_size, config);
            if (ret != 0) {
                LOG(ERROR) << "put_from failed for key=" << key;
                return ret;
            }
            if ((i + 1) % 50 == 0) {
                LOG(INFO) << "  Written " << (i + 1) << "/" << FLAGS_num_keys;
            }
        }
        LOG(INFO) << "Write phase complete";

        LOG(INFO) << "Waiting " << FLAGS_wait_seconds
                  << " seconds for offload/eviction to complete...";
        std::this_thread::sleep_for(std::chrono::seconds(FLAGS_wait_seconds));

        int warmup_ret = DoWarmup();
        if (warmup_ret != 0) {
            LOG(WARNING) << "Warmup had errors, continuing anyway";
        }

        LOG(INFO) << "Phase 2: Concurrent disk reads with " << FLAGS_num_threads
                  << " threads";

        BenchmarkStats stats;
        stats.InitThreads(FLAGS_num_threads,
                          FLAGS_num_keys / FLAGS_num_threads);
        stats.StartTimer();

        std::latch start_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::latch done_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::vector<std::thread> threads;

        size_t keys_per_thread = FLAGS_num_keys / FLAGS_num_threads;
        size_t remainder = FLAGS_num_keys % FLAGS_num_threads;

        for (size_t t = 0; t < FLAGS_num_threads; ++t) {
            size_t my_keys = keys_per_thread + (t < remainder ? 1 : 0);
            size_t key_offset = t * keys_per_thread + std::min(t, remainder);

            threads.emplace_back([&, t, my_keys, key_offset]() {
                ReadWorker(t, my_keys, key_offset, stats, start_latch,
                           done_latch);
            });
        }

        done_latch.wait();
        stats.StopTimer();

        for (auto& th : threads) {
            th.join();
        }

        stats.Finalize();
        stats.Print("LOCAL DISK READ BENCHMARK");

        if (FLAGS_verify) {
            int v = VerifyData();
            LOG_IF(INFO, v == 0) << "Data verification PASSED";
            LOG_IF(ERROR, v != 0) << "Data verification FAILED";
        }

        return 0;
    }

    int Run() {
        if (FLAGS_scenario == "local_memory") {
            return RunLocalMemory();
        } else if (FLAGS_scenario == "local_disk") {
            return RunLocalDisk();
        } else if (FLAGS_scenario == "remote_memory" ||
                   FLAGS_scenario == "remote_disk") {
            if (FLAGS_role == "writer") {
                return RunWriter();
            } else {
                return RunReader();
            }
        } else {
            LOG(ERROR) << "Unknown scenario: " << FLAGS_scenario;
            return -1;
        }
    }

   private:
    static std::string MakeKey(size_t idx) {
        return "bench_key_" + std::to_string(idx);
    }

    void FillBuffer(size_t seed) {
        uint64_t* ptr = reinterpret_cast<uint64_t*>(buffer_);
        size_t num_words = FLAGS_value_size / sizeof(uint64_t);
        uint64_t pattern = static_cast<uint64_t>(seed) * 0x9E3779B97F4A7C15ULL;
        for (size_t w = 0; w < num_words; ++w) {
            pattern = (pattern ^ (pattern >> 30)) * 0xBF58476D1CE4E5B9ULL;
            pattern = (pattern ^ (pattern >> 27)) * 0x94D049BB133111EBULL;
            ptr[w] = pattern ^ (pattern >> 31);
        }
    }

    bool CheckBuffer(size_t seed, const void* data, size_t size) const {
        const uint64_t* ptr = reinterpret_cast<const uint64_t*>(data);
        size_t num_words = size / sizeof(uint64_t);
        uint64_t pattern = static_cast<uint64_t>(seed) * 0x9E3779B97F4A7C15ULL;
        for (size_t w = 0; w < num_words; ++w) {
            pattern = (pattern ^ (pattern >> 30)) * 0xBF58476D1CE4E5B9ULL;
            pattern = (pattern ^ (pattern >> 27)) * 0x94D049BB133111EBULL;
            uint64_t expected = pattern ^ (pattern >> 31);
            if (ptr[w] != expected) {
                LOG(ERROR) << "Checksum mismatch at word " << w
                           << " for seed=" << seed << " expected=" << std::hex
                           << expected << " got=" << ptr[w] << std::dec;
                return false;
            }
        }
        return true;
    }

    int DoWarmup() {
        if (FLAGS_warmup_keys == 0) return 0;
        LOG(INFO) << "Warmup: reading " << FLAGS_warmup_keys << " keys...";

        size_t warmup_end = std::min(static_cast<size_t>(FLAGS_warmup_keys),
                                     static_cast<size_t>(FLAGS_num_keys));
        for (size_t i = 0; i < warmup_end; ++i) {
            std::string key = MakeKey(i);
            int64_t ret = client_->get_into(key, buffer_, FLAGS_value_size);
            if (ret < 0) {
                LOG(WARNING) << "Warmup get_into failed for key=" << key
                             << " ret=" << ret;
            }
        }
        LOG(INFO) << "Warmup complete";
        return 0;
    }

    void ReadWorker(size_t tid, size_t my_keys, size_t key_offset,
                    BenchmarkStats& stats, std::latch& start_latch,
                    std::latch& done_latch) {
        ThreadResult& result = stats.GetThreadResult(tid);
        result.latencies_ns.reserve(my_keys);

        start_latch.arrive_and_wait();

        size_t ops = 0;
        size_t failed = 0;
        size_t bytes = 0;

        if (FLAGS_batch_size <= 1) {
            for (size_t i = 0; i < my_keys; ++i) {
                size_t key_idx = key_offset + i;
                std::string key = MakeKey(key_idx);

                auto t0 = Clock::now();
                int64_t ret = client_->get_into(key, buffer_, FLAGS_value_size);
                auto t1 = Clock::now();

                int64_t lat_ns = ElapsedNanos(t0, t1);

                if (ret < 0) {
                    ++failed;
                    LOG_EVERY_N(ERROR, 100)
                        << "get_into failed key=" << key << " ret=" << ret;
                } else {
                    bytes += static_cast<size_t>(ret);
                }
                result.latencies_ns.push_back(lat_ns);
                ++ops;
            }
        } else {
            std::vector<std::string> keys;
            std::vector<void*> bufs;
            std::vector<size_t> sizes;
            keys.reserve(FLAGS_batch_size);
            bufs.reserve(FLAGS_batch_size);
            sizes.reserve(FLAGS_batch_size);

            size_t i = 0;
            while (i < my_keys) {
                keys.clear();
                bufs.clear();
                sizes.clear();

                size_t batch_end = std::min(i + FLAGS_batch_size, my_keys);
                for (size_t j = i; j < batch_end; ++j) {
                    size_t key_idx = key_offset + j;
                    keys.push_back(MakeKey(key_idx));
                    bufs.push_back(buffer_);
                    sizes.push_back(FLAGS_value_size);
                }

                auto t0 = Clock::now();
                auto results = client_->batch_get_into(keys, bufs, sizes);
                auto t1 = Clock::now();

                int64_t lat_ns = ElapsedNanos(t0, t1);
                result.latencies_ns.push_back(lat_ns);

                for (size_t k = 0; k < results.size(); ++k) {
                    if (results[k] < 0) {
                        ++failed;
                    } else {
                        bytes += static_cast<size_t>(results[k]);
                    }
                    ++ops;
                }

                i = batch_end;
            }
        }

        result.total_bytes = bytes;
        result.total_ops = ops;
        result.failed_ops = failed;

        done_latch.arrive_and_wait();
    }

    int VerifyData() {
        LOG(INFO) << "Verifying data integrity for " << FLAGS_num_keys
                  << " keys...";
        int errors = 0;

        std::vector<char> read_buf(FLAGS_value_size);
        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
            std::string key = MakeKey(i);
            int64_t ret =
                client_->get_into(key, read_buf.data(), FLAGS_value_size);
            if (ret < 0) {
                LOG(ERROR) << "Verify: get_into failed for key=" << key;
                ++errors;
                continue;
            }
            if (!CheckBuffer(i, read_buf.data(), static_cast<size_t>(ret))) {
                LOG(ERROR) << "Verify: data mismatch for key=" << key;
                ++errors;
            }
        }

        LOG(INFO) << "Verification complete: " << errors << " errors out of "
                  << FLAGS_num_keys << " keys";
        return errors > 0 ? -1 : 0;
    }

    std::shared_ptr<mooncake::RealClient> client_;
    char* buffer_;
    size_t buffer_size_;
};

int main(int argc, char* argv[]) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    FLAGS_logtostderr = true;

    LOG(INFO) << "Mooncake Stress Cluster Benchmark";
    LOG(INFO) << "  Scenario:       " << FLAGS_scenario;
    LOG(INFO) << "  Protocol:       " << FLAGS_protocol;
    LOG(INFO) << "  Value size:     " << FLAGS_value_size / MB << " MB";
    LOG(INFO) << "  Num keys:       " << FLAGS_num_keys;
    LOG(INFO) << "  Batch size:     " << FLAGS_batch_size;
    LOG(INFO) << "  Num threads:    " << FLAGS_num_threads;
    LOG(INFO) << "  SSD offload:    "
              << (FLAGS_enable_ssd_offload ? "yes" : "no");

    StressBenchmark bench;
    int ret = bench.Setup();
    if (ret != 0) {
        LOG(ERROR) << "Benchmark setup failed";
        return ret;
    }

    ret = bench.Run();
    return ret;
}
