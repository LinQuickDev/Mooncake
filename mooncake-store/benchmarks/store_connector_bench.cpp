// Store Connector Benchmark: 专测 vLLM 调用路径（batch_put_from_multi_buffers
// / batchIsExist / batch_get_into_multi_buffers），触发新增的 SpDiag 打点。
// 参考方案文档 vllm_spdiag_logging_plan.md 第 6 章。

#include <algorithm>
#include <atomic>
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
#include "mooncake_logging.h"
#include "real_client.h"

namespace {
constexpr size_t KB = 1024;
constexpr size_t MB = 1024 * KB;
constexpr size_t GB = 1024 * MB;

using Clock = std::chrono::steady_clock;
using Nanos = std::chrono::nanoseconds;

inline int64_t ElapsedNanos(Clock::time_point t0, Clock::time_point t1) {
    return std::chrono::duration_cast<Nanos>(t1 - t0).count();
}
inline double NanosToUs(int64_t ns) { return static_cast<double>(ns) / 1000.0; }
inline double NanosToSec(int64_t ns) {
    return static_cast<double>(ns) / 1e9;
}

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
}  // namespace

DEFINE_string(local_hostname, "localhost", "Local hostname");
DEFINE_string(
    metadata_server, "http://127.0.0.1:8080/metadata",
    "Metadata server URL (e.g. http://127.0.0.1:8080/metadata or etcd://...)");
DEFINE_string(master_server, "127.0.0.1:50051", "Master server address");
DEFINE_string(protocol, "tcp", "Transport protocol: tcp, rdma, ub");
DEFINE_string(device_name, "", "RDMA/UB device name (comma-separated)");
DEFINE_uint64(global_segment_size, 4 * GB, "Global segment size in bytes");
DEFINE_uint64(local_buffer_size, 512 * MB, "Local buffer size in bytes");
DEFINE_bool(enable_ssd_offload, false, "Enable SSD offload on this client");
DEFINE_string(ssd_offload_path, "", "SSD offload directory path");

DEFINE_string(scenario, "all",
              "Benchmark scenario: write, is_exist, get, all");
DEFINE_uint64(num_requests, 100,
              "Number of requests (each has num_layers keys)");
DEFINE_uint64(num_layers, 32,
              "Layers per request (simulate vLLM transformer)");
DEFINE_uint64(layer_size, 1 * MB, "Size of each layer buffer in bytes");
DEFINE_uint64(num_threads, 1, "Number of concurrent threads");
DEFINE_uint64(warmup_requests, 5, "Warmup requests (not counted)");

enum class Phase { WRITE, IS_EXIST, GET };

static std::string PhaseName(Phase p) {
    switch (p) {
        case Phase::WRITE:
            return "WRITE [batch_put_from_multi_buffers]";
        case Phase::IS_EXIST:
            return "IS_EXIST [batchIsExist]";
        case Phase::GET:
            return "GET [batch_get_into_multi_buffers]";
    }
    return "UNKNOWN";
}

struct ThreadResult {
    std::vector<int64_t> latencies_ns;
    size_t total_bytes = 0;
    size_t total_keys = 0;
    size_t total_queries = 0;
    size_t failed_ops = 0;
};

class BenchmarkStats {
   public:
    void InitThreads(size_t n) { thread_results_.resize(n); }
    ThreadResult& GetThreadResult(size_t tid) { return thread_results_[tid]; }
    void StartTimer() { start_ = Clock::now(); }
    void StopTimer() { end_ = Clock::now(); }
    double WallSeconds() const { return NanosToSec(ElapsedNanos(start_, end_)); }

    void Finalize() {
        merged_latencies_ns_.clear();
        total_bytes_ = total_keys_ = total_queries_ = total_failed_ = 0;
        for (auto& tr : thread_results_) {
            merged_latencies_ns_.insert(merged_latencies_ns_.end(),
                                        tr.latencies_ns.begin(),
                                        tr.latencies_ns.end());
            total_bytes_ += tr.total_bytes;
            total_keys_ += tr.total_keys;
            total_queries_ += tr.total_queries;
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
        double sum = static_cast<double>(std::accumulate(
            merged_latencies_ns_.begin(), merged_latencies_ns_.end(),
            int64_t(0)));
        return NanosToUs(sum / static_cast<double>(merged_latencies_ns_.size()));
    }

    double ThroughputMBps() const {
        double wall = WallSeconds();
        return (wall > 0) ? (static_cast<double>(total_bytes_) / MB) / wall : 0;
    }

    double KeysPerSec() const {
        double wall = WallSeconds();
        return (wall > 0) ? static_cast<double>(total_keys_) / wall : 0;
    }

    void Print(const std::string& title, bool show_bandwidth) const {
        std::cout << "\n========================================"
                     "========================================\n";
        std::cout << "  " << title << "\n";
        std::cout << "========================================"
                     "========================================\n";
        std::cout << std::fixed << std::setprecision(2);
        std::cout << "  Wall time:        " << WallSeconds() << " s\n";
        std::cout << "  Total queries:    " << total_queries_
                  << " (failed: " << total_failed_ << ")\n";
        std::cout << "  Total keys:       " << total_keys_ << "\n";
        if (show_bandwidth) {
            std::cout << "  Total data:       " << FormatBytes(total_bytes_)
                      << "\n";
            std::cout << "  Throughput:       " << ThroughputMBps()
                      << " MB/s";
            if (ThroughputMBps() > 1024)
                std::cout << " (" << ThroughputMBps() / 1024 << " GB/s)";
            std::cout << "\n";
        }
        std::cout << "  Keys/sec:         " << KeysPerSec() << "\n";

        if (!merged_latencies_ns_.empty()) {
            size_t n = merged_latencies_ns_.size();
            std::cout << "\n  Latency (us)      [n=" << n << ", per-query]\n";
            std::cout << "    Min:   " << std::setw(12)
                      << NanosToUs(merged_latencies_ns_.front()) << "\n";
            std::cout << "    Avg:   " << std::setw(12) << MeanLatencyUs()
                      << "\n";
            std::cout << "    P50:   " << std::setw(12) << PercentileUs(50)
                      << "\n";
            std::cout << "    P90:   " << std::setw(12) << PercentileUs(90)
                      << "\n";
            std::cout << "    P99:   " << std::setw(12) << PercentileUs(99)
                      << "\n";
            std::cout << "    Max:   " << std::setw(12)
                      << NanosToUs(merged_latencies_ns_.back()) << "\n";
        }
        std::cout << "========================================"
                     "========================================\n\n";
    }

   private:
    std::vector<ThreadResult> thread_results_;
    std::vector<int64_t> merged_latencies_ns_;
    size_t total_bytes_ = 0, total_keys_ = 0, total_queries_ = 0,
           total_failed_ = 0;
    Clock::time_point start_, end_;
};

class StoreConnectorBench {
   public:
    StoreConnectorBench() : client_(mooncake::RealClient::create()) {}

    ~StoreConnectorBench() {
        for (auto& tb : thread_buffers_) {
            if (tb.ptr) {
                try {
                    client_->unregister_buffer(tb.ptr);
                } catch (...) {
                    LOG(WARNING)
                        << "Failed to unregister thread buffer, ignoring";
                }
                free(tb.ptr);
            }
        }
        if (main_buffer_) {
            try {
                client_->unregister_buffer(main_buffer_);
            } catch (...) {
                LOG(WARNING) << "Failed to unregister main buffer, ignoring";
            }
            free(main_buffer_);
        }
    }

    int Setup() {
        int ret = client_->setup_real(
            FLAGS_local_hostname, FLAGS_metadata_server,
            FLAGS_global_segment_size, FLAGS_local_buffer_size,
            FLAGS_protocol, FLAGS_device_name, FLAGS_master_server, nullptr,
            "", FLAGS_enable_ssd_offload, FLAGS_ssd_offload_path);
        if (ret != 0) {
            LOG(ERROR) << "RealClient setup_real failed, ret=" << ret;
            return ret;
        }
        LOG(INFO) << "RealClient setup succeeded";

        // 主 buffer 用于 PrepareData 阶段
        main_buffer_size_ = FLAGS_num_layers * FLAGS_layer_size;
        main_buffer_ = static_cast<char*>(malloc(main_buffer_size_));
        if (!main_buffer_) {
            LOG(ERROR) << "Failed to allocate main buffer";
            return -1;
        }
        memset(main_buffer_, 0xAB, main_buffer_size_);  // 填充测试数据
        ret = client_->register_buffer(main_buffer_, main_buffer_size_);
        if (ret != 0) {
            LOG(ERROR) << "register_buffer failed for main buffer";
            return ret;
        }

        return AllocateThreadBuffers(FLAGS_num_threads);
    }

    // is_exist/get 模式前先写入数据（不计入统计）
    int PrepareData() {
        LOG(INFO) << "Preparing data: writing " << FLAGS_num_requests
                  << " requests...";
        mooncake::ReplicateConfig config;
        config.replica_num = 1;

        for (size_t r = 0; r < FLAGS_num_requests; ++r) {
            auto keys = MakeRequestKeys(r);
            auto all_buffers = MakeBufferList(main_buffer_);
            auto all_sizes = MakeSizeList();
            auto ret = client_->batch_put_from_multi_buffers(
                keys, all_buffers, all_sizes, config);
            for (int v : ret)
                if (v != 0) {
                    LOG(ERROR) << "PrepareData failed at request " << r;
                    return -1;
                }
            if ((r + 1) % 20 == 0)
                LOG(INFO) << "  Prepared " << (r + 1) << "/"
                          << FLAGS_num_requests;
        }
        LOG(INFO) << "Data preparation complete";
        return 0;
    }

    int RunPhase(Phase phase, bool is_warmup) {
        BenchmarkStats stats;
        stats.InitThreads(FLAGS_num_threads);
        stats.StartTimer();

        std::latch start_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::latch done_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));

        size_t total = FLAGS_num_requests;
        std::vector<std::thread> threads;
        for (size_t t = 0; t < FLAGS_num_threads; ++t) {
            size_t my = total / FLAGS_num_threads +
                       (t < total % FLAGS_num_threads ? 1 : 0);
            size_t offset = t * (total / FLAGS_num_threads) +
                            std::min(t, total % FLAGS_num_threads);
            threads.emplace_back([&, t, my, offset]() {
                PhaseWorker(t, my, offset, phase, stats, start_latch,
                            done_latch);
            });
        }
        done_latch.wait();
        stats.StopTimer();
        for (auto& th : threads) th.join();
        stats.Finalize();

        if (!is_warmup) {
            bool show_bw = (phase != Phase::IS_EXIST);
            stats.Print("BENCHMARK " + PhaseName(phase), show_bw);
        }
        return 0;
    }

    int Run() {
        // Warmup（所有模式都 warmup get，确保连接建立）
        if (FLAGS_warmup_requests > 0 && FLAGS_scenario != "write") {
            LOG(INFO) << "Warmup: " << FLAGS_warmup_requests << " requests";
            size_t saved = FLAGS_num_requests;
            FLAGS_num_requests = FLAGS_warmup_requests;
            RunPhase(Phase::GET, true);
            FLAGS_num_requests = saved;
        }

        if (FLAGS_scenario == "all") {
            RunPhase(Phase::WRITE, false);
            RunPhase(Phase::IS_EXIST, false);
            RunPhase(Phase::GET, false);
        } else if (FLAGS_scenario == "write") {
            RunPhase(Phase::WRITE, false);
        } else if (FLAGS_scenario == "is_exist") {
            if (PrepareData() != 0) return -1;
            RunPhase(Phase::IS_EXIST, false);
        } else if (FLAGS_scenario == "get") {
            if (PrepareData() != 0) return -1;
            RunPhase(Phase::GET, false);
        } else {
            LOG(ERROR) << "Unknown scenario: " << FLAGS_scenario;
            return -1;
        }
        return 0;
    }

   private:
    static std::vector<std::string> MakeRequestKeys(size_t req_id) {
        std::vector<std::string> keys;
        keys.reserve(FLAGS_num_layers);
        for (size_t l = 0; l < FLAGS_num_layers; ++l)
            keys.push_back("layer." + std::to_string(l) + ".req_" +
                           std::to_string(req_id));
        return keys;
    }

    // 每 key 对应 1 个 buffer（vLLM 场景），从大 buffer 切片
    std::vector<std::vector<void*>> MakeBufferList(char* base) {
        std::vector<std::vector<void*>> all_buffers(FLAGS_num_layers);
        for (size_t l = 0; l < FLAGS_num_layers; ++l)
            all_buffers[l] = {base + l * FLAGS_layer_size};
        return all_buffers;
    }

    std::vector<std::vector<size_t>> MakeSizeList() {
        return std::vector<std::vector<size_t>>(
            FLAGS_num_layers, {static_cast<size_t>(FLAGS_layer_size)});
    }

    void PhaseWorker(size_t tid, size_t my_requests, size_t offset,
                     Phase phase, BenchmarkStats& stats,
                     std::latch& start_latch, std::latch& done_latch) {
        ThreadResult& result = stats.GetThreadResult(tid);
        result.latencies_ns.reserve(my_requests);
        char* my_buf = thread_buffers_[tid].ptr;

        mooncake::ReplicateConfig config;
        config.replica_num = 1;

        start_latch.arrive_and_wait();

        size_t bytes_per_req = FLAGS_num_layers * FLAGS_layer_size;

        for (size_t i = 0; i < my_requests; ++i) {
            size_t req_id = offset + i;
            auto keys = MakeRequestKeys(req_id);

            auto t0 = Clock::now();
            std::vector<int> ret;

            if (phase == Phase::WRITE) {
                auto bufs = MakeBufferList(my_buf);
                auto sizes = MakeSizeList();
                ret = client_->batch_put_from_multi_buffers(keys, bufs, sizes,
                                                            config);
            } else if (phase == Phase::IS_EXIST) {
                ret = client_->batchIsExist(keys);
            } else {  // GET
                auto bufs = MakeBufferList(my_buf);
                auto sizes = MakeSizeList();
                ret = client_->batch_get_into_multi_buffers(keys, bufs, sizes,
                                                             false);
            }
            auto t1 = Clock::now();
            result.latencies_ns.push_back(ElapsedNanos(t0, t1));

            bool ok = true;
            for (int v : ret)
                if (v != 0) ok = false;
            if (ok) {
                result.total_keys += FLAGS_num_layers;
                if (phase != Phase::IS_EXIST)
                    result.total_bytes += bytes_per_req;
            } else {
                result.failed_ops++;
            }
            result.total_queries++;
        }

        done_latch.arrive_and_wait();
    }

    int AllocateThreadBuffers(size_t num_threads) {
        thread_buffers_.resize(num_threads);
        size_t per_buf = FLAGS_num_layers * FLAGS_layer_size;
        for (size_t t = 0; t < num_threads; ++t) {
            thread_buffers_[t].size = per_buf;
            thread_buffers_[t].ptr = static_cast<char*>(malloc(per_buf));
            if (!thread_buffers_[t].ptr) {
                LOG(ERROR) << "Failed to allocate buffer for thread " << t;
                return -1;
            }
            memset(thread_buffers_[t].ptr, 0, per_buf);
            int ret =
                client_->register_buffer(thread_buffers_[t].ptr, per_buf);
            if (ret != 0) {
                LOG(ERROR) << "register_buffer failed for thread " << t;
                return ret;
            }
        }
        LOG(INFO) << "Allocated " << num_threads << " thread buffers, each "
                  << FormatBytes(per_buf);
        return 0;
    }

    std::shared_ptr<mooncake::RealClient> client_;
    char* main_buffer_ = nullptr;
    size_t main_buffer_size_ = 0;
    struct ThreadBuf {
        char* ptr = nullptr;
        size_t size = 0;
    };
    std::vector<ThreadBuf> thread_buffers_;
};

int main(int argc, char* argv[]) {
    if (!google::IsGoogleLoggingInitialized()) {
        google::InitGoogleLogging(argv[0]);
    }
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    if (std::getenv("MC_LOG_DIR") == nullptr) {
        FLAGS_logtostderr = true;
    }
    mooncake::logging::ApplyMooncakeLogEnableToGlog();

    LOG(INFO) << "Mooncake Store Connector Benchmark (vLLM path)";
    LOG(INFO) << "  Scenario:     " << FLAGS_scenario;
    LOG(INFO) << "  Protocol:      " << FLAGS_protocol;
    LOG(INFO) << "  Requests:      " << FLAGS_num_requests;
    LOG(INFO) << "  Layers/req:    " << FLAGS_num_layers;
    LOG(INFO) << "  Layer size:    " << FormatBytes(FLAGS_layer_size);
    LOG(INFO) << "  Threads:       " << FLAGS_num_threads;
    size_t total_data =
        FLAGS_num_requests * FLAGS_num_layers * FLAGS_layer_size;
    LOG(INFO) << "  Total data:    " << FormatBytes(total_data);

    StoreConnectorBench bench;
    int ret = bench.Setup();
    if (ret != 0) {
        LOG(ERROR) << "Setup failed";
        return ret;
    }
    return bench.Run();
}
