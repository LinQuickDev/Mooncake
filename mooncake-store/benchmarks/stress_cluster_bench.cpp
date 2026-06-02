#include <numa.h>
#include <sched.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <limits>
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
#include <cstdlib>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "mooncake_logging.h"
#include "real_client.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {
constexpr size_t KB = 1024;
constexpr size_t MB = 1024 * KB;
constexpr size_t GB = 1024 * MB;

const static int NR_SOCKETS =
    numa_available() == 0 ? numa_num_configured_nodes() : 1;

static void bindToSocket(int socket_id) {
    if (numa_available() < 0) return;
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    if (socket_id < 0 || socket_id >= numa_num_configured_nodes())
        socket_id = 0;
    struct bitmask* cpu_list = numa_allocate_cpumask();
    numa_node_to_cpus(socket_id, cpu_list);
    int nr_possible_cpus = numa_num_possible_cpus();
    int nr_cpus = 0;
    for (int cpu = 0; cpu < nr_possible_cpus; ++cpu) {
        if (numa_bitmask_isbitset(cpu_list, cpu) &&
            numa_bitmask_isbitset(numa_all_cpus_ptr, cpu)) {
            CPU_SET(cpu, &cpu_set);
            ++nr_cpus;
        }
    }
    numa_free_cpumask(cpu_list);
    if (nr_cpus > 0) {
        if (sched_setaffinity(0, sizeof(cpu_set), &cpu_set) != 0) {
            PLOG(WARNING) << "Failed to set CPU affinity for NUMA socket " << socket_id;
        }
    }
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

static std::vector<std::string> DiscoverSegmentsFromMaster(
    const std::string& master_host, int master_admin_port) {
    std::vector<std::string> segments;

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        LOG(ERROR) << "Failed to create socket for discovering segments";
        return segments;
    }

    struct timeval timeout;
    timeout.tv_sec = 5;
    timeout.tv_usec = 0;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(master_admin_port);

    std::string host = master_host;
    size_t colon_pos = host.find(':');
    if (colon_pos != std::string::npos) {
        host = host.substr(0, colon_pos);
    }

    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        LOG(ERROR) << "Invalid master host: " << host;
        close(sockfd);
        return segments;
    }

    if (connect(sockfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        LOG(ERROR) << "Failed to connect to master admin at " << host << ":"
                   << master_admin_port;
        close(sockfd);
        return segments;
    }

    std::string request = "GET /get_all_segments HTTP/1.0\r\nHost: " +
                          host + "\r\nConnection: close\r\n\r\n";
    if (send(sockfd, request.c_str(), request.size(), 0) < 0) {
        LOG(ERROR) << "Failed to send HTTP request to master";
        close(sockfd);
        return segments;
    }

    std::string response;
    char buf[4096];
    ssize_t n;
    while ((n = recv(sockfd, buf, sizeof(buf), 0)) > 0) {
        response.append(buf, n);
    }
    close(sockfd);

    size_t header_end = response.find("\r\n\r\n");
    if (header_end == std::string::npos) {
        LOG(ERROR) << "Invalid HTTP response from master";
        return segments;
    }

    std::string header = response.substr(0, header_end);
    size_t status_pos = header.find(' ');
    if (status_pos == std::string::npos) {
        LOG(ERROR) << "Invalid HTTP response header from master";
        return segments;
    }
    size_t status_code_start = status_pos + 1;
    size_t status_code_end = header.find(' ', status_code_start);
    if (status_code_end == std::string::npos) {
        LOG(ERROR) << "Invalid HTTP status line from master";
        return segments;
    }
    std::string status_code = header.substr(status_code_start, status_code_end - status_code_start);
    if (status_code != "200") {
        LOG(ERROR) << "HTTP request failed with status " << status_code
                   << " from master at " << master_host << ":" << master_admin_port;
        return segments;
    }

    std::string body = response.substr(header_end + 4);
    std::istringstream iss(body);
    std::string line;
    while (std::getline(iss, line)) {
        while (!line.empty() && (line.back() == '\r' || line.back() == '\n' ||
                                 line.back() == ' ' || line.back() == '\t')) {
            line.pop_back();
        }
        if (!line.empty()) {
            segments.push_back(line);
        }
    }

    return segments;
}
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
              "remote_disk, segment_write, segment_read");
DEFINE_string(role, "writer",
              "Node role: writer (prefill data) or reader (benchmark reads)");
DEFINE_uint64(value_size, 4 * MB, "Size of each value in bytes");
DEFINE_uint64(num_keys, 100, "Number of keys to write/read");
DEFINE_uint64(batch_size, 32, "Batch size for put/get operations");
DEFINE_uint64(num_threads, 1, "Number of concurrent reader threads");
DEFINE_uint64(warmup_keys, 5, "Number of warmup keys (not counted in stats)");
DEFINE_uint64(wait_seconds, 5,
              "Seconds to wait before reading (for remote scenarios)");
DEFINE_bool(verify, true, "Verify data integrity after read");
DEFINE_uint64(replica_num, 1, "Number of replicas for each object");
DEFINE_bool(hard_pin, false,
            "Pin objects to prevent eviction during benchmark");

DEFINE_string(segments, "",
              "Comma-separated segment names for segment_write/segment_read "
              "scenarios. Use segment 'name' (typically hostname), NOT "
              "IP:port. Leave empty to auto-discover from master.");
DEFINE_uint64(master_admin_port, 9003,
              "Master admin HTTP port for auto-discovering segments");
DEFINE_uint64(read_segment_nums, 0,
              "Number of segments to read from in segment_read scenario (0 = "
              "read from all segments)");
DEFINE_uint64(duration, 0,
              "Duration in seconds for continuous reading in segment_read "
              "scenario (0 = read num_keys once)");
DEFINE_uint64(statis_interval, 5,
              "Statistics print interval in seconds for segment_read scenario");

using Clock = std::chrono::steady_clock;
using Nanos = std::chrono::nanoseconds;

inline int64_t ElapsedNanos(Clock::time_point t0, Clock::time_point t1) {
    return std::chrono::duration_cast<Nanos>(t1 - t0).count();
}

inline double NanosToUs(int64_t ns) { return static_cast<double>(ns) / 1000.0; }
inline double NanosToMs(int64_t ns) {
    return static_cast<double>(ns) / 1000000.0;
}
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
        int64_t ns_val =
            static_cast<int64_t>(merged_latencies_ns_[lo] * (1.0 - frac) +
                                 merged_latencies_ns_[hi] * frac);
        return NanosToUs(ns_val);
    }

    double MeanLatencyUs() const {
        if (merged_latencies_ns_.empty()) return 0.0;
        int64_t sum = std::accumulate(merged_latencies_ns_.begin(),
                                      merged_latencies_ns_.end(), int64_t(0));
        return NanosToUs(sum /
                         static_cast<int64_t>(merged_latencies_ns_.size()));
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
            for (auto& tb : thread_buffers_) {
                if (tb.ptr) {
                    client_->unregister_buffer(tb.ptr);
                    numa_free(tb.ptr, tb.size);
                }
            }
            thread_buffers_.clear();
            if (buffer_) {
                client_->unregister_buffer(buffer_);
                numa_free(buffer_, buffer_size_);
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
        LOG(INFO) << "RealClient setup succeeded"
                  << (FLAGS_enable_ssd_offload ? " (SSD offload enabled)" : "");

        buffer_size_ = FLAGS_batch_size * FLAGS_value_size;
        buffer_ = reinterpret_cast<char*>(numa_alloc_local(buffer_size_));
        if (!buffer_) {
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
        config.with_hard_pin = FLAGS_hard_pin;

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

        int buf_ret = AllocateThreadBuffers(FLAGS_num_threads);
        if (buf_ret != 0) return buf_ret;

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
        system("ubdiag clear");

        BenchmarkStats stats;
        stats.InitThreads(FLAGS_num_threads,
                          FLAGS_num_keys / FLAGS_num_threads);
        stats.StartTimer();

        std::latch start_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::latch done_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        auto threads = LaunchReadWorkers(
            FLAGS_num_threads, FLAGS_num_keys, stats, start_latch, done_latch,
            [](size_t idx) { return MakeKey(idx); });

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

        int buf_ret = AllocateThreadBuffers(FLAGS_num_threads);
        if (buf_ret != 0) return buf_ret;

        mooncake::ReplicateConfig config;
        config.replica_num = FLAGS_replica_num;
        config.with_hard_pin = FLAGS_hard_pin;

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
        auto threads = LaunchReadWorkers(
            FLAGS_num_threads, FLAGS_num_keys, stats, start_latch, done_latch,
            [](size_t idx) { return MakeKey(idx); });

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

        int buf_ret = AllocateThreadBuffers(FLAGS_num_threads);
        if (buf_ret != 0) return buf_ret;

        mooncake::ReplicateConfig config;
        config.replica_num = FLAGS_replica_num;
        config.with_hard_pin = FLAGS_hard_pin;

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
        auto threads = LaunchReadWorkers(
            FLAGS_num_threads, FLAGS_num_keys, stats, start_latch, done_latch,
            [](size_t idx) { return MakeKey(idx); });

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

    static std::vector<std::string> ParseSegments() {
        std::vector<std::string> segments;
        std::istringstream iss(FLAGS_segments);
        std::string seg;
        while (std::getline(iss, seg, ',')) {
            size_t start = seg.find_first_not_of(" \t");
            size_t end = seg.find_last_not_of(" \t");
            if (start != std::string::npos && end != std::string::npos) {
                segments.push_back(seg.substr(start, end - start + 1));
            }
        }
        return segments;
    }

    static std::string MakeSegmentKey(const std::string& segment,
                                      size_t idx) {
        static const char* kSpecialChars = ".:-/\\[]{}()@#$%^&*+=|<>,;!?`'\"~";
        std::string sanitized = segment;
        for (char& c : sanitized) {
            if (std::strchr(kSpecialChars, c) != nullptr || std::isspace(c)) {
                c = '_';
            }
        }
        return "seg_" + sanitized + "_key_" + std::to_string(idx);
    }

    int RunSegmentWrite() {
        auto segments = DiscoverSegmentsIfNeeded(
            "--segments not specified, auto-discovering");
        if (segments.empty()) {
            return -1;
        }
        LOG(INFO) << "Discovered " << segments.size()
                  << " segments from master";

        LOG(INFO) << "=== SEGMENT WRITE MODE ===";
        LOG(INFO) << "Writing to " << segments.size() << " segments, "
                  << FLAGS_num_keys << " keys per segment (interleaved), each "
                  << FLAGS_value_size / MB << " MB";

        std::vector<size_t> seg_written(segments.size(), 0);
        std::vector<size_t> seg_failed(segments.size(), 0);
        std::vector<mooncake::ReplicateConfig> configs(segments.size());
        for (size_t s = 0; s < segments.size(); ++s) {
            configs[s].replica_num = FLAGS_replica_num;
            configs[s].with_hard_pin = FLAGS_hard_pin;
            configs[s].preferred_segments = {segments[s]};
        }

        size_t total_written = 0;
        size_t total_failed = 0;

        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
            for (size_t s = 0; s < segments.size(); ++s) {
                const auto& segment = segments[s];
                std::string key = MakeSegmentKey(segment, i);
                FillBuffer(i);

                auto t0 = Clock::now();
                int ret = client_->put_from(key, buffer_, FLAGS_value_size, configs[s]);
                auto t1 = Clock::now();

                if (ret != 0) {
                    LOG(ERROR) << "put_from failed for key=" << key
                               << " segment=" << segment << " ret=" << ret;
                    ++seg_failed[s];
                    continue;
                }
                ++seg_written[s];
            }

            if ((i + 1) % 10 == 0 || i == FLAGS_num_keys - 1) {
                LOG(INFO) << "  Written " << (i + 1) << "/" << FLAGS_num_keys
                          << " keys to all " << segments.size() << " segments";
            }
        }

        for (size_t s = 0; s < segments.size(); ++s) {
            total_written += seg_written[s];
            total_failed += seg_failed[s];
            LOG(INFO) << "Segment [" << s << "] " << segments[s]
                      << " complete: " << seg_written[s] << " succeeded, "
                      << seg_failed[s] << " failed";
        }

        LOG(INFO) << "All segments write complete: " << total_written
                  << " succeeded, " << total_failed << " failed";

        LOG(INFO) << "Waiting " << FLAGS_wait_seconds
                  << " seconds for reader to connect...";
        std::this_thread::sleep_for(std::chrono::seconds(FLAGS_wait_seconds));

        return (total_failed > 0) ? -1 : 0;
    }

    int RunSegmentRead() {
        auto segments = DiscoverSegmentsIfNeeded(
            "--segments not specified, auto-discovering");
        if (segments.empty()) {
            return -1;
        }
        LOG(INFO) << "Discovered " << segments.size()
                  << " segments from master";

        size_t read_segment_nums = FLAGS_read_segment_nums;
        if (read_segment_nums == 0 || read_segment_nums > segments.size()) {
            read_segment_nums = segments.size();
        }

        std::vector<std::string> read_segments(
            segments.begin(), segments.begin() + read_segment_nums);

        LOG(INFO) << "=== SEGMENT READ MODE ===";
        LOG(INFO) << "Reading from " << read_segment_nums << " segments ("
                  << read_segment_nums << " nodes)";
        for (size_t s = 0; s < read_segments.size(); ++s) {
            LOG(INFO) << "  Segment [" << s << "]: " << read_segments[s];
        }
        LOG(INFO) << "Keys per segment: " << FLAGS_num_keys;
        LOG(INFO) << "Duration: "
                  << (FLAGS_duration > 0 ? std::to_string(FLAGS_duration) + "s"
                                         : "single pass");
        LOG(INFO) << "Stats interval: " << FLAGS_statis_interval << "s";

        int buf_ret = AllocateThreadBuffers(FLAGS_num_threads);
        if (buf_ret != 0) return buf_ret;

        std::vector<std::string> all_keys;
        for (size_t s = 0; s < read_segments.size(); ++s) {
            for (size_t i = 0; i < FLAGS_num_keys; ++i) {
                all_keys.push_back(
                    MakeSegmentKey(read_segments[s], i));
            }
        }
        LOG(INFO) << "Total keys to read: " << all_keys.size();

        size_t warmup_end =
            std::min(static_cast<size_t>(FLAGS_warmup_keys), all_keys.size());
        if (warmup_end > 0) {
            LOG(INFO) << "Warmup: reading " << warmup_end << " keys...";
            for (size_t i = 0; i < warmup_end; ++i) {
                int64_t ret =
                    client_->get_into(all_keys[i], buffer_, FLAGS_value_size);
                if (ret < 0) {
                    LOG(WARNING) << "Warmup get_into failed for key="
                                 << all_keys[i] << " ret=" << ret;
                }
            }
            LOG(INFO) << "Warmup complete";
        }

        if (FLAGS_duration == 0) {
            return RunSegmentReadSinglePass(read_segments, all_keys);
        }
        return RunSegmentReadDuration(read_segments, all_keys);
    }

    int RunSegmentReadSinglePass(
        const std::vector<std::string>& read_segments,
        const std::vector<std::string>& all_keys) {
        LOG(INFO) << "Single-pass read with " << FLAGS_num_threads
                  << " threads";

        BenchmarkStats stats;
        stats.InitThreads(FLAGS_num_threads, all_keys.size() / FLAGS_num_threads);
        stats.StartTimer();

        std::latch start_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::latch done_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        auto threads = LaunchReadWorkers(
            FLAGS_num_threads, all_keys.size(), stats, start_latch, done_latch,
            [&all_keys](size_t idx) { return all_keys[idx % all_keys.size()]; });

        done_latch.wait();
        stats.StopTimer();

        for (auto& th : threads) {
            th.join();
        }

        stats.Finalize();

        std::string title = "SEGMENT READ BENCHMARK [segments=" +
                            std::to_string(read_segments.size()) + "]";
        stats.Print(title);
        return 0;
    }

    struct IntervalLatencyStats {
        std::vector<int64_t> latencies_ns;
        int64_t min_latency_ns = std::numeric_limits<int64_t>::max();
        int64_t max_latency_ns = 0;
        double p99_latency_ns = 0;
        double p999_latency_ns = 0;
        double p9999_latency_ns = 0;
        double avg_latency_ns = 0;
        double throughput_mbps = 0;

        void Finalize() {
            if (latencies_ns.empty()) return;
            std::sort(latencies_ns.begin(), latencies_ns.end());
            min_latency_ns = latencies_ns.front();
            max_latency_ns = latencies_ns.back();
            size_t n = latencies_ns.size();
            avg_latency_ns = std::accumulate(latencies_ns.begin(), latencies_ns.end(), 0.0) / n;
            if (n >= 100) {
                p99_latency_ns = latencies_ns[static_cast<size_t>(n * 0.99)];
            }
            if (n >= 1000) {
                p999_latency_ns = latencies_ns[static_cast<size_t>(n * 0.999)];
            }
            if (n >= 10000) {
                p9999_latency_ns = latencies_ns[static_cast<size_t>(n * 0.9999)];
            }
        }

        void Aggregate(const IntervalLatencyStats& other) {
            min_latency_ns = std::min(min_latency_ns, other.min_latency_ns);
            max_latency_ns = std::max(max_latency_ns, other.max_latency_ns);
            p99_latency_ns = std::max(p99_latency_ns, other.p99_latency_ns);
            p999_latency_ns = std::max(p999_latency_ns, other.p999_latency_ns);
            p9999_latency_ns = std::max(p9999_latency_ns, other.p9999_latency_ns);
            throughput_mbps += other.throughput_mbps;
            total_samples += other.latencies_ns.size();
        }

        size_t total_samples = 0;
    };

    int RunSegmentReadDuration(
        const std::vector<std::string>& read_segments,
        const std::vector<std::string>& all_keys) {
        LOG(INFO) << "Duration-based continuous read with " << FLAGS_num_threads
                  << " threads for " << FLAGS_duration << "s, stats every "
                  << FLAGS_statis_interval << "s";

        std::atomic<bool> stop_flag{false};
        std::atomic<size_t> global_ops{0};
        std::atomic<size_t> global_bytes{0};
        std::atomic<size_t> global_failed{0};

        std::vector<std::vector<int64_t>> thread_latencies(FLAGS_num_threads);
        std::vector<std::mutex> latency_mutexes(FLAGS_num_threads);

        std::latch start_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::vector<std::thread> threads;

        size_t total_keys = all_keys.size();
        size_t keys_per_thread =
            (total_keys + FLAGS_num_threads - 1) / FLAGS_num_threads;

        for (size_t t = 0; t < FLAGS_num_threads; ++t) {
            threads.emplace_back([&, t, keys_per_thread, total_keys]() {
                bindToSocket(t % NR_SOCKETS);
                char* my_buf = thread_buffers_[t].ptr;

                start_latch.arrive_and_wait();

                size_t key_offset = t * keys_per_thread;
                size_t key_idx = key_offset;

                if (FLAGS_batch_size <= 1) {
                    while (!stop_flag.load(std::memory_order_relaxed)) {
                        const std::string& key =
                            all_keys[key_idx % total_keys];
                        auto t0 = Clock::now();
                        int64_t ret =
                            client_->get_into(key, my_buf, FLAGS_value_size);
                        auto t1 = Clock::now();
                        int64_t latency_ns = ElapsedNanos(t0, t1);

                        {
                            std::lock_guard<std::mutex> lock(latency_mutexes[t]);
                            thread_latencies[t].push_back(latency_ns);
                        }

                        if (ret < 0) {
                            global_failed.fetch_add(1,
                                                    std::memory_order_relaxed);
                        } else {
                            global_bytes.fetch_add(static_cast<size_t>(ret),
                                                   std::memory_order_relaxed);
                        }
                        global_ops.fetch_add(1, std::memory_order_relaxed);
                        ++key_idx;
                    }
                } else {
                    size_t per_key_buf = FLAGS_value_size;
                    while (!stop_flag.load(std::memory_order_relaxed)) {
                        std::vector<std::string> keys;
                        std::vector<void*> bufs;
                        std::vector<size_t> sizes;
                        keys.reserve(FLAGS_batch_size);
                        bufs.reserve(FLAGS_batch_size);
                        sizes.reserve(FLAGS_batch_size);

                        for (size_t b = 0; b < FLAGS_batch_size; ++b) {
                            const std::string& key =
                                all_keys[key_idx % total_keys];
                            keys.push_back(key);
                            bufs.push_back(my_buf + b * per_key_buf);
                            sizes.push_back(FLAGS_value_size);
                            ++key_idx;
                        }

                        auto t0 = Clock::now();
                        auto results =
                            client_->batch_get_into(keys, bufs, sizes);
                        auto t1 = Clock::now();
                        int64_t latency_ns = ElapsedNanos(t0, t1);

                        {
                            std::lock_guard<std::mutex> lock(latency_mutexes[t]);
                            for (size_t k = 0; k < results.size(); ++k) {
                                thread_latencies[t].push_back(latency_ns / FLAGS_batch_size);
                            }
                        }

                        for (size_t k = 0; k < results.size(); ++k) {
                            if (results[k] < 0) {
                                global_failed.fetch_add(
                                    1, std::memory_order_relaxed);
                            } else {
                                global_bytes.fetch_add(
                                    static_cast<size_t>(results[k]),
                                    std::memory_order_relaxed);
                            }
                            global_ops.fetch_add(1,
                                                 std::memory_order_relaxed);
                        }
                    }
                }
            });
        }

        auto bench_start = Clock::now();
        auto bench_end =
            bench_start + std::chrono::seconds(FLAGS_duration);
        auto next_statis = bench_start + std::chrono::seconds(FLAGS_statis_interval);

        size_t prev_ops = 0;
        size_t prev_bytes = 0;
        size_t prev_failed = 0;
        auto prev_time = bench_start;

        std::vector<IntervalLatencyStats> interval_stats_list;

        std::cout << "\n";
        std::cout << "========================================"
                  << "========================================\n";
        std::cout << "  SEGMENT READ DURATION BENCHMARK [segments="
                  << read_segments.size() << "]\n";
        std::cout << "========================================"
                  << "========================================\n";
        std::cout << std::fixed << std::setprecision(2);

        while (Clock::now() < bench_end) {
            auto now = Clock::now();
            if (now >= next_statis) {
                size_t cur_ops = global_ops.load(std::memory_order_relaxed);
                size_t cur_bytes = global_bytes.load(std::memory_order_relaxed);
                size_t cur_failed =
                    global_failed.load(std::memory_order_relaxed);

                double interval_sec = NanosToSec(ElapsedNanos(prev_time, now));
                size_t interval_ops = cur_ops - prev_ops;
                size_t interval_bytes = cur_bytes - prev_bytes;
                size_t interval_failed = cur_failed - prev_failed;

                double interval_throughput_mbps =
                    (interval_sec > 0)
                        ? (static_cast<double>(interval_bytes) / MB) /
                              interval_sec
                        : 0;
                double interval_ops_per_sec =
                    (interval_sec > 0)
                        ? static_cast<double>(interval_ops) / interval_sec
                        : 0;

                IntervalLatencyStats interval_stats;
                interval_stats.throughput_mbps = interval_throughput_mbps;
                for (size_t t = 0; t < FLAGS_num_threads; ++t) {
                    std::lock_guard<std::mutex> lock(latency_mutexes[t]);
                    interval_stats.latencies_ns.insert(
                        interval_stats.latencies_ns.end(),
                        thread_latencies[t].begin(),
                        thread_latencies[t].end());
                    thread_latencies[t].clear();
                }
                interval_stats.Finalize();
                interval_stats_list.push_back(interval_stats);

                double total_sec = NanosToSec(ElapsedNanos(bench_start, now));
                double total_throughput_mbps =
                    (total_sec > 0)
                        ? (static_cast<double>(cur_bytes) / MB) / total_sec
                        : 0;
                double total_ops_per_sec =
                    (total_sec > 0)
                        ? static_cast<double>(cur_ops) / total_sec
                        : 0;

                std::cout << "  [t=" << std::setw(6) << total_sec << "s]"
                          << "  interval: " << interval_throughput_mbps
                          << " MB/s, " << interval_ops_per_sec << " ops/s"
                          << " (failed=" << interval_failed << ")"
                          << "  lat[us]: min=" << NanosToUs(interval_stats.min_latency_ns)
                          << ", max=" << NanosToUs(interval_stats.max_latency_ns)
                          << ", avg=" << NanosToUs(interval_stats.avg_latency_ns)
                          << ", P99=" << NanosToUs(interval_stats.p99_latency_ns)
                          << "  total: " << cur_ops << " ops, "
                          << total_throughput_mbps << " MB/s, "
                          << total_ops_per_sec << " ops/s"
                          << " (failed=" << cur_failed << ")\n";

                prev_ops = cur_ops;
                prev_bytes = cur_bytes;
                prev_failed = cur_failed;
                prev_time = now;
                next_statis += std::chrono::seconds(FLAGS_statis_interval);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        stop_flag.store(true, std::memory_order_relaxed);
        for (auto& th : threads) {
            th.join();
        }

        auto final_time = Clock::now();
        double total_sec = NanosToSec(ElapsedNanos(bench_start, final_time));
        size_t final_ops = global_ops.load(std::memory_order_relaxed);
        size_t final_bytes = global_bytes.load(std::memory_order_relaxed);
        size_t final_failed = global_failed.load(std::memory_order_relaxed);

        double final_throughput_mbps =
            (total_sec > 0)
                ? (static_cast<double>(final_bytes) / MB) / total_sec
                : 0;
        double final_ops_per_sec =
            (total_sec > 0) ? static_cast<double>(final_ops) / total_sec : 0;

        IntervalLatencyStats overall;
        for (const auto& stats : interval_stats_list) {
            overall.Aggregate(stats);
        }
        double avg_throughput_mbps =
            !interval_stats_list.empty() ? overall.throughput_mbps / interval_stats_list.size() : 0;
        size_t total_latency_samples = overall.total_samples;

        std::cout << "\n  FINAL SUMMARY\n";
        std::cout << "  Total time:       " << total_sec << " s\n";
        std::cout << "  Total ops:        " << final_ops
                  << " (failed: " << final_failed << ")\n";
        std::cout << "  Total data:       " << FormatBytes(final_bytes)
                  << "\n";
        std::cout << "  Throughput:       " << final_throughput_mbps
                  << " MB/s (avg: " << avg_throughput_mbps << " MB/s)";
        if (final_throughput_mbps > 1024) {
            std::cout << " (" << final_throughput_mbps / 1024 << " GB/s)";
        }
        std::cout << "\n";
        std::cout << "  Ops/sec:          " << final_ops_per_sec << "\n";

        if (total_latency_samples > 0) {
            std::cout << "\n  Latency (us)      [n=" << total_latency_samples << "]\n";
            std::cout << "    Min:   " << std::setw(12) << NanosToUs(overall.min_latency_ns) << "\n";
            std::cout << "    Max:   " << std::setw(12) << NanosToUs(overall.max_latency_ns) << "\n";
            std::cout << "    P99:   " << std::setw(12) << NanosToUs(overall.p99_latency_ns);
            if (total_latency_samples < 100) std::cout << "  (n<100)";
            std::cout << "\n";
            std::cout << "    P999:  " << std::setw(12) << NanosToUs(overall.p999_latency_ns);
            if (total_latency_samples < 1000) std::cout << "  (n<1000)";
            std::cout << "\n";
            std::cout << "    P9999: " << std::setw(12) << NanosToUs(overall.p9999_latency_ns);
            if (total_latency_samples < 10000) std::cout << "  (n<10000)";
            std::cout << "\n";
        }

        std::cout << "========================================"
                  << "========================================\n\n";

        return 0;
    }

    int RunListSegments() {
        LOG(INFO) << "Discovering segments from master at "
                  << FLAGS_master_server << ":" << FLAGS_master_admin_port;

        auto segments = DiscoverSegmentsFromMaster(
            FLAGS_master_server,
            static_cast<int>(FLAGS_master_admin_port));

        if (segments.empty()) {
            LOG(ERROR) << "No segments discovered from master. "
                       << "Check master connectivity at "
                       << FLAGS_master_server << ":" << FLAGS_master_admin_port;
            return -1;
        }

        std::cout << "\n";
        std::cout << "========================================"
                  << "========================================\n";
        std::cout << "  DISCOVERED SEGMENTS [count=" << segments.size() << "]\n";
        std::cout << "========================================"
                  << "========================================\n";

        for (size_t i = 0; i < segments.size(); ++i) {
            std::cout << "  [" << std::setw(4) << i << "] " << segments[i] << "\n";
        }

        std::cout << "========================================"
                  << "========================================\n";
        std::cout << "  Total segments: " << segments.size() << "\n";
        std::cout << "========================================"
                  << "========================================\n\n";

        return 0;
    }

    int Run() {
        if (FLAGS_scenario == "local_memory") {
            return RunLocalMemory();
        } else if (FLAGS_scenario == "local_disk") {
            return RunLocalDisk();
        } else if (FLAGS_scenario == "segment_write") {
            return RunSegmentWrite();
        } else if (FLAGS_scenario == "segment_read") {
            return RunSegmentRead();
        } else if (FLAGS_scenario == "list_segments") {
            return RunListSegments();
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

    void BatchReadWorker(size_t tid, size_t my_keys, size_t key_offset,
                         BenchmarkStats& stats, std::latch& start_latch,
                         std::latch& done_latch,
                         const std::function<std::string(size_t)>& key_func) {
        bindToSocket(tid % NR_SOCKETS);

        ThreadResult& result = stats.GetThreadResult(tid);
        result.latencies_ns.reserve(my_keys);

        char* my_buf = thread_buffers_[tid].ptr;

        start_latch.arrive_and_wait();

        size_t ops = 0;
        size_t failed = 0;
        size_t bytes = 0;

        if (FLAGS_batch_size <= 1) {
            for (size_t i = 0; i < my_keys; ++i) {
                size_t key_idx = key_offset + i;
                std::string key = key_func(key_idx);

                auto t0 = Clock::now();
                int64_t ret = client_->get_into(key, my_buf, FLAGS_value_size);
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
            size_t per_key_buf = FLAGS_value_size;
            size_t i = 0;
            while (i < my_keys) {
                std::vector<std::string> keys;
                std::vector<void*> bufs;
                std::vector<size_t> sizes;
                size_t batch_end = std::min(i + FLAGS_batch_size, my_keys);
                keys.reserve(batch_end - i);
                bufs.reserve(batch_end - i);
                sizes.reserve(batch_end - i);

                for (size_t j = i; j < batch_end; ++j) {
                    size_t key_idx = key_offset + j;
                    keys.push_back(key_func(key_idx));
                    bufs.push_back(my_buf + (j - i) * per_key_buf);
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

    std::vector<std::thread> LaunchReadWorkers(
        size_t num_threads, size_t total_keys, BenchmarkStats& stats,
        std::latch& start_latch, std::latch& done_latch,
        const std::function<std::string(size_t)>& key_func) {
        std::vector<std::thread> threads;
        size_t keys_per_thread = total_keys / num_threads;
        size_t remainder = total_keys % num_threads;

        for (size_t t = 0; t < num_threads; ++t) {
            size_t my_keys = keys_per_thread + (t < remainder ? 1 : 0);
            size_t key_offset = t * keys_per_thread + std::min(t, remainder);

            threads.emplace_back([&, t, my_keys, key_offset]() {
                BatchReadWorker(t, my_keys, key_offset, stats, start_latch,
                               done_latch, key_func);
            });
        }
        return threads;
    }

    std::vector<std::string> DiscoverSegmentsIfNeeded(const std::string& context) {
        auto segments = ParseSegments();
        if (!segments.empty()) {
            return segments;
        }

        LOG(INFO) << context << ", auto-discovering from master at "
                  << FLAGS_master_server << ":" << FLAGS_master_admin_port;
        segments = DiscoverSegmentsFromMaster(
            FLAGS_master_server,
            static_cast<int>(FLAGS_master_admin_port));
        if (segments.empty()) {
            LOG(ERROR) << "No segments discovered from master. "
                       << "Check master connectivity.";
        }
        return segments;
    }

    int VerifyData() {
        LOG(INFO) << "Verifying data integrity for " << FLAGS_num_keys
                  << " keys...";
        int errors = 0;

        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
            std::string key = MakeKey(i);
            int64_t ret = client_->get_into(key, buffer_, FLAGS_value_size);
            if (ret < 0) {
                LOG(ERROR) << "Verify: get_into failed for key=" << key;
                ++errors;
                continue;
            }
            if (!CheckBuffer(i, buffer_, static_cast<size_t>(ret))) {
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

    struct ThreadBuffer {
        char* ptr = nullptr;
        size_t size = 0;
        int numa_node = -1;
    };
    std::vector<ThreadBuffer> thread_buffers_;

    int AllocateThreadBuffers(size_t num_threads) {
        thread_buffers_.resize(num_threads);
        size_t per_buf_size = FLAGS_batch_size * FLAGS_value_size;
        for (size_t t = 0; t < num_threads; ++t) {
            int node = t % NR_SOCKETS;
            thread_buffers_[t].size = per_buf_size;
            thread_buffers_[t].numa_node = node;
            thread_buffers_[t].ptr =
                reinterpret_cast<char*>(numa_alloc_onnode(per_buf_size, node));
            if (!thread_buffers_[t].ptr) {
                LOG(ERROR) << "Failed to allocate buffer for thread " << t
                           << " on NUMA node " << node;
                return -1;
            }
            std::memset(thread_buffers_[t].ptr, 0, per_buf_size);
            int ret =
                client_->register_buffer(thread_buffers_[t].ptr, per_buf_size);
            if (ret != 0) {
                LOG(ERROR) << "register_buffer failed for thread " << t
                           << " on NUMA node " << node;
                return ret;
            }
        }
        LOG(INFO) << "Allocated " << num_threads << " thread buffers, each "
                  << per_buf_size / MB << " MB (NUMA-aware, " << NR_SOCKETS
                  << " sockets)";
        return 0;
    }
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

    LOG(INFO) << "Mooncake Stress Cluster Benchmark";
    LOG(INFO) << "  Scenario:       " << FLAGS_scenario;
    LOG(INFO) << "  Protocol:       " << FLAGS_protocol;
    LOG(INFO) << "  Value size:     " << FLAGS_value_size / MB << " MB";
    LOG(INFO) << "  Num keys:       " << FLAGS_num_keys;
    LOG(INFO) << "  Batch size:     " << FLAGS_batch_size;
    LOG(INFO) << "  Num threads:    " << FLAGS_num_threads;
    LOG(INFO) << "  Hard pin:       " << (FLAGS_hard_pin ? "yes" : "no");
    LOG(INFO) << "  SSD offload:    "
              << (FLAGS_enable_ssd_offload ? "yes" : "no");
    if (!FLAGS_segments.empty()) {
        LOG(INFO) << "  Segments:       " << FLAGS_segments;
    } else {
        LOG(INFO) << "  Segments:       auto-discover from master";
    }
    LOG(INFO) << "  Master admin:   " << FLAGS_master_admin_port;
    LOG(INFO) << "  Read seg nums:  " << FLAGS_read_segment_nums;
    LOG(INFO) << "  Duration:       " << FLAGS_duration << "s";
    LOG(INFO) << "  Stats interval: " << FLAGS_statis_interval << "s";

    size_t total_data = FLAGS_num_keys * FLAGS_value_size;
    if (total_data > FLAGS_global_segment_size * 9.5 / 10) {
        LOG(WARNING) << "Total data (" << total_data / MB << " MB) may exceed "
                     << "95% of segment (" << FLAGS_global_segment_size / MB
                     << " MB). Master eviction may delete objects. "
                     << "Consider increasing --global_segment_size or "
                     << "decreasing --num_keys, or use --hard_pin=true.";
    }

    StressBenchmark bench;
    int ret = bench.Setup();
    if (ret != 0) {
        LOG(ERROR) << "Benchmark setup failed";
        return ret;
    }

    ret = bench.Run();
    return ret;
}
