// stress_cluster_ranges_bench: Benchmark for RealClient::get_into_ranges,
// split into two scenarios:
//
//   --scenario segment_range_write
//       Prefill `--num_keys` keys of `--value_size` bytes onto each
//       discovered (or --segments-listed) segment, using hard-pinning via
//       ReplicateConfig::preferred_segments so that each segment holds a
//       distinct set of keys.  Key naming follows stress_cluster_bench's
//       MakeSegmentKey convention so the reader side can reconstruct them.
//
//   --scenario segment_range_read
//       Read the keys back from the segments.  Two read modes are supported
//       via --read_mode:
//         ranged (default): use get_into_ranges with --fragment_size-byte
//                           fragments at random source offsets within each
//                           value, exercising the ranged-read path.
//         full            : use get_into to read the whole value in one go,
//                           serving as a baseline for comparison.
//
// Stats collection mirrors stress_cluster_bench: per-query latency histogram
// (min/avg/P50/P90/P99/P999/max), throughput (MB/s), keys/sec, queries/sec.

#include <arpa/inet.h>
#include <numa.h>
#include <sched.h>
#include <sys/socket.h>
#include <unistd.h>

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
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "mooncake_logging.h"
#include "real_client.h"

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
            PLOG(WARNING) << "Failed to set CPU affinity for NUMA socket "
                          << socket_id;
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

// Discover segment names from the master's admin HTTP endpoint.
// Mirrors stress_cluster_bench::DiscoverSegmentsFromMaster.
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

    std::string request = "GET /get_all_segments HTTP/1.0\r\nHost: " + host +
                          "\r\nConnection: close\r\n\r\n";
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
    std::string status_code =
        header.substr(status_code_start, status_code_end - status_code_start);
    if (status_code != "200") {
        LOG(ERROR) << "HTTP request failed with status " << status_code
                   << " from master at " << master_host << ":"
                   << master_admin_port;
        return segments;
    }

    std::string body = response.substr(header_end + 4);
    std::istringstream iss(body);
    std::string line;
    std::unordered_set<std::string> seen;
    while (std::getline(iss, line)) {
        while (!line.empty() && (line.back() == '\r' || line.back() == '\n' ||
                                 line.back() == ' ' || line.back() == '\t')) {
            line.pop_back();
        }
        if (!line.empty() && seen.insert(line).second) {
            segments.push_back(line);
        }
    }

    return segments;
}

// Parse a comma-separated list of segment names.
static std::vector<std::string> ParseSegments(const std::string& spec) {
    std::vector<std::string> segments;
    std::istringstream iss(spec);
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

// Sanitize a segment name into a filesystem/key-safe form, matching
// stress_cluster_bench::MakeSegmentKey so writers and readers agree on keys.
static std::string MakeSegmentKey(const std::string& segment, size_t idx) {
    static const char* kSpecialChars = ".:-/\\[]{}()@#$%^&*+=|<>,;!?`'\"~";
    std::string sanitized = segment;
    for (char& c : sanitized) {
        if (std::strchr(kSpecialChars, c) != nullptr || std::isspace(c)) {
            c = '_';
        }
    }
    return "seg_" + sanitized + "_key_" + std::to_string(idx);
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

DEFINE_string(scenario, "segment_range_read",
              "Benchmark scenario: segment_range_write (prefill data onto each "
              "segment) or segment_range_read (read data back, ranged or full)");
DEFINE_uint64(value_size, 4 * MB,
              "Size of each value in bytes (uint64). Each value is read back "
              "as a set of --fragment_size-byte fragments with randomly chosen "
              "source offsets in ranged read mode.");
DEFINE_uint64(num_keys, 100,
              "Number of keys to write/read per segment (segment scenarios)");
DEFINE_uint64(keys_per_query, 1,
              "Number of keys packed into each get_into_ranges call");
DEFINE_uint64(fragment_size, 8,
              "Size in bytes of each read fragment (ranged mode). Source "
              "offsets within each value are chosen randomly in "
              "[0, value_size-fragment_size]. value_size must be divisible by "
              "fragment_size; fragments-per-key = value_size/fragment_size.");
DEFINE_string(read_mode, "ranged",
              "Read mode for segment_range_read scenario: ranged "
              "(get_into_ranges with random offsets) or full (get_into)");
DEFINE_uint64(num_threads, 1, "Number of concurrent reader threads");
DEFINE_uint64(warmup_keys, 5,
              "Number of warmup keys (not counted in stats)");
DEFINE_bool(verify, true, "Verify data integrity after read");
DEFINE_uint64(replica_num, 1, "Number of replicas for each object");
DEFINE_bool(hard_pin, false,
            "Pin objects to prevent eviction during benchmark");
DEFINE_string(segments, "",
              "Comma-separated segment names. Use segment 'name' (typically "
              "hostname), NOT IP:port. Leave empty to auto-discover from "
              "master.");
DEFINE_uint64(master_admin_port, 9003,
              "Master admin HTTP port for auto-discovering segments");
DEFINE_uint64(read_segment_nums, 0,
              "Number of segments to read from in segment_range_read scenario "
              "(0 = read from all segments)");
DEFINE_uint64(wait_seconds, 5,
              "Seconds to wait before reading (writer/reader handoff)");

using Clock = std::chrono::steady_clock;
using Nanos = std::chrono::nanoseconds;

inline int64_t ElapsedNanos(Clock::time_point t0, Clock::time_point t1) {
    return std::chrono::duration_cast<Nanos>(t1 - t0).count();
}

inline double NanosToUs(int64_t ns) { return static_cast<double>(ns) / 1000.0; }
inline double NanosToSec(int64_t ns) { return static_cast<double>(ns) / 1e9; }

struct ThreadResult {
    std::vector<int64_t> latencies_ns;  // per-query latency
    size_t total_bytes = 0;
    size_t total_keys = 0;
    size_t total_queries = 0;
    size_t failed_ops = 0;
};

class BenchmarkStats {
   public:
    void InitThreads(size_t n, size_t /*expected_per_thread*/) {
        thread_results_.resize(n);
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
        total_keys_ = 0;
        total_queries_ = 0;
        total_failed_ = 0;

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
        int64_t ns_val =
            static_cast<int64_t>(merged_latencies_ns_[lo] * (1.0 - frac) +
                                 merged_latencies_ns_[hi] * frac);
        return NanosToUs(ns_val);
    }

    double MeanLatencyUs() const {
        if (merged_latencies_ns_.empty()) return 0.0;
        double sum = static_cast<double>(
            std::accumulate(merged_latencies_ns_.begin(),
                            merged_latencies_ns_.end(), int64_t(0)));
        return NanosToUs(sum /
                         static_cast<double>(merged_latencies_ns_.size()));
    }

    double ThroughputMBps() const {
        double wall = WallSeconds();
        return (wall > 0) ? (static_cast<double>(total_bytes_) / MB) / wall : 0;
    }

    double KeysPerSec() const {
        double wall = WallSeconds();
        return (wall > 0) ? static_cast<double>(total_keys_) / wall : 0;
    }

    double QueriesPerSec() const {
        double wall = WallSeconds();
        return (wall > 0) ? static_cast<double>(total_queries_) / wall : 0;
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
        std::cout << "  Total queries:    " << total_queries_
                  << " (failed: " << total_failed_ << ")\n";
        std::cout << "  Total keys:       " << total_keys_ << "\n";
        std::cout << "  Total data:       " << FormatBytes(total_bytes_)
                  << "\n";
        std::cout << "  Throughput:       " << ThroughputMBps() << " MB/s";
        if (ThroughputMBps() > 1024) {
            std::cout << " (" << ThroughputMBps() / 1024 << " GB/s)";
        }
        std::cout << "\n";
        std::cout << "  Keys/sec:         " << KeysPerSec() << "\n";
        std::cout << "  Queries/sec:      " << QueriesPerSec() << "\n";

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

   private:
    std::vector<ThreadResult> thread_results_;
    std::vector<int64_t> merged_latencies_ns_;
    size_t total_bytes_ = 0;
    size_t total_keys_ = 0;
    size_t total_queries_ = 0;
    size_t total_failed_ = 0;
    Clock::time_point start_;
    Clock::time_point end_;
};

class GetIntoRangesBench {
   public:
    GetIntoRangesBench() : client_(mooncake::RealClient::create()) {}

    ~GetIntoRangesBench() {
        if (!client_) return;
        for (auto& tb : thread_buffers_) {
            if (tb.ptr) {
                try {
                    client_->unregister_buffer(tb.ptr);
                } catch (...) {
                }
                numa_free(tb.ptr, tb.size);
                tb.ptr = nullptr;
            }
        }
        if (buffer_) {
            try {
                client_->unregister_buffer(buffer_);
            } catch (...) {
            }
            numa_free(buffer_, buffer_size_);
            buffer_ = nullptr;
        }
        client_ = nullptr;
    }

    int Setup() {
        int ret = client_->setup_real(
            FLAGS_local_hostname, FLAGS_metadata_server,
            FLAGS_global_segment_size, FLAGS_local_buffer_size, FLAGS_protocol,
            FLAGS_device_name, FLAGS_master_server, nullptr, "");
        if (ret != 0) {
            LOG(ERROR) << "RealClient setup_real failed, ret=" << ret;
            return ret;
        }
        LOG(INFO) << "RealClient setup succeeded";

        // Per-key buffer for prefill / verify.
        buffer_size_ = FLAGS_value_size;
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
        return 0;
    }

    int Run() {
        if (FLAGS_scenario == "segment_range_write") {
            return RunSegmentRangeWrite();
        } else if (FLAGS_scenario == "segment_range_read") {
            return RunSegmentRangeRead();
        }
        LOG(ERROR) << "Unknown scenario: " << FLAGS_scenario
                   << " (expected segment_range_write or segment_range_read)";
        return -1;
    }

   private:
    // Resolve the segment list from --segments or auto-discover from master.
    std::vector<std::string> DiscoverSegmentsIfNeeded(
        const std::string& context) {
        auto segments = ParseSegments(FLAGS_segments);
        if (!segments.empty()) return segments;

        LOG(INFO) << context << ", auto-discovering from master at "
                  << FLAGS_master_server << ":" << FLAGS_master_admin_port;
        segments = DiscoverSegmentsFromMaster(
            FLAGS_master_server, static_cast<int>(FLAGS_master_admin_port));
        if (segments.empty()) {
            LOG(ERROR) << "No segments discovered. Check master connectivity.";
        }
        return segments;
    }

    // ---- Scenario 1: segment_range_write -------------------------------
    // Write FLAGS_num_keys keys of FLAGS_value_size bytes onto each segment,
    // hard-pinned per segment via preferred_segments.  Key naming matches
    // MakeSegmentKey so the reader side can reconstruct the same keys.
    int RunSegmentRangeWrite() {
        auto segments = DiscoverSegmentsIfNeeded(
            "--segments not specified, auto-discovering");
        if (segments.empty()) return -1;
        LOG(INFO) << "Discovered " << segments.size() << " segments";

        LOG(INFO) << "=== SEGMENT RANGE WRITE MODE ===";
        LOG(INFO) << "Writing to " << segments.size() << " segments, "
                  << FLAGS_num_keys << " keys per segment, each "
                  << FormatBytes(FLAGS_value_size);

        std::vector<size_t> seg_written(segments.size(), 0);
        std::vector<size_t> seg_failed(segments.size(), 0);
        std::vector<mooncake::ReplicateConfig> configs(segments.size());
        for (size_t s = 0; s < segments.size(); ++s) {
            configs[s].replica_num = FLAGS_replica_num;
            configs[s].with_hard_pin = true;  // pin so data survives until read
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
                int ret = client_->put_from(key, buffer_, FLAGS_value_size,
                                            configs[s]);
                auto t1 = Clock::now();

                if (ret != 0) {
                    LOG(ERROR) << "put_from failed for key=" << key
                               << " segment=" << segment << " ret=" << ret;
                    ++seg_failed[s];
                    continue;
                }
                ++seg_written[s];
                if (VLOG_IS_ON(1)) {
                    LOG(INFO) << "  wrote " << key << " in "
                              << NanosToUs(ElapsedNanos(t0, t1)) << " us";
                }
            }

            if ((i + 1) % 10 == 0 || i == FLAGS_num_keys - 1) {
                LOG(INFO) << "  Written " << (i + 1) << "/" << FLAGS_num_keys
                          << " keys to all " << segments.size()
                          << " segments";
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

    // ---- Scenario 2: segment_range_read --------------------------------
    // Read keys back from segments.  In ranged mode each value is read as
    // frags_per_key fragments of fragment_size bytes at random source offsets
    // via get_into_ranges; in full mode each value is read in one go via
    // get_into (baseline).
    int RunSegmentRangeRead() {
        auto segments = DiscoverSegmentsIfNeeded(
            "--segments not specified, auto-discovering");
        if (segments.empty()) return -1;
        LOG(INFO) << "Discovered " << segments.size() << " segments";

        size_t read_segment_nums = FLAGS_read_segment_nums;
        if (read_segment_nums == 0 || read_segment_nums > segments.size()) {
            read_segment_nums = segments.size();
        }
        std::vector<std::string> read_segments(
            segments.begin(), segments.begin() + read_segment_nums);

        // Validate fragment configuration for ranged mode.
        const bool ranged_mode = (FLAGS_read_mode == "ranged");
        size_t fragment_size = FLAGS_fragment_size;
        size_t frags_per_key = 1;
        if (ranged_mode) {
            if (FLAGS_fragment_size == 0) {
                LOG(ERROR) << "fragment_size must be >= 1";
                return -1;
            }
            if (FLAGS_value_size % FLAGS_fragment_size != 0) {
                LOG(ERROR) << "value_size (" << FLAGS_value_size
                           << ") must be divisible by fragment_size ("
                           << FLAGS_fragment_size << ")";
                return -1;
            }
            if (FLAGS_fragment_size > FLAGS_value_size) {
                LOG(ERROR) << "fragment_size (" << FLAGS_fragment_size
                           << ") cannot exceed value_size ("
                           << FLAGS_value_size << ")";
                return -1;
            }
            fragment_size = FLAGS_fragment_size;
            frags_per_key = FLAGS_value_size / FLAGS_fragment_size;
        }

        LOG(INFO) << "=== SEGMENT RANGE READ MODE ===";
        LOG(INFO) << "Reading from " << read_segments.size() << " segment(s)";
        for (size_t s = 0; s < read_segments.size(); ++s) {
            LOG(INFO) << "  Segment [" << s << "]: " << read_segments[s];
        }
        LOG(INFO) << "Keys per segment: " << FLAGS_num_keys;
        LOG(INFO) << "Read mode:        " << FLAGS_read_mode;
        if (ranged_mode) {
            LOG(INFO) << "  Fragment size:   " << FormatBytes(fragment_size)
                      << " (" << frags_per_key
                      << " random-offset fragments per key)";
        }
        LOG(INFO) << "Keys per query:   " << FLAGS_keys_per_query;
        LOG(INFO) << "Threads:          " << FLAGS_num_threads;

        int buf_ret = AllocateThreadBuffers(FLAGS_num_threads);
        if (buf_ret != 0) return buf_ret;

        // Build the full key list across all read segments.
        std::vector<std::string> all_keys;
        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
            for (size_t s = 0; s < read_segments.size(); ++s) {
                all_keys.push_back(MakeSegmentKey(read_segments[s], i));
            }
        }
        LOG(INFO) << "Total keys to read: " << all_keys.size();

        // Warmup.
        DoWarmup(all_keys, ranged_mode, fragment_size, frags_per_key);

        // Benchmark.
        const size_t total_queries =
            (all_keys.size() + FLAGS_keys_per_query - 1) / FLAGS_keys_per_query;

        BenchmarkStats stats;
        stats.InitThreads(FLAGS_num_threads, total_queries / FLAGS_num_threads);
        stats.StartTimer();

        std::latch start_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        std::latch done_latch(static_cast<ptrdiff_t>(FLAGS_num_threads));
        auto threads = LaunchWorkers(FLAGS_num_threads, total_queries, stats,
                                     start_latch, done_latch, all_keys,
                                     ranged_mode, fragment_size, frags_per_key);

        done_latch.wait();
        stats.StopTimer();
        for (auto& th : threads) th.join();

        stats.Finalize();

        std::ostringstream title;
        title << "SEGMENT RANGE READ BENCHMARK [segments=" << read_segments.size()
              << ", keys=" << all_keys.size()
              << ", value=" << FormatBytes(FLAGS_value_size)
              << ", mode=" << FLAGS_read_mode;
        if (ranged_mode) {
            title << ", frag_size=" << fragment_size
                  << ", frags/key=" << frags_per_key;
        }
        title << ", keys/query=" << FLAGS_keys_per_query
              << ", threads=" << FLAGS_num_threads << "]";
        stats.Print(title.str());

        if (FLAGS_verify) {
            int v = VerifyData(all_keys);
            LOG_IF(INFO, v == 0) << "Data verification PASSED";
            LOG_IF(ERROR, v != 0) << "Data verification FAILED";
        }
        return 0;
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
                           << " for seed=" << seed;
                return false;
            }
        }
        return true;
    }

    void DoWarmup(const std::vector<std::string>& all_keys, bool ranged_mode,
                  size_t fragment_size, size_t frags_per_key) {
        if (FLAGS_warmup_keys == 0) return;
        LOG(INFO) << "Warmup: reading " << FLAGS_warmup_keys << " keys...";
        size_t warmup_end = std::min(static_cast<size_t>(FLAGS_warmup_keys),
                                     all_keys.size());
        std::mt19937_64 rng(0x1234);
        for (size_t i = 0; i < warmup_end; ++i) {
            const std::string& key = all_keys[i];
            if (ranged_mode) {
                std::vector<void*> buffers = {buffer_};
                std::vector<std::vector<std::string>> all_k = {{key}};
                std::vector<std::vector<std::vector<size_t>>> all_dst(1);
                std::vector<std::vector<std::vector<size_t>>> all_src(1);
                std::vector<std::vector<std::vector<size_t>>> all_sizes(1);
                std::vector<size_t> dst_off(frags_per_key);
                std::vector<size_t> src_off(frags_per_key);
                std::vector<size_t> sizes(frags_per_key, fragment_size);
                const size_t src_range =
                    FLAGS_value_size - fragment_size + 1;
                for (size_t k = 0; k < frags_per_key; ++k) {
                    dst_off[k] = k * fragment_size;
                    src_off[k] = static_cast<size_t>(rng()) % src_range;
                }
                all_dst[0].push_back(std::move(dst_off));
                all_src[0].push_back(std::move(src_off));
                all_sizes[0].push_back(std::move(sizes));
                auto results = client_->get_into_ranges(buffers, all_k, all_dst,
                                                        all_src, all_sizes);
                for (const auto& br : results)
                    for (const auto& kr : br)
                        for (int64_t r : kr)
                            if (r < 0)
                                LOG(WARNING)
                                    << "Warmup get_into_ranges failed key="
                                    << key << " ret=" << r;
            } else {
                int64_t ret =
                    client_->get_into(key, buffer_, FLAGS_value_size);
                if (ret < 0) {
                    LOG(WARNING) << "Warmup get_into failed key=" << key
                                 << " ret=" << ret;
                }
            }
        }
        LOG(INFO) << "Warmup complete";
    }

    // Execute one ranged query for keys [key_start, key_start+count) into buf.
    // Each value is read as frags_per_key fragments of fragment_size bytes;
    // source offsets are chosen randomly by rng within
    // [0, value_size - fragment_size].  Returns total bytes read.
    int64_t ExecuteRangedQuery(char* buf, size_t key_start, size_t count,
                               size_t fragment_size, size_t frags_per_key,
                               const std::vector<std::string>& all_keys,
                               std::mt19937_64& rng) {
        const size_t src_range = FLAGS_value_size - fragment_size + 1;

        std::vector<void*> buffers = {buf};
        std::vector<std::vector<std::string>> all_k(1);
        std::vector<std::vector<std::vector<size_t>>> all_dst(1);
        std::vector<std::vector<std::vector<size_t>>> all_src(1);
        std::vector<std::vector<std::vector<size_t>>> all_sizes(1);

        all_k[0].reserve(count);
        all_dst[0].reserve(count);
        all_src[0].reserve(count);
        all_sizes[0].reserve(count);

        for (size_t j = 0; j < count; ++j) {
            all_k[0].push_back(all_keys[key_start + j]);

            const size_t dst_base = j * FLAGS_value_size;
            std::vector<size_t> dst_off(frags_per_key);
            std::vector<size_t> src_off(frags_per_key);
            std::vector<size_t> sizes(frags_per_key, fragment_size);
            for (size_t k = 0; k < frags_per_key; ++k) {
                dst_off[k] = dst_base + k * fragment_size;
                src_off[k] = static_cast<size_t>(rng()) % src_range;
            }
            all_dst[0].push_back(std::move(dst_off));
            all_src[0].push_back(std::move(src_off));
            all_sizes[0].push_back(std::move(sizes));
        }

        auto results = client_->get_into_ranges(buffers, all_k, all_dst,
                                                all_src, all_sizes);
        int64_t total = 0;
        for (const auto& br : results)
            for (const auto& kr : br)
                for (int64_t r : kr)
                    if (r > 0) total += r;
        return total;
    }

    // Execute one full-read query for keys [key_start, key_start+count) into
    // buf via get_into.  Returns total bytes read.
    int64_t ExecuteFullQuery(char* buf, size_t key_start, size_t count,
                             const std::vector<std::string>& all_keys) {
        int64_t total = 0;
        for (size_t j = 0; j < count; ++j) {
            int64_t ret = client_->get_into(all_keys[key_start + j],
                                            buf + j * FLAGS_value_size,
                                            FLAGS_value_size);
            if (ret > 0) total += ret;
        }
        return total;
    }

    void Worker(size_t tid, size_t my_queries, size_t query_offset,
                BenchmarkStats& stats, std::latch& start_latch,
                std::latch& done_latch, const std::vector<std::string>& all_keys,
                bool ranged_mode, size_t fragment_size, size_t frags_per_key) {
        bindToSocket(tid % NR_SOCKETS);
        ThreadResult& result = stats.GetThreadResult(tid);
        result.latencies_ns.reserve(my_queries);

        char* my_buf = thread_buffers_[tid].ptr;
        std::mt19937_64 rng(0xC0FFEEULL + tid);
        start_latch.arrive_and_wait();

        size_t keys = 0;
        size_t queries = 0;
        size_t failed = 0;
        size_t bytes = 0;
        const size_t total_keys = all_keys.size();

        for (size_t q = 0; q < my_queries; ++q) {
            size_t global_q = query_offset + q;
            size_t key_start = global_q * FLAGS_keys_per_query;
            size_t count = std::min(FLAGS_keys_per_query,
                                    total_keys - key_start);
            if (count == 0) break;

            auto t0 = Clock::now();
            int64_t ret = ranged_mode
                              ? ExecuteRangedQuery(my_buf, key_start, count,
                                                   fragment_size, frags_per_key,
                                                   all_keys, rng)
                              : ExecuteFullQuery(my_buf, key_start, count,
                                                 all_keys);
            auto t1 = Clock::now();

            result.latencies_ns.push_back(ElapsedNanos(t0, t1));
            if (ret <= 0) {
                ++failed;
            } else {
                bytes += static_cast<size_t>(ret);
            }
            keys += count;
            ++queries;
        }

        result.total_bytes = bytes;
        result.total_keys = keys;
        result.total_queries = queries;
        result.failed_ops = failed;
        done_latch.arrive_and_wait();
    }

    std::vector<std::thread> LaunchWorkers(
        size_t num_threads, size_t total_queries, BenchmarkStats& stats,
        std::latch& start_latch, std::latch& done_latch,
        const std::vector<std::string>& all_keys, bool ranged_mode,
        size_t fragment_size, size_t frags_per_key) {
        std::vector<std::thread> threads;
        size_t queries_per_thread = total_queries / num_threads;
        size_t remainder = total_queries % num_threads;

        for (size_t t = 0; t < num_threads; ++t) {
            size_t my_queries =
                queries_per_thread + (t < remainder ? 1 : 0);
            size_t query_offset =
                t * queries_per_thread + std::min(t, remainder);
            threads.emplace_back([&, t, my_queries, query_offset]() {
                Worker(t, my_queries, query_offset, stats, start_latch,
                       done_latch, all_keys, ranged_mode, fragment_size,
                       frags_per_key);
            });
        }
        return threads;
    }

    int VerifyData(const std::vector<std::string>& all_keys) {
        LOG(INFO) << "Verifying data integrity for " << all_keys.size()
                  << " keys...";
        int errors = 0;
        for (size_t i = 0; i < all_keys.size(); ++i) {
            // Reconstruct the seed used during write.  Keys are laid out as
            // MakeSegmentKey(segment, idx) for idx in [0, num_keys) across
            // segments; the seed depends only on idx.
            size_t idx = i % FLAGS_num_keys;
            int64_t ret =
                client_->get_into(all_keys[i], buffer_, FLAGS_value_size);
            if (ret < 0) {
                LOG(ERROR) << "Verify: get_into failed for key=" << all_keys[i];
                ++errors;
                continue;
            }
            if (!CheckBuffer(idx, buffer_, static_cast<size_t>(ret))) {
                LOG(ERROR) << "Verify: data mismatch for key=" << all_keys[i];
                ++errors;
            }
        }
        LOG(INFO) << "Verification complete: " << errors << " errors out of "
                  << all_keys.size() << " keys";
        return errors > 0 ? -1 : 0;
    }

    int AllocateThreadBuffers(size_t num_threads) {
        thread_buffers_.resize(num_threads);
        size_t per_buf_size = FLAGS_keys_per_query * FLAGS_value_size;
        for (size_t t = 0; t < num_threads; ++t) {
            int node = t % NR_SOCKETS;
            thread_buffers_[t].size = per_buf_size;
            thread_buffers_[t].ptr = reinterpret_cast<char*>(
                numa_alloc_onnode(per_buf_size, node));
            if (!thread_buffers_[t].ptr) {
                LOG(ERROR) << "Failed to allocate buffer for thread " << t;
                return -1;
            }
            std::memset(thread_buffers_[t].ptr, 0, per_buf_size);
            int ret = client_->register_buffer(thread_buffers_[t].ptr,
                                               per_buf_size);
            if (ret != 0) {
                LOG(ERROR) << "register_buffer failed for thread " << t;
                return ret;
            }
        }
        LOG(INFO) << "Allocated " << num_threads << " thread buffers, each "
                  << FormatBytes(per_buf_size) << " (NUMA-aware, "
                  << NR_SOCKETS << " sockets)";
        return 0;
    }

    std::shared_ptr<mooncake::RealClient> client_;
    char* buffer_ = nullptr;
    size_t buffer_size_ = 0;

    struct ThreadBuffer {
        char* ptr = nullptr;
        size_t size = 0;
    };
    std::vector<ThreadBuffer> thread_buffers_;
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

    LOG(INFO) << "Mooncake stress_cluster_ranges Benchmark";
    LOG(INFO) << "  Scenario:        " << FLAGS_scenario;
    LOG(INFO) << "  Protocol:        " << FLAGS_protocol;
    LOG(INFO) << "  Value size:      " << FormatBytes(FLAGS_value_size);
    LOG(INFO) << "  Num keys:        " << FLAGS_num_keys;
    if (FLAGS_scenario == "segment_range_read") {
        LOG(INFO) << "  Read mode:       " << FLAGS_read_mode;
        LOG(INFO) << "  Keys/query:      " << FLAGS_keys_per_query;
        if (FLAGS_read_mode == "ranged") {
            LOG(INFO) << "  Fragment size:   " << FLAGS_fragment_size
                      << " bytes";
            LOG(INFO) << "  Fragments/key:   "
                      << (FLAGS_value_size / FLAGS_fragment_size)
                      << " (random source offsets)";
        }
        LOG(INFO) << "  Read seg nums:   " << FLAGS_read_segment_nums;
    }
    LOG(INFO) << "  Num threads:     " << FLAGS_num_threads;
    LOG(INFO) << "  Hard pin:        " << (FLAGS_hard_pin ? "yes" : "no");
    if (!FLAGS_segments.empty()) {
        LOG(INFO) << "  Segments:        " << FLAGS_segments;
    } else {
        LOG(INFO) << "  Segments:        auto-discover from master";
    }
    LOG(INFO) << "  Master admin:    " << FLAGS_master_admin_port;

    size_t total_data = FLAGS_num_keys * FLAGS_value_size;
    if (total_data > FLAGS_global_segment_size * 9.5 / 10) {
        LOG(WARNING) << "Total data per segment (" << FormatBytes(total_data)
                     << ") may exceed 95% of segment ("
                     << FormatBytes(FLAGS_global_segment_size)
                     << "). Consider --hard_pin=true.";
    }

    GetIntoRangesBench bench;
    int ret = bench.Setup();
    if (ret != 0) {
        LOG(ERROR) << "Benchmark setup failed";
        return ret;
    }
    return bench.Run();
}
