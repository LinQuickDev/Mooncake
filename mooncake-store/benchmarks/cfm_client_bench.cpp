// CFM client benchmark that models the vLLM KV-cache call path.
//
// One benchmark request consists of prompt_tokens + output_tokens. The KV
// cache is split into tokens_per_block blocks for every transformer layer,
// exactly as a vLLM connector would address its layer/block cache entries.
// Each request records inference and access metrics, reports a snapshot through
// CfmClientImpl, then prints both CFM call latency and IO Pattern metrics.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "io_pattern/cfm_client_impl.h"
#include "io_pattern/cfm_protocol.h"
#include "io_pattern/cfm_service.h"
#include "io_pattern/rpc_transport.h"
#include "io_pattern/runtime.h"

namespace {

using Clock = std::chrono::steady_clock;
using mooncake::ErrorCode;
using mooncake::TenantId;
using namespace mooncake::io_pattern;

DEFINE_uint64(requests, 20, "Number of vLLM-style inference requests");
DEFINE_uint64(prompt_tokens, 1024, "Input tokens in each inference request");
DEFINE_uint64(output_tokens, 128, "Decode tokens in each inference request");
DEFINE_uint64(tokens_per_block, 16, "Tokens represented by one KV block");
DEFINE_uint64(num_layers, 32, "Transformer layers represented per request");
DEFINE_uint64(kv_block_bytes, 256 * 1024,
              "Bytes in one layer/block KV-cache object");
DEFINE_uint64(num_sessions, 4, "Independent vLLM request sessions");
DEFINE_uint64(shared_prefix_tokens, 512,
              "Per-session prompt prefix reused by later requests");
DEFINE_uint64(report_capacity, 262144,
              "Maximum queued IO Pattern observations before reporting");
DEFINE_uint64(policy_queue_capacity, 4096,
              "Maximum CFM policy commands queued for the client");
DEFINE_uint64(report_flush_wait_ms, 1100,
              "Maximum wait for remote CFM policy production after a flush");
DEFINE_double(memory_used_ratio, 0.95,
              "Reported L1 memory use ratio; >= 0.90 triggers CFM planning");
DEFINE_string(tenant, "vllm-benchmark", "Tenant id");
DEFINE_string(node_id, "vllm-submaster-0", "CFM node/submaster id");
DEFINE_string(cfm_endpoint, "",
              "Remote CFM coro_rpc endpoint; empty uses an embedded CFM service");
DEFINE_string(cfm_auth_token, "cfm-client-benchmark-node-token",
              "Authentication token for the CFM node endpoint");

uint64_t SteadyNowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            Clock::now().time_since_epoch())
            .count());
}

double ToMicroseconds(Clock::duration duration) {
    return std::chrono::duration<double, std::micro>(duration).count();
}

size_t BlockCount(uint64_t tokens) {
    return static_cast<size_t>((tokens + FLAGS_tokens_per_block - 1) /
                               FLAGS_tokens_per_block);
}

std::string KvKey(size_t session, size_t request, size_t layer, size_t block,
                  bool is_shared_prefix) {
    const auto owner = is_shared_prefix ? std::string("prefix")
                                        : std::string("request-") +
                                              std::to_string(request);
    return "vllm/" + FLAGS_node_id + "/session-" +
           std::to_string(session) + "/" + owner + "/layer-" +
           std::to_string(layer) + "/block-" + std::to_string(block);
}

class ServiceBackedCfmTransport final : public CfmRpcTransport {
   public:
    ServiceBackedCfmTransport(std::shared_ptr<CfmService> service,
                              std::string node_id)
        : service_(std::move(service)), node_id_(std::move(node_id)) {}

    bool Authenticate(std::string_view token) override {
        if (!service_ || !service_->AuthenticateNode(token)) return false;
        token_ = std::string(token);
        authenticated_ = true;
        return true;
    }

    bool Send(std::string_view method, std::string_view payload,
              std::chrono::milliseconds) override {
        return authenticated_ && service_ &&
               service_->Send(node_id_, method, payload, token_);
    }

    CfmReceiveResult Receive(std::string_view method,
                             std::chrono::milliseconds) override {
        if (!authenticated_ || !service_ || method != "poll_policy") {
            return CfmReceiveResult::Error();
        }
        const auto delivery = service_->PollPolicy(node_id_, token_);
        return delivery
                   ? CfmReceiveResult::Payload(delivery->second, delivery->first)
                   : CfmReceiveResult::Empty();
    }

    bool Acknowledge(uint64_t delivery_id, bool success,
                     std::chrono::milliseconds) override {
        return authenticated_ && service_ &&
               service_->AcknowledgePolicy(node_id_, delivery_id, success,
                                           token_);
    }

   private:
    std::shared_ptr<CfmService> service_;
    std::string node_id_;
    std::string token_;
    bool authenticated_{false};
};

class LatencyStats final {
   public:
    void Record(double value_us) { values_us_.push_back(value_us); }

    double Percentile(double percentile) const {
        if (values_us_.empty()) return 0.0;
        const double rank = percentile / 100.0 * (values_us_.size() - 1);
        const auto lower = static_cast<size_t>(rank);
        const auto upper = std::min(lower + 1, values_us_.size() - 1);
        const double fraction = rank - lower;
        return values_us_[lower] * (1.0 - fraction) +
               values_us_[upper] * fraction;
    }

    void Finalize() { std::sort(values_us_.begin(), values_us_.end()); }

    double Mean() const {
        if (values_us_.empty()) return 0.0;
        return std::accumulate(values_us_.begin(), values_us_.end(), 0.0) /
               values_us_.size();
    }

   private:
    std::vector<double> values_us_;
};

struct MetricReportSnapshot {
    uint64_t calls{0};
    uint64_t failures{0};
    uint64_t observations{0};
    LatencyStats latency;
};

class MetricReportStats final {
   public:
    void Record(const MetricBatch& batch, double latency_us, bool success) {
        std::lock_guard lock(mutex_);
        ++calls;
        if (!success) ++failures;
        observations +=
            batch.inference.size() + batch.accesses.size() + batch.storage.size();
        latency.Record(latency_us);
    }

    MetricReportSnapshot Finalize() {
        std::lock_guard lock(mutex_);
        latency.Finalize();
        return {.calls = calls,
                .failures = failures,
                .observations = observations,
                .latency = latency};
    }

   private:
    std::mutex mutex_;
    uint64_t calls{0};
    uint64_t failures{0};
    uint64_t observations{0};
    LatencyStats latency;
};

struct RequestData {
    IoPatternSnapshot snapshot;
    std::vector<InferenceMetrics> inference;
    std::vector<AccessRecord> accesses;
};

RequestData BuildRequest(size_t request_index) {
    const size_t session = request_index % FLAGS_num_sessions;
    const uint64_t total_tokens = FLAGS_prompt_tokens + FLAGS_output_tokens;
    const size_t blocks = BlockCount(total_tokens);
    const size_t shared_blocks =
        std::min(blocks, BlockCount(FLAGS_shared_prefix_tokens));
    const bool prefix_is_cached = request_index >= FLAGS_num_sessions;
    const uint64_t now_ns = SteadyNowNs();

    RequestData request;
    request.snapshot.generated_at_ns = now_ns;
    request.inference.reserve(blocks * FLAGS_num_layers);
    request.accesses.reserve(blocks * FLAGS_num_layers);
    request.snapshot.keys.reserve(blocks * FLAGS_num_layers);
    const auto tenant = TenantId(FLAGS_tenant);
    const auto session_id = "vllm-session-" + std::to_string(session);

    for (size_t layer = 0; layer < FLAGS_num_layers; ++layer) {
        for (size_t block = 0; block < blocks; ++block) {
            const bool is_shared_prefix = block < shared_blocks;
            const bool is_hit = is_shared_prefix && prefix_is_cached;
            const ObjectRef object{
                .tenant_id = tenant,
                .key = KvKey(session, request_index, layer, block,
                             is_shared_prefix)};
            const auto block_end = std::min<uint64_t>(
                total_tokens, (block + 1) * FLAGS_tokens_per_block);
            const auto block_tokens = static_cast<uint32_t>(
                block_end - block * FLAGS_tokens_per_block);

            InferenceMetrics inference{
                .object = object,
                .session_id = session_id,
                .layout = CacheLayout::kLayerFirst,
                .layout_group = static_cast<uint32_t>(layer),
                .prefix_depth = static_cast<uint32_t>(shared_blocks),
                .prefix_fanout = static_cast<uint32_t>(FLAGS_num_sessions),
                .match_length = is_hit
                                    ? static_cast<uint32_t>(
                                          FLAGS_shared_prefix_tokens)
                                    : 0U,
                .continuous_prefix_length = is_hit
                                                ? static_cast<uint32_t>(
                                                      FLAGS_shared_prefix_tokens)
                                                : 0U,
                .token_count = block_tokens,
                .recompute_cost = is_hit ? 0.0F : static_cast<float>(block_tokens),
                .request_priority = 1};
            AccessRecord access{
                .object = object,
                .observed_at_ns = now_ns,
                .block_size = FLAGS_kv_block_bytes,
                .latency_us = is_hit ? 20U : 200U,
                .tier = CacheTier::kL1Host,
                .operation = is_hit ? IoOperation::kGet : IoOperation::kPut,
                .is_hit = is_hit,
                .write_batch_size = is_hit
                                        ? 0U
                                        : static_cast<uint32_t>(FLAGS_num_layers),
                .overwrite = !is_hit && is_shared_prefix};
            request.inference.push_back(inference);
            request.accesses.push_back(access);
            request.snapshot.keys.push_back(
                KeyMetrics{.object = object,
                           .session_id = session_id,
                           .last_access_time_ns = now_ns,
                           .access_count_window = 1,
                           .block_size = FLAGS_kv_block_bytes,
                           .token_count = block_tokens,
                           .prefix_depth = static_cast<uint32_t>(shared_blocks),
                           .prefix_fanout = static_cast<uint32_t>(FLAGS_num_sessions),
                           .match_length = inference.match_length,
                           .continuous_prefix_length =
                               inference.continuous_prefix_length,
                           .write_batch_size = access.write_batch_size,
                           .write_frequency =
                               access.operation == IoOperation::kPut ? 1U : 0U,
                           .write_object_size = FLAGS_kv_block_bytes,
                           .recompute_cost = inference.recompute_cost,
                           .overwrite_ratio = access.overwrite ? 1.0F : 0.0F,
                           .replica_tiers = CacheTierBit(CacheTier::kL1Host),
                           .layout = CacheLayout::kLayerFirst,
                           .layout_group = static_cast<uint32_t>(layer),
                           .request_priority = 1,
                           .active = is_hit,
                           .write_burst = !is_hit});
        }
    }
    request.snapshot.storage.push_back(
        StorageMetric{.source_id = FLAGS_node_id,
                      .observed_at_ns = now_ns,
                      .tier = CacheTier::kL1Host,
                      .read_bandwidth_bytes_per_sec = 20ULL * 1024 * 1024 * 1024,
                      .write_bandwidth_bytes_per_sec = 10ULL * 1024 * 1024 * 1024,
                      .read_latency_us = 20,
                      .write_latency_us = 200,
                      .used_bytes = static_cast<uint64_t>(
                          FLAGS_memory_used_ratio * 1024 * 1024 * 1024),
                      .capacity_bytes = 1024ULL * 1024 * 1024,
                      .rpc_latency_us = 100,
                      .memory_used_ratio =
                          static_cast<float>(FLAGS_memory_used_ratio)});
    return request;
}

void PrintObservability(std::string_view name,
                        const IoPatternObservabilitySnapshot& metrics) {
    std::cout << "\n  " << name << " IO Pattern metrics\n"
              << "    collect max latency:  " << metrics.collect_latency_us
              << " us\n"
              << "    analyze max latency:  " << metrics.analyze_latency_us
              << " us\n"
              << "    policy decisions:     " << metrics.policy_decisions
              << " (" << std::fixed << std::setprecision(2)
              << metrics.policy_decision_qps << " qps)\n"
              << "    strategy hit rate:    " << metrics.strategy_hit_rate * 100
              << "%\n"
              << "    false positive rate:  "
              << metrics.false_positive_rate * 100 << "%\n"
              << "    degraded:             " << metrics.degrade_count << "\n"
              << "    report drops:         " << metrics.report_drop_count
              << "\n";
}

bool ValidateFlags() {
    return FLAGS_requests != 0 && FLAGS_prompt_tokens + FLAGS_output_tokens != 0 &&
           FLAGS_tokens_per_block != 0 && FLAGS_num_layers != 0 &&
           FLAGS_kv_block_bytes != 0 && FLAGS_num_sessions != 0 &&
           FLAGS_report_capacity != 0 && FLAGS_policy_queue_capacity != 0 &&
           FLAGS_memory_used_ratio >= 0.0 && FLAGS_memory_used_ratio <= 1.0;
}

}  // namespace

int main(int argc, char* argv[]) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (!ValidateFlags()) {
        LOG(ERROR) << "All numeric size/count flags must be positive and "
                      "--memory_used_ratio must be within [0, 1]";
        return 1;
    }

    constexpr char kProducerToken[] = "cfm-client-benchmark-producer-token";
    std::atomic<uint64_t> eviction_commands{0};
    std::atomic<uint64_t> prefetch_commands{0};
    std::atomic<uint64_t> admission_commands{0};

    std::shared_ptr<CfmService> embedded_service;
    std::shared_ptr<CfmRpcTransport> transport;
    if (FLAGS_cfm_endpoint.empty()) {
        auto cfm_control_runtime = std::make_shared<IoPatternRuntime>(
            IoPatternRuntime::Handlers{
                .eviction = [](const EvictionPlan&) { return ErrorCode::OK; },
                .prefetch = [](const PrefetchPlan&) { return ErrorCode::OK; },
                .admission = [](const AdmissionResult&) {
                    return ErrorCode::OK;
                }});
        embedded_service = std::make_shared<CfmService>(
            cfm_control_runtime, FLAGS_cfm_auth_token,
            FLAGS_policy_queue_capacity, kProducerToken);
        transport = std::make_shared<ServiceBackedCfmTransport>(
            embedded_service, FLAGS_node_id);
    } else {
        transport = std::make_shared<CoroRpcCfmTransport>(
            FLAGS_cfm_endpoint, FLAGS_node_id, std::chrono::milliseconds(500));
    }
    auto codec = std::make_shared<CfmBinaryCodec>();
    auto channel = std::make_shared<CfmRpcChannel>(
        transport, codec,
        CfmRpcConfig{.timeout = std::chrono::milliseconds(500),
                     .auth_token = FLAGS_cfm_auth_token});

    IoPatternRuntime::Config source_config;
    source_config.report_capacity = FLAGS_report_capacity;
    MetricReportStats metric_reports;
    source_config.report_sink = [&channel, &metric_reports](const MetricBatch& batch) {
        const auto started = Clock::now();
        const bool success = channel->SendMetricBatch(batch);
        metric_reports.Record(batch, ToMicroseconds(Clock::now() - started),
                              success);
        return success;
    };
    auto source_runtime = std::make_shared<IoPatternRuntime>(
        IoPatternRuntime::Handlers{
            .eviction = [&eviction_commands](const EvictionPlan&) {
                ++eviction_commands;
                return ErrorCode::OK;
            },
            .prefetch = [&prefetch_commands](const PrefetchPlan&) {
                ++prefetch_commands;
                return ErrorCode::OK;
            },
            .admission = [&admission_commands](const AdmissionResult&) {
                ++admission_commands;
                return ErrorCode::OK;
            }},
        source_config);
    CfmClientImpl client(channel, [&source_runtime](const PolicyCommand& command) {
        return source_runtime->ExecuteCommand(command);
    });

    LatencyStats report_latency;
    uint64_t failed_reports = 0;
    uint64_t total_blocks = 0;
    const auto benchmark_start = Clock::now();
    for (size_t request_index = 0; request_index < FLAGS_requests;
         ++request_index) {
        auto request = BuildRequest(request_index);
        total_blocks += request.accesses.size();
        for (size_t i = 0; i < request.inference.size(); ++i) {
            source_runtime->ReportInferenceMetrics(request.inference[i]);
            source_runtime->RecordAccess(request.accesses[i].object.key,
                                         request.accesses[i]);
        }
        source_runtime->RecordStorageMetric(request.snapshot.storage.front());

        const auto report_start = Clock::now();
        const auto result = client.ReportSnapshot(request.snapshot);
        report_latency.Record(ToMicroseconds(Clock::now() - report_start));
        if (result != ErrorCode::OK) ++failed_reports;
    }
    const auto submission_seconds =
        std::chrono::duration<double>(Clock::now() - benchmark_start).count();

    // Stop joins the reporter worker and performs its final flush. No new
    // metric batch can reach CFM after this returns.
    source_runtime->StopReports();
    if (embedded_service) {
        if (!embedded_service->WaitForPolicyIdle(
                std::chrono::milliseconds(FLAGS_report_flush_wait_ms))) {
            LOG(WARNING) << "Timed out waiting for embedded CFM policy production";
        }
    } else {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(FLAGS_report_flush_wait_ms));
    }
    uint64_t observed_commands = 0;
    bool policy_drain_complete = false;
    const auto policy_deadline =
        Clock::now() + std::chrono::milliseconds(FLAGS_report_flush_wait_ms);
    while (Clock::now() < policy_deadline) {
        if (client.PollAndDispatchPolicy() != ErrorCode::OK) break;
        const uint64_t executed = eviction_commands + prefetch_commands +
                                  admission_commands;
        if (executed == observed_commands) {
            policy_drain_complete = embedded_service != nullptr;
            break;
        } else {
            observed_commands = executed;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    const auto end_to_end_seconds =
        std::chrono::duration<double>(Clock::now() - benchmark_start).count();

    const auto source_snapshot = source_runtime->Snapshot();
    const auto source_metrics =
        source_runtime->ObservabilitySnapshot(end_to_end_seconds);
    source_runtime.reset();
    report_latency.Finalize();
    const auto metric_report_snapshot = metric_reports.Finalize();
    const auto cfm_snapshot = embedded_service
                                  ? embedded_service->SnapshotForNode(FLAGS_node_id)
                                  : IoPatternSnapshot{};
    const auto cfm_metrics = embedded_service
                                 ? embedded_service->ObservabilityForNode(
                                       FLAGS_node_id, end_to_end_seconds)
                                 : IoPatternObservabilitySnapshot{};

    std::cout << "\n============================================================\n"
              << "CFM CLIENT BENCHMARK (vLLM inference request model)\n"
              << "============================================================\n"
              << "  Requests:                " << FLAGS_requests << "\n"
              << "  Tokens/request:          "
              << FLAGS_prompt_tokens + FLAGS_output_tokens << " (prompt="
              << FLAGS_prompt_tokens << ", decode=" << FLAGS_output_tokens
              << ")\n"
              << "  KV blocks/request:       " << BlockCount(
                     FLAGS_prompt_tokens + FLAGS_output_tokens) * FLAGS_num_layers
              << " (layers=" << FLAGS_num_layers << ")\n"
              << "  Total KV blocks:         " << total_blocks << "\n"
              << "  Request submission time: " << std::fixed
              << std::setprecision(2) << submission_seconds << " s\n"
              << "  Submission requests/sec: "
              << FLAGS_requests / submission_seconds << "\n"
              << "  End-to-end time:         " << end_to_end_seconds << " s\n"
              << "\n  CFM ReportSnapshot latency\n"
              << "    failed reports:        " << failed_reports << "\n"
              << "    mean:                  " << report_latency.Mean() << " us\n"
              << "    p50 / p90 / p99:       " << report_latency.Percentile(50)
              << " / " << report_latency.Percentile(90) << " / "
              << report_latency.Percentile(99) << " us\n"
              << "\n  CFM report_metric_batch latency\n"
              << "    calls / failures:      " << metric_report_snapshot.calls
              << " / " << metric_report_snapshot.failures << "\n"
              << "    observations:          "
              << metric_report_snapshot.observations
              << "\n"
              << "    mean:                  "
              << metric_report_snapshot.latency.Mean()
              << " us\n"
              << "    p50 / p90 / p99:       "
              << metric_report_snapshot.latency.Percentile(50) << " / "
              << metric_report_snapshot.latency.Percentile(90) << " / "
              << metric_report_snapshot.latency.Percentile(99) << " us\n"
              << "\n  CFM policy commands executed\n"
              << "    evictions:             " << eviction_commands << "\n"
              << "    prefetches:            " << prefetch_commands << "\n"
              << "    admissions:            " << admission_commands << "\n"
              << "    policy drain:          "
              << (embedded_service
                      ? (policy_drain_complete ? "complete" : "timed out/error")
                      : "remote endpoint is not verifiable")
              << "\n"
              << "\n  IO Pattern snapshots\n"
              << "    client keys / storage: " << source_snapshot.keys.size() << " / "
              << source_snapshot.storage.size() << "\n";
    if (embedded_service) {
        std::cout << "    CFM keys / storage:    " << cfm_snapshot.keys.size()
                  << " / " << cfm_snapshot.storage.size() << "\n";
    } else {
        std::cout << "    CFM keys / storage:    remote endpoint (not exposed to "
                     "the client)\n";
    }
    PrintObservability("Client", source_metrics);
    if (embedded_service) PrintObservability("CFM", cfm_metrics);
    std::cout << "============================================================\n";
    return failed_reports == 0 ? 0 : 2;
}
