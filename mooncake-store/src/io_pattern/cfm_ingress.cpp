#include "io_pattern/cfm_ingress.h"

#include <chrono>

namespace mooncake::io_pattern {

bool CfmIngress::Handle(std::string_view method, std::string_view payload,
                        std::string_view source_id) {
    if (!runtime_ || !codec_) return false;
    const std::string wire(payload);
    if (method == "report_snapshot") {
        const auto snapshot = codec_->DecodeSnapshot(wire);
        if (!snapshot) return false;
        runtime_->MergeSnapshot(*snapshot);
        return true;
    }
    if (method == "report_metric_batch") {
        const auto batch = codec_->DecodeMetricBatch(wire);
        if (!batch) return false;
        const auto received_at_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count());
        for (const auto& metric : batch->inference) {
            runtime_->ReportInferenceMetrics(metric);
        }
        for (const auto& access : batch->accesses) {
            auto normalized = access;
            // steady_clock epochs are process-local. CFM observations must be
            // rebased to the receiving Store's clock before windowing.
            normalized.observed_at_ns = received_at_ns;
            runtime_->RecordAccess(normalized.object.key, normalized);
        }
        for (const auto& storage : batch->storage) {
            auto normalized = storage;
            // The transport identity is authoritative for remote metrics. It
            // keeps per-node watermarks distinct even when a producer omitted
            // or accidentally reused StorageMetric::source_id.
            if (!source_id.empty()) normalized.source_id = source_id;
            normalized.observed_at_ns = received_at_ns;
            runtime_->RecordStorageMetric(normalized);
        }
        return true;
    }
    if (method == "execute_prefetch") {
        const auto command = codec_->DecodePolicy(wire);
        const auto* plan = command ? std::get_if<PrefetchPlan>(&*command) : nullptr;
        return plan && runtime_->ExecuteCommand(*plan) == ErrorCode::OK;
    }
    if (method == "execute_policy") {
        const auto command = codec_->DecodePolicy(wire);
        return command && runtime_->ExecuteCommand(*command) == ErrorCode::OK;
    }
    return false;
}

}  // namespace mooncake::io_pattern
