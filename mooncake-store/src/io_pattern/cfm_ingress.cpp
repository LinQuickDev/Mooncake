#include "io_pattern/cfm_ingress.h"

namespace mooncake::io_pattern {

bool CfmIngress::Handle(std::string_view method, std::string_view payload) {
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
        for (const auto& metric : batch->inference) {
            runtime_->ReportInferenceMetrics(metric);
        }
        for (const auto& access : batch->accesses) {
            runtime_->RecordAccess(access.object.key, access);
        }
        for (const auto& storage : batch->storage) {
            runtime_->RecordStorageMetric(storage);
        }
        return true;
    }
    if (method == "execute_prefetch") {
        const auto command = codec_->DecodePolicy(wire);
        const auto* plan = command ? std::get_if<PrefetchPlan>(&*command) : nullptr;
        return plan && runtime_->ExecuteCommand(*plan) == ErrorCode::OK;
    }
    return false;
}

}  // namespace mooncake::io_pattern
