#include "io_pattern/cfm_service.h"

#include <string>

namespace mooncake::io_pattern {

CfmService::CfmService(std::shared_ptr<IoPatternRuntime> runtime)
    : runtime_(std::move(runtime)),
      codec_(std::make_shared<CfmBinaryCodec>()),
      ingress_(runtime_, codec_) {}

bool CfmService::Send(std::string_view method, std::string_view payload,
                      std::string_view source_id) {
    // CFM is a component of this SubMaster: there is no per-reporting-node
    // runtime and no policy delivery queue. Every accepted method is handled
    // by the ingress against the local runtime so that collection, analysis
    // and execution all observe the keys this SubMaster owns.
    return ingress_.Handle(method, payload, source_id);
}

IoPatternSnapshot CfmService::Snapshot() const {
    return runtime_ ? runtime_->Snapshot() : IoPatternSnapshot{};
}

IoPatternObservabilitySnapshot CfmService::Observability(
    double window_seconds) const {
    return runtime_ ? runtime_->ObservabilitySnapshot(window_seconds)
                    : IoPatternObservabilitySnapshot{};
}

bool CfmRpcService::Send(const std::string& method, const std::string& payload,
                         const std::string& source_id) {
    return service_ && service_->Send(method, payload, source_id);
}

}  // namespace mooncake::io_pattern
