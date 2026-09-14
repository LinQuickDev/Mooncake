#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "cfm_ingress.h"
#include "observability.h"
#include "types.h"

namespace mooncake::io_pattern {

// Embedded CFM endpoint of one SubMaster. There is no standalone CFM Master:
// every SubMaster runs the full IO Pattern pipeline locally and accepts
// ownership-addressed metric reports over its regular coro_rpc port (the same
// endpoint the rest of Mooncake uses). Received batches are merged straight
// into the local runtime so collection, analysis and policy execution all stay
// on the SubMaster that owns the reported keys.
//
// Reports are addressed by key ownership, so a receiving SubMaster only ever
// merges observations for keys that belong to its own slots. A separate node
// identity, delivery queues and a poll/ack loop are therefore unnecessary: the
// reporting client fans each batch out to the owning SubMaster(s) using the
// CVM key->slot->submaster mapping.
class CfmService final {
   public:
    explicit CfmService(std::shared_ptr<IoPatternRuntime> runtime);

    // RPC entry point. Supported methods:
    //   report_metric_batch / report_snapshot  -> merge into the local runtime
    //   execute_prefetch / execute_policy      -> execute through the local
    //                                             runtime's storage handlers
    // `source_id` identifies the reporting process and is used to normalize
    // remote StorageMetric watermarks.
    bool Send(std::string_view method, std::string_view payload,
              std::string_view source_id = {});

    IoPatternSnapshot Snapshot() const;
    IoPatternObservabilitySnapshot Observability(
        double window_seconds = 0.0) const;

   private:
    std::shared_ptr<IoPatternRuntime> runtime_;
    std::shared_ptr<CfmBinaryCodec> codec_;
    CfmIngress ingress_;
};

// coro_rpc-facing adapter. Keeping the RPC signature here lets both the
// SubMaster server and integration tests register the exact production
// endpoint.
class CfmRpcService final {
   public:
    explicit CfmRpcService(std::shared_ptr<CfmService> service)
        : service_(std::move(service)) {}

    bool Send(const std::string& method, const std::string& payload,
              const std::string& source_id = {});

   private:
    std::shared_ptr<CfmService> service_;
};

}  // namespace mooncake::io_pattern
