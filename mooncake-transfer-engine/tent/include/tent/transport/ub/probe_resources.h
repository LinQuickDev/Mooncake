// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Contracts between the TENT UB probe-link subsystem and the pooling side
// (design §12). W1 freezes these interfaces and ships stubs/fakes; the native
// implementations arrive with the pooling-side milestone P0-B.
//
// Lifetime rules that both sides must honour:
//   * handles in ProbePortSupply/ProbeEdgeSupply are shared_ptr ownership
//     transfers. After an interface-4 snapshot or an interface-7 event returns,
//     the provider must not release them.
//   * provider/report calls are never made while ProbeLinkManager holds its
//     mutex, and providers must not call back into ProbeLinkManager from
//     inside those calls (design §14.1-6).

#ifndef TENT_TRANSPORT_UB_PROBE_RESOURCES_H_
#define TENT_TRANSPORT_UB_PROBE_RESOURCES_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "tent/common/status.h"
#include "tent/transport/ub/probe_types.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake {
namespace tent {
namespace ub {

// Per-WR remote probe Jetty identity (decision D-2). The probe WR names its
// target explicitly instead of relying on a local Jetty bind.
using RemoteJettyRef = RemoteJettyInfo;

// One local port's probe send resources (design §12.1). Exactly one active
// probe Jetty per local port; the source page is registered page-aligned and
// its content never changes.
struct ProbePortSupply {
    Topology::NicID local_nic{-1};
    ContextPtr context;
    JettyPtr probe_jetty;
    uint64_t jetty_generation{0};
    // nullptr means probe completions land on the business JFC (shared mode).
    JfcPtr probe_jfc;
    LocalSegmentPtr source_segment;
    uint64_t source_address{0};
    uint64_t source_length{0};
    // A new source page is registered for every probe-channel generation so a
    // retired generation's orphans can never alias the new page (design §9.5).
    uint64_t source_generation{0};
    uint32_t control_credit{0};
    // Pool-side mirror of the same peer-wide in-flight limit. W1 enforces
    // ProbeParams::peer_budget locally (design §8.2); the pooling side keeps
    // its own accounting authoritative.
    uint64_t peer_budget{0};
};

// One directed adjacency edge (design §12.1). The probe area is one page-sized
// slot per remote target (decision D-4); concurrently writing the same byte
// from several local ports is safe because the payload is constant.
struct ProbeEdgeSupply {
    PathVersion version;
    ProbePeerKey peer;
    RemoteSegmentPtr area_segment;
    uint64_t area_address{0};
    uint64_t area_length{0};
    uint64_t area_epoch{0};
    RemoteJettyRef remote_jetty;
    bool node_gate_ok{false};
    bool port_gate_ok{false};
    bool profile_gate_ok{false};
    // Bridge for design L-7: the business endpoint generation this path
    // version corresponds to. Business evidence that carries a different
    // generation is not current evidence. Zero means "provider did not
    // publish an anchor", in which case evidence falls back to the rail's
    // current PathVersion and probe_evidence_unmapped is counted. W3 replaces
    // this with the epochs carried on CompletionRecord.
    uint64_t endpoint_generation{0};
};

struct ProbeGraph {
    // Interface 4 is a full snapshot; this epoch starts the interface-7
    // (graph_epoch, event_seq) idempotency domain.
    uint64_t graph_epoch{0};
    std::vector<ProbePortSupply> ports;
    std::vector<ProbeEdgeSupply> edges;
};

// Terminology matches the total design's glossary: local context identity,
// native Jetty ID, and the probe channel generation.
struct LaneKey {
    Topology::NicID local_nic{-1};
    uint32_t native_jetty_id{0};
    uint64_t jetty_generation{0};

    bool operator==(const LaneKey&) const = default;
};

// Neutral handoff payload for unresolved probe responsibility (design §9.6 /
// §12.2). It is NOT a frozen interface yet: W2 maps these fields onto
// CompletionRecord. Acquire-before-release: the receiver must take the
// references before the sender drops them.
struct ProbeHandoff {
    uint64_t token{0};
    LaneKey lane;
    ProbeKey key;
    uint64_t posted_ns{0};
    uint64_t deadline_ns{0};
    bool timed_out{false};
    OrphanReason orphan_reason{OrphanReason::kNone};
    LocalSegmentPtr source;
    RemoteSegmentPtr area;
    uint32_t credit{0};
};

// Interface 7 event kinds (design §12.2). Every kind must carry its increment;
// a missing payload is a contract violation and the whole event is rejected.
enum class ProbeEventKind : uint8_t {
    kProbeHandleReplaced,
    kPortRevoked,
    kAreaReplaced,
    kTargetEpochChanged,
    kGateChanged,
    kEdgeAdded,
    kEdgeRemoved,
};

constexpr const char* probeEventKindName(ProbeEventKind kind) {
    switch (kind) {
        case ProbeEventKind::kProbeHandleReplaced:
            return "kProbeHandleReplaced";
        case ProbeEventKind::kPortRevoked:
            return "kPortRevoked";
        case ProbeEventKind::kAreaReplaced:
            return "kAreaReplaced";
        case ProbeEventKind::kTargetEpochChanged:
            return "kTargetEpochChanged";
        case ProbeEventKind::kGateChanged:
            return "kGateChanged";
        case ProbeEventKind::kEdgeAdded:
            return "kEdgeAdded";
        case ProbeEventKind::kEdgeRemoved:
            return "kEdgeRemoved";
    }
    return "unknown";
}

struct GateBits {
    bool node_ok{false};
    bool port_ok{false};
    bool profile_ok{false};
};

// Interface 7 payload (design §12.2). (graph_epoch, event_seq) is the
// idempotency key; event_seq starts at 1 inside each graph epoch and is
// strictly increasing.
struct ProbeEvent {
    ProbeEventKind kind{ProbeEventKind::kGateChanged};
    Topology::NicID local_nic{-1};
    uint64_t graph_epoch{0};
    uint64_t event_seq{0};
    std::optional<ProbePortSupply> port;
    std::optional<ProbeEdgeSupply> edge;
    std::optional<PathVersion> version;
    std::optional<GateBits> gate;
};

// Outbound reporting implemented by the pooling side (design §12.2). Error
// reporting is interface 2; handoffUnproven is a W1 seam that still has to be
// added to the total design's interface set (design §16 #10 / §17).
class ProbeReportSink {
   public:
    virtual ~ProbeReportSink() = default;

    // Non-blocking, idempotent and merged by the receiver. One call per real
    // lane retirement; repeated retirements of the same generation are
    // suppressed by ProbeLinkManager.
    virtual void retireProbeLane(const LaneKey& lane, uint64_t jetty_generation,
                                 uint64_t error_epoch, const PathVersion& path,
                                 const Status& reason) = 0;

    virtual void handoffUnproven(const ProbeHandoff& handoff) = 0;
};

// Interface 4 supply plus outbound reporting (design §12.2).
class ProbeAvailabilitySink;

using ProbeCompletionSink = std::function<Status(const Completion&)>;

class ProbeResourceProvider : public ProbeReportSink {
   public:
    // Full snapshot. Called once during transport startup before the scheduler
    // exists; returns non-OK when the probe resources are incomplete so the
    // transport can fail fast instead of silently degrading to "no probing".
    virtual Status snapshot(ProbeGraph& graph) = 0;

    // Interface-7 sink (PLM) and quiesced-completion sink (poller/PLM).
    // Providers that do not own probe-native resources ignore them.
    virtual void setAvailabilitySink(ProbeAvailabilitySink*) {}
    virtual void setCompletionSink(ProbeCompletionSink) {}
};

// Probe WR payload (design §8.4 / §12.2). One-byte payload, page-sized
// registration, remote target named per WR.
struct ProbePostTarget {
    JettyPtr probe_jetty;
    uint64_t jetty_generation{0};
    // RM: per-WR remote target. The adapter imports/caches it.
    RemoteJettyRef remote_jetty;
    // The WRITE source and destination segments; both must stay alive until
    // the completion is consumed.
    LocalSegmentPtr source_segment;
    RemoteSegmentPtr area_segment;
};

enum class ProbeOpcode : uint8_t {
    kWrite,
};

struct ProbeWorkRequest {
    ProbeOpcode opcode{ProbeOpcode::kWrite};
    uint64_t token{0};
    uint64_t source_address{0};
    uint64_t source_length{1};
    uint64_t area_address{0};
    uint64_t area_length{1};
};

// The real per-WR-target adapter arrives with P0-B; W1 uses a fake. The
// contract requires a non-blocking post (or a strictly bounded one); the
// scheduler observes probe_post_block_us.
class ProbePoster {
   public:
    virtual ~ProbePoster() = default;

    // posted == 1 means the native provider accepted the WR and a completion
    // (or a flush marker) will follow. posted == 0 with a non-OK status means
    // the WR was provably not accepted. Any other outcome is "unknown" and is
    // treated as accepted.
    virtual Status post(const ProbePostTarget& target,
                        const ProbeWorkRequest& request, size_t& posted) = 0;
};

// Interface 7 (design §12.2): implemented by the probe side, called directly
// by the pooling side. Synchronous and bounded; OK means applied, non-OK means
// the provider must resend the earliest unconfirmed event (same key).
class ProbeAvailabilitySink {
   public:
    virtual ~ProbeAvailabilitySink() = default;

    virtual Status onPoolAvailabilityChange(const ProbeEvent& event) = 0;
};

}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_PROBE_RESOURCES_H_
