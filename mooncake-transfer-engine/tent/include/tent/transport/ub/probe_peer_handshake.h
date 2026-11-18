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
// P2: local half of the probe peer handshake. A ProbePeerDescriptorSource
// yields wire-neutral descriptions published by the peer (probe area
// descriptor, area epoch/token, remote probe Jetty identity, epochs and gate
// bits). HandshakeProbePeerDirectory validates them, imports the peer probe
// area through the URMA adapter, caches imports per (local port, peer,
// remote port, topology/target epoch) and produces the ProbeEdgeSupply list
// consumed by the standalone provider. The bootstrap wire that carries these
// descriptors is wired in P5; tests drive the source directly.

#ifndef TENT_TRANSPORT_UB_PROBE_PEER_HANDSHAKE_H_
#define TENT_TRANSPORT_UB_PROBE_PEER_HANDSHAKE_H_

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <vector>

#include "tent/common/status.h"
#include "tent/transport/ub/probe_provider_standalone.h"
#include "tent/transport/ub/probe_resources.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake {
namespace tent {
namespace ub {

inline constexpr uint32_t kProbePeerDescriptorSchemaVersion = 1;

// Wire-neutral peer descriptor. The bootstrap extension is expected to fill
// these fields; a peer that does not understand probing advertises a different
// (or missing) schema version and is skipped with a counter.
struct ProbePeerDescriptor {
    uint32_t schema_version{kProbePeerDescriptorSchemaVersion};
    Topology::NicID local_nic{-1};
    SegmentID peer_segment_id{LOCAL_SEGMENT_ID};
    Topology::NicID remote_nic{-1};
    std::string peer_nic_path;
    uint64_t topology_epoch{1};
    uint64_t target_epoch{1};
    // The peer's probe area (the page this side writes its 1-byte probes to).
    SegmentDescriptor area_descriptor;
    uint64_t area_length{0};
    uint64_t area_epoch{0};
    uint32_t area_token{0};
    RemoteJettyRef remote_jetty;
    bool node_gate_ok{false};
    bool port_gate_ok{false};
    bool profile_gate_ok{false};
    uint64_t endpoint_generation{0};
};

// Peer descriptor supply. P5 fills this from the bootstrap/control channel;
// P2 tests use a stub. Must outlive the directory and must not call back into
// it.
class ProbePeerDescriptorSource {
   public:
    virtual ~ProbePeerDescriptorSource() = default;

    virtual Status snapshot(std::vector<ProbePeerDescriptor>& descriptors) = 0;
};

class HandshakeProbePeerDirectory final : public ProbePeerDirectory {
   public:
    HandshakeProbePeerDirectory(std::shared_ptr<UrmaAdapter> adapter,
                                std::map<Topology::NicID, ContextPtr> contexts,
                                ProbePeerDescriptorSource* source);
    ~HandshakeProbePeerDirectory() override;

    HandshakeProbePeerDirectory(const HandshakeProbePeerDirectory&) = delete;
    HandshakeProbePeerDirectory& operator=(const HandshakeProbePeerDirectory&) =
        delete;

    Status snapshotEdges(std::vector<ProbeEdgeSupply>& edges) override;

    // Releases every imported peer probe area. Idempotent; the provider is
    // shut down before the directory in the transport teardown order.
    Status shutdown();

    [[nodiscard]] size_t unsupportedPeerCount() const;
    [[nodiscard]] size_t importFailureCount() const;
    [[nodiscard]] size_t importCount() const;
    [[nodiscard]] size_t cachedEdgeCount() const;

   private:
    struct EdgeKey {
        Topology::NicID local_nic{-1};
        SegmentID peer_segment_id{LOCAL_SEGMENT_ID};
        Topology::NicID remote_nic{-1};
        uint64_t topology_epoch{0};
        uint64_t target_epoch{0};

        bool operator<(const EdgeKey& other) const {
            return std::tie(local_nic, peer_segment_id, remote_nic,
                            topology_epoch, target_epoch) <
                   std::tie(other.local_nic, other.peer_segment_id,
                            other.remote_nic, other.topology_epoch,
                            other.target_epoch);
        }
    };

    struct CachedEdge {
        ProbeEdgeSupply edge;
        // Descriptor used for the current import; a change forces a re-import.
        std::string descriptor_hex;
    };

    mutable std::mutex mutex_;
    const std::shared_ptr<UrmaAdapter> adapter_;
    const std::map<Topology::NicID, ContextPtr> contexts_;
    ProbePeerDescriptorSource* const source_;
    std::map<EdgeKey, CachedEdge> cache_;
    size_t unsupported_peer_count_{0};
    size_t import_failure_count_{0};
    size_t import_count_{0};
    bool shut_down_{false};
};

}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_PROBE_PEER_HANDSHAKE_H_
