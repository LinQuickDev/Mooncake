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
// Standalone probe resource provider (P1). It owns the per-port probe
// resources directly through the URMA adapter, so link probing does not depend
// on the business Jetty pool:
//
//   * one dedicated probe JFC per port (or a borrowed business JFC in shared
//     mode; the provider never deletes a borrowed JFC),
//   * one probe Jetty per port (no local RC bind),
//   * one page-aligned probe source page per port and generation.
//
// Remote probe areas and remote probe Jetty identities come through
// ProbePeerDirectory. P1 ships this seam with a test/stub implementation; the
// peer handshake (bootstrap extension) lands in P2. Lane retire/rebuild and
// quarantine transfer are recorded here and become native in P4.
//
// Design: docs/analyze/tent-ub-probe-standalone-supply.md.

#ifndef TENT_TRANSPORT_UB_PROBE_PROVIDER_STANDALONE_H_
#define TENT_TRANSPORT_UB_PROBE_PROVIDER_STANDALONE_H_

#include <cstddef>
#include <cstdint>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/status.h"
#include "tent/transport/ub/probe_resources.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake {
namespace tent {
namespace ub {

// One local probe port supplied by the transport. `generation` is the probe
// channel generation (non-zero); `shared_jfc` is required in shared mode and
// ignored in dedicated mode.
struct ProbeLocalPortConfig {
    Topology::NicID local_nic{-1};
    ContextPtr context;
    JfcPtr shared_jfc;
    uint64_t generation{1};
};

// Supplies the remote half of the probe graph: per-edge probe area descriptor
// and remote probe Jetty identity. P2 replaces the stub with the bootstrap
// handshake; the provider itself does not parse peer metadata. The directory
// must outlive the provider and must not call back into it.
class ProbePeerDirectory {
   public:
    virtual ~ProbePeerDirectory() = default;

    virtual Status snapshotEdges(std::vector<ProbeEdgeSupply>& edges) = 0;
};

// Creates and owns probe-native resources independently of the business Jetty
// pool. Not copyable; all entry points are safe to call from different
// threads.
class StandaloneProbeResourceProvider final : public ProbeResourceProvider {
   public:
    // Maps a probe device's topology_name to the business Topology NIC id.
    // Without a resolver (selftest/unit tests) the discovery index is used.
    using TopologyIdResolver =
        std::function<Topology::NicID(const std::string& topology_name)>;
    // Self-owned mode (v1.7): discovers non-bonding physical devices, opens
    // probe-only Contexts and creates JFC/Jetty/pages. The remote directory is
    // attached with setRemoteDirectory() after discover().
    StandaloneProbeResourceProvider(std::shared_ptr<UrmaAdapter> adapter,
                                    ProbeParams params,
                                    TopologyIdResolver topology_resolver = {});
    // Injected mode: used by unit tests and the legacy pool seam.
    StandaloneProbeResourceProvider(std::shared_ptr<UrmaAdapter> adapter,
                                    ProbeParams params,
                                    std::vector<ProbeLocalPortConfig> ports,
                                    ProbePeerDirectory* directory);
    ~StandaloneProbeResourceProvider() override;

    StandaloneProbeResourceProvider(const StandaloneProbeResourceProvider&) =
        delete;
    StandaloneProbeResourceProvider& operator=(
        const StandaloneProbeResourceProvider&) = delete;

    // Discovers the probe physical ports (bond devices are filtered out) and
    // opens one probe Context per port. Idempotent.
    Status discover();

    // Creates JFC/Jetty/source page/local area page without building the graph.
    // Used by the two-node selftest to publish the local identity before the
    // peer descriptors are available. snapshot() reuses these resources.
    Status prepareLocal();

    // Probe-local Contexts keyed by the stable local port index; used to build
    // the remote directory.
    [[nodiscard]] std::map<Topology::NicID, ContextPtr> contexts() const;

    // Attaches the remote descriptor source before snapshot().
    void setRemoteDirectory(ProbePeerDirectory* directory);

    // Interface 4: creates the local resources once and returns the full
    // graph. A second call after success is rejected; a failed call rolls back
    // every resource it created.
    Status snapshot(ProbeGraph& graph) override;

    // Interface 2. Quiesces the old probe Jetty, dispatches its completions
    // through the completion sink, replaces the Jetty/source page/area page on
    // the same JFC and emits kProbeHandleReplaced (interface 7). Requires both
    // sinks; without them the request is recorded and no native rebuild is
    // attempted.
    void retireProbeLane(const LaneKey& lane, uint64_t jetty_generation,
                         uint64_t error_epoch, const PathVersion& path,
                         const Status& reason) override;

    // Unresolved responsibility handoff. References stay quarantined until
    // shutdown; retired pages are unregistered only after both the quarantine
    // and the retired-page list are released.
    void handoffUnproven(const ProbeHandoff& handoff) override;

    // Releases every local probe resource. Idempotent; must run before the
    // owning contexts are shut down. Stops the maintenance thread first.
    Status shutdown();

    // Waits until every queued lane rebuild has been processed. Intended for
    // tests and orderly handoff; production callers rely on the async worker.
    bool waitForRebuilds(uint64_t timeout_ms = 5000);

    // Interface 7 sink (ProbeLinkManager) and interface-2 completion sink
    // (workers dispatcher). Both are required before a lane can be rebuilt.
    void setAvailabilitySink(ProbeAvailabilitySink* sink) override;
    void setCompletionSink(ProbeCompletionSink sink) override;

    struct PortView {
        Topology::NicID local_nic{-1};
        uint64_t generation{0};
        uint32_t native_jetty_id{0};
        uint64_t source_address{0};
        uint64_t source_length{0};
        uint64_t source_generation{0};
        bool owns_jfc{false};
        uint32_t native_jetty_uasid{0};
        std::string device_eid;
        std::string topology_name;
        std::string native_device_name;
    };

    // Local probe area page: the page peers write their 1-byte probes into.
    // It is registered with remote-write permission and published to peers by
    // the handshake layer; the PLM never needs it.
    struct LocalAreaView {
        Topology::NicID local_nic{-1};
        uint64_t generation{0};
        uint64_t address{0};
        uint64_t length{0};
        uint32_t token{0};
        uint64_t epoch{0};
        SegmentDescriptor descriptor;
    };

    [[nodiscard]] std::vector<PortView> portViews() const;
    [[nodiscard]] std::vector<LocalAreaView> localAreaViews() const;
    [[nodiscard]] size_t quarantineCount() const;
    [[nodiscard]] size_t retiredPageCount() const;
    [[nodiscard]] uint64_t retireCalls() const;
    [[nodiscard]] bool initialized() const;

   private:
    struct PortState {
        ProbeLocalPortConfig config;
        std::string topology_name;
        std::string native_device_name;
        bool available{true};
        JfcPtr probe_jfc;
        bool owns_jfc{false};
        JettyPtr probe_jetty;
        LocalSegmentPtr source_segment;
        void* source_memory{nullptr};
        size_t source_size{0};
        uint64_t source_generation{1};
        LocalSegmentPtr area_segment;
        void* area_memory{nullptr};
        size_t area_size{0};
        uint64_t area_epoch{0};
    };

    struct RetireRequest {
        LaneKey lane;
        uint64_t generation{0};
        uint64_t error_epoch{0};
        PathVersion path;
        Status reason;
    };

    struct RetiredPage {
        LocalSegmentPtr segment;
        void* memory{nullptr};
        size_t size{0};
        uint64_t generation{0};
    };

    void maintenanceLoop();
    void processRetire(const RetireRequest& request);
    Status discoverLocked();
    uint32_t probeDepth(const PortState& port) const;
    Status createLocalResourcesLocked();
    Status validatePageSizeLocked() const;
    size_t pageSize() const;

    Status createPortLocked(PortState& port);
    Status destroyPortLocked(PortState& port);
    void rollbackPortsLocked();
    void releaseQuarantineLocked();
    void releaseRetiredPagesLocked();
    Status buildGraphLocked(ProbeGraph& graph);
    ProbePortSupply makePortSupplyLocked(const PortState& port) const;
    Status rebuildPortLocked(PortState& port, uint64_t error_epoch,
                             std::vector<Completion>& completions);
    Status emitHandleReplacedLocked(const ProbePortSupply& supply);

    mutable std::mutex mutex_;
    std::thread maintenance_;
    std::mutex rebuild_mutex_;
    std::condition_variable rebuild_cv_;
    std::condition_variable rebuild_idle_cv_;
    std::deque<RetireRequest> rebuild_queue_;
    bool rebuild_stopping_{false};
    bool rebuild_active_{false};
    const std::shared_ptr<UrmaAdapter> adapter_;
    const ProbeParams params_;
    std::vector<PortState> ports_;
    ProbePeerDirectory* directory_;
    const TopologyIdResolver topology_resolver_;
    bool owns_contexts_{false};
    bool auto_discover_{false};
    std::vector<ProbeHandoff> quarantine_;
    std::vector<RetiredPage> retired_pages_;
    ProbeAvailabilitySink* availability_sink_{nullptr};
    ProbeCompletionSink completion_sink_;
    uint64_t graph_epoch_{0};
    uint64_t event_seq_{0};
    std::atomic<uint64_t> retire_calls_{0};
    uint64_t next_graph_epoch_{1};
    bool resources_ready_{false};
    bool initialized_{false};
    bool shut_down_{false};
};

}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_PROBE_PROVIDER_STANDALONE_H_
