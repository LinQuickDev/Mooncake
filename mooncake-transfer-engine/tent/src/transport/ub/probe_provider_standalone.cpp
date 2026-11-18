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

#include "tent/transport/ub/probe_provider_standalone.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <set>
#include <utility>

#include <glog/logging.h>
#include <unistd.h>

#include "tent/transport/ub/device_selection.h"

namespace mooncake {
namespace tent {
namespace ub {
namespace {

size_t probePageSize() {
    const long page = ::sysconf(_SC_PAGESIZE);
    return page > 0 ? static_cast<size_t>(page) : 4096;
}

// Token the peer must use when importing our probe area. P2 publishes it to
// peers through the handshake; the value is a placeholder until the bootstrap
// contract freezes the probe-area token.
constexpr uint32_t kProbeAreaToken = 0xACFE;

}  // namespace

StandaloneProbeResourceProvider::StandaloneProbeResourceProvider(
    std::shared_ptr<UrmaAdapter> adapter, ProbeParams params,
    TopologyIdResolver topology_resolver)
    : adapter_(std::move(adapter)),
      params_(std::move(params)),
      directory_(nullptr),
      topology_resolver_(std::move(topology_resolver)),
      owns_contexts_(true),
      auto_discover_(true) {
    maintenance_ = std::thread([this] { maintenanceLoop(); });
}

StandaloneProbeResourceProvider::StandaloneProbeResourceProvider(
    std::shared_ptr<UrmaAdapter> adapter, ProbeParams params,
    std::vector<ProbeLocalPortConfig> ports, ProbePeerDirectory* directory)
    : adapter_(std::move(adapter)),
      params_(std::move(params)),
      directory_(directory) {
    ports_.reserve(ports.size());
    for (auto& config : ports) {
        PortState state;
        state.config = std::move(config);
        state.source_generation = state.config.generation;
        ports_.push_back(std::move(state));
    }
    maintenance_ = std::thread([this] { maintenanceLoop(); });
}

StandaloneProbeResourceProvider::~StandaloneProbeResourceProvider() {
    (void)shutdown();
}

Status StandaloneProbeResourceProvider::discover() {
    std::lock_guard<std::mutex> lock(mutex_);
    return discoverLocked();
}

namespace {

std::string shortEidName(const DeviceInfo& device) {
    return device.native_device_name + ":eid" +
           std::to_string(device.eid_index);
}

bool matchesProbeFilter(const DeviceInfo& device,
                        const std::vector<std::string>& filter) {
    if (filter.empty()) return true;
    const std::string short_name = shortEidName(device);
    return std::find(filter.begin(), filter.end(), device.native_device_name) !=
               filter.end() ||
           std::find(filter.begin(), filter.end(), device.topology_name) !=
               filter.end() ||
           std::find(filter.begin(), filter.end(), short_name) != filter.end();
}

}  // namespace

Status StandaloneProbeResourceProvider::discoverLocked() {
    if (shut_down_) {
        return Status::InvalidArgument(
            "standalone probe provider is shut down");
    }
    if (!ports_.empty()) return Status::OK();
    if (!params_.valid()) {
        return Status::InvalidArgument("invalid probe parameters");
    }
    if (!adapter_) {
        return Status::InvalidArgument("probe provider has no URMA adapter");
    }
    auto status = validatePageSizeLocked();
    if (!status.ok()) return status;

    std::vector<DeviceInfo> devices;
    status = adapter_->discoverDevices(devices);
    if (!status.ok()) return status;
    if (devices.empty()) {
        return Status::DeviceNotFound("no URMA device is available for probe");
    }
    std::vector<DeviceInfo> physical;
    for (auto& device : devices) {
        if (!device.active) continue;
        // Bonding devices are always excluded: probe uses a physical port
        // directly and never shares the bond provider's device.
        if (isBondingDevice(device)) continue;
        if (!matchesProbeFilter(device, params_.device_filter)) continue;
        // Empty filter or a native-only entry selects every active EID of the
        // matching device(s). A native:eidN / topology entry selects exactly
        // that EID (and can coexist with other EIDs of the same device).
        physical.push_back(std::move(device));
    }
    if (physical.empty()) {
        return Status::DeviceNotFound(
            "no non-bonding physical device matches probe/device_filter");
    }

    size_t matched = 0;
    size_t available = 0;
    ports_.reserve(physical.size());
    for (size_t index = 0; index < physical.size(); ++index) {
        PortState port;
        // Production resolves the business Topology NIC id so business
        // evidence (UbPostPath.local_topology_id) and probe paths share the
        // same key. Selftest/unit tests fall back to the discovery order.
        port.config.local_nic = static_cast<Topology::NicID>(index);
        if (topology_resolver_) {
            const auto resolved =
                topology_resolver_(physical[index].topology_name);
            if (resolved >= 0) {
                port.config.local_nic = resolved;
            } else {
                LOG(WARNING)
                    << "UB probe: device " << physical[index].native_device_name
                    << " topology=" << physical[index].topology_name
                    << " is not in the business topology; using "
                       "discovery index "
                    << index;
            }
        }
        port.config.generation = 1;
        port.source_generation = 1;
        port.topology_name = physical[index].topology_name;
        port.native_device_name = physical[index].native_device_name;
        ++matched;
        ContextPtr context;
        status = adapter_->openContext(physical[index], context);
        if (!status.ok() || !context || !context->valid()) {
            port.available = false;
            LOG(WARNING) << "UB probe: cannot open probe context for "
                         << physical[index].native_device_name
                         << " status=" << status.ToString();
        } else {
            port.config.context = std::move(context);
            ++available;
            LOG(INFO) << "UB probe device "
                      << physical[index].native_device_name
                      << " topology=" << physical[index].topology_name
                      << " transport_modes="
                      << physical[index].capabilities.transport_modes;
        }
        ports_.push_back(std::move(port));
    }
    if (matched == 0) {
        ports_.clear();
        return Status::DeviceNotFound(
            "no non-bonding probe device matches probe/device_filter");
    }
    if (available == 0) {
        for (auto& port : ports_) {
            if (port.config.context) {
                (void)adapter_->closeContext(port.config.context);
            }
        }
        ports_.clear();
        return Status::DeviceNotFound(
            "cannot open any probe context on the selected devices");
    }
    LOG(INFO) << "UB probe: discovered " << available << " probe port(s) of "
              << matched << " selected device(s)";
    return Status::OK();
}

std::map<Topology::NicID, ContextPtr>
StandaloneProbeResourceProvider::contexts() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::map<Topology::NicID, ContextPtr> result;
    for (const auto& port : ports_) {
        if (!port.available || !port.config.context) continue;
        result.emplace(port.config.local_nic, port.config.context);
    }
    return result;
}

void StandaloneProbeResourceProvider::setRemoteDirectory(
    ProbePeerDirectory* directory) {
    std::lock_guard<std::mutex> lock(mutex_);
    directory_ = directory;
}

Status StandaloneProbeResourceProvider::validatePageSizeLocked() const {
    if (params_.source_page_size == 0) return Status::OK();
    const size_t system_page = probePageSize();
    if (params_.source_page_size < system_page ||
        params_.source_page_size % system_page != 0) {
        return Status::InvalidArgument(
            "transports/ub/probe/source_page_size must be a page-aligned "
            "multiple of the system page size");
    }
    return Status::OK();
}

size_t StandaloneProbeResourceProvider::pageSize() const {
    return params_.source_page_size == 0
               ? probePageSize()
               : static_cast<size_t>(params_.source_page_size);
}

uint32_t StandaloneProbeResourceProvider::probeDepth(
    const PortState& port) const {
    // UMDK's own RM health probe uses a 1024-deep JFC/Jetty. Smaller depths
    // were rejected by some providers with URMA_EINVAL; clamp to the device
    // capability when it is known.
    uint32_t depth = std::max<uint32_t>(params_.jfc_depth, 1024);
    if (port.config.context) {
        const auto& caps = port.config.context->deviceInfo().capabilities;
        if (caps.max_jfc_depth != 0) {
            depth = std::min(depth, caps.max_jfc_depth);
        }
        if (caps.max_jetty_depth != 0) {
            depth = std::min(depth, caps.max_jetty_depth);
        }
    }
    return std::max<uint32_t>(depth, 8);
}

Status StandaloneProbeResourceProvider::createPortLocked(PortState& port) {
    if (!params_.valid()) {
        return Status::InvalidArgument("invalid probe parameters");
    }
    if (!adapter_) {
        return Status::InvalidArgument("probe provider has no URMA adapter");
    }
    if (!port.config.context || !port.config.context->valid()) {
        return Status::InvalidArgument("probe port has no active context");
    }
    if (port.config.local_nic < 0) {
        return Status::InvalidArgument("probe port has an invalid local NIC");
    }
    if (port.config.generation == 0) {
        return Status::InvalidArgument(
            "probe port generation must be non-zero");
    }

    JfcPtr jfc;
    bool owns_jfc = false;
    if (params_.jfc_mode == ProbeJfcMode::kDedicated) {
        JfcOptions options;
        options.depth = probeDepth(port);
        // The probe JFC only carries completions; mirror UMDK's health-probe
        // JFR (depth 1, no receive token).
        options.receiver_depth = 1;
        options.token = 0;
        options.enable_completion_events = false;
        options.transport_mode = JettyTransportMode::kRm;
        auto status = adapter_->createJfc(port.config.context, options, jfc);
        if (!status.ok()) return status;
        if (!jfc) {
            return Status::InternalError("URMA adapter returned no probe JFC");
        }
        owns_jfc = true;
    } else {
        jfc = port.config.shared_jfc;
        if (!jfc || !jfc->valid()) {
            return Status::InvalidArgument(
                "shared probe mode requires an active business JFC");
        }
    }

    JettyOptions jetty_options;
    // Probe needs a single in-flight WR per port; keep the queue shallow and
    // the priority low so the RM Jetty fits the least capable physical port.
    jetty_options.depth = probeDepth(port);
    jetty_options.priority = 0;
    jetty_options.max_sge = 1;
    // Probe postings carry a per-WR remote target; RM keeps the probe Jetty
    // unbound and independent of any endpoint binding.
    jetty_options.transport_mode = JettyTransportMode::kRm;
    JettyPtr jetty;
    auto status =
        adapter_->createJetty(port.config.context, jfc, jetty_options, jetty);
    if (!status.ok() || !jetty) {
        if (owns_jfc) (void)adapter_->deleteJfc(jfc);
        return status.ok() ? Status::InternalError(
                                 "URMA adapter returned no probe Jetty")
                           : status;
    }

    const size_t page = pageSize();
    void* memory = nullptr;
    if (::posix_memalign(&memory, page, page) != 0 || memory == nullptr) {
        (void)adapter_->deleteJetty(jetty);
        if (owns_jfc) (void)adapter_->deleteJfc(jfc);
        return Status::InternalError("Cannot allocate the probe source page");
    }
    // Content is irrelevant: the peer never reads it and the payload is a
    // single constant byte. It must still cover a whole page because that is
    // the registration granularity (payload=1 != registration length 1).
    std::memset(memory, 0x5A, page);

    SegmentOptions segment_options;
    segment_options.access = SEGMENT_ACCESS_LOCAL_ONLY;
    LocalSegmentPtr segment;
    LOG(INFO) << "UB probe: register source page va=0x" << std::hex
              << reinterpret_cast<uint64_t>(memory) << " len=0x" << page
              << std::dec << " access=" << segment_options.access;
    status = adapter_->registerLocalSegment(port.config.context,
                                            reinterpret_cast<uint64_t>(memory),
                                            page, segment_options, segment);
    if (!status.ok() || !segment) {
        (void)adapter_->deleteJetty(jetty);
        if (owns_jfc) (void)adapter_->deleteJfc(jfc);
        std::free(memory);
        return status.ok()
                   ? Status::InternalError(
                         "URMA adapter returned no probe source segment")
                   : status;
    }

    // Local probe area page: the peer writes its 1-byte probes here. It needs
    // remote-write permission and its own token; the local side never reads it.
    void* area_memory = nullptr;
    if (::posix_memalign(&area_memory, page, page) != 0 ||
        area_memory == nullptr) {
        (void)adapter_->deleteJetty(jetty);
        if (owns_jfc) (void)adapter_->deleteJfc(jfc);
        (void)adapter_->unregisterLocalSegment(segment);
        std::free(memory);
        return Status::InternalError(
            "Cannot allocate the local probe area page");
    }
    std::memset(area_memory, 0, page);
    SegmentOptions area_options;
    // Some providers reject a write-only segment. The peer only writes this
    // page, but granting read as well keeps the registration portable.
    area_options.access = SEGMENT_ACCESS_READ | SEGMENT_ACCESS_WRITE;
    area_options.token = kProbeAreaToken;
    LocalSegmentPtr area_segment;
    LOG(INFO) << "UB probe: register local area page va=0x" << std::hex
              << reinterpret_cast<uint64_t>(area_memory) << " len=0x" << page
              << std::dec << " access=" << area_options.access
              << " token=" << area_options.token;
    status = adapter_->registerLocalSegment(
        port.config.context, reinterpret_cast<uint64_t>(area_memory), page,
        area_options, area_segment);
    if (!status.ok() || !area_segment) {
        (void)adapter_->deleteJetty(jetty);
        if (owns_jfc) (void)adapter_->deleteJfc(jfc);
        (void)adapter_->unregisterLocalSegment(segment);
        std::free(memory);
        std::free(area_memory);
        return status.ok() ? Status::InternalError(
                                 "URMA adapter returned no probe area segment")
                           : status;
    }

    port.probe_jfc = std::move(jfc);
    port.owns_jfc = owns_jfc;
    port.probe_jetty = std::move(jetty);
    port.source_segment = std::move(segment);
    port.source_memory = memory;
    port.source_size = page;
    port.source_generation = port.config.generation;
    port.area_segment = std::move(area_segment);
    port.area_memory = area_memory;
    port.area_size = page;
    port.area_epoch = port.config.generation;
    return Status::OK();
}

Status StandaloneProbeResourceProvider::destroyPortLocked(PortState& port) {
    Status first_error = Status::OK();
    if (!port.available) return first_error;
    if (adapter_ && port.probe_jetty) {
        // deleteJetty requires a reset Jetty. Probe has no in-flight WRs at
        // this point: the transport drains the PLM before provider shutdown.
        auto status = adapter_->resetJetty(port.probe_jetty);
        if (first_error.ok() && !status.ok()) first_error = status;
        status = adapter_->deleteJetty(port.probe_jetty);
        if (first_error.ok() && !status.ok()) first_error = status;
    }
    if (adapter_ && port.owns_jfc && port.probe_jfc) {
        auto status = adapter_->deleteJfc(port.probe_jfc);
        if (first_error.ok() && !status.ok()) first_error = status;
    }
    if (adapter_ && port.source_segment) {
        auto status = adapter_->unregisterLocalSegment(port.source_segment);
        if (first_error.ok() && !status.ok()) first_error = status;
    }
    if (adapter_ && port.area_segment) {
        auto status = adapter_->unregisterLocalSegment(port.area_segment);
        if (first_error.ok() && !status.ok()) first_error = status;
    }
    if (port.source_memory != nullptr) {
        std::free(port.source_memory);
        port.source_memory = nullptr;
    }
    if (port.area_memory != nullptr) {
        std::free(port.area_memory);
        port.area_memory = nullptr;
    }
    port.probe_jfc.reset();
    port.owns_jfc = false;
    port.probe_jetty.reset();
    port.source_segment.reset();
    port.source_size = 0;
    port.source_generation = 0;
    port.area_segment.reset();
    port.area_size = 0;
    port.area_epoch = 0;
    return first_error;
}

void StandaloneProbeResourceProvider::rollbackPortsLocked() {
    for (auto& port : ports_) {
        if (port.available) (void)destroyPortLocked(port);
    }
}

void StandaloneProbeResourceProvider::releaseQuarantineLocked() {
    if (quarantine_.empty()) return;
    // P1 keeps the references alive until shutdown. P4 turns this into a real
    // quarantine with per-generation source-page reclamation.
    LOG(INFO) << "UB standalone probe provider: releasing "
              << quarantine_.size()
              << " quarantined probe handoff(s) at shutdown";
    quarantine_.clear();
}

ProbePortSupply StandaloneProbeResourceProvider::makePortSupplyLocked(
    const PortState& port) const {
    ProbePortSupply supply;
    supply.local_nic = port.config.local_nic;
    supply.context = port.config.context;
    supply.probe_jetty = port.probe_jetty;
    supply.jetty_generation = port.config.generation;
    supply.probe_jfc = port.owns_jfc ? port.probe_jfc : nullptr;
    supply.source_segment = port.source_segment;
    supply.source_address = reinterpret_cast<uint64_t>(port.source_memory);
    supply.source_length = 1;
    supply.source_generation = port.source_generation;
    supply.control_credit = 1;
    supply.peer_budget = params_.peer_budget;
    return supply;
}

Status StandaloneProbeResourceProvider::buildGraphLocked(ProbeGraph& graph) {
    std::vector<ProbeEdgeSupply> edges;
    if (directory_ == nullptr) {
        return Status::InvalidArgument("probe provider has no peer directory");
    }
    auto status = directory_->snapshotEdges(edges);
    if (!status.ok()) return status;

    std::set<PathVersion> versions;
    std::set<Topology::NicID> available_ports;
    std::set<Topology::NicID> known_ports;
    for (const auto& port : ports_) {
        known_ports.insert(port.config.local_nic);
        if (port.available) available_ports.insert(port.config.local_nic);
    }

    std::vector<ProbeEdgeSupply> kept_edges;
    kept_edges.reserve(edges.size());
    size_t skipped_edges = 0;
    for (auto& edge : edges) {
        if (!edge.version.valid()) {
            return Status::InvalidArgument(
                "probe directory returned an invalid path version");
        }
        if (!edge.peer.valid()) {
            return Status::InvalidArgument(
                "probe directory returned an invalid peer");
        }
        if (!edge.area_segment || edge.area_length == 0 ||
            edge.area_epoch == 0) {
            return Status::InvalidArgument(
                "probe directory returned an invalid probe area");
        }
        if (available_ports.find(edge.version.path.local_nic) ==
            available_ports.end()) {
            if (known_ports.find(edge.version.path.local_nic) !=
                known_ports.end()) {
                // The local port exists but was filtered out or failed to
                // open; its remote descriptors are simply not used.
                ++skipped_edges;
                continue;
            }
            return Status::InvalidArgument(
                "probe directory edge references an unknown local port");
        }
        if (!versions.insert(edge.version).second) {
            return Status::InvalidArgument(
                "probe directory returned a duplicate path version");
        }
        kept_edges.push_back(std::move(edge));
    }
    if (skipped_edges != 0) {
        LOG(INFO) << "UB probe: ignoring " << skipped_edges
                  << " edge(s) on unavailable local port(s)";
    }

    ProbeGraph built;
    built.graph_epoch = next_graph_epoch_;
    built.ports.reserve(available_ports.size());
    for (const auto& port : ports_) {
        if (port.available) built.ports.push_back(makePortSupplyLocked(port));
    }
    built.edges = std::move(kept_edges);
    graph = std::move(built);
    return Status::OK();
}

Status StandaloneProbeResourceProvider::prepareLocal() {
    if (auto_discover_) {
        auto status = discover();
        if (!status.ok()) return status;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (shut_down_) {
        return Status::InvalidArgument(
            "standalone probe provider is shut down");
    }
    if (initialized_ || resources_ready_) return Status::OK();
    if (!params_.enabled) {
        return Status::InvalidArgument("probe is disabled by configuration");
    }
    return createLocalResourcesLocked();
}

Status StandaloneProbeResourceProvider::createLocalResourcesLocked() {
    auto page_status = validatePageSizeLocked();
    if (!page_status.ok()) return page_status;

    std::set<Topology::NicID> local_nics;
    size_t available = 0;
    for (const auto& port : ports_) {
        if (!port.available) continue;
        if (port.config.local_nic < 0 ||
            !local_nics.insert(port.config.local_nic).second ||
            port.config.generation == 0 || !port.config.context ||
            !port.config.context->valid()) {
            return Status::InvalidArgument(
                "invalid or duplicate local probe port");
        }
        ++available;
    }
    if (available == 0) {
        return Status::InvalidArgument("probe provider has no available port");
    }

    for (auto& port : ports_) {
        if (!port.available) continue;
        auto status = createPortLocked(port);
        if (!status.ok()) {
            rollbackPortsLocked();
            return status;
        }
    }
    resources_ready_ = true;
    return Status::OK();
}

Status StandaloneProbeResourceProvider::snapshot(ProbeGraph& graph) {
    if (auto_discover_) {
        auto status = discover();
        if (!status.ok()) return status;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (shut_down_) {
        return Status::InvalidArgument(
            "standalone probe provider is shut down");
    }
    if (initialized_) {
        return Status::InvalidArgument(
            "standalone probe provider is already snapshotted");
    }
    if (!params_.valid()) {
        return Status::InvalidArgument("invalid probe parameters");
    }
    if (!params_.enabled) {
        return Status::InvalidArgument("probe is disabled by configuration");
    }
    if (directory_ == nullptr) {
        return Status::InvalidArgument("probe provider has no peer directory");
    }
    if (!resources_ready_) {
        auto status = createLocalResourcesLocked();
        if (!status.ok()) return status;
    }
    size_t available = 0;
    for (const auto& port : ports_) {
        if (port.available) ++available;
    }

    ProbeGraph built;
    auto status = buildGraphLocked(built);
    if (!status.ok()) {
        rollbackPortsLocked();
        return status;
    }
    ++next_graph_epoch_;
    graph_epoch_ = built.graph_epoch;
    event_seq_ = 0;
    initialized_ = true;
    graph = std::move(built);
    LOG(INFO) << "UB standalone probe provider created " << available
              << " probe port(s)";
    return Status::OK();
}

void StandaloneProbeResourceProvider::retireProbeLane(const LaneKey& lane,
                                                      uint64_t jetty_generation,
                                                      uint64_t error_epoch,
                                                      const PathVersion& path,
                                                      const Status& reason) {
    ++retire_calls_;
    {
        std::lock_guard<std::mutex> lock(rebuild_mutex_);
        if (rebuild_stopping_) return;
        if (rebuild_queue_.size() >= 64) {
            LOG_EVERY_N(WARNING, 100)
                << "UB standalone probe provider: rebuild queue is full; "
                   "dropping retire for local_nic="
                << lane.local_nic;
            return;
        }
        rebuild_queue_.push_back(
            RetireRequest{lane, jetty_generation, error_epoch, path, reason});
    }
    rebuild_cv_.notify_one();
}

void StandaloneProbeResourceProvider::maintenanceLoop() {
    for (;;) {
        RetireRequest request;
        {
            std::unique_lock<std::mutex> lock(rebuild_mutex_);
            rebuild_cv_.wait(lock, [this] {
                return rebuild_stopping_ || !rebuild_queue_.empty();
            });
            if (rebuild_stopping_ && rebuild_queue_.empty()) break;
            request = std::move(rebuild_queue_.front());
            rebuild_queue_.pop_front();
            rebuild_active_ = true;
        }
        processRetire(request);
        {
            std::lock_guard<std::mutex> lock(rebuild_mutex_);
            rebuild_active_ = false;
            if (rebuild_queue_.empty()) rebuild_idle_cv_.notify_all();
        }
    }
}

void StandaloneProbeResourceProvider::processRetire(
    const RetireRequest& request) {
    std::vector<Completion> completions;
    Status rebuild_status = Status::OK();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!initialized_ || shut_down_) return;

        auto port = std::find_if(
            ports_.begin(), ports_.end(), [&](const PortState& candidate) {
                return candidate.config.local_nic == request.lane.local_nic;
            });
        if (port == ports_.end()) {
            LOG_EVERY_N(WARNING, 100)
                << "UB standalone probe provider: retire for unknown local "
                   "port "
                << request.lane.local_nic;
            return;
        }
        if (port->config.generation != request.generation ||
            !port->probe_jetty ||
            port->probe_jetty->id() != request.lane.native_jetty_id) {
            LOG_EVERY_N(WARNING, 100)
                << "UB standalone probe provider: stale lane retire ignored: "
                   "local_nic="
                << request.lane.local_nic
                << " generation=" << request.generation
                << " current=" << port->config.generation;
            return;
        }
        if (!completion_sink_ || !availability_sink_) {
            LOG_EVERY_N(WARNING, 100)
                << "UB standalone probe provider: lane retire recorded but no "
                   "completion/availability sink is configured; the lane "
                   "stays down (local_nic="
                << request.lane.local_nic << ")";
            return;
        }

        rebuild_status =
            rebuildPortLocked(*port, request.error_epoch, completions);
        if (rebuild_status.ok()) {
            LOG(INFO) << "UB standalone probe provider: rebuilt probe lane "
                      << request.path.path.local_nic << "/"
                      << request.path.path.segment_id << "/"
                      << request.path.path.remote_nic << " generation "
                      << request.generation << " -> "
                      << port->config.generation;
        }
    }

    // Dispatch quiesced completions outside the provider lock: the sink may
    // call back into handoffUnproven on this worker thread.
    ProbeCompletionSink sink;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        sink = completion_sink_;
    }
    if (sink) {
        for (const auto& completion : completions) {
            auto dispatched = sink(completion);
            if (!dispatched.ok()) {
                LOG_EVERY_N(WARNING, 100)
                    << "UB standalone probe provider: quiesced completion "
                       "dispatch failed token="
                    << completion.token << " status=" << dispatched.ToString();
            }
        }
    }
    if (!rebuild_status.ok()) {
        LOG_EVERY_N(ERROR, 100)
            << "UB standalone probe provider: lane rebuild failed: local_nic="
            << request.lane.local_nic << " generation=" << request.generation
            << " status=" << rebuild_status.ToString()
            << " reason=" << request.reason.ToString();
    }
}

bool StandaloneProbeResourceProvider::waitForRebuilds(uint64_t timeout_ms) {
    std::unique_lock<std::mutex> lock(rebuild_mutex_);
    return rebuild_idle_cv_.wait_for(
        lock, std::chrono::milliseconds(timeout_ms),
        [this] { return rebuild_queue_.empty() && !rebuild_active_; });
}

void StandaloneProbeResourceProvider::handoffUnproven(
    const ProbeHandoff& handoff) {
    std::lock_guard<std::mutex> lock(mutex_);
    quarantine_.push_back(handoff);
    LOG_EVERY_N(INFO, 100)
        << "UB standalone probe provider: quarantined probe handoff token="
        << handoff.token
        << " reason=" << static_cast<int>(handoff.orphan_reason);
}

void StandaloneProbeResourceProvider::setAvailabilitySink(
    ProbeAvailabilitySink* sink) {
    std::lock_guard<std::mutex> lock(mutex_);
    availability_sink_ = sink;
}

void StandaloneProbeResourceProvider::setCompletionSink(
    ProbeCompletionSink sink) {
    std::lock_guard<std::mutex> lock(mutex_);
    completion_sink_ = std::move(sink);
}

Status StandaloneProbeResourceProvider::rebuildPortLocked(
    PortState& port, uint64_t error_epoch,
    std::vector<Completion>& completions) {
    (void)error_epoch;
    const uint64_t old_generation = port.config.generation;
    if (!port.probe_jetty || (!port.probe_jfc && !port.config.shared_jfc)) {
        return Status::InvalidArgument("probe port has no Jetty/JFC");
    }

    const uint32_t timeout_ms = static_cast<uint32_t>(
        std::min<uint64_t>(params_.timeout_ms, UINT32_MAX));
    auto status =
        adapter_->quiesceJetty(port.probe_jetty, timeout_ms, completions);
    if (!status.ok()) return status;

    JettyPtr old_jetty = port.probe_jetty;
    JfcPtr jfc = port.owns_jfc ? port.probe_jfc : port.config.shared_jfc;
    JettyOptions jetty_options;
    jetty_options.depth = probeDepth(port);
    jetty_options.priority = 0;
    jetty_options.max_sge = 1;
    jetty_options.transport_mode = JettyTransportMode::kRm;
    JettyPtr new_jetty;
    status = adapter_->createJetty(port.config.context, jfc, jetty_options,
                                   new_jetty);
    if (!status.ok() || !new_jetty) {
        return status.ok()
                   ? Status::InternalError(
                         "URMA adapter returned no replacement probe Jetty")
                   : status;
    }
    const size_t page = pageSize();
    auto allocate_page = [&](uint32_t access, uint32_t token, int fill,
                             LocalSegmentPtr& segment,
                             void*& memory) -> Status {
        void* buffer = nullptr;
        if (::posix_memalign(&buffer, page, page) != 0 || buffer == nullptr) {
            return Status::InternalError(
                "Cannot allocate a probe generation page");
        }
        std::memset(buffer, fill, page);
        SegmentOptions options;
        options.access = access;
        options.token = token;
        LOG(INFO) << "UB probe: rebuild register page va=0x" << std::hex
                  << reinterpret_cast<uint64_t>(buffer) << " len=0x" << page
                  << std::dec << " access=" << access << " token=" << token;
        auto registered = adapter_->registerLocalSegment(
            port.config.context, reinterpret_cast<uint64_t>(buffer), page,
            options, segment);
        if (!registered.ok() || !segment) {
            std::free(buffer);
            return registered.ok()
                       ? Status::InternalError(
                             "URMA adapter returned no probe generation "
                             "segment")
                       : registered;
        }
        memory = buffer;
        return Status::OK();
    };

    LocalSegmentPtr new_source;
    void* new_source_memory = nullptr;
    status = allocate_page(SEGMENT_ACCESS_LOCAL_ONLY, 0, 0x5A, new_source,
                           new_source_memory);
    if (!status.ok()) {
        if (new_source) (void)adapter_->unregisterLocalSegment(new_source);
        if (new_source_memory) std::free(new_source_memory);
        (void)adapter_->resetJetty(new_jetty);
        (void)adapter_->deleteJetty(new_jetty);
        return status;
    }

    status = adapter_->resetJetty(old_jetty);
    if (!status.ok()) {
        if (new_source) (void)adapter_->unregisterLocalSegment(new_source);
        if (new_source_memory) std::free(new_source_memory);
        (void)adapter_->resetJetty(new_jetty);
        (void)adapter_->deleteJetty(new_jetty);
        return status;
    }

    const uint64_t next_generation = old_generation + 1;
    PortState next = port;
    next.probe_jetty = new_jetty;
    next.source_segment = new_source;
    next.source_memory = new_source_memory;
    next.source_size = page;
    next.config.generation = next_generation;
    next.source_generation = next_generation;
    // next.area_* deliberately unchanged: peer descriptors stay valid.

    // Publish only after the new source page and generation are ready. If the
    // event fails, keep the old port state and release the new source page.
    status = emitHandleReplacedLocked(makePortSupplyLocked(next));
    if (!status.ok()) {
        if (new_source) (void)adapter_->unregisterLocalSegment(new_source);
        if (new_source_memory) std::free(new_source_memory);
        (void)adapter_->resetJetty(new_jetty);
        (void)adapter_->deleteJetty(new_jetty);
        return status;
    }

    retired_pages_.push_back(RetiredPage{std::move(port.source_segment),
                                         port.source_memory, port.source_size,
                                         old_generation});
    port.source_memory = nullptr;
    port.source_size = 0;
    port.probe_jetty = std::move(new_jetty);
    port.source_segment = std::move(new_source);
    port.source_memory = new_source_memory;
    port.source_size = page;
    port.config.generation = next_generation;
    port.source_generation = next_generation;
    auto delete_status = adapter_->deleteJetty(old_jetty);
    if (!delete_status.ok()) {
        LOG(WARNING) << "UB probe: old Jetty deletion deferred: "
                     << delete_status.ToString();
    }
    return Status::OK();
}

Status StandaloneProbeResourceProvider::emitHandleReplacedLocked(
    const ProbePortSupply& supply) {
    if (availability_sink_ == nullptr) {
        return Status::InvalidArgument(
            "probe provider has no availability sink");
    }
    if (graph_epoch_ == 0) {
        return Status::InvalidArgument("probe graph epoch is not initialized");
    }
    ProbeEvent event;
    event.kind = ProbeEventKind::kProbeHandleReplaced;
    event.local_nic = supply.local_nic;
    event.graph_epoch = graph_epoch_;
    event.event_seq = event_seq_ + 1;
    event.port = supply;
    auto status = availability_sink_->onPoolAvailabilityChange(event);
    if (status.ok()) ++event_seq_;
    return status;
}

void StandaloneProbeResourceProvider::releaseRetiredPagesLocked() {
    for (auto& page : retired_pages_) {
        if (page.segment) {
            (void)adapter_->unregisterLocalSegment(page.segment);
        }
        if (page.memory != nullptr) std::free(page.memory);
    }
    retired_pages_.clear();
}

size_t StandaloneProbeResourceProvider::retiredPageCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return retired_pages_.size();
}

Status StandaloneProbeResourceProvider::shutdown() {
    {
        std::lock_guard<std::mutex> lock(rebuild_mutex_);
        rebuild_stopping_ = true;
        rebuild_queue_.clear();
    }
    rebuild_cv_.notify_all();
    if (maintenance_.joinable()) maintenance_.join();

    std::lock_guard<std::mutex> lock(mutex_);
    if (shut_down_) return Status::OK();
    Status first_error = Status::OK();
    // Drop handoff references first: they may still point at older-generation
    // source pages that the current port state does not own.
    releaseQuarantineLocked();
    releaseRetiredPagesLocked();
    for (auto& port : ports_) {
        auto status = destroyPortLocked(port);
        if (first_error.ok() && !status.ok()) first_error = status;
    }
    if (owns_contexts_) {
        for (auto& port : ports_) {
            if (!port.config.context) continue;
            auto status = adapter_->closeContext(port.config.context);
            if (first_error.ok() && !status.ok()) first_error = status;
        }
        ports_.clear();
    }
    resources_ready_ = false;
    initialized_ = false;
    shut_down_ = true;
    LOG(INFO) << "UB standalone probe provider shut down";
    return first_error;
}

std::vector<StandaloneProbeResourceProvider::PortView>
StandaloneProbeResourceProvider::portViews() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<PortView> views;
    views.reserve(ports_.size());
    for (const auto& port : ports_) {
        if (!port.available || !port.probe_jetty) continue;
        views.push_back(PortView{
            port.config.local_nic, port.config.generation,
            port.probe_jetty->id(),
            reinterpret_cast<uint64_t>(port.source_memory), 1,
            port.source_generation, port.owns_jfc, port.probe_jetty->uasid(),
            port.config.context ? port.config.context->deviceInfo().eid
                                : std::string(),
            port.topology_name, port.native_device_name});
    }
    return views;
}

std::vector<StandaloneProbeResourceProvider::LocalAreaView>
StandaloneProbeResourceProvider::localAreaViews() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<LocalAreaView> views;
    views.reserve(ports_.size());
    for (const auto& port : ports_) {
        if (!port.available || !port.area_segment) continue;
        LocalAreaView view;
        view.local_nic = port.config.local_nic;
        view.generation = port.config.generation;
        view.address = reinterpret_cast<uint64_t>(port.area_memory);
        view.length = port.area_size;
        view.token = kProbeAreaToken;
        view.epoch = port.area_epoch;
        view.descriptor = port.area_segment->descriptor();
        views.push_back(std::move(view));
    }
    return views;
}

size_t StandaloneProbeResourceProvider::quarantineCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return quarantine_.size();
}

uint64_t StandaloneProbeResourceProvider::retireCalls() const {
    return retire_calls_.load(std::memory_order_relaxed);
}

bool StandaloneProbeResourceProvider::initialized() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return initialized_;
}

}  // namespace ub
}  // namespace tent
}  // namespace mooncake
