// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/probe_link_manager.h"
#include "tent/transport/ub/probe_peer_handshake.h"
#include "tent/transport/ub/probe_poster_urma.h"
#include "tent/transport/ub/probe_provider_standalone.h"
#include "tent/transport/ub/probe_scheduler.h"
#include "tent/transport/ub/ub_transport.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <limits>
#include <map>
#include <mutex>
#include <new>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <glog/logging.h>

#include "tent/common/utils/string_builder.h"
#include "tent/runtime/segment.h"
#include "tent/thirdparty/nlohmann/json.h"
#include "tent/transport/ub/admission.h"
#include "tent/transport/ub/buffers.h"
#include "tent/transport/ub/context.h"
#include "tent/transport/ub/device_selection.h"
#include "tent/transport/ub/endpoint.h"
#include "tent/transport/ub/endpoint_store.h"
#include "tent/transport/ub/native_budget.h"
#include "tent/transport/ub/params.h"
#include "tent/transport/ub/send_jetty_pool.h"
#include "tent/transport/ub/quota.h"
#include "tent/transport/ub/rail_monitor.h"
#include "tent/transport/ub/slice.h"
#include "tent/transport/ub/urma_adapter.h"
#include "tent/transport/ub/workers.h"

namespace mooncake::tent {
namespace {

bool filterAllows(const std::vector<std::string>& filter,
                  const ub::DeviceInfo& device) {
    if (filter.empty()) return true;
    return std::find(filter.begin(), filter.end(), device.topology_name) !=
               filter.end() ||
           std::find(filter.begin(), filter.end(), device.native_device_name) !=
               filter.end();
}

uint64_t generationSeed() {
    const auto wall = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    const auto steady = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
    const uint64_t seed =
        (wall ^ (steady << 19) ^ (steady >> 5)) & 0x7fffffffffffffffULL;
    return seed == 0 ? 1 : seed;
}

std::string encodeDeviceMetadata(Topology::NicID topology_id,
                                 const ub::DeviceInfo& device) {
    const auto& caps = device.capabilities;
    return nlohmann::json{{"schema_version", 1},
                          {"topology_id", topology_id},
                          {"native_device_name", device.native_device_name},
                          {"native_device_path", device.native_device_path},
                          {"eid_index", device.eid_index},
                          {"eid", device.eid},
                          {"active", device.active},
                          {"capabilities",
                           {{"max_jfc", caps.max_jfc},
                            {"max_jfc_depth", caps.max_jfc_depth},
                            {"max_jfr_depth", caps.max_jfr_depth},
                            {"max_jetty", caps.max_jetty},
                            {"max_jetty_depth", caps.max_jetty_depth},
                            {"max_send_sge", caps.max_send_sge},
                            {"max_remote_sge", caps.max_remote_sge},
                            {"max_message_size", caps.max_message_size},
                            {"max_read_size", caps.max_read_size},
                            {"max_write_size", caps.max_write_size},
                            {"feature_flags", caps.feature_flags},
                            {"transport_modes", caps.transport_modes}}}}
        .dump();
}

}  // namespace

struct UbTransport::Impl {
    Impl(std::shared_ptr<ub::UrmaAdapter> injected,
         std::shared_ptr<ub::ProbeResourceProvider> injected_probe_provider,
         std::shared_ptr<ub::ProbePoster> injected_probe_poster,
         ub::ProbePeerDescriptorSource* injected_peer_source)
        : adapter(injected ? std::move(injected)
                           : ub::createDefaultUrmaAdapter()),
          probe_peer_source(injected_peer_source),
          probe_provider(std::move(injected_probe_provider)),
          probe_poster(std::move(injected_probe_poster)) {}

    Status install(const std::string& segment_name,
                   std::shared_ptr<ControlService> control,
                   std::shared_ptr<Topology> topology,
                   std::shared_ptr<Config> config) {
        std::lock_guard<std::mutex> lock(lifecycle_mutex);
        if (installed.load(std::memory_order_acquire)) {
            return Status::InvalidArgument(
                "UB transport has already been installed" LOC_MARK);
        }
        if (shutting_down.load(std::memory_order_acquire) || workers ||
            adapter_initialized) {
            return Status::InvalidArgument(
                "A previous UB uninstall has not drained yet" LOC_MARK);
        }
        if (segment_name.empty() || !control || !topology) {
            return Status::InvalidArgument(
                "UB install requires segment, control service and "
                "topology" LOC_MARK);
        }
        if (!adapter || !adapter->available()) {
            return Status::DeviceNotFound(
                "A real or injected URMA adapter is unavailable" LOC_MARK);
        }
        if (!config) config = std::make_shared<Config>();
        ub::UbParams parsed;
        CHECK_STATUS(ub::UbParams::FromConfig(*config, parsed));
        if (!parsed.enable) {
            return Status::InvalidArgument(
                "UB transport is disabled by configuration" LOC_MARK);
        }
        if (parsed.enable_notifications) {
            return Status::NotImplemented(
                "UB notifications are not supported by protocol version "
                "1" LOC_MARK);
        }
        if (parsed.runtime_profile == ub::UbRuntimeProfile::kRmLegacy) {
            // W3 wires exactly two profiles: the untouched RC legacy baseline
            // and the pooled rm_pool path (D-2). RM legacy depends on the
            // pooling side's bootstrap-v2 target import, which this baseline
            // does not carry yet (M-4), so it fails closed instead of silently
            // running RC under an RM label.
            return Status::NotImplemented(
                "runtime_profile=rm_legacy is not wired in W3; use rc_legacy "
                "or rm_pool" LOC_MARK);
        }

        shutting_down.store(false, std::memory_order_release);
        local_segment_name = segment_name;
        metadata = std::move(control);
        local_topology = std::move(topology);
        conf = std::move(config);
        params = parsed;

        auto status = adapter->initialize();
        if (!status.ok()) return failInstall(status);
        adapter_initialized = true;

        std::vector<ub::DeviceInfo> discovered;
        status = adapter->discoverDevices(discovered);
        if (!status.ok()) return failInstall(status);

        const bool explicit_filter = !params.device_filter.empty();
        const auto preferred_devices =
            ub::preferBondingDevicesIfPresent(discovered, explicit_filter);
        const bool prefer_bonding =
            !explicit_filter &&
            std::any_of(discovered.begin(), discovered.end(),
                        [](const ub::DeviceInfo& device) {
                            return ub::isBondingDevice(device);
                        });
        std::unordered_set<std::string> preferred_topology_names;
        preferred_topology_names.reserve(preferred_devices.size());
        for (const auto& device : preferred_devices) {
            preferred_topology_names.insert(device.topology_name);
        }

        std::unordered_map<std::string, ub::DeviceInfo> by_topology_name;
        std::unordered_map<Topology::NicID, uint32_t> jfc_depth_by_device;
        for (auto& device : discovered) {
            by_topology_name.emplace(device.topology_name, std::move(device));
        }

        std::vector<std::string> preferred_names;
        std::vector<std::string> skipped_names;

        for (size_t id = 0; id < local_topology->getNicCount(); ++id) {
            const auto* nic = local_topology->getNicEntry(static_cast<int>(id));
            if (!nic || nic->type != Topology::NIC_UB) continue;
            auto found = by_topology_name.find(nic->name);
            if (found == by_topology_name.end() || !found->second.active) {
                continue;
            }
            if (!filterAllows(params.device_filter, found->second)) continue;
            if (preferred_topology_names.find(found->second.topology_name) ==
                preferred_topology_names.end()) {
                skipped_names.push_back(found->second.native_device_name);
                continue;
            }

            ub::JfcOptions jfc_options;
            // W3 B2: UMDK requires JFS.trans_mode == shared JFR.trans_mode
            // (urma_adapter.cpp RealJetty::initialize). The business JFC of an
            // rm_pool context therefore has to create an RM JFR, otherwise
            // every createJetty for a pooled member is rejected by the provider
            // and tryLease can never succeed.
            if (params.runtime_profile == ub::UbRuntimeProfile::kRmPool) {
                jfc_options.transport_mode = ub::JettyTransportMode::kRm;
            }
            const auto& device_caps = found->second.capabilities;
            if (device_caps.max_jfc_depth != 0) {
                jfc_options.depth =
                    std::min(jfc_options.depth, device_caps.max_jfc_depth);
                jfc_options.receiver_depth = std::min(
                    jfc_options.receiver_depth, device_caps.max_jfc_depth);
            }
            if (device_caps.max_jfr_depth != 0) {
                jfc_options.receiver_depth = std::min(
                    jfc_options.receiver_depth, device_caps.max_jfr_depth);
            }
            auto context = std::make_shared<ub::UbContext>(
                static_cast<Topology::NicID>(id), found->second, adapter);
            status = context->initialize(params.jfc_per_context, jfc_options);
            if (!status.ok()) {
                if (context->state() == ub::UbContext::State::kDraining) {
                    // Initialization produced a native handle whose cleanup
                    // also failed. Adopt it into the transport graph and abort
                    // installation so uninstall can retry without a leak.
                    contexts.push_back(std::move(context));
                    return failInstall(status);
                }
                LOG(WARNING) << "Disable UB device " << nic->name << ": "
                             << status.ToString();
                continue;
            }
            preferred_names.push_back(found->second.native_device_name);
            context_by_topology_id.emplace(static_cast<int>(id), context);
            context_by_topology_name.emplace(nic->name, context);
            jfc_depth_by_device.emplace(static_cast<Topology::NicID>(id),
                                        jfc_options.depth);
            contexts.push_back(std::move(context));
        }

        {
            std::ostringstream selection_log;
            if (explicit_filter) {
                selection_log << "UB device selection: mode=explicit-filter";
            } else if (prefer_bonding) {
                selection_log
                    << "UB device selection: mode=auto-prefer-bonding";
            } else {
                selection_log << "UB device selection: mode=all-devices";
            }
            if (!preferred_names.empty()) {
                selection_log << " preferred=[";
                for (size_t i = 0; i < preferred_names.size(); ++i) {
                    if (i != 0) selection_log << ", ";
                    selection_log << preferred_names[i];
                }
                selection_log << "]";
            }
            if (!skipped_names.empty()) {
                selection_log << " skipped=[";
                for (size_t i = 0; i < skipped_names.size(); ++i) {
                    if (i != 0) selection_log << ", ";
                    selection_log << skipped_names[i];
                }
                selection_log << "]";
            }
            LOG(INFO) << selection_log.str();
        }

        if (contexts.empty()) {
            return failInstall(Status::DeviceNotFound(
                "No UB context initialized successfully" LOC_MARK));
        }

        size_t safe_slice_size = params.slice_size;
        ub::JettyOptions jetty_options;
        for (const auto& context : contexts) {
            const auto& device_caps = context->deviceInfo().capabilities;
            auto clamp_slice = [&safe_slice_size](uint64_t limit) {
                if (limit != 0) {
                    safe_slice_size = static_cast<size_t>(
                        std::min<uint64_t>(safe_slice_size, limit));
                }
            };
            clamp_slice(device_caps.max_message_size);
            clamp_slice(device_caps.max_read_size);
            clamp_slice(device_caps.max_write_size);
            if (device_caps.max_jetty_depth != 0) {
                jetty_options.depth =
                    std::min(jetty_options.depth, device_caps.max_jetty_depth);
            }
            if (device_caps.max_send_sge != 0) {
                jetty_options.max_sge = static_cast<uint8_t>(std::min<uint32_t>(
                    jetty_options.max_sge, device_caps.max_send_sge));
            }
            if (device_caps.max_remote_sge != 0) {
                jetty_options.max_sge = static_cast<uint8_t>(std::min<uint32_t>(
                    jetty_options.max_sge, device_caps.max_remote_sge));
            }
        }
        // rm_pool runs the RM transport (validated by the config gate). Both
        // the pooled members and this side's endpoint Jetties are created in RM
        // mode, and an RM Jetty is never bound: every WR carries its own
        // imported target (design §5.2 / M-4).
        if (parsed.runtime_profile == ub::UbRuntimeProfile::kRmPool) {
            jetty_options.transport_mode = ub::JettyTransportMode::kRm;
        }
        if (safe_slice_size == 0 || jetty_options.depth == 0 ||
            jetty_options.max_sge == 0) {
            return failInstall(Status::InvalidArgument(
                "UB device capabilities cannot support the configured data "
                "path" LOC_MARK));
        }
        if (safe_slice_size != params.slice_size) {
            LOG(WARNING) << "Clamp UB slice_size from " << params.slice_size
                         << " to device limit " << safe_slice_size;
            params.slice_size = safe_slice_size;
        }

        buffers = std::make_unique<ub::UbBufferManager>(adapter, contexts);
        endpoints = std::make_unique<ub::EndpointStore>(
            adapter, params.max_endpoints, params.jetty_per_endpoint,
            jetty_options);
        ub::RailMonitorConfig rail_config;
        rail_config.cooldown_ns =
            static_cast<uint64_t>(params.endpoint_cooldown_ms) * 1'000'000ULL;
        rails = std::make_unique<ub::RailMonitor>(rail_config);
        auto saturatedProduct = [](uint64_t lhs, uint64_t rhs) {
            if (lhs != 0 && rhs > std::numeric_limits<uint64_t>::max() / lhs) {
                return std::numeric_limits<uint64_t>::max();
            }
            return lhs * rhs;
        };
        const uint64_t path_wrs =
            saturatedProduct(jetty_options.depth, params.jetty_per_endpoint);
        ub::QuotaLimits path_limits{
            saturatedProduct(path_wrs, params.slice_size), path_wrs};
        quota = std::make_unique<ub::QuotaManager>(path_limits, path_limits);
        for (const auto& context : contexts) {
            const auto depth = jfc_depth_by_device.at(context->topologyId());
            const uint64_t device_wrs =
                saturatedProduct(depth, context->jfcs().size());
            (void)quota->setDeviceLimits(
                context->topologyId(),
                ub::QuotaLimits{saturatedProduct(device_wrs, params.slice_size),
                                device_wrs});
        }

        // ---- W3 pooled assembly (design §11.1) --------------------------
        // One SendJettyPool + one JfcCreditLedger per Context, and one
        // NativeBudget per physical device shared by every Context of that
        // device. Every step is fail-closed: a pool that cannot be budgeted or
        // credited must not exist, because a lease without credit accounting
        // would post WRs the JFC cannot hold.
        if (parsed.runtime_profile == ub::UbRuntimeProfile::kRmPool) {
            // M9: rm_pool runs the RM transport, so every selected device must
            // advertise it. Failing here is much cheaper than failing on the
            // first createJetty.
            for (const auto& context : contexts) {
                if (!context) continue;
                const uint16_t modes =
                    context->deviceInfo().capabilities.transport_modes;
                if (modes != 0 && (modes & ub::kUrmaTransportModeRm) == 0) {
                    return failInstall(Status::InvalidArgument(
                        "runtime_profile=rm_pool requires devices that support "
                        "the RM transport mode" LOC_MARK));
                }
            }
            status = installPooledSendPools(parsed, jetty_options,
                                            jfc_depth_by_device);
            if (!status.ok()) return failInstall(status);
        }

        status = publishLocalDevices();
        if (!status.ok()) return failInstall(status);

        metadata->setBootstrapUbCallback(
            [this](const UbBootstrapDesc& request, UbBootstrapDesc& response) {
                return onBootstrap(request, response);
            });
        callback_installed = true;

        // Probe assembly (design §13.3). Probing is disabled by default; when
        // enabled, the pooling side must supply a complete graph and a poster,
        // otherwise installation fails fast instead of degrading silently.
        std::vector<ub::ProbeJfcSlot> probe_jfcs;
        if (params.probe.enabled) {
            if (!probe_provider || !probe_poster) {
                if (params.probe.provider !=
                    ub::ProbeProviderKind::kStandalone) {
                    return failInstall(Status::InvalidArgument(
                        "UB probe is enabled but no probe provider/poster is "
                        "installed; inject one or set "
                        "transports/ub/probe/provider=standalone" LOC_MARK));
                }
                if (probe_peer_source == nullptr) {
                    return failInstall(Status::InvalidArgument(
                        "UB probe provider=standalone requires a peer "
                        "descriptor source" LOC_MARK));
                }
                auto standalone =
                    std::make_shared<ub::StandaloneProbeResourceProvider>(
                        adapter, params.probe,
                        [topology =
                             local_topology](const std::string& topology_name) {
                            return topology ? topology->getNicId(topology_name)
                                            : static_cast<Topology::NicID>(-1);
                        });
                auto discover = standalone->discover();
                if (!discover.ok()) return failInstall(discover);
                probe_directory =
                    std::make_shared<ub::HandshakeProbePeerDirectory>(
                        adapter, standalone->contexts(), probe_peer_source);
                standalone->setRemoteDirectory(probe_directory.get());
                probe_provider = standalone;
                probe_poster = std::make_shared<ub::UrmaProbePoster>(adapter);
            }
            ub::ProbeGraph graph;
            status = probe_provider->snapshot(graph);
            if (!status.ok()) return failInstall(status);
            probe_manager =
                std::make_unique<ub::ProbeLinkManager>(params.probe);
            probe_manager->setReportSink(probe_provider.get());
            status = probe_manager->initialize(graph);
            if (!status.ok()) return failInstall(status);
            probe_provider->setAvailabilitySink(
                probe_manager->availabilitySink());
            probe_provider->setCompletionSink(
                [manager = probe_manager.get()](
                    const ub::Completion& completion) -> Status {
                    if (manager == nullptr) {
                        return Status::InvalidArgument(
                            "probe manager is gone" LOC_MARK);
                    }
                    // Quiesce is per probe Jetty, so every completion here
                    // belongs to the probe namespace.
                    if (!ub::isProbeToken(completion.token)) {
                        return Status::InvalidArgument(
                            "non-probe completion on a probe lane" LOC_MARK);
                    }
                    if (!manager->onProbeCompletion(completion)) {
                        return Status::InternalError(
                            "probe completion was not accepted" LOC_MARK);
                    }
                    return Status::OK();
                });
            for (const auto& supply : graph.ports) {
                if (context_by_topology_id.find(supply.local_nic) ==
                    context_by_topology_id.end()) {
                    return failInstall(
                        Status::InvalidArgument("UB probe port has no matching "
                                                "local context" LOC_MARK));
                }
                // nullptr means shared mode: probe completions land on the
                // business JFC and are routed by token namespace.
                if (!supply.probe_jfc) continue;
                probe_jfcs.push_back(
                    ub::ProbeJfcSlot{supply.local_nic, supply.probe_jfc});
            }
        }

        workers = std::make_unique<ub::UbWorkers>(
            adapter, contexts, local_topology, &metadata->segmentManager(),
            buffers.get(), rails.get(), quota.get(), params,
            [this](const ub::EndpointResolveRequest& request,
                   std::shared_ptr<ub::UbEndpoint>& endpoint) {
                return resolveEndpoint(request, endpoint);
            },
            [this](const std::shared_ptr<ub::UbEndpoint>& endpoint) {
                if (endpoints) (void)endpoints->retire(endpoint);
            },
            [this](Topology::NicID local_topology_id) {
                if (!endpoints) {
                    return Status::InternalError(
                        "UB endpoint store is unavailable during device "
                        "failure cleanup" LOC_MARK);
                }
                return endpoints->retireLocalDevice(local_topology_id);
            },
            probe_manager.get(), std::move(probe_jfcs));
        status = workers->start();
        if (!status.ok()) return failInstall(status);

        // Pollers run before the scheduler so a probe completion can always be
        // consumed (design §13.3).
        if (probe_manager) {
            try {
                probe_scheduler = std::make_unique<ub::ProbeScheduler>(
                    *probe_manager, *probe_poster);
                probe_scheduler->start();
            } catch (const std::exception& error) {
                return failInstall(Status::InternalError(
                    std::string("Cannot start the UB probe scheduler: ") +
                    error.what() + LOC_MARK));
            }
        }

        installed.store(true, std::memory_order_release);
        return Status::OK();
    }

    // W3 pooled assembly (design §11.1). One pool + credit ledger per Context
    // and one native budget per physical device. Called from install() after
    // every device gate passed and before any pooled Jetty can be created.
    Status installPooledSendPools(
        const ub::UbParams& parsed, const ub::JettyOptions& jetty_options,
        const std::unordered_map<Topology::NicID, uint32_t>&
            jfc_depth_by_device) {
        const uint64_t credit_reserve = parsed.send_pool.jfc_credit_reserve;
        // M5: a static Serving target that the device budget cannot hold makes
        // tryLease miss forever (permanent defer instead of fail-fast).
        std::unordered_map<std::string, uint64_t> contexts_per_device;
        for (const auto& context : contexts) {
            if (!context) continue;
            const auto& device = context->deviceInfo();
            ++contexts_per_device[device.native_device_name + ":" +
                                  device.native_device_path + ":" + device.eid];
        }
        std::unordered_map<std::string, std::shared_ptr<ub::NativeBudget>>
            budgets;
        for (const auto& context : contexts) {
            if (!context || !context->handle()) continue;
            const auto& device = context->deviceInfo();
            // A physical device is identified by name + path + EID, so every
            // Context sitting on the same device shares one ledger and can
            // never overcommit the device's Jetty limit (C05).
            const std::string device_key = device.native_device_name + ":" +
                                           device.native_device_path + ":" +
                                           device.eid;
            auto& budget = budgets[device_key];
            if (!budget) {
                budget = std::make_shared<ub::NativeBudget>();
                const uint64_t verified = device.capabilities.max_jetty;
                // Derivation keeps the replacement reserve outside the device's
                // own limit: NativeBudget::configure() checks
                // (configured + reserve) <= verified, so deriving configured as
                // `verified` fails install on every device that reports its
                // limit. See deriveNativeBudgetJetties().
                const uint64_t configured = ub::deriveNativeBudgetJetties(
                    verified, parsed.native_budget.max_jetty_per_device,
                    parsed.native_budget.replacement_reserve,
                    parsed.send_pool.desired_per_context);
                // M5: the pooled members alone must fit per device. Endpoint
                // Jetties stay outside the NativeBudget ledger for now
                // (RK5/M10), so their share is surfaced instead of being
                // silently overcommitted.
                const uint64_t per_context =
                    static_cast<uint64_t>(parsed.send_pool.desired_per_context);
                const uint64_t contexts_on_device =
                    contexts_per_device.count(device_key) != 0
                        ? contexts_per_device[device_key]
                        : 1;
                const uint64_t endpoint_share =
                    static_cast<uint64_t>(parsed.jetty_per_endpoint);
                if (configured < per_context * contexts_on_device) {
                    return Status::InvalidArgument(
                        "transports/ub/native_budget/max_jetty_per_device is "
                        "smaller than send_pool/desired_per_context for " +
                        device_key);
                }
                if (verified != 0 && configured + endpoint_share > verified) {
                    LOG(WARNING)
                        << "UB rm_pool budget for device " << device_key
                        << " leaves no room for the endpoint Jetty set: "
                           "budget="
                        << configured << " endpoint=" << endpoint_share
                        << " verified=" << verified
                        << "; endpoint Jetties remain outside the ledger "
                           "(RK5)";
                }
                auto status =
                    budget->configure(device_key, verified, configured,
                                      parsed.native_budget.replacement_reserve);
                if (!status.ok()) return status;
            }

            auto credits = std::make_shared<ub::JfcCreditLedger>();
            const auto depth_it =
                jfc_depth_by_device.find(context->topologyId());
            const uint64_t depth =
                depth_it == jfc_depth_by_device.end() ? 0 : depth_it->second;
            if (depth <= credit_reserve) {
                return Status::InvalidArgument(
                    "transports/ub/send_pool/jfc_credit_reserve must be "
                    "smaller than the JFC depth" LOC_MARK);
            }
            for (const auto& jfc : context->jfcs()) {
                if (!jfc) continue;
                // capacity = jfc_depth - jfc_credit_reserve: the reserve
                // belongs to control records (markers, AE) and business WRs
                // never see it (C05/C06).
                auto status =
                    credits->install(jfc->index(), depth - credit_reserve);
                if (!status.ok()) return status;
            }

            ub::SendJettyPoolOptions options;
            options.desired_serving = parsed.send_pool.desired_per_context;
            options.max_creating = parsed.send_pool.max_creating_per_key;
            options.jetty_options = jetty_options;
            auto pool = std::make_shared<ub::SendJettyPool>(
                adapter, context->handle(), context->jfcs(), budget, device_key,
                credits, options);
            // Attaching the pool is what flips this Context into the W3 path
            // (D-2). Nothing else in the transport changes behavior.
            context->attachPooledSendPool(std::move(pool));
        }
        return Status::OK();
    }

    Status uninstall() {
        std::lock_guard<std::mutex> lock(lifecycle_mutex);
        return shutdownUnlocked();
    }

    Status failInstall(Status failure) {
        // Install now fails fast, so a failed rollback can leave native
        // resources owned by this object. Surface that instead of silently
        // discarding it, otherwise the leak is only visible as a later
        // install/uninstall failure.
        auto cleanup = shutdownUnlocked();
        if (!cleanup.ok()) {
            LOG(WARNING)
                << "UB transport rollback after a failed install could "
                   "not release every native resource: "
                << cleanup.ToString();
        }
        return failure;
    }

    Status shutdownUnlocked() {
        installed.store(false, std::memory_order_release);
        shutting_down.store(true, std::memory_order_release);

        // Callback replacement waits for a currently executing UB bootstrap
        // handler, fencing all control-plane access before resources retire.
        if (callback_installed && metadata) {
            metadata->setBootstrapUbCallback({});
            callback_installed = false;
        }
        // Stop interface-7 rebuilds before draining workers: a retire during
        // teardown must stay a record, not create resources that are about to
        // be destroyed.
        if (probe_provider) {
            probe_provider->setAvailabilitySink(nullptr);
            probe_provider->setCompletionSink(nullptr);
        }
        if (workers) {
            auto status = workers->stop();
            if (!status.ok()) {
                // A failed native drain fence is not permission to destroy
                // memory registrations, JFCs, Contexts, or the adapter. Keep
                // the complete ownership graph alive so uninstall can be
                // retried after a late completion/provider recovery.
                return status;
            }
            workers.reset();
        }
        // Workers joined the scheduler through drainBeforePollStop(); destroy
        // the PLM only after the workers that reference it are gone.
        probe_scheduler.reset();
        probe_manager.reset();
        probe_poster.reset();
        // Unimport remote probe areas while the probe Contexts are still
        // alive; the self-owned provider closes them in its shutdown.
        if (probe_directory) (void)probe_directory->shutdown();
        probe_provider.reset();
        probe_directory.reset();
        // W3 pooled teardown safety net (design §11.2). workers->stop() already
        // drained the pools, but an install that failed before the workers
        // existed can leave a pool attached to its context. beginStop() and
        // both drains are idempotent, so running them here covers both paths
        // and guarantees that no pooled Jetty outlives its budget ledger.
        for (const auto& context : contexts) {
            if (!context) continue;
            auto pool = context->pooledSendPool();
            if (!pool) continue;
            pool->beginStop();
            (void)pool->drainIdle();
            (void)pool->drainQuarantined();
        }
        if (endpoints) {
            auto status = endpoints->clear();
            if (!status.ok()) return status;
            endpoints.reset();
        }
        if (buffers) {
            auto status = buffers->clear();
            if (!status.ok()) return status;
            buffers.reset();
        }
        for (auto it = contexts.rbegin(); it != contexts.rend(); ++it) {
            if (!*it) continue;
            auto status = (*it)->shutdown();
            if (!status.ok()) {
                // Keep the entire context vector and both lookup maps until
                // every JFC and Context has been released. Some contexts may
                // already be closed, but their idempotent shutdown keeps the
                // ownership graph intact while the failed handle is retried.
                return status;
            }
        }
        contexts.clear();
        context_by_topology_id.clear();
        context_by_topology_name.clear();
        rails.reset();
        quota.reset();
        if (adapter_initialized && adapter) {
            auto status = adapter->shutdown();
            if (!status.ok()) {
                // A failed provider shutdown remains an initialized adapter
                // for lifecycle purposes. Do not permit reinstall or discard
                // it; a later uninstall retries the provider operation.
                return status;
            }
            adapter_initialized = false;
        }
        metadata.reset();
        local_topology.reset();
        conf.reset();
        local_segment_name.clear();
        shutting_down.store(false, std::memory_order_release);
        return Status::OK();
    }

    Status publishLocalDevices() {
        auto& manager = metadata->segmentManager();
        CHECK_STATUS(
            manager.updateLocal([this](SegmentDesc& segment) -> Status {
                if (segment.type != SegmentType::Memory) {
                    return Status::InvalidMetadataType(
                        "Local segment is not memory-backed" LOC_MARK);
                }
                auto& detail = std::get<MemorySegmentDesc>(segment.detail);
                detail.transport_attrs[TransportType::UB] = nlohmann::json{
                    {"schema_version", 1},
                    {"protocol", "urma"},
                    {"notifications", false}}.dump();
                std::unordered_set<std::string> existing;
                for (const auto& device : detail.devices) {
                    existing.insert(device.name);
                }
                for (const auto& context : contexts) {
                    if (!context || !context->active()) continue;
                    if (existing.insert(context->deviceInfo().topology_name)
                            .second) {
                        DeviceDesc device;
                        device.name = context->deviceInfo().topology_name;
                        device.lid = 0;
                        device.gid.clear();
                        device.transport_attrs[TransportType::UB] =
                            encodeDeviceMetadata(context->topologyId(),
                                                 context->deviceInfo());
                        detail.devices.push_back(std::move(device));
                    }
                }
                return Status::OK();
            }));
        return manager.synchronizeLocal();
    }

    void recordReadyEndpoint(const std::shared_ptr<ub::UbEndpoint>& endpoint) {
        if (!endpoint || !endpoint->ready() || !rails) return;

        // RailMonitor keeps the per-rail generation watermark and ignores
        // generations it has already seen, so this is safe to call from every
        // resolve as well as from converging or retried rebuild paths. Keeping
        // the watermark there also means no peer-keyed state accumulates here
        // across peer restarts, and the posting hot path takes only
        // RailMonitor's lock instead of a second transport-wide one.
        const auto& key = endpoint->key();
        rails->recordEndpointRebuild(
            ub::UbPostPath{key.local_topology_id, key.remote_segment_id,
                           key.remote_topology_id, endpoint->generation()});
    }

    Status resolveEndpoint(const ub::EndpointResolveRequest& request,
                           std::shared_ptr<ub::UbEndpoint>& endpoint) {
        endpoint.reset();
        if (shutting_down.load(std::memory_order_acquire) || !endpoints ||
            !request.local_context || !request.remote_segment) {
            return Status::InvalidArgument(
                "UB transport is shutting down or route is invalid" LOC_MARK);
        }
        const auto* remote_nic =
            request.remote_segment->getMemory().topology.getNicEntry(
                request.remote_topology_id);
        if (!remote_nic || remote_nic->type != Topology::NIC_UB) {
            return Status::DeviceNotFound(
                "Remote segment does not advertise a UB topology "
                "device" LOC_MARK);
        }
        const std::string peer_path =
            MakeNicPath(request.remote_segment->name, remote_nic->name);
        ub::UbEndpointKey key{request.local_context->topologyId(),
                              request.remote_segment_id,
                              request.remote_topology_id, peer_path};
        CHECK_STATUS(
            endpoints->getOrCreate(key, request.local_context, endpoint));
        if (endpoint->ready()) {
            recordReadyEndpoint(endpoint);
            return Status::OK();
        }

        const std::string local_path =
            MakeNicPath(local_segment_name,
                        request.local_context->deviceInfo().topology_name);
        UbBootstrapDesc bootstrap;
        auto status = endpoint->makeBootstrapDesc(
            local_segment_name, local_path, peer_path,
            request.segment_generation, bootstrap);
        if (!status.ok()) {
            (void)endpoints->retire(endpoint);
            endpoint.reset();
            return status;
        }
        UbBootstrapDesc response;
        status = ControlClient::bootstrapUb(
            request.remote_segment->rpc_server_addr, bootstrap, response);
        if (status.ok()) status = endpoint->bind(response);
        if (!status.ok()) {
            (void)endpoints->retire(endpoint);
            endpoint.reset();
            return status;
        }
        recordReadyEndpoint(endpoint);
        return Status::OK();
    }

    int onBootstrap(const UbBootstrapDesc& request, UbBootstrapDesc& response) {
        if (shutting_down.load(std::memory_order_acquire) || !endpoints) {
            response.reply_msg = "UB transport is shutting down";
            return -1;
        }
        const std::string local_name =
            getNicNameFromNicPath(request.peer_nic_path);
        auto context_it = context_by_topology_name.find(local_name);
        if (local_name.empty() ||
            context_it == context_by_topology_name.end()) {
            response.reply_msg = "UB bootstrap selected an unknown local NIC";
            return -1;
        }
        if (request.local_device_id < 0 || request.local_eid.empty() ||
            request.jetty_ids.empty() || request.endpoint_generation == 0) {
            response.reply_msg = "UB bootstrap request is incomplete";
            return -1;
        }

        ub::UbEndpointKey key{context_it->second->topologyId(),
                              LOCAL_SEGMENT_ID, request.local_device_id,
                              request.local_nic_path};
        std::shared_ptr<ub::UbEndpoint> endpoint;
        auto status = endpoints->getOrCreate(key, context_it->second, endpoint);
        if (status.ok()) status = endpoint->bind(request);
        if (status.ok()) {
            status = endpoint->makeBootstrapDesc(
                local_segment_name, request.peer_nic_path,
                request.local_nic_path, currentSegmentGeneration(), response);
        }
        if (status.ok()) {
            recordReadyEndpoint(endpoint);
        }
        if (!status.ok()) {
            if (endpoint) (void)endpoints->retire(endpoint);
            response = UbBootstrapDesc{};
            response.reply_msg = status.ToString();
            return -1;
        }
        return 0;
    }

    uint64_t currentSegmentGeneration() const {
        // Buffer generations are carried in buffer metadata. The bootstrap
        // field is a peer restart/refresh hint and must still be nonzero even
        // before the first user buffer is registered.
        return segment_generation.load(std::memory_order_relaxed);
    }

    mutable std::mutex lifecycle_mutex;
    std::shared_ptr<ub::UrmaAdapter> adapter;
    bool adapter_initialized{false};
    bool callback_installed{false};
    std::atomic<bool> installed{false};
    std::atomic<bool> shutting_down{false};
    std::atomic<uint64_t> segment_generation{generationSeed()};
    ub::UbParams params;
    std::string local_segment_name;
    std::shared_ptr<ControlService> metadata;
    std::shared_ptr<Topology> local_topology;
    std::shared_ptr<Config> conf;
    std::vector<ub::UbContextPtr> contexts;
    std::unordered_map<Topology::NicID, ub::UbContextPtr>
        context_by_topology_id;
    std::unordered_map<std::string, ub::UbContextPtr> context_by_topology_name;
    std::unique_ptr<ub::UbBufferManager> buffers;
    std::unique_ptr<ub::EndpointStore> endpoints;
    std::unique_ptr<ub::RailMonitor> rails;
    std::unique_ptr<ub::QuotaManager> quota;
    // Declared before `workers` so destruction runs workers -> scheduler ->
    // manager -> provider/poster (the workers hold a raw manager pointer).
    // probe_directory is declared before probe_provider so it is destroyed
    // after the provider that borrows it.
    ub::ProbePeerDescriptorSource* probe_peer_source{nullptr};
    std::shared_ptr<ub::HandshakeProbePeerDirectory> probe_directory;
    std::shared_ptr<ub::ProbeResourceProvider> probe_provider;
    std::shared_ptr<ub::ProbePoster> probe_poster;
    std::unique_ptr<ub::ProbeLinkManager> probe_manager;
    std::unique_ptr<ub::ProbeScheduler> probe_scheduler;
    std::unique_ptr<ub::UbWorkers> workers;
};

UbTransport::UbTransport(
    std::shared_ptr<ub::UrmaAdapter> adapter,
    std::shared_ptr<ub::ProbeResourceProvider> probe_provider,
    std::shared_ptr<ub::ProbePoster> probe_poster,
    ub::ProbePeerDescriptorSource* probe_peer_source)
    : impl_(
          std::make_unique<Impl>(std::move(adapter), std::move(probe_provider),
                                 std::move(probe_poster), probe_peer_source)) {}

UbTransport::~UbTransport() {
    auto status = uninstall();
    if (!status.ok()) {
        // There is no caller left to retry an explicit uninstall. Leaking the
        // still-live ownership graph is the only safe failure mode: destroying
        // joinable pollers or registered memory after a failed device fence
        // would terminate the process or permit DMA-after-free. The OS/provider
        // reclaims these resources at process exit.
        LOG(ERROR) << "Preserve undrained UB resources during destruction: "
                   << status.ToString();
        (void)impl_.release();
    }
}

Status UbTransport::install(std::string& local_segment_name,
                            std::shared_ptr<ControlService> metadata,
                            std::shared_ptr<Topology> local_topology,
                            std::shared_ptr<Config> conf) {
    auto status = impl_->install(local_segment_name, std::move(metadata),
                                 std::move(local_topology), std::move(conf));
    if (status.ok()) {
        caps = Capabilities{};
        caps.dram_to_dram = true;
    }
    return status;
}

Status UbTransport::uninstall() {
    auto status = impl_->uninstall();
    caps = Capabilities{};
    return status;
}

Status UbTransport::allocateSubBatch(SubBatchRef& batch, size_t max_size) {
    batch = nullptr;
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->installed.load(std::memory_order_acquire)) {
        return Status::InvalidArgument(
            "UB transport is not installed" LOC_MARK);
    }
    auto* ub_batch = new (std::nothrow) UbSubBatch();
    if (!ub_batch) {
        return Status::InternalError(
            "Unable to allocate UB sub-batch" LOC_MARK);
    }
    ub_batch->max_size = max_size;
    ub_batch->task_list.reserve(max_size);
    batch = ub_batch;
    return Status::OK();
}

Status UbTransport::freeSubBatch(SubBatchRef& batch) {
    auto* ub_batch = dynamic_cast<UbSubBatch*>(batch);
    if (!ub_batch) {
        return Status::InvalidArgument("Invalid UB sub-batch" LOC_MARK);
    }
    ub_batch->task_list.clear();
    delete ub_batch;
    batch = nullptr;
    return Status::OK();
}

Status UbTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& request_list) {
    auto* ub_batch = dynamic_cast<UbSubBatch*>(batch);
    if (!ub_batch) {
        return Status::InvalidArgument("Invalid UB sub-batch" LOC_MARK);
    }
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->installed.load(std::memory_order_acquire) || !impl_->workers) {
        return Status::InvalidArgument(
            "UB transport is not installed" LOC_MARK);
    }
    if (request_list.size() > ub_batch->max_size - ub_batch->task_list.size()) {
        return Status::TooManyRequests("Exceed UB batch capacity" LOC_MARK);
    }
    for (const auto& request : request_list) {
        const auto local_address = reinterpret_cast<uintptr_t>(request.source);
        if (!request.source || request.length == 0 ||
            request.length >
                std::numeric_limits<uint64_t>::max() - request.target_offset ||
            request.length >
                std::numeric_limits<uintptr_t>::max() - local_address) {
            return Status::InvalidArgument(
                "UB request range is empty or overflows" LOC_MARK);
        }
    }

    const auto notify_progress = ub_batch->notify_progress;
    const auto progress_batch_id = ub_batch->progress_batch_id;
    std::vector<ub::UbTask::Ptr> new_tasks;
    new_tasks.reserve(request_list.size());
    for (const auto& request : request_list) {
        const size_t slice_count =
            request.length / impl_->params.slice_size +
            (request.length % impl_->params.slice_size != 0 ? 1 : 0);
        if (slice_count > impl_->params.max_slices_per_task) {
            return Status::InvalidArgument(
                "UB request exceeds "
                "transports/ub/max_slices_per_task" LOC_MARK);
        }
        // W3 D-6: K lives on the task and only binds in pooled mode. A pooled
        // task is capped at task_max_inflight_attempts inflight attempts; the
        // legacy profiles keep the unlimited slice behavior of W2.
        const uint32_t task_limit =
            impl_->params.pooledSendPoolRequested()
                ? impl_->params.task_max_inflight_attempts
                : 0;
        auto task = ub::UbTask::create(
            request,
            [notify_progress, progress_batch_id](const TransferStatus&) {
                if (notify_progress) notify_progress(progress_batch_id);
            },
            /*created_ns=*/0, task_limit);
        size_t offset = 0;
        while (offset < request.length) {
            const size_t length =
                std::min(impl_->params.slice_size, request.length - offset);
            ub::UbSliceSpec spec;
            spec.local_address = static_cast<char*>(request.source) + offset;
            spec.remote_address = request.target_offset + offset;
            spec.length = length;
            spec.request_offset = offset;
            spec.max_retries = impl_->params.max_retries;
            if (!task->addSlice(spec)) {
                return Status::InternalError(
                    "Unable to construct UB slice" LOC_MARK);
            }
            offset += length;
        }
        (void)task->seal();
        new_tasks.push_back(std::move(task));
    }

    for (auto& task : new_tasks) {
        ub_batch->task_list.push_back(task);
        auto status = impl_->workers->submit(task, ub_batch->device_mask);
        if (!status.ok()) {
            for (auto& queued : new_tasks) {
                if (queued) queued->requestCancellation();
            }
            return status;
        }
    }
    return Status::OK();
}

Status UbTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                      TransferStatus& status) {
    auto* ub_batch = dynamic_cast<UbSubBatch*>(batch);
    if (!ub_batch || task_id < 0 ||
        static_cast<size_t>(task_id) >= ub_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid UB task ID" LOC_MARK);
    }
    status = ub_batch->task_list[task_id]->transferStatus();
    return Status::OK();
}

Status UbTransport::cancelTransferTask(SubBatchRef batch, int task_id) {
    auto* ub_batch = dynamic_cast<UbSubBatch*>(batch);
    if (!ub_batch || task_id < 0 ||
        static_cast<size_t>(task_id) >= ub_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid UB task ID" LOC_MARK);
    }
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->workers) {
        return Status::InvalidArgument("UB workers are not running" LOC_MARK);
    }
    return impl_->workers->cancel(ub_batch->task_list[task_id]);
}

Status UbTransport::addMemoryBuffer(BufferDesc& desc,
                                    const MemoryOptions& options) {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->buffers) {
        return Status::InvalidArgument(
            "UB transport is not installed" LOC_MARK);
    }
    auto status = impl_->buffers->addBuffer(desc, options);
    if (status.ok()) {
        impl_->segment_generation.fetch_add(1, std::memory_order_relaxed);
    }
    return status;
}

Status UbTransport::addMemoryBuffer(std::vector<BufferDesc>& desc_list,
                                    const MemoryOptions& options) {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->buffers) {
        return Status::InvalidArgument(
            "UB transport is not installed" LOC_MARK);
    }
    auto status = impl_->buffers->addBuffers(desc_list, options);
    if (status.ok()) {
        impl_->segment_generation.fetch_add(1, std::memory_order_relaxed);
    }
    return status;
}

Status UbTransport::removeMemoryBuffer(BufferDesc& desc) {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->buffers) return Status::OK();
    auto status = impl_->buffers->removeBuffer(desc);
    if (status.ok()) {
        impl_->segment_generation.fetch_add(1, std::memory_order_relaxed);
    }
    return status;
}

bool UbTransport::warmupMemory(void* addr, size_t length) {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!addr || length == 0 || !impl_->adapter || impl_->contexts.empty()) {
        return false;
    }
    ub::LocalSegmentPtr segment;
    ub::SegmentOptions options;
    auto status = impl_->adapter->registerLocalSegment(
        impl_->contexts.front()->handle(), reinterpret_cast<uint64_t>(addr),
        length, options, segment);
    if (!status.ok()) return false;
    return impl_->adapter->unregisterLocalSegment(segment).ok();
}

double UbTransport::getEstimatedBandwidth() const {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->params.enable_bandwidth_estimation || !impl_->rails)
        return -1.0;
    return impl_->rails->aggregateBandwidth();
}

Status UbTransport::getNicLoadStats(std::vector<NicLoadStats>& stats) const {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->quota || !impl_->rails || !impl_->local_topology) {
        return Status::OK();
    }

    std::unordered_map<Topology::NicID, double> bandwidth_by_device;
    for (const auto& rail : impl_->rails->allStats()) {
        if (rail.paused || rail.ewma_bandwidth_bytes_per_second < 0.0) {
            continue;
        }
        auto [it, inserted] = bandwidth_by_device.emplace(
            rail.key.local_topology_id, rail.ewma_bandwidth_bytes_per_second);
        if (!inserted) {
            it->second =
                std::max(it->second, rail.ewma_bandwidth_bytes_per_second);
        }
    }

    const auto device_stats = impl_->quota->allDeviceStats();
    stats.reserve(stats.size() + device_stats.size());
    for (const auto& device : device_stats) {
        std::string name =
            impl_->local_topology->getNicName(device.local_topology_id);
        if (name.empty()) name = std::to_string(device.local_topology_id);
        const auto bandwidth =
            bandwidth_by_device.find(device.local_topology_id);
        stats.push_back(NicLoadStats{
            std::move(name),
            device.usage.inflight_bytes,
            bandwidth == bandwidth_by_device.end() ? 0.0 : bandwidth->second,
        });
    }
    return Status::OK();
}

}  // namespace mooncake::tent
