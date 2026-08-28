#include "ha/leadership/master_service_supervisor.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>

#include <glog/logging.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "ha/leadership/leader_label_reconciler.h"
#include "ha/master_metrics_reporter.h"
#include "ha/standby_controller.h"
#include "k8s_lease_helper.h"
#include "master_admin_service.h"
#include "rpc_service.h"
#include "types.h"

#include "cvm/cvm_controller.h"
#include "cvm/cvm_service_delegate.h"
#include "cvm/etcd_view_store.h"
#include "etcd_helper.h"

namespace mooncake {
namespace ha {

namespace {

constexpr auto kLabelReconcileRetryInterval = std::chrono::seconds(1);
constexpr auto kSupervisorRetryInterval = std::chrono::seconds(1);
constexpr char kLeaderLabelKey[] = "mooncake.io/store-role";
constexpr char kLeaderLabelValue[] = "leader";

bool HasPodIdentity(const MasterServiceSupervisorConfig& config) {
    return !config.pod_name.empty() && !config.pod_namespace.empty() &&
           config.ha_backend_type == "k8s";
}

LeaderLabelReconciler MakeLeaderLabelReconciler(
    const MasterServiceSupervisorConfig& config) {
    return LeaderLabelReconciler(
        HasPodIdentity(config),
        [ns = config.pod_namespace, pod = config.pod_name](bool desired) {
            return desired ? K8sLeaseHelper::SetPodLabel(
                                 ns, pod, kLeaderLabelKey, kLeaderLabelValue)
                           : K8sLeaseHelper::ClearPodLabel(ns, pod,
                                                           kLeaderLabelKey);
        },
        kLabelReconcileRetryInterval);
}

std::string ResolveHABackendConnstring(
    const MasterServiceSupervisorConfig& config) {
    return ResolveConfiguredHABackendConnstring(config.ha_backend_type,
                                                config.ha_backend_connstring,
                                                config.etcd_endpoints);
}

tl::expected<HABackendSpec, ErrorCode> BuildHABackendSpec(
    const MasterServiceSupervisorConfig& config) {
    auto backend_type = ParseHABackendType(config.ha_backend_type);
    if (!backend_type.has_value()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto availability = ValidateHABackendAvailability(backend_type.value());
    if (availability != ErrorCode::OK) {
        return tl::make_unexpected(availability);
    }

    auto connstring = ResolveHABackendConnstring(config);
    if (connstring.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    return HABackendSpec{
        .type = backend_type.value(),
        .connstring = connstring,
        .cluster_namespace = config.cluster_id,
        .master_view_lease_ttl_sec = config.master_view_lease_ttl_sec,
    };
}

bool IsFatalHABackendError(ErrorCode err) {
    return err == ErrorCode::INVALID_PARAMS ||
           err == ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
}

bool HandleSupervisorError(std::string_view action, ErrorCode err,
                           HABackendType backend_type) {
    if (IsFatalHABackendError(err)) {
        LOG(ERROR) << "Failed to " << action << ": " << toString(err)
                   << ", backend_type=" << HABackendTypeToString(backend_type);
        return true;
    }

    LOG(WARNING) << "Failed to " << action << ": " << toString(err)
                 << ", backend_type=" << HABackendTypeToString(backend_type)
                 << ", retrying in " << kSupervisorRetryInterval.count() << "s";
    std::this_thread::sleep_for(kSupervisorRetryInterval);
    return false;
}

void SetRuntimeState(MasterAdminServer& admin_server,
                     MasterRuntimeState state) {
    admin_server.SetRuntimeState(state);
    LOG(INFO) << "Master runtime state -> " << MasterRuntimeStateToString(state)
              << ", role=" << MasterRuntimeRoleToString(state);
}

void ActivateServingState(MasterAdminServer& admin_server,
                          const std::shared_ptr<WrappedMasterService>& service,
                          LeaderLabelReconciler& label_reconciler) {
    admin_server.SetServiceDelegate(service);
    admin_server.SetServiceAvailable(true);
    SetRuntimeState(admin_server, MasterRuntimeState::kServing);
    label_reconciler.SetLeader(true);
}

void DeactivateServingState(MasterAdminServer& admin_server,
                            LeaderLabelReconciler& label_reconciler) {
    admin_server.SetServiceAvailable(false);
    admin_server.SetServiceDelegate(nullptr);
    label_reconciler.SetLeader(false);
}

void UpdateObservedLeader(MasterAdminServer& admin_server,
                          StandbyController& standby_controller,
                          const std::optional<MasterView>& leader_view,
                          const MasterSources& sources) {
    admin_server.SetObservedLeader(leader_view);
    standby_controller.UpdateObservedLeader(sources);
}

// Forward declarations: definitions live below the CVM membership bridge.
std::optional<MasterView> BuildCvmObservedLeader(
    const std::unique_ptr<cvm::CvmController>& cvm_controller);
MasterSources BuildCvmObservedSources(
    const std::unique_ptr<cvm::CvmController>& cvm_controller);

void EnterStandbyMode(
    MasterAdminServer& admin_server,
    StandbyController& standby_controller,
    std::atomic<bool>& accept_runtime_updates,
    const std::unique_ptr<cvm::CvmController>& cvm_controller) {
    accept_runtime_updates.store(true, std::memory_order_release);
    const MasterSources sources = BuildCvmObservedSources(cvm_controller);
    UpdateObservedLeader(admin_server, standby_controller,
                         BuildCvmObservedLeader(cvm_controller), sources);

    auto err = standby_controller.StartStandby(sources);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "Failed to start standby replication: "
                     << toString(err);
        SetRuntimeState(admin_server, MasterRuntimeState::kStandby);
        return;
    }

    SetRuntimeState(admin_server, standby_controller.GetStandbyRuntimeState());
}

// Bridges CvmController's membership role decisions to the supervisor's
// serving/standby state machine. CvmController::MembershipLoop runs on its own
// thread and calls OnRoleChanged; this class persists the role back to etcd
// (so slot partitioning immediately excludes demoted nodes) and publishes the
// change to the supervisor main loop. It never mutates the supervisor state
// machine directly: on demotion it only invokes a lightweight stop-server
// signal installed by the serve phase, and the main loop performs the full
// downgrade sequence serially after the server unblocks.
class CvmMembershipCoordinator : public cvm::CvmServiceDelegate {
   public:
    CvmMembershipCoordinator(std::string cluster_namespace,
                             std::string master_id)
        : cluster_namespace_(std::move(cluster_namespace)),
          master_id_(std::move(master_id)) {}

    void SetLeaseId(EtcdLeaseId lease_id) {
        lease_id_.store(lease_id, std::memory_order_relaxed);
    }

    // Installed by the serve phase; cleared (empty) when not serving. The
    // signal only stops the coro_rpc server so the serve phase unblocks; the
    // main loop then reads CvmController::GetCurrentRole() and performs the
    // full downgrade sequence.
    void SetServeStopSignal(std::function<void()> signal) {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_serve_signal_ = std::move(signal);
    }

    // Blocks until either a role change or a kv-view change is pending, then
    // clears both so each change is consumed exactly once. The caller re-reads
    // CvmController::GetCurrentRole() as the source of truth after waking; a
    // standby uses the wake-up to re-bind its replay sources.
    void WaitForRoleOrViewChange() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] {
            return has_pending_role_ || has_pending_view_;
        });
        has_pending_role_ = false;
        has_pending_view_ = false;
    }

    void OnSlotAcquired(uint16_t /*slot*/) override {}
    void OnSlotReleased(uint16_t /*slot*/) override {}

    void OnKvViewChanged() override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            has_pending_view_ = true;
        }
        cv_.notify_all();
    }

    void OnRoleChanged(cvm::MasterRole new_role) override {
        const EtcdLeaseId lease_id =
            lease_id_.load(std::memory_order_relaxed);
        ErrorCode err = cvm::EtcdViewStore::UpdateMasterRole(
            cluster_namespace_, master_id_, new_role, lease_id);
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "CvmMembershipCoordinator: failed to persist role="
                         << static_cast<int32_t>(new_role)
                         << " for master_id=" << master_id_ << ": " << err;
        }
        LOG(INFO) << "CvmMembershipCoordinator::OnRoleChanged: master_id="
                  << master_id_ << ", role=" << static_cast<int32_t>(new_role)
                  << ", lease_id=" << lease_id;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            has_pending_role_ = true;
        }
        cv_.notify_all();

        if (new_role == cvm::MasterRole::kStandby) {
            std::function<void()> signal;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                signal = stop_serve_signal_;
            }
            if (signal) {
                signal();
            }
        }
    }

   private:
    std::string cluster_namespace_;
    std::string master_id_;
    std::atomic<EtcdLeaseId> lease_id_{0};
    std::mutex mutex_;
    std::condition_variable cv_;
    bool has_pending_role_{false};
    bool has_pending_view_{false};
    std::function<void()> stop_serve_signal_;
};

// Builds the representative single-leader view for the admin surface. In the
// multi-submaster model there is no single "leader"; this reports the earliest
// primary for display only. Returns nullopt when no primary is available yet
// (or this node is the first primary itself, so there is no upstream).
std::optional<MasterView> BuildCvmObservedLeader(
    const std::unique_ptr<cvm::CvmController>& cvm_controller) {
    if (!cvm_controller) {
        return std::nullopt;
    }
    const std::string primary = cvm_controller->GetPrimaryAddress();
    if (primary.empty()) {
        return std::nullopt;
    }
    MasterView view;
    view.leader_address = primary;
    view.view_version = cvm_controller->GetKvViewVersion();
    return view;
}

// Builds the standby replay source set from the CVM membership ranking. Under
// dynamic binding (2c) a standby only follows the primary(s) that own the slot
// range it is responsible for, instead of following every primary (2b).
MasterSources BuildCvmObservedSources(
    const std::unique_ptr<cvm::CvmController>& cvm_controller) {
    MasterSources sources;
    if (!cvm_controller) {
        return sources;
    }
    for (const auto& member : cvm_controller->GetBindingSources()) {
        sources.push_back(MasterSource{member.master_id, member.address});
    }
    return sources;
}

int RunSupervisorLoop(const HABackendSpec& spec,
                      const MasterServiceSupervisorConfig& config,
                      MasterAdminServer& admin_server) {
    // ── Metrics reporter (writes master capacity/usage to HA backend) ──
    MasterMetricsReporter::Config reporter_config;
    reporter_config.enabled = config.enable_metrics_report_to_backend;
    reporter_config.report_interval_sec = config.metrics_report_interval_sec;
    reporter_config.lease_ttl_sec = config.metrics_report_lease_ttl_sec;
    reporter_config.master_id = UuidToString(generate_uuid());
    reporter_config.local_hostname = config.local_hostname;
    reporter_config.cluster_namespace = config.cluster_id;
    reporter_config.ha_backend_connstring =
        ResolveHABackendConnstring(config);
    MasterMetricsReporter metrics_reporter(reporter_config);

    auto label_reconciler = MakeLeaderLabelReconciler(config);
    label_reconciler.SetLeader(false);
    SetRuntimeState(admin_server, MasterRuntimeState::kStarting);
    auto standby_controller = CreateStandbyController(spec, config);
    std::atomic<bool> accept_standby_runtime_updates{false};
    standby_controller->SetStandbyRuntimeStateCallback(
        [&](MasterRuntimeState state) {
            if (!accept_standby_runtime_updates.load(
                    std::memory_order_acquire)) {
                return;
            }
            SetRuntimeState(admin_server, state);
        });

    // ── CVM membership (quota coordination) ──
    // The CvmController owns the etcd lease, master registration, view
    // snapshot aggregation and the membership loop that decides this node's
    // role (primary vs standby) via first-come-first-served ranking. It lives
    // here (supervisor layer) so membership keeps running whether or not this
    // node is currently serving; MasterService only publishes slot ownership.
    // NOTE: the coordinator is declared before the controller so it is
    // destroyed after it — the controller's Stop() joins the membership thread,
    // guaranteeing no OnRoleChanged callback is in flight when the coordinator
    // is torn down.
    CvmMembershipCoordinator cvm_membership_coordinator(config.cluster_id,
                                                        config.local_hostname);
    std::unique_ptr<cvm::CvmController> cvm_controller;
    if (spec.type == HABackendType::ETCD && !config.local_hostname.empty() &&
        !config.cluster_id.empty()) {
        ErrorCode connect_err = EtcdHelper::ConnectToEtcdStoreClient(
            ResolveHABackendConnstring(config));
        if (connect_err != ErrorCode::OK) {
            LOG(WARNING) << "CVM membership disabled: failed to connect etcd: "
                         << connect_err;
        } else {
            cvm::CvmController::Config cc_config;
            cc_config.cluster_namespace = config.cluster_id;
            cc_config.master_id = config.local_hostname;
            cc_config.address = config.local_hostname;
            // Start as standby; the membership loop promotes to primary when
            // this node ranks within the submaster quota.
            cc_config.role = cvm::MasterRole::kStandby;
            cc_config.http_port = config.cvm_http_port;
            cc_config.http_host = config.cvm_http_host;
            cc_config.submaster_count = config.submaster_count;
            cvm_controller =
                std::make_unique<cvm::CvmController>(std::move(cc_config));
            cvm_controller->SetDelegate(&cvm_membership_coordinator);
            ErrorCode cc_err = cvm_controller->Start();
            if (cc_err != ErrorCode::OK) {
                LOG(WARNING) << "Failed to start CvmController: " << cc_err;
                cvm_controller.reset();
            } else {
                cvm_membership_coordinator.SetLeaseId(
                    cvm_controller->GetLeaseId());
                LOG(INFO) << "Started supervisor-owned CvmController: master_id="
                          << config.local_hostname
                          << ", cluster_namespace=" << config.cluster_id
                          << ", lease_id=" << cvm_controller->GetLeaseId()
                          << ", submaster_count=" << config.submaster_count;
            }
        }
    }

    EnterStandbyMode(admin_server, *standby_controller,
                     accept_standby_runtime_updates,
                     cvm_controller);

    while (true) {
        // CVM membership decides this node's role via first-come-first-served
        // ranking against the submaster quota. Without a CvmController
        // (non-etcd backend or missing identity) there is no quota
        // coordination, so fall back to unconditional primary serving.
        const cvm::MasterRole target_role =
            cvm_controller ? cvm_controller->GetCurrentRole()
                           : cvm::MasterRole::kPrimary;

        if (target_role == cvm::MasterRole::kPrimary) {
            // ── Upgrade sequence (kStandby → kPrimary) ──
            // first primary（无上游）无需 final catch-up；有上游 primary 时才
            // 从 standby 导出回放数据。PromoteStandbyAndExport 内部完成 final
            // catch-up + 导出 PromotionContext，新 primary 从它恢复。
            accept_standby_runtime_updates.store(false,
                                                 std::memory_order_release);
            const bool has_upstream =
                cvm_controller && !cvm_controller->GetPrimaryAddress().empty();
            PromotionContext promotion_ctx{};
            if (has_upstream) {
                auto ctx = standby_controller->PromoteStandbyAndExport();
                if (!ctx) {
                    EnterStandbyMode(admin_server, *standby_controller,
                                     accept_standby_runtime_updates,
                                     cvm_controller);
                    if (HandleSupervisorError("promote standby for serve",
                                              ctx.error(), spec.type)) {
                        return -1;
                    }
                    continue;
                }
                promotion_ctx = std::move(*ctx);
            }

            LOG(INFO) << "Starting serve phase (CVM role=kPrimary)...";
            coro_rpc::coro_rpc_server server(
                config.rpc_thread_num, config.rpc_port, config.rpc_address,
                config.rpc_conn_timeout, config.rpc_enable_tcp_no_delay);
            const char* protocol = std::getenv("MC_RPC_PROTOCOL");
            if (protocol && std::string_view(protocol) == "rdma") {
                server.init_ibv();
            }

            const ViewVersionId view_version =
                cvm_controller ? cvm_controller->GetKvViewVersion() : 0;
            mooncake::WrappedMasterServiceConfig wrapped_config(config,
                                                                 view_version);
            // In HA serving-primary mode, snapshot bootstrap belongs to
            // standby. The new primary must restore from PromotionContext only.
            wrapped_config.enable_snapshot_restore = false;
            auto wrapped_master_service =
                std::make_shared<WrappedMasterService>(
                    wrapped_config, config.http_metadata_server,
                    config.http_metadata_remote_url);

            // Inject the supervisor-owned CvmController lease so the
            // slot/segment ownership records share this master's registration
            // lifecycle.
            if (cvm_controller) {
                wrapped_master_service->SetCvmLeaseId(
                    cvm_controller->GetLeaseId());
            }

            // Restore from standby if we have context.
            if (promotion_ctx.applied_seq_id > 0 ||
                !promotion_ctx.objects.empty() ||
                !promotion_ctx.segments.empty()) {
                wrapped_master_service->RestoreFromStandby(
                    promotion_ctx.objects, promotion_ctx.applied_seq_id,
                    promotion_ctx.segments);
            }

            mooncake::RegisterRpcService(server, *wrapped_master_service);

            async_simple::Future<coro_rpc::err_code> ec = server.async_start();
            if (ec.hasResult()) {
                LOG(ERROR) << "Failed to start master service: "
                           << ec.result().value();
                DeactivateServingState(admin_server, label_reconciler);
                EnterStandbyMode(admin_server, *standby_controller,
                                 accept_standby_runtime_updates,
                                 cvm_controller);
                return -1;
            }

            // Lightweight demotion signal: only stops the coro_rpc server so
            // the blocking get() below unblocks; the main loop then performs
            // the full downgrade sequence serially.
            cvm_membership_coordinator.SetServeStopSignal([&server]() {
                LOG(INFO) << "CVM quota demotion: stopping server";
                server.stop();
            });

            // A demotion may have been decided while the server was starting
            // up (before the stop signal was installed). If our role is no
            // longer primary, stop the server now so the get() below returns
            // promptly instead of blocking forever.
            const bool demoted_during_startup =
                cvm_controller &&
                cvm_controller->GetCurrentRole() == cvm::MasterRole::kStandby;

            if (!demoted_during_startup) {
                ActivateServingState(admin_server, wrapped_master_service,
                                     label_reconciler);
                metrics_reporter.SetRole("primary");
                metrics_reporter.Start();
                if (wrapped_master_service->StartSlotOwnerHeartbeat() !=
                    ErrorCode::OK) {
                    LOG(WARNING) << "Failed to start SlotOwnerHeartbeat during "
                                    "serve phase";
                }
            } else {
                LOG(INFO) << "Demoted during serve startup; stopping server";
                server.stop();
            }

            auto server_err = std::move(ec).get();
            LOG(INFO) << "Master service stopped: " << server_err;

            // ── Downgrade sequence (kPrimary → kStandby) ──
            metrics_reporter.Stop();
            metrics_reporter.SetRole("standby");
            wrapped_master_service->StopSlotOwnerHeartbeat();
            cvm_membership_coordinator.SetServeStopSignal(nullptr);
            DeactivateServingState(admin_server, label_reconciler);
            EnterStandbyMode(admin_server, *standby_controller,
                             accept_standby_runtime_updates,
                             cvm_controller);
        } else {
            // ── Standby (kStandby): keep replicating, wait for promotion ──
            EnterStandbyMode(admin_server, *standby_controller,
                             accept_standby_runtime_updates,
                             cvm_controller);
            if (cvm_controller) {
                cvm_membership_coordinator.WaitForRoleOrViewChange();
            } else {
                // Unreachable: target_role is kPrimary when cvm_controller is
                // null. Sleep defensively to avoid a tight loop.
                std::this_thread::sleep_for(kSupervisorRetryInterval);
            }
        }
    }

    return 0;
}

}  // namespace

MasterServiceSupervisor::MasterServiceSupervisor(
    const MasterServiceSupervisorConfig& config)
    : config_(config) {}

int MasterServiceSupervisor::Start() {
    auto spec = BuildHABackendSpec(config_);
    if (!spec) {
        LOG(ERROR) << "Failed to parse HA backend config: "
                   << toString(spec.error())
                   << ", backend_type=" << config_.ha_backend_type;
        return -1;
    }

    mooncake::MasterAdminServer admin_server(
        static_cast<uint16_t>(config_.metrics_port),
        config_.enable_metric_reporting);
    if (!admin_server.Start()) {
        LOG(ERROR) << "Failed to start master admin server, metrics_port="
                   << config_.metrics_port;
        return -1;
    }
    return RunSupervisorLoop(*spec, config_, admin_server);
}

}  // namespace ha
}  // namespace mooncake
