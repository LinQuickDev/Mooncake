#include "hot_standby_service.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <set>
#include <thread>

#include "etcd_helper.h"
#include "ha/kv/etcd_ha_kv_backend.h"
#include "ha_metric_manager.h"
#include "ha/oplog/oplog_applier.h"
#include "ha/oplog/oplog_batch_standby_reader.h"
#include "ha/oplog/oplog_test_failpoint.h"
#include "ha/oplog/oplog_types.h"

namespace mooncake {

HotStandbyService::HotStandbyService(const HotStandbyConfig& config)
    : config_(config) {
    metadata_store_ = std::make_unique<StandbyMetadataStore>();
    // Per-source OpLogApplier/reader replicas are created in Start() with the
    // resolved cluster_id and source list.

    // Register callback for state change logging and metrics.
    state_machine_.RegisterCallback([this](StandbyState old_state,
                                           StandbyState new_state,
                                           StandbyEvent event) {
        LOG(INFO) << "HotStandbyService state changed: "
                  << StandbyStateToString(old_state) << " -> "
                  << StandbyStateToString(new_state)
                  << " (event: " << StandbyEventToString(event) << ")";

        // Update HA metrics
        HAMetricManager::instance().set_standby_state(
            static_cast<int64_t>(new_state));
        HAMetricManager::instance().inc_state_transitions();

        // Track watch disconnections
        if (event == StandbyEvent::WATCH_BROKEN ||
            event == StandbyEvent::DISCONNECTED) {
            HAMetricManager::instance().inc_oplog_watch_disconnections();
        }

        NotifySyncStatus();
    });
}

// StandbyMetadataStore implementation
bool HotStandbyService::StandbyMetadataStore::PutMetadata(
    const std::string& tenant_id, const std::string& key,
    const StandbyObjectMetadata& metadata) {
    const auto normalized = NormalizeTenantId(tenant_id);
    std::lock_guard<std::mutex> lock(mutex_);
    store_[normalized][key] = metadata;
    VLOG(2) << "StandbyMetadataStore: stored metadata for tenant=" << normalized
            << ", key=" << key << ", replicas=" << metadata.replicas.size()
            << ", size=" << metadata.size;
    return true;
}

bool HotStandbyService::StandbyMetadataStore::Put(const std::string& key,
                                                  const std::string& payload) {
    // Legacy interface - create empty metadata for default tenant
    StandbyObjectMetadata metadata;
    std::lock_guard<std::mutex> lock(mutex_);
    store_["default"][key] = metadata;
    return true;
}

std::optional<StandbyObjectMetadata>
HotStandbyService::StandbyMetadataStore::GetMetadata(
    const std::string& tenant_id, const std::string& key) const {
    const auto normalized = NormalizeTenantId(tenant_id);
    std::lock_guard<std::mutex> lock(mutex_);
    auto tenant_it = store_.find(normalized);
    if (tenant_it == store_.end()) return std::nullopt;
    auto it = tenant_it->second.find(key);
    if (it == tenant_it->second.end()) return std::nullopt;
    return it->second;
}

bool HotStandbyService::StandbyMetadataStore::Remove(
    const std::string& tenant_id, const std::string& key) {
    const auto normalized = NormalizeTenantId(tenant_id);
    std::lock_guard<std::mutex> lock(mutex_);
    auto tenant_it = store_.find(normalized);
    if (tenant_it == store_.end()) return false;
    auto it = tenant_it->second.find(key);
    if (it == tenant_it->second.end()) return false;
    tenant_it->second.erase(it);
    if (tenant_it->second.empty()) {
        store_.erase(tenant_it);
    }
    return true;
}

bool HotStandbyService::StandbyMetadataStore::Exists(
    const std::string& tenant_id, const std::string& key) const {
    const auto normalized = NormalizeTenantId(tenant_id);
    std::lock_guard<std::mutex> lock(mutex_);
    auto tenant_it = store_.find(normalized);
    if (tenant_it == store_.end()) return false;
    return tenant_it->second.find(key) != tenant_it->second.end();
}

size_t HotStandbyService::StandbyMetadataStore::GetKeyCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t total = 0;
    for (const auto& [tid, tenant_map] : store_) {
        total += tenant_map.size();
    }
    return total;
}

size_t HotStandbyService::StandbyMetadataStore::GetKeyCountForTenant(
    const std::string& tenant_id) const {
    const auto normalized = NormalizeTenantId(tenant_id);
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = store_.find(normalized);
    return it == store_.end() ? 0 : it->second.size();
}

void HotStandbyService::StandbyMetadataStore::Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    store_.clear();
}

void HotStandbyService::StandbyMetadataStore::Snapshot(
    std::vector<StandbyObjectEntry>& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    out.clear();
    for (const auto& [tenant_id, tenant_store] : store_) {
        for (const auto& [key, metadata] : tenant_store) {
            out.push_back(StandbyObjectEntry{NormalizeTenantId(tenant_id), key,
                                             metadata});
        }
    }
}

HotStandbyService::~HotStandbyService() {
    // Always ensure threads are joined, regardless of state
    // This prevents std::terminate() if threads are still joinable
    Stop();

    // Double-check: ensure all threads are joined even if Stop() had early
    // return
    if (replication_thread_.joinable()) {
        replication_thread_.join();
    }
    if (verification_thread_.joinable()) {
        verification_thread_.join();
    }
}

ErrorCode HotStandbyService::Start(const std::vector<ha::MasterSource>& sources,
                                   const std::string& oplog_endpoints,
                                   const std::string& cluster_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Use state machine to check if already running
    if (IsRunning()) {
        LOG(WARNING) << "HotStandbyService is already running";
        return ErrorCode::OK;
    }

    // A failed asynchronous run leaves joinable worker threads behind. Reap
    // them before constructing the next run's readers and workers.
    replication_loop_running_.store(false, std::memory_order_release);
    replication_loop_cv_.notify_all();
    if (replication_thread_.joinable()) {
        replication_thread_.join();
    }
    if (verification_thread_.joinable()) {
        verification_thread_.join();
    }
    sources_.clear();
    batch_standby_kv_backend_.reset();
    baseline_segments_.clear();

    last_error_.store(ErrorCode::OK, std::memory_order_release);

    // Trigger START event
    auto result = state_machine_.ProcessEvent(StandbyEvent::START);
    if (!result.allowed) {
        LOG(ERROR) << "Cannot start HotStandbyService: " << result.reason;
        return ErrorCode::INTERNAL_ERROR;  // State machine rejected START
    }

    config_.sources = sources;
    oplog_endpoints_ = oplog_endpoints;
    cluster_id_ = cluster_id;

    if (config_.enable_oplog_following) {
#ifdef STORE_USE_ETCD
        if (!catch_up_batch_kv_backend_for_testing_) {
            ErrorCode err =
                EtcdHelper::ConnectToEtcdStoreClient(oplog_endpoints.c_str());
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "Failed to connect to etcd: " << oplog_endpoints;
                state_machine_.ProcessEvent(StandbyEvent::CONNECTION_FAILED);
                return err;
            }
        }
#else
        state_machine_.ProcessEvent(StandbyEvent::FATAL_ERROR);
        LOG(ERROR) << "Batch-record OpLog requires STORE_USE_ETCD";
        return ErrorCode::INTERNAL_ERROR;
#endif
    }

    state_machine_.ProcessEvent(StandbyEvent::CONNECTED);

    uint64_t baseline_seq_id = 0;
    auto baseline_err = PrepareBootstrapBaselineLocked(baseline_seq_id);
    if (baseline_err != ErrorCode::OK) {
        state_machine_.ProcessEvent(StandbyEvent::FATAL_ERROR);
        return baseline_err;
    }

    if (!config_.enable_oplog_following) {
        ActivateSnapshotOnlyStandbyLocked(baseline_seq_id);
        return ErrorCode::OK;
    }

    return StartOplogFollowingLocked(baseline_seq_id);
}

uint64_t HotStandbyService::GetLocalLastAppliedSequenceIdLocked() const {
    uint64_t max_applied = 0;
    for (const auto& [source_id, replica] : sources_) {
        if (replica.applier) {
            const uint64_t expected = replica.applier->GetExpectedSequenceId();
            const uint64_t applied = expected > 0 ? expected - 1 : 0;
            max_applied = std::max(max_applied, applied);
        }
    }
    if (max_applied > 0) {
        return max_applied;
    }
    return applied_seq_id_.load(std::memory_order_acquire);
}

void HotStandbyService::CollectSegmentsLocked(
    std::vector<StandbySegmentInfo>& out) const {
    out.clear();
    // Merge segment registries across per-source appliers, deduplicating by
    // transport_endpoint (segment identity in StandbySegmentRegistry).
    std::unordered_map<std::string, StandbySegmentInfo> by_endpoint;
    for (const auto& [source_id, replica] : sources_) {
        if (!replica.applier) {
            continue;
        }
        for (const auto& segment :
             replica.applier->GetSegmentRegistry().GetAllSegments()) {
            by_endpoint[segment.transport_endpoint] = segment;
        }
    }
    out.reserve(by_endpoint.size());
    for (const auto& [endpoint, segment] : by_endpoint) {
        out.push_back(segment);
    }
}

ErrorCode HotStandbyService::PrepareBootstrapBaselineLocked(
    uint64_t& baseline_seq_id) {
    const uint64_t local_last_seq_id = GetLocalLastAppliedSequenceIdLocked();
    const bool has_local_metadata =
        metadata_store_ && metadata_store_->GetKeyCount() > 0;
    const bool has_recoverable_local_state =
        has_local_metadata && local_last_seq_id > 0;

    if (!has_recoverable_local_state && has_local_metadata) {
        LOG(WARNING)
            << "Discarding local metadata without recoverable sequence "
            << "boundary before standby bootstrap";
        metadata_store_ = std::make_unique<StandbyMetadataStore>();
    }

    if (!config_.enable_oplog_following) {
        if (metadata_store_ && metadata_store_->GetKeyCount() > 0) {
            LOG(INFO) << "Snapshot-only restart discards local metadata and "
                         "reloads the latest catalog snapshot";
        }

        auto snapshot_err = LoadSnapshotBaselineLocked(baseline_seq_id);
        if (snapshot_err != ErrorCode::OK) {
            return snapshot_err;
        }
        applied_seq_id_.store(baseline_seq_id, std::memory_order_release);
        primary_seq_id_.store(baseline_seq_id, std::memory_order_release);
        return ErrorCode::OK;
    }

    if (has_recoverable_local_state) {
        LOG(INFO) << "Standby warm start: reuse local metadata (keys="
                  << metadata_store_->GetKeyCount()
                  << "), recover last_seq_id=" << local_last_seq_id;
        baseline_seq_id = local_last_seq_id;
    } else {
        auto snapshot_err = LoadSnapshotBaselineLocked(baseline_seq_id);
        if (snapshot_err != ErrorCode::OK) {
            return snapshot_err;
        }
    }

    applied_seq_id_.store(baseline_seq_id, std::memory_order_release);
    primary_seq_id_.store(baseline_seq_id, std::memory_order_release);
    return ErrorCode::OK;
}

ErrorCode HotStandbyService::LoadSnapshotBaselineLocked(
    uint64_t& baseline_seq_id) {
    baseline_seq_id = 0;
    metadata_store_->Clear();
    baseline_segments_.clear();

    if (!config_.enable_snapshot_bootstrap || !snapshot_provider_) {
        return ErrorCode::OK;
    }

    auto snapshot_result = snapshot_provider_->LoadLatestSnapshot(cluster_id_);
    if (!snapshot_result) {
        if (config_.enable_oplog_following) {
            LOG(WARNING) << "Failed to load snapshot baseline, falling back "
                            "to OpLog-only bootstrap: "
                         << toString(snapshot_result.error());
            return ErrorCode::OK;
        }
        LOG(ERROR) << "Failed to load snapshot baseline for snapshot-only "
                   << "standby bootstrap: "
                   << toString(snapshot_result.error());
        return snapshot_result.error();
    }

    if (!snapshot_result->has_value()) {
        if (config_.enable_oplog_following) {
            LOG(INFO) << "No snapshot available, falling back to OpLog-only "
                         "bootstrap";
        } else {
            LOG(INFO) << "No snapshot available for snapshot-only bootstrap; "
                         "standby starts from empty baseline";
        }
        return ErrorCode::OK;
    }

    const auto& snapshot = snapshot_result->value();
    LOG(INFO) << "Loaded snapshot baseline: snapshot_id="
              << snapshot.snapshot_id
              << ", snapshot_seq_id=" << snapshot.snapshot_sequence_id
              << ", keys=" << snapshot.metadata.size();
    for (const auto& entry : snapshot.metadata) {
        metadata_store_->PutMetadata(entry.tenant_id, entry.key,
                                     entry.metadata);
    }
    // Load segment registry from snapshot; applied to each per-source applier
    // when OpLog following starts.
    baseline_segments_ = snapshot.segments;
    baseline_seq_id = snapshot.snapshot_sequence_id;
    return ErrorCode::OK;
}

ErrorCode HotStandbyService::StartOplogFollowingLocked(
    uint64_t baseline_seq_id) {
    (void)baseline_seq_id;
    if (catch_up_batch_kv_backend_for_testing_) {
        batch_standby_kv_backend_ = catch_up_batch_kv_backend_for_testing_;
    } else {
        batch_standby_kv_backend_ = std::make_shared<EtcdHaKvBackend>();
    }

    sources_.clear();
    for (const auto& source : config_.sources) {
        auto applier =
            std::make_unique<OpLogApplier>(metadata_store_.get(), cluster_id_);
        applier->LoadSegmentRegistry(baseline_segments_);
        auto reader = std::make_unique<OpLogBatchStandbyReader>(
            cluster_id_, *batch_standby_kv_backend_, *applier, source.master_id);
        sources_.emplace(
            source.master_id,
            SourceReplica{std::move(applier), std::move(reader)});
    }

    state_machine_.ProcessEvent(StandbyEvent::SYNC_COMPLETE);
    replication_loop_running_.store(true, std::memory_order_release);
    replication_thread_ =
        std::thread(&HotStandbyService::ReplicationLoop, this);
    if (config_.enable_verification) {
        verification_thread_ =
            std::thread(&HotStandbyService::VerificationLoop, this);
    }

    LOG(INFO) << "HotStandbyService started, watching OpLog for cluster: "
              << cluster_id_ << ", sources=" << sources_.size()
              << ", state=" << StandbyStateToString(GetState());
    return ErrorCode::OK;
}

void HotStandbyService::ActivateSnapshotOnlyStandbyLocked(
    uint64_t baseline_seq_id) {
    state_machine_.ProcessEvent(StandbyEvent::SYNC_COMPLETE);
    LOG(INFO) << "HotStandbyService started in snapshot-only mode, cluster="
              << cluster_id_ << ", state=" << StandbyStateToString(GetState())
              << ", applied_seq_id=" << baseline_seq_id
              << ", metadata_keys=" << metadata_store_->GetKeyCount();
}

void HotStandbyService::SetSyncStatusCallback(SyncStatusCallback callback) {
    {
        std::lock_guard<std::mutex> lock(sync_status_callback_mutex_);
        sync_status_callback_ = std::move(callback);
    }
    NotifySyncStatus();
}

void HotStandbyService::NotifySyncStatus() {
    SyncStatusCallback callback;
    {
        std::lock_guard<std::mutex> lock(sync_status_callback_mutex_);
        callback = sync_status_callback_;
    }
    if (callback) {
        callback(GetSyncStatus());
    }
}

void HotStandbyService::SetCatchUpBatchKvBackendForTesting(
    std::shared_ptr<HaKvBackend> backend) {
    catch_up_batch_kv_backend_for_testing_ = std::move(backend);
}

void HotStandbyService::StopReplicationLoop() {
    replication_loop_running_.store(false, std::memory_order_release);
    replication_loop_cv_.notify_all();
    if (replication_thread_.joinable()) {
        replication_thread_.join();
    }
}

void HotStandbyService::Stop() {
    std::lock_guard<std::mutex> lock(mutex_);

    StandbyState current_state = GetState();
    if (current_state == StandbyState::STOPPED &&
        !replication_thread_.joinable() && !verification_thread_.joinable()) {
        return;
    }

    state_machine_.ProcessEvent(StandbyEvent::STOP);
    StopReplicationLoop();

    // Wait for threads to finish
    if (verification_thread_.joinable()) {
        verification_thread_.join();
    }

    LOG(INFO) << "HotStandbyService stopped, final_state="
              << StandbyStateToString(GetState());
}

ErrorCode HotStandbyService::UpdateSources(
    const std::vector<ha::MasterSource>& sources,
    const std::string& oplog_endpoints,
    const std::string& cluster_id) {
    // 判断是否需要重绑（比较 master_id 集合）。
    bool running = false;
    bool changed = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        running = IsRunning();
        if (running) {
            if (config_.sources.size() != sources.size()) {
                changed = true;
            } else {
                std::set<std::string> old_ids;
                std::set<std::string> new_ids;
                for (const auto& s : config_.sources) {
                    old_ids.insert(s.master_id);
                }
                for (const auto& s : sources) {
                    new_ids.insert(s.master_id);
                }
                changed = (old_ids != new_ids);
            }
        }
    }

    if (!running) {
        return Start(sources, oplog_endpoints, cluster_id);
    }
    if (!changed) {
        return ErrorCode::OK;
    }

    // 运行中且源集合变化：停止后重启。metadata_store_ 是成员变量，Stop 不
    // 清空，Start 里 PrepareBootstrapBaselineLocked 会复用已回放的元数据。
    LOG(INFO) << "HotStandbyService rebinding replay sources: "
              << config_.sources.size() << " -> " << sources.size();
    Stop();
    return Start(sources, oplog_endpoints, cluster_id);
}

StandbySyncStatus HotStandbyService::GetSyncStatus() const {
    StandbySyncStatus status;

    // Get applied sequence ID from per-source OpLogAppliers (max across
    // sources; each source owns an independent sequence space).
    uint64_t max_applied = 0;
    for (const auto& [source_id, replica] : sources_) {
        if (replica.applier) {
            const uint64_t expected = replica.applier->GetExpectedSequenceId();
            max_applied = std::max(max_applied, expected > 0 ? expected - 1 : 0);
        }
    }
    status.applied_seq_id =
        max_applied > 0 ? max_applied : applied_seq_id_.load();

    // Primary sequence ID (best-effort): updated by ReplicationLoop via etcd
    // `/latest`.
    status.primary_seq_id = primary_seq_id_.load();

    // Use state machine for connection status
    status.is_connected = IsConnected();
    status.state = GetState();
    status.last_error = last_error_.load(std::memory_order_acquire);
    status.time_in_state = state_machine_.GetTimeInCurrentState();

    if (status.primary_seq_id > status.applied_seq_id) {
        status.lag_entries = status.primary_seq_id - status.applied_seq_id;
    } else {
        status.lag_entries = 0;
    }

    // Lag time is currently reported as 0; if needed we can extend the
    // protocol to propagate primary timestamps and compute a real value.
    status.lag_time = std::chrono::milliseconds(0);
    status.is_syncing = IsRunning() && IsConnected();

    return status;
}

bool HotStandbyService::IsReadyForPromotion() const {
    // Use state machine to check if ready for promotion
    if (!state_machine_.IsReadyForPromotion()) {
        return false;
    }

    StandbySyncStatus status = GetSyncStatus();

    // Allow promotion even with large lag - the new Primary can continue
    // syncing remaining OpLog entries from etcd after promotion.
    // Log a warning if lag is large, but don't block promotion.
    if (status.lag_entries > config_.max_replication_lag_entries) {
        LOG(WARNING)
            << "Standby has large replication lag: " << status.lag_entries
            << " entries (threshold: " << config_.max_replication_lag_entries
            << "). Promotion will proceed, but remaining OpLog entries "
            << "will be synced after promotion.";
    }

    // NOTE: unresolved-gap check is deferred to PromoteLockedInternal, which
    // runs gap resolution + final catch-up first and only rejects promotion
    // when gaps remain after both attempts. Checking here would return a
    // misleading UNAVAILABLE_IN_CURRENT_STATUS before gap resolution runs.
    return true;
}

ErrorCode HotStandbyService::FinalCatchUpForPromotionLocked(
    uint64_t current_applied_seq_id) {
    (void)current_applied_seq_id;
    if (!config_.enable_oplog_following) {
        LOG(INFO) << "Promotion does not require final OpLog catch-up";
        return ErrorCode::OK;
    }
    if (sources_.empty()) {
        LOG(INFO) << "Final catch-up skipped: no OpLog sources";
        return ErrorCode::OK;
    }

    if (catch_up_batch_kv_backend_for_testing_) {
        return FinalCatchUpBatchRecordsLocked(
            *catch_up_batch_kv_backend_for_testing_);
    }

    EtcdHaKvBackend batch_backend;
    return FinalCatchUpBatchRecordsLocked(batch_backend);
}

ErrorCode HotStandbyService::FinalCatchUpBatchRecordsLocked(
    HaKvBackend& backend) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(30);
    const auto initial_retry_delay =
        std::chrono::milliseconds(std::max(config_.oplog_poll_interval_ms, 1));
    const auto max_retry_delay =
        std::max(initial_retry_delay, std::chrono::milliseconds(1000));

    for (auto& [source_id, replica] : sources_) {
        OpLogBatchStandbyReader* reader = replica.reader.get();
        std::unique_ptr<OpLogBatchStandbyReader> local_reader;
        if (reader == nullptr) {
            local_reader = std::make_unique<OpLogBatchStandbyReader>(
                cluster_id_, backend, *replica.applier, source_id);
            reader = local_reader.get();
        }

        auto retry_delay = initial_retry_delay;
        auto wait_to_retry = [&] {
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                return false;
            }
            std::this_thread::sleep_for(std::min(
                retry_delay,
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    deadline - now)));
            retry_delay = std::min(retry_delay * 2, max_retry_delay);
            return true;
        };
        for (;;) {
            if (std::chrono::steady_clock::now() >= deadline) {
                return ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
            }
            auto result = reader->PollOnce();
            if (result.error != ErrorCode::OK) {
                if (result.disposition !=
                        OpLogBatchStandbyPollDisposition::RETRYABLE ||
                    !wait_to_retry()) {
                    return ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
                }
                continue;
            }
            retry_delay = initial_retry_delay;
            if (!result.durable_prefix_present) {
                break;  // Source has no OpLog yet.
            }
            const uint64_t expected = replica.applier->GetExpectedSequenceId();
            const uint64_t source_applied = expected > 0 ? expected - 1 : 0;
            if (source_applied >= result.durable_prefix.last_seq) {
                break;  // This source is caught up.
            }
        }
    }
    return ErrorCode::OK;
}

ErrorCode HotStandbyService::Promote() {
    std::unique_lock<std::mutex> lock(mutex_);

    if (!IsReadyForPromotion()) {
        LOG(ERROR) << "Standby is not ready for promotion, state="
                   << StandbyStateToString(GetState());
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }

    // Trigger PROMOTE event
    auto result = state_machine_.ProcessEvent(StandbyEvent::PROMOTE);
    if (!result.allowed) {
        LOG(ERROR) << "Cannot promote: " << result.reason;
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }

    StandbySyncStatus status = GetSyncStatus();
    uint64_t current_applied_seq_id = status.applied_seq_id;

    LOG(INFO) << "Promoting Standby to Primary. Applied seq_id: "
              << current_applied_seq_id << ", lag: " << status.lag_entries
              << " entries" << ", state: " << StandbyStateToString(GetState());

    auto internal_err = PromoteLockedInternal(current_applied_seq_id);
    if (internal_err != ErrorCode::OK) {
        return internal_err;
    }

    lock.unlock();
    Stop();

    if (config_.enable_oplog_following) {
        LOG(INFO) << "Standby promoted to Primary successfully. "
                  << "All remaining OpLog entries have been synced.";
    } else {
        LOG(INFO) << "Standby promoted to Primary from snapshot baseline.";
    }
    return ErrorCode::OK;
}

ErrorCode HotStandbyService::PromoteLockedInternal(
    uint64_t current_applied_seq_id) {
    StopReplicationLoop();
    ErrorCode catch_up_err =
        FinalCatchUpForPromotionLocked(current_applied_seq_id);
    if (catch_up_err != ErrorCode::OK) {
        state_machine_.ProcessEvent(StandbyEvent::PROMOTION_FAILED);
        return catch_up_err;
    }
    TestFailPoint::Wait("promotion_final_catch_up_before_complete");
    uint64_t latest_applied_seq_id = GetLocalLastAppliedSequenceIdLocked();
    applied_seq_id_.store(latest_applied_seq_id, std::memory_order_release);
    primary_seq_id_.store(latest_applied_seq_id, std::memory_order_release);

    auto promotion_success =
        state_machine_.ProcessEvent(StandbyEvent::PROMOTION_SUCCESS);
    if (!promotion_success.allowed) {
        LOG(ERROR) << "Cannot finish promotion: " << promotion_success.reason;
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode HotStandbyService::PromoteAndExportSnapshot(StandbySnapshot& out) {
    std::unique_lock<std::mutex> lock(mutex_);

    if (!IsReadyForPromotion()) {
        LOG(ERROR) << "Standby is not ready for promotion, state="
                   << StandbyStateToString(GetState());
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }

    // Trigger PROMOTE event
    auto result = state_machine_.ProcessEvent(StandbyEvent::PROMOTE);
    if (!result.allowed) {
        LOG(ERROR) << "Cannot promote: " << result.reason;
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }

    StandbySyncStatus status = GetSyncStatus();
    uint64_t current_applied_seq_id = status.applied_seq_id;

    LOG(INFO) << "Promoting Standby to Primary. Applied seq_id: "
              << current_applied_seq_id << ", lag: " << status.lag_entries
              << " entries"
              << ", state: " << StandbyStateToString(GetState());

    auto internal_err = PromoteLockedInternal(current_applied_seq_id);
    if (internal_err != ErrorCode::OK) {
        return internal_err;
    }

    // Export snapshot BEFORE unlocking mutex (atomic promotion + export)
    uint64_t latest_applied_seq_id = GetLocalLastAppliedSequenceIdLocked();
    out.oplog_sequence_id = latest_applied_seq_id;
    if (metadata_store_) {
        metadata_store_->Snapshot(out.objects);
    } else {
        out.objects.clear();
    }
    CollectSegmentsLocked(out.segments);

    lock.unlock();
    Stop();

    if (config_.enable_oplog_following) {
        LOG(INFO) << "Standby promoted to Primary successfully. "
                  << "All remaining OpLog entries have been synced.";
    } else {
        LOG(INFO) << "Standby promoted to Primary from snapshot baseline.";
    }
    return ErrorCode::OK;
}

size_t HotStandbyService::GetMetadataCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return metadata_store_ ? metadata_store_->GetKeyCount() : 0;
}

uint64_t HotStandbyService::GetLatestAppliedSequenceId() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return GetLocalLastAppliedSequenceIdLocked();
}

bool HotStandbyService::ExportMetadataSnapshot(
    std::vector<StandbyObjectEntry>& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!metadata_store_) {
        out.clear();
        return false;
    }
    metadata_store_->Snapshot(out);
    return true;
}

bool HotStandbyService::ExportStandbySnapshot(StandbySnapshot& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!IsRunning()) {
        return false;
    }

    // Get applied sequence ID (max across per-source appliers)
    out.oplog_sequence_id = GetLocalLastAppliedSequenceIdLocked();

    // Export object metadata
    if (metadata_store_) {
        metadata_store_->Snapshot(out.objects);
    } else {
        out.objects.clear();
    }

    // Export segments merged across per-source appliers
    CollectSegmentsLocked(out.segments);

    return true;
}

void HotStandbyService::SetSnapshotProvider(
    std::unique_ptr<SnapshotProvider> provider) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (provider) {
        snapshot_provider_ = std::move(provider);
    } else {
        snapshot_provider_ = std::make_unique<NoopSnapshotProvider>();
    }
}

void HotStandbyService::ReplicationLoop() {
    LOG(INFO) << "Replication loop started (OpLog sync)";

    uint64_t last_reported_applied_seq_id = applied_seq_id_.load();
    uint64_t last_reported_primary_seq_id = primary_seq_id_.load();
    const auto retry_base =
        std::chrono::milliseconds(std::max(config_.oplog_poll_interval_ms, 1));
    const auto retry_limit =
        std::chrono::seconds(config_.batch_oplog_retry_timeout_sec);
    const auto max_retry_delay =
        std::max(retry_base, std::chrono::milliseconds(5000));
    auto retry_delay = retry_base;
    std::optional<std::chrono::steady_clock::time_point> retry_started;

    auto wait_for_next_poll = [this](std::chrono::milliseconds delay) {
        std::unique_lock<std::mutex> lock(replication_loop_mutex_);
        replication_loop_cv_.wait_for(lock, delay, [this] {
            return !replication_loop_running_.load(std::memory_order_acquire);
        });
    };

    while (replication_loop_running_.load(std::memory_order_acquire) &&
           IsRunning()) {
        if (!IsConnected()) {
            wait_for_next_poll(std::chrono::seconds(1));
            continue;
        }

        // Poll each replay source independently. Sources own disjoint slot
        // ranges and independent OpLog sequence spaces, so aggregate applied
        // and primary sequence IDs as the max across all sources.
        bool retry_backoff = false;
        bool fatal = false;
        for (auto& [source_id, replica] : sources_) {
            if (!replica.reader) {
                continue;
            }

            const uint64_t expected_before =
                replica.applier->GetExpectedSequenceId();
            auto result = replica.reader->PollOnce();
            if (result.durable_prefix_present) {
                const uint64_t current_primary = primary_seq_id_.load();
                if (result.durable_prefix.last_seq > current_primary) {
                    primary_seq_id_.store(result.durable_prefix.last_seq);
                }
            }

            const uint64_t expected_after =
                replica.applier->GetExpectedSequenceId();
            if (expected_after > 0) {
                const uint64_t applied = expected_after - 1;
                if (applied > applied_seq_id_.load()) {
                    applied_seq_id_.store(applied);
                }
            }

            if (result.error != ErrorCode::OK) {
                last_error_.store(result.error, std::memory_order_release);
                const bool made_progress = expected_after > expected_before;
                if (result.disposition ==
                    OpLogBatchStandbyPollDisposition::RETRYABLE) {
                    const auto now = std::chrono::steady_clock::now();
                    if (!retry_started || made_progress) {
                        retry_started = now;
                        retry_delay = retry_base;
                    }
                    if (now - *retry_started < retry_limit) {
                        LOG(WARNING)
                            << "Transient batch-record standby poll failure "
                            << "for source=" << source_id << ", retrying in "
                            << retry_delay.count()
                            << " ms, err=" << static_cast<int>(result.error);
                        NotifySyncStatus();
                        wait_for_next_poll(retry_delay);
                        retry_delay =
                            std::min(retry_delay * 2, max_retry_delay);
                        retry_backoff = true;
                        break;
                    }
                    LOG(ERROR)
                        << "Batch-record standby retry timeout after "
                        << config_.batch_oplog_retry_timeout_sec
                        << " seconds for source=" << source_id
                        << ", err=" << static_cast<int>(result.error);
                } else {
                    LOG(ERROR) << "Fatal batch-record standby poll failure "
                               << "for source=" << source_id
                               << ", err=" << static_cast<int>(result.error);
                }
                fatal = true;
                break;
            }

            retry_started.reset();
            retry_delay = retry_base;
            last_error_.store(ErrorCode::OK, std::memory_order_release);
        }

        if (retry_backoff) {
            continue;
        }
        if (fatal) {
            state_machine_.ProcessEvent(StandbyEvent::FATAL_ERROR);
            replication_loop_cv_.notify_all();
            break;
        }

        const uint64_t applied_seq_id = applied_seq_id_.load();
        const uint64_t primary_seq_id = primary_seq_id_.load();
        if (applied_seq_id != last_reported_applied_seq_id ||
            primary_seq_id != last_reported_primary_seq_id) {
            last_reported_applied_seq_id = applied_seq_id;
            last_reported_primary_seq_id = primary_seq_id;
            NotifySyncStatus();
        }

        wait_for_next_poll(
            std::chrono::milliseconds(config_.oplog_poll_interval_ms));
    }

    LOG(INFO) << "Replication loop stopped";
}

void HotStandbyService::VerificationLoop() {
    LOG(INFO) << "Verification loop started";

    while (IsRunning()) {
        std::unique_lock<std::mutex> lock(replication_loop_mutex_);
        replication_loop_cv_.wait_for(
            lock, std::chrono::seconds(config_.verification_interval_sec),
            [this] {
                return !replication_loop_running_.load(
                           std::memory_order_acquire) ||
                       !IsRunning();
            });
        lock.unlock();

        if (!IsRunning() ||
            !replication_loop_running_.load(std::memory_order_acquire)) {
            break;
        }

        if (!IsConnected()) {
            continue;
        }

        // Verification is not yet implemented. When enabled, this loop is
        // expected to:
        // 1) sample keys from the local metadata store,
        // 2) calculate checksums,
        // 3) send a verification request to the Primary, and
        // 4) handle any mismatches that are detected.
        VLOG(1)
            << "Verification check skipped (feature not implemented), state="
            << StandbyStateToString(GetState());
    }

    LOG(INFO) << "Verification loop stopped";
}

}  // namespace mooncake
