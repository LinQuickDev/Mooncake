#include "master_service.h"

#include <array>
#include <algorithm>
#include <cassert>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <future>
#include <limits>
#include <map>
#include <shared_mutex>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <regex>
#include <unordered_set>
#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <ylt/util/tl/expected.hpp>
#include <boost/algorithm/string.hpp>

#include "http_metadata_server.h"
#include "master_metric_manager.h"
#include "master_perf.h"
#include "mooncake_logging.h"
#include "common.h"
#include "segment.h"
#ifdef USE_HTTP
#include "transfer_metadata_plugin.h"
#endif
#ifdef USE_NOF
#include "spdk/spdk_wrapper.h"
#endif
#ifdef STORE_USE_ETCD
#include "etcd_helper.h"
#include "ha/kv/etcd_ha_kv_backend.h"
#include "cvm/etcd_view_store.h"
#include "cvm/cvm_keys.h"
#endif
#include "ha/oplog/oplog_batch_storage.h"
#include "ha/oplog/ordered_oplog_writer.h"
#include "ha/snapshot/catalog/backends/embedded/embedded_snapshot_catalog_store.h"
#include "ha/snapshot/catalog/backends/redis/redis_snapshot_catalog_store.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "ha/snapshot/snapshot_constants.h"
#include "types.h"
#include "serialize/serializer.h"
#include "ha/snapshot/snapshot_logger.h"
#include "utils/zstd_util.h"
#include "utils/file_util.h"
#include "random.h"
#include "utils.h"
#include "kv_event/kv_event_config.h"
#include "master_snapshot_manager.h"
#include "master_snapshot_repository.h"
#include "ha_metric_manager.h"
#include "metadata_store.h"

namespace mooncake {

namespace {

constexpr int kMaxTenantQuotaEvictionRetries = 2;

// Per-cycle offload cap as a fraction of `offloading_queue_limit_`. Used only
// when offload-on-evict mode is active. Defers memory eviction for at most
// this fraction of the queue limit per BatchEvict cycle; beyond that, eviction
// falls back according to `offload_force_evict_`.
// NOTE: Both offloading_queue_limit_ and offload_cap_ratio_ are now
// configurable via --offloading_queue_limit and --offload_cap_ratio flags.

enum class SnapshotCatalogBackendKind {
    kEmbedded,
    kRedis,
};

tl::expected<SnapshotCatalogBackendKind, std::string> ParseSnapshotCatalogKind(
    std::string_view store_type) {
    if (store_type.empty() || store_type == "embedded" ||
        store_type == "payload") {
        return SnapshotCatalogBackendKind::kEmbedded;
    }
    if (store_type == "redis") {
        return SnapshotCatalogBackendKind::kRedis;
    }
    return tl::make_unexpected("unknown snapshot catalog store type: " +
                               std::string(store_type));
}

uint64_t SaturatingAdd(uint64_t lhs, uint64_t rhs) {
    if (lhs > std::numeric_limits<uint64_t>::max() - rhs) {
        return std::numeric_limits<uint64_t>::max();
    }
    return lhs + rhs;
}

uint64_t SaturatingMultiply(uint64_t lhs, uint64_t rhs) {
    if (lhs != 0 && rhs > std::numeric_limits<uint64_t>::max() / lhs) {
        return std::numeric_limits<uint64_t>::max();
    }
    return lhs * rhs;
}

// Decides whether PutStart may proceed with the replicas that were
// actually allocated. Three deliberately different policies apply:
//
//  - Memory-only (nof_replica_num == 0): best-effort by default. Fewer
//    than config.replica_num replicas (but at least one) still succeed,
//    even though DetermineReplicaWriteMode() classifies such configs as
//    RELIABLE_MULTI_REPLICA. The shortfall is surfaced via a WARNING log
//    (action=put_start_partial_allocation) and the
//    master_put_start_partial_allocations_total metric.
//    When strict_memory_only is true (master flag
//    --strict_replica_allocation), the allocation must match
//    config.replica_num exactly, otherwise PutStart fails with
//    NO_AVAILABLE_HANDLE instead of degrading.
//  - FLEXIBLE_DUAL_REPLICA (1 memory + 1 NoF): allocating either side
//    alone is sufficient.
//  - Any other config with nof_replica_num > 0: strict. Both replica
//    types must match the requested counts exactly, otherwise PutStart
//    fails with NO_AVAILABLE_HANDLE.
//
// The "reliable" guarantee of RELIABLE_MULTI_REPLICA is enforced at the
// transfer stage (all allocated replicas must complete or the put is
// revoked), not at the allocation stage for memory-only configs unless
// strict_memory_only is enabled.
bool HasExpectedReplicaAllocation(const ReplicateConfig& config,
                                  size_t allocated_memory_replicas,
                                  size_t allocated_nof_replicas,
                                  bool strict_memory_only) {
    if (config.nof_replica_num == 0) {
        if (strict_memory_only) {
            return allocated_memory_replicas == config.replica_num;
        }
        return allocated_memory_replicas > 0;
    }
    if (DetermineReplicaWriteMode(config) ==
        ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
        return allocated_memory_replicas + allocated_nof_replicas > 0;
    }
    return allocated_memory_replicas == config.replica_num &&
           allocated_nof_replicas == config.nof_replica_num;
}

tl::expected<std::string, ErrorCode> GetGroupIdForKey(
    const ReplicateConfig& config, size_t key_count, size_t key_index) {
    if (!config.group_ids.has_value()) {
        return "";
    }
    if (config.group_ids->size() != key_count || key_index >= key_count) {
        MC_LOG(ERROR) << "group_ids.size()=" << config.group_ids->size()
                      << ", key_count=" << key_count
                      << ", error=invalid_group_ids";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return config.group_ids->at(key_index);
}

}  // namespace

MasterService::MasterService() : MasterService(MasterServiceConfig()) {}

MasterService::MasterService(const MasterServiceConfig& config)
    : graceful_unmount_scheduler_(
          [this](const GracefulUnmountDeadlineRecord& record) {
              auto result =
                  this->UnmountSegment(record.segment_id, record.client_id);
              if (!result.has_value()) {
                  LOG(WARNING)
                      << "Failed to complete graceful unmount, segment_id="
                      << record.segment_id << ", client_id=" << record.client_id
                      << ", error=" << toString(result.error());
              }
          }),
      default_kv_lease_ttl_(config.default_kv_lease_ttl),
      default_kv_soft_pin_ttl_(config.default_kv_soft_pin_ttl),
      allow_evict_soft_pinned_objects_(config.allow_evict_soft_pinned_objects),
      eviction_ratio_(config.eviction_ratio),
      eviction_high_watermark_ratio_(config.eviction_high_watermark_ratio),
      nof_eviction_ratio_(config.nof_eviction_ratio),
      nof_eviction_high_watermark_ratio_(
          config.nof_eviction_high_watermark_ratio),
      view_version_(config.view_version),
      client_live_ttl_sec_(config.client_live_ttl_sec),
      nof_heartbeat_interval_sec_(
          std::chrono::seconds(config.nof_heartbeat_interval_sec)),
      nof_heartbeat_probe_timeout_ms_(
          std::chrono::milliseconds(config.nof_heartbeat_probe_timeout_ms)),
      nof_heartbeat_failures_threshold_(
          config.nof_heartbeat_failures_threshold),
      enable_ha_(config.enable_ha),
      enable_offload_(config.enable_offload),
      ha_backend_type_(config.ha_backend_type),
      ha_backend_connstring_(config.ha_backend_connstring),
      enable_oplog_(config.enable_ha && config.enable_oplog &&
                    config.ha_backend_type == "etcd"),
      oplog_batch_max_entries_(config.oplog_batch_max_entries),
      cluster_id_(config.cluster_id),
      master_id_(config.master_id),
      cvm_http_port_(config.cvm_http_port),
      cvm_http_host_(config.cvm_http_host),
      submaster_count_(config.submaster_count),
      root_fs_dir_(config.root_fs_dir),
      global_file_segment_size_(config.global_file_segment_size),
      enable_disk_eviction_(config.enable_disk_eviction),
      quota_bytes_(config.quota_bytes),
      enable_multi_tenants_(config.enable_multi_tenants),
      tenant_quota_connector_type_(config.tenant_quota_connector_type),
      tenant_quota_connector_uri_(config.tenant_quota_connector_uri),
      segment_manager_(config.memory_allocator, config.enable_cxl),
      nof_segment_manager_(config.memory_allocator),
      vchunk_manager_(config.vchunk_config, config.vchunk_metadata_store),
      vchunk_enabled_(config.vchunk_config.enabled),
      vchunk_reaper_interval_ms_(config.vchunk_config.reaper_interval_ms),
      vchunk_reaper_max_scan_(config.vchunk_config.reaper_max_scan),
      memory_allocator_type_(config.memory_allocator),
      allocation_strategy_type_(config.enable_cxl
                                    ? AllocationStrategyType::CXL
                                    : config.allocation_strategy_type),
      allocation_strategy_(CreateAllocationStrategy(allocation_strategy_type_)),
      enable_snapshot_restore_(config.enable_snapshot_restore),
      enable_snapshot_(config.enable_snapshot),
      snapshot_backup_dir_(config.snapshot_backup_dir),
      snapshot_interval_seconds_(config.snapshot_interval_seconds),
      snapshot_child_timeout_seconds_(config.snapshot_child_timeout_seconds),
      snapshot_retention_count_(config.snapshot_retention_count),
      snapshot_catalog_store_type_(config.snapshot_catalog_store_type),
      snapshot_catalog_store_connstring_(
          config.snapshot_catalog_store_connstring),
      put_start_discard_timeout_sec_(config.put_start_discard_timeout_sec),
      put_start_release_timeout_sec_(config.put_start_release_timeout_sec),
      cxl_path_(config.cxl_path),
      cxl_size_(config.cxl_size),
      enable_cxl_(config.enable_cxl),
      offloading_queue_limit_(config.offloading_queue_limit),
      offload_cap_ratio_(config.offload_cap_ratio),
      task_manager_(config.task_manager_config) {
    if (config.vchunk_config.enabled && config.enable_ha) {
        throw std::invalid_argument(
            "vchunk HA is not supported until allocator ranges can be restored");
    }
    if (config.vchunk_config.enabled && config.vchunk_metadata_store) {
        const auto error = vchunk_manager_.Recover(getCurrentTimeInMilli());
        if (error != ErrorCode::OK) {
            throw std::runtime_error("failed to recover vchunk metadata");
        }
    }
    // Initialize HTTP metadata key prefix (read env var once at startup)
    const char* custom_prefix = std::getenv("MC_METADATA_CLUSTER_ID");
    if (custom_prefix && std::strlen(custom_prefix) > 0) {
        http_metadata_prefix_ = "mooncake/" + std::string(custom_prefix);
        if (http_metadata_prefix_.back() != '/') {
            http_metadata_prefix_ += '/';
        }
    } else {
        http_metadata_prefix_ = "mooncake/";
    }
    if (allocation_strategy_type_ == AllocationStrategyType::LOCAL_FIRST) {
        LOG(INFO) << "Local-first allocation strategy enabled";
    }

    if (enable_snapshot_ || enable_snapshot_restore_) {
        try {
            auto object_store_type =
                ParseSnapshotObjectStoreType(config.snapshot_object_store_type);
            snapshot_object_store_ =
                SnapshotObjectStore::Create(object_store_type);
            snapshot_catalog_store_ = CreateSnapshotCatalogStore();
        } catch (const std::exception& e) {
            LOG(ERROR) << "Failed to create snapshot stores: " << e.what();
            throw std::runtime_error(
                fmt::format("Failed to create snapshot stores: {}", e.what()));
        }
        if (!snapshot_backup_dir_.empty()) {
            use_snapshot_backup_dir_ = true;
        }

        // Initialize repository and codec for both save and restore
        snapshot_repository_ = std::make_unique<MasterSnapshotRepository>(
            snapshot_object_store_.get(), snapshot_catalog_store_.get(),
            snapshot_backup_dir_, use_snapshot_backup_dir_);
        snapshot_codec_ = std::make_unique<ha::MasterSnapshotCodec>();
    }

    if (enable_multi_tenants_) {
        auto store = CreateTenantQuotaPolicyStore(tenant_quota_connector_type_,
                                                  tenant_quota_connector_uri_,
                                                  cluster_id_);
        if (!store) {
            throw std::invalid_argument(store.error());
        }
        tenant_quota_policy_store_ = std::move(store.value());
    }

    if (enable_snapshot_restore_) {
        RestoreState();
    }
    if (enable_multi_tenants_) {
        LoadTenantQuotaPoliciesFromStoreOrThrow();
        RebuildTenantQuotaUsageFromMetadata();
    }
    if (enable_snapshot_ && snapshot_retention_count_ == 0) {
        LOG(ERROR) << "snapshot_retention_count must be greater than 0";
        throw std::invalid_argument("snapshot_retention_count must be > 0");
    }
    if (eviction_ratio_ < 0.0 || eviction_ratio_ > 1.0) {
        LOG(ERROR) << "Eviction ratio must be between 0.0 and 1.0, "
                   << "current value: " << eviction_ratio_;
        throw std::invalid_argument("Invalid eviction ratio");
    }
    if (eviction_high_watermark_ratio_ < 0.0 ||
        eviction_high_watermark_ratio_ > 1.0) {
        LOG(ERROR)
            << "Eviction high watermark ratio must be between 0.0 and 1.0, "
            << "current value: " << eviction_high_watermark_ratio_;
        throw std::invalid_argument("Invalid eviction high watermark ratio");
    }

    // Validate offload tuning knobs here (not only via gflags validator),
    // because values loaded from a configuration file bypass the gflags
    // validator chain.
    if (offload_cap_ratio_ < 0.0 || offload_cap_ratio_ > 1.0) {
        LOG(ERROR) << "offload_cap_ratio must be between 0.0 and 1.0, "
                   << "current value: " << offload_cap_ratio_;
        throw std::invalid_argument("Invalid offload_cap_ratio");
    }
    if (offloading_queue_limit_ == 0) {
        LOG(ERROR) << "offloading_queue_limit must be greater than 0";
        throw std::invalid_argument("Invalid offloading_queue_limit");
    }
    if (offloading_queue_limit_ > 100'000'000ULL) {
        LOG(ERROR) << "offloading_queue_limit must be <= 100000000 to avoid "
                   << "overflow when computing offload_cap, current value: "
                   << offloading_queue_limit_;
        throw std::invalid_argument("Invalid offloading_queue_limit");
    }

    if (put_start_release_timeout_sec_ <= put_start_discard_timeout_sec_) {
        LOG(ERROR) << "put_start_release_timeout="
                   << put_start_release_timeout_sec_.count()
                   << " must be larger than put_start_discard_timeout_sec="
                   << put_start_discard_timeout_sec_.count();
        throw std::invalid_argument(
            "put_start_release_timeout must be larger than "
            "put_start_discard_timeout_sec");
    }

#ifdef USE_NOF
    if (nof_heartbeat_interval_sec_.count() <= 0) {
        LOG(ERROR) << "nof_heartbeat_interval_sec must be positive, current "
                   << nof_heartbeat_interval_sec_.count();
        throw std::invalid_argument("Invalid nof heartbeat interval");
    }
    if (nof_heartbeat_probe_timeout_ms_.count() <= 0) {
        LOG(ERROR) << "nof_heartbeat_probe_timeout_ms must be positive, "
                   << "current " << nof_heartbeat_probe_timeout_ms_.count();
        throw std::invalid_argument("Invalid nof heartbeat probe timeout");
    }
    if (nof_heartbeat_failures_threshold_ == 0) {
        LOG(ERROR) << "nof_heartbeat_failures_threshold must be positive";
        throw std::invalid_argument("Invalid nof heartbeat failure threshold");
    }

    nof_probe_fn_ = [](const std::string& te_endpoint, uint32_t timeout_ms,
                       std::string* error_reason) {
        return SpdkWrapper::GetInstance().ProbeNofSegment(
            te_endpoint, timeout_ms, error_reason);
    };
#endif

    // Strict replica allocation: memory-only multi-replica requests must
    // allocate exactly replica_num replicas instead of degrading.
    strict_replica_allocation_ = config.strict_replica_allocation;
    if (strict_replica_allocation_) {
        MC_LOG(INFO) << "Strict replica allocation enabled: memory-only "
                        "multi-replica requests must be fully satisfied";
    }

    // Offload-on-evict: defer LOCAL_DISK offload to eviction time
    offload_on_evict_ = enable_offload_ && config.offload_on_evict;
    if (offload_on_evict_) {
        LOG(INFO) << "Offload-on-evict mode enabled: DRAM offload to "
                     "LOCAL_DISK will occur at eviction time instead of "
                     "PutEnd";
        offload_force_evict_ = config.offload_force_evict;
        if (offload_force_evict_) {
            LOG(INFO) << "Force-evict enabled: objects exceeding offload "
                         "cap will be evicted without disk offload";
        }
    }

    // Promotion-on-hit: when Get observes a LOCAL_DISK-only key, queue an
    // async copy back to MEMORY. Only meaningful when offload is enabled
    // (otherwise no LOCAL_DISK replicas exist in the first place).
    promotion_on_hit_ = enable_offload_ && config.promotion_on_hit;
    promotion_admission_threshold_ = config.promotion_admission_threshold;
    promotion_queue_limit_ = config.promotion_queue_limit;
    promotion_max_per_heartbeat_ = config.promotion_max_per_heartbeat;
    // Clamp to >=1: 0 would make PromotionObjectHeartbeat return an empty
    // batch every call, silently disabling promotion delivery.
    if (promotion_max_per_heartbeat_ == 0) {
        promotion_max_per_heartbeat_ = 1;
    }
    // Defense-in-depth clamp: master.cpp clamps threshold into [1, 255]
    // at flag-parse time, but direct MasterServiceConfig construction
    // (tests, embedded users) bypasses that. Without the clamp here,
    // threshold=0 would silently bypass the frequency gate entirely
    // (freq < 0 is never true for uint8_t).
    if (promotion_admission_threshold_ == 0) {
        promotion_admission_threshold_ = 1;
    } else if (promotion_admission_threshold_ > 255) {
        promotion_admission_threshold_ = 255;
    }
    if (config.promotion_on_hit && !enable_offload_) {
        LOG(WARNING) << "promotion_on_hit=true was requested but "
                     << "enable_offload=false; promotion is silently "
                     << "disabled because it requires offload to produce "
                     << "LOCAL_DISK replicas. Set enable_offload=true to "
                     << "use this feature.";
    }
    if (promotion_on_hit_) {
        promotion_sketch_ = std::make_unique<CountMinSketch>();
        LOG(INFO) << "Promotion-on-hit mode enabled: LOCAL_DISK-only Gets "
                     "will queue async promotion to MEMORY (threshold="
                  << promotion_admission_threshold_
                  << ", queue_limit=" << promotion_queue_limit_
                  << ", max_per_heartbeat=" << promotion_max_per_heartbeat_
                  << ")";
    }

    kv_event_publisher_ =
        std::make_unique<KvEventPublisher>(BuildKvEventConfig(config));

    if (enable_oplog_ && !cluster_id_.empty()) {
#ifdef STORE_USE_ETCD
        if (ha_backend_connstring_.empty()) {
            LOG(INFO) << "Skipping automatic batch-record OpLog writer "
                         "initialization; no HA backend connstring configured";
        } else {
            ErrorCode connect_err = EtcdHelper::ConnectToEtcdStoreClient(
                ha_backend_connstring_.c_str());
            if (connect_err != ErrorCode::OK) {
                throw std::runtime_error(fmt::format(
                    "failed to connect HA batch-record OpLog writer to etcd: "
                    "{}",
                    toString(connect_err)));
            }
            auto backend = std::make_shared<EtcdHaKvBackend>();
            ErrorCode err = InitializeBatchOpLogWriter(std::move(backend));
            if (err != ErrorCode::OK) {
                throw std::runtime_error(fmt::format(
                    "failed to create HA batch-record OpLog writer: {}",
                    toString(err)));
            }
        }
#else
        if (ha_backend_connstring_.empty()) {
            LOG(INFO) << "Skipping automatic batch-record OpLog writer "
                         "initialization; no HA backend connstring configured";
        } else {
            throw std::runtime_error(
                "failed to create HA batch-record OpLog writer: ETCD support "
                "not compiled in");
        }
#endif
    }

    // KV partition (CVM) ownership is now owned by the HA supervisor: the
    // supervisor creates the CvmController (etcd lease + master registration +
    // snapshot aggregation + membership loop), then injects the lease id and
    // drives SlotOwnerHeartbeat around serve start/stop via
    // SetCvmLeaseId() / StartSlotOwnerHeartbeat() / StopSlotOwnerHeartbeat().
    // MasterService itself only publishes slot/segment ownership records.

    eviction_running_ = true;
    eviction_thread_ = std::thread(&MasterService::EvictionThreadFunc, this);
    VLOG(1) << "action=start_eviction_thread";

    // Start client monitor thread in all modes so TTL/heartbeat works
    client_monitor_running_ = true;
    client_monitor_thread_ =
        std::thread(&MasterService::ClientMonitorFunc, this);
    VLOG(1) << "action=start_client_monitor_thread";

#ifdef USE_NOF
    nof_heartbeat_running_ = true;
    nof_heartbeat_thread_ =
        std::thread(&MasterService::NofHeartbeatThreadFunc, this);
    VLOG(1) << "action=start_nof_heartbeat_thread";
#endif

    // Start task cleanup thread
    task_cleanup_running_ = true;
    task_cleanup_thread_ =
        std::thread(&MasterService::TaskCleanupThreadFunc, this);
    VLOG(1) << "action=start_task_cleanup_thread";

    // NOTE: The async HTTP metadata cleanup worker is started lazily in
    // setHttpMetadataRemoteUrl() once http_metadata_remote_ is initialized,
    // since that happens after this constructor returns (in
    // WrappedMasterService).

    job_dispatch_running_ = true;
    job_dispatch_thread_ =
        std::thread(&MasterService::JobDispatchThreadFunc, this);
    VLOG(1) << "action=start_job_dispatch_thread";

    if (!root_fs_dir_.empty()) {
        use_disk_replica_ = true;
        if (global_file_segment_size_ == std::numeric_limits<int64_t>::max()) {
            MasterMetricManager::instance().set_dfs_capacity_unlimited(true);
        } else {
            MasterMetricManager::instance().inc_total_file_capacity(
                global_file_segment_size_);
        }
    }

    if (enable_snapshot_ && !enable_oplog_) {
        if (memory_allocator_type_ == BufferAllocatorType::OFFSET) {
            // Initialize and start snapshot manager
            MasterSnapshotManagerOptions snapshot_options;
            snapshot_options.enable_snapshot = enable_snapshot_;
            snapshot_options.snapshot_interval_seconds =
                snapshot_interval_seconds_;
            snapshot_options.snapshot_child_timeout_seconds =
                snapshot_child_timeout_seconds_;
            snapshot_options.snapshot_retention_count =
                snapshot_retention_count_;
            snapshot_options.snapshot_backup_dir = snapshot_backup_dir_;
            snapshot_options.use_snapshot_backup_dir = use_snapshot_backup_dir_;
            snapshot_options.snapshot_catalog_store_type =
                snapshot_catalog_store_type_;
            snapshot_options.snapshot_catalog_store_connstring =
                snapshot_catalog_store_connstring_;
            snapshot_options.ha_backend_type = ha_backend_type_;
            snapshot_options.ha_backend_connstring = ha_backend_connstring_;
            snapshot_options.cluster_id = cluster_id_;
            snapshot_options.enable_ha = enable_ha_;

            snapshot_manager_ = std::make_unique<MasterSnapshotManager>(
                this, snapshot_options, snapshot_mutex_,
                snapshot_object_store_.get(), snapshot_catalog_store_.get());
            snapshot_manager_->Start();
        }
    } else if (enable_snapshot_ && enable_oplog_) {
        LOG(INFO) << "Skipping primary snapshot generation in batch-record "
                     "OpLog mode; snapshots are owned by standby";
    }

    if (enable_cxl_) {
        allocation_strategy_ = std::make_shared<CxlAllocationStrategy>();
        segment_manager_.initializeCxlAllocator(cxl_path_, cxl_size_);
        VLOG(1) << "action=start_cxl_global_allocator";
    }
    if (vchunk_enabled_) {
        vchunk_reaper_running_ = true;
        vchunk_reaper_thread_ =
            std::thread(&MasterService::VChunkReaperThreadFunc, this);
    }
}

tl::expected<VChunkMetadataRecord, ErrorCode> MasterService::VChunkPutStart(
    const TenantId& tenant_id, const std::string& key, uint64_t total_size,
    bool is_ssd_segment, int64_t now_ms,
    const std::set<std::string>& excluded_segments) {
    auto allocator_access = segment_manager_.getAllocatorAccess();
    return vchunk_manager_.PutStart(allocator_access.getAllocatorManager(),
                                    tenant_id, key, total_size,
                                    is_ssd_segment, now_ms,
                                    excluded_segments);
}

ErrorCode MasterService::VChunkPutEnd(const TenantId& tenant_id,
                                      const std::string& key,
                                      const std::string& vchunk_id,
                                      int64_t now_ms) {
    return vchunk_manager_.PutEnd(tenant_id, key, vchunk_id, now_ms);
}

ErrorCode MasterService::VChunkPutRevoke(const TenantId& tenant_id,
                                         const std::string& key,
                                         const std::string& vchunk_id) {
    auto allocator_access = segment_manager_.getAllocatorAccess();
    return vchunk_manager_.PutRevoke(tenant_id, key, vchunk_id);
}

tl::expected<VChunkMetadataRecord, ErrorCode> MasterService::GetVChunk(
    const TenantId& tenant_id, const std::string& key) const {
    return vchunk_manager_.Get(tenant_id, key);
}

tl::expected<VChunkMasterManager::ReadHandle, ErrorCode>
MasterService::AcquireVChunkRead(const TenantId& tenant_id,
                                 const std::string& key) const {
    return vchunk_manager_.AcquireRead(tenant_id, key);
}

ErrorCode MasterService::RemoveVChunk(const TenantId& tenant_id,
                                      const std::string& key,
                                      int64_t now_ms) {
    auto allocator_access = segment_manager_.getAllocatorAccess();
    return vchunk_manager_.Remove(tenant_id, key, now_ms);
}

VChunkRuntimeInfo MasterService::GetVChunkRuntimeInfo() const {
    return {vchunk_enabled_, vchunk_manager_.HasPersistentMetadata()};
}

tl::expected<size_t, ErrorCode> MasterService::ReapExpiredVChunks(
    int64_t now_ms, size_t max_scan) {
    auto allocator_access = segment_manager_.getAllocatorAccess();
    return vchunk_manager_.ReapExpired(now_ms, max_scan);
}

VChunkMetricsSnapshot MasterService::GetVChunkMetrics() const {
    return vchunk_manager_.MetricsSnapshot();
}

void MasterService::VChunkReaperThreadFunc() {
    std::unique_lock<std::mutex> lock(vchunk_reaper_mutex_);
    while (vchunk_reaper_running_) {
        if (vchunk_reaper_cv_.wait_for(
                lock, std::chrono::milliseconds(vchunk_reaper_interval_ms_),
                [this] { return !vchunk_reaper_running_.load(); })) {
            break;
        }
        lock.unlock();
        const auto result = ReapExpiredVChunks(getCurrentTimeInMilli(),
                                               vchunk_reaper_max_scan_);
        if (!result) {
            LOG(ERROR) << "vchunk reaper failed, error="
                       << static_cast<int>(result.error());
        }
        lock.lock();
    }
}

std::unique_ptr<ha::SnapshotCatalogStore>
MasterService::CreateSnapshotCatalogStore() {
    auto catalog_kind = ParseSnapshotCatalogKind(snapshot_catalog_store_type_);
    if (!catalog_kind) {
        throw std::invalid_argument(catalog_kind.error());
    }

    switch (catalog_kind.value()) {
        case SnapshotCatalogBackendKind::kEmbedded:
            return std::make_unique<
                ha::backends::embedded::EmbeddedSnapshotCatalogStore>(
                snapshot_object_store_.get(), cluster_id_);
        case SnapshotCatalogBackendKind::kRedis: {
#ifndef STORE_USE_REDIS
            throw std::invalid_argument(
                "redis snapshot catalog store is unavailable in the current "
                "build");
#else
            const auto connstring = !snapshot_catalog_store_connstring_.empty()
                                        ? snapshot_catalog_store_connstring_
                                        : ha_backend_connstring_;
            if (connstring.empty()) {
                throw std::invalid_argument(
                    "redis snapshot catalog store requires a connection "
                    "string");
            }
            return std::make_unique<
                ha::backends::redis::RedisSnapshotCatalogStore>(
                snapshot_object_store_.get(), connstring, cluster_id_);
#endif
        }
    }

    throw std::invalid_argument("unknown snapshot catalog store type");
}

void MasterService::SetCvmLeaseId(EtcdLeaseId lease_id) {
    cvm_lease_id_ = lease_id;
}

void MasterService::StopSlotOwnerHeartbeat() {
    if (slot_owner_heartbeat_) {
        slot_owner_heartbeat_->Stop();
        slot_owner_heartbeat_.reset();
    }
}

#ifdef STORE_USE_ETCD
ErrorCode MasterService::StartSlotOwnerHeartbeat() {
    const bool kv_partition_enabled = enable_ha_ &&
                                      ha_backend_type_ == "etcd" &&
                                      !master_id_.empty() &&
                                      !cluster_id_.empty();
    if (!kv_partition_enabled) {
        return ErrorCode::OK;
    }

    // The supervisor's CvmController already connected the etcd client before
    // this point; ConnectToEtcdStoreClient is idempotent so re-connecting is a
    // safe no-op for direct constructions / tests.
    ErrorCode connect_err =
        EtcdHelper::ConnectToEtcdStoreClient(ha_backend_connstring_);
    if (connect_err != ErrorCode::OK) {
        LOG(WARNING) << "StartSlotOwnerHeartbeat: failed to connect etcd: "
                     << connect_err;
        return connect_err;
    }

    if (slot_owner_heartbeat_) {
        return ErrorCode::OK;  // already running
    }

    cvm::SlotOwnerHeartbeat::Config hb_config;
    hb_config.cluster_namespace = cluster_id_;
    hb_config.master_id = master_id_;
    // Dynamic partition: recompute the owned slot set from the etcd master
    // membership on every heartbeat so multiple submaster instances split the
    // 16384 slots without overwriting each other.
    hb_config.dynamic_slot_resolver = [this]() {
        auto slots = ResolveOwnedSlotsForCvm();
        UpdateOwnedSlots(slots);
        return slots;
    };
    hb_config.lease_id = cvm_lease_id_;
    // live primary → live primary 元数据交接（P4 技术债 1）：slot 平移时在
    // 释放端导出对象元数据、在获得端导入，避免只依赖 standby 回放晋升路径。
    hb_config.on_slot_acquired = [this](uint16_t slot) {
        (void)ImportSlotMetadata(slot);
    };
    hb_config.on_slot_released = [this](uint16_t slot) {
        (void)ExportSlotMetadata(slot);
    };
    const bool lease_bound = hb_config.lease_id != 0;
    slot_owner_heartbeat_ =
        std::make_unique<cvm::SlotOwnerHeartbeat>(std::move(hb_config));
    ErrorCode hb_err = slot_owner_heartbeat_->Start();
    if (hb_err != ErrorCode::OK) {
        LOG(WARNING) << "Failed to start SlotOwnerHeartbeat: " << hb_err;
        slot_owner_heartbeat_.reset();
        return hb_err;
    }
    LOG(INFO) << "Started SlotOwnerHeartbeat: master_id=" << master_id_
              << ", cluster_namespace=" << cluster_id_
              << ", dynamic_partition=true"
              << ", lease_bound=" << lease_bound;
    return ErrorCode::OK;
}

#ifdef STORE_USE_ETCD
ErrorCode MasterService::StartInterMasterRpc() {
    const bool cvm_enabled = enable_ha_ && ha_backend_type_ == "etcd" &&
                             !master_id_.empty() && !cluster_id_.empty();
    if (!cvm_enabled) {
        return ErrorCode::OK;
    }

    // The supervisor's CvmController already connected the etcd client;
    // re-connecting is an idempotent no-op.
    ErrorCode connect_err =
        EtcdHelper::ConnectToEtcdStoreClient(ha_backend_connstring_);
    if (connect_err != ErrorCode::OK) {
        LOG(WARNING) << "StartInterMasterRpc: failed to connect etcd: "
                     << connect_err;
        return connect_err;
    }

    if (inter_master_rpc_) {
        return ErrorCode::OK;  // already running
    }

    inter_master_rpc_ = std::make_unique<cvm::InterMasterRpcClient>();
    ErrorCode rc = inter_master_rpc_->Start(cluster_id_, master_id_);
    if (rc != ErrorCode::OK) {
        LOG(WARNING) << "StartInterMasterRpc: refresh loop not started: "
                     << rc << " (manual member updates still work)";
        // Keep the client object for manual member updates; only the
        // etcd-driven refresh thread is unavailable.
    }
    LOG(INFO) << "Started InterMasterRpcClient: master_id=" << master_id_
              << ", cluster_namespace=" << cluster_id_;
    return ErrorCode::OK;
}

void MasterService::StopInterMasterRpc() {
    if (inter_master_rpc_) {
        inter_master_rpc_->Stop();
        inter_master_rpc_.reset();
    }
}
#else
ErrorCode MasterService::StartInterMasterRpc() { return ErrorCode::OK; }
void MasterService::StopInterMasterRpc() {}
#endif

uint32_t MasterService::GetOwnedSlotCount() const {
    std::shared_lock<std::shared_mutex> lock(owned_slots_mutex_);
    if (!owned_slots_ready_) {
        return 0;
    }
    return static_cast<uint32_t>(
        std::count(owned_slot_lookup_.begin(), owned_slot_lookup_.end(), true));
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
MasterService::InterMasterAllocateReplicas(
    const std::string& tenant_id, const std::string& key,
    uint64_t slice_length, uint64_t replica_num,
    const std::vector<std::string>& preferred_segments) {
    if (key.empty() || slice_length == 0 || replica_num == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    ScopedAllocatorAccess allocator_access =
        segment_manager_.getAllocatorAccess();
    const auto& allocator_manager = allocator_access.getAllocatorManager();
    const auto& local_names = allocator_manager.getNames();
    if (local_names.empty()) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    // Strict mode: when preferred segments are given, only allocate in them.
    // Exclude every other local segment so the strategy's random fallback
    // cannot leak the allocation into unrelated segments.
    std::set<std::string> excluded;
    if (!preferred_segments.empty()) {
        const std::unordered_set<std::string> preferred(
            preferred_segments.begin(), preferred_segments.end());
        bool any_preferred_local = false;
        for (const auto& name : local_names) {
            if (preferred.count(name) > 0) {
                any_preferred_local = true;
            } else {
                excluded.insert(name);
            }
        }
        if (!any_preferred_local) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
    }

    auto allocation = allocation_strategy_->Allocate(
        allocator_manager, slice_length, replica_num, preferred_segments,
        excluded, ReplicaType::MEMORY);
    if (!allocation.has_value()) {
        return tl::make_unexpected(allocation.error());
    }
    // Partial allocation is not forwarded: the slot owner requires the full
    // replica count. Dropped here => destructors free the partial handles.
    if (allocation->size() != replica_num) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    std::vector<Replica::Descriptor> descriptors;
    descriptors.reserve(allocation->size());
    for (const auto& replica : allocation.value()) {
        descriptors.push_back(replica.get_descriptor());
    }
    const std::string scoped_key = TenantId(tenant_id).MakeScopedKey(key);
    {
        std::lock_guard<std::mutex> lock(inter_master_keepalive_mutex_);
        // Replace any stale keepalive entry for the same key: erasing frees
        // the old handles (idempotent re-PutStart after an aborted write).
        inter_master_keepalive_.erase(scoped_key);
        inter_master_keepalive_.emplace(scoped_key,
                                        std::move(allocation.value()));
    }
    LOG(INFO) << "InterMasterAllocateReplicas: allocated "
             << descriptors.size() << " replica(s) for scoped_key="
             << scoped_key << ", slice_length=" << slice_length
             << ", preferred_segments=" << preferred_segments.size();
    return descriptors;
}

tl::expected<bool, ErrorCode> MasterService::InterMasterFreeReplicas(
    const std::string& tenant_id, const std::string& key) {
    const std::string scoped_key = TenantId(tenant_id).MakeScopedKey(key);
    std::vector<Replica> released;
    {
        std::lock_guard<std::mutex> lock(inter_master_keepalive_mutex_);
        auto it = inter_master_keepalive_.find(scoped_key);
        if (it == inter_master_keepalive_.end()) {
            return false;  // not the owner of this key's handles
        }
        released = std::move(it->second);
        inter_master_keepalive_.erase(it);
    }
    // Replica destructors free the handles at this (segment owning) master.
    LOG(INFO) << "InterMasterFreeReplicas: freed " << released.size()
              << " replica(s) for scoped_key=" << scoped_key;
    return true;
}

tl::expected<GetReplicaListResponse, ErrorCode>
MasterService::InterMasterGetReplicaList(const std::string& key,
                                         const std::string& tenant_id) {
    const TenantId tenant(tenant_id);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant);
    // peer 互信：不校验 OwnsSlot、不再次转发，直接本地查询（转发链止于
    // 第一跳，避免视图不一致时的循环转发）。
    return GetReplicaListLocal(object_id);
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
MasterService::InterMasterBatchGetReplicaList(
    const std::vector<std::string>& keys, const std::string& tenant_id) {
    const TenantId tenant(tenant_id);
    return BatchGetReplicaListLocal(keys, tenant);
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
MasterService::InterMasterPutStart(
    const UUID& client_id, const std::string& key, const std::string& tenant_id,
    uint64_t slice_length, const ReplicateConfig& config) {
    const TenantId tenant(tenant_id);
    // peer 互信：调用方已按 slot 归属解析本机为 owner，直接执行完整本地
    // PutStart（分配 + 写元数据 + keepalive）。本机 OwnsSlot==true，不会再
    // 二次转发（转发链止于第一跳，避免视图不一致时的循环转发）。
    return PutStart(client_id, key, tenant, slice_length, config);
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
MasterService::InterMasterUpsertStart(
    const UUID& client_id, const std::string& key, const std::string& tenant_id,
    uint64_t slice_length, const ReplicateConfig& config) {
    const TenantId tenant(tenant_id);
    // Upsert 转发：本机为 slot owner，执行完整本地 UpsertStart 以保留
    // "已存在则覆盖（preemption）"语义；由 PutStart 转发走 PutStart 会丢失
    // 该覆盖语义。peer 互信，不二次转发。
    return UpsertStart(client_id, key, tenant, slice_length, config);
}

tl::expected<std::vector<Replica>, ErrorCode>
MasterService::TryAllocateReplicasRemotely(
    const std::string& key, const TenantId& tenant_id, uint64_t value_length,
    size_t replica_num, const std::vector<std::string>& preferred_segments) {
#ifdef STORE_USE_ETCD
    if (!inter_master_rpc_ || replica_num == 0) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    const std::string tenant_str = tenant_id.value();
    for (const auto& member : inter_master_rpc_->GetMembers()) {
        if (member.master_id.empty() || member.master_id == master_id_) {
            continue;
        }
        auto remote = inter_master_rpc_->AllocateReplicas(
            member.master_id, tenant_str, key, value_length,
            static_cast<uint64_t>(replica_num), preferred_segments);
        if (!remote.has_value()) {
            VLOG(1) << "Remote allocation on " << member.master_id
                    << " failed for key=" << key << ": "
                    << toString(remote.error());
            continue;
        }
        if (remote->size() < replica_num) {
            // Partial result: free it at the peer and keep probing.
            inter_master_rpc_->FreeReplicas(member.master_id, tenant_str, key);
            continue;
        }

        // Materialize dummy-allocator replicas from the descriptors (same
        // pattern as ImportSlotMetadata). The real handles stay alive at the
        // segment owner's keepalive registry.
        std::vector<Replica> replicas;
        replicas.reserve(remote->size());
        for (const auto& desc : remote.value()) {
            if (!desc.is_memory_replica()) {
                continue;
            }
            const auto& mem_desc = desc.get_memory_descriptor();
            const std::string& endpoint =
                mem_desc.buffer_descriptor.transport_endpoint_;
            std::shared_ptr<DummyBufferAllocator> alloc;
            {
                std::lock_guard<std::mutex> lock(
                    remote_replica_allocator_keepalive_mutex_);
                auto& slot = remote_replica_allocator_keepalive_[endpoint];
                if (!slot) {
                    slot = std::make_shared<DummyBufferAllocator>(endpoint,
                                                                   endpoint);
                }
                alloc = slot;
            }
            replicas.emplace_back(
                std::make_unique<AllocatedBuffer>(
                    alloc, mem_desc.buffer_descriptor),
                desc.status);
        }
        if (replicas.size() != replica_num) {
            // Unexpected descriptor types: undo at the peer and fail.
            inter_master_rpc_->FreeReplicas(member.master_id, tenant_str, key);
            continue;
        }

        {
            std::lock_guard<std::mutex> lock(remote_allocated_keys_mutex_);
            remote_allocated_keys_.insert(tenant_id.MakeScopedKey(key));
        }
        LOG(INFO) << "Allocated " << replicas.size()
                  << " remote replica(s) for key=" << key
                  << " on segment owner " << member.master_id;
        return replicas;
    }
    return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
#else
    (void)key;
    (void)tenant_id;
    (void)value_length;
    (void)replica_num;
    (void)preferred_segments;
    return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
#endif
}

void MasterService::EnqueueRemoteFreeIfTracked(const TenantId& tenant_id,
                                               const std::string& key,
                                               QuotaEraseMode quota_mode) {
#ifdef STORE_USE_ETCD
    {
        std::lock_guard<std::mutex> lock(remote_allocated_keys_mutex_);
        if (remote_allocated_keys_.erase(tenant_id.MakeScopedKey(key)) == 0) {
            return;  // handles (if any) are local
        }
    }
    if (quota_mode == QuotaEraseMode::kHandoff) {
        // Slot migration: the data bytes live on. The importing master
        // re-registers the key and owns the remote free from now on.
        return;
    }
    if (inter_master_rpc_) {
        inter_master_rpc_->EnqueueBroadcastFree(tenant_id.value(), key);
    }
#endif
}

ErrorCode MasterService::ExportSlotMetadata(uint16_t slot) {
    // 1. Collect this slot's object metadata across all shards.
    SlotMetadataExport export_payload;
    export_payload.slot = slot;
    export_payload.source_master_id = master_id_;

    for (size_t shard_idx = 0; shard_idx < kNumShards; ++shard_idx) {
        MetadataShardAccessorRO shard(this, shard_idx);
        for (const auto& [tenant_id, tenant_state] : shard->tenants) {
            for (const auto& [user_key, metadata] : tenant_state.metadata) {
                if (cvm::KeySlot(tenant_id, user_key) != slot) {
                    continue;
                }
                StandbyObjectEntry entry;
                entry.tenant_id = tenant_id.value();
                entry.key = user_key;
                entry.metadata.client_id = metadata.client_id;
                entry.metadata.size = metadata.size;
                entry.metadata.group_id = metadata.group_id;
                entry.metadata.data_type = metadata.data_type;
                const auto& replicas = metadata.GetAllReplicas();
                entry.metadata.replicas.reserve(replicas.size());
                for (const auto& replica : replicas) {
                    entry.metadata.replicas.push_back(replica.get_descriptor());
                }
                export_payload.objects.push_back(std::move(entry));
            }
        }
    }

    // 2. Serialize (struct_pack) and persist as a binary etcd value.
    auto bytes = struct_pack::serialize(export_payload);
    std::string value(bytes.begin(), bytes.end());
    const std::string key = cvm::SlotMetadataExportKey(cluster_id_, slot);
    ErrorCode err =
        EtcdHelper::Put(key.data(), key.size(), value.data(), value.size());
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "ExportSlotMetadata: put failed slot=" << slot
                     << ", objects=" << export_payload.objects.size()
                     << ", err=" << err;
        return err;
    }

    // 3. Drop the exported objects from local metadata (release cleanup).
    for (size_t shard_idx = 0; shard_idx < kNumShards; ++shard_idx) {
        MetadataShardAccessorRW shard(this, shard_idx);
        for (auto tenant_it = shard->tenants.begin();
             tenant_it != shard->tenants.end();) {
            auto& tenant_state = tenant_it->second;
            for (auto it = tenant_state.metadata.begin();
                 it != tenant_state.metadata.end();) {
                if (cvm::KeySlot(tenant_it->first, it->first) == slot) {
                    it = EraseMetadata(tenant_state, it, tenant_it->first,
                                       QuotaEraseMode::kHandoff);
                } else {
                    ++it;
                }
            }
            if (tenant_state.Empty()) {
                tenant_it = shard->tenants.erase(tenant_it);
            } else {
                ++tenant_it;
            }
        }
    }

    LOG(INFO) << "Exported slot metadata: slot=" << slot
              << ", objects=" << export_payload.objects.size()
              << ", bytes=" << value.size() << ", source=" << master_id_;
    return ErrorCode::OK;
}

ErrorCode MasterService::ImportSlotMetadata(uint16_t slot) {
    const std::string key = cvm::SlotMetadataExportKey(cluster_id_, slot);
    std::string value;
    EtcdRevisionId revision = 0;
    ErrorCode err = EtcdHelper::Get(key.data(), key.size(), value, revision);
    if (err == ErrorCode::ETCD_KEY_NOT_EXIST) {
        // No graceful export available (e.g. the previous owner died and the
        // slot was reclaimed via standby replay). Nothing to pull.
        return ErrorCode::OK;
    }
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "ImportSlotMetadata: get failed slot=" << slot
                     << ", err=" << err;
        return err;
    }

    SlotMetadataExport export_payload;
    if (struct_pack::deserialize_to(export_payload, value) !=
        struct_pack::errc::ok) {
        LOG(ERROR) << "ImportSlotMetadata: deserialize failed slot=" << slot
                   << ", bytes=" << value.size();
        return ErrorCode::DESERIALIZE_FAIL;
    }

    const auto resolve = [](const StandbyObjectEntry& entry) {
        auto [scoped_tenant_id, user_key] = TenantId::ParseScopedKey(entry.key);
        TenantId tenant_id(entry.tenant_id);
        if (tenant_id.IsDefault() && !scoped_tenant_id.IsDefault()) {
            tenant_id = std::move(scoped_tenant_id);
        }
        return std::make_pair(std::move(tenant_id), std::move(user_key));
    };

    std::unordered_map<size_t, std::vector<const StandbyObjectEntry*>>
        objects_by_shard;
    for (const auto& entry : export_payload.objects) {
        auto [tenant_id, user_key] = resolve(entry);
        if (!tenant_id.IsValid()) {
            LOG(WARNING) << "ImportSlotMetadata: invalid tenant for slot="
                         << slot << ", key=" << entry.key;
            continue;
        }
        const auto shard_idx = entry.metadata.group_id.empty()
                                   ? getShardIndex(tenant_id, user_key)
                                   : getShardIndex(entry.metadata.group_id);
        objects_by_shard[shard_idx].push_back(&entry);
    }

    size_t imported = 0;
    for (const auto& [shard_idx, shard_objects] : objects_by_shard) {
        MetadataShardAccessorRW shard(this, shard_idx);
        auto now = std::chrono::system_clock::now();
        for (const auto* entry_ptr : shard_objects) {
            const auto& entry = *entry_ptr;
            auto [tenant_id, user_key] = resolve(entry);
            const auto& standby_meta = entry.metadata;
            std::vector<Replica> replicas;
            replicas.reserve(standby_meta.replicas.size());
            for (const auto& desc : standby_meta.replicas) {
                if (desc.is_memory_replica()) {
                    const auto& mem_desc = desc.get_memory_descriptor();
                    const std::string& endpoint =
                        mem_desc.buffer_descriptor.transport_endpoint_;
                    auto& alloc = standby_allocator_keepalive_[endpoint];
                    if (!alloc) {
                        alloc = std::make_shared<DummyBufferAllocator>(
                            endpoint, endpoint);
                    }
                    replicas.emplace_back(
                        std::make_unique<AllocatedBuffer>(
                            alloc, mem_desc.buffer_descriptor),
                        desc.status);
                } else if (desc.is_nof_replica()) {
                    const auto& nof_desc = desc.get_nof_descriptor();
                    const std::string& endpoint =
                        nof_desc.buffer_descriptor.transport_endpoint_;
                    auto& alloc = standby_allocator_keepalive_[endpoint];
                    if (!alloc) {
                        alloc = std::make_shared<DummyBufferAllocator>(
                            endpoint, endpoint);
                    }
                    replicas.emplace_back(
                        std::make_unique<AllocatedBuffer>(
                            alloc, nof_desc.buffer_descriptor),
                        desc.status, ReplicaType::NOF_SSD);
                } else if (desc.is_disk_replica()) {
                    const auto& disk_desc = desc.get_disk_descriptor();
                    replicas.emplace_back(disk_desc.file_path,
                                          disk_desc.object_size, desc.status);
                } else if (desc.is_local_disk_replica()) {
                    const auto& local_disk_desc =
                        desc.get_local_disk_descriptor();
                    replicas.emplace_back(local_disk_desc.client_id,
                                          local_disk_desc.object_size,
                                          local_disk_desc.transport_endpoint,
                                          desc.status);
                }
            }

            auto& tenant_state = shard->tenants[tenant_id];
            auto [metadata_it, inserted] = tenant_state.metadata.emplace(
                std::piecewise_construct, std::forward_as_tuple(user_key),
                std::forward_as_tuple(
                    standby_meta.client_id, now, standby_meta.size,
                    std::move(replicas), false, false, standby_meta.data_type,
                    standby_meta.group_id, tenant_id, user_key));
            if (!inserted) {
                // 新获得 slot 时理论上不应碰撞；若碰撞则跳过以避免重复记账。
                LOG(WARNING) << "ImportSlotMetadata: duplicate key slot=" << slot
                             << ", key=" << entry.key << ", skipped";
                continue;
            }
            auto& metadata = metadata_it->second;
            if (!standby_meta.group_id.empty()) {
                RegisterGroupMember(tenant_state, tenant_id, user_key,
                                    standby_meta.group_id);
            }
            tenant_state.processing_keys.erase(user_key);

            // A2：补回交接对象的账务（object count、cache 计数、quota），与
            // Export 侧 kHandoff 的释放对称，避免新 primary 账务缺失。
            IncrementTenantMetadataObjectCount(tenant_id);
            SyncCacheTotalAccounting(metadata);
            const uint64_t committed_charge =
                CompletedMemoryQuotaCharge(metadata);
            metadata.reserved_quota_charge_bytes = 0;
            metadata.pending_replaced_quota_charge_bytes = 0;
            metadata.committed_quota_charge_bytes = 0;
            if (committed_charge > 0) {
                auto reserve_result =
                    ReserveTenantQuota(tenant_id, committed_charge);
                if (reserve_result) {
                    CommitTenantQuota(tenant_id, committed_charge);
                    metadata.committed_quota_charge_bytes = committed_charge;
                } else {
                    LOG(WARNING)
                        << "ImportSlotMetadata: quota reserve failed tenant="
                        << tenant_id.value() << ", bytes=" << committed_charge
                        << ", err=" << reserve_result.error();
                }
            }
            ++imported;
        }
    }

    LOG(INFO) << "Imported slot metadata: slot=" << slot
              << ", objects=" << imported
              << ", source=" << export_payload.source_master_id;

    // 技术债 3.3：一次性交接完成后删除 slot_meta 键，避免二进制导出残留。
    // 删除失败仅告警不阻断——残留键会在下次 acquire 时被幂等重导。
    const std::string end = cvm::PrefixEnd(key);
    ErrorCode del_err =
        EtcdHelper::DeleteRange(key.data(), key.size(), end.data(), end.size());
    if (del_err != ErrorCode::OK) {
        LOG(WARNING) << "ImportSlotMetadata: delete slot_meta failed slot="
                     << slot << ", err=" << del_err;
    }
    return ErrorCode::OK;
}
#else
ErrorCode MasterService::StartSlotOwnerHeartbeat() { return ErrorCode::OK; }

ErrorCode MasterService::ExportSlotMetadata(uint16_t /*slot*/) {
    return ErrorCode::OK;
}

ErrorCode MasterService::ImportSlotMetadata(uint16_t /*slot*/) {
    return ErrorCode::OK;
}
#endif

#ifdef STORE_USE_ETCD
void MasterService::PublishSegmentOwnerForCvm(const Segment& segment) {
    if (!enable_ha_ || ha_backend_type_ != "etcd" || master_id_.empty() ||
        cluster_id_.empty()) {
        return;
    }
    cvm::SegmentOwner owner;
    owner.segment_id = UuidToString(segment.id);
    owner.owner_master_id = master_id_;
    owner.state = static_cast<int32_t>(cvm::SegmentOwnerState::kStable);

    // Bind to the supervisor-owned CvmController's lease so segment ownership
    // is auto-removed when this master dies; fall back to a persistent record
    // when no lease has been injected yet.
    ErrorCode err;
    if (cvm_lease_id_ != 0) {
        err = cvm::EtcdViewStore::SaveSegmentOwnerWithLease(
            cluster_id_, owner, cvm_lease_id_);
    } else {
        err = cvm::EtcdViewStore::SaveSegmentOwner(cluster_id_, owner);
    }
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "PublishSegmentOwnerForCvm SaveSegmentOwner failed: "
                        "segment="
                     << owner.segment_id << " err=" << err;
    }
}

void MasterService::RemoveSegmentOwnerForCvm(const UUID& segment_id) {
    if (!enable_ha_ || ha_backend_type_ != "etcd" || master_id_.empty() ||
        cluster_id_.empty()) {
        return;
    }
    const std::string id = UuidToString(segment_id);
    ErrorCode err = cvm::EtcdViewStore::DeleteSegmentOwner(cluster_id_, id);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "RemoveSegmentOwnerForCvm DeleteSegmentOwner failed: "
                        "segment="
                     << id << " err=" << err;
    }
}

std::vector<uint16_t> MasterService::ResolveOwnedSlotsForCvm() {
    // etcd 读取失败：sticky 策略——沿用上一轮成功解析的结果（可能为空）。
    // 绝不回退为全量接管：瞬时抖动引发的全量认领会与对端产生覆盖战
    // （双方反复重写对方 slot 记录，导致 slot 分布持续震荡不收敛）。
    std::vector<cvm::MasterRegistration> masters;
    ViewVersionId version;
    ErrorCode err =
        cvm::EtcdViewStore::LoadAllMasters(cluster_id_, masters, version);
    if (err != ErrorCode::OK) {
        std::lock_guard<std::mutex> lock(cvm_resolver_mutex_);
        LOG(WARNING) << "ResolveOwnedSlotsForCvm: LoadAllMasters failed: " << err
                     << ", keeping previous owned set (sticky), count="
                     << cvm_last_resolved_owned_slots_.size();
        return cvm_last_resolved_owned_slots_;
    }

    std::vector<std::string> ids;
    ids.reserve(masters.size());
    for (const auto& m : masters) {
        // Only serving primaries own slots. Standbys do not publish slot
        // ownership, so including them would strand part of the slot space.
        if (static_cast<cvm::MasterRole>(m.role) != cvm::MasterRole::kPrimary) {
            continue;
        }
        if (!m.master_id.empty()) {
            ids.push_back(m.master_id);
        }
    }
    std::sort(ids.begin(), ids.end());
    ids.erase(std::unique(ids.begin(), ids.end()), ids.end());

    // etcd 注册表是唯一事实源：本机必须已注册为 primary 才参与分配。
    // 不再无条件注入本机 master_id——否则任一节点视图缺失对端时都会以
    // n==1 身份全量接管，是 slot 覆盖战的直接根源。
    const bool self_registered =
        std::binary_search(ids.begin(), ids.end(), master_id_);
    std::vector<uint16_t> slots;
    if (self_registered) {
        // 技术债 3.1：一致性哈希环分配已提取到 cvm::ResolveOwnedSlotsOnRing
        // 纯函数（可单元测试），本机 owned slots 由 (ids, master_id_) 决定。
        slots = cvm::ResolveOwnedSlotsOnRing(ids, master_id_);
    } else {
        LOG(WARNING) << "ResolveOwnedSlotsForCvm: self not registered as "
                        "primary in etcd (masters="
                     << ids.size() << "), owning no slots this cycle";
    }

    {
        std::lock_guard<std::mutex> lock(cvm_resolver_mutex_);
        cvm_last_resolved_owned_slots_ = slots;
    }
    return slots;
}

std::optional<std::string> MasterService::ResolveSlotOwnerMasterId(
    uint16_t slot) const {
    std::vector<cvm::MasterRegistration> masters;
    ViewVersionId version;
    ErrorCode err =
        cvm::EtcdViewStore::LoadAllMasters(cluster_id_, masters, version);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "ResolveSlotOwnerMasterId: LoadAllMasters failed: "
                     << err << ", slot=" << slot;
        return std::nullopt;
    }

    std::vector<std::string> ids;
    ids.reserve(masters.size());
    for (const auto& m : masters) {
        if (static_cast<cvm::MasterRole>(m.role) != cvm::MasterRole::kPrimary) {
            continue;
        }
        if (!m.master_id.empty()) {
            ids.push_back(m.master_id);
        }
    }
    std::sort(ids.begin(), ids.end());
    ids.erase(std::unique(ids.begin(), ids.end()), ids.end());

    // 与 ResolveOwnedSlotsForCvm 保持一致：纯粹以 etcd 注册表计算环，
    // 不注入本机 master_id，保证所有权判定与认领发布使用同一个环。
    // 解析结果为空（注册表无 primary）时由调用方跳过转发。
    return cvm::ResolveSlotOwnerOnRing(ids, slot);
}
#else
void MasterService::PublishSegmentOwnerForCvm(const Segment&) {}
void MasterService::RemoveSegmentOwnerForCvm(const UUID&) {}
std::vector<uint16_t> MasterService::ResolveOwnedSlotsForCvm() { return {}; }
std::optional<std::string> MasterService::ResolveSlotOwnerMasterId(
    uint16_t /*slot*/) const {
    return std::nullopt;
}
#endif

void MasterService::UpdateOwnedSlots(const std::vector<uint16_t>& slots) {
    std::unique_lock<std::shared_mutex> lock(owned_slots_mutex_);
    owned_slot_lookup_.assign(cvm::kSlotCount, false);
    for (uint16_t slot : slots) {
        if (slot < cvm::kSlotCount) {
            owned_slot_lookup_[slot] = true;
        }
    }
    owned_slots_ready_ = true;
    // 一致性哈希环分配结果（3.1）：仅在数量变化（含首次解析）时打印，
    // 记录本机当前负责的 slot 规模；心跳周期内重复不变则静默。
    if (!owned_slot_count_logged_ || slots.size() != last_logged_owned_count_) {
        LOG(INFO) << "Owned slots updated (consistent-hash ring): count="
                  << slots.size() << "/" << cvm::kSlotCount
                  << (owned_slot_count_logged_ ? " (changed)" : "");
        owned_slot_count_logged_ = true;
        last_logged_owned_count_ = slots.size();
    }
    VLOG(1) << "UpdateOwnedSlots: " << slots.size() << " slots";
}

bool MasterService::OwnsSlot(uint16_t slot) const {
    std::shared_lock<std::shared_mutex> lock(owned_slots_mutex_);
    if (!owned_slots_ready_) {
        return true;  // partition 未启用或尚未解析过，放行
    }
    return slot < owned_slot_lookup_.size() && owned_slot_lookup_[slot];
}

MasterService::~MasterService() {
    if (ordered_oplog_writer_) {
        ordered_oplog_writer_->Stop();
    }

    // Stop the SlotOwnerHeartbeat thread before tearing down the rest.
    StopSlotOwnerHeartbeat();
    StopInterMasterRpc();

    // Stop and join the threads
    eviction_running_ = false;
    client_monitor_running_ = false;

    // Stop snapshot manager (non-blocking)
    if (snapshot_manager_) {
        snapshot_manager_->Stop();
    }

    task_cleanup_running_ = false;
    job_dispatch_running_ = false;
    http_metadata_cleanup_running_ = false;
    vchunk_reaper_running_ = false;
    graceful_unmount_scheduler_.Stop();
#ifdef USE_NOF
    nof_heartbeat_running_ = false;
#endif

    // Wake sleepers so join() doesn't block for long sleep intervals.
    task_cleanup_cv_.notify_all();
    http_metadata_cleanup_cv_.notify_all();
    vchunk_reaper_cv_.notify_all();

    if (eviction_thread_.joinable()) {
        eviction_thread_.join();
    }
    if (client_monitor_thread_.joinable()) {
        client_monitor_thread_.join();
    }
#ifdef USE_NOF
    if (nof_heartbeat_thread_.joinable()) {
        nof_heartbeat_thread_.join();
    }
#endif
    if (task_cleanup_thread_.joinable()) {
        task_cleanup_thread_.join();
    }
    if (http_metadata_cleanup_thread_.joinable()) {
        http_metadata_cleanup_thread_.join();
    }
    if (job_dispatch_thread_.joinable()) {
        job_dispatch_thread_.join();
    }
    if (vchunk_reaper_thread_.joinable()) {
        vchunk_reaper_thread_.join();
    }

    // Reset snapshot manager after all other threads have joined
    // This triggers the destructor which joins the snapshot thread
    if (snapshot_manager_) {
        snapshot_manager_.reset();
    }
    for (const auto& [segment, bytes] : standby_accounted_memory_bytes_) {
        MasterMetricManager::instance().dec_allocated_mem_size(
            segment, static_cast<int64_t>(bytes));
    }

    // Segments still mounted here never went through CommitUnmountSegment;
    // release their capacity contribution so the process-lifetime
    // MasterMetricManager stays consistent when the next leadership term
    // constructs a fresh MasterService and the clients remount.
    segment_manager_.releaseCapacityMetrics();
}

ErrorCode MasterService::SetBatchOpLogBackendForTesting(
    std::shared_ptr<HaKvBackend> backend) {
    return InitializeBatchOpLogWriter(std::move(backend));
}

void MasterService::RunBatchEvictForTesting(double evict_ratio_target,
                                            double evict_ratio_lowerbound) {
    BatchEvict(evict_ratio_target, evict_ratio_lowerbound);
}

void MasterService::RunNoFBatchEvictForTesting(double evict_ratio_target,
                                               double evict_ratio_lowerbound) {
    NoFBatchEvict(evict_ratio_target, evict_ratio_lowerbound);
}

void MasterService::SetNoFProbeFnForTesting(NoFProbeFn fn) {
#ifdef USE_NOF
    std::lock_guard<std::mutex> lock(nof_probe_fn_mutex_);
    if (fn) {
        nof_probe_fn_ = std::move(fn);
        return;
    }
    nof_probe_fn_ = [](const std::string& te_endpoint, uint32_t timeout_ms,
                       std::string* error_reason) {
        return SpdkWrapper::GetInstance().ProbeNofSegment(
            te_endpoint, timeout_ms, error_reason);
    };
#else
    (void)fn;
#endif
}

size_t MasterService::GetMountedNoFSegmentCountForTesting() {
    std::vector<MountedNoFSegmentSnapshot> mounted_segments;
    nof_segment_manager_.GetMountedSegmentsSnapshot(mounted_segments);
    return mounted_segments.size();
}

bool MasterService::IsNoFSegmentMountedForTesting(const UUID& segment_id) {
    std::vector<MountedNoFSegmentSnapshot> mounted_segments;
    nof_segment_manager_.GetMountedSegmentsSnapshot(mounted_segments);
    return std::any_of(
        mounted_segments.begin(), mounted_segments.end(),
        [&segment_id](const MountedNoFSegmentSnapshot& snapshot) {
            return snapshot.segment_id == segment_id &&
                   snapshot.status == SegmentStatus::OK;
        });
}

std::optional<uint32_t> MasterService::GetNoFHeartbeatFailureCountForTesting(
    const UUID& segment_id) {
    std::lock_guard<std::mutex> lock(nof_heartbeat_mutex_);
    auto it = nof_heartbeat_states_.find(segment_id);
    if (it == nof_heartbeat_states_.end()) {
        return std::nullopt;
    }
    return it->second.consecutive_failures;
}

bool MasterService::IsTenantQuotaEnabled() const {
    return enable_multi_tenants_;
}

std::vector<TenantQuotaSnapshot> MasterService::ListTenantQuotaSnapshots()
    const {
    return tenant_quota_table_.ListTenantSnapshots();
}

std::optional<TenantQuotaSnapshot> MasterService::GetTenantQuotaSnapshot(
    const TenantId& tenant_id) const {
    assert(tenant_id.IsValid());
    return tenant_quota_table_.GetTenantSnapshot(tenant_id);
}

tl::expected<TenantQuotaSnapshot, ErrorCode>
MasterService::UpsertTenantQuotaPolicy(const TenantId& tenant_id,
                                       uint64_t requested_quota_bytes) {
    assert(tenant_id.IsValid());
    if (!enable_multi_tenants_) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    if (requested_quota_bytes == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::lock_guard<std::mutex> policy_lock(tenant_quota_policy_mutex_);
    auto policy = BuildTenantQuotaPolicySnapshot();
    policy.tenant_quotas[tenant_id.value()] = requested_quota_bytes;
    auto save_result = tenant_quota_policy_store_->Save(policy);
    if (!save_result) {
        LOG(ERROR) << "failed to save tenant quota policy: "
                   << save_result.error();
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }
    ApplyTenantQuotaPolicies(policy);
    auto result_snapshot = GetTenantQuotaSnapshot(tenant_id);
    if (!result_snapshot.has_value()) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return result_snapshot.value();
}

tl::expected<std::optional<TenantQuotaSnapshot>, ErrorCode>
MasterService::DeleteTenantQuotaPolicy(const TenantId& tenant_id) {
    assert(tenant_id.IsValid());
    if (!enable_multi_tenants_) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }

    std::lock_guard<std::mutex> policy_lock(tenant_quota_policy_mutex_);
    auto policy = BuildTenantQuotaPolicySnapshot();
    auto policy_it = policy.tenant_quotas.find(tenant_id.value());
    if (policy_it == policy.tenant_quotas.end()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    const uint64_t requested_quota_bytes = policy_it->second;

    auto restore_policy = [&] {
        std::lock_guard<std::mutex> recompute_lock(
            tenant_quota_recompute_mutex_);
        const uint64_t capacity = GetTenantQuotaAllocatableCapacityBytes();
        auto result = tenant_quota_table_.UpsertTenantPolicy(
            tenant_id, requested_quota_bytes, capacity);
        if (!result) {
            LOG(ERROR) << "failed to restore tenant quota policy tenant="
                       << tenant_id.value();
        }
    };

    auto disable_result =
        tenant_quota_table_.DisableTenantPolicyIfEmpty(tenant_id);
    if (!disable_result) {
        return tl::make_unexpected(disable_result.error() ==
                                           TenantQuotaError::kTenantNotEmpty
                                       ? ErrorCode::TENANT_NOT_EMPTY
                                       : ErrorCode::OBJECT_NOT_FOUND);
    }

    auto post_mark_snapshot = GetTenantQuotaSnapshot(tenant_id);
    if (TenantHasObjects(tenant_id) ||
        (post_mark_snapshot.has_value() &&
         (post_mark_snapshot->used_bytes != 0 ||
          post_mark_snapshot->reserved_bytes != 0 ||
          post_mark_snapshot->committed_count != 0 ||
          post_mark_snapshot->metadata_object_count != 0))) {
        restore_policy();
        return tl::make_unexpected(ErrorCode::TENANT_NOT_EMPTY);
    }

    policy.tenant_quotas.erase(policy_it);
    auto save_result = tenant_quota_policy_store_->Save(policy);
    if (!save_result) {
        restore_policy();
        LOG(ERROR) << "failed to save tenant quota policy: "
                   << save_result.error();
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }
    ApplyTenantQuotaPolicies(policy);
    return GetTenantQuotaSnapshot(tenant_id);
}

auto MasterService::MountSegment(const Segment& segment, const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    ErrorCode mount_result = ErrorCode::OK;
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();

        // Tell the client monitor thread to start timing for this client. To
        // avoid the following undesired situations, this message must be sent
        // after locking the segment mutex and before the mounting operation
        // completes:
        // 1. Sending the message before the lock: the client expires and
        // unmouting invokes before this mounting are completed, which prevents
        // this segment being able to be unmounted forever;
        // 2. Sending the message after mounting the segment: After mounting
        // this segment, when trying to push id to the queue, the queue is
        // already full. However, at this point, the message must be sent,
        // otherwise this client cannot be monitored and expired.
        {
            PodUUID pod_client_id;
            pod_client_id.first = client_id.first;
            pod_client_id.second = client_id.second;
            if (!client_ping_queue_.push(pod_client_id)) {
                LOG(ERROR) << "segment_name=" << segment.name
                           << ", error=client_ping_queue_full";
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
        }

        LOG(INFO) << "client_id=" << client_id
                  << ", action=mount_segment, segment_name=" << segment.name;

        auto err = segment_access.MountSegment(segment, client_id);
        if (err == ErrorCode::SEGMENT_ALREADY_EXISTS) {
            // Return OK because this is an idempotent operation
            mount_result = err;
        } else if (err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }
    }

    if (enable_oplog_ && ordered_oplog_writer_) {
        SegmentMountOp op;
        op.segment_name = segment.name;
        op.transport_endpoint = segment.te_endpoint;
        op.capacity = segment.size;
        op.is_memory_segment = true;
        op.file_path.clear();
        auto bytes = struct_pack::serialize(op);
        PersistSegmentOpForHAOrEnqueue("MountSegment", OpType::SEGMENT_MOUNT,
                                       segment.te_endpoint,
                                       std::string(bytes.begin(), bytes.end()));
    }
    UpdateClientHostId(client_id, segment.host_id);
    if (mount_result == ErrorCode::OK) {
        RecomputeTenantEffectiveQuotas();
    }
    PublishSegmentOwnerForCvm(segment);
    return {};
}

auto MasterService::MountNoFSegment(const NoFSegment& segment,
                                    const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
#ifndef USE_NOF
    LOG(ERROR) << "client_id=" << client_id << ", segment_name=" << segment.name
               << ", error=nof_pool_disabled";
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
#else
    ScopedNoFSegmentAccess nof_segment_access =
        nof_segment_manager_.getNoFSegmentAccess();

    LOG(INFO) << "NoF segment mount: "
              << "client_id=" << client_id
              << ", action=mount_segment, segment_name=" << segment.name;

    auto err = nof_segment_access.MountSegment(segment, client_id);
    if (err == ErrorCode::SEGMENT_ALREADY_EXISTS) {
        // Return OK because this is an idempotent operation
        return {};
    } else if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return {};
#endif
}

ErrorCode MasterService::ValidateStandbyRemountSegment(
    const Segment& segment) const {
    const StandbySegmentInfo* match = nullptr;
    for (const auto& standby : standby_memory_segments_) {
        if (standby.transport_endpoint == segment.te_endpoint ||
            standby.segment_name == segment.name) {
            if (match != nullptr && match != &standby) {
                return ErrorCode::INVALID_PARAMS;
            }
            match = &standby;
        }
    }
    if (match != nullptr && segment.protocol == "cxl") {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    }
    if (match != nullptr && (match->segment_name != segment.name ||
                             match->transport_endpoint != segment.te_endpoint ||
                             match->capacity != segment.size)) {
        return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::OK;
}

auto MasterService::ReMountSegment(const std::vector<Segment>& segments,
                                   const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    std::unique_lock<std::shared_mutex> snapshot_lock(snapshot_mutex_);
    {
        std::unique_lock<std::shared_mutex> lock(client_mutex_);
        for (const auto& segment : segments) {
            if (!segment.host_id.empty()) {
                client_host_id_[client_id] = segment.host_id;
                break;
            }
        }
        {
            auto segment_access = segment_manager_.getSegmentAccess();
            for (const auto& segment : segments) {
                auto standby_validation =
                    ValidateStandbyRemountSegment(segment);
                if (standby_validation != ErrorCode::OK) {
                    return tl::make_unexpected(standby_validation);
                }
                auto validation =
                    segment_access.ValidateRemountSegment(segment, client_id);
                if (validation != ErrorCode::OK) {
                    return tl::make_unexpected(validation);
                }
            }
        }
        if (ok_client_.contains(client_id)) {
            LOG(WARNING) << "client_id=" << client_id
                         << ", warn=client_already_remounted";
            // Return OK because this is an idempotent operation
            return {};
        }

        {
            ScopedSegmentAccess segment_access =
                segment_manager_.getSegmentAccess();
            std::vector<bool> segment_existed(segments.size());
            for (size_t i = 0; i < segments.size(); ++i) {
                segment_existed[i] =
                    segment_access.GetAllocator(segments[i].id) != nullptr;
            }
            auto rollback_new_segments = [&] {
                for (size_t i = 0; i < segments.size(); ++i) {
                    if (segment_existed[i] ||
                        !segment_access.GetAllocator(segments[i].id)) {
                        continue;
                    }
                    size_t capacity = 0;
                    if (segment_access.PrepareUnmountSegment(
                            segments[i].id, capacity) != ErrorCode::OK) {
                        LOG(ERROR) << "segment_name=" << segments[i].name
                                   << ", error=remount_rollback_prepare_failed";
                        continue;
                    }
                    if (segment_access.CommitUnmountSegment(
                            segments[i].id, client_id, capacity) !=
                        ErrorCode::OK) {
                        LOG(ERROR) << "segment_name=" << segments[i].name
                                   << ", error=remount_rollback_commit_failed";
                    }
                }
            };
            auto fail_remount =
                [&](ErrorCode error) -> tl::expected<void, ErrorCode> {
                rollback_new_segments();
                return tl::make_unexpected(error);
            };

            // Tell the client monitor thread to start timing for this client.
            // To avoid the following undesired situations, this message must be
            // sent after locking the segment mutex or client mutex and before
            // the remounting operation completes:
            // 1. Sending the message before the lock: the client expires and
            // unmouting invokes before this remounting are completed, which
            // prevents this segment being able to be unmounted forever;
            // 2. Sending the message after remounting the segments: After
            // remounting these segments, when trying to push id to the queue,
            // the queue is already full. However, at this point, the message
            // must be sent, otherwise this client cannot be monitored and
            // expired.
            PodUUID pod_client_id;
            pod_client_id.first = client_id.first;
            pod_client_id.second = client_id.second;
            if (!client_ping_queue_.push(pod_client_id)) {
                LOG(ERROR) << "client_id=" << client_id
                           << ", error=client_ping_queue_full";
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }

            ErrorCode err = segment_access.ReMountSegment(segments, client_id);
            if (err != ErrorCode::OK) {
                return fail_remount(err);
            }

            struct SegmentRestore {
                Segment segment;
                std::shared_ptr<BufferAllocatorBase> old_allocator;
                std::shared_ptr<BufferAllocatorBase> restored_allocator;
                std::vector<Replica*> replicas;
                std::vector<AllocatedBuffer::Descriptor> descriptors;
                std::vector<std::unique_ptr<AllocatedBuffer>> buffers;
                uint64_t imported_size{0};
            };
            std::vector<SegmentRestore> restores;
            restores.reserve(segments.size());
            for (const auto& segment : segments) {
                auto allocator = segment_access.GetAllocator(segment.id);
                Segment authoritative;
                if (!allocator ||
                    !segment_access.GetSegment(segment.id, authoritative)) {
                    return fail_remount(ErrorCode::INTERNAL_ERROR);
                }
                restores.push_back({std::move(authoritative),
                                    std::move(allocator),
                                    nullptr,
                                    {},
                                    {},
                                    {},
                                    0});
            }

            bool ambiguous_endpoint = false;
            bool unsupported_cxl = false;
            // Track pre-remount readability so the first successful recovery
            // starts a fresh lease without extending already-readable objects.
            std::unordered_map<ObjectMetadata*, bool> restored_objects;
            for (size_t shard_index = 0; shard_index < kNumShards;
                 ++shard_index) {
                MetadataShardAccessorRW shard(this, shard_index);
                for (auto& [tenant_id, tenant] : shard->tenants) {
                    (void)tenant_id;
                    for (auto& [key, metadata] : tenant.metadata) {
                        (void)key;
                        metadata.VisitReplicas(
                            [](const Replica& replica) {
                                return replica.is_memory_replica() &&
                                       replica.status() !=
                                           ReplicaStatus::REMOVED &&
                                       replica.status() !=
                                           ReplicaStatus::FAILED &&
                                       !replica.has_invalid_mem_handle();
                            },
                            [&](Replica& replica) {
                                auto descriptor = replica.get_descriptor()
                                                      .get_memory_descriptor()
                                                      .buffer_descriptor;
                                SegmentRestore* match = nullptr;
                                for (auto& restore : restores) {
                                    if (descriptor.transport_endpoint_ ==
                                            restore.segment.te_endpoint ||
                                        descriptor.transport_endpoint_ ==
                                            restore.segment.name) {
                                        // When multiple segments share the
                                        // same endpoint (e.g. UB per-NUMA
                                        // segments), disambiguate by
                                        // checking whether the replica's
                                        // buffer address falls within this
                                        // segment's virtual address range
                                        // [base, base+size).  Each
                                        // per-NUMA segment occupies a
                                        // contiguous and non-overlapping
                                        // range, so at most one segment
                                        // matches.
                                        if (descriptor.buffer_address_ <
                                                restore.segment.base ||
                                            descriptor.buffer_address_ >=
                                                restore.segment.base +
                                                    restore.segment.size) {
                                            continue;
                                        }
                                        if (match != nullptr) {
                                            ambiguous_endpoint = true;
                                            return;
                                        }
                                        match = &restore;
                                    }
                                }
                                if (match != nullptr) {
                                    if (descriptor.protocol_ == "cxl") {
                                        unsupported_cxl = true;
                                        return;
                                    }
                                    descriptor.transport_endpoint_ =
                                        match->segment.te_endpoint;
                                    if (!restored_objects.contains(&metadata)) {
                                        restored_objects.emplace(
                                            &metadata,
                                            metadata.HasReplica(
                                                [this](
                                                    const Replica& candidate) {
                                                    return candidate
                                                               .is_memory_replica() &&
                                                           IsReplicaReadable(
                                                               candidate);
                                                }));
                                    }
                                    match->replicas.push_back(&replica);
                                    match->descriptors.push_back(descriptor);
                                }
                            });
                    }
                }
            }
            if (ambiguous_endpoint) {
                return fail_remount(ErrorCode::INVALID_PARAMS);
            }
            if (unsupported_cxl) {
                return fail_remount(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
            }

            for (auto& restore : restores) {
                if (restore.descriptors.empty()) {
                    continue;
                }
                if (std::dynamic_pointer_cast<OffsetBufferAllocator>(
                        restore.old_allocator)) {
                    auto restored = RestoreOffsetBufferAllocator(
                        restore.segment.name, restore.segment.base,
                        restore.segment.size, restore.segment.te_endpoint,
                        restore.descriptors);
                    if (!restored) {
                        return fail_remount(ErrorCode::INVALID_PARAMS);
                    }
                    restore.restored_allocator = std::move(restored->allocator);
                    restore.buffers = std::move(restored->buffers);
                } else if (std::dynamic_pointer_cast<CachelibBufferAllocator>(
                               restore.old_allocator)) {
                    auto restored = RestoreCachelibBufferAllocator(
                        restore.segment.name, restore.segment.base,
                        restore.segment.size, restore.segment.te_endpoint,
                        restore.descriptors);
                    if (!restored) {
                        return fail_remount(ErrorCode::INVALID_PARAMS);
                    }
                    restore.restored_allocator = std::move(restored->allocator);
                    restore.buffers = std::move(restored->buffers);
                } else {
                    return fail_remount(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
                }
            }

            std::vector<ScopedSegmentAccess::AllocatorReplacement>
                allocator_replacements;
            for (auto& restore : restores) {
                if (restore.restored_allocator) {
                    if (restore.buffers.size() != restore.replicas.size() ||
                        std::any_of(
                            restore.buffers.begin(), restore.buffers.end(),
                            [](const auto& buffer) { return !buffer; })) {
                        return fail_remount(ErrorCode::INTERNAL_ERROR);
                    }
                    restore.imported_size = std::accumulate(
                        restore.descriptors.begin(), restore.descriptors.end(),
                        uint64_t{0}, [](uint64_t sum, const auto& descriptor) {
                            return sum + descriptor.size_;
                        });
                    auto accounted = standby_accounted_memory_bytes_.find(
                        restore.segment.name);
                    if (accounted == standby_accounted_memory_bytes_.end() ||
                        accounted->second < restore.imported_size) {
                        return fail_remount(ErrorCode::INTERNAL_ERROR);
                    }
                    allocator_replacements.push_back(
                        {restore.segment.id, restore.old_allocator,
                         restore.restored_allocator});
                }
            }
            if (!segment_access.ReplaceAllocators(allocator_replacements)) {
                return fail_remount(ErrorCode::INTERNAL_ERROR);
            }
            for (auto& restore : restores) {
                if (restore.imported_size != 0) {
                    MasterMetricManager::instance().dec_allocated_mem_size(
                        restore.segment.name,
                        static_cast<int64_t>(restore.imported_size));
                    auto accounted = standby_accounted_memory_bytes_.find(
                        restore.segment.name);
                    accounted->second -= restore.imported_size;
                    if (accounted->second == 0) {
                        standby_accounted_memory_bytes_.erase(accounted);
                    }
                }
                for (size_t i = 0; i < restore.replicas.size(); ++i) {
                    (void)restore.replicas[i]->replace_memory_buffer(
                        std::move(restore.buffers[i]));
                }
                invalid_replica_endpoints_.erase(restore.segment.te_endpoint);
                invalid_replica_endpoints_.erase(restore.segment.name);
                standby_allocator_keepalive_.erase(restore.segment.te_endpoint);
                standby_allocator_keepalive_.erase(restore.segment.name);
            }
            for (const auto& [metadata, was_readable] : restored_objects) {
                if (!was_readable &&
                    metadata->HasReplica([this](const Replica& replica) {
                        return replica.is_memory_replica() &&
                               IsReplicaReadable(replica);
                    })) {
                    metadata->GrantLease(default_kv_lease_ttl_,
                                         default_kv_soft_pin_ttl_);
                }
            }
        }

        // Change the client status to OK
        ok_client_.insert(client_id);
        MasterMetricManager::instance().inc_active_clients();
    }

    if (enable_oplog_ && ordered_oplog_writer_) {
        for (const auto& seg : segments) {
            SegmentMountOp op;
            op.segment_name = seg.name;
            op.transport_endpoint = seg.te_endpoint;
            op.capacity = seg.size;
            op.is_memory_segment = true;
            op.file_path.clear();
            auto bytes = struct_pack::serialize(op);
            PersistSegmentOpForHAOrEnqueue(
                "ReMountSegment", OpType::SEGMENT_MOUNT, seg.name,
                std::string(bytes.begin(), bytes.end()));
        }
    }
    RecomputeTenantEffectiveQuotas();

    for (const auto& seg : segments) {
        PublishSegmentOwnerForCvm(seg);
    }

    return {};
}

auto MasterService::ReMountNoFSegment(const std::vector<NoFSegment>& segments,
                                      const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
#ifndef USE_NOF
    LOG(ERROR) << "client_id=" << client_id
               << ", segments_count=" << segments.size()
               << ", error=nof_pool_disabled";
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
#else
    ScopedNoFSegmentAccess nof_segment_access =
        nof_segment_manager_.getNoFSegmentAccess();
    ErrorCode err = nof_segment_access.ReMountSegment(segments, client_id);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return {};
#endif
}

std::unordered_set<UUID, boost::hash<UUID>>
MasterService::getAliveClientsSnapshot() const {
    std::shared_lock<std::shared_mutex> lock(client_mutex_);
    return ok_client_;
}

void MasterService::UpdateClientHostId(const UUID& client_id,
                                       const std::string& host_id) {
    if (host_id.empty()) {
        return;
    }
    {
        std::shared_lock<std::shared_mutex> lock(client_mutex_);
        auto it = client_host_id_.find(client_id);
        if (it != client_host_id_.end() && it->second == host_id) {
            return;
        }
    }

    std::unique_lock<std::shared_mutex> lock(client_mutex_);
    auto it = client_host_id_.find(client_id);
    if (it == client_host_id_.end() || it->second != host_id) {
        client_host_id_[client_id] = host_id;
    }
}

std::string MasterService::GetClientHostId(const UUID& client_id) const {
    std::shared_lock<std::shared_mutex> lock(client_mutex_);
    auto it = client_host_id_.find(client_id);
    return it == client_host_id_.end() ? std::string() : it->second;
}

size_t MasterService::getMetadataShardIndex(const TenantId& tenant_id,
                                            const std::string& key) const {
    std::shared_lock<std::shared_mutex> lock(group_routing_mutex_);
    auto it = object_group_ids_.find(tenant_id.MakeScopedKey(key));
    if (it == object_group_ids_.end()) {
        return getShardIndex(tenant_id, key);
    }
    return getShardIndex(it->second);
}

const TenantId& MasterService::ResolveRequestTenantId(
    const TenantId& tenant_id) const {
    assert(tenant_id.IsValid());
    if (!enable_multi_tenants_) {
        return TenantId::Default();
    }
    return tenant_id;
}

MasterService::ObjectIdentity MasterService::MakeObjectIdentityForRequest(
    const std::string& user_key, const TenantId& tenant_id) const {
    return {ResolveRequestTenantId(tenant_id), user_key};
}

bool MasterService::IsTenantRegistered(const TenantId& tenant_id) const {
    if (!enable_multi_tenants_) {
        return true;
    }
    return tenant_quota_table_.IsTenantRegistered(tenant_id);
}

tl::expected<TenantId, ErrorCode> MasterService::ResolveTenantIdForWrite(
    const TenantId& tenant_id) const {
    assert(tenant_id.IsValid());
    if (!enable_multi_tenants_) {
        return TenantId::Default();
    }
    std::lock_guard<std::mutex> policy_lock(tenant_quota_policy_mutex_);
    return ResolveTenantIdForWriteLocked(tenant_id);
}

tl::expected<TenantId, ErrorCode> MasterService::ResolveTenantIdForWriteLocked(
    const TenantId& tenant_id) const {
    assert(tenant_id.IsValid());
    if (!enable_multi_tenants_) {
        return TenantId::Default();
    }
    if (!IsTenantRegistered(tenant_id)) {
        return tl::make_unexpected(ErrorCode::TENANT_NOT_REGISTERED);
    }
    return tenant_id;
}

bool MasterService::TenantHasObjects(const TenantId& tenant_id) const {
    for (size_t i = 0; i < kNumShards; ++i) {
        MetadataShardAccessorRO shard(this, i);
        auto tenant_it = shard->tenants.find(tenant_id);
        if (tenant_it != shard->tenants.end() &&
            !tenant_it->second.metadata.empty()) {
            return true;
        }
    }
    return false;
}

TenantQuotaPolicySnapshot MasterService::BuildTenantQuotaPolicySnapshot()
    const {
    TenantQuotaPolicySnapshot snapshot;
    for (const auto& [tenant_id, requested_quota_bytes] :
         tenant_quota_table_.GetTenantPolicies()) {
        snapshot.tenant_quotas.emplace(tenant_id.value(),
                                       requested_quota_bytes);
    }
    return snapshot;
}

void MasterService::ApplyTenantQuotaPolicies(
    const TenantQuotaPolicySnapshot& snapshot) {
    TenantQuotaPolicyMap policies;
    for (const auto& [tenant_id, requested_quota_bytes] :
         snapshot.tenant_quotas) {
        policies.emplace(TenantId(tenant_id), requested_quota_bytes);
    }
    std::lock_guard<std::mutex> recompute_lock(tenant_quota_recompute_mutex_);
    const uint64_t capacity = GetTenantQuotaAllocatableCapacityBytes();
    tenant_quota_table_.ApplyTenantPolicies(policies, capacity);
}

void MasterService::LoadTenantQuotaPoliciesFromStoreOrThrow() {
    if (!enable_multi_tenants_) {
        return;
    }
    if (!tenant_quota_policy_store_) {
        throw std::runtime_error(
            "tenant quota policy store is not initialized");
    }
    std::lock_guard<std::mutex> policy_lock(tenant_quota_policy_mutex_);
    auto snapshot = tenant_quota_policy_store_->Load();
    if (!snapshot) {
        throw std::runtime_error("failed to load tenant quota policy: " +
                                 snapshot.error());
    }
    ApplyTenantQuotaPolicies(snapshot.value());
}

uint64_t MasterService::CompletedMemoryQuotaCharge(
    const ObjectMetadata& metadata) const {
    return static_cast<uint64_t>(metadata.size) *
           metadata.CountReplicas([](const Replica& replica) {
               return replica.is_memory_replica() && replica.is_completed();
           });
}

uint64_t MasterService::RequestedMemoryQuotaCharge(
    uint64_t value_length, const ReplicateConfig& config) const {
    const unsigned __int128 charge =
        static_cast<unsigned __int128>(value_length) * config.replica_num;
    if (charge > std::numeric_limits<uint64_t>::max()) {
        return std::numeric_limits<uint64_t>::max();
    }
    return static_cast<uint64_t>(charge);
}

bool MasterService::ShouldProtectZeroChargeMetadataCreate(
    uint64_t requested_quota_charge) const {
    return enable_multi_tenants_ && requested_quota_charge == 0;
}

uint64_t MasterService::GetTenantQuotaAllocatableCapacityBytes() {
    uint64_t capacity = 0;
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    std::vector<std::pair<Segment, UUID>> segments;
    if (segment_access.GetAllSegments(segments) != ErrorCode::OK) {
        return 0;
    }
    for (const auto& [segment, _] : segments) {
        if (capacity > std::numeric_limits<uint64_t>::max() - segment.size) {
            return std::numeric_limits<uint64_t>::max();
        }
        capacity += segment.size;
    }
    return capacity;
}

void MasterService::RecomputeTenantEffectiveQuotas() {
    if (!enable_multi_tenants_) {
        return;
    }
    std::lock_guard<std::mutex> recompute_lock(tenant_quota_recompute_mutex_);
    const uint64_t capacity = GetTenantQuotaAllocatableCapacityBytes();
    tenant_quota_table_.RecomputeEffectiveQuotas(capacity);
}

tl::expected<void, ErrorCode> MasterService::ReserveTenantQuota(
    const TenantId& tenant_id, uint64_t bytes) {
    if (!enable_multi_tenants_) {
        return {};
    }
    auto result = tenant_quota_table_.Reserve(tenant_id, bytes);
    if (result) {
        return {};
    }
    return tl::make_unexpected(
        result.error() == TenantQuotaError::kTenantNotRegistered
            ? ErrorCode::TENANT_NOT_REGISTERED
        : result.error() == TenantQuotaError::kQuotaExceeded
            ? ErrorCode::TENANT_QUOTA_EXCEEDED
            : ErrorCode::INTERNAL_ERROR);
}

void MasterService::CommitTenantQuota(const TenantId& tenant_id,
                                      uint64_t bytes) {
    if (!enable_multi_tenants_ || bytes == 0) {
        return;
    }
    if (!tenant_quota_table_.Commit(tenant_id, bytes)) {
        LOG(ERROR) << "tenant quota commit mismatch tenant="
                   << tenant_id.value() << ", bytes=" << bytes;
    }
}

void MasterService::AbortTenantQuota(const TenantId& tenant_id,
                                     uint64_t bytes) {
    if (!enable_multi_tenants_ || bytes == 0) {
        return;
    }
    if (!tenant_quota_table_.Abort(tenant_id, bytes)) {
        LOG(ERROR) << "tenant quota abort mismatch tenant=" << tenant_id.value()
                   << ", bytes=" << bytes;
    }
}

void MasterService::ReleaseTenantQuota(const TenantId& tenant_id,
                                       uint64_t bytes) {
    if (!enable_multi_tenants_ || bytes == 0) {
        return;
    }
    if (!tenant_quota_table_.Release(tenant_id, bytes)) {
        LOG(ERROR) << "tenant quota release mismatch tenant="
                   << tenant_id.value() << ", bytes=" << bytes;
    }
}

void MasterService::ReleaseTenantQuotaPartial(const TenantId& tenant_id,
                                              uint64_t bytes) {
    if (!enable_multi_tenants_ || bytes == 0) {
        return;
    }
    if (!tenant_quota_table_.ReleasePartial(tenant_id, bytes)) {
        LOG(ERROR) << "tenant quota partial release mismatch tenant="
                   << tenant_id.value() << ", bytes=" << bytes;
    }
}

void MasterService::CommitAdditionalTenantQuota(const TenantId& tenant_id,
                                                uint64_t bytes) {
    if (!enable_multi_tenants_ || bytes == 0) {
        return;
    }
    if (!tenant_quota_table_.CommitAdditional(tenant_id, bytes)) {
        LOG(ERROR) << "tenant quota additional commit mismatch tenant="
                   << tenant_id.value() << ", bytes=" << bytes;
    }
}

void MasterService::IncrementTenantMetadataObjectCount(
    const TenantId& tenant_id) {
    if (!enable_multi_tenants_) {
        return;
    }
    tenant_quota_table_.IncrementMetadataObjectCount(tenant_id);
}

void MasterService::DecrementTenantMetadataObjectCount(
    const TenantId& tenant_id) {
    if (!enable_multi_tenants_) {
        return;
    }
    if (!tenant_quota_table_.DecrementMetadataObjectCount(tenant_id)) {
        LOG(WARNING) << "tenant metadata object count decrement mismatch "
                     << "tenant=" << tenant_id.value();
    }
}

void MasterService::ReleaseCommittedQuotaCharge(ObjectMetadata& metadata,
                                                uint64_t bytes) {
    if (!enable_multi_tenants_ || bytes == 0) {
        return;
    }
    const uint64_t release_bytes =
        std::min(bytes, metadata.committed_quota_charge_bytes);
    if (release_bytes == metadata.committed_quota_charge_bytes) {
        ReleaseTenantQuota(metadata.tenant_id, release_bytes);
    } else {
        ReleaseTenantQuotaPartial(metadata.tenant_id, release_bytes);
    }
    metadata.committed_quota_charge_bytes -= release_bytes;
}

void MasterService::RebuildTenantQuotaUsageFromMetadata() {
    if (!enable_multi_tenants_) {
        return;
    }

    TenantQuotaUsageMap usage;
    for (size_t i = 0; i < kNumShards; ++i) {
        MetadataShardAccessorRW shard(this, i);
        for (auto& [tenant_id, tenant_state] : shard->tenants) {
            for (auto& [_, metadata] : tenant_state.metadata) {
                auto& tenant_usage = usage[tenant_id];
                ++tenant_usage.metadata_object_count;
                const uint64_t charge = CompletedMemoryQuotaCharge(metadata);
                metadata.reserved_quota_charge_bytes = 0;
                metadata.committed_quota_charge_bytes = charge;
                metadata.pending_replaced_quota_charge_bytes = 0;
                if (charge == 0) {
                    continue;
                }
                tenant_usage.used_bytes += charge;
                ++tenant_usage.committed_count;
            }
        }
    }

    for (const auto& [tenant_id, _] : usage) {
        if (!tenant_quota_table_.IsTenantRegistered(tenant_id)) {
            LOG(WARNING)
                << "tenant " << tenant_id.value()
                << " exists in metadata but has no connector quota policy; "
                   "creating orphan quota state";
        }
    }
    std::lock_guard<std::mutex> recompute_lock(tenant_quota_recompute_mutex_);
    const uint64_t capacity = GetTenantQuotaAllocatableCapacityBytes();
    tenant_quota_table_.RebuildUsage(usage, capacity);
}

std::optional<std::string> MasterService::GetGroupRoute(
    const TenantId& tenant_id, const std::string& key) const {
    std::shared_lock<std::shared_mutex> lock(group_routing_mutex_);
    auto it = object_group_ids_.find(tenant_id.MakeScopedKey(key));
    if (it == object_group_ids_.end()) {
        return std::nullopt;
    }
    return it->second;
}

MasterService::ObjectOperationLock MasterService::AcquireObjectOperationLock(
    const TenantId& tenant_id, const std::string& key) {
    const auto scoped_key = tenant_id.MakeScopedKey(key);
    const auto stripe_idx =
        std::hash<std::string>{}(scoped_key) % kObjectOperationLockStripes;
    return {std::unique_lock<std::mutex>(object_operation_locks_[stripe_idx])};
}

void MasterService::RegisterGroupMember(TenantState& tenant_state,
                                        const TenantId& tenant_id,
                                        const std::string& key,
                                        const std::string& group_id) {
    if (group_id.empty()) {
        return;
    }
    std::unique_lock<std::shared_mutex> lock(group_routing_mutex_);
    object_group_ids_[tenant_id.MakeScopedKey(key)] = group_id;
    groups_needing_lease_refresh_.insert(tenant_id.MakeScopedKey(group_id));
    tenant_state.group_members[group_id].insert(key);
}

void MasterService::UnregisterGroupMember(TenantState& tenant_state,
                                          const TenantId& tenant_id,
                                          const std::string& key,
                                          const std::string& group_id) {
    if (group_id.empty()) {
        return;
    }
    bool group_empty = false;
    auto group_it = tenant_state.group_members.find(group_id);
    if (group_it != tenant_state.group_members.end()) {
        group_it->second.erase(key);
        if (group_it->second.empty()) {
            tenant_state.group_members.erase(group_it);
            group_empty = true;
        }
    }
    std::unique_lock<std::shared_mutex> lock(group_routing_mutex_);
    auto route_it = object_group_ids_.find(tenant_id.MakeScopedKey(key));
    if (route_it != object_group_ids_.end() && route_it->second == group_id) {
        object_group_ids_.erase(route_it);
    }
    if (group_empty) {
        groups_needing_lease_refresh_.erase(tenant_id.MakeScopedKey(group_id));
    }
}

bool MasterService::HasCompletedMemoryCacheReplica(
    const ObjectMetadata& metadata) {
    return metadata.HasReplica([](const Replica& replica) {
        return replica.is_memory_replica() && replica.is_completed();
    });
}

bool MasterService::HasCompletedDiskCacheReplica(
    const ObjectMetadata& metadata) {
    return metadata.HasReplica([](const Replica& replica) {
        return (replica.is_disk_replica() || replica.is_local_disk_replica()) &&
               replica.is_completed();
    });
}

void MasterService::SyncCacheTotalAccounting(ObjectMetadata& metadata) {
    const bool has_memory_cache_replica =
        HasCompletedMemoryCacheReplica(metadata);
    const bool has_disk_cache_replica = HasCompletedDiskCacheReplica(metadata);

    if (!metadata.memory_cache_total_accounted && has_memory_cache_replica) {
        MasterMetricManager::instance().inc_mem_cache_nums();
        metadata.memory_cache_total_accounted = true;
    } else if (metadata.memory_cache_total_accounted &&
               !has_memory_cache_replica) {
        MasterMetricManager::instance().dec_mem_cache_nums();
        metadata.memory_cache_total_accounted = false;
    }

    if (!metadata.disk_cache_total_accounted && has_disk_cache_replica) {
        MasterMetricManager::instance().inc_file_cache_nums();
        metadata.disk_cache_total_accounted = true;
    } else if (metadata.disk_cache_total_accounted && !has_disk_cache_replica) {
        MasterMetricManager::instance().dec_file_cache_nums();
        metadata.disk_cache_total_accounted = false;
    }
}

void MasterService::AccountCacheTotalRemoval(ObjectMetadata& metadata) {
    if (metadata.memory_cache_total_accounted) {
        MasterMetricManager::instance().dec_mem_cache_nums();
        metadata.memory_cache_total_accounted = false;
    }
    if (metadata.disk_cache_total_accounted) {
        MasterMetricManager::instance().dec_file_cache_nums();
        metadata.disk_cache_total_accounted = false;
    }
}

void MasterService::RebuildCacheTotalAccounting() {
    MasterMetricManager::instance().reset_cache_total_nums();
    for (auto& shard : metadata_shards_) {
        for (auto& tenant_entry : shard.tenants) {
            for (auto& metadata_entry : tenant_entry.second.metadata) {
                SyncCacheTotalAccounting(metadata_entry.second);
            }
        }
    }
}

std::vector<Replica> MasterService::PopReplicasWithCacheTotalAccounting(
    ObjectMetadata& metadata,
    const std::function<bool(const Replica&)>& pred_fn) {
    auto replicas = metadata.PopReplicas(pred_fn);
    SyncCacheTotalAccounting(metadata);
    return replicas;
}

std::vector<Replica> MasterService::PopReplicasWithCacheTotalAccounting(
    ObjectMetadata& metadata) {
    auto replicas = metadata.PopReplicas();
    SyncCacheTotalAccounting(metadata);
    return replicas;
}

size_t MasterService::EraseReplicasWithCacheTotalAccounting(
    ObjectMetadata& metadata,
    const std::function<bool(const Replica&)>& pred_fn) {
    auto erased_replicas =
        PopReplicasWithCacheTotalAccounting(metadata, pred_fn);
    // Release SSD/local-disk usage for any local-disk replicas being removed.
    // No-op for memory/noF replicas, so it is safe to call unconditionally.
    ReleaseLocalDiskUsage(erased_replicas);
    return erased_replicas.size();
}

void MasterService::FinalizeRemovedReplicasAfterDurable(
    const OpLogEntry& durable_entry, const std::vector<ReplicaID>& replica_ids,
    QuotaEraseMode quota_mode) {
    if (replica_ids.empty()) {
        return;
    }

    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const TenantId tenant_id(durable_entry.tenant_id);
    const size_t shard_idx =
        getMetadataShardIndex(tenant_id, durable_entry.object_key);
    MetadataShardAccessorRW shard(this, shard_idx);
    auto tenant_it = shard->tenants.find(tenant_id);
    if (tenant_it == shard->tenants.end()) {
        return;
    }
    auto& tenant_state = tenant_it->second;
    auto metadata_it = tenant_state.metadata.find(durable_entry.object_key);
    if (metadata_it == tenant_state.metadata.end()) {
        return;
    }

    std::unordered_set<ReplicaID> ids(replica_ids.begin(), replica_ids.end());
    auto& metadata = metadata_it->second;
    auto erased_replicas = PopReplicasWithCacheTotalAccounting(
        metadata, [&ids](const Replica& replica) {
            return replica.status() == ReplicaStatus::REMOVED &&
                   ids.contains(replica.id());
        });
    if (erased_replicas.empty()) {
        return;
    }
    const uint64_t erased_memory_replicas = static_cast<uint64_t>(std::count_if(
        erased_replicas.begin(), erased_replicas.end(),
        [](const Replica& replica) { return replica.is_memory_replica(); }));
    if (erased_memory_replicas > 0) {
        ReleaseCommittedQuotaCharge(
            metadata, SaturatingMultiply(static_cast<uint64_t>(metadata.size),
                                         erased_memory_replicas));
    }
    const bool erased_local_disk = std::any_of(
        erased_replicas.begin(), erased_replicas.end(),
        [](const Replica& replica) { return replica.is_local_disk_replica(); });
    std::vector<UUID> local_disk_holders;
    for (const auto& replica : erased_replicas) {
        if (!replica.is_local_disk_replica()) continue;
        auto client_id = replica.get_local_disk_client_id();
        if (client_id.has_value()) {
            local_disk_holders.push_back(client_id.value());
        }
    }
    ReleaseLocalDiskUsage(erased_replicas);
    if (erased_local_disk) {
        shard.OnDiskReplicaRemoved(erased_local_disk, metadata);
    }
    if (!metadata.IsValid()) {
        EraseMetadata(tenant_state, metadata_it, tenant_id, quota_mode, &shard);
        if (tenant_state.Empty()) {
            shard->tenants.erase(tenant_it);
        }
    }
    if (erased_local_disk) {
        EnqueueRemoveTasks(
            local_disk_holders,
            RemoveTaskItem{tenant_id.value(), durable_entry.object_key});
    }
}

void MasterService::FinalizeMetadataEraseAfterDurable(
    const OpLogEntry& durable_entry, QuotaEraseMode quota_mode) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const TenantId tenant_id(durable_entry.tenant_id);
    const size_t shard_idx =
        getMetadataShardIndex(tenant_id, durable_entry.object_key);
    MetadataShardAccessorRW shard(this, shard_idx);
    auto tenant_it = shard->tenants.find(tenant_id);
    if (tenant_it == shard->tenants.end()) {
        return;
    }
    auto& tenant_state = tenant_it->second;
    auto metadata_it = tenant_state.metadata.find(durable_entry.object_key);
    if (metadata_it == tenant_state.metadata.end()) {
        return;
    }
    EraseMetadata(tenant_state, metadata_it, tenant_id, quota_mode, &shard);
    if (tenant_state.Empty()) {
        shard->tenants.erase(tenant_it);
    }
}

void MasterService::FinalizeExpiredProcessingReplicasAfterDurable(
    const OpLogEntry& durable_entry,
    const std::chrono::system_clock::time_point& ttl) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const TenantId tenant_id(durable_entry.tenant_id);
    MetadataAccessorRW accessor(this, MakeObjectIdentityForRequest(
                                          durable_entry.object_key, tenant_id));
    if (!accessor.Exists()) {
        return;
    }

    auto& metadata = accessor.Get();

    auto replicas = PopReplicasWithCacheTotalAccounting(
        metadata, &Replica::fn_is_processing);
    if (!replicas.empty()) {
        std::lock_guard lock(discarded_replicas_mutex_);
        discarded_replicas_.emplace_back(std::move(replicas), ttl);
    }
    if (!metadata.IsValid()) {
        accessor.Erase();
    } else if (accessor.InProcessing()) {
        accessor.EraseFromProcessing();
    }
}

void MasterService::FinalizeExpiredReplicationTaskAfterDurable(
    const OpLogEntry& durable_entry, ReplicaID source_id,
    const std::vector<ReplicaID>& target_ids,
    const std::chrono::system_clock::time_point& ttl) {
    if (target_ids.empty()) {
        return;
    }

    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const TenantId tenant_id(durable_entry.tenant_id);
    MetadataAccessorRW accessor(this, MakeObjectIdentityForRequest(
                                          durable_entry.object_key, tenant_id));
    if (!accessor.Exists()) {
        return;
    }

    auto& metadata = accessor.Get();
    if (auto source = metadata.GetReplicaByID(source_id); source != nullptr) {
        source->dec_refcnt();
    }

    std::unordered_set<ReplicaID> ids(target_ids.begin(), target_ids.end());
    auto replicas = PopReplicasWithCacheTotalAccounting(
        metadata,
        [&ids](const Replica& replica) { return ids.contains(replica.id()); });
    if (!replicas.empty()) {
        std::lock_guard lock(discarded_replicas_mutex_);
        discarded_replicas_.emplace_back(std::move(replicas), ttl);
    }
    if (!metadata.IsValid()) {
        accessor.Erase();
    } else if (accessor.HasReplicationTask()) {
        AbortTenantQuota(
            tenant_id,
            accessor.GetReplicationTask().reserved_quota_charge_bytes);
        accessor.EraseReplicationTask();
    }
}

MasterService::StaleHandleCleanupPlan
MasterService::BuildStaleHandleCleanupPlan(
    const ObjectMetadata& metadata,
    const std::unordered_set<UUID, boost::hash<UUID>>& alive_clients) const {
    StaleHandleCleanupPlan plan;
    bool has_valid_after_cleanup = false;
    for (const auto& replica : metadata.GetAllReplicas()) {
        const bool stale =
            (replica.has_invalid_mem_handle() ||
             replica.has_invalid_nof_handle() ||
             replica.has_stale_local_disk_client(alive_clients)) &&
            replica.is_completed();
        if (stale) {
            plan.removed_ids.push_back(replica.id());
            continue;
        }
        if (replica.status() == ReplicaStatus::COMPLETE) {
            plan.remaining.push_back(replica.get_descriptor());
        }
        if (!replica.is_memory_replica() || !replica.has_invalid_mem_handle()) {
            has_valid_after_cleanup = true;
        }
    }
    plan.would_invalidate = metadata.size == 0 || !has_valid_after_cleanup;
    return plan;
}

tl::expected<void, ErrorCode> MasterService::PersistStaleHandleCleanupForHA(
    const std::string& why, const TenantId& tenant_id, const std::string& key,
    ObjectMetadata& metadata, const StaleHandleCleanupPlan& plan) {
    if (plan.removed_ids.empty() || !enable_oplog_) {
        return {};
    }

    const auto op_type =
        plan.would_invalidate ? OpType::REMOVE : OpType::PUT_END;
    const std::string payload =
        plan.would_invalidate
            ? std::string{}
            : SerializeMetadataForOpLogFromReplicaDescriptors(
                  metadata.client_id, metadata.size, plan.remaining,
                  metadata.group_id, metadata.data_type);

    auto reservation = ReserveBatchOpLogSlot();
    if (!reservation) {
        return tl::make_unexpected(reservation.error());
    }
    const std::unordered_set<ReplicaID> ids(plan.removed_ids.begin(),
                                            plan.removed_ids.end());
    metadata.VisitReplicas(
        [&ids](const Replica& replica) { return ids.contains(replica.id()); },
        [](Replica& replica) { replica.mark_removed(); });
    auto result = AppendReservedOpLogWithDurableFinalize(
        std::move(reservation.value()), op_type, tenant_id.value(), key,
        payload,
        [this,
         removed_ids = plan.removed_ids](const OpLogEntry& durable_entry) {
            FinalizeRemovedReplicasAfterDurable(durable_entry, removed_ids,
                                                QuotaEraseMode::kFull);
        });
    if (!result) {
        LOG(WARNING) << why
                     << ": stale cleanup OpLog queue failed for key=" << key
                     << ", err=" << static_cast<int>(result.error());
        return tl::make_unexpected(result.error());
    }
    return {};
}

namespace {

constexpr int kOplogRetryMaxAttempts = 10;
constexpr int kOplogRetryMaxAttemptsUnavailable = 5;
constexpr int kOplogRetryMaxDelayMs = 16;

template <typename F>
auto RetryOplogPersist(F&& persist_fn) -> decltype(std::declval<F>()()) {
    for (int attempt = 0; attempt <= kOplogRetryMaxAttempts; ++attempt) {
        auto result = persist_fn();
        if (result) {
            return result;
        }
        const ErrorCode err = result.error();

        if (err == ErrorCode::TASK_PENDING_LIMIT_EXCEEDED) {
            // Slots full: writer is sealing the current batch and will
            // free capacity imminently. Worth waiting.
            if (attempt == kOplogRetryMaxAttempts) {
                return result;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(
                std::min(1 << attempt, kOplogRetryMaxDelayMs)));
            continue;
        }

        if (err == ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS) {
            // Writer not accepting: write_batch is retrying against the
            // KV backend. Recovery depends on the backend; bail out
            // earlier to avoid spinning on a persistent outage.
            if (attempt >= kOplogRetryMaxAttemptsUnavailable) {
                LOG(WARNING) << "Oplog writer not accepting after "
                             << attempt << " retries, falling back to local";
                return result;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(
                std::min(1 << attempt, kOplogRetryMaxDelayMs)));
            continue;
        }

        // Non-backpressure errors (INVALID_PARAMS etc.) — no retry.
        return result;
    }
    return tl::unexpected(ErrorCode::INTERNAL_ERROR);
}

}  // namespace

std::unordered_map<std::string, MasterService::ObjectMetadata>::iterator
MasterService::EraseMetadata(
    TenantState& tenant_state,
    std::unordered_map<std::string, ObjectMetadata>::iterator it,
    const TenantId& tenant_id) {
    return EraseMetadata(tenant_state, it, tenant_id, QuotaEraseMode::kFull);
}

std::unordered_map<std::string, MasterService::ObjectMetadata>::iterator
MasterService::EraseMetadata(
    TenantState& tenant_state,
    std::unordered_map<std::string, ObjectMetadata>::iterator it,
    const TenantId& tenant_id, QuotaEraseMode quota_mode) {
    return EraseMetadata(tenant_state, it, tenant_id, quota_mode, nullptr);
}

// EraseMetadata deletes the object metadata and also cleans up all
// associated per-key state: offloading_tasks (with dec_refcnt),
// processing_keys, replication_tasks, and promotion tasks.
// Callers no longer need to clean these up manually before calling.
std::unordered_map<std::string, MasterService::ObjectMetadata>::iterator
MasterService::EraseMetadata(
    TenantState& tenant_state,
    std::unordered_map<std::string, ObjectMetadata>::iterator it,
    const TenantId& tenant_id, QuotaEraseMode quota_mode,
    MetadataShardAccessorRW* shard) {
    bool had_completed_disk = it->second.HasReplica([](const Replica& r) {
        return r.is_local_disk_replica() && r.is_completed();
    });
    const std::string key = it->first;
    const std::string group_id = it->second.group_id;
    auto& metadata = it->second;

    // Clean up offloading_tasks + dec_refcnt before erasing metadata.
    // When BatchEvict deletes metadata, Store Worker may still have an
    // in-flight offload for this key. Without this cleanup the task
    // becomes an orphan that only expires after 600s.
    auto offload_it = tenant_state.offloading_tasks.find(key);
    if (offload_it != tenant_state.offloading_tasks.end()) {
        for (const auto& task : offload_it->second) {
            auto source = metadata.GetReplicaByID(task.source_id);
            if (source != nullptr) {
                source->dec_refcnt();
            }
        }
        tenant_state.offloading_tasks.erase(offload_it);

        // Mirror entry in local_disk_segment.offloading_objects must be
        // dropped too, otherwise the next OffloadObjectHeartbeat drains a
        // task-less key back to the client and produces an orphan bucket.
        const std::string scoped_key = tenant_id.MakeScopedKey(key);
        ScopedLocalDiskSegmentAccess ssd_access =
            segment_manager_.getLocalDiskSegmentAccess();
        for (auto& [_, segment] : ssd_access.getClientLocalDiskSegment()) {
            MutexLocker locker(&segment->offloading_mutex_);
            segment->offloading_objects.erase(scoped_key);
        }
    }
    tenant_state.processing_keys.erase(key);
    tenant_state.replication_tasks.erase(key);
    ErasePromotionTaskIfPresent(tenant_state, key, tenant_id);

    // kHandoff 跳过 ReleaseLocalDiskUsage：数据字节仍留在共享 segment，
    // 不应在此扣减 ssd_used_bytes（否则造成账务偏差）。
    if (quota_mode != QuotaEraseMode::kHandoff) {
        ReleaseLocalDiskUsage(metadata.GetAllReplicas());
    }
    AccountCacheTotalRemoval(metadata);
    switch (quota_mode) {
        case QuotaEraseMode::kFull:
        case QuotaEraseMode::kHandoff:
            AbortTenantQuota(tenant_id, metadata.reserved_quota_charge_bytes);
            ReleaseTenantQuota(tenant_id,
                               metadata.committed_quota_charge_bytes);
            ReleaseTenantQuota(tenant_id,
                               metadata.pending_replaced_quota_charge_bytes);
            break;
        case QuotaEraseMode::kPreserveOld:
            AbortTenantQuota(tenant_id, metadata.reserved_quota_charge_bytes);
            break;
        case QuotaEraseMode::kAbortOnly:
            AbortTenantQuota(tenant_id, metadata.reserved_quota_charge_bytes);
            break;
    }
    // CVM 多 submaster：若该对象的句柄由 peer 分配（远程分配/迁移入），
    // 广播释放到所有 peer 的 keepalive 注册表；kHandoff（slot 迁移）跳过，
    // 保留数据字节并让导入方接管远程释放责任。
    EnqueueRemoteFreeIfTracked(tenant_id, key, quota_mode);
    auto next = tenant_state.metadata.erase(it);
    DecrementTenantMetadataObjectCount(tenant_id);
    if (had_completed_disk && shard) {
        shard->OnDiskReplicaRemoved(had_completed_disk);
    }
    UnregisterGroupMember(tenant_state, tenant_id, key, group_id);
    return next;
}

void MasterService::ReleaseLocalDiskUsage(
    const std::vector<Replica>& replicas) {
    std::unordered_map<UUID, int64_t, boost::hash<UUID>> bytes_by_client;
    for (const auto& replica : replicas) {
        if (!replica.is_local_disk_replica()) {
            continue;
        }
        const auto descriptor =
            replica.get_descriptor().get_local_disk_descriptor();
        if (descriptor.object_size > 0) {
            bytes_by_client[descriptor.client_id] += descriptor.object_size;
        }
    }
    if (bytes_by_client.empty()) {
        return;
    }

    ScopedLocalDiskSegmentAccess ssd_access =
        segment_manager_.getLocalDiskSegmentAccess();
    auto& client_segments = ssd_access.getClientLocalDiskSegment();
    for (const auto& [client_id, bytes] : bytes_by_client) {
        auto disk_it = client_segments.find(client_id);
        if (disk_it != client_segments.end()) {
            disk_it->second->ssd_used_bytes.fetch_sub(
                bytes, std::memory_order_relaxed);
        }
    }
}

void MasterService::RebuildGroupRoutingIndex() {
    std::unordered_map<std::string, std::string> rebuilt_group_ids;
    std::unordered_set<std::string> groups_needing_refresh;
    for (size_t shard_idx = 0; shard_idx < kNumShards; ++shard_idx) {
        MetadataShardAccessorRW shard(this, shard_idx);
        for (auto& [tenant_id, tenant_state] : shard->tenants) {
            tenant_state.group_members.clear();
            for (const auto& [key, metadata] : tenant_state.metadata) {
                if (!metadata.IsGrouped()) {
                    continue;
                }
                tenant_state.group_members[metadata.group_id].insert(key);
                rebuilt_group_ids[tenant_id.MakeScopedKey(key)] =
                    metadata.group_id;
                groups_needing_refresh.insert(
                    tenant_id.MakeScopedKey(metadata.group_id));
            }
        }
    }
    {
        std::unique_lock<std::shared_mutex> lock(group_routing_mutex_);
        object_group_ids_ = std::move(rebuilt_group_ids);
        groups_needing_lease_refresh_ = std::move(groups_needing_refresh);
    }
}

void MasterService::GrantLeaseForGroup(const TenantState& tenant_state,
                                       const std::string& key,
                                       const ObjectMetadata& metadata) const {
    if (!metadata.IsGrouped()) {
        metadata.GrantLease(default_kv_lease_ttl_, default_kv_soft_pin_ttl_);
        return;
    }

    bool needs_refresh = metadata.NeedsLeaseRefresh(default_kv_lease_ttl_,
                                                    default_kv_soft_pin_ttl_);
    if (!needs_refresh) {
        std::shared_lock<std::shared_mutex> lock(group_routing_mutex_);
        needs_refresh =
            groups_needing_lease_refresh_.find(metadata.tenant_id.MakeScopedKey(
                metadata.group_id)) != groups_needing_lease_refresh_.end();
    }
    if (!needs_refresh) {
        return;
    }

    auto group_it = tenant_state.group_members.find(metadata.group_id);
    if (group_it == tenant_state.group_members.end()) {
        metadata.GrantLease(default_kv_lease_ttl_, default_kv_soft_pin_ttl_);
        return;
    }

    for (const auto& member_key : group_it->second) {
        auto mit = tenant_state.metadata.find(member_key);
        if (mit != tenant_state.metadata.end()) {
            mit->second.GrantLease(default_kv_lease_ttl_,
                                   default_kv_soft_pin_ttl_);
        }
    }
    if (group_it->second.find(key) == group_it->second.end()) {
        metadata.GrantLease(default_kv_lease_ttl_, default_kv_soft_pin_ttl_);
    }
    {
        std::unique_lock<std::shared_mutex> lock(group_routing_mutex_);
        groups_needing_lease_refresh_.erase(
            metadata.tenant_id.MakeScopedKey(metadata.group_id));
    }
}

void MasterService::ClearInvalidHandles() {
    ClearInvalidHandles(getAliveClientsSnapshot());
}

void MasterService::ClearInvalidHandles(
    const std::unordered_set<UUID, boost::hash<UUID>>& alive_clients) {
    for (size_t i = 0; i < kNumShards; i++) {
        MetadataShardAccessorRW shard(this, i);
        for (auto tenant_it = shard->tenants.begin();
             tenant_it != shard->tenants.end();) {
            auto& tenant_state = tenant_it->second;
            auto it = tenant_state.metadata.begin();
            while (it != tenant_state.metadata.end()) {
                const auto cleanup_plan =
                    BuildStaleHandleCleanupPlan(it->second, alive_clients);
                if (!cleanup_plan.removed_ids.empty()) {
                    if (enable_ha_) {
                        if (enable_oplog_) {
                            auto persist_result =
                                RetryOplogPersist([&]() {
                                    return PersistStaleHandleCleanupForHA(
                                        "ClearInvalidHandles",
                                        tenant_it->first, it->first,
                                        it->second, cleanup_plan);
                                });
                            if (persist_result) {
                                ++it;
                                continue;
                            }
                        }
                    }
                    if (CleanupStaleHandles(it->second, alive_clients,
                                            &shard)) {
                        it = EraseMetadata(tenant_state, it, tenant_it->first,
                                           QuotaEraseMode::kFull, &shard);
                    } else {
                        ++it;
                    }
                } else if (!it->second.IsValid()) {
                    if (enable_ha_) {
                        if (enable_oplog_) {
                            auto persist_result =
                                RetryOplogPersist([&]() {
                                    return AppendOpLogWithDurableFinalize(
                                        OpType::REMOVE,
                                        tenant_it->first.value(),
                                        it->first, {},
                                        [this](
                                            const OpLogEntry& durable_entry) {
                                            FinalizeMetadataEraseAfterDurable(
                                                durable_entry,
                                                QuotaEraseMode::kFull);
                                        });
                                });
                            if (persist_result) {
                                // OPLog path succeeded – skip local erase.
                                ++it;
                                continue;
                            }
                            LOG(WARNING)
                                << "ClearInvalidHandles(last replica)"
                                << ": REMOVE persist failed for key="
                                << it->first << ", err="
                                << static_cast<int>(persist_result.error());
                            // Fall through to local erase.
                        }
                    }
                    it = EraseMetadata(tenant_state, it, tenant_it->first,
                                       QuotaEraseMode::kFull, &shard);
                } else {
                    ++it;
                }
            }
            if (tenant_state.Empty()) {
                tenant_it = shard->tenants.erase(tenant_it);
            } else {
                ++tenant_it;
            }
        }
    }
}

void MasterService::TaskCleanupThreadFunc() {
    LOG(INFO) << "Task cleanup thread started";
    while (task_cleanup_running_) {
        // Wait for the next cleanup interval, but allow fast shutdown.
        {
            std::unique_lock<std::mutex> lk(task_cleanup_mutex_);
            task_cleanup_cv_.wait_for(
                lk, std::chrono::milliseconds(kTaskCleanupThreadSleepMs),
                [&] { return !task_cleanup_running_.load(); });
        }

        if (!task_cleanup_running_) {
            break;
        }

        std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
        auto write_access = task_manager_.get_write_access();
        write_access.prune_expired_tasks();
        write_access.prune_finished_tasks();
    }
    LOG(INFO) << "Task cleanup thread stopped";
}

auto MasterService::UnmountSegment(const UUID& segment_id,
                                   const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    size_t metrics_dec_capacity = 0;  // to update the metrics

    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    // 1. Prepare to unmount the segment by deleting its allocator
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        ErrorCode err = segment_access.PrepareUnmountSegment(
            segment_id, metrics_dec_capacity);
        if (err == ErrorCode::SEGMENT_NOT_FOUND) {
            // Return OK because this is an idempotent operation
            return {};
        }
        if (err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }
    }  // Release the segment mutex before long-running step 2 and avoid
       // deadlocks

    // 2. Remove the metadata of the related objects
    ClearInvalidHandles();

    // Cache endpoint before commit removes segment from registry.
    std::string segment_name;
    std::string te_endpoint;
    if (!segment_manager_.GetSegmentBasicInfo(segment_id, segment_name,
                                              te_endpoint)) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }

    // 3. Commit the unmount operation
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        auto err = segment_access.CommitUnmountSegment(segment_id, client_id,
                                                       metrics_dec_capacity);
        if (err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }
    }

    if (enable_oplog_ && ordered_oplog_writer_ && !te_endpoint.empty()) {
        SegmentUnmountOp op{te_endpoint};
        auto bytes = struct_pack::serialize(op);
        PersistSegmentOpForHAOrEnqueue("UnmountSegment",
                                       OpType::SEGMENT_UNMOUNT, te_endpoint,
                                       std::string(bytes.begin(), bytes.end()));
    }
    RecomputeTenantEffectiveQuotas();
    RemoveSegmentOwnerForCvm(segment_id);
    return {};
}

auto MasterService::GracefulUnmountSegment(const UUID& segment_id,
                                           const UUID& client_id,
                                           uint64_t grace_period_ms)
    -> tl::expected<void, ErrorCode> {
    std::unique_lock<std::shared_mutex> lock(snapshot_mutex_);
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();

    // Verify ownership: the segment must belong to the calling client
    std::vector<Segment> client_segments;
    auto err = segment_access.GetClientSegments(client_id, client_segments);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    bool owned = false;
    for (auto& seg : client_segments) {
        if (seg.id == segment_id) {
            owned = true;
            break;
        }
    }
    if (!owned) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    err = segment_access.PrepareGracefulUnmountSegment(segment_id);
    if (err == ErrorCode::SEGMENT_NOT_FOUND) {
        return {};
    }
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    auto expire_time = std::chrono::steady_clock::now() +
                       std::chrono::milliseconds(grace_period_ms);
    graceful_unmount_scheduler_.Schedule({segment_id, client_id}, expire_time);
    return {};
}

auto MasterService::UnmountNoFSegment(const UUID& segment_id,
                                      const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
#ifndef USE_NOF
    LOG(ERROR) << "client_id=" << client_id << ", segment_id=" << segment_id
               << ", error=nof_pool_disabled";
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
#else
    size_t metrics_dec_capacity = 0;  // to update the metrics

    // 1. Prepare to unmount the segment by deleting its allocator
    {
        ScopedNoFSegmentAccess segment_access =
            nof_segment_manager_.getNoFSegmentAccess();
        ErrorCode err = segment_access.PrepareUnmountSegment(
            segment_id, metrics_dec_capacity);
        if (err == ErrorCode::SEGMENT_NOT_FOUND) {
            // Return OK because this is an idempotent operation
            return {};
        }
        if (err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }
    }  // Release the segment mutex before long-running step 2 and avoid
       // deadlocks

    // 2. Remove the metadata of the related objects
    ClearInvalidHandles();

    // 3. Commit the unmount operation
    ScopedNoFSegmentAccess segment_access =
        nof_segment_manager_.getNoFSegmentAccess();
    auto err = segment_access.CommitUnmountSegment(segment_id, client_id,
                                                   metrics_dec_capacity);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    {
        std::lock_guard<std::mutex> lock(nof_heartbeat_mutex_);
        nof_heartbeat_states_.erase(segment_id);
    }
    return {};
#endif
}

auto MasterService::ExistKey(const std::string& key, const TenantId& tenant_id)
    -> tl::expected<bool, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRO accessor(this,
                                MakeObjectIdentityForRequest(key, tenant_id));
    if (!accessor.Exists()) {
        MC_VLOG(1) << "key=" << key << ", info=object_not_found";
        return false;
    }

    const auto& metadata = accessor.Get();
    if (!metadata.HasReplica(&Replica::fn_is_completed)) {
        return false;
    }

    // Grant a lease to the object as it may be further used by the client.
    auto* ts = accessor.GetTenantState();
    if (ts) {
        GrantLeaseForGroup(*ts, key, metadata);
    } else {
        metadata.GrantLease(default_kv_lease_ttl_, default_kv_soft_pin_ttl_);
    }
    return true;
}

std::vector<tl::expected<bool, ErrorCode>> MasterService::BatchExistKey(
    const std::vector<std::string>& keys, const TenantId& tenant_id) {
    const TenantId& normalized_tenant = ResolveRequestTenantId(tenant_id);
    std::vector<tl::expected<bool, ErrorCode>> results(keys.size());
    if (keys.empty()) {
        return results;
    }

    std::vector<std::vector<size_t>> indices_by_shard(kNumShards);
    {
        std::shared_lock<std::shared_mutex> group_routing_lock(
            group_routing_mutex_);
        for (size_t i = 0; i < keys.size(); ++i) {
            auto route_it = object_group_ids_.find(
                normalized_tenant.MakeScopedKey(keys[i]));
            const size_t shard_idx =
                route_it == object_group_ids_.end()
                    ? getShardIndex(normalized_tenant, keys[i])
                    : getShardIndex(route_it->second);
            indices_by_shard[shard_idx].push_back(i);
        }
    }

    const size_t start_shard = randomIndex(kNumShards);
    for (size_t scanned = 0; scanned < kNumShards; ++scanned) {
        const size_t shard_idx =
            (start_shard + kNumShards - scanned) % kNumShards;
        const auto& key_indices = indices_by_shard[shard_idx];
        if (key_indices.empty()) {
            continue;
        }

        std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
        MetadataShardAccessorRO shard(this, shard_idx);
        auto tenant_it = shard->tenants.find(normalized_tenant);
        if (tenant_it == shard->tenants.end()) {
            for (const size_t i : key_indices) {
                VLOG(1) << "key=" << keys[i]
                        << ", tenant_id=" << normalized_tenant
                        << ", info=object_not_found";
                results[i] = false;
            }
            continue;
        }

        const auto& tenant_state = tenant_it->second;
        for (const size_t i : key_indices) {
            const auto& key = keys[i];
            auto it = tenant_state.metadata.find(key);
            if (it == tenant_state.metadata.end() || !it->second.IsValid()) {
                VLOG(1) << "key=" << key << ", tenant_id=" << normalized_tenant
                        << ", info=object_not_found";
                results[i] = false;
                continue;
            }

            const auto& metadata = it->second;
            if (!metadata.HasReplica(&Replica::fn_is_completed)) {
                results[i] = false;
                continue;
            }
            GrantLeaseForGroup(tenant_state, key, metadata);
            results[i] = true;
        }
    }
    return results;
}

auto MasterService::GetAllKeys(const TenantId& tenant_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::vector<std::string> all_keys;
    const TenantId& normalized_tenant = ResolveRequestTenantId(tenant_id);
    for (size_t i = 0; i < kNumShards; i++) {
        MetadataShardAccessorRO shard(this, i);
        auto tenant_it = shard->tenants.find(normalized_tenant);
        if (tenant_it == shard->tenants.end()) {
            continue;
        }
        for (const auto& item : tenant_it->second.metadata) {
            all_keys.push_back(item.second.user_key.empty()
                                   ? item.first
                                   : item.second.user_key);
        }
    }
    return all_keys;
}

auto MasterService::GetAllSegments()
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    std::vector<std::string> all_segments;
    auto err = segment_access.GetAllSegments(all_segments);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return all_segments;
}

auto MasterService::GetAllNoFSegments()
    -> tl::expected<std::vector<NoFSegment>, ErrorCode> {
    std::vector<MountedNoFSegmentSnapshot> mounted_segments;
    nof_segment_manager_.GetMountedSegmentsSnapshot(mounted_segments);

    std::vector<NoFSegment> result;
    for (const auto& segment : mounted_segments) {
        result.push_back(segment.segment);
    }

    return result;
}

auto MasterService::GetNoFSegmentsByName(const std::string& segment_name)
    -> tl::expected<std::vector<NoFSegmentOwnerInfo>, ErrorCode> {
    return nof_segment_manager_.GetSegmentsByName(segment_name);
}

auto MasterService::GetSegmentsDetail()
    -> tl::expected<std::vector<SegmentDetailInfo>, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();

    // Get full info of all segments (including Segment and client_id)
    std::vector<std::pair<Segment, UUID>> all_segments;
    auto err = segment_access.GetAllSegments(all_segments);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    std::vector<SegmentDetailInfo> result;
    result.reserve(all_segments.size());

    for (const auto& [segment, client_id] : all_segments) {
        SegmentDetailInfo info;
        info.segment_name = segment.name;
        info.segment_id = segment.id;
        info.client_id = client_id;
        info.base_address = segment.base;
        info.size_bytes = segment.size;
        info.te_endpoint = segment.te_endpoint;
        info.protocol = segment.protocol;

        // Query segment status
        segment_access.GetSegmentStatusByName(segment.name, info.status);

        // Query allocator used/capacity
        size_t used = 0, capacity = 0;
        segment_access.QuerySegments(segment.name, used, capacity);
        info.allocator_used_bytes = used;
        info.allocator_capacity_bytes = capacity;

        result.push_back(std::move(info));
    }

    return result;
}

auto MasterService::QuerySegments(const std::string& segment)
    -> tl::expected<std::pair<size_t, size_t>, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    size_t used, capacity;
    auto err = segment_access.QuerySegments(segment, used, capacity);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return std::make_pair(used, capacity);
}

auto MasterService::QuerySegmentStatus(const std::string& segment_name)
    -> tl::expected<SegmentStatus, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    SegmentStatus status = SegmentStatus::UNDEFINED;
    auto err = segment_access.GetSegmentStatusByName(segment_name, status);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return status;
}

auto MasterService::QuerySegmentStatusById(const UUID& segment_id)
    -> tl::expected<SegmentStatus, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    SegmentStatus status = SegmentStatus::UNDEFINED;
    auto err = segment_access.GetSegmentStatusById(segment_id, status);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return status;
}

void MasterService::RestoreFromStandbySnapshot(
    const std::vector<StandbyObjectEntry>& objects,
    uint64_t initial_oplog_sequence_id,
    const std::vector<StandbySegmentInfo>& segments) {
    // The ordered writer initializes its sequence from durable_prefix.
    (void)initial_oplog_sequence_id;

    // 2. Build allocator keepalive map for standby segments.
    for (const auto& [segment, bytes] : standby_accounted_memory_bytes_) {
        MasterMetricManager::instance().dec_allocated_mem_size(
            segment, static_cast<int64_t>(bytes));
    }
    standby_accounted_memory_bytes_.clear();
    standby_memory_segments_.clear();
    standby_allocator_keepalive_.clear();
    invalid_replica_endpoints_.clear();
    for (const auto& seg : segments) {
        if (seg.is_memory_segment) {
            standby_memory_segments_.push_back(seg);
            auto allocator = std::make_shared<DummyBufferAllocator>(
                seg.segment_name, seg.transport_endpoint);
            standby_allocator_keepalive_[seg.transport_endpoint] = allocator;
            if (seg.segment_name != seg.transport_endpoint) {
                standby_allocator_keepalive_[seg.segment_name] = allocator;
            }
        }
        if (!segment_manager_.HasSegmentByEndpoint(seg.transport_endpoint)) {
            invalid_replica_endpoints_.insert(seg.transport_endpoint);
            if (seg.segment_name != seg.transport_endpoint) {
                invalid_replica_endpoints_.insert(seg.segment_name);
            }
        }
    }

    // 3. Restore object metadata.
    const auto resolve_standby_object = [](const StandbyObjectEntry& entry) {
        auto [scoped_tenant_id, user_key] = TenantId::ParseScopedKey(entry.key);
        TenantId tenant_id(entry.tenant_id);
        if (tenant_id.IsDefault() && !scoped_tenant_id.IsDefault()) {
            tenant_id = std::move(scoped_tenant_id);
        }
        return std::make_pair(std::move(tenant_id), std::move(user_key));
    };

    std::unordered_map<size_t, std::vector<const StandbyObjectEntry*>>
        objects_by_shard;

    // P4：晋升时只物化「本机负责 slot」的对象元数据（数据字节留在 segment）。
    // 仅在 etcd HA 动态分区下过滤；非 HA / 单机 / 测试路径 lookup 为空，退化
    // 为恢复全部。ResolveOwnedSlotsForCvm 失败时 sticky 沿用上一轮结果
    // （standby 晋升前为空 → 全量恢复），同样等价于不过滤。
    std::vector<bool> owned_slot_lookup;
    {
        const bool kv_partition_enabled =
            enable_ha_ && ha_backend_type_ == "etcd" &&
            !master_id_.empty() && !cluster_id_.empty();
        if (kv_partition_enabled) {
            const std::vector<uint16_t> owned_slots =
                ResolveOwnedSlotsForCvm();
            if (!owned_slots.empty()) {
                owned_slot_lookup.assign(cvm::kSlotCount, false);
                for (uint16_t slot : owned_slots) {
                    owned_slot_lookup[slot] = true;
                }
                // 同步到读路径 owned-slot 位图（A1），让新 primary 立即按
                // 最新分区拒绝非本机 slot 的读请求。
                UpdateOwnedSlots(owned_slots);
            }
        }
    }

    for (const auto& entry : objects) {
        auto [tenant_id, user_key] = resolve_standby_object(entry);
        if (!tenant_id.IsValid()) {
            LOG(WARNING) << "RestoreFromStandbySnapshot: invalid tenant_id="
                         << entry.tenant_id << ", key=" << entry.key
                         << ", skipping";
            continue;
        }
        // slot 过滤：只物化本机负责 slot 的元数据（P4 元数据迁移）。
        if (!owned_slot_lookup.empty()) {
            const uint16_t slot = cvm::KeySlot(tenant_id, user_key);
            if (!owned_slot_lookup[slot]) {
                continue;
            }
        }
        const auto shard_idx = entry.metadata.group_id.empty()
                                   ? getShardIndex(tenant_id, user_key)
                                   : getShardIndex(entry.metadata.group_id);
        objects_by_shard[shard_idx].push_back(&entry);
    }

    for (const auto& [shard_idx, shard_objects] : objects_by_shard) {
        MetadataShardAccessorRW shard(this, shard_idx);
        auto now = std::chrono::system_clock::now();
        for (const auto* entry_ptr : shard_objects) {
            const auto& entry = *entry_ptr;
            auto [tenant_id, user_key] = resolve_standby_object(entry);
            const auto& standby_meta = entry.metadata;
            std::vector<Replica> replicas;
            replicas.reserve(standby_meta.replicas.size());

            for (const auto& desc : standby_meta.replicas) {
                if (desc.is_memory_replica()) {
                    const auto& mem_desc = desc.get_memory_descriptor();
                    const std::string& endpoint =
                        mem_desc.buffer_descriptor.transport_endpoint_;
                    auto it = standby_allocator_keepalive_.find(endpoint);
                    if (it != standby_allocator_keepalive_.end()) {
                        auto alloc = it->second;
                        replicas.emplace_back(
                            std::make_unique<AllocatedBuffer>(
                                alloc, mem_desc.buffer_descriptor),
                            desc.status);
                        MasterMetricManager::instance().inc_allocated_mem_size(
                            alloc->getSegmentName(),
                            static_cast<int64_t>(
                                mem_desc.buffer_descriptor.size_));
                        standby_accounted_memory_bytes_
                            [alloc->getSegmentName()] +=
                            mem_desc.buffer_descriptor.size_;
                    } else {
                        invalid_replica_endpoints_.insert(endpoint);
                    }
                } else if (desc.is_nof_replica()) {
                    const auto& nof_desc = desc.get_nof_descriptor();
                    const std::string& endpoint =
                        nof_desc.buffer_descriptor.transport_endpoint_;
                    auto& alloc = standby_allocator_keepalive_[endpoint];
                    if (!alloc) {
                        alloc = std::make_shared<DummyBufferAllocator>(
                            endpoint, endpoint);
                    }
                    replicas.emplace_back(
                        std::make_unique<AllocatedBuffer>(
                            alloc, nof_desc.buffer_descriptor),
                        desc.status, ReplicaType::NOF_SSD);
                } else if (desc.is_disk_replica()) {
                    const auto& disk_desc = desc.get_disk_descriptor();
                    replicas.emplace_back(disk_desc.file_path,
                                          disk_desc.object_size, desc.status);
                } else if (desc.is_local_disk_replica()) {
                    const auto& local_disk_desc =
                        desc.get_local_disk_descriptor();
                    replicas.emplace_back(
                        local_disk_desc.client_id, local_disk_desc.object_size,
                        local_disk_desc.transport_endpoint, desc.status);
                }
            }

            auto& tenant_state = shard->tenants[tenant_id];
            tenant_state.metadata.emplace(
                std::piecewise_construct, std::forward_as_tuple(user_key),
                std::forward_as_tuple(
                    standby_meta.client_id, now, standby_meta.size,
                    std::move(replicas), false, false, standby_meta.data_type,
                    standby_meta.group_id, tenant_id, user_key));
            if (!standby_meta.group_id.empty()) {
                RegisterGroupMember(tenant_state, tenant_id, user_key,
                                    standby_meta.group_id);
            }
            tenant_state.processing_keys.erase(user_key);
        }
    }

    // 4. Log the result.
    LOG(INFO) << "Restored from standby: " << objects.size() << " objects, "
              << segments.size()
              << " segments, initial_seq_id=" << initial_oplog_sequence_id
              << ", invalid_endpoints=" << invalid_replica_endpoints_.size();
}

auto MasterService::QueryIp(const UUID& client_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    std::vector<Segment> segments;
    ErrorCode err = segment_access.GetClientSegments(client_id, segments);
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::SEGMENT_NOT_FOUND) {
            VLOG(1) << "QueryIp: client_id=" << client_id
                    << " not found or has no segments";
            return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
        }

        LOG(ERROR) << "QueryIp: failed to get segments for client_id="
                   << client_id << ", error=" << toString(err);

        return tl::make_unexpected(err);
    }

    std::unordered_set<std::string> unique_ips;
    unique_ips.reserve(segments.size());
    for (const auto& segment : segments) {
        if (!segment.te_endpoint.empty()) {
            unique_ips.emplace(getHostNameWithoutPort(segment.te_endpoint));
        }
    }

    if (unique_ips.empty()) {
        LOG(WARNING) << "QueryIp: client_id=" << client_id
                     << " has no valid IP addresses";
        return {};
    }
    std::vector<std::string> result(unique_ips.begin(), unique_ips.end());
    return result;
}

auto MasterService::BatchQueryIp(const std::vector<UUID>& client_ids)
    -> tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode> {
    std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>
        results;
    results.reserve(client_ids.size());
    for (const auto& client_id : client_ids) {
        auto ip_result = QueryIp(client_id);
        if (ip_result.has_value()) {
            results.emplace(client_id, std::move(ip_result.value()));
        }
    }
    return results;
}

auto MasterService::BatchReplicaClear(
    const std::vector<std::string>& object_keys, const UUID& client_id,
    const std::string& segment_name)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    return BatchReplicaClear(object_keys, client_id, segment_name, "default");
}

auto MasterService::BatchReplicaClear(
    const std::vector<std::string>& object_keys, const UUID& client_id,
    const std::string& segment_name, const std::string& tenant_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    std::vector<std::string> cleared_keys;
    cleared_keys.reserve(object_keys.size());
    const bool clear_all_segments = segment_name.empty();
    const TenantId requested_tenant(tenant_id);
    if (!requested_tenant.IsValid()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const TenantId& normalized_tenant =
        ResolveRequestTenantId(requested_tenant);

    for (const auto& key : object_keys) {
        if (key.empty()) {
            LOG(WARNING) << "BatchReplicaClear: tenant=" << normalized_tenant
                         << " empty key, skipping";
            continue;
        }
        MetadataAccessorRW accessor(this,
                                    MakeObjectIdentity(key, normalized_tenant));
        if (!accessor.Exists()) {
            LOG(WARNING) << "BatchReplicaClear: tenant=" << normalized_tenant
                         << " key=" << key << " not found, skipping";
            continue;
        }

        auto& metadata = accessor.Get();

        // Security check: Ensure the requesting client owns the object.
        if (metadata.client_id != client_id) {
            LOG(WARNING) << "BatchReplicaClear: tenant=" << normalized_tenant
                         << " key=" << key << " belongs to different client_id="
                         << metadata.client_id << ", expected=" << client_id
                         << ", skipping";
            continue;
        }

        // Safety check: Do not clear an object that has an active lease.
        if (!metadata.IsLeaseExpired()) {
            LOG(WARNING) << "BatchReplicaClear: tenant=" << normalized_tenant
                         << " key=" << key << " has active lease, skipping";
            continue;
        }

        if (clear_all_segments) {
            // Check if all replicas are complete. Incomplete replicas could
            // indicate an ongoing Put operation, and clearing during this time
            // could lead to an inconsistent state or interfere with the write.
            if (!metadata.AllReplicas(&Replica::fn_is_completed)) {
                LOG(WARNING)
                    << "BatchReplicaClear: tenant=" << normalized_tenant
                    << " key=" << key << " has incomplete replicas, skipping";
                continue;
            }

            if (enable_ha_) {
                if (enable_oplog_) {
                    auto reservation = ReserveBatchOpLogSlot();
                    if (!reservation) {
                        continue;
                    }
                    std::vector<ReplicaID> removed_ids;
                    metadata.VisitReplicas(
                        &Replica::fn_is_completed,
                        [&removed_ids](Replica& replica) {
                            removed_ids.push_back(replica.id());
                            replica.mark_removed();
                        });
                    auto persist_result =
                        AppendReservedOpLogWithDurableFinalize(
                            std::move(reservation.value()), OpType::REMOVE,
                            normalized_tenant.value(), key, {},
                            [this, removed_ids = std::move(removed_ids)](
                                const OpLogEntry& durable_entry) {
                                FinalizeRemovedReplicasAfterDurable(
                                    durable_entry, removed_ids,
                                    QuotaEraseMode::kFull);
                            });
                    if (!persist_result) {
                        continue;
                    }
                    cleared_keys.emplace_back(key);
                    VLOG(1)
                        << "BatchReplicaClear: tenant=" << normalized_tenant
                        << " successfully cleared all replicas for key=" << key
                        << " for client_id=" << client_id;
                    continue;
                }
            }

            // Erase the entire metadata (all replicas will be deallocated)
            // accessor.Erase() internally calls EraseMetadata which already
            // decrements disk_object_count via OnDiskReplicaRemoved.
            accessor.Erase();
            cleared_keys.emplace_back(key);
            VLOG(1) << "BatchReplicaClear: tenant=" << normalized_tenant
                    << " successfully cleared all replicas for key=" << key
                    << " for client_id=" << client_id;
        } else {
            // Clear only replicas on the specified segment_name
            const auto match_replica_on_segment =
                [&](const Replica& replica) -> bool {
                if (!replica.is_completed()) {
                    return false;
                }
                const auto segment_names = replica.get_segment_names();
                for (const auto& seg_name : segment_names) {
                    if (seg_name.has_value() &&
                        seg_name.value() == segment_name) {
                        return true;
                    }
                }
                return false;
            };

            if (!metadata.HasReplica(match_replica_on_segment)) {
                LOG(WARNING)
                    << "BatchReplicaClear: tenant=" << normalized_tenant
                    << " key=" << key
                    << " has no replica on segment_name=" << segment_name
                    << ", skipping";
                continue;
            }

            bool had_completed_disk_on_segment =
                metadata.HasReplica([&segment_name](const Replica& r) {
                    if (!r.is_local_disk_replica() || !r.is_completed())
                        return false;
                    for (const auto& name : r.get_segment_names()) {
                        if (name.has_value() && name.value() == segment_name)
                            return true;
                    }
                    return false;
                });

            if (enable_ha_) {
                if (enable_oplog_) {
                    auto reservation = ReserveBatchOpLogSlot();
                    if (!reservation) {
                        continue;
                    }
                    auto remaining = BuildRemainingReplicaDescriptors(
                        metadata,
                        [&match_replica_on_segment](const Replica& r) {
                            return match_replica_on_segment(r);
                        });
                    std::vector<ReplicaID> removed_ids;
                    metadata.VisitReplicas(
                        match_replica_on_segment,
                        [&removed_ids](Replica& replica) {
                            removed_ids.push_back(replica.id());
                            replica.mark_removed();
                        });

                    tl::expected<OpLogEntry, ErrorCode> persist_result;
                    if (remaining.empty()) {
                        persist_result = AppendReservedOpLogWithDurableFinalize(
                            std::move(reservation.value()), OpType::REMOVE,
                            normalized_tenant.value(), key, {},
                            [this, removed_ids = std::move(removed_ids)](
                                const OpLogEntry& durable_entry) {
                                FinalizeRemovedReplicasAfterDurable(
                                    durable_entry, removed_ids,
                                    QuotaEraseMode::kFull);
                            });
                    } else {
                        persist_result = AppendReservedOpLogWithDurableFinalize(
                            std::move(reservation.value()), OpType::PUT_END,
                            normalized_tenant.value(), key,
                            SerializeMetadataForOpLogFromReplicaDescriptors(
                                metadata.client_id, metadata.size, remaining,
                                metadata.group_id, metadata.data_type),
                            [this, removed_ids = std::move(removed_ids)](
                                const OpLogEntry& durable_entry) {
                                FinalizeRemovedReplicasAfterDurable(
                                    durable_entry, removed_ids,
                                    QuotaEraseMode::kFull);
                            });
                    }
                    if (!persist_result) {
                        continue;
                    }
                    cleared_keys.emplace_back(key);
                    VLOG(1) << "BatchReplicaClear: tenant=" << normalized_tenant
                            << " successfully cleared replicas on segment_name="
                            << segment_name << " for key=" << key
                            << " for client_id=" << client_id;
                    continue;
                }
            }

            EraseReplicasWithCacheTotalAccounting(metadata,
                                                  match_replica_on_segment);

            if (had_completed_disk_on_segment &&
                !metadata.HasReplica([](const Replica& r) {
                    return r.is_local_disk_replica() && r.is_completed();
                })) {
                auto& shard = accessor.GetShard();
                shard.OnDiskReplicaRemoved(had_completed_disk_on_segment,
                                           metadata);
            }

            // If no valid replicas remain, erase the entire metadata
            // accessor.Erase() internally calls EraseMetadata which already
            // decrements disk_object_count via OnDiskReplicaRemoved.
            if (!metadata.IsValid()) {
                accessor.Erase();
            }

            cleared_keys.emplace_back(key);
            VLOG(1) << "BatchReplicaClear: tenant=" << normalized_tenant
                    << " successfully cleared replicas on segment_name="
                    << segment_name << " for key=" << key
                    << " for client_id=" << client_id;
        }
    }

    return cleared_keys;
}

bool MasterService::IsReplicaReadable(const Replica& replica) const {
    if (!replica.is_completed() || replica.has_invalid_mem_handle() ||
        replica.has_invalid_nof_handle()) {
        return false;
    }
    const auto descriptor = replica.get_descriptor();
    std::optional<std::string> endpoint;
    if (descriptor.is_memory_replica()) {
        endpoint = descriptor.get_memory_descriptor()
                       .buffer_descriptor.transport_endpoint_;
    } else if (descriptor.is_nof_replica()) {
        endpoint = descriptor.get_nof_descriptor()
                       .buffer_descriptor.transport_endpoint_;
    } else if (descriptor.is_local_disk_replica()) {
        endpoint = descriptor.get_local_disk_descriptor().transport_endpoint;
    }
    return !endpoint || !invalid_replica_endpoints_.contains(*endpoint);
}

bool MasterService::IsMemoryReplicaEvictable(const Replica& replica) const {
    return replica.is_memory_replica() && replica.is_completed() &&
           replica.get_refcnt() == 0 && IsReplicaReadable(replica);
}

auto MasterService::GetReplicaListByRegex(const std::string& regex_pattern,
                                          const TenantId& tenant_id)
    -> tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode> {
    std::unordered_map<std::string, std::vector<Replica::Descriptor>> results;
    std::regex pattern;

    try {
        pattern = std::regex(regex_pattern, std::regex::ECMAScript);
    } catch (const std::regex_error& e) {
        LOG(ERROR) << "Invalid regex pattern: " << regex_pattern
                   << ", error: " << e.what();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);

    // Build the set of currently alive client UUIDs
    std::unordered_set<UUID, boost::hash<UUID>> alive_clients;
    {
        std::shared_lock<std::shared_mutex> client_lock(client_mutex_);
        for (const auto& [client_id, host] : client_host_id_) {
            alive_clients.insert(client_id);
        }
    }

    const TenantId& normalized_tenant = ResolveRequestTenantId(tenant_id);
    for (size_t i = 0; i < kNumShards; ++i) {
        MetadataShardAccessorRO shard(this, i);
        auto tenant_it = shard->tenants.find(normalized_tenant);
        if (tenant_it == shard->tenants.end()) {
            continue;
        }
        for (const auto& [key, metadata] : tenant_it->second.metadata) {
            if (std::regex_search(key, pattern)) {
                std::vector<Replica::Descriptor> replica_list;
                metadata.VisitReplicas(
                    [this, &alive_clients](const Replica& replica) {
                        return IsReplicaReadable(replica) &&
                               !replica.has_stale_local_disk_client(
                                   alive_clients);
                    },
                    [&replica_list](const Replica& replica) {
                        replica_list.emplace_back(replica.get_descriptor());
                    });

                if (replica_list.empty()) {
                    LOG(WARNING)
                        << "key=" << key
                        << " matched by regex, but has no complete replicas.";
                    continue;
                }

                results.emplace(key, std::move(replica_list));
                GrantLeaseForGroup(tenant_it->second, key, metadata);
            }
        }
    }

    return results;
}

auto MasterService::GetOffloadEndpoints()
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::unordered_set<std::string> unique_endpoints;

    // Build the set of currently alive client UUIDs
    std::unordered_set<UUID, boost::hash<UUID>> alive_clients;
    {
        std::shared_lock<std::shared_mutex> client_lock(client_mutex_);
        for (const auto& [client_id, host] : client_host_id_) {
            alive_clients.insert(client_id);
        }
    }

    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    for (size_t i = 0; i < kNumShards; ++i) {
        MetadataShardAccessorRO shard(this, i);
        for (const auto& tenant_it : shard->tenants) {
            for (const auto& metadata_it : tenant_it.second.metadata) {
                const auto& metadata = metadata_it.second;
                metadata.VisitReplicas(
                    [&alive_clients](const Replica& replica) {
                        return replica.is_completed() &&
                               replica.is_local_disk_replica() &&
                               !replica.has_stale_local_disk_client(
                                   alive_clients);
                    },
                    [&unique_endpoints](const Replica& replica) {
                        const auto desc = replica.get_descriptor();
                        const auto& endpoint =
                            desc.get_local_disk_descriptor()
                                .transport_endpoint;
                        if (!endpoint.empty()) {
                            unique_endpoints.emplace(endpoint);
                        }
                    });
            }
        }
    }

    std::vector<std::string> endpoints;
    endpoints.reserve(unique_endpoints.size());
    for (const auto& endpoint : unique_endpoints) {
        endpoints.emplace_back(endpoint);
    }
    return endpoints;
}

auto MasterService::GetReplicaList(const std::string& key,
                                   const TenantId& tenant_id)
    -> tl::expected<GetReplicaListResponse, ErrorCode> {
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);

    // A1：读路径 slot 所有权校验。分区重均衡后本机不再负责该 slot 时，先
    // 尝试向 slot owner 转发（方案 B 第二阶段）；转发失败再回退到
    // SLOT_NOT_OWNED 让客户端根据最新 slot→master 视图重新路由。
    const uint16_t slot = cvm::KeySlot(object_id.tenant_id, object_id.user_key);
    if (!OwnsSlot(slot)) {
#ifdef STORE_USE_ETCD
        auto owner = ResolveSlotOwnerMasterId(slot);
        if (owner && !owner->empty() && *owner != master_id_ &&
            inter_master_rpc_) {
            auto result = inter_master_rpc_->GetReplicaList(
                *owner, key, object_id.tenant_id.value());
            if (result.has_value()) {
                LOG(INFO) << "GetReplicaList forwarded: key=" << key
                          << " slot=" << slot << " -> owner=" << *owner;
                return result;
            }
            VLOG(1) << "GetReplicaList forward failed: key=" << key
                    << " owner=" << *owner
                    << " error=" << toString(result.error());
        }
#endif
        LOG(WARNING) << "GetReplicaList: key=" << key << " slot=" << slot
                     << " not owned by this master and forwarding failed, "
                        "rejecting with SLOT_NOT_OWNED (client should "
                        "re-route)";
        return tl::make_unexpected(ErrorCode::SLOT_NOT_OWNED);
    }

    return GetReplicaListLocal(object_id);
}

auto MasterService::GetReplicaListLocal(const ObjectIdentity& object_id)
    -> tl::expected<GetReplicaListResponse, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const std::string& key = object_id.user_key;

    GetReplicaListResponse resp({}, default_kv_lease_ttl_);
    bool promotion_eligible = false;
    {
        MetadataAccessorRO accessor(this, object_id);

        MasterMetricManager::instance().inc_total_get_nums();

        if (!accessor.Exists()) {
            VLOG(1) << "key=" << key << ", info=object_not_found";
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        const auto& metadata = accessor.Get();

        // Build the set of currently alive client UUIDs
        std::unordered_set<UUID, boost::hash<UUID>> alive_clients;
        {
            std::shared_lock<std::shared_mutex> client_lock(client_mutex_);
            for (const auto& [client_id, host] : client_host_id_) {
                alive_clients.insert(client_id);
            }
        }

        std::vector<Replica::Descriptor> replica_list;
        metadata.VisitReplicas(
            [this, &alive_clients](const Replica& replica) {
                return IsReplicaReadable(replica) &&
                       !replica.has_stale_local_disk_client(alive_clients);
            },
            [&replica_list](const Replica& replica) {
                replica_list.emplace_back(replica.get_descriptor());
            });

        if (replica_list.empty()) {
            if (metadata.AllReplicas([](const Replica& replica) {
                    return replica.status() == ReplicaStatus::REMOVED;
                })) {
                return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            }
            LOG(WARNING) << "key=" << key << ", error=replica_not_ready";
            return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
        }

        // TODO: NoF SSD support (ranhaojia)
        if (replica_list[0].is_memory_replica()) {
            MasterMetricManager::instance().inc_mem_cache_hit_nums();
            MasterMetricManager::instance().inc_mem_cache_hit_bytes(
                static_cast<int64_t>(metadata.size));
        } else if (replica_list[0].is_local_disk_replica() ||
                   replica_list[0].is_disk_replica()) {
            MasterMetricManager::instance().inc_file_cache_hit_nums();
            MasterMetricManager::instance().inc_file_cache_hit_bytes(
                static_cast<int64_t>(metadata.size));
        }
        MasterMetricManager::instance().inc_valid_get_nums();
        // Grant a lease to the object so it will not be removed
        // when the client is reading it.
        auto* ts = accessor.GetTenantState();
        if (ts) {
            GrantLeaseForGroup(*ts, key, metadata);
        } else {
            metadata.GrantLease(default_kv_lease_ttl_,
                                default_kv_soft_pin_ttl_);
        }

        // Promotion-on-hit eligibility: only when no MEMORY replica is
        // present but at least one LOCAL_DISK replica is. Decided here while
        // we hold the RO accessor; the actual enqueue happens after we
        // release the accessor below to avoid lock-upgrade complexity.
        if (promotion_on_hit_) {
            const bool any_memory =
                metadata.HasReplica(&Replica::fn_is_memory_replica);
            const bool any_local_disk =
                metadata.HasReplica(&Replica::fn_is_local_disk_replica);
            promotion_eligible = !any_memory && any_local_disk;
        }

        resp = GetReplicaListResponse(std::move(replica_list),
                                      default_kv_lease_ttl_,
                                      metadata.object_checksum);
    }
    // RO accessor released. Safe to take a fresh RW accessor now.
    if (promotion_eligible) {
        TryPushPromotionQueue(object_id);
    }
    return resp;
}

auto MasterService::GetReplicaListForAdmin(const std::string& key,
                                           const TenantId& tenant_id)
    -> tl::expected<GetReplicaListResponse, ErrorCode> {
    assert(tenant_id.IsValid());
    const auto object_id = MakeObjectIdentity(key, tenant_id);

    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRO accessor(this, object_id);

    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    const auto& metadata = accessor.Get();

    std::vector<Replica::Descriptor> replica_list;
    metadata.VisitReplicas(
        &Replica::fn_is_completed, [&replica_list](const Replica& replica) {
            replica_list.emplace_back(replica.get_descriptor());
        });

    if (replica_list.empty()) {
        LOG(WARNING) << "key=" << key << ", error=replica_not_ready";
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    return GetReplicaListResponse(std::move(replica_list),
                                  default_kv_lease_ttl_,
                                  metadata.object_checksum);
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
MasterService::BatchGetReplicaList(const std::vector<std::string>& keys,
                                   const TenantId& tenant_id) {
    using GetResult = tl::expected<GetReplicaListResponse, ErrorCode>;

    assert(tenant_id.IsValid());

    std::vector<GetResult> results(
        keys.size(), tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
    if (keys.empty()) {
        return results;
    }

    const TenantId& normalized_tenant = ResolveRequestTenantId(tenant_id);

    // A1 + 方案 B 第二阶段：先按 slot 所有权把 keys 拆成「本地组」与
    // 「转发组（按 owner master_id 分组）」。本地组走本地批量查询，转发组
    // 通过 InterMasterRpcClient 转发给对应 slot owner。
    std::vector<size_t> local_indices;
    std::map<std::string, std::vector<size_t>> forward_groups;
    for (size_t i = 0; i < keys.size(); ++i) {
        const uint16_t slot = cvm::KeySlot(normalized_tenant, keys[i]);
        if (OwnsSlot(slot)) {
            local_indices.push_back(i);
            continue;
        }
#ifdef STORE_USE_ETCD
        auto owner = ResolveSlotOwnerMasterId(slot);
        if (owner && !owner->empty() && *owner != master_id_ &&
            inter_master_rpc_) {
            forward_groups[*owner].push_back(i);
            continue;
        }
#endif
        LOG(WARNING) << "BatchGetReplicaList: key=" << keys[i]
                     << " slot=" << slot
                     << " not owned by this master and no forward target, "
                        "rejecting with SLOT_NOT_OWNED";
        results[i] = tl::make_unexpected(ErrorCode::SLOT_NOT_OWNED);
    }

    if (!local_indices.empty()) {
        std::vector<std::string> local_keys;
        local_keys.reserve(local_indices.size());
        for (size_t idx : local_indices) {
            local_keys.push_back(keys[idx]);
        }
        auto local_results =
            BatchGetReplicaListLocal(local_keys, normalized_tenant);
        for (size_t j = 0; j < local_indices.size(); ++j) {
            results[local_indices[j]] = std::move(local_results[j]);
        }
    }

#ifdef STORE_USE_ETCD
    for (const auto& [owner, indices] : forward_groups) {
        std::vector<std::string> group_keys;
        group_keys.reserve(indices.size());
        for (size_t idx : indices) {
            group_keys.push_back(keys[idx]);
        }
        auto group_result = inter_master_rpc_->BatchGetReplicaList(
            owner, group_keys, normalized_tenant.value());
        for (size_t j = 0; j < indices.size() && j < group_result.size();
             ++j) {
            results[indices[j]] = std::move(group_result[j]);
        }
    }
#endif

    return results;
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
MasterService::BatchGetReplicaListLocal(const std::vector<std::string>& keys,
                                        const TenantId& tenant_id) {
    using GetResult = tl::expected<GetReplicaListResponse, ErrorCode>;

    assert(tenant_id.IsValid());

    std::vector<GetResult> results(
        keys.size(), tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
    if (keys.empty()) {
        return results;
    }

    const TenantId& normalized_tenant = ResolveRequestTenantId(tenant_id);
    constexpr size_t kInvalidKeyIndex = std::numeric_limits<size_t>::max();
    std::array<size_t, kNumShards> key_list_heads;
    key_list_heads.fill(kInvalidKeyIndex);
    std::vector<size_t> next_key_indexes(keys.size(), kInvalidKeyIndex);
    {
        std::shared_lock<std::shared_mutex> lock(group_routing_mutex_);
        for (size_t i = keys.size(); i > 0; --i) {
            const size_t original_idx = i - 1;
            const auto scoped_key =
                normalized_tenant.MakeScopedKey(keys[original_idx]);
            const auto route_it = object_group_ids_.find(scoped_key);
            const size_t shard_idx =
                route_it == object_group_ids_.end()
                    ? getShardIndex(normalized_tenant, keys[original_idx])
                    : getShardIndex(route_it->second);
            next_key_indexes[original_idx] = key_list_heads[shard_idx];
            key_list_heads[shard_idx] = original_idx;
        }
    }

    const size_t start_shard = randomIndex(kNumShards);
    for (size_t scanned = 0; scanned < kNumShards; ++scanned) {
        const size_t shard_idx =
            (start_shard + kNumShards - scanned) % kNumShards;
        if (key_list_heads[shard_idx] == kInvalidKeyIndex) {
            continue;
        }

        std::vector<ObjectIdentity> promotion_candidates;
        std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
        {
            MetadataShardAccessorRO shard(this, shard_idx);
            const auto tenant_it = shard->tenants.find(normalized_tenant);
            for (size_t original_idx = key_list_heads[shard_idx];
                 original_idx != kInvalidKeyIndex;
                 original_idx = next_key_indexes[original_idx]) {
                const std::string& key = keys[original_idx];

                MasterMetricManager::instance().inc_total_get_nums();

                if (tenant_it == shard->tenants.end()) {
                    VLOG(1) << "key=" << key << ", info=object_not_found";
                    results[original_idx] =
                        tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                    continue;
                }

                const auto& tenant_state = tenant_it->second;
                const auto metadata_it = tenant_state.metadata.find(key);
                if (metadata_it == tenant_state.metadata.end() ||
                    !metadata_it->second.IsValid()) {
                    VLOG(1) << "key=" << key << ", info=object_not_found";
                    results[original_idx] =
                        tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                    continue;
                }

                const auto& metadata = metadata_it->second;
                std::vector<Replica::Descriptor> replica_list;
                metadata.VisitReplicas(
                    [this](const Replica& replica) {
                        return IsReplicaReadable(replica);
                    },
                    [&replica_list](const Replica& replica) {
                        replica_list.emplace_back(replica.get_descriptor());
                    });

                if (replica_list.empty()) {
                    if (metadata.AllReplicas([](const Replica& replica) {
                            return replica.status() == ReplicaStatus::REMOVED;
                        })) {
                        results[original_idx] =
                            tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                        continue;
                    }
                    LOG(WARNING)
                        << "key=" << key << ", error=replica_not_ready";
                    results[original_idx] =
                        tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
                    continue;
                }

                if (replica_list[0].is_memory_replica()) {
                    MasterMetricManager::instance().inc_mem_cache_hit_nums();
                    MasterMetricManager::instance().inc_mem_cache_hit_bytes(
                        static_cast<int64_t>(metadata.size));
                } else if (replica_list[0].is_local_disk_replica() ||
                           replica_list[0].is_disk_replica()) {
                    MasterMetricManager::instance().inc_file_cache_hit_nums();
                    MasterMetricManager::instance().inc_file_cache_hit_bytes(
                        static_cast<int64_t>(metadata.size));
                }
                MasterMetricManager::instance().inc_valid_get_nums();
                GrantLeaseForGroup(tenant_state, key, metadata);

                if (promotion_on_hit_) {
                    const bool any_memory =
                        metadata.HasReplica(&Replica::fn_is_memory_replica);
                    const bool any_local_disk =
                        metadata.HasReplica(&Replica::fn_is_local_disk_replica);
                    if (!any_memory && any_local_disk) {
                        promotion_candidates.push_back(
                            MakeObjectIdentity(key, normalized_tenant));
                    }
                }

                results[original_idx] = GetReplicaListResponse(
                    std::move(replica_list), default_kv_lease_ttl_,
                    metadata.object_checksum);
            }
        }

        for (const auto& object_id : promotion_candidates) {
            TryPushPromotionQueue(object_id);
        }
    }

    return results;
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
MasterService::BatchGetReplicaListForAdmin(const std::vector<std::string>& keys,
                                           const TenantId& tenant_id) {
    using GetResult = tl::expected<GetReplicaListResponse, ErrorCode>;

    assert(tenant_id.IsValid());

    std::vector<GetResult> results(
        keys.size(), tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
    if (keys.empty()) {
        return results;
    }

    const TenantId& normalized_tenant = tenant_id;
    constexpr size_t kInvalidKeyIndex = std::numeric_limits<size_t>::max();
    std::array<size_t, kNumShards> key_list_heads;
    key_list_heads.fill(kInvalidKeyIndex);
    std::vector<size_t> next_key_indexes(keys.size(), kInvalidKeyIndex);
    {
        std::shared_lock<std::shared_mutex> lock(group_routing_mutex_);
        for (size_t i = keys.size(); i > 0; --i) {
            const size_t original_idx = i - 1;
            const auto scoped_key =
                normalized_tenant.MakeScopedKey(keys[original_idx]);
            const auto route_it = object_group_ids_.find(scoped_key);
            const size_t shard_idx =
                route_it == object_group_ids_.end()
                    ? getShardIndex(normalized_tenant, keys[original_idx])
                    : getShardIndex(route_it->second);
            next_key_indexes[original_idx] = key_list_heads[shard_idx];
            key_list_heads[shard_idx] = original_idx;
        }
    }

    const size_t start_shard = randomIndex(kNumShards);
    for (size_t scanned = 0; scanned < kNumShards; ++scanned) {
        const size_t shard_idx =
            (start_shard + kNumShards - scanned) % kNumShards;
        if (key_list_heads[shard_idx] == kInvalidKeyIndex) {
            continue;
        }

        std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
        {
            MetadataShardAccessorRO shard(this, shard_idx);
            const auto tenant_it = shard->tenants.find(normalized_tenant);
            for (size_t original_idx = key_list_heads[shard_idx];
                 original_idx != kInvalidKeyIndex;
                 original_idx = next_key_indexes[original_idx]) {
                const std::string& key = keys[original_idx];

                if (tenant_it == shard->tenants.end()) {
                    results[original_idx] =
                        tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                    continue;
                }

                const auto& tenant_state = tenant_it->second;
                const auto metadata_it = tenant_state.metadata.find(key);
                if (metadata_it == tenant_state.metadata.end() ||
                    !metadata_it->second.IsValid()) {
                    results[original_idx] =
                        tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                    continue;
                }

                const auto& metadata = metadata_it->second;
                std::vector<Replica::Descriptor> replica_list;
                metadata.VisitReplicas(
                    &Replica::fn_is_completed,
                    [&replica_list](const Replica& replica) {
                        replica_list.emplace_back(replica.get_descriptor());
                    });

                if (replica_list.empty()) {
                    results[original_idx] =
                        tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
                    continue;
                }

                results[original_idx] = GetReplicaListResponse(
                    std::move(replica_list), default_kv_lease_ttl_,
                    metadata.object_checksum);
            }
        }
    }

    return results;
}

auto MasterService::AllocateAndInsertMetadata(
    MetadataShardAccessorRW& shard, const UUID& client_id,
    const std::string& key, uint64_t value_length,
    const ReplicateConfig& config, const std::string& group_id,
    const TenantId& tenant_id, const std::chrono::system_clock::time_point& now)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    auto& tenant_state = shard->tenants[tenant_id];
    if (tenant_state.metadata.contains(key)) {
        LOG(INFO) << "key=" << key << ", info=object_already_exists";
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }
    if (GetGroupRoute(tenant_id, key).has_value()) {
        LOG(INFO) << "key=" << key << ", info=object_already_exists";
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }

    const uint64_t reserved_quota_charge =
        RequestedMemoryQuotaCharge(value_length, config);
    auto quota_result = ReserveTenantQuota(tenant_id, reserved_quota_charge);
    if (!quota_result) {
        return tl::make_unexpected(quota_result.error());
    }
    auto abort_reserved_quota = [&] {
        AbortTenantQuota(tenant_id, reserved_quota_charge);
    };

    std::vector<Replica> replicas;
    const auto write_mode = DetermineReplicaWriteMode(config);
    size_t allocated_memory_replicas = 0;
    size_t allocated_nof_replicas = 0;
    bool memory_eviction_may_help = false;
    if (config.replica_num > 0) {
        const bool use_local_first =
            allocation_strategy_type_ == AllocationStrategyType::LOCAL_FIRST &&
            config.replica_num == 1;
        std::string writer_host_id;
        if (use_local_first) {
            writer_host_id = config.host_id.empty() ? GetClientHostId(client_id)
                                                    : config.host_id;
        }

        std::vector<std::string> preferred_segments;
        tl::expected<std::vector<Replica>, ErrorCode> allocation_result =
            tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        {
            ScopedAllocatorAccess allocator_access =
                segment_manager_.getAllocatorAccess();
            const auto& allocator_manager =
                allocator_access.getAllocatorManager();
            if (allocator_manager.getNames().size() >= config.replica_num) {
                for (const auto& name : allocator_manager.getNames()) {
                    const auto* allocators =
                        allocator_manager.getAllocators(name);
                    if (allocators != nullptr &&
                        std::any_of(allocators->begin(), allocators->end(),
                                    [](const auto& allocator) {
                                        return allocator &&
                                               allocator->size() > 0;
                                    })) {
                        memory_eviction_may_help = true;
                        break;
                    }
                }
            }

            auto append_preferred_segment = [&preferred_segments](
                                                const std::string&
                                                    segment_name) {
                if (!segment_name.empty() &&
                    std::find(preferred_segments.begin(),
                              preferred_segments.end(),
                              segment_name) == preferred_segments.end()) {
                    preferred_segments.push_back(segment_name);
                }
            };
            if (!config.preferred_segment.empty()) {
                append_preferred_segment(config.preferred_segment);
            } else {
                for (const auto& preferred_segment :
                     config.preferred_segments) {
                    append_preferred_segment(preferred_segment);
                }
            }
            if (!writer_host_id.empty()) {
                auto host_ordered_segments =
                    allocator_access.GetHostOrderedSegments(writer_host_id,
                                                            key);
                for (const auto& segment_name : host_ordered_segments) {
                    append_preferred_segment(segment_name);
                }
                if (!host_ordered_segments.empty()) {
                    VLOG(1) << "key=" << key
                            << ", writer_host_id=" << writer_host_id
                            << ", local_first_preferred_segments="
                            << host_ordered_segments.size();
                }
            }

            const SsdMetricsProvider* ssd_provider = nullptr;
            std::optional<ScopedLocalDiskSegmentAccess> ssd_access;
            if (allocation_strategy_type_ ==
                AllocationStrategyType::SSD_FREE_RATIO_FIRST) {
                ssd_access.emplace(
                    segment_manager_.getLocalDiskSegmentAccess());
                ssd_provider = &*ssd_access;
            }

            SpDiag::PerfPoint pt_alloc_mem(PerfKey::MASTER_PUT_ALLOCATE_MEM,
                                           SpDiag::PerfLevel::KEY_MODULE);
            pt_alloc_mem.Start();
            allocation_result = allocation_strategy_->Allocate(
                allocator_manager, value_length, config.replica_num,
                preferred_segments, std::set<std::string>(),
                ReplicaType::MEMORY, ssd_provider);
            pt_alloc_mem.End(allocation_result.has_value() ? 0 : -1);
        }  // allocator_access 在此释放；远程转发在锁外执行，避免分布式死锁

        if (!allocation_result.has_value()) {
            VLOG(1) << "Failed to allocate replicas locally for key=" << key
                    << ", error: " << allocation_result.error();
            // CVM 多 submaster（方案 B 第二阶段）：本机本地分配失败（未挂载
            // segment 或空间不足）时，尝试向持有该 segment 的 peer submaster
            // 转发分配。远程分配成功则以 dummy-allocator 副本物化到本机元数据，
            // 真实句柄留在 segment owner 的 keepalive 注册表中。
            auto remote_replicas = TryAllocateReplicasRemotely(
                key, tenant_id, value_length, config.replica_num,
                preferred_segments);
            if (remote_replicas.has_value()) {
                replicas = std::move(remote_replicas.value());
                allocated_memory_replicas = replicas.size();
            } else {
                if (allocation_result.error() == ErrorCode::INVALID_PARAMS) {
                    abort_reserved_quota();
                    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                }
                if (write_mode != ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
                    MasterMetricManager::instance()
                        .inc_put_start_alloc_failures();
                    if (memory_eviction_may_help) {
                        need_mem_eviction_ = true;
                    }
                    abort_reserved_quota();
                    return tl::make_unexpected(
                        ErrorCode::NO_AVAILABLE_HANDLE);
                }
            }
        } else {
            allocated_memory_replicas = allocation_result->size();
            replicas = std::move(allocation_result.value());
        }
    }

#ifdef USE_NOF
    if (config.nof_replica_num > 0 &&
        nof_segment_manager_.getMountedSegmentCount() > 0) {
        ScopedAllocatorAccess allocator_access =
            nof_segment_manager_.getAllocatorAccess();
        const auto& allocator_manager = allocator_access.getAllocatorManager();

        std::vector<std::string> preferred_segments =
            config.preferred_nof_segments;

        SpDiag::PerfPoint pt_alloc_nof(PerfKey::MASTER_PUT_ALLOCATE_NOF,
                                       SpDiag::PerfLevel::KEY_MODULE);
        pt_alloc_nof.Start();
        auto allocation_result = allocation_strategy_->Allocate(
            allocator_manager, value_length, config.nof_replica_num,
            preferred_segments, std::set<std::string>(), ReplicaType::NOF_SSD);
        pt_alloc_nof.End(allocation_result.has_value() ? 0 : -1);

        if (!allocation_result.has_value()) {
            VLOG(1) << "Failed to allocate nof replicas for key=" << key
                    << ", error: " << allocation_result.error();
            if (allocation_result.error() == ErrorCode::INVALID_PARAMS) {
                abort_reserved_quota();
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            if (write_mode != ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
                MasterMetricManager::instance().inc_put_start_alloc_failures();
                need_nof_eviction_ = true;
                abort_reserved_quota();
                return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
            }
        } else {
            allocated_nof_replicas = allocation_result->size();
            for (auto& replica : allocation_result.value()) {
                replicas.push_back(std::move(replica));
            }
        }
    }
#endif

    if (!HasExpectedReplicaAllocation(config, allocated_memory_replicas,
                                      allocated_nof_replicas,
                                      strict_replica_allocation_)) {
        if ((config.replica_num > 0 &&
             allocated_memory_replicas != config.replica_num) ||
            (config.nof_replica_num > 0 &&
             allocated_nof_replicas != config.nof_replica_num)) {
            MasterMetricManager::instance().inc_put_start_alloc_failures();
            if (config.replica_num > 0 &&
                allocated_memory_replicas != config.replica_num &&
                memory_eviction_may_help) {
                need_mem_eviction_ = true;
            }
            if (config.nof_replica_num > 0 &&
                allocated_nof_replicas != config.nof_replica_num) {
                need_nof_eviction_ = true;
            }
        }
        VLOG(1) << "Failed to satisfy replica allocation requirement for key="
                << key << ", requested_memory_replicas=" << config.replica_num
                << ", allocated_memory_replicas=" << allocated_memory_replicas
                << ", requested_nof_replicas=" << config.nof_replica_num
                << ", allocated_nof_replicas=" << allocated_nof_replicas;
        abort_reserved_quota();
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    // Best-effort / flexible modes may pass the check above with fewer
    // replicas than requested (see HasExpectedReplicaAllocation). Surface
    // the degradation so callers and operators can detect the reduced
    // redundancy instead of failing silently.
    if (allocated_memory_replicas < config.replica_num ||
        allocated_nof_replicas < config.nof_replica_num) {
        MasterMetricManager::instance().inc_put_start_partial_allocations();
        LOG(WARNING) << "key=" << key << ", action=put_start_partial_allocation"
                     << ", requested_memory_replicas=" << config.replica_num
                     << ", allocated_memory_replicas="
                     << allocated_memory_replicas
                     << ", requested_nof_replicas=" << config.nof_replica_num
                     << ", allocated_nof_replicas=" << allocated_nof_replicas;
    }

    if (use_disk_replica_) {
        std::string file_path =
            ResolvePathFromKey(key, root_fs_dir_, cluster_id_);
        replicas.emplace_back(file_path, value_length,
                              ReplicaStatus::PROCESSING);
    }

    std::vector<Replica::Descriptor> replica_list;
    replica_list.reserve(replicas.size());
    int i = 0;
    MC_VLOG(1) << "PutStart, create replicas: client_id=" << client_id
               << ", key=" << key << ", value_length=" << value_length;
    for (const auto& replica : replicas) {
        const auto desc = replica.get_descriptor();
        replica_list.emplace_back(desc);

        if (replica.is_memory_replica()) {
            const auto& mem_desc = desc.get_memory_descriptor();
            MC_VLOG(1) << "Replica #" << ++i << ": buffer_address="
                       << mem_desc.buffer_descriptor.buffer_address_
                       << ", transport_endpoint="
                       << mem_desc.buffer_descriptor.transport_endpoint_;
        } else if (replica.is_nof_replica()) {
            const auto& nof_desc = desc.get_nof_descriptor();
            MC_VLOG(1) << "Replica #" << ++i << ": buffer_address="
                       << nof_desc.buffer_descriptor.buffer_address_
                       << ", transport_endpoint="
                       << nof_desc.buffer_descriptor.transport_endpoint_;
        }
    }

    auto [it, inserted] = tenant_state.metadata.emplace(
        std::piecewise_construct, std::forward_as_tuple(key),
        std::forward_as_tuple(client_id, now, value_length, std::move(replicas),
                              config.with_soft_pin, config.with_hard_pin,
                              config.data_type, group_id, tenant_id, key));
    if (!inserted) {
        LOG(INFO) << "key=" << key << ", info=object_already_exists";
        abort_reserved_quota();
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }
    IncrementTenantMetadataObjectCount(tenant_id);
    it->second.reserved_quota_charge_bytes = reserved_quota_charge;
    RegisterGroupMember(tenant_state, tenant_id, key, group_id);
    tenant_state.processing_keys.insert(key);

    return replica_list;
}

auto MasterService::PutStart(const UUID& client_id, const std::string& key,
                             const TenantId& tenant_id,
                             const uint64_t slice_length,
                             const ReplicateConfig& config)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    auto normalized_tenant_result = ResolveTenantIdForWrite(tenant_id);
    if (!normalized_tenant_result) {
        return tl::make_unexpected(normalized_tenant_result.error());
    }
    const ObjectIdentity object_id{std::move(normalized_tenant_result.value()),
                                   key};
    if ((config.replica_num == 0 && config.nof_replica_num == 0) ||
        key.empty() || slice_length == 0) {
        LOG(ERROR) << "key=" << key << ", replica_num=" << config.replica_num
                   << ", nof_replica_num=" << config.nof_replica_num
                   << ", slice_length=" << slice_length
                   << ", key_size=" << key.size() << ", error=invalid_params";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.prefer_alloc_in_same_node && config.nof_replica_num > 0) {
        LOG(ERROR) << "key=" << key
                   << ", nof_replica_num=" << config.nof_replica_num
                   << ", prefer_alloc_in_same_node="
                   << config.prefer_alloc_in_same_node
                   << ", error=nof_not_supported_with_prefer_same_node";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
#ifndef USE_NOF
    if (config.nof_replica_num > 0) {
        LOG(ERROR) << "key=" << key
                   << ", nof_replica_num=" << config.nof_replica_num
                   << ", error=nof_pool_disabled";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
#endif

    UpdateClientHostId(client_id, config.host_id);

#ifdef STORE_USE_ETCD
    // 写路径 slot 归属校验 + 完整转发（模型 B）：本机若非该 key 的 slot
    // owner，不得在本地写入元数据（否则元数据落在"非环 owner"上，而读路径
    // 按环归属，会造成"写后读不到/写到错误位置"的不一致）。此处把整个
    // PutStart 转发给 slot owner，由其本地分配 + 写元数据并返回 Descriptor。
    // owner 侧 peer 互信（OwnsSlot==true）不再二次转发，转发链止于第一跳。
    {
        const uint16_t slot =
            cvm::KeySlot(object_id.tenant_id, object_id.user_key);
        if (!OwnsSlot(slot)) {
            auto owner = ResolveSlotOwnerMasterId(slot);
            if (owner && !owner->empty() && *owner != master_id_ &&
                inter_master_rpc_) {
                LOG(INFO) << "PutStart forwarded: key=" << object_id.user_key
                          << " slot=" << slot << " -> owner=" << *owner;
                return inter_master_rpc_->PutStart(
                    *owner, client_id, object_id.user_key,
                    object_id.tenant_id.value(), slice_length, config);
            }
            LOG(INFO) << "PutStart rejected with SLOT_NOT_OWNED: key="
                      << object_id.user_key << " slot=" << slot;
            return tl::make_unexpected(ErrorCode::SLOT_NOT_OWNED);
        }
    }
#endif

    if ((memory_allocator_type_ == BufferAllocatorType::CACHELIB) &&
        (slice_length > kMaxSliceSize)) {
        LOG(ERROR) << "key=" << key << ", slice_length=" << slice_length
                   << ", max_size=" << kMaxSliceSize
                   << ", error=invalid_slice_size";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    MC_VLOG(1) << "key=" << key << ", value_length=" << slice_length
               << ", config=" << config << ", action=put_start_begin";

    auto group_id_result = GetGroupIdForKey(config, 1, 0);
    if (!group_id_result) {
        return tl::make_unexpected(group_id_result.error());
    }
    const std::string group_id = group_id_result.value();

    [[maybe_unused]] auto object_operation_lock =
        AcquireObjectOperationLock(object_id.tenant_id, object_id.user_key);
    const uint64_t requested_quota_charge =
        RequestedMemoryQuotaCharge(slice_length, config);

    auto attempt_once =
        [&]() -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
        std::unique_lock<std::mutex> zero_charge_policy_lock(
            tenant_quota_policy_mutex_, std::defer_lock);
        if (ShouldProtectZeroChargeMetadataCreate(requested_quota_charge)) {
            zero_charge_policy_lock.lock();
            auto latest_tenant_result =
                ResolveTenantIdForWriteLocked(tenant_id);
            if (!latest_tenant_result) {
                return tl::make_unexpected(latest_tenant_result.error());
            }
        }

        auto now = std::chrono::system_clock::now();
        std::optional<size_t> retry_shard_idx;
        {
            auto alive_clients = getAliveClientsSnapshot();
            std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
            const size_t lookup_shard_idx =
                getMetadataShardIndex(object_id.tenant_id, object_id.user_key);
            MetadataShardAccessorRW shard(this, lookup_shard_idx);
            auto& tenant_state = shard->tenants[object_id.tenant_id];

            auto it = tenant_state.metadata.find(key);
            if (it != tenant_state.metadata.end()) {
                auto cleanup_plan =
                    BuildStaleHandleCleanupPlan(it->second, alive_clients);
                if (!cleanup_plan.removed_ids.empty()) {
                    auto persist_result = PersistStaleHandleCleanupForHA(
                        "PutStart(stale cleanup)", object_id.tenant_id, key,
                        it->second, cleanup_plan);
                    if (!persist_result) {
                        return tl::make_unexpected(persist_result.error());
                    }
                    if (enable_oplog_) {
                        return tl::make_unexpected(
                            ErrorCode::OBJECT_ALREADY_EXISTS);
                    } else if (CleanupStaleHandles(it->second, alive_clients,
                                                   &shard)) {
                        EraseMetadata(tenant_state, it, object_id.tenant_id,
                                      QuotaEraseMode::kFull, &shard);
                        it = tenant_state.metadata.end();
                    }
                }
                if (it != tenant_state.metadata.end()) {
                    auto& metadata = it->second;
                    if (metadata.HasReplica(&Replica::fn_is_completed) ||
                        metadata.put_start_time +
                                put_start_discard_timeout_sec_ >=
                            now) {
                        LOG(INFO)
                            << "key=" << key << ", info=object_already_exists";
                        return tl::make_unexpected(
                            ErrorCode::OBJECT_ALREADY_EXISTS);
                    }
                    if (enable_oplog_ && ordered_oplog_writer_) {
                        auto err =
                            PersistRemoveForHA("PutStart(stale cleanup REMOVE)",
                                               object_id.tenant_id, key);
                        if (!err) {
                            return tl::make_unexpected(err.error());
                        }
                    }
                    auto replicas = PopReplicasWithCacheTotalAccounting(
                        metadata, &Replica::fn_is_processing);
                    if (!replicas.empty()) {
                        std::lock_guard lock(discarded_replicas_mutex_);
                        discarded_replicas_.emplace_back(
                            std::move(replicas),
                            metadata.put_start_time +
                                put_start_release_timeout_sec_);
                    }
                    EraseMetadata(tenant_state, it, object_id.tenant_id,
                                  QuotaEraseMode::kFull, &shard);
                    it = tenant_state.metadata.end();
                }
            }

            if (it == tenant_state.metadata.end()) {
                const size_t target_shard_idx =
                    group_id.empty()
                        ? getShardIndex(object_id.tenant_id, object_id.user_key)
                        : getShardIndex(group_id);
                if (target_shard_idx != lookup_shard_idx) {
                    retry_shard_idx = target_shard_idx;
                    if (tenant_state.Empty()) {
                        shard->tenants.erase(object_id.tenant_id);
                    }
                } else {
                    return AllocateAndInsertMetadata(
                        shard, client_id, key, slice_length, config, group_id,
                        object_id.tenant_id, now);
                }
            }
        }

        std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
        MetadataShardAccessorRW shard(this, retry_shard_idx.value());
        auto& retry_tenant_state = shard->tenants[object_id.tenant_id];
        if (GetGroupRoute(object_id.tenant_id, object_id.user_key)
                .has_value() ||
            retry_tenant_state.metadata.contains(key)) {
            LOG(INFO) << "key=" << key << ", info=object_already_exists";
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }
        return AllocateAndInsertMetadata(shard, client_id, key, slice_length,
                                         config, group_id, object_id.tenant_id,
                                         now);
    };

    for (int attempt = 0; attempt <= kMaxTenantQuotaEvictionRetries;
         ++attempt) {
        auto result = attempt_once();
        if (result.has_value() ||
            result.error() != ErrorCode::TENANT_QUOTA_EXCEEDED) {
            return result;
        }
        if (attempt == kMaxTenantQuotaEvictionRetries) {
            MasterMetricManager::instance().inc_tenant_quota_reject(
                object_id.tenant_id.value(), "quota_exceeded");
            return result;
        }
        EvictTenantMemoryForQuota(
            object_id.tenant_id,
            tenant_quota_table_.ComputeDeficit(object_id.tenant_id,
                                               requested_quota_charge));
    }
    return tl::make_unexpected(ErrorCode::TENANT_QUOTA_EXCEEDED);
}

auto MasterService::PutEnd(const UUID& client_id, const ObjectMeta& object_meta,
                           const TenantId& tenant_id, ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    const auto& key = object_meta.key;
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();
    if (client_id != metadata.client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to PutEnd key " << key
                   << ", was PutStart-ed by " << metadata.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    metadata.VisitReplicas(
        [replica_type](const Replica& replica) {
            if (replica_type == ReplicaType::ALL) {
                return (replica.is_memory_replica() &&
                        !replica.has_invalid_mem_handle()) ||
                       (replica.is_nof_replica() &&
                        !replica.has_invalid_nof_handle());
            }
            if (replica_type == ReplicaType::MEMORY) {
                return replica.is_memory_replica() &&
                       !replica.has_invalid_mem_handle();
            }
            if (replica_type == ReplicaType::NOF_SSD) {
                return replica.is_nof_replica() &&
                       !replica.has_invalid_nof_handle();
            }
            return replica.type() == replica_type;
        },
        [](Replica& replica) { replica.mark_complete(); });

    if (object_meta.object_checksum.has_value() ||
        replica_type == ReplicaType::ALL ||
        replica_type == ReplicaType::MEMORY ||
        replica_type == ReplicaType::NOF_SSD) {
        metadata.object_checksum = object_meta.object_checksum;
    }

    const bool has_memory_replica = metadata.HasMemReplica();
    const bool should_settle_quota =
        replica_type == ReplicaType::MEMORY ||
        (replica_type == ReplicaType::ALL && has_memory_replica) ||
        !has_memory_replica;
    if (metadata.reserved_quota_charge_bytes > 0 && should_settle_quota) {
        const uint64_t actual_charge = CompletedMemoryQuotaCharge(metadata);
        const uint64_t commit_charge =
            actual_charge > metadata.committed_quota_charge_bytes
                ? actual_charge - metadata.committed_quota_charge_bytes
                : 0;
        const uint64_t abort_charge =
            metadata.reserved_quota_charge_bytes > commit_charge
                ? metadata.reserved_quota_charge_bytes - commit_charge
                : 0;
        CommitTenantQuota(object_id.tenant_id, commit_charge);
        AbortTenantQuota(object_id.tenant_id, abort_charge);
        metadata.reserved_quota_charge_bytes = 0;
        metadata.committed_quota_charge_bytes = actual_charge;
        ReleaseTenantQuota(object_id.tenant_id,
                           metadata.pending_replaced_quota_charge_bytes);
        metadata.pending_replaced_quota_charge_bytes = 0;
    }

    if (enable_offload_ && !offload_on_evict_) {
        auto& tenant_state = accessor.GetTenantState();
        metadata.VisitReplicas(
            [](const Replica& replica) {
                return replica.is_completed() && replica.is_memory_replica();
            },
            [this, &object_id, &tenant_state](Replica& replica) {
                auto result = PushOffloadingQueue(object_id, replica);
                if (!result) {
                    return;
                }
                auto& tasks = tenant_state.offloading_tasks[object_id.user_key];
                const auto now = std::chrono::system_clock::now();
                for (const auto& client_id : result.value()) {
                    replica.inc_refcnt();
                    tasks.push_back(
                        OffloadingTask{replica.id(), now, client_id});
                }
            });
    }

    // If the object is completed, remove it from the processing set.
    if (metadata.AllReplicas(&Replica::fn_is_completed) &&
        accessor.InProcessing()) {
        accessor.EraseFromProcessing();
    }

    SyncCacheTotalAccounting(metadata);
    // TODO: add inc_nof_cache_nums() (ranhaojia)
    // 1. Set lease timeout to now, indicating that the object has no lease
    // at beginning. 2. If this object has soft pin enabled, set it to be soft
    // pinned.
    metadata.GrantLease(0, default_kv_soft_pin_ttl_);
    PublishKvStored(key, replica_type, metadata, object_id.tenant_id);

    if (enable_oplog_ && ordered_oplog_writer_) {
        std::string payload = SerializeMetadataForOpLog(metadata);
        auto result = AppendOpLogVisibleBeforeDurable(
            OpType::PUT_END, object_id.tenant_id.value(), key, payload);
        if (!result) {
            LOG(WARNING) << "PutEnd: OpLog queue failed for key=" << key
                         << ", err=" << static_cast<int>(result.error());
        }
    }
    return {};
}

auto MasterService::AddReplica(const UUID& client_id, const std::string& key,
                               const TenantId& tenant_id, Replica& replica)
    -> tl::expected<bool, ErrorCode> {
    assert(tenant_id.IsValid());
    TenantId normalized_tenant;
    std::unique_lock<std::mutex> policy_lock(tenant_quota_policy_mutex_,
                                             std::defer_lock);
    if (enable_multi_tenants_) {
        policy_lock.lock();
        auto normalized_tenant_result =
            ResolveTenantIdForWriteLocked(tenant_id);
        if (!normalized_tenant_result) {
            return tl::make_unexpected(normalized_tenant_result.error());
        }
        normalized_tenant = std::move(normalized_tenant_result.value());
    }
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const ObjectIdentity object_id{std::move(normalized_tenant), key};
    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        accessor.Create(
            client_id,
            replica.get_descriptor().get_local_disk_descriptor().object_size,
            std::vector<Replica>{}, false);
    }
    auto& metadata = accessor.Get();
    if (replica.type() != ReplicaType::LOCAL_DISK) {
        LOG(ERROR) << "Invalid replica type: " << replica.type()
                   << ". Expected ReplicaType::LOCAL_DISK.";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    const bool replacing_existing =
        metadata.HasReplica(&Replica::fn_is_local_disk_replica);

    // Build OPLog payload BEFORE moving the replica, so that
    // get_descriptor() is still valid on `replica`.
    std::string oplog_payload;
    bool oplog_required = false;
    if (enable_oplog_ && ordered_oplog_writer_) {
        std::vector<Replica::Descriptor> post;
        for (const auto& existing : metadata.GetAllReplicas()) {
            if (existing.status() != ReplicaStatus::COMPLETE) continue;
            if (replacing_existing &&
                existing.type() == ReplicaType::LOCAL_DISK &&
                existing.get_descriptor()
                        .get_local_disk_descriptor()
                        .client_id == client_id) {
                // Substitute with the updated descriptor.
                Replica::Descriptor updated = existing.get_descriptor();
                updated.get_local_disk_descriptor().transport_endpoint =
                    replica.get_descriptor()
                        .get_local_disk_descriptor()
                        .transport_endpoint;
                updated.get_local_disk_descriptor().object_size =
                    replica.get_descriptor()
                        .get_local_disk_descriptor()
                        .object_size;
                post.push_back(std::move(updated));
            } else {
                post.push_back(existing.get_descriptor());
            }
        }
        if (!replacing_existing) {
            // The new LOCAL_DISK replica is COMPLETE upon AddReplica.
            post.push_back(replica.get_descriptor());
        }
        oplog_payload = SerializeMetadataForOpLogFromReplicaDescriptors(
            metadata.client_id, metadata.size, post, metadata.group_id,
            metadata.data_type);
        oplog_required = true;
    }

    // Step 1: Update metadata first for LOCAL_DISK replicas.
    // This ensures the replica is registered immediately, even when the
    // OPLog writer is backed up (e.g. during standby promotion recovery).
    // LOCAL_DISK is a secondary replica type: losing its OPLog entry is
    // recoverable because the client will re-register on remount.
    if (!replacing_existing) {
        std::vector<Replica> replicas;
        replicas.emplace_back(std::move(replica));
        metadata.AddReplicas(std::move(replicas));
        auto& shard = accessor.GetShard();
        shard.OnDiskReplicaAdded(metadata);
        SyncCacheTotalAccounting(metadata);

        // Step 2: Best-effort OPLog write.
        if (oplog_required) {
            auto persist_result = AppendOpLogVisibleBeforeDurable(
                OpType::PUT_END, object_id.tenant_id.value(), key,
                oplog_payload);
            if (!persist_result) {
                LOG(WARNING) << "AddReplica: OpLog skipped for local_disk"
                             << " (metadata already updated), key=" << key
                             << ", err="
                             << static_cast<int>(persist_result.error());
            }
        }
        return true;
    }

    // Replace-existing path: update the existing LOCAL_DISK replica.
    // Record every LOCAL_DISK replica per owning client. First try to refresh
    // an existing replica owned by THIS client (idempotent re-offload or
    // restart re-registration only changes the endpoint/size). If this client
    // has no LOCAL_DISK replica yet, append a new one so that a key offloaded
    // by multiple nodes keeps one LOCAL_DISK replica per node.
    size_t updated = metadata.VisitReplicas(
        [client_id](const Replica& rep) {
            return rep.type() == ReplicaType::LOCAL_DISK &&
                   rep.get_descriptor().get_local_disk_descriptor().client_id ==
                       client_id;
        },
        [&replica](Replica& rep) {
            const auto desc =
                replica.get_descriptor().get_local_disk_descriptor();
            rep.update_local_disk_location(desc.transport_endpoint,
                                           desc.object_size);
        });

    if (updated == 0) {
        std::vector<Replica> replicas;
        replicas.emplace_back(std::move(replica));
        metadata.AddReplicas(std::move(replicas));
    }

    // Best-effort OPLog write for replace-existing path.
    if (oplog_required) {
        auto persist_result = AppendOpLogVisibleBeforeDurable(
            OpType::PUT_END, object_id.tenant_id.value(), key, oplog_payload);
        if (!persist_result) {
            LOG(WARNING) << "AddReplica: OpLog skipped for local_disk"
                         << " (metadata already updated), key=" << key
                         << ", err=" << static_cast<int>(persist_result.error());
        }
    }
    return false;
}

auto MasterService::PutRevoke(const UUID& client_id, const std::string& key,
                              const TenantId& tenant_id,
                              ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        MC_LOG(INFO) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();
    if (client_id != metadata.client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to PutRevoke key "
                   << key << ", was PutStart-ed by " << metadata.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    auto processing_rep = metadata.GetFirstReplica([replica_type](
                                                       const Replica& replica) {
        if (replica_type == ReplicaType::ALL) {
            return (replica.is_memory_replica() || replica.is_nof_replica()) &&
                   !replica.is_processing();
        }
        return replica.type() == replica_type && !replica.is_processing();
    });
    if (processing_rep != nullptr) {
        LOG(ERROR) << "key=" << key << ", status=" << processing_rep->status()
                   << ", error=invalid_replica_status";
        return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    }

    auto target_pred = [replica_type](const Replica& r) {
        if (replica_type == ReplicaType::ALL) {
            return r.is_memory_replica() || r.is_nof_replica();
        }
        return r.type() == replica_type;
    };

    if (enable_oplog_ && ordered_oplog_writer_) {
        auto remaining =
            BuildRemainingReplicaDescriptors(metadata, target_pred);
        std::vector<ReplicaID> removed_ids;
        auto reservation = ReserveBatchOpLogSlot();
        if (!reservation) {
            return tl::make_unexpected(reservation.error());
        }
        metadata.VisitReplicas(target_pred, [&removed_ids](Replica& r) {
            removed_ids.push_back(r.id());
            r.mark_removed();
        });

        tl::expected<OpLogEntry, ErrorCode> persist_result;
        if (remaining.empty()) {
            persist_result = AppendReservedOpLogWithDurableFinalize(
                std::move(reservation.value()), OpType::REMOVE,
                tenant_id.value(), key, {},
                [this, removed_ids = std::move(removed_ids)](
                    const OpLogEntry& durable_entry) {
                    FinalizeRemovedReplicasAfterDurable(
                        durable_entry, removed_ids, QuotaEraseMode::kFull);
                });
        } else {
            persist_result = AppendReservedOpLogWithDurableFinalize(
                std::move(reservation.value()), OpType::PUT_END,
                tenant_id.value(), key,
                SerializeMetadataForOpLogFromReplicaDescriptors(
                    metadata.client_id, metadata.size, remaining,
                    metadata.group_id, metadata.data_type),
                [this, removed_ids = std::move(removed_ids)](
                    const OpLogEntry& durable_entry) {
                    FinalizeRemovedReplicasAfterDurable(
                        durable_entry, removed_ids, QuotaEraseMode::kFull);
                });
        }
        if (!persist_result) {
            return tl::make_unexpected(persist_result.error());
        }
        return {};
    }

    const uint64_t before_charge = CompletedMemoryQuotaCharge(metadata);
    EraseReplicasWithCacheTotalAccounting(metadata, target_pred);
    const uint64_t after_charge = CompletedMemoryQuotaCharge(metadata);
    if (before_charge > after_charge) {
        ReleaseCommittedQuotaCharge(metadata, before_charge - after_charge);
    }
    if (!metadata.HasReplica(&Replica::fn_is_memory_replica)) {
        AbortTenantQuota(object_id.tenant_id,
                         metadata.reserved_quota_charge_bytes);
        metadata.reserved_quota_charge_bytes = 0;
    }

    // If the object is completed, remove it from the processing set.
    if (metadata.AllReplicas(&Replica::fn_is_completed) &&
        accessor.InProcessing()) {
        accessor.EraseFromProcessing();
    }

    if (metadata.IsValid() == false) {
        accessor.Erase();
    }
    return {};
}

auto MasterService::PutEnd(const UUID& client_id, const std::string& key,
                           const TenantId& tenant_id, ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    return PutEnd(client_id, ObjectMeta{key, std::nullopt}, tenant_id,
                  replica_type);
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchPutEnd(
    const UUID& client_id, const std::vector<ObjectMeta>& object_metas,
    const TenantId& tenant_id, ReplicaType replica_type) {
    assert(tenant_id.IsValid());
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(object_metas.size());
    for (const auto& object_meta : object_metas) {
        results.emplace_back(
            PutEnd(client_id, object_meta, tenant_id, replica_type));
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchPutRevoke(
    const UUID& client_id, const std::vector<std::string>& keys,
    const TenantId& tenant_id, ReplicaType replica_type) {
    assert(tenant_id.IsValid());
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(
            PutRevoke(client_id, key, tenant_id, replica_type));
    }
    return results;
}

// UpsertStart — insert-or-update entry point.
//
// Three-way dispatch depending on key state:
//   Case A: key does not exist  → allocate new buffers (same as PutStart)
//   Case B: key exists, same size → in-place update (reuse existing buffers)
//   Case C: key exists, different size → discard old + allocate new
//
// Before reaching Case B/C the function runs safety checks and may preempt
// an in-progress Put/Upsert on the same key.  Preempted PROCESSING replicas
// are moved to discarded_replicas_ for delayed release (the previous writer
// may still be performing RDMA writes to those buffers).
//
// Note: during Case B the key is temporarily unreadable (all replicas are
// PROCESSING).  Readers will get REPLICA_IS_NOT_READY until UpsertEnd.
auto MasterService::UpsertStart(const UUID& client_id, const std::string& key,
                                const TenantId& tenant_id,
                                const uint64_t slice_length,
                                const ReplicateConfig& config)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    auto normalized_tenant_result = ResolveTenantIdForWrite(tenant_id);
    if (!normalized_tenant_result) {
        return tl::make_unexpected(normalized_tenant_result.error());
    }
    const ObjectIdentity object_id{std::move(normalized_tenant_result.value()),
                                   key};
    // --- Parameter validation (same as PutStart) ---
    if ((config.replica_num == 0 && config.nof_replica_num == 0) ||
        key.empty() || slice_length == 0) {
        LOG(ERROR) << "key=" << key << ", replica_num=" << config.replica_num
                   << ", nof_replica_num=" << config.nof_replica_num
                   << ", slice_length=" << slice_length
                   << ", key_size=" << key.size() << ", error=invalid_params";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.prefer_alloc_in_same_node && config.nof_replica_num > 0) {
        LOG(ERROR) << "key=" << key
                   << ", nof_replica_num=" << config.nof_replica_num
                   << ", prefer_alloc_in_same_node="
                   << config.prefer_alloc_in_same_node
                   << ", error=nof_not_supported_with_prefer_same_node";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
#ifndef USE_NOF
    if (config.nof_replica_num > 0) {
        LOG(ERROR) << "key=" << key
                   << ", nof_replica_num=" << config.nof_replica_num
                   << ", error=nof_pool_disabled";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
#endif

    UpdateClientHostId(client_id, config.host_id);

#ifdef STORE_USE_ETCD
    // 写路径 slot 归属校验 + 完整转发（模型 B）：Upsert 为覆盖式写，一旦
    // client 路由过期把请求发到非 slot owner，错位覆盖比 PutStart 危害更大。
    // 此处与 PutStart 对称：把整个 UpsertStart 转发给 slot owner 执行。
    // owner 侧 peer 互信（OwnsSlot==true）不再二次转发，转发链止于第一跳。
    {
        const uint16_t slot =
            cvm::KeySlot(object_id.tenant_id, object_id.user_key);
        if (!OwnsSlot(slot)) {
            auto owner = ResolveSlotOwnerMasterId(slot);
            if (owner && !owner->empty() && *owner != master_id_ &&
                inter_master_rpc_) {
                LOG(INFO) << "UpsertStart forwarded: key=" << object_id.user_key
                          << " slot=" << slot << " -> owner=" << *owner;
                return inter_master_rpc_->UpsertStart(
                    *owner, client_id, object_id.user_key,
                    object_id.tenant_id.value(), slice_length, config);
            }
            LOG(INFO) << "UpsertStart rejected with SLOT_NOT_OWNED: key="
                      << object_id.user_key << " slot=" << slot;
            return tl::make_unexpected(ErrorCode::SLOT_NOT_OWNED);
        }
    }
#endif

    if ((memory_allocator_type_ == BufferAllocatorType::CACHELIB) &&
        (slice_length > kMaxSliceSize)) {
        LOG(ERROR) << "key=" << key << ", slice_length=" << slice_length
                   << ", max_size=" << kMaxSliceSize
                   << ", error=invalid_slice_size";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    MC_VLOG(1) << "key=" << key << ", value_length=" << slice_length
               << ", config=" << config << ", action=upsert_start_begin";

    auto group_id_result = GetGroupIdForKey(config, 1, 0);
    if (!group_id_result) {
        return tl::make_unexpected(group_id_result.error());
    }
    const std::string group_id = group_id_result.value();

    [[maybe_unused]] auto object_operation_lock =
        AcquireObjectOperationLock(object_id.tenant_id, object_id.user_key);
    const uint64_t requested_quota_charge =
        RequestedMemoryQuotaCharge(slice_length, config);

    auto attempt_once =
        [&]() -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
        std::unique_lock<std::mutex> zero_charge_policy_lock(
            tenant_quota_policy_mutex_, std::defer_lock);
        if (ShouldProtectZeroChargeMetadataCreate(requested_quota_charge)) {
            zero_charge_policy_lock.lock();
            auto latest_tenant_result =
                ResolveTenantIdForWriteLocked(tenant_id);
            if (!latest_tenant_result) {
                return tl::make_unexpected(latest_tenant_result.error());
            }
        }

        auto now = std::chrono::system_clock::now();
        std::optional<size_t> case_a_retry_shard_idx;
        {
            // --- Lock acquisition ---
            auto alive_clients = getAliveClientsSnapshot();
            std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
            // Use getMetadataShardIndex to find the object at its current shard
            // (handles both grouped and ungrouped routing).
            const size_t lookup_shard_idx =
                getMetadataShardIndex(object_id.tenant_id, object_id.user_key);
            MetadataShardAccessorRW shard(this, lookup_shard_idx);
            auto& tenant_state = shard->tenants[object_id.tenant_id];

            auto it = tenant_state.metadata.find(key);

            // --- Step 0: stale handle cleanup ---
            if (it != tenant_state.metadata.end()) {
                auto cleanup_plan =
                    BuildStaleHandleCleanupPlan(it->second, alive_clients);
                if (!cleanup_plan.removed_ids.empty()) {
                    auto persist_result = PersistStaleHandleCleanupForHA(
                        "UpsertStart(stale cleanup)", object_id.tenant_id, key,
                        it->second, cleanup_plan);
                    if (!persist_result) {
                        return tl::make_unexpected(persist_result.error());
                    }
                    if (enable_oplog_) {
                        return tl::make_unexpected(
                            ErrorCode::OBJECT_ALREADY_EXISTS);
                    } else if (CleanupStaleHandles(it->second, alive_clients,
                                                   &shard)) {
                        // EraseMetadata handles processing_keys,
                        // replication_tasks, offloading_tasks (with
                        // dec_refcnt), and promotion task cleanup.
                        EraseMetadata(tenant_state, it, object_id.tenant_id,
                                      QuotaEraseMode::kFull, &shard);
                        it = tenant_state.metadata.end();
                    }
                }
            }

            // --- Step 1: safety checks and preemption (only if key exists) ---
            if (it != tenant_state.metadata.end()) {
                auto& metadata = it->second;

                // Reject if the caller tries to change group membership.
                // Group membership is immutable while an object exists.
                if (config.group_ids.has_value() &&
                    metadata.group_id != group_id) {
                    LOG(ERROR) << "key=" << key
                               << ", error=group_membership_is_immutable";
                    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                }

                // Reject if a Copy/Move task is actively reading this key's
                // replicas.
                if (tenant_state.replication_tasks.count(key) > 0) {
                    LOG(INFO) << "key=" << key
                              << ", error=object_has_replication_task";
                    return tl::make_unexpected(
                        ErrorCode::OBJECT_HAS_REPLICATION_TASK);
                }

                // Reject if an offload-to-disk task is in progress (same
                // reason).
                if (tenant_state.offloading_tasks.count(key) > 0) {
                    LOG(INFO) << "key=" << key
                              << ", error=object_has_offloading_task";
                    return tl::make_unexpected(
                        ErrorCode::OBJECT_HAS_REPLICATION_TASK);
                }

                // Preempt an in-progress Put/Upsert on the same key.  The
                // previous writer's PROCESSING replicas are moved to
                // discarded_replicas_ with a TTL so they are not freed while
                // the old writer may still be doing RDMA writes.  Unlike
                // PutStart (which only preempts after a timeout), UpsertStart
                // preempts immediately.
                if (tenant_state.processing_keys.count(key) > 0) {
                    auto processing_replicas =
                        metadata.PopReplicas(&Replica::fn_is_processing);
                    if (!processing_replicas.empty()) {
                        std::lock_guard lock(discarded_replicas_mutex_);
                        discarded_replicas_.emplace_back(
                            std::move(processing_replicas),
                            now + put_start_release_timeout_sec_);
                    }
                    tenant_state.processing_keys.erase(key);

                    // If no COMPLETE replicas survive the preemption, this key
                    // effectively does not exist — fall through to Case A.
                    if (!metadata.HasReplica(&Replica::fn_is_completed)) {
                        EraseMetadata(tenant_state, it, object_id.tenant_id,
                                      QuotaEraseMode::kFull, &shard);
                        it = tenant_state.metadata.end();
                    }
                }
            }

            // --- Case A: key does not exist (or was erased above) ---
            // Allocate fresh buffers, identical to PutStart.
            if (it == tenant_state.metadata.end()) {
                VLOG(1) << "key=" << key << ", action=upsert_start_case_a";
                const size_t case_a_shard_idx =
                    group_id.empty()
                        ? getShardIndex(object_id.tenant_id, object_id.user_key)
                        : getShardIndex(group_id);
                if (case_a_shard_idx != lookup_shard_idx) {
                    case_a_retry_shard_idx = case_a_shard_idx;
                    if (tenant_state.Empty()) {
                        shard->tenants.erase(object_id.tenant_id);
                    }
                } else {
                    return AllocateAndInsertMetadata(
                        shard, client_id, key, slice_length, config, group_id,
                        object_id.tenant_id, now);
                }
            } else {
                // --- Step 2: key exists with COMPLETE replicas → Case B or C
                // ---
                auto& metadata = it->second;

                // Reject if any reader holds a reference (refcnt > 0).
                // Overwriting a buffer that an RDMA read is streaming from
                // would cause data corruption. The client should retry after
                // readers finish.
                if (metadata.HasReplica(&Replica::fn_is_busy)) {
                    LOG(INFO) << "key=" << key << ", error=object_replica_busy";
                    return tl::make_unexpected(ErrorCode::OBJECT_REPLICA_BUSY);
                }

                if (metadata.size == slice_length) {
                    // --- Case B: same size — in-place update ---
                    // Reuse existing buffer addresses.  No allocation or
                    // deallocation. The client will RDMA-write new data to the
                    // same addresses.
                    //
                    // hard_pinned is const and preserved automatically — upsert
                    // does not change the eviction protection level of an
                    // existing object.
                    metadata.client_id = client_id;
                    metadata.put_start_time = now;

                    // Reconcile soft_pin state with the incoming config.
                    {
                        SpinLocker locker(&metadata.lock);
                        if (config.with_soft_pin &&
                            !metadata.soft_pin_timeout) {
                            metadata.soft_pin_timeout.emplace();
                            MasterMetricManager::instance()
                                .inc_soft_pin_key_count(1);
                        } else if (!config.with_soft_pin &&
                                   metadata.soft_pin_timeout) {
                            metadata.soft_pin_timeout.reset();
                            MasterMetricManager::instance()
                                .dec_soft_pin_key_count(1);
                        }
                    }

                    // Mark COMPLETE → PROCESSING so readers won't see stale
                    // data mid-transfer.  The key becomes unreadable until
                    // UpsertEnd.
                    metadata.VisitReplicas(
                        &Replica::fn_is_completed,
                        [](Replica& replica) { replica.mark_processing(); });
                    SyncCacheTotalAccounting(metadata);

                    tenant_state.processing_keys.insert(key);

                    // Return the existing descriptors — same buffer addresses
                    // as before.
                    std::vector<Replica::Descriptor> replica_list;
                    const auto& all_replicas = metadata.GetAllReplicas();
                    replica_list.reserve(all_replicas.size());
                    for (const auto& replica : all_replicas) {
                        replica_list.emplace_back(replica.get_descriptor());
                    }

                    VLOG(1) << "key=" << key
                            << ", action=upsert_start_case_b_inplace";
                    return replica_list;
                }

                // --- Case C: different size — discard old replicas and
                // reallocate
                // --- Old buffers cannot be reused.  Move them to
                // discarded_replicas_ for delayed release (readers may still
                // hold descriptors without refcnt), then allocate fresh buffers
                // at the new size.
                //
                // Preserve hard_pin and soft_pin from the old metadata so that
                // eviction protection survives a size-changing upsert (RFC
                // §2.2.2).
                ReplicateConfig merged_config = config;
                merged_config.with_hard_pin =
                    merged_config.with_hard_pin || metadata.IsHardPinned();
                merged_config.with_soft_pin =
                    merged_config.with_soft_pin || metadata.IsSoftPinned();

                const std::string existing_group_id = metadata.group_id;
                const uint64_t old_quota_charge =
                    metadata.committed_quota_charge_bytes != 0
                        ? metadata.committed_quota_charge_bytes
                        : CompletedMemoryQuotaCharge(metadata);
                auto old_replicas =
                    PopReplicasWithCacheTotalAccounting(metadata);
                if (!old_replicas.empty()) {
                    std::lock_guard lock(discarded_replicas_mutex_);
                    discarded_replicas_.emplace_back(
                        std::move(old_replicas),
                        now + put_start_release_timeout_sec_);
                }
                EraseMetadata(tenant_state, it, object_id.tenant_id,
                              QuotaEraseMode::kPreserveOld, &shard);

                VLOG(1) << "key=" << key
                        << ", action=upsert_start_case_c_reallocate";
                auto allocate_result = AllocateAndInsertMetadata(
                    shard, client_id, key, slice_length, merged_config,
                    existing_group_id, object_id.tenant_id, now);
                if (!allocate_result) {
                    ReleaseTenantQuota(object_id.tenant_id, old_quota_charge);
                    return allocate_result;
                }
                auto new_it = tenant_state.metadata.find(key);
                if (new_it != tenant_state.metadata.end()) {
                    new_it->second.pending_replaced_quota_charge_bytes =
                        old_quota_charge;
                }
                return allocate_result;
            }
        }
        std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
        MetadataShardAccessorRW shard(this, case_a_retry_shard_idx.value());
        auto& retry_tenant_state = shard->tenants[object_id.tenant_id];
        const auto current_route =
            GetGroupRoute(object_id.tenant_id, object_id.user_key);
        if (current_route.has_value() ||
            retry_tenant_state.metadata.contains(key)) {
            LOG(INFO) << "key=" << key << ", info=object_already_exists";
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }
        return AllocateAndInsertMetadata(shard, client_id, key, slice_length,
                                         config, group_id, object_id.tenant_id,
                                         now);
    };

    for (int attempt = 0; attempt <= kMaxTenantQuotaEvictionRetries;
         ++attempt) {
        auto result = attempt_once();
        if (result.has_value() ||
            result.error() != ErrorCode::TENANT_QUOTA_EXCEEDED) {
            return result;
        }
        if (attempt == kMaxTenantQuotaEvictionRetries) {
            MasterMetricManager::instance().inc_tenant_quota_reject(
                object_id.tenant_id.value(), "quota_exceeded");
            return result;
        }
        EvictTenantMemoryForQuota(
            object_id.tenant_id,
            tenant_quota_table_.ComputeDeficit(object_id.tenant_id,
                                               requested_quota_charge));
    }
    return tl::make_unexpected(ErrorCode::TENANT_QUOTA_EXCEEDED);
}

auto MasterService::UpsertEnd(const UUID& client_id,
                              const ObjectMeta& object_meta,
                              const TenantId& tenant_id,
                              ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    return PutEnd(client_id, object_meta, tenant_id, replica_type);
}

auto MasterService::UpsertEnd(const UUID& client_id, const std::string& key,
                              const TenantId& tenant_id,
                              ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    return UpsertEnd(client_id, ObjectMeta{key, std::nullopt}, tenant_id,
                     replica_type);
}

auto MasterService::UpsertRevoke(const UUID& client_id, const std::string& key,
                                 const TenantId& tenant_id,
                                 ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    return PutRevoke(client_id, key, tenant_id, replica_type);
}

std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
MasterService::BatchUpsertStart(const UUID& client_id,
                                const std::vector<std::string>& keys,
                                const TenantId& tenant_id,
                                const std::vector<uint64_t>& slice_lengths,
                                const ReplicateConfig& config) {
    assert(tenant_id.IsValid());
    if (keys.size() != slice_lengths.size()) {
        LOG(ERROR) << "BatchUpsertStart: keys.size()=" << keys.size()
                   << " != slice_lengths.size()=" << slice_lengths.size();
        return std::vector<
            tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>(
            keys.size(), tl::make_unexpected(ErrorCode::INVALID_PARAMS));
    }
    if (config.group_ids.has_value() &&
        config.group_ids->size() != keys.size()) {
        LOG(ERROR) << "BatchUpsertStart: group_ids.size()="
                   << config.group_ids->size()
                   << " != keys.size()=" << keys.size();
        return std::vector<
            tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>(
            keys.size(), tl::make_unexpected(ErrorCode::INVALID_PARAMS));
    }
    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
        results;
    results.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        auto key_config = config.ForSingleKey(i);
        results.emplace_back(UpsertStart(client_id, keys[i], tenant_id,
                                         slice_lengths[i], key_config));
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchUpsertEnd(
    const UUID& client_id, const std::vector<ObjectMeta>& object_metas,
    const TenantId& tenant_id) {
    return BatchPutEnd(client_id, object_metas, tenant_id, ReplicaType::ALL);
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchUpsertRevoke(
    const UUID& client_id, const std::vector<std::string>& keys,
    const TenantId& tenant_id) {
    return BatchPutRevoke(client_id, keys, tenant_id);
}

auto MasterService::EvictDiskReplica(const UUID& client_id,
                                     const std::string& key,
                                     const TenantId& tenant_id,
                                     ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        LOG(INFO) << "key=" << key
                  << ", tenant_id=" << object_id.tenant_id.value()
                  << ", info=object_not_found_for_eviction";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();

    if (replica_type != ReplicaType::DISK &&
        replica_type != ReplicaType::LOCAL_DISK) {
        LOG(ERROR) << "key=" << key
                   << ", error=invalid_replica_type_for_eviction";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto target_pred = [replica_type, &client_id](const Replica& r) {
        if (replica_type == ReplicaType::DISK) {
            return r.is_disk_replica();
        } else if (replica_type == ReplicaType::LOCAL_DISK) {
            return r.is_local_disk_replica() &&
                   r.get_descriptor().get_local_disk_descriptor().client_id ==
                       client_id;
        }
        return false;
    };

    if (enable_oplog_ && ordered_oplog_writer_) {
        auto remaining =
            BuildRemainingReplicaDescriptors(metadata, target_pred);
        if (enable_oplog_) {
            auto reservation = ReserveBatchOpLogSlot();
            if (!reservation) {
                return tl::make_unexpected(reservation.error());
            }
            std::vector<ReplicaID> removed_ids;
            metadata.VisitReplicas(target_pred,
                                   [&removed_ids](Replica& replica) {
                                       removed_ids.push_back(replica.id());
                                       replica.mark_removed();
                                   });

            tl::expected<OpLogEntry, ErrorCode> persist_result;
            if (remaining.empty()) {
                persist_result = AppendReservedOpLogWithDurableFinalize(
                    std::move(reservation.value()), OpType::REMOVE,
                    metadata.tenant_id.value(), key, {},
                    [this, removed_ids = std::move(removed_ids)](
                        const OpLogEntry& durable_entry) {
                        FinalizeRemovedReplicasAfterDurable(
                            durable_entry, removed_ids, QuotaEraseMode::kFull);
                    });
            } else {
                persist_result = AppendReservedOpLogWithDurableFinalize(
                    std::move(reservation.value()), OpType::PUT_END,
                    metadata.tenant_id.value(), key,
                    SerializeMetadataForOpLogFromReplicaDescriptors(
                        metadata.client_id, metadata.size, remaining,
                        metadata.group_id, metadata.data_type),
                    [this, removed_ids = std::move(removed_ids)](
                        const OpLogEntry& durable_entry) {
                        FinalizeRemovedReplicasAfterDurable(
                            durable_entry, removed_ids, QuotaEraseMode::kFull);
                    });
            }
            if (!persist_result) {
                return tl::make_unexpected(persist_result.error());
            }
            return {};
        }

        tl::expected<OpLogEntry, ErrorCode> persist_result;
        if (remaining.empty()) {
            persist_result = AppendOpLogWithDurableFinalize(
                OpType::REMOVE, metadata.tenant_id.value(), key, {}, nullptr);
        } else {
            persist_result = AppendOpLogWithDurableFinalize(
                OpType::PUT_END, metadata.tenant_id.value(), key,
                SerializeMetadataForOpLogFromReplicaDescriptors(
                    metadata.client_id, metadata.size, remaining,
                    metadata.group_id, metadata.data_type),
                nullptr);
        }
        if (!persist_result) {
            return tl::make_unexpected(persist_result.error());
        }
    }

    if (replica_type == ReplicaType::DISK) {
        EraseReplicasWithCacheTotalAccounting(metadata, target_pred);
    } else if (replica_type == ReplicaType::LOCAL_DISK) {
        bool had_completed_disk = metadata.HasReplica([](const Replica& r) {
            return r.is_local_disk_replica() && r.is_completed();
        });
        EraseReplicasWithCacheTotalAccounting(metadata, target_pred);
        if (had_completed_disk) {
            auto& shard = accessor.GetShard();
            shard.OnDiskReplicaRemoved(had_completed_disk, metadata);
        }
    }

    if (!metadata.IsValid()) {
        PublishKvRemoved(key, metadata, object_id.tenant_id);
        accessor.Erase();
    }
    return {};
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchEvictDiskReplica(
    const UUID& client_id, const std::vector<std::string>& keys,
    const TenantId& tenant_id, ReplicaType replica_type) {
    assert(tenant_id.IsValid());
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.push_back(
            EvictDiskReplica(client_id, key, tenant_id, replica_type));
    }
    return results;
}

tl::expected<CopyStartResponse, ErrorCode> MasterService::CopyStart(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id,
    const std::string& src_segment,
    const std::vector<std::string>& tgt_segments) {
    auto normalized_tenant_result = ResolveTenantIdForWrite(tenant_id);
    if (!normalized_tenant_result) {
        return tl::make_unexpected(normalized_tenant_result.error());
    }
    const ObjectIdentity object_id{std::move(normalized_tenant_result.value()),
                                   key};
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        for (const auto& tgt_segment : tgt_segments) {
            if (!segment_access.ExistsSegmentName(tgt_segment)) {
                LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                           << ", error=target_segment_not_found";
                return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
            }
            if (!segment_access.IsSegmentAllocatable(tgt_segment)) {
                LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                           << ", error=target_segment_not_allocatable";
                return tl::make_unexpected(
                    ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
            }
        }
    }
    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", object not found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << " already has an ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    }

    auto& metadata = accessor.Get();
    auto source = metadata.GetReplicaBySegmentName(src_segment);
    if (source == nullptr || !source->is_completed() ||
        source->has_invalid_mem_handle()) {
        LOG(ERROR) << "key=" << key << ", src_segment=" << src_segment
                   << ", replica not found or not valid";
        return tl::make_unexpected(ErrorCode::REPLICA_NOT_FOUND);
    }

    size_t new_replica_count = 0;
    for (const auto& tgt_segment : tgt_segments) {
        if (metadata.GetReplicaBySegmentName(tgt_segment) == nullptr) {
            ++new_replica_count;
        }
    }

    const uint64_t reserved_quota_charge =
        SaturatingMultiply(static_cast<uint64_t>(metadata.size),
                           static_cast<uint64_t>(new_replica_count));
    auto quota_result =
        ReserveTenantQuota(object_id.tenant_id, reserved_quota_charge);
    if (!quota_result) {
        if (quota_result.error() == ErrorCode::TENANT_QUOTA_EXCEEDED) {
            MasterMetricManager::instance().inc_tenant_quota_reject(
                object_id.tenant_id.value(), "quota_exceeded");
        }
        return tl::make_unexpected(quota_result.error());
    }
    auto abort_reserved_quota = [&] {
        AbortTenantQuota(object_id.tenant_id, reserved_quota_charge);
    };

    std::vector<Replica> replicas;
    replicas.reserve(new_replica_count);
    {
        ScopedAllocatorAccess allocator_access =
            segment_manager_.getAllocatorAccess();
        const auto& allocator_manager = allocator_access.getAllocatorManager();

        for (auto& tgt_segment : tgt_segments) {
            if (metadata.GetReplicaBySegmentName(tgt_segment) != nullptr) {
                // Skip used segments.
                continue;
            }

            auto replica = allocation_strategy_->AllocateFrom(
                allocator_manager, metadata.size, tgt_segment);
            if (!replica.has_value()) {
                LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                           << ", failed to allocate replica";
                abort_reserved_quota();
                return tl::make_unexpected(replica.error());
            }
            replicas.push_back(std::move(*replica));
        }
    }

    CopyStartResponse response;
    response.targets.reserve(replicas.size());
    std::vector<ReplicaID> replica_ids;
    replica_ids.reserve(replicas.size());

    response.source = source->get_descriptor();
    for (const auto& replica : replicas) {
        replica_ids.push_back(replica.id());
        response.targets.emplace_back(replica.get_descriptor());
    }

    // Create replication task for tracking.
    auto& tenant_state = accessor.GetTenantState();
    auto task_insert = tenant_state.replication_tasks.emplace(
        std::piecewise_construct, std::forward_as_tuple(key),
        std::forward_as_tuple(client_id, std::chrono::system_clock::now(),
                              ReplicationTask::Type::COPY, source->id(),
                              std::move(replica_ids), reserved_quota_charge));
    if (!task_insert.second) {
        abort_reserved_quota();
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    }

    // Increase source refcnt to protect it from eviction.
    source->inc_refcnt();

    // Add replicas to the object.
    // DO NOT ACCESS source AFTER THIS !!!
    metadata.AddReplicas(std::move(replicas));

    return response;
}

tl::expected<void, ErrorCode> MasterService::CopyEnd(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRW accessor(this,
                                MakeObjectIdentityForRequest(key, tenant_id));
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (!accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << ", error=object has no ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_NO_REPLICATION_TASK);
    }

    auto& task = accessor.GetReplicationTask();
    if (task.client_id != client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to CopyEnd key "
                   << key << ", was CopyStart-ed by " << task.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    if (task.type != ReplicationTask::Type::COPY) {
        LOG(ERROR) << "Ongoing replication task type is MOVE instead of COPY";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto& metadata = accessor.Get();
    auto source_id = task.source_id;
    auto source = metadata.GetReplicaByID(source_id);
    if (source == nullptr || !source->is_completed() ||
        source->has_invalid_mem_handle()) {
        LOG(ERROR) << "key=" << key << ", source_id=" << source_id
                   << ", status=" << (source == nullptr ? "nullptr" : "invalid")
                   << ", copy source becomes invalid during data transfer";
        // Release the refcnt taken in CopyStart. The success path below does
        // this once the copy completes; this error path must do it too, or the
        // source replica stays pinned and can never be evicted.
        if (source != nullptr) {
            source->dec_refcnt();
        }
        // Discard target replicas and clear the replication task.
        EraseReplicasWithCacheTotalAccounting(
            metadata, [&task](const Replica& replica) {
                return std::find(task.replica_ids.begin(),
                                 task.replica_ids.end(),
                                 replica.id()) != task.replica_ids.end();
            });
        AbortTenantQuota(metadata.tenant_id, task.reserved_quota_charge_bytes);
        accessor.EraseReplicationTask();
        if (!metadata.IsValid()) {
            // Remove the object if it does not have any replicas.
            accessor.Erase();
        }
        return tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
    }

    // First validate that all target replicas are still healthy. If any
    // replica is invalid we won't be able to mark it complete; this affects
    // the post-mutation descriptor list.
    bool all_complete = true;
    uint64_t completed_quota_charge = 0;
    std::vector<ReplicaID> commit_target_ids;
    commit_target_ids.reserve(task.replica_ids.size());
    for (const auto& replica_id : task.replica_ids) {
        auto replica = metadata.GetReplicaByID(replica_id);
        if (replica == nullptr || replica->has_invalid_mem_handle()) {
            LOG(WARNING)
                << "key=" << key << ", replica_id=" << replica_id
                << ", copy target becomes invalid during data transfer";
            all_complete = false;
        } else {
            commit_target_ids.push_back(replica_id);
        }
    }

    std::optional<OrderedOpLogWriter::Reservation> batch_reservation;
    if (enable_ha_ && enable_oplog_) {
        auto reservation = ReserveBatchOpLogSlot();
        if (!reservation) {
            return tl::make_unexpected(reservation.error());
        }
        batch_reservation = std::move(reservation.value());
    }

    source->dec_refcnt();
    for (const auto& replica_id : commit_target_ids) {
        auto replica = metadata.GetReplicaByID(replica_id);
        if (replica != nullptr) {
            replica->mark_complete();
            completed_quota_charge = SaturatingAdd(
                completed_quota_charge, static_cast<uint64_t>(metadata.size));
        }
    }

    if (enable_oplog_ && ordered_oplog_writer_) {
        std::vector<Replica::Descriptor> post;
        metadata.VisitReplicas(&Replica::fn_is_completed,
                               [&post](const Replica& replica) {
                                   post.push_back(replica.get_descriptor());
                               });
        auto payload = SerializeMetadataForOpLogFromReplicaDescriptors(
            metadata.client_id, metadata.size, post, metadata.group_id,
            metadata.data_type);
        if (batch_reservation) {
            auto persist_result = AppendReservedOpLogWithDurableFinalize(
                std::move(*batch_reservation), OpType::PUT_END,
                tenant_id.value(), key, payload, nullptr);
            if (!persist_result) {
                LOG(WARNING)
                    << "CopyEnd: PUT_END persist failed for key=" << key
                    << ", err=" << static_cast<int>(persist_result.error());
            }
        } else {
            auto persist_result = AppendOpLogVisibleBeforeDurable(
                OpType::PUT_END, tenant_id.value(), key, payload);
            if (!persist_result) {
                LOG(WARNING)
                    << "CopyEnd: PUT_END persist failed for key=" << key
                    << ", err=" << static_cast<int>(persist_result.error());
            }
        }
    }

    SyncCacheTotalAccounting(metadata);

    const uint64_t commit_charge =
        std::min(completed_quota_charge, task.reserved_quota_charge_bytes);
    const uint64_t abort_charge =
        task.reserved_quota_charge_bytes - commit_charge;
    CommitAdditionalTenantQuota(metadata.tenant_id, commit_charge);
    AbortTenantQuota(metadata.tenant_id, abort_charge);
    metadata.committed_quota_charge_bytes =
        SaturatingAdd(metadata.committed_quota_charge_bytes, commit_charge);

    accessor.EraseReplicationTask();

    return all_complete ? tl::expected<void, ErrorCode>()
                        : tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
}

tl::expected<void, ErrorCode> MasterService::CopyRevoke(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRW accessor(this,
                                MakeObjectIdentityForRequest(key, tenant_id));
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (!accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << ", error=object has no ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_NO_REPLICATION_TASK);
    }

    auto& task = accessor.GetReplicationTask();
    if (task.client_id != client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to CopyRevoke key "
                   << key << ", was CopyStart-ed by " << task.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    if (task.type != ReplicationTask::Type::COPY) {
        LOG(ERROR) << "Ongoing replication task type is MOVE instead of COPY";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto& metadata = accessor.Get();
    auto source_id = task.source_id;
    auto source = metadata.GetReplicaByID(source_id);
    if (source == nullptr) {
        LOG(WARNING) << "key=" << key << ", source_id=" << source_id
                     << ", copy source not found during revoke";
    } else {
        // Decrement source reference count
        source->dec_refcnt();
    }

    // Erase all replica_ids
    for (const auto& replica_id : task.replica_ids) {
        EraseReplicasWithCacheTotalAccounting(
            metadata, [&replica_id](const Replica& replica) {
                return replica.id() == replica_id;
            });
    }

    AbortTenantQuota(metadata.tenant_id, task.reserved_quota_charge_bytes);
    accessor.EraseReplicationTask();

    if (!metadata.IsValid()) {
        // Remove the object if it does not have any replicas.
        accessor.Erase();
    }

    return {};
}

tl::expected<MoveStartResponse, ErrorCode> MasterService::MoveStart(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id,
    const std::string& src_segment, const std::string& tgt_segment) {
    auto normalized_tenant_result = ResolveTenantIdForWrite(tenant_id);
    if (!normalized_tenant_result) {
        return tl::make_unexpected(normalized_tenant_result.error());
    }
    const ObjectIdentity object_id{std::move(normalized_tenant_result.value()),
                                   key};
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    if (src_segment == tgt_segment) {
        LOG(ERROR) << "key=" << key << ", move_tgt=" << tgt_segment
                   << " cannot be the same as move_src=" << src_segment;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        if (!segment_access.ExistsSegmentName(tgt_segment)) {
            LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                       << ", error=target_segment_not_found";
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        if (!segment_access.IsSegmentAllocatable(tgt_segment)) {
            LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                       << ", error=target_segment_not_allocatable";
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
    }

    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", object not found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << " already has an ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    }

    auto& metadata = accessor.Get();
    auto source = metadata.GetReplicaBySegmentName(src_segment);
    if (source == nullptr || !source->is_completed() ||
        source->has_invalid_mem_handle()) {
        LOG(ERROR) << "key=" << key << ", src_segment=" << src_segment
                   << ", replica not found or not completed";
        return tl::make_unexpected(ErrorCode::REPLICA_NOT_FOUND);
    }

    std::vector<Replica> replicas;
    if (metadata.GetReplicaBySegmentName(tgt_segment) == nullptr) {
        const uint64_t reserved_quota_charge =
            SaturatingMultiply(static_cast<uint64_t>(metadata.size), 1);
        auto quota_result =
            ReserveTenantQuota(object_id.tenant_id, reserved_quota_charge);
        if (!quota_result) {
            if (quota_result.error() == ErrorCode::TENANT_QUOTA_EXCEEDED) {
                MasterMetricManager::instance().inc_tenant_quota_reject(
                    object_id.tenant_id.value(), "quota_exceeded");
            }
            return tl::make_unexpected(quota_result.error());
        }
        auto abort_reserved_quota = [&] {
            AbortTenantQuota(object_id.tenant_id, reserved_quota_charge);
        };

        ScopedAllocatorAccess allocator_access =
            segment_manager_.getAllocatorAccess();
        const auto& allocator_manager = allocator_access.getAllocatorManager();

        auto replica = allocation_strategy_->AllocateFrom(
            allocator_manager, metadata.size, tgt_segment);
        if (!replica.has_value()) {
            LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                       << ", failed to allocate replica";
            abort_reserved_quota();
            return tl::make_unexpected(replica.error());
        }
        replicas.push_back(std::move(*replica));
    } else {
        auto quota_result = ReserveTenantQuota(object_id.tenant_id, 0);
        if (!quota_result) {
            return tl::make_unexpected(quota_result.error());
        }
    }

    const uint64_t reserved_quota_charge =
        replicas.empty()
            ? 0
            : SaturatingMultiply(static_cast<uint64_t>(metadata.size), 1);

    MoveStartResponse response;
    std::vector<ReplicaID> replica_ids;

    response.source = source->get_descriptor();
    if (!replicas.empty()) {
        replica_ids.push_back(replicas[0].id());
        response.target = replicas[0].get_descriptor();
    } else {
        response.target = std::nullopt;
    }

    // Create replication task for tracking.
    auto& tenant_state = accessor.GetTenantState();
    auto task_insert = tenant_state.replication_tasks.emplace(
        std::piecewise_construct, std::forward_as_tuple(key),
        std::forward_as_tuple(client_id, std::chrono::system_clock::now(),
                              ReplicationTask::Type::MOVE, source->id(),
                              std::move(replica_ids), reserved_quota_charge));
    if (!task_insert.second) {
        AbortTenantQuota(object_id.tenant_id, reserved_quota_charge);
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    }

    // Increase source refcnt to protect it from eviction.
    source->inc_refcnt();

    // Add replicas to the object.
    // DO NOT ACCESS source AFTER THIS !!!
    metadata.AddReplicas(std::move(replicas));

    return response;
}

tl::expected<void, ErrorCode> MasterService::MoveEnd(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRW accessor(this,
                                MakeObjectIdentityForRequest(key, tenant_id));
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (!accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << ", error=object has no ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_NO_REPLICATION_TASK);
    }

    auto& task = accessor.GetReplicationTask();
    if (task.client_id != client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to MoveEnd key "
                   << key << ", was MoveStart-ed by " << task.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    if (task.type != ReplicationTask::Type::MOVE) {
        LOG(ERROR) << "Ongoing replication task type is COPY instead of MOVE";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto& metadata = accessor.Get();
    auto source_id = task.source_id;
    auto source = metadata.GetReplicaByID(source_id);
    if (source == nullptr || !source->is_completed() ||
        source->has_invalid_mem_handle()) {
        LOG(ERROR) << "key=" << key << ", source_id=" << source_id
                   << ", status=" << (source == nullptr ? "nullptr" : "invalid")
                   << ", move source becomes invalid during data transfer";
        // Release the refcnt taken in MoveStart. The success path below does
        // this once the move completes; this error path must do it too, or the
        // source replica stays pinned and can never be evicted.
        if (source != nullptr) {
            source->dec_refcnt();
        }
        // Discard target replica and clear the replication task.
        EraseReplicasWithCacheTotalAccounting(
            metadata, [&task](const Replica& replica) {
                return std::find(task.replica_ids.begin(),
                                 task.replica_ids.end(),
                                 replica.id()) != task.replica_ids.end();
            });
        AbortTenantQuota(metadata.tenant_id, task.reserved_quota_charge_bytes);
        accessor.EraseReplicationTask();
        if (!metadata.IsValid()) {
            // Remove the object if it does not have any replicas.
            accessor.Erase();
        }
        return tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
    }

    // Validate the target replica before any mutation. Source dec_refcnt
    // and target mark_complete are deferred until after persist.
    bool has_target = !task.replica_ids.empty();
    ReplicaID target_id = has_target ? task.replica_ids[0] : ReplicaID{};
    if (has_target) {
        auto replica = metadata.GetReplicaByID(target_id);
        if (replica == nullptr || replica->has_invalid_mem_handle()) {
            LOG(WARNING)
                << "key=" << key << ", replica_id=" << target_id
                << ", move target becomes invalid during data transfer";
            AbortTenantQuota(metadata.tenant_id,
                             task.reserved_quota_charge_bytes);
            // Source untouched; safe to drop the broken task.
            accessor.EraseReplicationTask();
            return tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
        }
    }

    if (enable_oplog_ && ordered_oplog_writer_) {
        // Build post-mutation descriptors:
        //   - existing COMPLETE replicas, except the source (about to be
        //   popped)
        //   - target (if any) flipped to COMPLETE
        std::vector<Replica::Descriptor> post;
        for (const auto& rep : metadata.GetAllReplicas()) {
            if (rep.id() == source_id) continue;
            if (rep.status() == ReplicaStatus::COMPLETE) {
                post.push_back(rep.get_descriptor());
                continue;
            }
            if (has_target && rep.id() == target_id) {
                Replica::Descriptor desc = rep.get_descriptor();
                desc.status = ReplicaStatus::COMPLETE;
                post.push_back(std::move(desc));
            }
        }

        tl::expected<OpLogEntry, ErrorCode> persist_result;
        if (enable_oplog_) {
            auto reservation = ReserveBatchOpLogSlot();
            if (!reservation) {
                return tl::make_unexpected(reservation.error());
            }
            source->mark_removed();
            persist_result = AppendReservedOpLogWithDurableFinalize(
                std::move(reservation.value()), OpType::PUT_END,
                metadata.tenant_id.value(), key,
                SerializeMetadataForOpLogFromReplicaDescriptors(
                    metadata.client_id, metadata.size, post, metadata.group_id,
                    metadata.data_type),
                [this, removed_ids = std::vector<ReplicaID>{source_id}](
                    const OpLogEntry& durable_entry) {
                    FinalizeRemovedReplicasAfterDurable(
                        durable_entry, removed_ids, QuotaEraseMode::kFull);
                });
        } else {
            persist_result = AppendOpLogWithDurableFinalize(
                OpType::PUT_END, metadata.tenant_id.value(), key,
                SerializeMetadataForOpLogFromReplicaDescriptors(
                    metadata.client_id, metadata.size, post, metadata.group_id,
                    metadata.data_type),
                nullptr);
        }
        if (!persist_result) {
            return tl::make_unexpected(persist_result.error());
        }
    }

    // Persist OK — apply local commit.
    source->dec_refcnt();
    if (has_target) {
        auto replica = metadata.GetReplicaByID(target_id);
        if (replica != nullptr) {
            replica->mark_complete();
        }
    }

    if (!(enable_ha_ && enable_oplog_)) {
        // Remove the source replica and release its space later.
        auto source_replica = PopReplicasWithCacheTotalAccounting(
            metadata, [&source_id](const Replica& replica) {
                return replica.id() == source_id;
            });
        if (!source_replica.empty()) {
            std::lock_guard lock(discarded_replicas_mutex_);
            discarded_replicas_.emplace_back(
                std::move(source_replica), std::chrono::system_clock::now() +
                                               put_start_release_timeout_sec_);
        }
    }

    AbortTenantQuota(metadata.tenant_id, task.reserved_quota_charge_bytes);
    accessor.EraseReplicationTask();

    return {};
}

tl::expected<void, ErrorCode> MasterService::MoveRevoke(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRW accessor(this,
                                MakeObjectIdentityForRequest(key, tenant_id));
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (!accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << ", error=object has no ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_NO_REPLICATION_TASK);
    }

    auto& task = accessor.GetReplicationTask();
    if (task.client_id != client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to MoveRevoke key "
                   << key << ", was MoveStart-ed by " << task.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    if (task.type != ReplicationTask::Type::MOVE) {
        LOG(ERROR) << "Ongoing replication task type is COPY instead of MOVE";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto& metadata = accessor.Get();
    auto source_id = task.source_id;
    auto source = metadata.GetReplicaByID(source_id);
    if (source == nullptr) {
        LOG(WARNING) << "key=" << key << ", source_id=" << source_id
                     << ", move source not found during revoke";
    } else {
        // Decrement source reference count
        source->dec_refcnt();
    }

    // Erase all replica_ids (in MOVE operation, there should be at most one)
    for (const auto& replica_id : task.replica_ids) {
        EraseReplicasWithCacheTotalAccounting(
            metadata, [&replica_id](const Replica& replica) {
                return replica.id() == replica_id;
            });
    }

    AbortTenantQuota(metadata.tenant_id, task.reserved_quota_charge_bytes);
    accessor.EraseReplicationTask();

    if (!metadata.IsValid()) {
        // Remove the object if it does not have any replicas.
        accessor.Erase();
    }

    return {};
}

auto MasterService::Remove(const std::string& key, const TenantId& tenant_id,
                           bool force) -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();
    std::vector<UUID> local_disk_holders;
    metadata.VisitReplicas(
        [](const Replica& replica) {
            return replica.is_local_disk_replica();
        },
        [&local_disk_holders](Replica& replica) {
            auto client_id = replica.get_local_disk_client_id();
            if (client_id.has_value()) {
                local_disk_holders.push_back(client_id.value());
            }
        });

    if (!force && !metadata.IsLeaseExpired()) {
        VLOG(1) << "key=" << key << ", error=object_has_lease";
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_LEASE);
    }

    /**
     * The reason the force operation here does not bypass the replica
     * check is that put operations (which could also be copy or move)
     * and remove operations might be happening concurrently, making it
     * extremely dangerous to perform a direct removal at this point.
     */
    if (!metadata.AllReplicas(&Replica::fn_is_completed)) {
        LOG(ERROR) << "key=" << key << ", error=replica_not_ready";
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    if (accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key << ", error=object_has_replication_task";
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    }

    if (enable_ha_) {
        if (enable_oplog_) {
            auto reservation = ReserveBatchOpLogSlot();
            if (!reservation) {
                return tl::make_unexpected(reservation.error());
            }
            std::vector<ReplicaID> removed_ids;
            metadata.VisitReplicas(&Replica::fn_is_completed,
                                   [&removed_ids](Replica& replica) {
                                       removed_ids.push_back(replica.id());
                                       replica.mark_removed();
                                   });
            auto persist_result = AppendReservedOpLogWithDurableFinalize(
                std::move(reservation.value()), OpType::REMOVE,
                object_id.tenant_id.value(), key, {},
                [this, removed_ids = std::move(removed_ids),
                 local_disk_holders,
                 tenant_id_for_task = object_id.tenant_id.value(), key](
                    const OpLogEntry& durable_entry) {
                    FinalizeRemovedReplicasAfterDurable(
                        durable_entry, removed_ids, QuotaEraseMode::kFull);
                    EnqueueRemoveTasks(
                        local_disk_holders,
                        RemoveTaskItem{tenant_id_for_task, key});
                });
            if (!persist_result) {
                return tl::make_unexpected(persist_result.error());
            }
            return {};
        }
    }
    PublishKvRemoved(key, metadata, object_id.tenant_id);

    // Before erasing metadata, collect LOCAL_DISK replica holders so we
    // can notify them to reclaim SSD space via RemoveObjectHeartbeat.
    accessor.Erase();

    // Push removed key to each LOCAL_DISK holder's removed_keys queue.
    EnqueueRemoveTasks(local_disk_holders, RemoveTaskItem{tenant_id.value(), key});

    return {};
}

auto MasterService::RemoveByRegex(const std::string& regex_pattern,
                                  const TenantId& tenant_id, bool force)
    -> tl::expected<long, ErrorCode> {
    assert(tenant_id.IsValid());
    long removed_count = 0;
    std::regex pattern;

    try {
        pattern = std::regex(regex_pattern, std::regex::ECMAScript);
    } catch (const std::regex_error& e) {
        LOG(ERROR) << "Invalid regex pattern: " << regex_pattern
                   << ", error: " << e.what();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const TenantId& normalized_tenant = ResolveRequestTenantId(tenant_id);
    for (size_t i = 0; i < kNumShards; ++i) {
        MetadataShardAccessorRW shard(this, i);
        auto tenant_it = shard->tenants.find(normalized_tenant);
        if (tenant_it == shard->tenants.end()) {
            continue;
        }
        auto& tenant_state = tenant_it->second;

        for (auto it = tenant_state.metadata.begin();
             it != tenant_state.metadata.end();) {
            if (std::regex_search(it->first, pattern)) {
                if (!force && !it->second.IsLeaseExpired()) {
                    VLOG(1) << "key=" << it->first
                            << " matched by regex, but has lease. Skipping "
                            << "removal.";
                    ++it;
                    continue;
                }
                /**
                 * The reason the force operation here does not bypass the
                 * replica check is that put operations (which could also be
                 * copy or move) and remove operations might be happening
                 * concurrently, making it extremely dangerous to perform a
                 * direct removal at this point.
                 */
                if (!it->second.AllReplicas(&Replica::fn_is_completed)) {
                    LOG(WARNING) << "key=" << it->first
                                 << " matched by regex, but not all replicas "
                                    "are complete. Skipping removal.";
                    ++it;
                    continue;
                }
                if (tenant_state.replication_tasks.contains(it->first)) {
                    LOG(WARNING) << "key=" << it->first
                                 << ", matched by regex, but has replication "
                                    "task. Skipping removal.";
                    ++it;
                    continue;
                }

                VLOG(1) << "key=" << it->first
                        << " matched by regex. Removing.";
                if (enable_ha_) {
                    if (enable_oplog_) {
                        auto reservation = ReserveBatchOpLogSlot();
                        if (!reservation) {
                            ++it;
                            continue;
                        }
                        std::vector<ReplicaID> removed_ids;
                        it->second.VisitReplicas(
                            &Replica::fn_is_completed,
                            [&removed_ids](Replica& replica) {
                                removed_ids.push_back(replica.id());
                                replica.mark_removed();
                            });
                        auto persist_result =
                            AppendReservedOpLogWithDurableFinalize(
                                std::move(reservation.value()), OpType::REMOVE,
                                normalized_tenant.value(), it->first, {},
                                [this, removed_ids = std::move(removed_ids)](
                                    const OpLogEntry& durable_entry) {
                                    FinalizeRemovedReplicasAfterDurable(
                                        durable_entry, removed_ids,
                                        QuotaEraseMode::kFull);
                                });
                        if (!persist_result) {
                            ++it;
                            continue;
                        }
                        ++it;
                        removed_count++;
                        continue;
                    }
                }
                it = EraseMetadata(tenant_state, it, normalized_tenant,
                                   QuotaEraseMode::kFull, &shard);
                removed_count++;
            } else {
                ++it;
            }
        }
        if (tenant_state.Empty()) {
            shard->tenants.erase(tenant_it);
        }
    }

    VLOG(1) << "action=remove_by_regex, pattern=" << regex_pattern
            << ", removed_count=" << removed_count;
    return removed_count;
}

long MasterService::RemoveAll(bool force) {
    long removed_count = 0;
    int64_t total_freed_size = 0;
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    auto now = std::chrono::system_clock::now();

    // Since RemoveAll clears everything, signal ALL clients with a
    // LocalDiskSegment to physically clear their SSD immediately.
    // This lets client cleanup overlap with master metadata deletion.
    {
        ScopedLocalDiskSegmentAccess local_disk_segment_access =
            segment_manager_.getLocalDiskSegmentAccess();
        auto& client_local_disk_segment =
            local_disk_segment_access.getClientLocalDiskSegment();
        for (auto& [client_id, segment] : client_local_disk_segment) {
            MutexLocker locker(&segment->offloading_mutex_);
            segment->pending_remove_all = true;
        }
    }

    // Delete metadata — runs concurrently with client SSD cleanup.
    for (size_t i = 0; i < kNumShards; i++) {
        MetadataShardAccessorRW shard(this, i);
        for (auto tenant_it = shard->tenants.begin();
             tenant_it != shard->tenants.end();) {
            auto& tenant_state = tenant_it->second;
            auto it = tenant_state.metadata.begin();
            while (it != tenant_state.metadata.end()) {
                if ((force || it->second.IsLeaseExpired(now)) &&
                    it->second.AllReplicas(&Replica::fn_is_completed) &&
                    !tenant_state.replication_tasks.contains(it->first)) {
                    auto mem_rep_count = it->second.CountReplicas(
                        &Replica::fn_is_memory_replica);

                    if (enable_ha_) {
                        if (enable_oplog_) {
                            auto reservation = ReserveBatchOpLogSlot();
                            if (!reservation) {
                                ++it;
                                continue;
                            }
                            std::vector<ReplicaID> removed_ids;
                            it->second.VisitReplicas(
                                &Replica::fn_is_completed,
                                [&removed_ids](Replica& replica) {
                                    removed_ids.push_back(replica.id());
                                    replica.mark_removed();
                                });
                            auto persist_result =
                                AppendReservedOpLogWithDurableFinalize(
                                    std::move(reservation.value()),
                                    OpType::REMOVE, tenant_it->first.value(),
                                    it->first, {},
                                    [this,
                                     removed_ids = std::move(removed_ids)](
                                        const OpLogEntry& durable_entry) {
                                        FinalizeRemovedReplicasAfterDurable(
                                            durable_entry, removed_ids,
                                            QuotaEraseMode::kFull);
                                    });
                            if (!persist_result) {
                                ++it;
                                continue;
                            }
                            total_freed_size += it->second.size * mem_rep_count;
                            ++it;
                            removed_count++;
                            continue;
                        }
                    }

                    total_freed_size += it->second.size * mem_rep_count;
                    ErasePromotionTaskIfPresent(tenant_state, it->first,
                                                tenant_it->first);
                    it = EraseMetadata(tenant_state, it, tenant_it->first,
                                       QuotaEraseMode::kFull, &shard);
                    removed_count++;
                } else {
                    ++it;
                }
            }
            if (tenant_state.Empty()) {
                tenant_it = shard->tenants.erase(tenant_it);
            } else {
                ++tenant_it;
            }
        }
    }

    VLOG(1) << "action=remove_all_objects"
            << ", removed_count=" << removed_count
            << ", total_freed_size=" << total_freed_size;
    return removed_count;
}

long MasterService::RemoveAll(const TenantId& tenant_id, bool force) {
    long removed_count = 0;
    int64_t total_freed_size = 0;
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    auto now = std::chrono::system_clock::now();
    const TenantId& normalized_tenant = ResolveRequestTenantId(tenant_id);

    // For the tenant-scoped overload, only signal clients that own LOCAL_DISK
    // replicas of THIS tenant — clearing all clients would cross-delete other
    // tenants' SSD data.
    std::unordered_set<UUID, boost::hash<UUID>> clients_with_disk_replicas;

    for (size_t i = 0; i < kNumShards; i++) {
        MetadataShardAccessorRW shard(this, i);
        auto tenant_it = shard->tenants.find(normalized_tenant);
        if (tenant_it == shard->tenants.end()) {
            continue;
        }
        auto& tenant_state = tenant_it->second;
        auto it = tenant_state.metadata.begin();
        while (it != tenant_state.metadata.end()) {
            if ((force || it->second.IsLeaseExpired(now)) &&
                it->second.AllReplicas(&Replica::fn_is_completed) &&
                !tenant_state.replication_tasks.contains(it->first)) {
                it->second.VisitReplicas(
                    &Replica::fn_is_local_disk_replica,
                    [&clients_with_disk_replicas](const Replica& replica) {
                        auto cid = replica.get_local_disk_client_id();
                        if (cid) {
                            clients_with_disk_replicas.insert(*cid);
                        }
                    });
                auto mem_rep_count =
                    it->second.CountReplicas(&Replica::fn_is_memory_replica);
                if (enable_ha_) {
                    if (enable_oplog_) {
                        auto reservation = ReserveBatchOpLogSlot();
                        if (!reservation) {
                            ++it;
                            continue;
                        }
                        std::vector<ReplicaID> removed_ids;
                        it->second.VisitReplicas(
                            &Replica::fn_is_completed,
                            [&removed_ids](Replica& replica) {
                                removed_ids.push_back(replica.id());
                                replica.mark_removed();
                            });
                        auto persist_result =
                            AppendReservedOpLogWithDurableFinalize(
                                std::move(reservation.value()), OpType::REMOVE,
                                normalized_tenant.value(), it->first, {},
                                [this, removed_ids = std::move(removed_ids)](
                                    const OpLogEntry& durable_entry) {
                                    FinalizeRemovedReplicasAfterDurable(
                                        durable_entry, removed_ids,
                                        QuotaEraseMode::kFull);
                                });
                        if (!persist_result) {
                            ++it;
                            continue;
                        }
                        total_freed_size += it->second.size * mem_rep_count;
                        ++it;
                        removed_count++;
                        continue;
                    }
                }
                total_freed_size += it->second.size * mem_rep_count;
                ErasePromotionTaskIfPresent(tenant_state, it->first,
                                            normalized_tenant);
                it = EraseMetadata(tenant_state, it, normalized_tenant,
                                   QuotaEraseMode::kFull, &shard);
                removed_count++;
            } else {
                ++it;
            }
        }
        if (tenant_state.Empty()) {
            shard->tenants.erase(tenant_it);
        }
    }

    if (!clients_with_disk_replicas.empty()) {
        ScopedLocalDiskSegmentAccess local_disk_segment_access =
            segment_manager_.getLocalDiskSegmentAccess();
        auto& client_local_disk_segment =
            local_disk_segment_access.getClientLocalDiskSegment();
        for (const auto& client_id : clients_with_disk_replicas) {
            auto seg_it = client_local_disk_segment.find(client_id);
            if (seg_it != client_local_disk_segment.end()) {
                MutexLocker locker(&seg_it->second->offloading_mutex_);
                seg_it->second->pending_remove_all = true;
            }
        }
    }

    VLOG(1) << "action=remove_all_objects"
            << ", tenant_id=" << normalized_tenant.value()
            << ", removed_count=" << removed_count
            << ", total_freed_size=" << total_freed_size
            << ", signaled_clients=" << clients_with_disk_replicas.size();
    return removed_count;
}

auto MasterService::BatchRemove(const std::vector<std::string>& keys,
                                const TenantId& tenant_id, bool force)
    -> std::vector<tl::expected<void, ErrorCode>> {
    std::vector<tl::expected<void, ErrorCode>> results(keys.size());
    const TenantId& normalized_tenant = ResolveRequestTenantId(tenant_id);

    // Group keys by shard to reduce lock contention
    std::unordered_map<size_t,
                       std::vector<std::pair<size_t, const std::string*>>>
        keys_by_shard;
    keys_by_shard.reserve(
        std::min(keys.size(), static_cast<size_t>(kNumShards)));

    for (size_t i = 0; i < keys.size(); ++i) {
        size_t shard_idx = getMetadataShardIndex(normalized_tenant, keys[i]);
        keys_by_shard[shard_idx].emplace_back(i, &keys[i]);
    }

    std::shared_lock<std::shared_mutex> snapshot_lock(snapshot_mutex_);

    auto alive_clients = getAliveClientsSnapshot();

    // Process each shard once, acquiring lock per shard
    for (auto& [shard_idx, key_group] : keys_by_shard) {
        MetadataShardAccessorRW shard(this, shard_idx);
        auto now = std::chrono::system_clock::now();

        for (const auto& [original_idx, key_ptr] : key_group) {
            const std::string& key = *key_ptr;
            auto tenant_it = shard->tenants.find(normalized_tenant);
            if (tenant_it == shard->tenants.end()) {
                VLOG(1) << "key=" << key << ", error=object_not_found";
                results[original_idx] =
                    tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                continue;
            }
            auto& tenant_state = tenant_it->second;
            auto it = tenant_state.metadata.find(key);

            if (it == tenant_state.metadata.end()) {
                VLOG(1) << "key=" << key << ", error=object_not_found";
                results[original_idx] =
                    tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                continue;
            }

            // Clean up stale replica handles (consistent with single Remove).
            auto cleanup_plan =
                BuildStaleHandleCleanupPlan(it->second, alive_clients);
            if (!cleanup_plan.removed_ids.empty()) {
                auto persist_result = PersistStaleHandleCleanupForHA(
                    "BatchRemove(stale cleanup)", normalized_tenant, key,
                    it->second, cleanup_plan);
                if (!persist_result) {
                    results[original_idx] =
                        tl::make_unexpected(persist_result.error());
                    continue;
                }
                if (enable_oplog_) {
                    results[original_idx] = tl::make_unexpected(
                        cleanup_plan.would_invalidate
                            ? ErrorCode::OBJECT_NOT_FOUND
                            : ErrorCode::OBJECT_ALREADY_EXISTS);
                    continue;
                } else if (CleanupStaleHandles(it->second, alive_clients,
                                               &shard)) {
                    EraseMetadata(tenant_state, it, normalized_tenant,
                                  QuotaEraseMode::kFull, &shard);
                    if (tenant_state.Empty()) {
                        shard->tenants.erase(tenant_it);
                    }
                    results[original_idx] =
                        tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                    continue;
                }
            }
            if (!it->second.IsValid()) {
                results[original_idx] =
                    tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                continue;
            }

            auto& metadata = it->second;

            std::vector<UUID> batch_local_disk_holders;
            metadata.VisitReplicas(
                [](const Replica& replica) {
                    return replica.is_local_disk_replica();
                },
                [&batch_local_disk_holders](Replica& replica) {
                    auto cid = replica.get_local_disk_client_id();
                    if (cid.has_value()) {
                        batch_local_disk_holders.push_back(cid.value());
                    }
                });

            if (!force && !metadata.IsLeaseExpired(now)) {
                VLOG(1) << "key=" << key << ", error=object_has_lease";
                results[original_idx] =
                    tl::make_unexpected(ErrorCode::OBJECT_HAS_LEASE);
                continue;
            }

            if (!metadata.AllReplicas(&Replica::fn_is_completed)) {
                LOG(ERROR) << "key=" << key << ", error=replica_not_ready";
                results[original_idx] =
                    tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
                continue;
            }

            if (tenant_state.replication_tasks.contains(key)) {
                LOG(ERROR) << "key=" << key
                           << ", error=object_has_replication_task";
                results[original_idx] =
                    tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
                continue;
            }

            // Remove object metadata
            if (enable_ha_) {
                if (enable_oplog_) {
                    auto reservation = ReserveBatchOpLogSlot();
                    if (!reservation) {
                        results[original_idx] =
                            tl::make_unexpected(reservation.error());
                        continue;
                    }
                    std::vector<ReplicaID> removed_ids;
                    metadata.VisitReplicas(
                        &Replica::fn_is_completed,
                        [&removed_ids](Replica& replica) {
                            removed_ids.push_back(replica.id());
                            replica.mark_removed();
                        });
                    auto persist_result =
                        AppendReservedOpLogWithDurableFinalize(
                            std::move(reservation.value()), OpType::REMOVE,
                            normalized_tenant.value(), key, {},
                            [this, removed_ids = std::move(removed_ids),
                             batch_local_disk_holders,
                             tenant_id = normalized_tenant.value(), key](
                                const OpLogEntry& durable_entry) {
                                FinalizeRemovedReplicasAfterDurable(
                                    durable_entry, removed_ids,
                                    QuotaEraseMode::kFull);
                                EnqueueRemoveTasks(
                                    batch_local_disk_holders,
                                    RemoveTaskItem{tenant_id, key});
                            });
                    if (!persist_result) {
                        results[original_idx] =
                            tl::make_unexpected(persist_result.error());
                        continue;
                    }
                    results[original_idx] = {};
                    continue;
                }
            }

            // Collect LOCAL_DISK replica holders before erasing, so we
            // can notify them to reclaim SSD space via RemoveObjectHeartbeat.
            EraseMetadata(tenant_state, it, normalized_tenant,
                          QuotaEraseMode::kFull, &shard);
            if (tenant_state.Empty()) {
                shard->tenants.erase(tenant_it);
            }

            // Push removed key to each LOCAL_DISK holder's removed_keys queue.
            EnqueueRemoveTasks(
                batch_local_disk_holders,
                RemoveTaskItem{normalized_tenant.value(), key});

            results[original_idx] = {};  // Success
        }
    }

    return results;
}

bool MasterService::CleanupStaleHandles(
    ObjectMetadata& metadata,
    const std::unordered_set<UUID, boost::hash<UUID>>& alive_clients,
    MetadataShardAccessorRW* shard) {
    bool had_completed_disk = metadata.HasReplica([](const Replica& r) {
        return r.is_local_disk_replica() && r.is_completed();
    });
    // Remove those with invalid allocators (memory replicas on unmounted
    // segments) and local_disk replicas whose owner client is no longer alive.
    const uint64_t before_charge = CompletedMemoryQuotaCharge(metadata);
    EraseReplicasWithCacheTotalAccounting(
        metadata, [&alive_clients](const Replica& replica) {
            return (replica.has_invalid_mem_handle() ||
                    replica.has_invalid_nof_handle() ||
                    replica.has_stale_local_disk_client(alive_clients)) &&
                   replica.is_completed();
        });
    const uint64_t after_charge = CompletedMemoryQuotaCharge(metadata);
    if (before_charge > after_charge) {
        ReleaseCommittedQuotaCharge(metadata, before_charge - after_charge);
    }
    if (had_completed_disk && shard &&
        !metadata.HasReplica([](const Replica& r) {
            return r.is_local_disk_replica() && r.is_completed();
        })) {
        shard->OnDiskReplicaRemoved(had_completed_disk, metadata);
    }

    // Return true if no valid replicas remain after cleanup
    return !metadata.IsValid();
}

size_t MasterService::GetKeyCount() const {
    size_t total = 0;
    for (size_t i = 0; i < kNumShards; i++) {
        MetadataShardAccessorRO shard(this, i);
        for (const auto& [tenant_id, tenant_state] : shard->tenants) {
            total += tenant_state.metadata.size();
        }
    }
    return total;
}

auto MasterService::Ping(const UUID& client_id)
    -> tl::expected<PingResponse, ErrorCode> {
    ClientStatus client_status;
    {
        std::shared_lock<std::shared_mutex> lock(client_mutex_);
        auto it = ok_client_.find(client_id);
        if (it != ok_client_.end()) {
            client_status = ClientStatus::OK;
        } else {
            client_status = ClientStatus::NEED_REMOUNT;
        }
    }
    PodUUID pod_client_id = {client_id.first, client_id.second};
    if (!client_ping_queue_.push(pod_client_id)) {
        // Queue is full
        LOG(ERROR) << "client_id=" << client_id
                   << ", error=client_ping_queue_full";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return PingResponse(view_version_, client_status);
}

tl::expected<std::string, ErrorCode> MasterService::GetFsdir() const {
    if (root_fs_dir_.empty() || cluster_id_.empty()) {
        LOG(INFO)
            << "Storage root directory or cluster ID is not set. persisting "
               "data is disabled.";
        return std::string();
    }
    return root_fs_dir_ + "/" + cluster_id_;
}

tl::expected<GetStorageConfigResponse, ErrorCode>
MasterService::GetStorageConfig() const {
    if (root_fs_dir_.empty() || cluster_id_.empty()) {
        LOG(INFO)
            << "Storage root directory or cluster ID is not set. persisting "
               "data is disabled.";
        return GetStorageConfigResponse("", enable_disk_eviction_,
                                        quota_bytes_);
    }
    std::string fsdir = root_fs_dir_ + "/" + cluster_id_;
    return GetStorageConfigResponse(fsdir, enable_disk_eviction_, quota_bytes_);
}

auto MasterService::MountLocalDiskSegment(const UUID& client_id,
                                          bool enable_offloading)
    -> tl::expected<void, ErrorCode> {
    if (!enable_offload_) {
        LOG(ERROR) << "	The offload functionality is not enabled";
        return tl::make_unexpected(ErrorCode::UNABLE_OFFLOAD);
    }
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();

    auto err =
        segment_access.MountLocalDiskSegment(client_id, enable_offloading);
    if (err == ErrorCode::SEGMENT_ALREADY_EXISTS) {
        // Return OK because this is an idempotent operation
        return {};
    } else if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    // Notify the client monitor thread to start tracking this client's TTL.
    // Without this, a client that only mounts a LOCAL_DISK segment (and
    // doesn't ping) would be considered expired by ClientMonitorFunc, which
    // would then clear all its LOCAL_DISK replicas.
    PodUUID pod_client_id;
    pod_client_id.first = client_id.first;
    pod_client_id.second = client_id.second;
    if (!client_ping_queue_.push(pod_client_id)) {
        LOG(ERROR) << "client_id=" << client_id
                   << ", error=client_ping_queue_full";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    return {};
}

auto MasterService::OffloadObjectHeartbeat(const UUID& client_id,
                                           bool enable_offloading)
    -> tl::expected<std::vector<OffloadTaskItem>, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    ScopedLocalDiskSegmentAccess local_disk_segment_access =
        segment_manager_.getLocalDiskSegmentAccess();
    auto& client_local_disk_segment =
        local_disk_segment_access.getClientLocalDiskSegment();
    auto local_disk_segment_it = client_local_disk_segment.find(client_id);
    if (local_disk_segment_it == client_local_disk_segment.end()) {
        LOG(ERROR) << "Local disk segment not found with client id = "
                   << client_id;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    std::vector<OffloadTaskItem> result;
    std::unordered_map<std::string, OffloadTaskItem> offloading_objects_copy;
    {
        MutexLocker locker(&local_disk_segment_it->second->offloading_mutex_);
        local_disk_segment_it->second->enable_offloading = enable_offloading;
        if (enable_offloading) {
            result.reserve(
                local_disk_segment_it->second->offloading_objects.size());
            for (const auto& [_, task] :
                 local_disk_segment_it->second->offloading_objects) {
                result.push_back(task);
            }
            local_disk_segment_it->second->offloading_objects.clear();
            return result;
        }
        // Offloading is disabled: clear the pending queue to prevent
        // unbounded growth that would trigger KEYS_ULTRA_LIMIT in
        // PushOffloadingQueue. We must also clean up corresponding
        // offloading_tasks and decrement source replica refcounts to avoid
        // resource leaks and blocked writes (OBJECT_HAS_REPLICATION_TASK).
        // Copy keys out before releasing the mutex to avoid lock order
        // violation: the lock order is Shard Lock -> offloading_mutex_, so we
        // must release offloading_mutex_ before taking shard locks via
        // MetadataAccessorRW.
        offloading_objects_copy =
            std::move(local_disk_segment_it->second->offloading_objects);
    }

    for (auto& [_, task] : offloading_objects_copy) {
        const auto object_id =
            MakeObjectIdentity(task.key, TenantId(task.tenant_id));
        MetadataAccessorRW accessor(this, object_id);
        if (accessor.Exists()) {
            auto& tenant_state = accessor.GetTenantState();
            auto task_it =
                tenant_state.offloading_tasks.find(object_id.user_key);
            if (task_it != tenant_state.offloading_tasks.end()) {
                auto& tasks = task_it->second;
                auto offload_it =
                    std::find_if(tasks.begin(), tasks.end(),
                                 [&client_id](const OffloadingTask& t) {
                                     return t.source_client_id == client_id;
                                 });
                if (offload_it != tasks.end()) {
                    auto source =
                        accessor.Get().GetReplicaByID(offload_it->source_id);
                    if (source) {
                        source->dec_refcnt();
                    }
                    tasks.erase(offload_it);
                    if (tasks.empty()) {
                        tenant_state.offloading_tasks.erase(task_it);
                    }
                }
            }
        }
    }
    return result;
}

auto MasterService::PollRemoveAll(const UUID& client_id)
    -> tl::expected<bool, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    ScopedLocalDiskSegmentAccess local_disk_segment_access =
        segment_manager_.getLocalDiskSegmentAccess();
    auto& client_local_disk_segment =
        local_disk_segment_access.getClientLocalDiskSegment();
    auto local_disk_segment_it = client_local_disk_segment.find(client_id);
    if (local_disk_segment_it == client_local_disk_segment.end()) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    bool result;
    {
        MutexLocker locker(&local_disk_segment_it->second->offloading_mutex_);
        result = local_disk_segment_it->second->pending_remove_all;
        local_disk_segment_it->second->pending_remove_all = false;
    }
    return result;
}

auto MasterService::RemoveObjectHeartbeat(const UUID& client_id)
    -> tl::expected<std::vector<RemoveTaskItem>, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    ScopedLocalDiskSegmentAccess local_disk_segment_access =
        segment_manager_.getLocalDiskSegmentAccess();
    auto& client_local_disk_segment =
        local_disk_segment_access.getClientLocalDiskSegment();
    auto local_disk_segment_it = client_local_disk_segment.find(client_id);
    if (local_disk_segment_it == client_local_disk_segment.end()) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    {
        MutexLocker locker(&local_disk_segment_it->second->offloading_mutex_);
        return local_disk_segment_it->second->removed_keys;
    }
}

void MasterService::EnqueueRemoveTasks(
    const std::vector<UUID>& holder_ids, const RemoveTaskItem& task) {
    if (holder_ids.empty()) return;
    ScopedLocalDiskSegmentAccess access =
        segment_manager_.getLocalDiskSegmentAccess();
    auto& segments = access.getClientLocalDiskSegment();
    for (const auto& holder_id : holder_ids) {
        auto it = segments.find(holder_id);
        if (it == segments.end()) continue;
        MutexLocker locker(&it->second->offloading_mutex_);
        if (std::find(it->second->removed_keys.begin(),
                      it->second->removed_keys.end(), task) ==
            it->second->removed_keys.end()) {
            it->second->removed_keys.push_back(task);
        }
    }
}

auto MasterService::AckRemoveObjectHeartbeat(
    const UUID& client_id, const std::vector<RemoveTaskItem>& tasks)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    ScopedLocalDiskSegmentAccess access =
        segment_manager_.getLocalDiskSegmentAccess();
    auto& segments = access.getClientLocalDiskSegment();
    auto it = segments.find(client_id);
    if (it == segments.end()) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    MutexLocker locker(&it->second->offloading_mutex_);
    auto& pending = it->second->removed_keys;
    pending.erase(std::remove_if(pending.begin(), pending.end(),
                                 [&tasks](const RemoveTaskItem& task) {
                                     return std::find(tasks.begin(),
                                                      tasks.end(), task) !=
                                            tasks.end();
                                 }),
                  pending.end());
    return {};
}

auto MasterService::ReportSsdCapacity(const UUID& client_id,
                                      int64_t ssd_total_capacity_bytes)
    -> tl::expected<void, ErrorCode> {
    if (ssd_total_capacity_bytes < 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    ScopedLocalDiskSegmentAccess local_disk_segment_access =
        segment_manager_.getLocalDiskSegmentAccess();
    auto& client_local_disk_segment =
        local_disk_segment_access.getClientLocalDiskSegment();
    auto local_disk_segment_it = client_local_disk_segment.find(client_id);
    if (local_disk_segment_it == client_local_disk_segment.end()) {
        LOG(ERROR) << "Local disk segment not found with client id = "
                   << client_id;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    MutexLocker locker(&local_disk_segment_it->second->offloading_mutex_);
    int64_t old_capacity =
        local_disk_segment_it->second->ssd_total_capacity_bytes;
    if (ssd_total_capacity_bytes != old_capacity) {
        local_disk_segment_it->second->ssd_total_capacity_bytes =
            ssd_total_capacity_bytes;
        if (old_capacity > 0) {
            MasterMetricManager::instance().dec_total_file_capacity(
                old_capacity);
        }
        if (ssd_total_capacity_bytes > 0) {
            MasterMetricManager::instance().inc_total_file_capacity(
                ssd_total_capacity_bytes);
        }
    }
    return {};
}

auto MasterService::NotifyOffloadSuccess(
    const UUID& client_id, const std::vector<OffloadTaskItem>& tasks,
    const std::vector<StorageObjectMetadata>& metadatas)
    -> tl::expected<void, ErrorCode> {
    if (tasks.size() != metadatas.size()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::shared_ptr<LocalDiskSegment> local_disk_segment;
    {
        ScopedLocalDiskSegmentAccess ssd_access =
            segment_manager_.getLocalDiskSegmentAccess();
        auto& client_segments = ssd_access.getClientLocalDiskSegment();
        auto disk_it = client_segments.find(client_id);
        if (disk_it != client_segments.end()) {
            local_disk_segment = disk_it->second;
        }
    }

    for (size_t i = 0; i < tasks.size(); ++i) {
        const auto& task = tasks[i];
        const auto& metadata = metadatas[i];
        const TenantId task_tenant = enable_multi_tenants_
                                         ? TenantId(task.tenant_id)
                                         : TenantId::Default();
        const auto request_object_id =
            MakeObjectIdentityForRequest(task.key, task_tenant);

        // NACK sentinel: offload failed on worker. Clean up the
        // offloading_task + dec_refcnt but skip AddReplica.
        if (metadata.data_size < 0) {
            std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
            MetadataAccessorRW accessor(this, request_object_id);
            if (accessor.Exists()) {
                auto& tenant_state = accessor.GetTenantState();
                auto task_it = tenant_state.offloading_tasks.find(
                    request_object_id.user_key);
                if (task_it != tenant_state.offloading_tasks.end()) {
                    auto& tasks = task_it->second;
                    auto offload_it =
                        std::find_if(tasks.begin(), tasks.end(),
                                     [&client_id](const OffloadingTask& t) {
                                         return t.source_client_id == client_id;
                                     });
                    if (offload_it != tasks.end()) {
                        auto source =
                            accessor.Get().GetReplicaByID(offload_it->source_id);
                        if (source != nullptr) {
                            source->dec_refcnt();
                        }
                        tasks.erase(offload_it);
                        if (tasks.empty()) {
                            tenant_state.offloading_tasks.erase(task_it);
                        }
                    }
                }
            }
            continue;
        }

        Replica replica(client_id, metadata.data_size,
                        metadata.transport_endpoint, ReplicaStatus::COMPLETE);
        bool handled_existing_object = false;
        bool added_new_local_disk_replica = false;
        {
            std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
            MetadataAccessorRW accessor(this, request_object_id);
            if (accessor.Exists()) {
                auto& obj_metadata = accessor.Get();
                auto& tenant_state = accessor.GetTenantState();
                auto task_it = tenant_state.offloading_tasks.find(
                    request_object_id.user_key);
                if (task_it != tenant_state.offloading_tasks.end() &&
                    replica.type() != ReplicaType::LOCAL_DISK) {
                    LOG(ERROR) << "Invalid replica type: " << replica.type()
                               << ". Expected ReplicaType::LOCAL_DISK.";
                    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                }

                // Clean up the offloading task if present.
                if (task_it != tenant_state.offloading_tasks.end()) {
                    auto& tasks = task_it->second;
                    auto offload_it =
                        std::find_if(tasks.begin(), tasks.end(),
                                     [&client_id](const OffloadingTask& t) {
                                         return t.source_client_id == client_id;
                                     });
                    if (offload_it != tasks.end()) {
                        auto source =
                            obj_metadata.GetReplicaByID(offload_it->source_id);
                        if (source != nullptr) {
                            source->dec_refcnt();
                        }
                        tasks.erase(offload_it);
                        if (tasks.empty()) {
                            tenant_state.offloading_tasks.erase(task_it);
                        }
                    }
                }

                // Register / update the LOCAL_DISK replica for this
                // object. Handles both the offload-completion case and
                // the remount / re-registration case.
                if (!obj_metadata.HasReplica(
                        &Replica::fn_is_local_disk_replica)) {
                    std::vector<Replica> replicas;
                    replicas.emplace_back(std::move(replica));
                    obj_metadata.AddReplicas(std::move(replicas));
                    auto& shard = accessor.GetShard();
                    shard.OnDiskReplicaAdded(obj_metadata);
                    SyncCacheTotalAccounting(obj_metadata);
                    added_new_local_disk_replica = true;
                } else {
                    size_t updated = obj_metadata.VisitReplicas(
                        [client_id](const Replica& rep) {
                            return rep.type() == ReplicaType::LOCAL_DISK &&
                                   rep.get_local_disk_client_id() ==
                                       client_id;
                        },
                        [&metadata](Replica& rep) {
                            rep.update_local_disk_location(
                                metadata.transport_endpoint,
                                metadata.data_size);
                        });
                    if (updated == 0) {
                        std::vector<Replica> replicas;
                        replicas.emplace_back(std::move(replica));
                        obj_metadata.AddReplicas(std::move(replicas));
                        auto& shard = accessor.GetShard();
                        shard.OnDiskReplicaAdded(obj_metadata);
                        SyncCacheTotalAccounting(obj_metadata);
                        added_new_local_disk_replica = true;
                    }
                }
                handled_existing_object = true;
            }
        }

        if (!handled_existing_object) {
            auto normalized_tenant_result =
                ResolveTenantIdForWrite(request_object_id.tenant_id);
            if (!normalized_tenant_result) {
                return tl::make_unexpected(normalized_tenant_result.error());
            }
            const ObjectIdentity object_id{
                std::move(normalized_tenant_result.value()),
                request_object_id.user_key};

            auto res = AddReplica(client_id, object_id.user_key,
                                  object_id.tenant_id, replica);
            if (!res) {
                if (res.error() == ErrorCode::OBJECT_NOT_FOUND) {
                    continue;
                }
                LOG(WARNING) << "Failed to add replica, skipping object: "
                             << "error=" << res.error()
                             << ", client_id=" << client_id
                             << ", tenant_id=" << object_id.tenant_id.value()
                             << ", key=" << object_id.user_key;
                continue;
            }
            added_new_local_disk_replica = res.value();
        }
        if (local_disk_segment && metadata.data_size > 0 &&
            added_new_local_disk_replica) {
            local_disk_segment->ssd_used_bytes.fetch_add(
                metadata.data_size, std::memory_order_relaxed);
        }
    }

    return {};
}

tl::expected<std::vector<UUID>, ErrorCode> MasterService::PushOffloadingQueue(
    const ObjectIdentity& object_id, Replica& replica) {
    const auto& segment_names = replica.get_segment_names();
    if (segment_names.empty()) {
        return {};
    }
    std::vector<UUID> queued_clients;
    queued_clients.reserve(segment_names.size());
    for (const auto& segment_name_it : segment_names) {
        if (!segment_name_it.has_value()) {
            continue;
        }
        ScopedLocalDiskSegmentAccess local_disk_segment_access =
            segment_manager_.getLocalDiskSegmentAccess();
        const auto& client_by_name =
            local_disk_segment_access.getClientByName();
        auto client_id_it = client_by_name.find(segment_name_it.value());
        if (client_id_it == client_by_name.end()) {
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        auto& client_local_disk_segment =
            local_disk_segment_access.getClientLocalDiskSegment();
        auto local_disk_segment_it =
            client_local_disk_segment.find(client_id_it->second);
        if (local_disk_segment_it == client_local_disk_segment.end()) {
            return tl::make_unexpected(ErrorCode::UNABLE_OFFLOADING);
        }
        MutexLocker locker(&local_disk_segment_it->second->offloading_mutex_);
        if (!local_disk_segment_it->second->enable_offloading) {
            return tl::make_unexpected(ErrorCode::UNABLE_OFFLOADING);
        }
        if (local_disk_segment_it->second->offloading_objects.size() >=
            offloading_queue_limit_) {
            return tl::make_unexpected(ErrorCode::KEYS_ULTRA_LIMIT);
        }
        const int64_t size = replica.get_descriptor()
                                 .get_memory_descriptor()
                                 .buffer_descriptor.size_;
        auto res = local_disk_segment_it->second->offloading_objects.emplace(
            object_id.tenant_id.MakeScopedKey(object_id.user_key),
            OffloadTaskItem{.tenant_id = object_id.tenant_id.value(),
                            .key = object_id.user_key,
                            .size = size});
        if (!res.second) {
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }
        queued_clients.push_back(client_id_it->second);
    }
    return queued_clients;
}

// Promotion-on-hit

// Push a key onto the holder client's promotion_objects map. Resolves the
// holder via the LOCAL_DISK replica's embedded client_id rather than via
// the segment-name reverse lookup.
tl::expected<void, ErrorCode> MasterService::PushPromotionQueue(
    const ObjectIdentity& object_id, Replica& source_replica) {
    auto holder_id = source_replica.get_local_disk_client_id();
    if (!holder_id.has_value()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    ScopedLocalDiskSegmentAccess local_disk_segment_access =
        segment_manager_.getLocalDiskSegmentAccess();
    auto& client_local_disk_segment =
        local_disk_segment_access.getClientLocalDiskSegment();
    auto local_disk_segment_it =
        client_local_disk_segment.find(holder_id.value());
    if (local_disk_segment_it == client_local_disk_segment.end()) {
        // Holder client expired or never had a LocalDiskSegment registered;
        // the LOCAL_DISK replica will be cleaned up by ClientMonitorFunc on
        // its own schedule.
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    MutexLocker locker(&local_disk_segment_it->second->offloading_mutex_);
    auto res = local_disk_segment_it->second->promotion_objects.emplace(
        object_id.tenant_id.MakeScopedKey(object_id.user_key),
        PromotionTaskItem{
            .tenant_id = object_id.tenant_id.value(),
            .key = object_id.user_key,
            .size = static_cast<int64_t>(source_replica.get_descriptor()
                                             .get_local_disk_descriptor()
                                             .object_size)});
    if (!res.second) {
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }
    return {};
}

// --- Promotion retry candidate helpers ---

void MasterService::DecrementCandidateCount() {
    uint64_t count = promotion_candidate_count_.load(std::memory_order_relaxed);
    while (count > 0) {
        if (promotion_candidate_count_.compare_exchange_weak(
                count, count - 1, std::memory_order_relaxed)) {
            return;
        }
    }
}

void MasterService::EraseCandidate(TenantState& tenant_state,
                                   const std::string& key) {
    if (tenant_state.promotion_candidates.erase(key) > 0) {
        DecrementCandidateCount();
    }
}

void MasterService::EraseCandidate(const ObjectIdentity& object_id) {
    MetadataShardAccessorRW shard(
        this, getMetadataShardIndex(object_id.tenant_id, object_id.user_key));
    auto tenant_it = shard->tenants.find(object_id.tenant_id);
    if (tenant_it == shard->tenants.end()) return;
    EraseCandidate(tenant_it->second, object_id.user_key);
    if (tenant_it->second.Empty()) {
        shard->tenants.erase(tenant_it);
    }
}

void MasterService::RecordOrUpdateCandidate(TenantState& tenant_state,
                                            const std::string& key,
                                            uint8_t sketch_score,
                                            PromotionCandidateReason reason,
                                            ErrorCode last_error) {
    const auto now = std::chrono::steady_clock::now();
    auto it = tenant_state.promotion_candidates.find(key);
    if (it != tenant_state.promotion_candidates.end()) {
        // Update existing entry: refresh last_seen, reset
        // retry_after/retry_count.
        it->second.last_seen = now;
        it->second.last_reason = reason;
        it->second.last_error = last_error;
        if (sketch_score > it->second.sketch_score) {
            it->second.sketch_score = sketch_score;
        }
        it->second.retry_after = now;
        it->second.retry_count = 0;
        return;
    }

    // Reserve a slot in the global candidate limit.
    uint64_t count = promotion_candidate_count_.load(std::memory_order_relaxed);
    while (count < kPromotionCandidateLimit) {
        if (promotion_candidate_count_.compare_exchange_weak(
                count, count + 1, std::memory_order_relaxed)) {
            break;
        }
    }
    if (count >= kPromotionCandidateLimit) {
        VLOG(1) << "promotion_candidate_dropped key=" << key
                << " reason=global_limit";
        MasterMetricManager::instance().inc_promotion_candidate_dropped_limit();
        return;
    }

    auto [emplace_it, inserted] = tenant_state.promotion_candidates.emplace(
        key, PromotionCandidate{.sketch_score = sketch_score,
                                .first_seen = now,
                                .last_seen = now,
                                .retry_after = now,
                                .last_reason = reason,
                                .last_error = last_error,
                                .retry_count = 0});
    if (inserted) {
        MasterMetricManager::instance().inc_promotion_candidate_recorded();
        VLOG(1) << "promotion_candidate_recorded key=" << key;
    } else {
        DecrementCandidateCount();
    }
}

std::chrono::milliseconds MasterService::CandidateBackoff(
    uint32_t retry_count) const {
    uint64_t backoff_ms =
        static_cast<uint64_t>(kPromotionCandidateInitialBackoff.count());
    for (uint32_t i = 1; i < retry_count; ++i) {
        backoff_ms = std::min<uint64_t>(
            backoff_ms * 2,
            static_cast<uint64_t>(kPromotionCandidateMaxBackoff.count()));
    }
    return std::chrono::milliseconds(backoff_ms);
}

bool MasterService::IsTransientResult(PromotionQueueResult result) const {
    return result == PromotionQueueResult::kWatermarkRejected ||
           result == PromotionQueueResult::kQueueCapRejected ||
           result == PromotionQueueResult::kPushFailed;
}

void MasterService::BackoffCandidate(const ObjectIdentity& object_id,
                                     PromotionQueueResult result) {
    const auto now = std::chrono::steady_clock::now();
    MetadataShardAccessorRW shard(
        this, getMetadataShardIndex(object_id.tenant_id, object_id.user_key));
    auto tenant_it = shard->tenants.find(object_id.tenant_id);
    if (tenant_it == shard->tenants.end()) return;
    auto& tenant_state = tenant_it->second;
    auto candidate_it =
        tenant_state.promotion_candidates.find(object_id.user_key);
    if (candidate_it == tenant_state.promotion_candidates.end()) return;

    auto& c = candidate_it->second;
    c.retry_count++;
    if (result == PromotionQueueResult::kWatermarkRejected) {
        c.last_reason = PromotionCandidateReason::kWatermark;
        c.last_error = ErrorCode::OK;
    } else if (result == PromotionQueueResult::kQueueCapRejected) {
        c.last_reason = PromotionCandidateReason::kQueueCap;
        c.last_error = ErrorCode::OK;
    } else {
        c.last_reason = PromotionCandidateReason::kPushFailed;
    }

    const bool ttl_expired = now - c.last_seen >= kPromotionCandidateTtl;
    if (ttl_expired || c.retry_count >= kPromotionCandidateMaxRetries) {
        VLOG(1) << "promotion_candidate_gave_up key=" << object_id.user_key
                << " retries=" << c.retry_count;
        EraseCandidate(tenant_state, object_id.user_key);
        MasterMetricManager::instance()
            .inc_promotion_candidate_expired_evaluated();
    } else {
        c.retry_after = now + CandidateBackoff(c.retry_count);
    }

    if (tenant_state.Empty()) {
        shard->tenants.erase(tenant_it);
    }
}

void MasterService::ClearCandidatesForReload() {
    for (size_t i = 0; i < kNumShards; ++i) {
        MetadataShardAccessorRW shard(this, i);
        for (auto& [tenant_id, tenant_state] : shard->tenants) {
            (void)tenant_id;
            tenant_state.promotion_candidates.clear();
        }
    }
    promotion_candidate_count_.store(0, std::memory_order_relaxed);
    promotion_retry_cursor_.store(0, std::memory_order_relaxed);
    promotion_in_flight_.store(0, std::memory_order_relaxed);
}

size_t MasterService::RunPromotionCandidateRetry() {
    return RunPromotionCandidateRetry(kPromotionRetryShardBatch);
}

size_t MasterService::RunPromotionCandidateRetryForTesting() {
    return RunPromotionCandidateRetry(kNumShards);
}

size_t MasterService::CountCandidatesForTesting(const TenantId& tenant_id) {
    size_t count = 0;
    std::shared_lock<std::shared_mutex> lock(snapshot_mutex_);
    for (size_t i = 0; i < kNumShards; i++) {
        MetadataShardAccessorRO shard(this, i);
        auto it = shard->tenants.find(tenant_id);
        if (it != shard->tenants.end()) {
            count += it->second.promotion_candidates.size();
        }
    }
    return count;
}

void MasterService::ResetCandidateBackoffsForTesting() {
    const auto epoch = std::chrono::steady_clock::time_point{};
    for (size_t i = 0; i < kNumShards; i++) {
        MetadataShardAccessorRW shard(this, i);
        for (auto& [tenant_id, tenant_state] : shard->tenants) {
            (void)tenant_id;
            for (auto& [key, candidate] : tenant_state.promotion_candidates) {
                (void)key;
                candidate.retry_after = epoch;
            }
        }
    }
}

size_t MasterService::RunPromotionCandidateRetry(size_t max_shards_to_scan) {
    if (!promotion_on_hit_ ||
        promotion_candidate_count_.load(std::memory_order_relaxed) == 0) {
        return 0;
    }

    const auto now = std::chrono::steady_clock::now();
    std::vector<ObjectIdentity> due_candidates;
    due_candidates.reserve(kPromotionRetryBatchSize);

    const size_t shards_to_scan = std::min(max_shards_to_scan, kNumShards);
    if (shards_to_scan == 0) return 0;
    const size_t start_shard = promotion_retry_cursor_.fetch_add(
                                   shards_to_scan, std::memory_order_relaxed) %
                               kNumShards;

    {
        std::shared_lock<std::shared_mutex> snap_lock(snapshot_mutex_);
        for (size_t scanned = 0;
             scanned < shards_to_scan &&
             due_candidates.size() < kPromotionRetryBatchSize;
             ++scanned) {
            const size_t i = (start_shard + scanned) % kNumShards;
            MetadataShardAccessorRW shard(this, i);
            for (auto tenant_it = shard->tenants.begin();
                 tenant_it != shard->tenants.end() &&
                 due_candidates.size() < kPromotionRetryBatchSize;) {
                auto& tenant_state = tenant_it->second;
                for (auto cit = tenant_state.promotion_candidates.begin();
                     cit != tenant_state.promotion_candidates.end() &&
                     due_candidates.size() < kPromotionRetryBatchSize;) {
                    const auto& key = cit->first;
                    auto& c = cit->second;

                    const bool ttl_expired =
                        now - c.last_seen >= kPromotionCandidateTtl;
                    if (ttl_expired ||
                        c.retry_count >= kPromotionCandidateMaxRetries) {
                        VLOG(1) << "promotion_candidate_expired key=" << key
                                << " retry_count=" << c.retry_count;
                        const uint32_t saved_retry_count = c.retry_count;
                        cit = tenant_state.promotion_candidates.erase(cit);
                        DecrementCandidateCount();
                        // retry_count == 0: scheduler never reached this
                        // candidate before TTL elapsed — scan budget was
                        // too small. retry_count > 0: scheduler evaluated
                        // it but gave up after retries or TTL.
                        if (saved_retry_count == 0) {
                            MasterMetricManager::instance()
                                .inc_promotion_candidate_expired_unevaluated();
                        } else {
                            MasterMetricManager::instance()
                                .inc_promotion_candidate_expired_evaluated();
                        }
                        continue;
                    }
                    if (c.retry_after > now) {
                        ++cit;
                        continue;
                    }

                    // Quick pre-filter under shard lock to avoid adding
                    // candidates that are obviously ineligible.
                    auto meta_it = tenant_state.metadata.find(key);
                    if (meta_it == tenant_state.metadata.end() ||
                        !meta_it->second.IsValid() ||
                        tenant_state.promotion_tasks.count(key) > 0 ||
                        meta_it->second.HasReplica(
                            &Replica::fn_is_memory_replica) ||
                        !meta_it->second.HasReplica(
                            &Replica::fn_is_local_disk_replica)) {
                        cit = tenant_state.promotion_candidates.erase(cit);
                        DecrementCandidateCount();
                        continue;
                    }

                    due_candidates.push_back(ObjectIdentity{
                        .tenant_id = tenant_it->first, .user_key = key});
                    ++cit;
                }

                if (tenant_state.Empty()) {
                    tenant_it = shard->tenants.erase(tenant_it);
                } else {
                    ++tenant_it;
                }
            }
        }
    }

    size_t queued = 0;
    {
        std::shared_lock<std::shared_mutex> snap_lock(snapshot_mutex_);
        for (const auto& object_id : due_candidates) {
            const auto result =
                TryPushPromotionQueue(object_id, /*record_candidate=*/false);
            if (result == PromotionQueueResult::kQueued) {
                queued++;
                MasterMetricManager::instance()
                    .inc_promotion_candidate_admitted();
            } else if (IsTransientResult(result)) {
                MasterMetricManager::instance()
                    .inc_promotion_candidate_admission_rejected();
                BackoffCandidate(object_id, result);
            } else {
                EraseCandidate(object_id);
            }
        }
    }

    return queued;
}

MasterService::PromotionQueueResult MasterService::TryPushPromotionQueue(
    const ObjectIdentity& object_id, bool record_candidate) {
    if (!promotion_on_hit_ || !promotion_sketch_) {
        return PromotionQueueResult::kDisabled;
    }
    const auto& key = object_id.user_key;
    const auto admission_key = object_id.tenant_id.MakeScopedKey(key);

    // Frequency gate: bump and compare against the threshold. The sketch
    // returns uint8_t (saturating at 255); promotion_admission_threshold_
    // is clamped into [1, 255] at config parse time (see master.cpp), so
    // direct comparison is well-defined and threshold=0 (which would
    // bypass the gate entirely since freq is uint8_t) cannot reach here.
    const uint8_t freq = promotion_sketch_->increment(admission_key);
    if (freq < promotion_admission_threshold_) {
        MasterMetricManager::instance().inc_promotion_rejected_frequency();
        return PromotionQueueResult::kFrequencyRejected;
    }

    // Watermark gate: don't promote if DRAM is already under eviction
    // pressure. The check is best-effort (state can change between this
    // sample and the actual allocation in PromotionAllocStart).
    const double used_ratio =
        MasterMetricManager::instance().get_global_mem_used_ratio();
    if (used_ratio >= eviction_high_watermark_ratio_) {
        MasterMetricManager::instance().inc_promotion_rejected_watermark();
        if (record_candidate) {
            MetadataAccessorRW accessor(this, object_id);
            if (accessor.Exists()) {
                RecordOrUpdateCandidate(accessor.GetTenantState(), key, freq,
                                        PromotionCandidateReason::kWatermark,
                                        ErrorCode::OK);
            }
        }
        return PromotionQueueResult::kWatermarkRejected;
    }

    // Acquire a fresh RW shard accessor for dedup, refcnt-pin, and task
    // record. Safe to call here because GetReplicaList has already released
    // its RO accessor.
    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        return PromotionQueueResult::kNotFound;
    }
    auto& metadata = accessor.Get();
    auto& tenant_state = accessor.GetTenantState();

    // Dedup: don't queue twice if a promotion is already in flight or if a
    // MEMORY replica has appeared since GetReplicaList observed only-disk.
    if (tenant_state.promotion_tasks.count(key) > 0) {
        EraseCandidate(tenant_state, key);
        return PromotionQueueResult::kAlreadyInFlight;
    }
    if (metadata.HasReplica(&Replica::fn_is_memory_replica)) {
        EraseCandidate(tenant_state, key);
        return PromotionQueueResult::kMemoryReplicaPresent;
    }

    // Find the LOCAL_DISK source replica.
    Replica* source = nullptr;
    metadata.VisitReplicas(&Replica::fn_is_local_disk_replica,
                           [&source](Replica& r) {
                               if (source == nullptr) source = &r;
                           });
    if (source == nullptr) {
        EraseCandidate(tenant_state, key);
        return PromotionQueueResult::kNoLocalDiskSource;
    }

    // Cap gate: read the cluster-wide in-flight count. Soft cap — a
    // benign TOCTOU race between this load and the emplace below can let
    // a few extra tasks slip in, but the per-shard mutex already
    // serializes inserts within a shard and the dedup gate above prevents
    // duplicate work, so the worst case is N concurrent inserters across
    // distinct shards each admitting one extra task. Atomic load is
    // relaxed because the value is purely advisory.
    if (promotion_in_flight_.load(std::memory_order_relaxed) >=
        promotion_queue_limit_) {
        MasterMetricManager::instance().inc_promotion_rejected_cap();
        if (record_candidate) {
            RecordOrUpdateCandidate(tenant_state, key, freq,
                                    PromotionCandidateReason::kQueueCap,
                                    ErrorCode::OK);
        }
        return PromotionQueueResult::kQueueCapRejected;
    }

    // Pin the source replica.
    source->inc_refcnt();
    const uint64_t object_size =
        source->get_descriptor().get_local_disk_descriptor().object_size;

    // Try to enqueue on the holder client. On failure, drop the refcnt back.
    auto push_result = PushPromotionQueue(object_id, *source);
    if (!push_result) {
        source->dec_refcnt();
        VLOG(1) << "promotion_push_failed key=" << key
                << " error=" << push_result.error();
        if (push_result.error() == ErrorCode::OBJECT_ALREADY_EXISTS) {
            EraseCandidate(tenant_state, key);
            return PromotionQueueResult::kAlreadyInFlight;
        }
        if (push_result.error() == ErrorCode::SEGMENT_NOT_FOUND ||
            push_result.error() == ErrorCode::INVALID_PARAMS) {
            EraseCandidate(tenant_state, key);
            return PromotionQueueResult::kNoLocalDiskSource;
        }
        if (record_candidate) {
            RecordOrUpdateCandidate(tenant_state, key, freq,
                                    PromotionCandidateReason::kPushFailed,
                                    push_result.error());
        }
        return PromotionQueueResult::kPushFailed;
    }

    // Capture the holder client_id so NotifyPromotionSuccess can reject
    // calls from other clients. PushPromotionQueue already validated
    // get_local_disk_client_id() returns a value, so .value() is safe.
    const UUID holder_id = source->get_local_disk_client_id().value();

    // Record the in-flight task. alloc_id is filled in by
    // PromotionAllocStart once the new MEMORY replica is staged.
    EraseCandidate(tenant_state, key);
    tenant_state.promotion_tasks.emplace(
        key, PromotionTask{.source_id = source->id(),
                           .alloc_id = 0,
                           .object_size = object_size,
                           .start_time = std::chrono::system_clock::now(),
                           .holder_id = holder_id});
    promotion_in_flight_.fetch_add(1, std::memory_order_relaxed);
    MasterMetricManager::instance().inc_promotion_in_flight();
    MasterMetricManager::instance().inc_promotion_admitted();
    VLOG(1) << "promotion_queued key=" << key << " size=" << object_size;
    return PromotionQueueResult::kQueued;
}

auto MasterService::PromotionObjectHeartbeat(const UUID& client_id)
    -> tl::expected<std::vector<PromotionTaskItem>, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    ScopedLocalDiskSegmentAccess local_disk_segment_access =
        segment_manager_.getLocalDiskSegmentAccess();
    auto& client_local_disk_segment =
        local_disk_segment_access.getClientLocalDiskSegment();
    auto local_disk_segment_it = client_local_disk_segment.find(client_id);
    if (local_disk_segment_it == client_local_disk_segment.end()) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    MutexLocker locker(&local_disk_segment_it->second->offloading_mutex_);
    // Return at most promotion_max_per_heartbeat_ tasks. Each task does
    // a synchronous SSD read + RDMA write on the client side; allowing
    // more than one per heartbeat risks blocking past the client-
    // liveness window and the master marking the client dead. The rest
    // stay queued in promotion_objects for subsequent heartbeats. The
    // cap must live here (server side) rather than on the client so
    // leftover work isn't silently dropped.
    auto& src = local_disk_segment_it->second->promotion_objects;
    std::vector<PromotionTaskItem> result;
    while (result.size() < promotion_max_per_heartbeat_ && !src.empty()) {
        auto node = src.extract(src.begin());
        result.push_back(std::move(node.mapped()));
    }
    return result;
}

auto MasterService::PromotionAllocStart(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id,
    uint64_t size, const std::vector<std::string>& preferred_segments)
    -> tl::expected<PromotionAllocStartResponse, ErrorCode> {
    auto normalized_tenant_result = ResolveTenantIdForWrite(tenant_id);
    if (!normalized_tenant_result) {
        return tl::make_unexpected(normalized_tenant_result.error());
    }
    const ObjectIdentity object_id{std::move(normalized_tenant_result.value()),
                                   key};
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto& metadata = accessor.Get();

    // Verify the in-flight task still exists before allocating. The
    // reaper can sweep it between the holder's heartbeat and this
    // AllocStart call (a hung client, GC pause, or HA failover can
    // stall AllocStart past put_start_release_timeout_sec_). If we
    // allocated and AddReplicas'd anyway, the staged PROCESSING MEMORY
    // replica would have no PromotionTask pointing at it: the generic
    // PROCESSING reaper iterates tenant_state.processing_keys (never
    // populated by promotion) and the promotion-task reaper would have
    // nothing left to iterate, leaking the buffer until the object is
    // removed or evicted. The shard mutex is held for the rest of this
    // function, so the iterator stays valid across the allocation step.
    auto& tenant_state = accessor.GetTenantState();
    auto task_it = tenant_state.promotion_tasks.find(object_id.user_key);
    if (task_it == tenant_state.promotion_tasks.end()) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    // Holder-only gate (see PromotionTask::holder_id doc).
    if (task_it->second.holder_id != client_id) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Defensive size check: must match the source LOCAL_DISK
    // descriptor's object_size captured at admission. A mismatch would
    // let a buggy caller request a wrong-sized allocation — smaller
    // risks RDMA overflow, larger wastes DRAM pinned until reaper TTL.
    if (task_it->second.object_size != size) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (metadata.HasReplica(&Replica::fn_is_memory_replica)) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }
    if (task_it->second.alloc_id != 0 ||
        task_it->second.reserved_quota_charge_bytes != 0) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    const uint64_t reserved_quota_charge = size;
    auto quota_result =
        ReserveTenantQuota(object_id.tenant_id, reserved_quota_charge);
    if (!quota_result) {
        return tl::make_unexpected(quota_result.error());
    }
    auto abort_reserved_quota = [&] {
        AbortTenantQuota(object_id.tenant_id, reserved_quota_charge);
    };

    // Allocate a single MEMORY replica via the existing strategy, biased to
    // the holder's mem segment when possible.
    ReplicateConfig config;
    config.replica_num = 1;
    if (!preferred_segments.empty()) {
        config.preferred_segments = preferred_segments;
    }

    std::vector<Replica> staged_replicas;
    {
        ScopedAllocatorAccess allocator_access =
            segment_manager_.getAllocatorAccess();
        const auto& allocator_manager = allocator_access.getAllocatorManager();
        auto allocation_result = allocation_strategy_->Allocate(
            allocator_manager, size, config.replica_num, preferred_segments);
        if (!allocation_result) {
            abort_reserved_quota();
            return tl::make_unexpected(allocation_result.error());
        }
        staged_replicas = std::move(allocation_result.value());
    }
    if (staged_replicas.empty()) {
        abort_reserved_quota();
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    // Append the new PROCESSING MEMORY replica to the existing object's
    // metadata. Visible only after NotifyPromotionSuccess flips it COMPLETE.
    Replica::Descriptor desc = staged_replicas[0].get_descriptor();
    const ReplicaID new_id = staged_replicas[0].id();
    std::vector<Replica> to_add;
    to_add.push_back(std::move(staged_replicas[0]));
    metadata.AddReplicas(std::move(to_add));

    // Record the new replica's ID on the in-flight PromotionTask so
    // NotifyPromotionSuccess knows exactly which replica to commit. A
    // concurrent Put on this key may stage other PROCESSING MEMORY
    // replicas; using alloc_id avoids the "first PROCESSING memory"
    // ambiguity.
    //
    // Also reset start_time so the reaper TTL covers the active-
    // transfer phase (AllocStart -> SSD read -> RDMA write -> Notify)
    // measured from when a master-allocated buffer becomes vulnerable,
    // rather than being consumed by queue-waiting. Without the reset,
    // a backlogged task could enter active transfer with little TTL
    // remaining and the reaper could free the staged replica via
    // EraseReplicaByID mid-RDMA-write. The queue-waiting phase
    // (alloc_id == 0) is bounded by its own original start_time window
    // during which the reaper's EraseReplicaByID branch is a no-op.
    task_it->second.alloc_id = new_id;
    task_it->second.reserved_quota_charge_bytes = reserved_quota_charge;
    task_it->second.start_time = std::chrono::system_clock::now();
    return PromotionAllocStartResponse{std::move(desc)};
}

auto MasterService::NotifyPromotionSuccess(const UUID& client_id,
                                           const std::string& key,
                                           const TenantId& tenant_id)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto& metadata = accessor.Get();
    auto& tenant_state = accessor.GetTenantState();

    // Look up the in-flight task to find the exact replica we staged. A
    // concurrent Put on this key may have created other PROCESSING MEMORY
    // replicas, so we must not just "mark first PROCESSING memory
    // complete" — that would risk committing someone else's half-written
    // replica.
    auto task_it = tenant_state.promotion_tasks.find(object_id.user_key);
    if (task_it == tenant_state.promotion_tasks.end() ||
        task_it->second.alloc_id == 0) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    // Holder-only gate (see PromotionTask::holder_id doc).
    if (task_it->second.holder_id != client_id) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    bool committed = false;
    Replica* staged = metadata.GetReplicaByID(task_it->second.alloc_id);
    if (staged != nullptr && staged->is_memory_replica() &&
        staged->is_processing()) {
        std::optional<OrderedOpLogWriter::Reservation> batch_reservation;
        if (enable_ha_ && enable_oplog_) {
            auto reservation = ReserveBatchOpLogSlot();
            if (!reservation) {
                return tl::make_unexpected(reservation.error());
            }
            batch_reservation = std::move(reservation.value());
        }
        staged->mark_complete();
        committed = true;
        if (enable_oplog_ && ordered_oplog_writer_) {
            std::vector<Replica::Descriptor> post;
            metadata.VisitReplicas(&Replica::fn_is_completed,
                                   [&post](const Replica& replica) {
                                       post.push_back(replica.get_descriptor());
                                   });

            const auto payload =
                SerializeMetadataForOpLogFromReplicaDescriptors(
                    metadata.client_id, metadata.size, post, metadata.group_id,
                    metadata.data_type);
            if (batch_reservation) {
                auto persist_result = AppendReservedOpLogWithDurableFinalize(
                    std::move(*batch_reservation), OpType::PUT_END,
                    tenant_id.value(), key, payload, nullptr);
                if (!persist_result) {
                    LOG(WARNING)
                        << "NotifyPromotionSuccess: PUT_END persist failed "
                        << "for key=" << key
                        << ", err=" << static_cast<int>(persist_result.error());
                }
            } else {
                auto persist_result = AppendOpLogVisibleBeforeDurable(
                    OpType::PUT_END, tenant_id.value(), key, payload);
                if (!persist_result) {
                    LOG(WARNING)
                        << "NotifyPromotionSuccess: PUT_END persist failed "
                        << "for key=" << key
                        << ", err=" << static_cast<int>(persist_result.error());
                }
            }
        }
    }

    // Drop the source LOCAL_DISK replica's refcnt and erase the task.
    auto* source = metadata.GetReplicaByID(task_it->second.source_id);
    if (source != nullptr) {
        source->dec_refcnt();
    }
    const uint64_t completed_bytes = task_it->second.object_size;
    const uint64_t reserved_quota_charge =
        task_it->second.reserved_quota_charge_bytes;
    if (committed) {
        const uint64_t actual_charge = CompletedMemoryQuotaCharge(metadata);
        const uint64_t commit_charge =
            actual_charge > metadata.committed_quota_charge_bytes
                ? actual_charge - metadata.committed_quota_charge_bytes
                : 0;
        const uint64_t abort_charge =
            reserved_quota_charge > commit_charge
                ? reserved_quota_charge - commit_charge
                : 0;
        CommitTenantQuota(object_id.tenant_id, commit_charge);
        AbortTenantQuota(object_id.tenant_id, abort_charge);
        metadata.committed_quota_charge_bytes = actual_charge;
    } else {
        AbortTenantQuota(object_id.tenant_id, reserved_quota_charge);
    }
    task_it->second.reserved_quota_charge_bytes = 0;
    tenant_state.promotion_tasks.erase(task_it);
    promotion_in_flight_.fetch_sub(1, std::memory_order_relaxed);
    MasterMetricManager::instance().dec_promotion_in_flight();
    if (committed) {
        SyncCacheTotalAccounting(metadata);
        MasterMetricManager::instance().inc_promotion_completed();
        MasterMetricManager::instance().inc_promotion_completed_bytes(
            static_cast<int64_t>(completed_bytes));
    } else {
        MasterMetricManager::instance().inc_promotion_cancelled();
    }

    // Erase the per-client promotion_objects entry (best-effort; the
    // heartbeat may have already drained it).
    {
        ScopedLocalDiskSegmentAccess local_disk_segment_access =
            segment_manager_.getLocalDiskSegmentAccess();
        auto& client_local_disk_segment =
            local_disk_segment_access.getClientLocalDiskSegment();
        auto it = client_local_disk_segment.find(client_id);
        if (it != client_local_disk_segment.end()) {
            MutexLocker locker(&it->second->offloading_mutex_);
            it->second->promotion_objects.erase(
                object_id.tenant_id.MakeScopedKey(object_id.user_key));
        }
    }

    if (!committed) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }
    return {};
}

auto MasterService::NotifyPromotionFailure(const UUID& client_id,
                                           const std::string& key,
                                           const TenantId& tenant_id)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto& metadata = accessor.Get();
    auto& tenant_state = accessor.GetTenantState();

    auto task_it = tenant_state.promotion_tasks.find(object_id.user_key);
    if (task_it == tenant_state.promotion_tasks.end()) {
        // No task to release. Either the reaper already swept it, or the
        // client never had a task here. Return OK to keep this RPC
        // idempotent — repeated failure notifications on the same key
        // should be safe.
        return {};
    }

    // Holder-only gate (see PromotionTask::holder_id doc).
    if (task_it->second.holder_id != client_id) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Mirror the reaper's expiry path; see DiscardExpiredProcessingReplicas
    // Part 4 for the full rationale on each step.
    auto* source = metadata.GetReplicaByID(task_it->second.source_id);
    if (source != nullptr) {
        source->dec_refcnt();
    }
    if (task_it->second.alloc_id != 0) {
        const ReplicaID alloc_id = task_it->second.alloc_id;
        EraseReplicasWithCacheTotalAccounting(
            metadata, [alloc_id](const Replica& replica) {
                return replica.id() == alloc_id;
            });
    }
    AbortTenantQuota(object_id.tenant_id,
                     task_it->second.reserved_quota_charge_bytes);
    task_it->second.reserved_quota_charge_bytes = 0;
    tenant_state.promotion_tasks.erase(task_it);
    promotion_in_flight_.fetch_sub(1, std::memory_order_relaxed);
    MasterMetricManager::instance().dec_promotion_in_flight();
    MasterMetricManager::instance().inc_promotion_failed();

    // Clear the holder's per-client promotion_objects entry. Same
    // best-effort cleanup pattern as NotifyPromotionSuccess — the
    // heartbeat may have already drained it.
    {
        ScopedLocalDiskSegmentAccess local_disk_segment_access =
            segment_manager_.getLocalDiskSegmentAccess();
        auto& client_local_disk_segment =
            local_disk_segment_access.getClientLocalDiskSegment();
        auto it = client_local_disk_segment.find(client_id);
        if (it != client_local_disk_segment.end()) {
            MutexLocker locker(&it->second->offloading_mutex_);
            it->second->promotion_objects.erase(
                object_id.tenant_id.MakeScopedKey(object_id.user_key));
        }
    }

    return {};
}

void MasterService::EvictionThreadFunc() {
    VLOG(1) << "action=eviction_thread_started";

    auto last_discard_time = std::chrono::system_clock::now();
    while (eviction_running_) {
        const auto now = std::chrono::system_clock::now();
        double used_ratio =
            MasterMetricManager::instance().get_global_mem_used_ratio();
        if (used_ratio > eviction_high_watermark_ratio_ ||
            (need_mem_eviction_ && eviction_ratio_ > 0.0)) {
            LOG(INFO) << "[EVICT-TRIGGER] memory_ratio=" << used_ratio
                      << " high_watermark=" << eviction_high_watermark_ratio_
                      << " need_mem_eviction=" << need_mem_eviction_
                      << " eviction_ratio=" << eviction_ratio_;
            double evict_ratio_target = std::max(
                eviction_ratio_,
                used_ratio - eviction_high_watermark_ratio_ + eviction_ratio_);
            double evict_ratio_lowerbound =
                std::max(evict_ratio_target * 0.5,
                         used_ratio - eviction_high_watermark_ratio_);
            BatchEvict(evict_ratio_target, evict_ratio_lowerbound);
            LOG(INFO) << "[EVICT-DONE] BatchEvict execution completed.";
            last_discard_time = now;
        } else if (now - last_discard_time > put_start_release_timeout_sec_) {
            // Try discarding expired processing keys and ongoing replication
            // tasks if we have not done this for a long time.
            {
                SpDiag::PerfPoint pt_discard(PerfKey::MASTER_BG_DISCARD_EXPIRED,
                                             SpDiag::PerfLevel::MODULE);
                pt_discard.Start();
                std::shared_lock<std::shared_mutex> shared_lock(
                    snapshot_mutex_);
                for (size_t i = 0; i < kNumShards; i++) {
                    MetadataShardAccessorRW shard(this, i);
                    DiscardExpiredProcessingReplicas(shard, now);
                }
                ReleaseExpiredDiscardedReplicas(now);
                pt_discard.End(0);
            }
            last_discard_time = now;
        }

#ifdef USE_NOF
        double nof_used_ratio =
            MasterMetricManager::instance().get_global_nof_used_ratio();
        if (nof_used_ratio > nof_eviction_high_watermark_ratio_ ||
            (need_nof_eviction_ && nof_eviction_ratio_ > 0.0)) {
            double nof_evict_ratio_target =
                std::max(nof_eviction_ratio_,
                         nof_used_ratio - nof_eviction_high_watermark_ratio_ +
                             nof_eviction_ratio_);
            double nof_evict_ratio_lowerbound =
                std::max(nof_evict_ratio_target * 0.5,
                         nof_used_ratio - nof_eviction_high_watermark_ratio_);
            NoFBatchEvict(nof_evict_ratio_target, nof_evict_ratio_lowerbound);
        }
#endif

        if (promotion_candidate_count_.load(std::memory_order_relaxed) > 0) {
            RunPromotionCandidateRetry();
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(kEvictionThreadSleepMs));
    }

    VLOG(1) << "action=eviction_thread_stopped";
}

void MasterService::DiscardExpiredProcessingReplicas(
    MetadataShardAccessorRW& shard,
    const std::chrono::system_clock::time_point& now) {
    std::list<DiscardedReplicas> discarded_replicas;

    for (auto tenant_it = shard->tenants.begin();
         tenant_it != shard->tenants.end();) {
        auto& tenant_state = tenant_it->second;

        for (auto key_it = tenant_state.processing_keys.begin();
             key_it != tenant_state.processing_keys.end();) {
            auto it = tenant_state.metadata.find(*key_it);
            if (it == tenant_state.metadata.end()) {
                LOG(ERROR) << "Key " << *key_it
                           << " was removed while in processing";
                key_it = tenant_state.processing_keys.erase(key_it);
                continue;
            }

            auto& metadata = it->second;
            if (!metadata.IsValid() ||
                metadata.AllReplicas(&Replica::fn_is_completed)) {
                if (!metadata.IsValid()) {
                    auto next_key_it = std::next(key_it);
                    EraseMetadata(tenant_state, it, tenant_it->first,
                                  QuotaEraseMode::kFull, &shard);
                    key_it = next_key_it;
                } else {
                    key_it = tenant_state.processing_keys.erase(key_it);
                }
                continue;
            }

            const auto ttl =
                metadata.put_start_time + put_start_release_timeout_sec_;
            if (ttl < now) {
                const bool had_complete_replica =
                    metadata.HasReplica(&Replica::fn_is_completed);
                // Predict post-discard descriptors WITHOUT mutating: drop
                // PROCESSING replicas; keep COMPLETE replicas.
                auto post_descriptors = BuildRemainingReplicaDescriptors(
                    metadata, &Replica::fn_is_processing);
                const bool would_invalidate = post_descriptors.empty();

                if (had_complete_replica && enable_oplog_ &&
                    ordered_oplog_writer_) {
                    tl::expected<OpLogEntry, ErrorCode> persist_result;
                    if (would_invalidate) {
                        persist_result = AppendOpLogWithDurableFinalize(
                            OpType::REMOVE, tenant_it->first.value(), *key_it,
                            {},
                            enable_oplog_
                                ? [this, ttl](const OpLogEntry& durable_entry) {
                                      FinalizeExpiredProcessingReplicasAfterDurable(
                                          durable_entry, ttl);
                                  }
                                : DurableFinalizeCallback{});
                    } else {
                        persist_result = AppendOpLogWithDurableFinalize(
                            OpType::PUT_END, tenant_it->first.value(), *key_it,
                            SerializeMetadataForOpLogFromReplicaDescriptors(
                                metadata.client_id, metadata.size,
                                post_descriptors, metadata.group_id,
                                metadata.data_type),
                            enable_oplog_
                                ? [this, ttl](const OpLogEntry& durable_entry) {
                                      FinalizeExpiredProcessingReplicasAfterDurable(
                                          durable_entry, ttl);
                                  }
                                : DurableFinalizeCallback{});
                    }
                    if (!persist_result) {
                        LOG(WARNING) << "DiscardExpiredProcessingReplicas: "
                                        "OpLog persist failed for key="
                                     << *key_it << ", err="
                                     << static_cast<int>(persist_result.error())
                                     << ", deferring discard";
                        ++key_it;
                        continue;
                    }
                    if (enable_oplog_) {
                        ++key_it;
                        continue;
                    }
                }

                // Persist OK (or HA disabled / never published) — apply.
                auto replicas =
                    metadata.PopReplicas(&Replica::fn_is_processing);
                if (!replicas.empty()) {
                    discarded_replicas.emplace_back(std::move(replicas), ttl);
                }
                if (!metadata.IsValid()) {
                    auto next_key_it = std::next(key_it);
                    EraseMetadata(tenant_state, it, tenant_it->first,
                                  QuotaEraseMode::kFull, &shard);
                    key_it = next_key_it;
                } else {
                    key_it = tenant_state.processing_keys.erase(key_it);
                }
                continue;
            }
            key_it++;
        }

        for (auto task_it = tenant_state.replication_tasks.begin();
             task_it != tenant_state.replication_tasks.end();) {
            auto metadata_it = tenant_state.metadata.find(task_it->first);
            if (metadata_it == tenant_state.metadata.end()) {
                LOG(ERROR) << "Key " << task_it->first
                           << " was removed with ongoing replication task";
                AbortTenantQuota(tenant_it->first,
                                 task_it->second.reserved_quota_charge_bytes);
                task_it = tenant_state.replication_tasks.erase(task_it);
                continue;
            }

            const auto ttl =
                task_it->second.start_time + put_start_release_timeout_sec_;
            if (ttl > now) {
                task_it++;
                continue;
            }

            auto& metadata = metadata_it->second;

            const bool had_complete_replica =
                metadata.HasReplica(&Replica::fn_is_completed);
            auto& replica_ids = task_it->second.replica_ids;

            const auto target_pred = [&replica_ids](const Replica& r) {
                return std::find(replica_ids.begin(), replica_ids.end(),
                                 r.id()) != replica_ids.end();
            };
            // Predict post-discard descriptor list WITHOUT mutating: drop
            // task target replicas; keep the rest of the COMPLETE replicas.
            auto post_descriptors =
                BuildRemainingReplicaDescriptors(metadata, target_pred);
            const bool would_invalidate = post_descriptors.empty();

            if (had_complete_replica && enable_oplog_ &&
                ordered_oplog_writer_) {
                tl::expected<OpLogEntry, ErrorCode> persist_result;
                auto source_id = task_it->second.source_id;
                auto target_ids = replica_ids;
                if (would_invalidate) {
                    persist_result = AppendOpLogWithDurableFinalize(
                        OpType::REMOVE, tenant_it->first.value(),
                        task_it->first, {},
                        enable_oplog_
                            ? [this, source_id,
                               target_ids = std::move(target_ids),
                               ttl](const OpLogEntry& durable_entry) {
                                  FinalizeExpiredReplicationTaskAfterDurable(
                                      durable_entry, source_id, target_ids,
                                      ttl);
                              }
                            : DurableFinalizeCallback{});
                } else {
                    persist_result = AppendOpLogWithDurableFinalize(
                        OpType::PUT_END, tenant_it->first.value(),
                        task_it->first,
                        SerializeMetadataForOpLogFromReplicaDescriptors(
                            metadata.client_id, metadata.size, post_descriptors,
                            metadata.group_id, metadata.data_type),
                        enable_oplog_
                            ? [this, source_id,
                               target_ids = std::move(target_ids),
                               ttl](const OpLogEntry& durable_entry) {
                                  FinalizeExpiredReplicationTaskAfterDurable(
                                      durable_entry, source_id, target_ids,
                                      ttl);
                              }
                            : DurableFinalizeCallback{});
                }
                if (!persist_result) {
                    LOG(WARNING)
                        << "DiscardExpiredProcessingReplicas: OpLog persist "
                           "failed for replication task key="
                        << task_it->first
                        << ", err=" << static_cast<int>(persist_result.error())
                        << ", deferring discard";
                    ++task_it;
                    continue;
                }
                if (enable_oplog_) {
                    ++task_it;
                    continue;
                }
            }

            auto source = metadata.GetReplicaByID(task_it->second.source_id);
            if (source != nullptr) {
                source->dec_refcnt();
            }

            auto replicas =
                PopReplicasWithCacheTotalAccounting(metadata, target_pred);
            if (!replicas.empty()) {
                discarded_replicas.emplace_back(std::move(replicas), ttl);
            }
            if (!metadata.IsValid()) {
                auto next_task_it = std::next(task_it);
                EraseMetadata(tenant_state, metadata_it, tenant_it->first,
                              QuotaEraseMode::kFull, &shard);
                task_it = next_task_it;
            } else {
                task_it = tenant_state.replication_tasks.erase(task_it);
            }
        }

        for (auto task_it = tenant_state.offloading_tasks.begin();
             task_it != tenant_state.offloading_tasks.end();) {
            auto& tasks = task_it->second;
            auto metadata_it = tenant_state.metadata.find(task_it->first);
            for (auto t = tasks.begin(); t != tasks.end();) {
                const auto ttl =
                    t->start_time + put_start_release_timeout_sec_;
                if (ttl > now) {
                    t++;
                    continue;
                }
                if (metadata_it != tenant_state.metadata.end()) {
                    auto source = metadata_it->second.GetReplicaByID(
                        t->source_id);
                    if (source != nullptr) {
                        source->dec_refcnt();
                    }
                }
                LOG(WARNING) << "Offloading task expired for key: "
                             << task_it->first << " tenant=" << tenant_it->first;
                t = tasks.erase(t);
            }
            if (tasks.empty()) {
                task_it = tenant_state.offloading_tasks.erase(task_it);
            } else {
                task_it++;
            }
        }

        for (auto task_it = tenant_state.promotion_tasks.begin();
             task_it != tenant_state.promotion_tasks.end();) {
            const auto ttl =
                task_it->second.start_time + put_start_release_timeout_sec_;
            if (ttl > now) {
                task_it++;
                continue;
            }
            auto metadata_it = tenant_state.metadata.find(task_it->first);
            if (metadata_it != tenant_state.metadata.end()) {
                auto source = metadata_it->second.GetReplicaByID(
                    task_it->second.source_id);
                if (source != nullptr) {
                    source->dec_refcnt();
                }
                if (task_it->second.alloc_id != 0) {
                    const ReplicaID alloc_id = task_it->second.alloc_id;
                    EraseReplicasWithCacheTotalAccounting(
                        metadata_it->second,
                        [alloc_id](const Replica& replica) {
                            return replica.id() == alloc_id;
                        });
                }
            }
            AbortTenantQuota(tenant_it->first,
                             task_it->second.reserved_quota_charge_bytes);
            task_it->second.reserved_quota_charge_bytes = 0;
            LOG(WARNING) << "Promotion task expired for key: "
                         << task_it->first;
            task_it = tenant_state.promotion_tasks.erase(task_it);
            promotion_in_flight_.fetch_sub(1, std::memory_order_relaxed);
            MasterMetricManager::instance().dec_promotion_in_flight();
            MasterMetricManager::instance().inc_promotion_expired();
        }

        if (tenant_state.Empty()) {
            tenant_it = shard->tenants.erase(tenant_it);
        } else {
            ++tenant_it;
        }
    }

    if (!discarded_replicas.empty()) {
        std::lock_guard lock(discarded_replicas_mutex_);
        discarded_replicas_.splice(discarded_replicas_.end(),
                                   std::move(discarded_replicas));
    }
}

uint64_t MasterService::ReleaseExpiredDiscardedReplicas(
    const std::chrono::system_clock::time_point& now) {
    uint64_t released_cnt = 0;
    std::lock_guard lock(discarded_replicas_mutex_);
    discarded_replicas_.remove_if(
        [&now, &released_cnt](const DiscardedReplicas& item) {
            const bool expired = item.isExpired(now);
            if (expired && item.memSize() > 0) {
                released_cnt++;
            }
            return expired;
        });
    return released_cnt;
}

/**
 * @brief Restore master state from snapshot using three-phase architecture.
 *
 * Phase 1 (Repository): Load candidate snapshots from catalog
 * Phase 2 (Repository + Codec): Download payloads and decode to memory
 * Phase 3 (Service): Apply decoded state and rebuild metrics
 *
 * Attempts restore from candidates in chronological order until one succeeds.
 * If all candidates fail, starts with a fresh state.
 */
void MasterService::RestoreState() {
    auto* snapshot_catalog_store = snapshot_catalog_store_.get();
    if (!snapshot_catalog_store) {
        LOG(ERROR) << "[Restore] Snapshot catalog store is not initialized, "
                      "starting fresh";
        return;
    }

    LOG(INFO) << "[Restore] Backend info: "
              << snapshot_object_store_->GetConnectionInfo();

    // Phase 1: Find snapshot candidates (repository responsibility)
    auto latest_result = snapshot_repository_->LoadLatestSnapshot();
    std::optional<ha::SnapshotDescriptor> latest_snapshot;
    if (!latest_result) {
        LOG(WARNING) << "[Restore] Failed to load latest snapshot marker: "
                     << toString(latest_result.error())
                     << ", falling back to published snapshot listing";
    } else {
        latest_snapshot = latest_result.value();
    }

    auto candidates_result =
        snapshot_repository_->LoadRestoreCandidates(latest_snapshot);
    if (!candidates_result || candidates_result->empty()) {
        LOG(ERROR) << "[Restore] No previous snapshot found, starting fresh";
        return;
    }

    // Phase 2 & 3: Try each candidate
    const auto now = std::chrono::system_clock::now();
    for (const auto& snapshot : candidates_result.value()) {
        ResetStateAfterFailedRestoreAttempt();

        try {
            // Phase 2a: Download payloads (repository responsibility)
            auto payloads_result =
                snapshot_repository_->DownloadSnapshotPayloads(snapshot);
            if (!payloads_result) {
                LOG(WARNING)
                    << "[Restore] Snapshot candidate " << snapshot.snapshot_id
                    << " is unusable: failed to download payloads: "
                    << payloads_result.error().message;
                continue;
            }

            // Phase 2b: Decode payloads (codec responsibility)
            auto decode_result =
                snapshot_codec_->Decode(this, payloads_result.value());
            if (!decode_result) {
                LOG(WARNING)
                    << "[Restore] Snapshot candidate " << snapshot.snapshot_id
                    << " is unusable: " << decode_result.error().message;
                continue;
            }

            // Phase 3: Apply state (master service responsibility)
            auto apply_result = ApplySnapshotState(now);
            if (!apply_result) {
                LOG(WARNING)
                    << "[Restore] Snapshot candidate " << snapshot.snapshot_id
                    << " is unusable: failed to apply state: "
                    << apply_result.error().message;
                continue;
            }

            LOG(INFO) << "[Restore] Successfully restored state from snapshot: "
                      << snapshot.snapshot_id;
            return;
        } catch (const std::exception& e) {
            LOG(WARNING) << "[Restore] Snapshot candidate "
                         << snapshot.snapshot_id
                         << " is unusable: exception during restore: "
                         << e.what();
            // State reset already happened at loop start; continue to next
            continue;
        } catch (...) {
            LOG(WARNING) << "[Restore] Snapshot candidate "
                         << snapshot.snapshot_id
                         << " is unusable: unknown exception during restore";
            continue;
        }
    }

    ResetStateAfterFailedRestoreAttempt();
    LOG(ERROR) << "[Restore] Failed to restore from all candidate snapshots "
               << "(count=" << candidates_result->size() << "), starting fresh";
}

void MasterService::ResetStateAfterFailedRestoreAttempt() {
    SegmentSerializer segment_serializer(&segment_manager_);
    MetadataSerializer metadata_serializer(this);
    TaskManagerSerializer task_manager_serializer(&task_manager_);

    task_manager_serializer.Reset();
    metadata_serializer.Reset();
    segment_serializer.Reset();

    {
        std::unique_lock<std::shared_mutex> lock(client_mutex_);
        ok_client_.clear();
    }
    PodUUID pod_uuid;
    while (client_ping_queue_.pop(pod_uuid)) {
    }

    MasterMetricManager::instance().reset_allocated_mem_size();
    MasterMetricManager::instance().reset_total_mem_capacity();
    MasterMetricManager::instance().reset_cache_total_nums();
}

tl::expected<void, SerializationError> MasterService::ApplySnapshotState(
    const std::chrono::system_clock::time_point& now) {
    // Note: Codec has already called Deserialize() on all payloads,
    // so the internal state is already restored. This method handles
    // post-restore cleanup and metrics rebuilding.

    std::vector<std::string> segment_names;
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        segment_access.GetAllSegmentNames(segment_names);
    }

    // Cleanup expired metadata (unless test environment disables it)
    {
        const bool skip_cleanup =
            std::getenv("MOONCAKE_MASTER_SERVICE_SNAPSHOT_TEST_SKIP_CLEANUP");
        if (!skip_cleanup) {
            auto cleanup_now = now;
            for (auto& shard : metadata_shards_) {
                for (auto tenant_it = shard.tenants.begin();
                     tenant_it != shard.tenants.end();) {
                    auto& tenant_state = tenant_it->second;
                    for (auto it = tenant_state.metadata.begin();
                         it != tenant_state.metadata.end();) {
                        if (it->second.HasDiffRepStatus(
                                ReplicaStatus::COMPLETE) ||
                            (it->second.IsLeaseExpired(cleanup_now) &&
                             !it->second.IsSoftPinned(cleanup_now))) {
                            VLOG(1) << "clear metadata key=" << it->first;
                            it = EraseMetadata(tenant_state, it,
                                               tenant_it->first);
                        } else {
                            ++it;
                        }
                    }
                    if (tenant_state.Empty()) {
                        tenant_it = shard.tenants.erase(tenant_it);
                    } else {
                        ++tenant_it;
                    }
                }
            }
        }

        // Rebuild allocated memory metrics
        MasterMetricManager::instance().reset_allocated_mem_size();
        RebuildCacheTotalAccounting();
        for (auto& segment_name : segment_names) {
            MasterMetricManager::instance().reset_segment_allocated_mem_size(
                segment_name);
        }

        for (auto& shard : metadata_shards_) {
            for (auto& [tenant_id, tenant_state] : shard.tenants) {
                for (auto it = tenant_state.metadata.begin();
                     it != tenant_state.metadata.end();) {
                    for (auto& replica : it->second.GetAllReplicas()) {
                        if (!replica.get_descriptor().is_memory_replica()) {
                            continue;
                        }
                        auto temp_segment_names = replica.get_segment_names();
                        if (temp_segment_names.empty()) {
                            continue;
                        }
                        if (!temp_segment_names[0].has_value()) {
                            continue;
                        }
                        auto buffer_descriptor = replica.get_descriptor()
                                                     .get_memory_descriptor()
                                                     .buffer_descriptor;
                        MasterMetricManager::instance().inc_allocated_mem_size(
                            temp_segment_names[0].value(),
                            static_cast<int64_t>(buffer_descriptor.size_));
                    }
                    ++it;
                }
            }
        }

        LOG(INFO) << "[Restore] Total allocated size after restore: "
                  << MasterMetricManager::instance().get_allocated_mem_size();
    }

    // Rebuild total capacity metrics
    {
        MasterMetricManager::instance().reset_total_mem_capacity();
        for (auto& segment_name : segment_names) {
            MasterMetricManager::instance().reset_segment_total_mem_capacity(
                segment_name);
        }

        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        std::vector<std::pair<Segment, UUID>> unready_segments;
        if (segment_access.GetUnreadySegments(unready_segments) ==
            ErrorCode::OK) {
            for (const auto& [segment, client_id] : unready_segments) {
                UnmountSegment(segment.id, client_id);
            }
        }

        std::vector<std::pair<Segment, UUID>> all_segments;
        auto err = segment_access.GetAllSegments(all_segments);

        if (err == ErrorCode::OK) {
            int64_t total_size = 0;
            for (const auto& [segment, client_id] : all_segments) {
                Ping(client_id);
                total_size += static_cast<int64_t>(segment.size);
                MasterMetricManager::instance().inc_total_mem_capacity(
                    segment.name, segment.size);
            }
            LOG(INFO) << "[Restore] Total capacity size after restore: "
                      << total_size;
        } else {
            LOG(ERROR) << "[Restore] Failed to get all segments, error: "
                       << err;
        }
    }

    return {};
}

MasterService::TenantQuotaEvictionResult
MasterService::EvictTenantMemoryForQuota(const TenantId& tenant_id,
                                         uint64_t target_bytes) {
    TenantQuotaEvictionResult total;
    if (!enable_multi_tenants_ || target_bytes == 0) {
        return total;
    }

    const TenantId normalized_tenant(tenant_id);
    auto now = std::chrono::system_clock::now();
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);

    auto is_evictable_memory_replica = [this](const Replica& replica) {
        return IsMemoryReplicaEvictable(replica);
    };
    auto can_evict_replicas = [&](const ObjectMetadata& metadata) {
        return metadata.HasReplica(is_evictable_memory_replica);
    };
    auto has_local_disk_replica = [](const ObjectMetadata& metadata) {
        return metadata.HasReplica(&Replica::fn_is_local_disk_replica);
    };
    auto evict_replicas =
        [&, this](ObjectMetadata& metadata,
                  std::vector<std::vector<Replica>>& deferred_replicas) {
            const uint64_t before_charge = CompletedMemoryQuotaCharge(metadata);
            auto replicas = PopReplicasWithCacheTotalAccounting(
                metadata, is_evictable_memory_replica);
            const uint64_t replica_count = replicas.size();
            if (!replicas.empty()) {
                deferred_replicas.emplace_back(std::move(replicas));
            }
            const uint64_t after_charge = CompletedMemoryQuotaCharge(metadata);
            if (before_charge > after_charge) {
                ReleaseCommittedQuotaCharge(metadata,
                                            before_charge - after_charge);
            }
            return metadata.size * replica_count;
        };
    long offload_queued_this_call = 0;
    long offload_deferred_count = 0;
    long offload_cap_forced_count = 0;
    long offload_push_failed_forced = 0;
    const long offload_cap =
        offload_on_evict_
            ? static_cast<long>(offloading_queue_limit_ * offload_cap_ratio_)
            : 0;

    auto try_evict_or_offload =
        [&, this](const std::string& key, ObjectMetadata& metadata,
                  TenantState& tenant_state,
                  std::vector<std::vector<Replica>>& deferred_replicas) {
            if (!offload_on_evict_) {
                return evict_replicas(metadata, deferred_replicas);
            }

            if (has_local_disk_replica(metadata)) {
                return evict_replicas(metadata, deferred_replicas);
            }

            if (offload_force_evict_ &&
                offload_queued_this_call >= offload_cap) {
                ++offload_cap_forced_count;
                return evict_replicas(metadata, deferred_replicas);
            }

            bool queued = false;
            metadata.VisitReplicas(
                is_evictable_memory_replica,
                [this, &key, &normalized_tenant, &tenant_state, &queued,
                 &now](Replica& replica) {
                    if (queued) {
                        return;
                    }
                    auto result = PushOffloadingQueue(
                        MakeObjectIdentity(key, normalized_tenant), replica);
                    if (result && !result.value().empty()) {
                        auto& tasks = tenant_state.offloading_tasks[key];
                        for (const auto& client_id : result.value()) {
                            replica.inc_refcnt();
                            tasks.push_back(
                                OffloadingTask{replica.id(), now, client_id});
                        }
                        queued = true;
                    }
                });

            if (queued) {
                ++offload_queued_this_call;
                ++offload_deferred_count;
                return evict_replicas(metadata, deferred_replicas);
            }

            if (offload_force_evict_) {
                ++offload_push_failed_forced;
                return evict_replicas(metadata, deferred_replicas);
            }
            return uint64_t{0};
        };

    auto try_evict_group_or_object =
        [&, this](const std::string& key, ObjectMetadata& metadata,
                  TenantState& tenant_state,
                  std::vector<std::vector<Replica>>& deferred_replicas,
                  bool allow_soft_pinned) -> TenantQuotaEvictionResult {
        if (!metadata.IsGrouped()) {
            uint64_t freed = try_evict_or_offload(key, metadata, tenant_state,
                                                  deferred_replicas);
            return {.freed_bytes = freed,
                    .evicted_objects = freed > 0 ? 1U : 0U};
        }

        auto group_it = tenant_state.group_members.find(metadata.group_id);
        if (group_it == tenant_state.group_members.end()) {
            uint64_t freed = try_evict_or_offload(key, metadata, tenant_state,
                                                  deferred_replicas);
            return {.freed_bytes = freed,
                    .evicted_objects = freed > 0 ? 1U : 0U};
        }

        for (const auto& member_key : group_it->second) {
            auto member_it = tenant_state.metadata.find(member_key);
            if (member_it != tenant_state.metadata.end() &&
                !member_it->second.IsLeaseExpired(now)) {
                return {};
            }
        }

        TenantQuotaEvictionResult result;
        std::vector<std::string> member_keys(group_it->second.begin(),
                                             group_it->second.end());
        for (const auto& member_key : member_keys) {
            auto member_it = tenant_state.metadata.find(member_key);
            if (member_it == tenant_state.metadata.end()) {
                continue;
            }
            auto& member_metadata = member_it->second;
            if (member_metadata.IsHardPinned() ||
                !member_metadata.IsLeaseExpired(now) ||
                (!allow_soft_pinned && member_metadata.IsSoftPinned(now)) ||
                !can_evict_replicas(member_metadata)) {
                continue;
            }

            const uint64_t freed = try_evict_or_offload(
                member_key, member_metadata, tenant_state, deferred_replicas);
            result.freed_bytes += freed;
            if (freed > 0) {
                ++result.evicted_objects;
            }
            if (member_key != key && !member_metadata.IsValid()) {
                EraseMetadata(tenant_state, member_it, normalized_tenant);
            }
        }
        return result;
    };

    auto pass = [&](bool allow_soft_pinned) {
        const size_t start_shard = randomIndex(kNumShards);
        for (size_t scanned = 0;
             scanned < kNumShards && total.freed_bytes < target_bytes;
             ++scanned) {
            const size_t shard_idx = (start_shard + scanned) % kNumShards;
            std::vector<std::vector<Replica>> deferred_replicas;
            {
                MetadataShardAccessorRW shard(this, shard_idx);
                auto tenant_it = shard->tenants.find(normalized_tenant);
                if (tenant_it == shard->tenants.end()) {
                    continue;
                }
                auto& tenant_state = tenant_it->second;
                for (auto it = tenant_state.metadata.begin();
                     it != tenant_state.metadata.end() &&
                     total.freed_bytes < target_bytes;) {
                    auto& metadata = it->second;
                    if (metadata.IsHardPinned() ||
                        !metadata.IsLeaseExpired(now) ||
                        (!allow_soft_pinned && metadata.IsSoftPinned(now)) ||
                        !can_evict_replicas(metadata)) {
                        ++it;
                        continue;
                    }

                    auto evict_result = try_evict_group_or_object(
                        it->first, metadata, tenant_state, deferred_replicas,
                        allow_soft_pinned);
                    total.freed_bytes += evict_result.freed_bytes;
                    total.evicted_objects += evict_result.evicted_objects;
                    if (!metadata.IsValid()) {
                        it = EraseMetadata(tenant_state, it, normalized_tenant);
                    } else {
                        ++it;
                    }
                }
                if (tenant_state.Empty()) {
                    shard->tenants.erase(tenant_it);
                }
            }
        }
    };

    pass(/*allow_soft_pinned=*/false);
    if (allow_evict_soft_pinned_objects_ && total.freed_bytes < target_bytes) {
        pass(/*allow_soft_pinned=*/true);
    }

    if (total.freed_bytes > 0) {
        MasterMetricManager::instance().inc_tenant_evict_bytes(
            normalized_tenant.value(),
            static_cast<int64_t>(std::min<uint64_t>(
                total.freed_bytes,
                static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))));
    }
    if (offload_on_evict_ && total.freed_bytes == 0 &&
        offload_deferred_count > 0) {
        LOG(WARNING) << "[TENANT-EVICT] No memory freed for tenant "
                     << normalized_tenant << "; " << offload_deferred_count
                     << " object(s) deferred for disk offload.";
    }
    if (offload_cap_forced_count > 0) {
        LOG(WARNING) << "[TENANT-EVICT] Offload cap (" << offload_cap
                     << ") reached for tenant " << normalized_tenant
                     << "; force-evicted " << offload_cap_forced_count
                     << " object(s) without disk offload.";
    }
    if (offload_push_failed_forced > 0) {
        LOG(WARNING) << "[TENANT-EVICT] PushOffloadingQueue failed for tenant "
                     << normalized_tenant << " on "
                     << offload_push_failed_forced
                     << " object(s); force-evicted without disk offload "
                        "(offload_force_evict=true).";
    }
    return total;
}

void MasterService::BatchEvict(double evict_ratio_target,
                               double evict_ratio_lowerbound) {
    SpDiag::PerfPoint pt_evict(PerfKey::MASTER_BG_BATCH_EVICT,
                               SpDiag::PerfLevel::KEY_MODULE);
    pt_evict.Start();

    if (evict_ratio_target < evict_ratio_lowerbound) {
        LOG(ERROR) << "evict_ratio_target=" << evict_ratio_target
                   << ", evict_ratio_lowerbound=" << evict_ratio_lowerbound
                   << ", error=invalid_params";
        evict_ratio_lowerbound = evict_ratio_target;
    }

    auto now = std::chrono::system_clock::now();

    auto is_evictable_memory_replica = [this](const Replica& replica) {
        return IsMemoryReplicaEvictable(replica);
    };

    auto can_evict_replicas = [&](const ObjectMetadata& metadata) {
        return metadata.HasReplica(is_evictable_memory_replica);
    };

    auto evict_replicas =
        [&, this](ObjectMetadata& metadata,
                  std::vector<std::vector<Replica>>& deferred_replicas) {
            if (enable_oplog_) {
                return metadata.size *
                       metadata.CountReplicas([](const Replica& replica) {
                           return replica.is_memory_replica() &&
                                  replica.status() == ReplicaStatus::REMOVED;
                       });
            }
            const uint64_t before_charge = CompletedMemoryQuotaCharge(metadata);
            auto replicas = PopReplicasWithCacheTotalAccounting(
                metadata, is_evictable_memory_replica);
            const size_t replica_count = replicas.size();
            if (!replicas.empty()) {
                deferred_replicas.emplace_back(std::move(replicas));
            }
            const uint64_t after_charge = CompletedMemoryQuotaCharge(metadata);
            if (before_charge > after_charge) {
                ReleaseCommittedQuotaCharge(metadata,
                                            before_charge - after_charge);
            }
            return metadata.size * replica_count;
        };

    // --- Offload-on-evict support ---
    long offload_queued_this_cycle = 0;
    long offload_deferred_count = 0;
    long offload_cap_forced_count = 0;    // #keys force-evicted due to cap
    long offload_push_failed_forced = 0;  // #keys force-evicted on push fail
    const long offload_cap =
        offload_on_evict_
            ? static_cast<long>(offloading_queue_limit_ * offload_cap_ratio_)
            : 0;

    auto has_local_disk_replica = [](const ObjectMetadata& metadata) {
        return metadata.HasReplica(&Replica::fn_is_local_disk_replica);
    };

    // Returns freed bytes. Returns 0 if offload-queued and no additional
    // replicas were evicted (all MEMORY replicas of the key are now pinned).
    auto try_evict_or_offload =
        [&, this](
            const TenantId& tenant_id, const std::string& key,
            ObjectMetadata& metadata, TenantState& tenant_state,
            std::vector<std::vector<Replica>>& deferred_replicas) -> uint64_t {
        if (enable_oplog_) {
            return evict_replicas(metadata, deferred_replicas);
        }
        if (!offload_on_evict_) {
            // Original behavior
            return evict_replicas(metadata, deferred_replicas);
        }

        // LOCAL_DISK replica already exists — safe to delete MEMORY immediately
        if (has_local_disk_replica(metadata)) {
            return evict_replicas(metadata, deferred_replicas);
        }

        // Force-evict cap: if force_evict enabled and cap reached, force
        // delete. Warning is aggregated at the end of the cycle to avoid log
        // flooding.
        if (offload_force_evict_ && offload_queued_this_cycle >= offload_cap) {
            offload_cap_forced_count++;
            return evict_replicas(metadata, deferred_replicas);
        }

        // Queue one MEMORY replica for offload; others will be evicted below.
        bool queued = false;
        metadata.VisitReplicas(
            is_evictable_memory_replica, [this, &tenant_id, &key, &tenant_state,
                                          &queued, &now](Replica& replica) {
                if (queued) return;  // only need to pin one replica for offload
                auto result = PushOffloadingQueue(
                    MakeObjectIdentity(key, tenant_id), replica);
                if (result && !result.value().empty()) {
                    auto& tasks = tenant_state.offloading_tasks[key];
                    for (const auto& client_id : result.value()) {
                        replica.inc_refcnt();
                        tasks.push_back(
                            OffloadingTask{replica.id(), now, client_id});
                    }
                    queued = true;
                }
            });

        if (queued) {
            offload_queued_this_cycle++;
            offload_deferred_count++;
            // Any remaining MEMORY replicas with refcnt==0 are redundant copies
            // (data survives via the pinned replica → disk). Evict them now to
            // reclaim memory immediately rather than waiting another cycle.
            return evict_replicas(metadata, deferred_replicas);
        }

        // PushOffloadingQueue failed. Default (data-preserving) behavior is to
        // skip this cycle — the outer eviction loop will retry after the
        // offload queue drains. Only force-evict when explicitly opted in, to
        // prevent silent data loss when the queue is unavailable.
        if (offload_force_evict_) {
            offload_push_failed_forced++;
            return evict_replicas(metadata, deferred_replicas);
        }
        return 0;
    };

    // kSubmitted means accepted by the ordered writer, not yet durable.
    enum class EvictOpLogSubmissionResult {
        kNotRequired,
        kSubmitted,
        kReservationFailed,
        kSubmissionFailed,
    };

    // HA strong-consistency: submit the post-eviction state before the caller
    // proceeds with eviction.
    auto submit_evict_oplog_if_needed =
        [&, this](const TenantId& tenant_id, const std::string& key,
                  ObjectMetadata& metadata) -> EvictOpLogSubmissionResult {
        if (!enable_oplog_ || !ordered_oplog_writer_) {
            return EvictOpLogSubmissionResult::kNotRequired;
        }

        // Predict the descriptor list after evict_replicas() runs.
        auto remaining = BuildRemainingReplicaDescriptors(
            metadata, is_evictable_memory_replica);

        if (enable_oplog_) {
            auto reservation = ReserveBatchOpLogSlot();
            if (!reservation) {
                LOG(WARNING)
                    << "BatchEvict: OpLog reservation failed for key=" << key
                    << ", err=" << static_cast<int>(reservation.error())
                    << ", stopping eviction cycle";
                return EvictOpLogSubmissionResult::kReservationFailed;
            }
            std::vector<ReplicaID> removed_ids;
            metadata.VisitReplicas(is_evictable_memory_replica,
                                   [&removed_ids](Replica& replica) {
                                       removed_ids.push_back(replica.id());
                                       replica.mark_removed();
                                   });
            tl::expected<OpLogEntry, ErrorCode> submission_result;
            if (remaining.empty()) {
                submission_result = AppendReservedOpLogWithDurableFinalize(
                    std::move(reservation.value()), OpType::REMOVE,
                    tenant_id.value(), key, {},
                    [this, removed_ids = std::move(removed_ids)](
                        const OpLogEntry& durable_entry) {
                        FinalizeRemovedReplicasAfterDurable(
                            durable_entry, removed_ids, QuotaEraseMode::kFull);
                    });
            } else {
                submission_result = AppendReservedOpLogWithDurableFinalize(
                    std::move(reservation.value()), OpType::PUT_END,
                    tenant_id.value(), key,
                    SerializeMetadataForOpLogFromReplicaDescriptors(
                        metadata.client_id, metadata.size, remaining,
                        metadata.group_id, metadata.data_type),
                    [this, removed_ids = std::move(removed_ids)](
                        const OpLogEntry& durable_entry) {
                        FinalizeRemovedReplicasAfterDurable(
                            durable_entry, removed_ids, QuotaEraseMode::kFull);
                    });
            }
            if (!submission_result) {
                LOG(WARNING)
                    << "BatchEvict: OpLog submission failed for key=" << key
                    << ", err=" << static_cast<int>(submission_result.error())
                    << ", skipping object eviction";
                return EvictOpLogSubmissionResult::kSubmissionFailed;
            }
            return EvictOpLogSubmissionResult::kSubmitted;
        }

        tl::expected<OpLogEntry, ErrorCode> submission_result;
        if (remaining.empty()) {
            submission_result = AppendOpLogWithDurableFinalize(
                OpType::REMOVE, tenant_id.value(), key, {}, nullptr);
        } else {
            submission_result = AppendOpLogWithDurableFinalize(
                OpType::PUT_END, tenant_id.value(), key,
                SerializeMetadataForOpLogFromReplicaDescriptors(
                    metadata.client_id, metadata.size, remaining,
                    metadata.group_id, metadata.data_type),
                nullptr);
        }
        if (!submission_result) {
            LOG(WARNING) << "BatchEvict: OpLog submission failed for key="
                         << key << ", err="
                         << static_cast<int>(submission_result.error())
                         << ", skipping object eviction";
            return EvictOpLogSubmissionResult::kSubmissionFailed;
        }
        return EvictOpLogSubmissionResult::kSubmitted;
    };

    struct EvictionResult {
        uint64_t freed_bytes{0};
        long evicted_objects{0};
        bool stop_cycle{false};
    };

    auto try_evict_group_or_object =
        [&, this](const TenantId& tenant_id, const std::string& key,
                  ObjectMetadata& metadata, MetadataShardAccessorRW& shard,
                  TenantState& tenant_state,
                  std::vector<std::vector<Replica>>& deferred_replicas,
                  bool allow_soft_pinned) -> EvictionResult {
        if (!metadata.IsGrouped()) {
            auto submission_result =
                submit_evict_oplog_if_needed(tenant_id, key, metadata);
            if (submission_result ==
                EvictOpLogSubmissionResult::kReservationFailed) {
                return {.stop_cycle = true};
            }
            if (submission_result ==
                EvictOpLogSubmissionResult::kSubmissionFailed) {
                return {};
            }
            uint64_t freed = try_evict_or_offload(
                tenant_id, key, metadata, tenant_state, deferred_replicas);
            return {.freed_bytes = freed, .evicted_objects = freed > 0 ? 1 : 0};
        }

        auto group_it = tenant_state.group_members.find(metadata.group_id);
        if (group_it == tenant_state.group_members.end()) {
            auto submission_result =
                submit_evict_oplog_if_needed(tenant_id, key, metadata);
            if (submission_result ==
                EvictOpLogSubmissionResult::kReservationFailed) {
                return {.stop_cycle = true};
            }
            if (submission_result ==
                EvictOpLogSubmissionResult::kSubmissionFailed) {
                return {};
            }
            uint64_t freed = try_evict_or_offload(
                tenant_id, key, metadata, tenant_state, deferred_replicas);
            return {.freed_bytes = freed, .evicted_objects = freed > 0 ? 1 : 0};
        }

        for (const auto& member_key : group_it->second) {
            auto member_it = tenant_state.metadata.find(member_key);
            if (member_it != tenant_state.metadata.end() &&
                !member_it->second.IsLeaseExpired(now)) {
                return {};
            }
        }

        EvictionResult result;
        std::vector<std::string> member_keys(group_it->second.begin(),
                                             group_it->second.end());
        for (const auto& member_key : member_keys) {
            auto member_it = tenant_state.metadata.find(member_key);
            if (member_it == tenant_state.metadata.end()) {
                continue;
            }
            auto& member_metadata = member_it->second;
            if (member_metadata.IsHardPinned() ||
                !member_metadata.IsLeaseExpired(now) ||
                (!allow_soft_pinned && member_metadata.IsSoftPinned(now)) ||
                !can_evict_replicas(member_metadata)) {
                continue;
            }

            auto submission_result = submit_evict_oplog_if_needed(
                tenant_id, member_key, member_metadata);
            if (submission_result ==
                EvictOpLogSubmissionResult::kReservationFailed) {
                result.stop_cycle = true;
                break;
            }
            if (submission_result ==
                EvictOpLogSubmissionResult::kSubmissionFailed) {
                continue;
            }
            uint64_t freed =
                try_evict_or_offload(tenant_id, member_key, member_metadata,
                                     tenant_state, deferred_replicas);
            result.freed_bytes += freed;
            if (freed > 0) {
                result.evicted_objects++;
                if (!enable_oplog_) {
                    PublishKvRemovedAfterEvict(member_key, freed, "cpu",
                                               member_metadata, tenant_id);
                }
            }
            if (member_key != key && !enable_oplog_ &&
                !member_metadata.IsValid()) {
                EraseMetadata(tenant_state, member_it, tenant_id,
                              QuotaEraseMode::kFull, &shard);
            }
        }
        return result;
    };

    // Candidate carries key for safe lookup after releasing shard lock.
    // Iterators would be invalid if the shard is modified between phases.
    struct Candidate {
        size_t shard_idx;
        TenantId tenant_id;
        std::string key;
        std::chrono::system_clock::time_point lease_timeout;
    };

    // Randomly select a starting shard to avoid imbalance eviction between
    // shards.
    size_t start_idx = randomIndex(kNumShards);
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);

    // ===== Phase 1: Parallel candidate collection =====
    // N threads each scan a batch of shards, collecting Candidates with
    // shard_idx + tenant_id + key for safe re-lookup in Phase 2.
    int num_threads = std::min((int)kNumShards, 16);
    size_t shards_per_thread = (kNumShards + num_threads - 1) / num_threads;

    std::vector<std::vector<Candidate>> local_candidates(num_threads);
    std::vector<long> local_eviction_base(num_threads, 0);
    std::vector<long> local_object_count(num_threads, 0);
    std::vector<std::vector<std::chrono::system_clock::time_point>>
        local_soft_pin(num_threads);

    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t] {
            size_t s_start = t * shards_per_thread;
            size_t s_end = std::min(s_start + shards_per_thread, kNumShards);
            for (size_t s = s_start; s < s_end; s++) {
                MetadataShardAccessorRW shard(this, s);
                DiscardExpiredProcessingReplicas(shard, now);

                size_t shard_metadata_count = 0;
                size_t shard_evictable_count = 0;
                for (const auto& [tenant_id, tenant_state] : shard->tenants) {
                    shard_metadata_count += tenant_state.metadata.size();
                    for (auto it = tenant_state.metadata.begin();
                         it != tenant_state.metadata.end(); ++it) {
                        if (it->second.IsHardPinned()) continue;
                        bool has_evictable = can_evict_replicas(it->second);
                        if (has_evictable) shard_evictable_count++;
                        if (!it->second.IsLeaseExpired(now) || !has_evictable)
                            continue;
                        if (!it->second.IsSoftPinned(now)) {
                            local_candidates[t].push_back(
                                {s, tenant_id, it->first,
                                 it->second.lease_timeout});
                        } else if (allow_evict_soft_pinned_objects_) {
                            local_soft_pin[t].push_back(
                                it->second.lease_timeout);
                        }
                    }
                }
                local_object_count[t] += shard_metadata_count;
                local_eviction_base[t] += shard_evictable_count;
            }
        });
    }
    for (auto& t : threads) t.join();

    // Merge per-thread results
    long total_eviction_base = 0;
    for (auto v : local_eviction_base) total_eviction_base += v;

    long object_count = 0;
    for (auto v : local_object_count) object_count += v;

    std::vector<Candidate> candidates;
    {
        size_t total = 0;
        for (auto& v : local_candidates) total += v.size();
        candidates.reserve(total);
    }
    for (auto& v : local_candidates) {
        candidates.insert(candidates.end(), std::make_move_iterator(v.begin()),
                          std::make_move_iterator(v.end()));
    }

    std::vector<std::chrono::system_clock::time_point> soft_pin_objects;
    {
        size_t total = 0;
        for (auto& v : local_soft_pin) total += v.size();
        soft_pin_objects.reserve(total);
    }
    for (auto& v : local_soft_pin) {
        soft_pin_objects.insert(soft_pin_objects.end(),
                                std::make_move_iterator(v.begin()),
                                std::make_move_iterator(v.end()));
    }

    if (total_eviction_base == 0) {
        need_mem_eviction_ = false;
        VLOG(1) << "[EVICT-DIAG] object_count=" << object_count
                << " eviction_base=0 (no evictable memory objects)";
        return;
    }

    // ===== Phase 2: Serial eviction via key lookup =====
    long evicted_count = 0;
    uint64_t total_freed_size = 0;
    std::vector<std::chrono::system_clock::time_point> no_pin_objects;
    std::vector<std::vector<Replica>> deferred_replicas;
    bool stop_eviction = false;

    // First pass: evict candidates with no soft pin
    if (!candidates.empty()) {
        long ideal_evict_num =
            std::ceil(total_eviction_base * evict_ratio_target);
        long evict_num = std::min(ideal_evict_num, (long)candidates.size());

        std::nth_element(candidates.begin(),
                         candidates.begin() + (evict_num - 1), candidates.end(),
                         [](const Candidate& a, const Candidate& b) {
                             return a.lease_timeout < b.lease_timeout;
                         });
        auto target_timeout = candidates[evict_num - 1].lease_timeout;

        // Treat evict_num as a minimum: if re-validation skips a candidate,
        // continue trying the next one so actual evicted count reaches
        // evict_num. This matches the old per-shard over-eviction behavior.
        long evicted_this_pass = 0;
        for (auto& c : candidates) {
            if (stop_eviction) break;
            if (evicted_this_pass >= evict_num &&
                c.lease_timeout > target_timeout) {
                no_pin_objects.push_back(c.lease_timeout);
                continue;
            }
            {
                MetadataShardAccessorRW shard(this, c.shard_idx);
                auto tenant_it = shard->tenants.find(c.tenant_id);
                if (tenant_it == shard->tenants.end()) continue;
                auto& tenant_state = tenant_it->second;
                auto it = tenant_state.metadata.find(c.key);
                if (it == tenant_state.metadata.end()) continue;
                // Re-validate: state may have changed since Phase 1
                if (!it->second.IsLeaseExpired(now) ||
                    it->second.IsSoftPinned(now) ||
                    !can_evict_replicas(it->second)) {
                    no_pin_objects.push_back(c.lease_timeout);
                    continue;
                }
                auto evict_result = try_evict_group_or_object(
                    c.tenant_id, c.key, it->second, shard, tenant_state,
                    deferred_replicas,
                    /*allow_soft_pinned=*/false);
                stop_eviction = evict_result.stop_cycle;
                total_freed_size += evict_result.freed_bytes;
                if (!enable_oplog_ && !it->second.IsGrouped()) {
                    PublishKvRemovedAfterEvict(c.key, evict_result.freed_bytes,
                                               "cpu", it->second, c.tenant_id);
                }
                if (!enable_oplog_ && !it->second.IsValid()) {
                    EraseMetadata(tenant_state, it, c.tenant_id,
                                  QuotaEraseMode::kFull, &shard);
                }
                if (tenant_state.Empty()) {
                    shard->tenants.erase(tenant_it);
                }
                evicted_count += evict_result.evicted_objects;
                evicted_this_pass += evict_result.evicted_objects;
            }
            deferred_replicas.clear();
        }
    }

    // Try releasing discarded replicas before we decide whether to do the
    // second pass.
    uint64_t released_discarded_cnt =
        stop_eviction ? 0 : ReleaseExpiredDiscardedReplicas(now);

    // The ideal number of objects to evict in the second pass
    long target_evict_num =
        std::ceil(total_eviction_base * evict_ratio_lowerbound) -
        evicted_count - released_discarded_cnt;
    // The actual number of objects we can evict in the second pass
    target_evict_num =
        std::min(target_evict_num,
                 (long)no_pin_objects.size() + (long)soft_pin_objects.size());

    // Do second pass eviction only if 1). there are candidates that can be
    // evicted AND 2). The evicted number in the first pass is less than
    // evict_ratio_lowerbound.
    if (!stop_eviction && target_evict_num > 0) {
        if (target_evict_num <= static_cast<long>(no_pin_objects.size())) {
            // Second pass A: only evict objects without soft pin.
            std::nth_element(no_pin_objects.begin(),
                             no_pin_objects.begin() + (target_evict_num - 1),
                             no_pin_objects.end());
            auto target_timeout = no_pin_objects[target_evict_num - 1];

            // Evict via key lookup — avoid full metadata traversal
            for (size_t i = 0;
                 i < kNumShards && target_evict_num > 0 && !stop_eviction;
                 i++) {
                {
                    MetadataShardAccessorRW shard(this,
                                                  (start_idx + i) % kNumShards);
                    for (auto tenant_it = shard->tenants.begin();
                         tenant_it != shard->tenants.end() &&
                         target_evict_num > 0 && !stop_eviction;) {
                        auto& tenant_state = tenant_it->second;
                        auto it = tenant_state.metadata.begin();
                        while (it != tenant_state.metadata.end() &&
                               target_evict_num > 0 && !stop_eviction) {
                            if (!it->second.IsHardPinned() &&
                                it->second.IsLeaseExpired(now) &&
                                it->second.lease_timeout <= target_timeout &&
                                !it->second.IsSoftPinned(now) &&
                                can_evict_replicas(it->second)) {
                                auto evict_result = try_evict_group_or_object(
                                    tenant_it->first, it->first, it->second,
                                    shard, tenant_state, deferred_replicas,
                                    /*allow_soft_pinned=*/false);
                                stop_eviction = evict_result.stop_cycle;
                                total_freed_size += evict_result.freed_bytes;
                                if (!enable_oplog_ && !it->second.IsGrouped()) {
                                    PublishKvRemovedAfterEvict(
                                        it->first, evict_result.freed_bytes,
                                        "cpu", it->second, tenant_it->first);
                                }
                                if (!enable_oplog_ && !it->second.IsValid()) {
                                    it = EraseMetadata(
                                        tenant_state, it, tenant_it->first,
                                        QuotaEraseMode::kFull, &shard);
                                } else {
                                    ++it;
                                }
                                evicted_count += evict_result.evicted_objects;
                                target_evict_num -=
                                    evict_result.evicted_objects;
                                if (stop_eviction) break;
                            } else {
                                ++it;
                            }
                        }
                        if (tenant_state.Empty()) {
                            tenant_it = shard->tenants.erase(tenant_it);
                        } else {
                            ++tenant_it;
                        }
                    }
                }
                deferred_replicas.clear();
            }
        } else if (!soft_pin_objects.empty()) {
            // Second pass B: Prioritize evicting objects without soft pin,
            // but also allow evicting soft pinned objects.
            const long soft_pin_evict_num =
                target_evict_num - static_cast<long>(no_pin_objects.size());
            std::nth_element(
                soft_pin_objects.begin(),
                soft_pin_objects.begin() + (soft_pin_evict_num - 1),
                soft_pin_objects.end());
            auto soft_target_timeout = soft_pin_objects[soft_pin_evict_num - 1];

            for (size_t i = 0;
                 i < kNumShards && target_evict_num > 0 && !stop_eviction;
                 i++) {
                {
                    MetadataShardAccessorRW shard(this,
                                                  (start_idx + i) % kNumShards);

                    for (auto tenant_it = shard->tenants.begin();
                         tenant_it != shard->tenants.end() &&
                         target_evict_num > 0 && !stop_eviction;) {
                        auto& tenant_state = tenant_it->second;
                        auto it = tenant_state.metadata.begin();
                        while (it != tenant_state.metadata.end() &&
                               target_evict_num > 0 && !stop_eviction) {
                            if (it->second.IsHardPinned() ||
                                !it->second.IsLeaseExpired(now) ||
                                !can_evict_replicas(it->second)) {
                                ++it;
                                continue;
                            }
                            if (!it->second.IsSoftPinned(now) ||
                                it->second.lease_timeout <=
                                    soft_target_timeout) {
                                auto evict_result = try_evict_group_or_object(
                                    tenant_it->first, it->first, it->second,
                                    shard, tenant_state, deferred_replicas,
                                    /*allow_soft_pinned=*/true);
                                stop_eviction = evict_result.stop_cycle;
                                total_freed_size += evict_result.freed_bytes;
                                if (!enable_oplog_ && !it->second.IsGrouped()) {
                                    PublishKvRemovedAfterEvict(
                                        it->first, evict_result.freed_bytes,
                                        "cpu", it->second, tenant_it->first);
                                }
                                if (!enable_oplog_ && !it->second.IsValid()) {
                                    it = EraseMetadata(
                                        tenant_state, it, tenant_it->first,
                                        QuotaEraseMode::kFull, &shard);
                                } else {
                                    ++it;
                                }
                                evicted_count += evict_result.evicted_objects;
                                target_evict_num -=
                                    evict_result.evicted_objects;
                                if (stop_eviction) break;
                            } else {
                                ++it;
                            }
                        }
                        if (tenant_state.Empty()) {
                            tenant_it = shard->tenants.erase(tenant_it);
                        } else {
                            ++tenant_it;
                        }
                    }
                }
                deferred_replicas.clear();
            }
        } else {
            LOG(ERROR) << "Error in second pass eviction: target_evict_num="
                       << target_evict_num
                       << ", no_pin_objects.size()=" << no_pin_objects.size()
                       << ", soft_pin_objects.size()="
                       << soft_pin_objects.size()
                       << ", evicted_count=" << evicted_count
                       << ", eviction_base=" << total_eviction_base
                       << ", evict_ratio_target=" << evict_ratio_target
                       << ", evict_ratio_lowerbound=" << evict_ratio_lowerbound;
        }
    }

    if (stop_eviction) {
        // Reservation backpressure is transient; retry remaining work later.
        need_mem_eviction_ = true;
        if (evicted_count > 0) {
            MasterMetricManager::instance().inc_eviction_success(
                evicted_count, total_freed_size);
            MasterMetricManager::instance().inc_mem_eviction_success(
                evicted_count, total_freed_size);
        } else {
            MasterMetricManager::instance().inc_eviction_fail();
            MasterMetricManager::instance().inc_mem_eviction_fail();
        }
    } else if (evicted_count > 0 || released_discarded_cnt > 0) {
        need_mem_eviction_ = false;
        MasterMetricManager::instance().inc_eviction_success(evicted_count,
                                                             total_freed_size);
        MasterMetricManager::instance().inc_mem_eviction_success(
            evicted_count, total_freed_size);
    } else if (offload_deferred_count > 0) {
        need_mem_eviction_ = false;
        MasterMetricManager::instance().inc_eviction_success(0, 0);
        MasterMetricManager::instance().inc_mem_eviction_success(0, 0);
    } else {
        if (total_eviction_base == 0) {
            need_mem_eviction_ = false;
        }
        MasterMetricManager::instance().inc_eviction_fail();
        MasterMetricManager::instance().inc_mem_eviction_fail();
    }
    VLOG(1) << "action=evict_objects"
            << ", evicted_count=" << evicted_count
            << ", offload_deferred=" << offload_deferred_count
            << ", offload_cap_forced=" << offload_cap_forced_count
            << ", offload_push_failed_forced=" << offload_push_failed_forced
            << ", total_freed_size=" << total_freed_size
            << ", eviction_base=" << total_eviction_base
            << ", actual_evict_ratio="
            << (total_eviction_base > 0
                    ? (double)evicted_count / total_eviction_base
                    : 0.0)
            << ", target_evict_ratio=" << evict_ratio_target;
    VLOG(1) << "[EVICT-DIAG] object_count=" << object_count
            << " disk_object_count=" << (object_count - total_eviction_base)
            << " eviction_base=" << total_eviction_base << " disk_ratio="
            << (object_count > 0
                    ? (double)(object_count - total_eviction_base) /
                          object_count
                    : 0.0)
            << " ideal_evict_num_inflated="
            << (long)std::ceil(object_count * evict_ratio_target)
            << " ideal_evict_num_correct="
            << (long)std::ceil(total_eviction_base * evict_ratio_target);
    LOG(INFO) << "[EVICT-RESULT] evicted_count=" << evicted_count
              << ", eviction_base=" << total_eviction_base
              << ", actual_evict_ratio="
              << (total_eviction_base > 0
                      ? (double)evicted_count / total_eviction_base
                      : 0.0)
              << ", target_evict_ratio=" << evict_ratio_target;
    if (offload_on_evict_ && evicted_count == 0 && offload_deferred_count > 0) {
        LOG(WARNING) << "[EVICT] No memory freed this cycle; "
                     << offload_deferred_count
                     << " objects deferred for disk offload. "
                        "Consider lowering eviction_high_watermark_ratio.";
    }
    if (offload_cap_forced_count > 0) {
        LOG(WARNING) << "[EVICT] Offload cap (" << offload_cap
                     << ") reached; force-evicted " << offload_cap_forced_count
                     << " object(s) without disk offload this cycle.";
    }
    if (offload_push_failed_forced > 0) {
        LOG(WARNING) << "[EVICT] PushOffloadingQueue failed for "
                     << offload_push_failed_forced
                     << " object(s); force-evicted without disk offload "
                        "(offload_force_evict=true).";
    }

    pt_evict.End(0);
}

void MasterService::NoFBatchEvict(double evict_ratio_target,
                                  double evict_ratio_lowerbound) {
    SpDiag::PerfPoint pt_nof_evict(PerfKey::MASTER_BG_NOF_BATCH_EVICT,
                                   SpDiag::PerfLevel::KEY_MODULE);
    pt_nof_evict.Start();

    if (evict_ratio_target < evict_ratio_lowerbound) {
        MC_LOG(ERROR) << "nof_evict_ratio_target=" << evict_ratio_target
                      << ", nof_evict_ratio_lowerbound="
                      << evict_ratio_lowerbound << ", error=invalid_params";
        evict_ratio_lowerbound = evict_ratio_target;
    }

    auto now = std::chrono::system_clock::now();
    long evicted_count = 0;
    long object_count = 0;
    uint64_t total_freed_size = 0;

    auto is_evictable_nof_replica = [](const Replica& replica) {
        return replica.is_nof_replica() && replica.is_completed() &&
               replica.get_refcnt() == 0;
    };

    size_t start_idx = randomIndex(metadata_shards_.size());
    for (size_t i = 0; i < metadata_shards_.size(); i++) {
        MetadataShardAccessorRW shard(
            this, (start_idx + i) % metadata_shards_.size());
        DiscardExpiredProcessingReplicas(shard, now);
        for (const auto& [tenant_id, tenant_state] : shard->tenants) {
            object_count += tenant_state.metadata.size();
        }

        const long ideal_evict_num =
            std::ceil(object_count * evict_ratio_target) - evicted_count;
        if (ideal_evict_num <= 0) {
            continue;
        }

        long shard_evicted_count = 0;
        for (auto tenant_it = shard->tenants.begin();
             tenant_it != shard->tenants.end() &&
             shard_evicted_count < ideal_evict_num;) {
            auto& tenant_state = tenant_it->second;
            for (auto it = tenant_state.metadata.begin();
                 it != tenant_state.metadata.end() &&
                 shard_evicted_count < ideal_evict_num;) {
                auto& metadata = it->second;
                if (metadata.IsHardPinned() || !metadata.IsLeaseExpired(now) ||
                    metadata.IsSoftPinned(now)) {
                    ++it;
                    continue;
                }

                // Probe: any NoF replicas eligible for eviction?
                const bool has_evictable_nof =
                    metadata.HasReplica(is_evictable_nof_replica);
                if (!has_evictable_nof) {
                    ++it;
                    continue;
                }

                // HA strong consistency: persist BEFORE erasing NoF replicas.
                // Skip the key on persist failure.
                if (enable_oplog_ && ordered_oplog_writer_) {
                    auto remaining = BuildRemainingReplicaDescriptors(
                        metadata, is_evictable_nof_replica);
                    if (enable_oplog_) {
                        auto reservation = ReserveBatchOpLogSlot();
                        if (!reservation) {
                            LOG(WARNING)
                                << "NoFBatchEvict: OpLog reservation failed "
                                   "for key="
                                << it->first << ", err="
                                << static_cast<int>(reservation.error())
                                << ", skipping eviction";
                            ++it;
                            continue;
                        }
                        std::vector<ReplicaID> removed_ids;
                        metadata.VisitReplicas(
                            is_evictable_nof_replica,
                            [&removed_ids](Replica& replica) {
                                removed_ids.push_back(replica.id());
                                replica.mark_removed();
                            });
                        const size_t removed_count = removed_ids.size();
                        tl::expected<OpLogEntry, ErrorCode> persist_result;
                        if (remaining.empty()) {
                            persist_result =
                                AppendReservedOpLogWithDurableFinalize(
                                    std::move(reservation.value()),
                                    OpType::REMOVE, tenant_it->first.value(),
                                    it->first, {},
                                    [this,
                                     removed_ids = std::move(removed_ids)](
                                        const OpLogEntry& durable_entry) {
                                        FinalizeRemovedReplicasAfterDurable(
                                            durable_entry, removed_ids,
                                            QuotaEraseMode::kFull);
                                    });
                        } else {
                            persist_result = AppendReservedOpLogWithDurableFinalize(
                                std::move(reservation.value()), OpType::PUT_END,
                                tenant_it->first.value(), it->first,
                                SerializeMetadataForOpLogFromReplicaDescriptors(
                                    metadata.client_id, metadata.size,
                                    remaining, metadata.group_id,
                                    metadata.data_type),
                                [this, removed_ids = std::move(removed_ids)](
                                    const OpLogEntry& durable_entry) {
                                    FinalizeRemovedReplicasAfterDurable(
                                        durable_entry, removed_ids,
                                        QuotaEraseMode::kFull);
                                });
                        }
                        if (!persist_result) {
                            LOG(WARNING)
                                << "NoFBatchEvict: OpLog persist failed for "
                                   "key="
                                << it->first << ", err="
                                << static_cast<int>(persist_result.error())
                                << ", skipping eviction";
                            ++it;
                            continue;
                        }
                        total_freed_size += metadata.size * removed_count;
                        shard_evicted_count++;
                        ++it;
                        continue;
                    }

                    tl::expected<OpLogEntry, ErrorCode> persist_result;
                    if (remaining.empty()) {
                        persist_result = AppendOpLogWithDurableFinalize(
                            OpType::REMOVE, tenant_it->first.value(), it->first,
                            {}, nullptr);
                    } else {
                        persist_result = AppendOpLogWithDurableFinalize(
                            OpType::PUT_END, tenant_it->first.value(),
                            it->first,
                            SerializeMetadataForOpLogFromReplicaDescriptors(
                                metadata.client_id, metadata.size, remaining,
                                metadata.group_id, metadata.data_type),
                            nullptr);
                    }
                    if (!persist_result) {
                        LOG(WARNING)
                            << "NoFBatchEvict: OpLog persist failed for key="
                            << it->first << ", err="
                            << static_cast<int>(persist_result.error())
                            << ", skipping eviction";
                        ++it;
                        continue;
                    }
                }

                const size_t erased =
                    metadata.EraseReplicas(is_evictable_nof_replica);
                if (erased == 0) {
                    ++it;
                    continue;
                }

                total_freed_size += metadata.size * erased;
                shard_evicted_count++;
                PublishKvRemovedAfterEvict(it->first, metadata.size * erased,
                                           "disk", metadata, tenant_it->first);
                if (!metadata.IsValid()) {
                    it = EraseMetadata(tenant_state, it, tenant_it->first,
                                       QuotaEraseMode::kFull, &shard);
                } else {
                    ++it;
                }
            }
            if (tenant_state.Empty()) {
                tenant_it = shard->tenants.erase(tenant_it);
            } else {
                ++tenant_it;
            }
        }
        evicted_count += shard_evicted_count;
    }

    if (evicted_count > 0) {
        need_nof_eviction_ = false;
        MasterMetricManager::instance().inc_eviction_success(evicted_count,
                                                             total_freed_size);
        MasterMetricManager::instance().inc_nof_eviction_success(
            evicted_count, total_freed_size);
    } else {
        if (object_count == 0) {
            need_nof_eviction_ = false;
        }
        MasterMetricManager::instance().inc_eviction_fail();
        MasterMetricManager::instance().inc_nof_eviction_fail();
    }

    VLOG(1) << "action=evict_nof_replicas"
            << ", evicted_count=" << evicted_count
            << ", total_freed_size=" << total_freed_size;

    pt_nof_evict.End(0);
}

void MasterService::ClientMonitorFunc() {
    std::unordered_map<UUID, std::chrono::steady_clock::time_point,
                       boost::hash<UUID>>
        client_ttl;
    while (client_monitor_running_) {
        SpDiag::PerfPoint pt_monitor(PerfKey::MASTER_BG_CLIENT_MONITOR,
                                     SpDiag::PerfLevel::MODULE);
        pt_monitor.Start();

        auto now = std::chrono::steady_clock::now();

        // Update the client ttl
        PodUUID pod_client_id;
        while (client_ping_queue_.pop(pod_client_id)) {
            UUID client_id = {pod_client_id.first, pod_client_id.second};
            client_ttl[client_id] =
                now + std::chrono::seconds(client_live_ttl_sec_);
        }

        // Find out expired clients
        std::vector<UUID> expired_clients;
        for (auto it = client_ttl.begin(); it != client_ttl.end();) {
            if (it->second < now) {
                LOG(INFO) << "client_id=" << it->first
                          << ", action=client_expired";
                expired_clients.push_back(it->first);
                it = client_ttl.erase(it);
            } else {
                ++it;
            }
        }

        // Update the client status to NEED_REMOUNT
        if (!expired_clients.empty()) {
            SpDiag::PerfPoint pt_unmount(PerfKey::MASTER_BG_CLIENT_UNMOUNT,
                                         SpDiag::PerfLevel::MODULE);
            pt_unmount.Start();
            // Notify graceful unmount scheduler to drop pending records
            // for expired clients. The actual unmount is handled below.
            for (auto& cid : expired_clients) {
                graceful_unmount_scheduler_.RemoveIf(
                    [&cid](const GracefulUnmountDeadlineRecord& record) {
                        return record.client_id == cid;
                    });
            }

            // Record which segments are unmounted, will be used in the commit
            // phase.
            std::vector<UUID> unmount_segments;
            std::vector<size_t> dec_capacities;
            std::vector<UUID> client_ids;
            std::vector<std::string> segment_names;
            std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
            {
                // Lock client_mutex and segment_mutex
                std::unique_lock<std::shared_mutex> lock(client_mutex_);
                for (auto& client_id : expired_clients) {
                    auto it = ok_client_.find(client_id);
                    if (it != ok_client_.end()) {
                        ok_client_.erase(it);
                        MasterMetricManager::instance().dec_active_clients();
                    }
                    client_host_id_.erase(client_id);
                }

                ScopedSegmentAccess segment_access =
                    segment_manager_.getSegmentAccess();
                for (auto& client_id : expired_clients) {
                    // mounted mem segemtns of this expired client
                    std::vector<Segment> segments;
                    segment_access.GetClientSegments(client_id, segments);
                    for (auto& seg : segments) {
                        size_t metrics_dec_capacity = 0;
                        if (segment_access.PrepareUnmountSegment(
                                seg.id, metrics_dec_capacity) ==
                            ErrorCode::OK) {
                            unmount_segments.push_back(seg.id);
                            dec_capacities.push_back(metrics_dec_capacity);
                            client_ids.push_back(client_id);
                            segment_names.push_back(seg.name);
                        } else {
                            LOG(ERROR) << "client_id=" << client_id
                                       << ", segment_name=" << seg.name
                                       << ", "
                                          "error=prepare_unmount_expired_"
                                          "mem_segment_failed";
                        }
                    }
                }
            }  // Release the mutex before long-running ClearInvalidHandles and
               // avoid deadlocks

            // Always clean up invalid handles when there are expired clients,
            // even if no memory segments were unmounted. This is necessary
            // to clean up local_disk replicas whose owner client has expired.
            ClearInvalidHandles();

            // Commit unmount of memory segments and clean up local_disk
            // segments for expired clients. Both require the exclusive
            // segment lock.
            {
                ScopedSegmentAccess segment_access =
                    segment_manager_.getSegmentAccess();
                for (size_t i = 0; i < unmount_segments.size(); i++) {
                    segment_access.CommitUnmountSegment(
                        unmount_segments[i], client_ids[i], dec_capacities[i]);
                    LOG(INFO) << "client_id=" << client_ids[i]
                              << ", segment_name=" << segment_names[i]
                              << ", action=unmount_expired_mem_segment";
                    // Clean up HTTP metadata if enabled
                    cleanupHttpMetadata(segment_names[i]);
                }
                for (auto& client_id : expired_clients) {
                    segment_access.UnmountLocalDiskSegment(client_id);
                }
            }
            RecomputeTenantEffectiveQuotas();
            pt_unmount.End(0);
        }

        pt_monitor.End(0);
        std::this_thread::sleep_for(
            std::chrono::milliseconds(kClientMonitorSleepMs));
    }
}

bool MasterService::ProbeNoFSegment(const std::string& te_endpoint,
                                    std::string* error_reason) {
#ifndef USE_NOF
    if (error_reason) {
        *error_reason = "nof_pool_disabled";
    }
    return false;
#else
    NoFProbeFn probe_fn;
    {
        std::lock_guard<std::mutex> lock(nof_probe_fn_mutex_);
        probe_fn = nof_probe_fn_;
    }
    if (!probe_fn) {
        if (error_reason) {
            *error_reason = "probe_not_configured";
        }
        return false;
    }
    return probe_fn(
        te_endpoint,
        static_cast<uint32_t>(nof_heartbeat_probe_timeout_ms_.count()),
        error_reason);
#endif
}

bool MasterService::TryUnmountNoFSegmentByHeartbeat(
    const MountedNoFSegmentSnapshot& snapshot,
    const std::string& error_reason) {
    size_t metrics_dec_capacity = 0;
    {
        auto nof_segment_access = nof_segment_manager_.getNoFSegmentAccess();
        ErrorCode err = nof_segment_access.PrepareUnmountSegment(
            snapshot.segment_id, metrics_dec_capacity);
        if (err == ErrorCode::SEGMENT_NOT_FOUND ||
            err == ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS) {
            std::lock_guard<std::mutex> lock(nof_heartbeat_mutex_);
            nof_heartbeat_states_.erase(snapshot.segment_id);
            VLOG(1) << "segment_id=" << snapshot.segment_id
                    << ", action=skip_nof_heartbeat_unmount"
                    << ", reason=" << toString(err);
            return false;
        }
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "segment_id=" << snapshot.segment_id
                       << ", segment_name=" << snapshot.segment.name
                       << ", error=prepare_unmount_nof_segment_by_"
                          "heartbeat_failed"
                       << ", reason=" << err;
            return false;
        }
    }

    ClearInvalidHandles();

    {
        auto nof_segment_access = nof_segment_manager_.getNoFSegmentAccess();
        ErrorCode err = nof_segment_access.CommitUnmountSegment(
            snapshot.segment_id, snapshot.client_id, metrics_dec_capacity);
        if (err != ErrorCode::OK && err != ErrorCode::SEGMENT_NOT_FOUND) {
            LOG(ERROR) << "segment_id=" << snapshot.segment_id
                       << ", segment_name=" << snapshot.segment.name
                       << ", error=commit_unmount_nof_segment_by_"
                          "heartbeat_failed"
                       << ", reason=" << err;
            return false;
        }
    }

    {
        std::lock_guard<std::mutex> lock(nof_heartbeat_mutex_);
        nof_heartbeat_states_.erase(snapshot.segment_id);
    }
    MasterMetricManager::instance()
        .inc_nof_segments_unmounted_by_heartbeat_total();
    LOG(INFO) << "segment_id=" << snapshot.segment_id
              << ", client_id=" << snapshot.client_id
              << ", segment_name=" << snapshot.segment.name
              << ", endpoint=" << snapshot.segment.te_endpoint
              << ", action=unmount_nof_segment_by_heartbeat"
              << ", last_error_reason=" << error_reason;
    return true;
}

void MasterService::NofHeartbeatThreadFunc() {
    size_t next_probe_index = 0;
    while (nof_heartbeat_running_) {
        auto now = std::chrono::steady_clock::now();
        std::vector<MountedNoFSegmentSnapshot> mounted_segments;
        nof_segment_manager_.GetMountedSegmentsSnapshot(mounted_segments);

        std::vector<MountedNoFSegmentSnapshot> ok_segments;
        ok_segments.reserve(mounted_segments.size());
        for (const auto& snapshot : mounted_segments) {
            if (snapshot.status == SegmentStatus::OK) {
                ok_segments.push_back(snapshot);
            }
        }

        std::optional<MountedNoFSegmentSnapshot> probe_target;
        {
            std::lock_guard<std::mutex> lock(nof_heartbeat_mutex_);
            std::unordered_set<UUID, boost::hash<UUID>> live_segment_ids;
            live_segment_ids.reserve(ok_segments.size());

            const auto interval_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    nof_heartbeat_interval_sec_);
            for (size_t i = 0; i < ok_segments.size(); ++i) {
                const auto& snapshot = ok_segments[i];
                live_segment_ids.insert(snapshot.segment_id);
                auto [it, inserted] =
                    nof_heartbeat_states_.try_emplace(snapshot.segment_id);
                auto& state = it->second;
                state.owner_client_id = snapshot.client_id;
                state.segment_name = snapshot.segment.name;
                state.te_endpoint = snapshot.segment.te_endpoint;
                if (inserted) {
                    int64_t spread_ms = 0;
                    if (!ok_segments.empty()) {
                        spread_ms = static_cast<int64_t>(
                            (interval_ms.count() * i) / ok_segments.size());
                    }
                    state.last_success_at = now;
                    state.next_probe_at = now + nof_heartbeat_interval_sec_ +
                                          std::chrono::milliseconds(spread_ms);
                }
            }

            for (auto it = nof_heartbeat_states_.begin();
                 it != nof_heartbeat_states_.end();) {
                if (!live_segment_ids.contains(it->first)) {
                    it = nof_heartbeat_states_.erase(it);
                } else {
                    ++it;
                }
            }

            if (!ok_segments.empty()) {
                next_probe_index %= ok_segments.size();
                for (size_t offset = 0; offset < ok_segments.size(); ++offset) {
                    const auto& candidate =
                        ok_segments[(next_probe_index + offset) %
                                    ok_segments.size()];
                    auto state_it =
                        nof_heartbeat_states_.find(candidate.segment_id);
                    if (state_it == nof_heartbeat_states_.end()) {
                        continue;
                    }
                    if (state_it->second.next_probe_at <= now) {
                        probe_target = candidate;
                        next_probe_index = (next_probe_index + offset + 1) %
                                           ok_segments.size();
                        break;
                    }
                }
            }
        }

        if (!probe_target.has_value()) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kNoFHeartbeatThreadSleepMs));
            continue;
        }

        auto probe_start = std::chrono::steady_clock::now();
        std::string error_reason;
        bool probe_success =
            ProbeNoFSegment(probe_target->segment.te_endpoint, &error_reason);
        auto latency_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - probe_start)
                              .count();
        MasterMetricManager::instance().observe_nof_heartbeat_probe_latency_ms(
            latency_ms);

        if (probe_success) {
            MasterMetricManager::instance().inc_nof_heartbeat_success_total();
            auto success_time = std::chrono::steady_clock::now();
            {
                std::lock_guard<std::mutex> lock(nof_heartbeat_mutex_);
                auto it = nof_heartbeat_states_.find(probe_target->segment_id);
                if (it != nof_heartbeat_states_.end()) {
                    it->second.consecutive_failures = 0;
                    it->second.last_success_at = success_time;
                    it->second.last_error_reason.clear();
                    it->second.next_probe_at =
                        success_time + nof_heartbeat_interval_sec_;
                }
            }
            VLOG(1) << "segment_id=" << probe_target->segment_id
                    << ", segment_name=" << probe_target->segment.name
                    << ", endpoint=" << probe_target->segment.te_endpoint
                    << ", action=nof_heartbeat_success"
                    << ", latency_ms=" << latency_ms;
            continue;
        }

        MasterMetricManager::instance().inc_nof_heartbeat_failure_total();
        if (error_reason == "completion_timeout") {
            MasterMetricManager::instance().inc_nof_heartbeat_timeout_total();
        }

        bool should_unmount = false;
        uint32_t failure_count = 0;
        auto failure_time = std::chrono::steady_clock::now();
        auto alive_timeout =
            nof_heartbeat_interval_sec_ *
            static_cast<int64_t>(nof_heartbeat_failures_threshold_);
        {
            std::lock_guard<std::mutex> lock(nof_heartbeat_mutex_);
            auto it = nof_heartbeat_states_.find(probe_target->segment_id);
            if (it != nof_heartbeat_states_.end()) {
                it->second.consecutive_failures++;
                failure_count = it->second.consecutive_failures;
                it->second.last_error_reason = error_reason;
                it->second.next_probe_at =
                    failure_time + nof_heartbeat_interval_sec_;
                should_unmount =
                    failure_time - it->second.last_success_at >= alive_timeout;
            }
        }

        LOG(WARNING) << "segment_id=" << probe_target->segment_id
                     << ", segment_name=" << probe_target->segment.name
                     << ", endpoint=" << probe_target->segment.te_endpoint
                     << ", action=nof_heartbeat_failure"
                     << ", failure_count=" << failure_count
                     << ", latency_ms=" << latency_ms
                     << ", reason=" << error_reason;

        if (should_unmount) {
            TryUnmountNoFSegmentByHeartbeat(*probe_target, error_reason);
        }
    }
}

tl::expected<std::vector<uint8_t>, SerializationError>
MasterService::MetadataSerializer::Serialize() {
    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(&sbuf);

    // Create top-level map with 3 fields: "shards", "discarded_replicas",
    // "replica_next_id"
    packer.pack_map(3);

    // 1. Serialize metadata shards
    packer.pack("shards");

    // First count shards that have actual metadata entries.
    // A shard may have empty tenants left after eviction erased all
    // metadata but didn't clean up the tenant map; using metadata_count
    // (not tenants.empty()) ensures the count matches the skip logic below.
    size_t valid_shards = 0;
    for (size_t i = 0; i < kNumShards; ++i) {
        size_t metadata_count = 0;
        for (const auto& [tid, ts] : service_->metadata_shards_[i].tenants) {
            metadata_count += ts.metadata.size();
        }
        if (metadata_count > 0) {
            valid_shards++;
        }
    }

    // Create shards map
    packer.pack_map(valid_shards);

    // Iterate through all shards, serialize each shard independently
    for (size_t shard_idx = 0; shard_idx < kNumShards; ++shard_idx) {
        const auto& shard = service_->metadata_shards_[shard_idx];

        // Skip shards with no actual metadata entries.
        // A shard may have empty tenants left after eviction erased all
        // metadata but didn't clean up the tenant map; serializing those
        // would produce an entry that deserialization never recreates,
        // breaking the snapshot round-trip comparison.
        size_t metadata_count = 0;
        for (const auto& [tid, ts] : shard.tenants) {
            metadata_count += ts.metadata.size();
        }
        if (metadata_count == 0) {
            continue;
        }

        // Use shard index as key
        packer.pack(shard_idx);

        // Create independent serialization buffer for current shard
        msgpack::sbuffer shard_buffer;
        msgpack::packer<msgpack::sbuffer> shard_packer(&shard_buffer);

        // Serialize shard using SerializeShard
        auto result = SerializeShard(shard, shard_packer);
        if (!result) {
            return tl::make_unexpected(SerializationError(
                result.error().code,
                fmt::format("Failed to serialize shard {}: {}", shard_idx,
                            result.error().message)));
        }

        // Compress data
        std::vector<uint8_t> compressed_data =
            zstd_compress(reinterpret_cast<const uint8_t*>(shard_buffer.data()),
                          shard_buffer.size(), 3);
        // Write entire shard serialized data as binary to main buffer
        packer.pack_bin(compressed_data.size());
        packer.pack_bin_body(
            reinterpret_cast<const char*>(compressed_data.data()),
            compressed_data.size());
    }

    // 2. Serialize discarded_replicas
    packer.pack("discarded_replicas");
    auto dr_result = SerializeDiscardedReplicas(packer);
    if (!dr_result) {
        return tl::make_unexpected(SerializationError(
            dr_result.error().code, "Failed to serialize discarded_replicas: " +
                                        dr_result.error().message));
    }

    // 3. Serialize replica_next_id (static variable for generating unique
    // replica IDs)
    packer.pack("replica_next_id");
    packer.pack(static_cast<uint64_t>(Replica::next_id_.load()));

    return std::vector<uint8_t>(
        reinterpret_cast<const uint8_t*>(sbuf.data()),
        reinterpret_cast<const uint8_t*>(sbuf.data()) + sbuf.size());
}

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::Deserialize(
    const std::vector<uint8_t>& data) {
    // Parse MessagePack data directly
    msgpack::object_handle oh;
    try {
        oh = msgpack::unpack(reinterpret_cast<const char*>(data.data()),
                             data.size());
    } catch (const std::exception& e) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "Failed to unpack MessagePack data: " + std::string(e.what())));
    }

    const msgpack::object& obj = oh.get();

    // Check if it's a map
    if (obj.type != msgpack::type::MAP) {
        return tl::make_unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "Invalid MessagePack format: expected map"));
    }

    // Expected format: top-level map with "shards", "discarded_replicas",
    // and "replica_next_id"
    const msgpack::object* shards_obj = nullptr;
    const msgpack::object* discarded_replicas_obj = nullptr;
    const msgpack::object* replica_next_id_obj = nullptr;

    // Extract fields from top-level map
    for (uint32_t i = 0; i < obj.via.map.size; ++i) {
        const auto& key_obj = obj.via.map.ptr[i].key;
        if (key_obj.type == msgpack::type::STR) {
            std::string key = key_obj.as<std::string>();
            if (key == "shards") {
                shards_obj = &obj.via.map.ptr[i].val;
            } else if (key == "discarded_replicas") {
                discarded_replicas_obj = &obj.via.map.ptr[i].val;
            } else if (key == "replica_next_id") {
                replica_next_id_obj = &obj.via.map.ptr[i].val;
            }
        }
    }

    // Check required "shards" field
    if (shards_obj == nullptr) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL, "Missing 'shards' field"));
    }

    // Iterate and deserialize each shard
    for (uint32_t i = 0; i < shards_obj->via.map.size; ++i) {
        // Get shard index
        uint32_t shard_idx = shards_obj->via.map.ptr[i].key.as<uint32_t>();

        // Check shard index validity
        if (shard_idx >= kNumShards) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format("Invalid shard index: {}", shard_idx)));
        }

        // Get shard binary data
        const msgpack::object& shard_data_obj = shards_obj->via.map.ptr[i].val;
        if (shard_data_obj.type != msgpack::type::BIN) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "Invalid MessagePack format: expected binary data for shard"));
        }

        // Parse shard binary data directly, avoiding copy
        msgpack::object_handle shard_oh;
        try {
            auto decompressed_data = zstd_decompress(
                reinterpret_cast<const uint8_t*>(shard_data_obj.via.bin.ptr),
                shard_data_obj.via.bin.size);
            shard_oh = msgpack::unpack(
                reinterpret_cast<const char*>(decompressed_data.data()),
                decompressed_data.size());
        } catch (const std::exception& e) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "Failed to unpack shard data: " + std::string(e.what())));
        }

        const msgpack::object& shard_obj = shard_oh.get();

        // Get shard reference and deserialize
        auto& shard = service_->metadata_shards_[shard_idx];
        auto result = DeserializeShard(shard_obj, shard);
        if (!result) {
            return tl::make_unexpected(SerializationError(
                result.error().code,
                fmt::format("Failed to deserialize shard {}: {}", shard_idx,
                            result.error().message)));
        }
    }

    // Deserialize discarded_replicas
    if (discarded_replicas_obj == nullptr) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "Missing required field 'discarded_replicas' in snapshot data"));
    }
    auto dr_result = DeserializeDiscardedReplicas(*discarded_replicas_obj);
    if (!dr_result) {
        return tl::make_unexpected(
            SerializationError(dr_result.error().code,
                               "Failed to deserialize discarded_replicas: " +
                                   dr_result.error().message));
    }

    // Restore replica_next_id
    if (replica_next_id_obj == nullptr) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "Missing required field 'replica_next_id' in snapshot data"));
    }
    auto next_id = replica_next_id_obj->as<uint64_t>();
    Replica::next_id_.store(next_id);
    LOG(INFO) << "Restored Replica::next_id_ to " << next_id;
    service_->RebuildGroupRoutingIndex();
    service_->ClearCandidatesForReload();
    return {};
}

void MasterService::MetadataSerializer::Reset() {
    for (auto& shard : service_->metadata_shards_) {
        shard.tenants.clear();
    }
    {
        std::unique_lock<std::shared_mutex> lock(
            service_->group_routing_mutex_);
        service_->object_group_ids_.clear();
        service_->groups_needing_lease_refresh_.clear();
    }
    {
        std::lock_guard lock(service_->discarded_replicas_mutex_);
        service_->discarded_replicas_.clear();
    }
    Replica::next_id_.store(1);
    service_->ClearCandidatesForReload();
}

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::SerializeShard(const MetadataShard& shard,
                                                  MsgpackPacker& packer) const {
    // MetadataShard format: map with "metadata" field
    packer.pack_map(1);

    // Serialize metadata
    packer.pack("metadata");
    size_t metadata_count = 0;
    for (const auto& [tenant_id, tenant_state] : shard.tenants) {
        metadata_count += tenant_state.metadata.size();
    }
    packer.pack_array(metadata_count);

    // Sort tenant/key pairs to ensure consistent serialization order.
    // NOTE: sort may be slow for large shards.
    struct SortedEntry {
        std::string tenant_id;
        std::string key;
        const ObjectMetadata* metadata;
    };
    std::vector<SortedEntry> sorted_entries;
    sorted_entries.reserve(metadata_count);
    for (const auto& [tenant_id, tenant_state] : shard.tenants) {
        for (const auto& [key, metadata] : tenant_state.metadata) {
            sorted_entries.push_back({tenant_id.value(), key, &metadata});
        }
    }
    std::sort(sorted_entries.begin(), sorted_entries.end(),
              [](const SortedEntry& lhs, const SortedEntry& rhs) {
                  if (lhs.tenant_id != rhs.tenant_id) {
                      return lhs.tenant_id < rhs.tenant_id;
                  }
                  return lhs.key < rhs.key;
              });

    for (const auto& entry : sorted_entries) {
        // Each metadata item format: [tenant_id, key, metadata_object].
        packer.pack_array(3);
        packer.pack(entry.tenant_id);
        packer.pack(entry.key);

        auto result = SerializeMetadata(*entry.metadata, packer);
        if (!result) {
            return tl::make_unexpected(SerializationError(
                result.error().code,
                fmt::format("Failed to serialize metadata for key '{}': {}",
                            entry.key, result.error().message)));
        }
    }

    return {};
}

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::DeserializeShard(const msgpack::object& obj,
                                                    MetadataShard& shard) {
    if (obj.type != msgpack::type::MAP) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL, "Invalid shard format: expected map"));
    }

    const msgpack::object* metadata_array = nullptr;

    // Extract fields from shard map
    for (uint32_t i = 0; i < obj.via.map.size; ++i) {
        const auto& key_obj = obj.via.map.ptr[i].key;
        if (key_obj.type == msgpack::type::STR) {
            std::string field_key(key_obj.via.str.ptr, key_obj.via.str.size);
            if (field_key == "metadata") {
                metadata_array = &obj.via.map.ptr[i].val;
            }
        }
    }

    // Clear existing data
    shard.tenants.clear();

    // Deserialize metadata
    if (metadata_array == nullptr ||
        metadata_array->type != msgpack::type::ARRAY) {
        return tl::make_unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "Missing or invalid 'metadata' field in shard"));
    }

    shard.tenants.reserve(metadata_array->via.array.size);

    for (uint32_t j = 0; j < metadata_array->via.array.size; ++j) {
        const msgpack::object& item = metadata_array->via.array.ptr[j];

        if (item.type != msgpack::type::ARRAY ||
            (item.via.array.size != 2 && item.via.array.size != 3)) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "Invalid metadata item format: expected [key, metadata] or "
                "[tenant_id, key, metadata]"));
        }

        TenantId tenant_id;
        std::string key;
        const msgpack::object* value_obj = nullptr;
        if (item.via.array.size == 2) {
            key = item.via.array.ptr[0].as<std::string>();
            value_obj = &item.via.array.ptr[1];
        } else {
            tenant_id = TenantId(item.via.array.ptr[0].as<std::string>());
            key = item.via.array.ptr[1].as<std::string>();
            value_obj = &item.via.array.ptr[2];
        }

        auto metadata_result = DeserializeMetadata(*value_obj);
        if (!metadata_result) {
            LOG(ERROR) << "Failed to deserialize metadata for key: " << key
                       << ": " << metadata_result.error().message;
            continue;
        }

        auto metadata_ptr = std::move(metadata_result.value());
        auto& tenant_state = shard.tenants[tenant_id];
        const std::string user_key = key;
        auto [it, inserted] = tenant_state.metadata.emplace(
            std::piecewise_construct, std::forward_as_tuple(std::move(key)),
            std::forward_as_tuple(
                metadata_ptr->client_id, metadata_ptr->put_start_time,
                metadata_ptr->size, metadata_ptr->PopReplicas(),
                metadata_ptr->soft_pin_timeout.has_value(),
                metadata_ptr->IsHardPinned(), metadata_ptr->data_type,
                metadata_ptr->group_id, tenant_id, user_key));

        it->second.lease_timeout = metadata_ptr->lease_timeout;
        it->second.soft_pin_timeout = metadata_ptr->soft_pin_timeout;
        it->second.object_checksum = metadata_ptr->object_checksum;

        // Recompute disk_object_count for restored metadata
        if (it->second.HasReplica([](const Replica& r) {
                return r.is_local_disk_replica() && r.is_completed();
            })) {
            shard.disk_object_count++;
        }
    }

    return {};
}

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::SerializeMetadata(
    const MasterService::ObjectMetadata& metadata,
    MsgpackPacker& packer) const {
    // Pack ObjectMetadata using array structure for efficiency
    // Format: [client_id, put_start_time, size, lease_timeout,
    // has_soft_pin_timeout, soft_pin_timeout, replicas_count, data_type,
    // replicas..., hard_pinned, group_id, object_checksum?]

    size_t array_size = 10;  // client_id, put_start_time, size, lease_timeout,
                             // has_soft_pin_timeout, soft_pin_timeout,
                             // replicas_count, data_type, hard_pinned, group_id
    array_size += metadata.CountReplicas();  // One element per replica
    if (metadata.object_checksum.has_value()) {
        ++array_size;
    }
    packer.pack_array(array_size);

    // Serialize client_id
    std::string client_id = UuidToString(metadata.client_id);
    packer.pack(client_id);

    // Serialize put_start_time (convert to timestamp)
    auto put_start_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                              metadata.put_start_time.time_since_epoch())
                              .count();
    packer.pack(put_start_time);

    // Serialize size
    packer.pack(static_cast<uint64_t>(metadata.size));

    // Serialize lease_timeout (convert to timestamp)
    auto lease_timestamp =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            metadata.lease_timeout.time_since_epoch())
            .count();
    packer.pack(lease_timestamp);

    // Serialize soft_pin_timeout (if exists)
    if (metadata.soft_pin_timeout.has_value()) {
        packer.pack(true);  // Mark soft_pin_timeout exists
        auto soft_pin_timestamp =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                metadata.soft_pin_timeout.value().time_since_epoch())
                .count();
        packer.pack(soft_pin_timestamp);
    } else {
        packer.pack(false);        // Mark soft_pin_timeout does not exist
        packer.pack(uint64_t(0));  // Placeholder
    }

    // Serialize replicas count
    packer.pack(static_cast<uint32_t>(metadata.CountReplicas()));

    // Serialize data_type
    packer.pack(static_cast<uint8_t>(metadata.data_type));

    // Serialize replicas
    for (const auto& replica : metadata.GetAllReplicas()) {
        auto result = Serializer<Replica>::serialize(
            replica, service_->segment_manager_.getView(), packer);
        if (!result) {
            return tl::unexpected(result.error());
        }
    }

    packer.pack(metadata.IsHardPinned());
    packer.pack(metadata.group_id);
    if (metadata.object_checksum.has_value()) {
        packer.pack(*metadata.object_checksum);
    }

    return {};
}

tl::expected<std::unique_ptr<MasterService::ObjectMetadata>, SerializationError>
MasterService::MetadataSerializer::DeserializeMetadata(
    const msgpack::object& obj) const {
    // Check if input is a valid array
    if (obj.type != msgpack::type::ARRAY) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "deserialize ObjectMetadata state is not an array"));
    }

    // Need at least 7 elements: client_id, put_start_time, size, lease_timeout,
    // has_soft_pin_timeout, soft_pin_timeout, replicas_count
    if (obj.via.array.size < 7) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "deserialize ObjectMetadata array size is too small"));
    }

    msgpack::object* array = obj.via.array.ptr;
    uint32_t index = 0;

    // Deserialize client_id string
    std::string client_id_str = array[index++].as<std::string>();
    UUID client_id;
    if (!StringToUuid(client_id_str, client_id)) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize ObjectMetadata invalid client_id UUID: {}",
                        client_id_str)));
    }

    // Deserialize put_start_time
    uint64_t put_start_time_timestamp = array[index++].as<uint64_t>();

    // Deserialize size
    auto size = static_cast<size_t>(array[index++].as<uint64_t>());

    // Deserialize lease_timeout
    uint64_t lease_timestamp = array[index++].as<uint64_t>();

    // Deserialize soft_pin_timeout flag
    bool has_soft_pin_timeout = array[index++].as<bool>();

    // Deserialize soft_pin_timeout value
    uint64_t soft_pin_timestamp = array[index++].as<uint64_t>();

    // Deserialize replicas count
    uint32_t replicas_count = array[index++].as<uint32_t>();

    // Format detection (decode optional fields by type for back-compat):
    //   v1: 7 + replicas_count, no optional fields
    //   v2: 8 + replicas_count, either data_type or hard_pinned
    //   v3: 9 + replicas_count, data_type + hard_pinned or hard_pinned +
    //   group_id v4: 10 + replicas_count, data_type + hard_pinned + group_id
    //   v5: 11 + replicas_count, v4 + object_checksum
    // 64-bit arithmetic keeps an attacker-controlled near-UINT32_MAX
    // replicas_count from wrapping the bounds and slipping an out-of-bounds
    // index past the size check.
    constexpr uint64_t kBaseFieldCount = 7;
    constexpr uint64_t kMaxOptionalFieldCount = 4;
    const uint64_t total_elements = obj.via.array.size;
    const uint64_t min_elements = kBaseFieldCount + replicas_count;
    if (total_elements < min_elements ||
        total_elements > min_elements + kMaxOptionalFieldCount) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "deserialize ObjectMetadata array size mismatch"));
    }

    ObjectDataType data_type = ObjectDataType::UNKNOWN;
    if (index < total_elements &&
        array[index].type == msgpack::type::POSITIVE_INTEGER) {
        data_type = static_cast<ObjectDataType>(array[index++].as<uint8_t>());
    }

    // Deserialize replicas
    std::vector<Replica> replicas;
    replicas.reserve(replicas_count);

    for (uint32_t i = 0; i < replicas_count; i++) {
        // Defensive bound: the data_type skip above can consume a slot the
        // size check counted on, so a crafted entry whose first post-count
        // field looks like a data_type could otherwise read past the array.
        // Mirrors the standby reader in catalog_backed_snapshot_provider.cpp.
        if (index >= total_elements) {
            return tl::unexpected(
                SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                   "deserialize ObjectMetadata truncated"));
        }
        auto result = Serializer<Replica>::deserialize(
            array[index++], service_->segment_manager_.getView());
        if (!result) {
            return tl::unexpected(result.error());
        }
        replicas.emplace_back(std::move(*result.value()));
    }

    // Deserialize hard_pinned (if present, otherwise default to false)
    bool is_hard_pinned = false;
    if (index < obj.via.array.size &&
        array[index].type == msgpack::type::BOOLEAN) {
        is_hard_pinned = array[index++].as<bool>();
    }

    std::string group_id;
    if (index < obj.via.array.size && array[index].type == msgpack::type::STR) {
        group_id = array[index++].as<std::string>();
    }

    std::optional<uint64_t> object_checksum;
    if (index < total_elements &&
        array[index].type == msgpack::type::POSITIVE_INTEGER) {
        object_checksum = array[index++].as<uint64_t>();
    }
    if (index != total_elements) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "deserialize ObjectMetadata optional field type mismatch"));
    }

    // Create ObjectMetadata instance
    bool enable_soft_pin = has_soft_pin_timeout;
    auto metadata = std::make_unique<ObjectMetadata>(
        client_id,
        std::chrono::system_clock::time_point(
            std::chrono::milliseconds(put_start_time_timestamp)),
        size, std::move(replicas), enable_soft_pin, is_hard_pinned, data_type,
        group_id);
    metadata->object_checksum = object_checksum;
    metadata->lease_timeout = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(lease_timestamp));

    // Set soft_pin_timeout (if exists)
    if (has_soft_pin_timeout) {
        metadata->soft_pin_timeout.emplace(
            std::chrono::system_clock::time_point(
                std::chrono::milliseconds(soft_pin_timestamp)));
    }

    return metadata;
}

tl::expected<UUID, ErrorCode> MasterService::CreateCopyTask(
    const std::string& key, const TenantId& tenant_id,
    const std::vector<std::string>& targets) {
    auto normalized_tenant_result = ResolveTenantIdForWrite(tenant_id);
    if (!normalized_tenant_result) {
        return tl::make_unexpected(normalized_tenant_result.error());
    }
    const ObjectIdentity object_id{std::move(normalized_tenant_result.value()),
                                   key};
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    if (targets.empty()) {
        LOG(ERROR) << "key=" << key << ", error=empty_targets";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    MetadataAccessorRO accessor(this, object_id);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    ScopedSegmentAccess segment_accessor = segment_manager_.getSegmentAccess();
    for (const auto& target : targets) {
        if (!segment_accessor.ExistsSegmentName(target)) {
            LOG(ERROR) << "key=" << key << ", target_segment=" << target
                       << ", error=target_segment_not_mounted";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (!segment_accessor.IsSegmentAllocatable(target)) {
            LOG(ERROR) << "key=" << key << ", target_segment=" << target
                       << ", error=target_segment_not_allocatable";
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
    }

    const auto& metadata = accessor.Get();
    const auto& segment_names = metadata.GetReplicaSegmentNames();
    if (segment_names.empty()) {
        LOG(ERROR) << "key=" << key << ", error=no_valid_source_replicas";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    // Randomly pick a segment from the source replicas
    std::string selected_source_segment =
        segment_names[randomIndex(segment_names.size())];
    UUID select_client;
    ErrorCode error = segment_accessor.GetClientIdBySegmentName(
        selected_source_segment, select_client);
    if (error != ErrorCode::OK) {
        LOG(ERROR) << "key=" << key
                   << ", segment_name=" << selected_source_segment
                   << ", error=client_id_not_found";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return task_manager_.get_write_access()
        .submit_task_typed<TaskType::REPLICA_COPY>(
            select_client, {.tenant_id = object_id.tenant_id.value(),
                            .key = object_id.user_key,
                            .source = selected_source_segment,
                            .targets = targets});
}

tl::expected<UUID, ErrorCode> MasterService::CreateMoveTask(
    const std::string& key, const TenantId& tenant_id,
    const std::string& source, const std::string& target) {
    auto normalized_tenant_result = ResolveTenantIdForWrite(tenant_id);
    if (!normalized_tenant_result) {
        return tl::make_unexpected(normalized_tenant_result.error());
    }
    const ObjectIdentity object_id{std::move(normalized_tenant_result.value()),
                                   key};
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRO accessor(this, object_id);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (source == target) {
        LOG(ERROR) << "key=" << key << ", source_segment=" << source
                   << ", target_segment=" << target
                   << ", error=source_target_segments_are_same";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    ScopedSegmentAccess segment_accessor = segment_manager_.getSegmentAccess();
    if (!segment_accessor.ExistsSegmentName(target)) {
        LOG(ERROR) << "key=" << key << ", target_segment=" << target
                   << ", error=target_segment_not_mounted";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!segment_accessor.IsSegmentAllocatable(target)) {
        LOG(ERROR) << "key=" << key << ", target_segment=" << target
                   << ", error=target_segment_not_allocatable";
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }

    const auto& metadata = accessor.Get();
    const auto& segment_names = metadata.GetReplicaSegmentNames();
    if (std::find(segment_names.begin(), segment_names.end(), source) ==
        segment_names.end()) {
        LOG(ERROR) << "key=" << key << ", source_segment=" << source
                   << ", error=source_segment_not_found";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    UUID select_client;
    ErrorCode error =
        segment_accessor.GetClientIdBySegmentName(source, select_client);

    if (error != ErrorCode::OK) {
        LOG(ERROR) << "key=" << key << ", segment_name=" << source
                   << ", error=client_id_not_found";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    return task_manager_.get_write_access()
        .submit_task_typed<TaskType::REPLICA_MOVE>(
            select_client, {.tenant_id = object_id.tenant_id.value(),
                            .key = object_id.user_key,
                            .source = source,
                            .target = target});
}

tl::expected<QueryTaskResponse, ErrorCode> MasterService::QueryTask(
    const UUID& task_id) {
    const auto& task_option =
        task_manager_.get_read_access().find_task_by_id(task_id);
    if (!task_option.has_value()) {
        LOG(ERROR) << "task_id=" << task_id << ", error=task_not_found";
        return tl::make_unexpected(ErrorCode::TASK_NOT_FOUND);
    }
    return QueryTaskResponse(task_option.value());
}

tl::expected<std::vector<TaskAssignment>, ErrorCode> MasterService::FetchTasks(
    const UUID& client_id, size_t batch_size) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto& tasks =
        task_manager_.get_write_access().pop_tasks(client_id, batch_size);
    std::vector<TaskAssignment> assignments;
    for (const auto& task : tasks) {
        assignments.emplace_back(task);
    }
    return assignments;
}

tl::expected<void, ErrorCode> MasterService::MarkTaskToComplete(
    const UUID& client_id, const TaskCompleteRequest& request) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    auto write_access = task_manager_.get_write_access();
    ErrorCode err = write_access.complete_task(client_id, request.id,
                                               request.status, request.message);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "task_id=" << request.id
                   << ", error=complete_task_failed";
        return tl::make_unexpected(err);
    }
    return {};
}

tl::expected<void, ErrorCode> MasterService::ValidateDrainRequest(
    const CreateDrainJobRequest& request) {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    return ValidateDrainRequestLocked(segment_access, request);
}

tl::expected<void, ErrorCode> MasterService::ValidateDrainRequestLocked(
    ScopedSegmentAccess& segment_access, const CreateDrainJobRequest& request) {
    if (request.segments.empty() || request.max_concurrency == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::unordered_set<std::string> unique_segments(request.segments.begin(),
                                                    request.segments.end());
    if (unique_segments.size() != request.segments.size()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    for (const auto& segment_name : request.segments) {
        if (!segment_access.ExistsSegmentName(segment_name)) {
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        SegmentStatus status = SegmentStatus::UNDEFINED;
        auto err = segment_access.GetSegmentStatusByName(segment_name, status);
        if (err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }
        if (status != SegmentStatus::OK) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
    }

    for (const auto& target_segment : request.target_segments) {
        if (unique_segments.contains(target_segment)) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (!segment_access.ExistsSegmentName(target_segment)) {
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        if (!segment_access.IsSegmentAllocatable(target_segment)) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
    }
    return {};
}

tl::expected<UUID, ErrorCode> MasterService::CreateDrainJob(
    const CreateDrainJobRequest& request) {
    std::vector<std::string> draining_segments;
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        auto valid = ValidateDrainRequestLocked(segment_access, request);
        if (!valid.has_value()) {
            return tl::make_unexpected(valid.error());
        }

        draining_segments.reserve(request.segments.size());
        for (const auto& segment_name : request.segments) {
            auto err = segment_access.SetSegmentStatusByName(
                segment_name, SegmentStatus::DRAINING);
            if (err != ErrorCode::OK) {
                for (const auto& updated_segment : draining_segments) {
                    (void)segment_access.SetSegmentStatusByName(
                        updated_segment, SegmentStatus::OK);
                }
                return tl::make_unexpected(err);
            }
            draining_segments.push_back(segment_name);
        }
    }

    auto job = std::make_shared<DrainJob>();
    job->id = generate_uuid();
    job->request = request;
    job->created_at = std::chrono::system_clock::now();
    job->last_updated_at = job->created_at;
    job->status = JobStatus::CREATED;
    job->message = "Drain job created";

    {
        std::lock_guard<std::mutex> lock(job_mutex_);
        drain_jobs_.emplace(job->id, job);
    }

    return job->id;
}

tl::expected<QueryJobResponse, ErrorCode> MasterService::QueryDrainJob(
    const UUID& job_id) {
    std::shared_ptr<DrainJob> job;
    {
        std::lock_guard<std::mutex> lock(job_mutex_);
        auto it = drain_jobs_.find(job_id);
        if (it == drain_jobs_.end()) {
            return tl::make_unexpected(ErrorCode::JOB_NOT_FOUND);
        }
        job = it->second;
    }

    std::lock_guard<std::mutex> job_lock(job->mutex);
    QueryJobResponse response;
    response.id = job->id;
    response.type = job->type;
    response.status = job->status;
    response.created_at_ms_epoch = static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            job->created_at.time_since_epoch())
            .count());
    response.last_updated_at_ms_epoch = static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            job->last_updated_at.time_since_epoch())
            .count());
    response.segments = job->request.segments;
    response.succeeded_units = job->succeeded_units;
    response.failed_units = job->failed_units;
    response.blocked_units = job->blocked_units;
    response.active_units = static_cast<uint64_t>(job->active_tasks.size());
    response.migrated_bytes = job->migrated_bytes;
    response.message = job->message;
    return response;
}

tl::expected<void, ErrorCode> MasterService::CancelDrainJob(
    const UUID& job_id) {
    std::shared_ptr<DrainJob> job;
    {
        std::lock_guard<std::mutex> lock(job_mutex_);
        auto it = drain_jobs_.find(job_id);
        if (it == drain_jobs_.end()) {
            return tl::make_unexpected(ErrorCode::JOB_NOT_FOUND);
        }
        job = it->second;
    }

    std::vector<std::string> segments_to_restore;
    {
        std::lock_guard<std::mutex> job_lock(job->mutex);
        if (job->status == JobStatus::SUCCEEDED ||
            job->status == JobStatus::FAILED ||
            job->status == JobStatus::CANCELED || !job->active_tasks.empty()) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }

        job->status = JobStatus::CANCELED;
        job->last_updated_at = std::chrono::system_clock::now();
        job->message = "Drain job canceled";
        segments_to_restore = job->request.segments;
    }

    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    for (const auto& segment_name : segments_to_restore) {
        SegmentStatus status = SegmentStatus::UNDEFINED;
        if (segment_access.GetSegmentStatusByName(segment_name, status) ==
                ErrorCode::OK &&
            status != SegmentStatus::UNMOUNTING) {
            (void)segment_access.SetSegmentStatusByName(segment_name,
                                                        SegmentStatus::OK);
        }
    }
    return {};
}

std::string MasterService::MakeDrainUnitKey(
    const TenantId& tenant_id, const std::string& key,
    const std::string& source_segment) const {
    return std::to_string(tenant_id.value().size()) + ":" + tenant_id.value() +
           ":" + std::to_string(key.size()) + ":" + key + ":" + source_segment;
}

std::optional<std::string> MasterService::SelectDrainTargetForKey(
    const ObjectMetadata& metadata, const std::string& source_segment,
    const std::vector<std::string>& requested_targets) {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    std::vector<std::string> candidate_segments = requested_targets;
    if (candidate_segments.empty()) {
        auto err = segment_access.GetAllSegments(candidate_segments);
        if (err != ErrorCode::OK) {
            return std::nullopt;
        }
    }

    const auto existing_segments = metadata.GetReplicaSegmentNames();
    double best_util = std::numeric_limits<double>::max();
    std::optional<std::string> best_target;
    for (const auto& candidate : candidate_segments) {
        if (candidate == source_segment) {
            continue;
        }
        if (std::find(existing_segments.begin(), existing_segments.end(),
                      candidate) != existing_segments.end()) {
            continue;
        }
        if (!segment_access.IsSegmentAllocatable(candidate)) {
            continue;
        }
        size_t used = 0, capacity = 0;
        if (segment_access.QuerySegments(candidate, used, capacity) !=
                ErrorCode::OK ||
            capacity == 0) {
            continue;
        }
        const double util =
            static_cast<double>(used) / static_cast<double>(capacity);
        if (util < best_util) {
            best_util = util;
            best_target = candidate;
        }
    }
    return best_target;
}

void MasterService::RefreshDrainJobTasks(DrainJob& job) {
    auto read_access = task_manager_.get_read_access();
    std::vector<UUID> finished_task_ids;
    finished_task_ids.reserve(job.active_tasks.size());

    for (const auto& [task_id, active_task] : job.active_tasks) {
        auto task_opt = read_access.find_task_by_id(task_id);
        if (!task_opt.has_value()) {
            finished_task_ids.push_back(task_id);
            job.failed_units++;
            job.terminal_failed_unit_keys.insert(active_task.unit_key);
            continue;
        }
        if (!task_opt->is_finished()) {
            continue;
        }

        finished_task_ids.push_back(task_id);
        if (task_opt->status == TaskStatus::SUCCESS) {
            job.succeeded_units++;
            job.migrated_bytes += active_task.bytes;
            job.completed_unit_keys.insert(active_task.unit_key);
        } else {
            job.failed_units++;
            auto& retry_count = job.retry_counts[active_task.unit_key];
            retry_count++;
            if (retry_count >= kMaxDrainUnitRetries) {
                job.terminal_failed_unit_keys.insert(active_task.unit_key);
            }
        }
    }

    for (const auto& task_id : finished_task_ids) {
        job.active_tasks.erase(task_id);
    }
}

void MasterService::ScheduleDrainJobTasks(DrainJob& job) {
    if (job.status == JobStatus::CREATED) {
        job.status = JobStatus::PLANNING;
    }

    const uint32_t max_concurrency =
        std::max<uint32_t>(1, job.request.max_concurrency);
    if (job.active_tasks.size() >= max_concurrency) {
        job.status = JobStatus::RUNNING;
        return;
    }

    struct DrainPlan {
        TenantId tenant_id;
        std::string key;
        std::string source_segment;
        std::string target_segment;
        size_t bytes;
        std::string unit_key;
    };

    const size_t slots = max_concurrency - job.active_tasks.size();
    std::vector<DrainPlan> plans;
    plans.reserve(slots);
    std::unordered_set<std::string> active_unit_keys;
    for (const auto& [_, task] : job.active_tasks) {
        active_unit_keys.insert(task.unit_key);
    }

    std::unordered_set<std::string> blocked_unit_keys;
    {
        std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
        for (size_t i = 0; i < kNumShards; ++i) {
            MetadataShardAccessorRO shard(this, i);
            for (const auto& [tenant_id, tenant_state] : shard->tenants) {
                for (const auto& [key, metadata] : tenant_state.metadata) {
                    for (const auto& source_segment : job.request.segments) {
                        const auto unit_key =
                            MakeDrainUnitKey(tenant_id, key, source_segment);
                        if (job.completed_unit_keys.contains(unit_key) ||
                            active_unit_keys.contains(unit_key) ||
                            job.terminal_failed_unit_keys.contains(unit_key)) {
                            continue;
                        }

                        const auto replica_segments =
                            metadata.GetReplicaSegmentNames();
                        if (std::find(replica_segments.begin(),
                                      replica_segments.end(), source_segment) ==
                            replica_segments.end()) {
                            continue;
                        }

                        if (metadata.IsHardPinned() ||
                            !metadata.IsLeaseExpired() ||
                            !metadata.AllReplicas(&Replica::fn_is_completed) ||
                            tenant_state.replication_tasks.contains(key)) {
                            blocked_unit_keys.insert(unit_key);
                            continue;
                        }

                        auto target = SelectDrainTargetForKey(
                            metadata, source_segment,
                            job.request.target_segments);
                        if (!target.has_value()) {
                            blocked_unit_keys.insert(unit_key);
                            continue;
                        }

                        if (plans.size() < slots) {
                            plans.push_back({tenant_id, key, source_segment,
                                             *target, metadata.size, unit_key});
                        }
                    }
                }
            }
        }
    }

    job.blocked_units = blocked_unit_keys.size();

    for (const auto& plan : plans) {
        auto task_id = CreateMoveTask(plan.key, plan.tenant_id,
                                      plan.source_segment, plan.target_segment);
        if (task_id.has_value()) {
            ActiveDrainTask active_task;
            active_task.task_id = task_id.value();
            active_task.tenant_id = plan.tenant_id;
            active_task.key = plan.key;
            active_task.source_segment = plan.source_segment;
            active_task.target_segment = plan.target_segment;
            active_task.bytes = plan.bytes;
            active_task.unit_key = plan.unit_key;
            job.active_tasks.emplace(task_id.value(), std::move(active_task));
        } else if (task_id.error() == ErrorCode::NO_AVAILABLE_HANDLE ||
                   task_id.error() ==
                       ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS ||
                   task_id.error() == ErrorCode::OBJECT_HAS_REPLICATION_TASK) {
            job.blocked_units++;
        } else {
            job.failed_units++;
            auto& retry_count = job.retry_counts[plan.unit_key];
            retry_count++;
            if (retry_count >= kMaxDrainUnitRetries) {
                job.terminal_failed_unit_keys.insert(plan.unit_key);
            }
        }
    }

    job.status = JobStatus::RUNNING;
    job.last_updated_at = std::chrono::system_clock::now();
    job.message = "Drain job running";
}

bool MasterService::MaybeCompleteDrainJob(DrainJob& job) {
    if (!job.active_tasks.empty()) {
        return false;
    }

    std::unordered_set<std::string> remaining_segments;
    std::unordered_set<std::string> remaining_unit_keys;
    {
        std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
        for (size_t i = 0; i < kNumShards; ++i) {
            MetadataShardAccessorRO shard(this, i);
            for (const auto& [tenant_id, tenant_state] : shard->tenants) {
                for (const auto& [key, metadata] : tenant_state.metadata) {
                    const auto replica_segments =
                        metadata.GetReplicaSegmentNames();
                    for (const auto& source_segment : job.request.segments) {
                        if (std::find(replica_segments.begin(),
                                      replica_segments.end(), source_segment) !=
                            replica_segments.end()) {
                            remaining_segments.insert(source_segment);
                            remaining_unit_keys.insert(MakeDrainUnitKey(
                                tenant_id, key, source_segment));
                        }
                    }
                }
            }
        }
    }

    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        for (const auto& segment_name : job.request.segments) {
            if (!remaining_segments.contains(segment_name)) {
                (void)segment_access.SetSegmentStatusByName(
                    segment_name, SegmentStatus::DRAINED);
            }
        }
    }

    if (remaining_segments.empty()) {
        job.status = JobStatus::SUCCEEDED;
        job.last_updated_at = std::chrono::system_clock::now();
        job.message = "Drain job finished successfully";
        return true;
    }

    bool all_remaining_terminal_failed = !remaining_unit_keys.empty();
    for (const auto& unit_key : remaining_unit_keys) {
        if (!job.terminal_failed_unit_keys.contains(unit_key)) {
            all_remaining_terminal_failed = false;
            break;
        }
    }
    if (!all_remaining_terminal_failed) {
        return false;
    }

    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        for (const auto& segment_name : job.request.segments) {
            SegmentStatus status = SegmentStatus::UNDEFINED;
            if (segment_access.GetSegmentStatusByName(segment_name, status) ==
                    ErrorCode::OK &&
                status != SegmentStatus::UNMOUNTING) {
                (void)segment_access.SetSegmentStatusByName(segment_name,
                                                            SegmentStatus::OK);
            }
        }
    }

    job.status = JobStatus::FAILED;
    job.last_updated_at = std::chrono::system_clock::now();
    job.message = "Drain job failed: unrecoverable units remain";
    return true;
}

void MasterService::ProcessDrainJobs() {
    std::vector<std::shared_ptr<DrainJob>> jobs;
    {
        std::lock_guard<std::mutex> lock(job_mutex_);
        jobs.reserve(drain_jobs_.size());
        for (const auto& [_, job] : drain_jobs_) {
            jobs.push_back(job);
        }
    }

    for (const auto& job : jobs) {
        if (!job) {
            continue;
        }
        std::lock_guard<std::mutex> job_lock(job->mutex);
        if (job->status == JobStatus::SUCCEEDED ||
            job->status == JobStatus::FAILED ||
            job->status == JobStatus::CANCELED) {
            continue;
        }
        RefreshDrainJobTasks(*job);
        if (MaybeCompleteDrainJob(*job)) {
            continue;
        }
        ScheduleDrainJobTasks(*job);
    }
}

void MasterService::JobDispatchThreadFunc() {
    while (job_dispatch_running_) {
        ProcessDrainJobs();
        std::this_thread::sleep_for(
            std::chrono::milliseconds(kJobDispatchThreadSleepMs));
    }
}

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::SerializeDiscardedReplicas(
    MsgpackPacker& packer) const {
    std::lock_guard lock(service_->discarded_replicas_mutex_);

    // Serialize as array: [count, item1, item2, ...]
    packer.pack_array(service_->discarded_replicas_.size());

    for (const auto& item : service_->discarded_replicas_) {
        // Each item: [ttl_timestamp, mem_size, replica_count, replica1,
        // replica2, ...]
        auto ttl_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          item.ttl_.time_since_epoch())
                          .count();

        packer.pack_array(3 + item.replicas_.size());
        packer.pack(ttl_ms);          // ttl timestamp
        packer.pack(item.mem_size_);  // mem_size
        packer.pack(
            static_cast<uint32_t>(item.replicas_.size()));  // replica count

        // Serialize each replica
        for (const auto& replica : item.replicas_) {
            auto result = Serializer<Replica>::serialize(
                replica, service_->segment_manager_.getView(), packer);
            if (!result) {
                return tl::unexpected(result.error());
            }
        }
    }

    return {};
}

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::DeserializeDiscardedReplicas(
    const msgpack::object& obj) {
    if (obj.type != msgpack::type::ARRAY) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL, "discarded_replicas: expected array"));
    }

    std::list<DiscardedReplicas> temp_list;

    for (uint32_t i = 0; i < obj.via.array.size; ++i) {
        const msgpack::object& item_obj = obj.via.array.ptr[i];

        if (item_obj.type != msgpack::type::ARRAY ||
            item_obj.via.array.size < 3) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format("Invalid discarded_replicas item at index {}: "
                            "expected array with at least 3 elements",
                            i)));
        }

        const msgpack::object* item_array = item_obj.via.array.ptr;

        // Deserialize ttl
        uint64_t ttl_ms = item_array[0].as<uint64_t>();
        auto ttl = std::chrono::system_clock::time_point(
            std::chrono::milliseconds(ttl_ms));

        // Deserialize mem_size
        uint64_t mem_size = item_array[1].as<uint64_t>();

        // Deserialize replica count
        uint32_t replica_count = item_array[2].as<uint32_t>();

        if (item_obj.via.array.size != 3 + replica_count) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format(
                    "Discarded replicas item size mismatch at index {}: "
                    "expected {} elements, got {}",
                    i, 3 + replica_count, item_obj.via.array.size)));
        }

        // Deserialize replicas
        std::vector<Replica> replicas;
        replicas.reserve(replica_count);

        for (uint32_t j = 0; j < replica_count; ++j) {
            auto replica_result = Serializer<Replica>::deserialize(
                item_array[3 + j], service_->segment_manager_.getView());
            if (!replica_result) {
                return tl::make_unexpected(SerializationError(
                    ErrorCode::DESERIALIZE_FAIL,
                    fmt::format("Failed to deserialize replica {} in "
                                "discarded_replicas item {}: {}",
                                j, i, replica_result.error().message)));
            }
            replicas.emplace_back(std::move(*replica_result.value()));
        }

        // Create DiscardedReplicas and manually set mem_size_
        temp_list.emplace_back(std::move(replicas), ttl);
        // Set the deserialized mem_size
        temp_list.back().mem_size_ = mem_size;
    }

    // Move deserialized items to service's discarded_replicas_
    if (!temp_list.empty()) {
        std::lock_guard lock(service_->discarded_replicas_mutex_);
        service_->discarded_replicas_ = std::move(temp_list);
    }

    return {};
}

KvEventConfig MasterService::BuildKvEventConfig(
    const MasterServiceConfig& config) {
    KvEventConfig kv_config;
    kv_config.enabled = config.enable_kv_events;
    kv_config.bind_endpoint = config.kv_events_bind_endpoint;
    kv_config.model_name = config.kv_events_model_name;
    kv_config.backend_id = config.kv_events_backend_id;
    kv_config.tenant_id = config.kv_events_tenant_id;
    kv_config.additional_salt = config.kv_events_additional_salt;
    kv_config.lora_name = config.kv_events_lora_name;
    kv_config.block_size = config.kv_events_block_size;
    kv_config.dp_rank = config.kv_events_dp_rank;
    kv_config.emit_legacy_compat_fields = config.kv_events_emit_legacy_compat;
    kv_config.emit_object_key = config.kv_events_emit_object_key;
    kv_config.queue_capacity = config.kv_events_queue_capacity;
    return kv_config;
}

std::string MasterService::MediumForReplicaType(ReplicaType replica_type) {
    switch (replica_type) {
        case ReplicaType::MEMORY:
            return "cpu";
        case ReplicaType::DISK:
        case ReplicaType::LOCAL_DISK:
        case ReplicaType::NOF_SSD:
            return "disk";
        case ReplicaType::ALL:
        default:
            return "cpu";
    }
}

std::string MasterService::MediumForMetadata(const ObjectMetadata& metadata) {
    if (metadata.HasMemReplica()) {
        return "cpu";
    }
    if (metadata.HasReplica(&Replica::fn_is_nof_replica) ||
        metadata.HasReplica(&Replica::fn_is_disk_replica) ||
        metadata.HasReplica(&Replica::fn_is_local_disk_replica)) {
        return "disk";
    }
    return "cpu";
}

void MasterService::PublishKvStored(const std::string& key,
                                    ReplicaType replica_type,
                                    const ObjectMetadata& metadata,
                                    const TenantId& tenant_id) {
    if (!kv_event_publisher_ || !kv_event_publisher_->enabled()) {
        return;
    }
    std::string medium = MediumForReplicaType(replica_type);
    if (replica_type == ReplicaType::ALL) {
        medium = MediumForMetadata(metadata);
    }
    kv_event_publisher_->PublishStored(key, medium, tenant_id,
                                       metadata.group_id);
}

void MasterService::PublishKvRemoved(const std::string& key,
                                     const std::string& medium,
                                     const TenantId& tenant_id,
                                     const std::string& group_id) {
    if (!kv_event_publisher_ || !kv_event_publisher_->enabled()) {
        return;
    }
    kv_event_publisher_->PublishRemoved(key, medium, tenant_id, group_id);
}

void MasterService::PublishKvRemoved(const std::string& key,
                                     const ObjectMetadata& metadata,
                                     const TenantId& tenant_id) {
    PublishKvRemoved(key, MediumForMetadata(metadata), tenant_id,
                     metadata.group_id);
}

void MasterService::PublishKvRemovedAfterEvict(const std::string& key,
                                               uint64_t freed_bytes,
                                               const std::string& medium,
                                               const ObjectMetadata& metadata,
                                               const TenantId& tenant_id) {
    (void)freed_bytes;
    (void)medium;
    if (!kv_event_publisher_ || !kv_event_publisher_->enabled()) {
        return;
    }
    if (!metadata.IsValid()) {
        PublishKvRemoved(key, metadata, tenant_id);
    }
}

bool MasterService::KvEventsEnabled() const {
    return kv_event_publisher_ && kv_event_publisher_->enabled();
}

KvEventPublisher::Stats MasterService::GetKvEventStats() const {
    if (!kv_event_publisher_) {
        return {};
    }
    return kv_event_publisher_->GetStats();
}

void MasterService::setHttpMetadataServer(HttpMetadataServer* server) {
    http_metadata_server_ = server;
    if (server) {
        LOG(INFO) << "HTTP metadata cleanup on client timeout: enabled "
                     "(co-located metadata server)";
    }
}

void MasterService::setHttpMetadataRemoteUrl(
    const std::string& metadata_connstring) {
#ifdef USE_HTTP
    // Only http(s) is supported; guard the scheme to avoid
    // MetadataStoragePlugin::Create()'s LOG(FATAL) on other backends.
    if (metadata_connstring.rfind("http://", 0) == 0 ||
        metadata_connstring.rfind("https://", 0) == 0) {
        try {
            http_metadata_remote_ =
                MetadataStoragePlugin::Create(metadata_connstring);
            LOG(INFO) << "HTTP metadata cleanup on client timeout: enabled "
                         "(remote metadata server "
                      << metadata_connstring << ")";
            // Start async cleanup worker now that http_metadata_remote_ is
            // ready
            http_metadata_cleanup_running_ = true;
            http_metadata_cleanup_thread_ = std::thread(
                &MasterService::HttpMetadataCleanupThreadFunc, this);
            LOG(INFO) << "HTTP metadata cleanup worker thread started";
        } catch (const std::exception& e) {
            LOG(WARNING) << "Failed to initialize remote HTTP metadata client "
                            "for "
                         << metadata_connstring << ": " << e.what()
                         << ". Metadata cleanup on timeout disabled.";
            http_metadata_remote_.reset();
        }
        return;
    }
    LOG(WARNING) << "enable_metadata_cleanup_on_timeout is set but the "
                    "configured metadata server '"
                 << metadata_connstring
                 << "' is not an HTTP endpoint; remote cleanup currently "
                    "supports only http(s). Metadata cleanup on timeout "
                    "disabled.";
#else
    (void)metadata_connstring;
    LOG(WARNING) << "enable_metadata_cleanup_on_timeout is set but this build "
                    "has no HTTP metadata support (USE_HTTP=OFF); metadata "
                    "cleanup on timeout disabled.";
#endif
}

void MasterService::cleanupHttpMetadata(const std::string& segment_name) {
    // Co-located: remove in-process, safe to run inline (no network I/O).
    if (http_metadata_server_) {
        const std::string ram_key =
            http_metadata_prefix_ + "ram/" + segment_name;
        const std::string rpc_key =
            http_metadata_prefix_ + "rpc_meta/" + segment_name;
        bool ram_removed = http_metadata_server_->removeKey(ram_key);
        bool rpc_removed = http_metadata_server_->removeKey(rpc_key);
        LOG(INFO) << "Cleaned up HTTP metadata for segment: " << segment_name
                  << ", ram_key_removed=" << ram_removed
                  << ", rpc_key_removed=" << rpc_removed;
        return;
    }

    // Separately-deployed: enqueue for async cleanup so a slow/unreachable
    // server never blocks the client monitor thread.
    if (http_metadata_remote_) {
        {
            std::lock_guard<std::mutex> lk(http_metadata_cleanup_mutex_);
            http_metadata_cleanup_queue_.push_back(segment_name);
        }
        http_metadata_cleanup_cv_.notify_one();
        return;
    }

    // Neither configured: cleanup is disabled, nothing to do.
}

void MasterService::HttpMetadataCleanupThreadFunc() {
    LOG(INFO) << "HTTP metadata cleanup worker started";
    while (http_metadata_cleanup_running_) {
        std::vector<std::string> batch;
        {
            std::unique_lock<std::mutex> lk(http_metadata_cleanup_mutex_);
            http_metadata_cleanup_cv_.wait(lk, [&] {
                return !http_metadata_cleanup_queue_.empty() ||
                       !http_metadata_cleanup_running_.load();
            });
            if (!http_metadata_cleanup_running_ &&
                http_metadata_cleanup_queue_.empty()) {
                break;
            }
            batch.swap(http_metadata_cleanup_queue_);
        }

        for (const auto& segment_name : batch) {
            const std::string ram_key =
                http_metadata_prefix_ + "ram/" + segment_name;
            const std::string rpc_key =
                http_metadata_prefix_ + "rpc_meta/" + segment_name;

            // Each key attempted independently so one failure does not
            // prevent cleanup of the other.
            bool ram_removed = false;
            bool rpc_removed = false;
            try {
                ram_removed = http_metadata_remote_->remove(ram_key);
            } catch (const std::exception& e) {
                LOG(WARNING)
                    << "Remote HTTP metadata cleanup failed for ram_key: "
                    << ram_key << ": " << e.what();
            }
            try {
                rpc_removed = http_metadata_remote_->remove(rpc_key);
            } catch (const std::exception& e) {
                LOG(WARNING)
                    << "Remote HTTP metadata cleanup failed for rpc_key: "
                    << rpc_key << ": " << e.what();
            }
            LOG(INFO) << "Cleaned up remote HTTP metadata for segment: "
                      << segment_name << ", ram_key_removed=" << ram_removed
                      << ", rpc_key_removed=" << rpc_removed;
        }
    }
    LOG(INFO) << "HTTP metadata cleanup worker stopped";
}

std::string MasterService::SerializeMetadataForOpLog(
    const ObjectMetadata& metadata) const {
    MetadataPayload payload;
    payload.client_id = metadata.client_id;
    payload.size = metadata.size;
    payload.group_id = metadata.group_id;
    payload.data_type = metadata.data_type;

    // Extract replica descriptors - get them all at once
    const auto& replicas = metadata.GetAllReplicas();
    payload.replicas.reserve(replicas.size());
    for (const auto& replica : replicas) {
        payload.replicas.push_back(replica.get_descriptor());
    }

    // NOTE: Lease information is NOT serialized because:
    // 1. Standby does not perform eviction, so lease info is not used
    // 2. After promotion, new Primary should grant fresh leases, not restore
    // old ones

    // Serialize using struct_pack (msgpack binary format)
    auto result = struct_pack::serialize(payload);
    return std::string(result.begin(), result.end());
}

std::string MasterService::SerializeMetadataForOpLogWithoutMemReplicas(
    const ObjectMetadata& metadata) const {
    MetadataPayload payload;
    payload.client_id = metadata.client_id;
    payload.size = metadata.size;
    payload.group_id = metadata.group_id;
    payload.data_type = metadata.data_type;

    const auto& replicas = metadata.GetAllReplicas();
    payload.replicas.reserve(replicas.size());
    for (const auto& replica : replicas) {
        if (replica.type() == ReplicaType::MEMORY) {
            continue;
        }
        payload.replicas.push_back(replica.get_descriptor());
    }

    auto result = struct_pack::serialize(payload);
    return std::string(result.begin(), result.end());
}

std::string MasterService::SerializeMetadataForOpLogFromReplicaDescriptors(
    const UUID& client_id, uint64_t size,
    const std::vector<Replica::Descriptor>& replicas,
    const std::string& group_id, ObjectDataType data_type) const {
    MetadataPayload payload;
    payload.client_id = client_id;
    payload.size = size;
    payload.replicas = replicas;
    payload.group_id = group_id;
    payload.data_type = data_type;
    auto result = struct_pack::serialize(payload);
    return std::string(result.begin(), result.end());
}

ErrorCode MasterService::InitializeBatchOpLogWriter(
    std::shared_ptr<HaKvBackend> backend) {
    if (!backend || !backend->SupportsTxn()) {
        return ErrorCode::INVALID_PARAMS;
    }

    auto storage = std::make_unique<OpLogBatchStorage>(cluster_id_, *backend,
                                                       master_id_);
    DurablePrefix durable_prefix;
    ErrorCode err = storage->InitDurablePrefix(durable_prefix);
    if (err != ErrorCode::OK) {
        return err;
    }

    OrderedOpLogWriterConfig writer_config;
    writer_config.max_entries_per_batch = oplog_batch_max_entries_;
    writer_config.initial_durable_prefix = durable_prefix;
    OpLogBatchStorage* storage_ptr = storage.get();
    auto writer = std::make_unique<OrderedOpLogWriter>(
        writer_config, [storage_ptr](const OpLogBatchRecord& batch,
                                     const DurablePrefix& expected_prefix) {
            return storage_ptr->WriteBatchAndAdvancePrefix(batch,
                                                           expected_prefix);
        });
    if (!writer->IsAccepting()) {
        return writer->LastError();
    }
    writer->Start();

    if (ordered_oplog_writer_) {
        ordered_oplog_writer_->Stop();
    }
    batch_oplog_kv_backend_ = std::move(backend);
    batch_oplog_storage_ = std::move(storage);
    ordered_oplog_writer_ = std::move(writer);
    return ErrorCode::OK;
}

tl::expected<uint64_t, ErrorCode>
MasterService::AppendOpLogVisibleBeforeDurable(OpType type,
                                               const std::string& tenant_id,
                                               const std::string& key,
                                               const std::string& payload) {
    if (!enable_oplog_) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!ordered_oplog_writer_) {
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    const TenantId resolved_tenant(enable_multi_tenants_
                                       ? tenant_id
                                       : std::string(TenantId::kDefaultValue));
    if (!resolved_tenant.IsValid()) {
        return tl::unexpected(ErrorCode::TENANT_NOT_REGISTERED);
    }

    auto reservation = ordered_oplog_writer_->Reserve();
    if (!reservation) {
        return tl::unexpected(reservation.error());
    }
    OpLogEntry entry;
    entry.op_type = type;
    entry.tenant_id = resolved_tenant.value();
    entry.object_key = key;
    entry.payload = payload;
    auto pending = ordered_oplog_writer_->Commit(std::move(reservation.value()),
                                                 std::move(entry), nullptr);
    if (!pending) {
        return tl::unexpected(pending.error());
    }
    return pending.value().sequence_id();
}

tl::expected<OpLogEntry, ErrorCode>
MasterService::AppendOpLogWithDurableFinalize(
    OpType type, const std::string& tenant_id, const std::string& key,
    const std::string& payload, DurableFinalizeCallback callback) {
    if (!enable_oplog_) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto reservation = ReserveBatchOpLogSlot();
    if (!reservation) {
        return tl::unexpected(reservation.error());
    }
    return AppendReservedOpLogWithDurableFinalize(
        std::move(reservation.value()), type, tenant_id, key, payload,
        std::move(callback));
}

tl::expected<OrderedOpLogWriter::Reservation, ErrorCode>
MasterService::ReserveBatchOpLogSlot() {
    if (!enable_oplog_) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!ordered_oplog_writer_) {
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return ordered_oplog_writer_->Reserve();
}

tl::expected<OpLogEntry, ErrorCode>
MasterService::AppendReservedOpLogWithDurableFinalize(
    OrderedOpLogWriter::Reservation&& reservation, OpType type,
    const std::string& tenant_id, const std::string& key,
    const std::string& payload, DurableFinalizeCallback callback) {
    if (!enable_oplog_) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    const TenantId resolved_tenant(enable_multi_tenants_
                                       ? tenant_id
                                       : std::string(TenantId::kDefaultValue));
    if (!resolved_tenant.IsValid()) {
        return tl::unexpected(ErrorCode::TENANT_NOT_REGISTERED);
    }
    OpLogEntry entry;
    entry.op_type = type;
    entry.tenant_id = resolved_tenant.value();
    entry.object_key = key;
    entry.payload = payload;
    auto pending = ordered_oplog_writer_->Commit(std::move(reservation), entry,
                                                 std::move(callback));
    if (!pending) {
        return tl::unexpected(pending.error());
    }
    entry.sequence_id = pending.value().sequence_id();
    return entry;
}

tl::expected<void, ErrorCode> MasterService::PersistRemoveForHA(
    const char* why, const std::string& key) {
    return PersistRemoveForHA(why, TenantId::Default(), key);
}

tl::expected<void, ErrorCode> MasterService::PersistRemoveForHA(
    const char* why, const TenantId& tenant_id, const std::string& key) {
    auto result = AppendOpLogWithDurableFinalize(
        OpType::REMOVE, tenant_id.value(), key, {}, nullptr);
    if (!result) {
        LOG(WARNING) << why << ": REMOVE persist failed for key=" << key
                     << ", err=" << static_cast<int>(result.error());
        return tl::unexpected(result.error());
    }
    return {};
}

void MasterService::PersistSegmentOpForHAOrEnqueue(const char* why, OpType type,
                                                   const std::string& key,
                                                   const std::string& payload) {
    PersistSegmentOpForHAOrEnqueue(why, type, TenantId::Default(), key,
                                   payload);
}

void MasterService::PersistSegmentOpForHAOrEnqueue(const char* why, OpType type,
                                                   const TenantId& tenant_id,
                                                   const std::string& key,
                                                   const std::string& payload) {
    auto result =
        AppendOpLogVisibleBeforeDurable(type, tenant_id.value(), key, payload);
    if (!result) {
        LOG(WARNING) << why << ": segment OpLog queue failed for key=" << key
                     << ", type=" << static_cast<int>(type)
                     << ", err=" << static_cast<int>(result.error());
    }
}

std::vector<Replica::Descriptor>
MasterService::BuildRemainingReplicaDescriptors(
    const ObjectMetadata& metadata,
    const std::function<bool(const Replica&)>& should_remove) const {
    std::vector<Replica::Descriptor> remaining;
    for (const auto& replica : metadata.GetAllReplicas()) {
        if (!should_remove(replica) &&
            replica.status() == ReplicaStatus::COMPLETE) {
            remaining.push_back(replica.get_descriptor());
        }
    }
    return remaining;
}

}  // namespace mooncake
