#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "allocator.h"
#include "tenant_id.h"
#include "vchunk_allocation_strategy.h"
#include "vchunk_config.h"
#include "vchunk_metadata.h"
#include "vchunk_metadata_store.h"
#include "vchunk_metrics.h"

namespace mooncake {

// In-memory master-side vchunk lifecycle manager. The caller must hold the
// SegmentManager allocator access guard while PutStart uses AllocatorManager.
class VChunkMasterManager {
   public:
    using OwnershipPredicate =
        std::function<bool(const VChunkMetadataRecord&)>;

    class ReadHandle {
       public:
        ReadHandle() = default;
        const VChunkMetadataRecord& record() const { return record_; }

       private:
        friend class VChunkMasterManager;
        VChunkMetadataRecord record_;
        std::shared_ptr<const void> lifetime_;
    };

    explicit VChunkMasterManager(
        VChunkConfig config,
        std::shared_ptr<VChunkMetadataStore> metadata_store = nullptr,
        std::shared_ptr<VChunkMetrics> metrics = nullptr);

    VChunkMasterManager(const VChunkMasterManager&) = delete;
    VChunkMasterManager& operator=(const VChunkMasterManager&) = delete;

    tl::expected<VChunkMetadataRecord, ErrorCode> PutStart(
        const AllocatorManager& allocator_manager, const TenantId& tenant_id,
        const std::string& key, uint64_t total_size, bool is_ssd_segment,
        int64_t now_ms,
        const std::set<std::string>& excluded_segments = {});

    ErrorCode PutEnd(const TenantId& tenant_id, const std::string& key,
                     const std::string& vchunk_id, int64_t now_ms);
    ErrorCode PutRevoke(const TenantId& tenant_id, const std::string& key,
                        const std::string& vchunk_id);

    tl::expected<VChunkMetadataRecord, ErrorCode> Get(
        const TenantId& tenant_id, const std::string& key) const;
    tl::expected<ReadHandle, ErrorCode> AcquireRead(
        const TenantId& tenant_id, const std::string& key) const;

    ErrorCode Remove(const TenantId& tenant_id, const std::string& key,
                     int64_t now_ms);

    ErrorCode Recover(int64_t now_ms, OwnershipPredicate owns = {});
    tl::expected<size_t, ErrorCode> ReapExpired(int64_t now_ms,
                                                size_t max_scan,
                                                OwnershipPredicate owns = {});
    VChunkMetricsSnapshot MetricsSnapshot() const;

    size_t SizeForTesting() const;
    bool HasPersistentMetadata() const { return metadata_store_->IsPersistent(); }

   private:
    struct Entry {
        VChunkMetadataRecord record;
        std::vector<std::unique_ptr<AllocatedBuffer>> buffers;
    };

    static std::string ScopedKey(const TenantId& tenant_id,
                                 const std::string& key);
    void RefreshStateMetricsLocked();
    void ReleasePendingPut(const std::string& scoped_key);

    const VChunkConfig config_;
    const std::shared_ptr<VChunkMetadataStore> metadata_store_;
    const std::shared_ptr<VChunkMetrics> metrics_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<Entry>> entries_;
    std::unordered_set<std::string> pending_puts_;
    std::string reaper_cursor_key_;
};

}  // namespace mooncake
