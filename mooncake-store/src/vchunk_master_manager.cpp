#include "vchunk_master_manager.h"

#include <limits>
#include <utility>

#include "types.h"

namespace mooncake {

VChunkMasterManager::VChunkMasterManager(
    VChunkConfig config, std::shared_ptr<VChunkMetadataStore> metadata_store,
    std::shared_ptr<VChunkMetrics> metrics)
    : config_(std::move(config)),
      metadata_store_(metadata_store ? std::move(metadata_store)
                                     : std::make_shared<
                                           InMemoryVChunkMetadataStore>()),
      metrics_(metrics ? std::move(metrics)
                       : std::make_shared<VChunkMetrics>()) {}

std::string VChunkMasterManager::ScopedKey(const TenantId& tenant_id,
                                           const std::string& key) {
    return tenant_id.MakeScopedKey(key);
}

tl::expected<VChunkMetadataRecord, ErrorCode> VChunkMasterManager::PutStart(
    const AllocatorManager& allocator_manager, const TenantId& tenant_id,
    const std::string& key, uint64_t total_size, bool is_ssd_segment,
    int64_t now_ms, const std::set<std::string>& excluded_segments) {
    if (!config_.enabled || config_.Validate() != ErrorCode::OK ||
        !tenant_id.IsValid() || key.empty() || total_size == 0 || now_ms < 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    // The current version owns buffers from SegmentManager and supports
    // memory segments only. SSD/NoF routing is introduced in a later stage.
    if (is_ssd_segment) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto scoped_key = ScopedKey(tenant_id, key);
    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (entries_.contains(scoped_key)) {
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }
        size_t creating = 0;
        for (const auto& [_, entry] : entries_) {
            creating += entry->record.status == VChunkStatus::CREATING;
        }
        if (creating >= config_.max_creating_objects) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
    }

    const auto slice_size_level =
        SelectVChunkSliceSize(total_size, is_ssd_segment);
    const uint64_t slice_size = SliceSizeLevelToBytes(slice_size_level);
    if (total_size > std::numeric_limits<uint64_t>::max() - (slice_size - 1) ||
        (total_size + slice_size - 1) / slice_size >
            config_.max_slice_count) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto allocation = AllocateVChunk(allocator_manager, total_size,
                                     slice_size_level, excluded_segments);
    if (!allocation) {
        metrics_->AddAllocationFailure();
        return tl::make_unexpected(allocation.error());
    }

    auto entry = std::make_shared<Entry>();
    auto& record = entry->record;
    record.vchunk_id = UuidToString(generate_uuid());
    record.tenant_id = tenant_id.value();
    record.key = key;
    record.total_size = total_size;
    record.slice_count =
        static_cast<uint32_t>(allocation->allocations.size());
    record.slice_size_level = slice_size_level;
    record.row_size = static_cast<uint32_t>(allocation->row_size);
    record.status = VChunkStatus::CREATING;
    record.created_at_ms = now_ms;
    record.last_updated_at_ms = now_ms;
    record.slices.reserve(record.slice_count);
    entry->buffers.reserve(record.slice_count);
    for (auto& allocated : allocation->allocations) {
        record.slices.push_back(VCSliceDescriptor{
            allocated.slice_index, allocated.segment_name,
            allocated.target_offset, allocated.logical_length,
            allocated.allocated_length, VCSliceStatus::PENDING, 0});
        entry->buffers.push_back(std::move(allocated.buffer));
    }
    const auto serialized = SerializeVChunkMetadata(record, config_);
    if (!serialized) {
        return tl::make_unexpected(serialized.error());
    }
    if (const auto error = metadata_store_->Put(record);
        error != ErrorCode::OK) {
        return tl::make_unexpected(error);
    }

    std::lock_guard<std::mutex> guard(mutex_);
    if (entries_.contains(scoped_key)) {
        metadata_store_->Remove(record);
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }
    const auto snapshot = record;
    entries_.emplace(scoped_key, std::move(entry));
    metrics_->AddSlices(snapshot.slice_count);
    metrics_->ObserveLayout(snapshot);
    metrics_->AddMetadataBytes(serialized->size());
    RefreshStateMetricsLocked();
    return snapshot;
}

ErrorCode VChunkMasterManager::PutEnd(const TenantId& tenant_id,
                                      const std::string& key,
                                      const std::string& vchunk_id,
                                      int64_t now_ms) {
    std::lock_guard<std::mutex> guard(mutex_);
    const auto it = entries_.find(ScopedKey(tenant_id, key));
    if (it == entries_.end()) {
        return ErrorCode::OBJECT_NOT_FOUND;
    }
    auto& record = it->second->record;
    if (record.vchunk_id != vchunk_id) {
        return ErrorCode::INVALID_VERSION;
    }
    if (record.status == VChunkStatus::ACTIVE) {
        return ErrorCode::OK;
    }
    if (ValidateVChunkTransition(record.status, VChunkStatus::ACTIVE) !=
            ErrorCode::OK ||
        now_ms < record.last_updated_at_ms) {
        return ErrorCode::INVALID_PARAMS;
    }
    auto durable = record;
    for (auto& slice : durable.slices) {
        slice.status = VCSliceStatus::COMPLETED;
    }
    durable.status = VChunkStatus::ACTIVE;
    durable.last_updated_at_ms = now_ms;
    if (const auto error = metadata_store_->Put(durable);
        error != ErrorCode::OK) {
        return error;
    }
    record = std::move(durable);
    RefreshStateMetricsLocked();
    return ErrorCode::OK;
}

ErrorCode VChunkMasterManager::PutRevoke(const TenantId& tenant_id,
                                         const std::string& key,
                                         const std::string& vchunk_id) {
    std::lock_guard<std::mutex> guard(mutex_);
    const auto it = entries_.find(ScopedKey(tenant_id, key));
    if (it == entries_.end()) {
        return ErrorCode::OK;
    }
    if (it->second->record.vchunk_id != vchunk_id) {
        return ErrorCode::INVALID_VERSION;
    }
    if (it->second->record.status != VChunkStatus::CREATING &&
        it->second->record.status != VChunkStatus::FAILED) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (const auto error = metadata_store_->Remove(it->second->record);
        error != ErrorCode::OK) {
        return error;
    }
    entries_.erase(it);
    metrics_->AddRollback();
    RefreshStateMetricsLocked();
    return ErrorCode::OK;
}

tl::expected<VChunkMetadataRecord, ErrorCode> VChunkMasterManager::Get(
    const TenantId& tenant_id, const std::string& key) const {
    auto handle = AcquireRead(tenant_id, key);
    if (!handle) {
        return tl::make_unexpected(handle.error());
    }
    return handle->record();
}

tl::expected<VChunkMasterManager::ReadHandle, ErrorCode>
VChunkMasterManager::AcquireRead(const TenantId& tenant_id,
                                 const std::string& key) const {
    std::lock_guard<std::mutex> guard(mutex_);
    const auto it = entries_.find(ScopedKey(tenant_id, key));
    if (it == entries_.end()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    if (it->second->record.status != VChunkStatus::ACTIVE) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }
    ReadHandle handle;
    handle.record_ = it->second->record;
    handle.lifetime_ = it->second;
    return handle;
}

ErrorCode VChunkMasterManager::Remove(const TenantId& tenant_id,
                                      const std::string& key,
                                      int64_t now_ms) {
    std::lock_guard<std::mutex> guard(mutex_);
    const auto scoped_key = ScopedKey(tenant_id, key);
    const auto it = entries_.find(scoped_key);
    if (it == entries_.end()) {
        return ErrorCode::OK;
    }
    auto& record = it->second->record;
    if ((record.status != VChunkStatus::ACTIVE &&
         record.status != VChunkStatus::RELEASING) ||
        now_ms < record.last_updated_at_ms) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (record.status == VChunkStatus::ACTIVE) {
        auto releasing = record;
        releasing.status = VChunkStatus::RELEASING;
        releasing.last_updated_at_ms = now_ms;
        if (const auto error = metadata_store_->Put(releasing);
            error != ErrorCode::OK) {
            return error;
        }
        record = std::move(releasing);
    }
    if (const auto error = metadata_store_->Remove(record);
        error != ErrorCode::OK) {
        return error;
    }
    entries_.erase(it);
    RefreshStateMetricsLocked();
    return ErrorCode::OK;
}

ErrorCode VChunkMasterManager::Recover(int64_t now_ms) {
    if (now_ms < 0) {
        return ErrorCode::INVALID_PARAMS;
    }
    auto records = metadata_store_->List();
    if (!records) {
        return records.error();
    }
    std::lock_guard<std::mutex> guard(mutex_);
    for (const auto& record : *records) {
        const auto validation = ValidateVChunkMetadata(record, config_);
        if (validation != ErrorCode::OK) {
            return validation;
        }
        const bool creating_expired =
            record.status == VChunkStatus::CREATING &&
            now_ms >= record.last_updated_at_ms &&
            static_cast<uint64_t>(now_ms - record.last_updated_at_ms) >=
                config_.creating_timeout_ms;
        if (creating_expired || record.status == VChunkStatus::RELEASING ||
            record.status == VChunkStatus::RELEASED ||
            record.status == VChunkStatus::FAILED) {
            const auto error = metadata_store_->Remove(record);
            if (error != ErrorCode::OK) {
                return error;
            }
            continue;
        }
        const auto scoped_key =
            ScopedKey(TenantId(record.tenant_id), record.key);
        if (entries_.contains(scoped_key)) {
            continue;
        }
        auto entry = std::make_shared<Entry>();
        entry->record = record;
        entries_.emplace(scoped_key, std::move(entry));
    }
    RefreshStateMetricsLocked();
    return ErrorCode::OK;
}

tl::expected<size_t, ErrorCode> VChunkMasterManager::ReapExpired(
    int64_t now_ms, size_t max_scan) {
    if (now_ms < 0 || max_scan == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::lock_guard<std::mutex> guard(mutex_);
    size_t scanned = 0;
    size_t removed = 0;
    for (auto it = entries_.begin(); it != entries_.end() && scanned < max_scan;) {
        ++scanned;
        const auto& record = it->second->record;
        const bool time_is_valid = now_ms >= record.last_updated_at_ms;
        const auto age = time_is_valid
                             ? static_cast<uint64_t>(now_ms -
                                                     record.last_updated_at_ms)
                             : 0;
        const bool expired =
            time_is_valid &&
            ((record.status == VChunkStatus::CREATING &&
              age >= config_.creating_timeout_ms) ||
             (record.status == VChunkStatus::RELEASING &&
              age >= config_.releasing_timeout_ms));
        if (!expired) {
            ++it;
            continue;
        }
        if (const auto error = metadata_store_->Remove(record);
            error != ErrorCode::OK) {
            return tl::make_unexpected(error);
        }
        it = entries_.erase(it);
        ++removed;
        metrics_->AddRollback();
    }
    RefreshStateMetricsLocked();
    return removed;
}

VChunkMetricsSnapshot VChunkMasterManager::MetricsSnapshot() const {
    return metrics_->Snapshot();
}

void VChunkMasterManager::RefreshStateMetricsLocked() {
    std::array<uint64_t, 5> counts{};
    for (const auto& [_, entry] : entries_) {
        ++counts[static_cast<size_t>(entry->record.status)];
    }
    for (size_t i = 0; i < counts.size(); ++i) {
        metrics_->SetStateCount(static_cast<VChunkStatus>(i), counts[i]);
    }
}

size_t VChunkMasterManager::SizeForTesting() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return entries_.size();
}

}  // namespace mooncake
