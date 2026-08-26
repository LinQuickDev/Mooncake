#include "vchunk_master_manager.h"

#include <limits>
#include <utility>

#include "types.h"

namespace mooncake {

VChunkMasterManager::VChunkMasterManager(VChunkConfig config)
    : config_(std::move(config)) {}

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
    // The piercing version owns buffers from SegmentManager and supports
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
        return tl::make_unexpected(allocation.error());
    }

    auto entry = std::make_unique<Entry>();
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

    std::lock_guard<std::mutex> guard(mutex_);
    if (entries_.contains(scoped_key)) {
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }
    const auto snapshot = record;
    entries_.emplace(scoped_key, std::move(entry));
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
    for (auto& slice : record.slices) {
        slice.status = VCSliceStatus::COMPLETED;
    }
    record.status = VChunkStatus::ACTIVE;
    record.last_updated_at_ms = now_ms;
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
    entries_.erase(it);
    return ErrorCode::OK;
}

tl::expected<VChunkMetadataRecord, ErrorCode> VChunkMasterManager::Get(
    const TenantId& tenant_id, const std::string& key) const {
    std::lock_guard<std::mutex> guard(mutex_);
    const auto it = entries_.find(ScopedKey(tenant_id, key));
    if (it == entries_.end()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    if (it->second->record.status != VChunkStatus::ACTIVE) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }
    return it->second->record;
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
    if (record.status != VChunkStatus::ACTIVE ||
        now_ms < record.last_updated_at_ms) {
        return ErrorCode::INVALID_PARAMS;
    }
    record.status = VChunkStatus::RELEASING;
    record.last_updated_at_ms = now_ms;
    record.status = VChunkStatus::RELEASED;
    entries_.erase(it);
    return ErrorCode::OK;
}

size_t VChunkMasterManager::SizeForTesting() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return entries_.size();
}

}  // namespace mooncake
