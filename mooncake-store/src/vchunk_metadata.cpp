#include "vchunk_metadata.h"

#include <limits>
#include <unordered_set>
#include <utility>

namespace mooncake {
namespace {

bool IsKnownSliceSize(VCSliceSizeLevel level) {
    switch (level) {
        case VCSliceSizeLevel::k4K:
        case VCSliceSizeLevel::k64K:
        case VCSliceSizeLevel::k256K:
        case VCSliceSizeLevel::k1M:
            return true;
    }
    return false;
}

bool IsKnownSliceStatus(VCSliceStatus status) {
    switch (status) {
        case VCSliceStatus::PENDING:
        case VCSliceStatus::COMPLETED:
        case VCSliceStatus::FAILED:
            return true;
    }
    return false;
}

bool IsKnownVChunkStatus(VChunkStatus status) {
    switch (status) {
        case VChunkStatus::CREATING:
        case VChunkStatus::ACTIVE:
        case VChunkStatus::RELEASING:
        case VChunkStatus::RELEASED:
        case VChunkStatus::FAILED:
            return true;
    }
    return false;
}

}  // namespace

ErrorCode ValidateVChunkMetadata(const VChunkMetadataRecord& record,
                                 const VChunkConfig& config) {
    if (config.Validate() != ErrorCode::OK) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (record.schema_version != kVChunkMetadataSchemaVersion) {
        return ErrorCode::INVALID_VERSION;
    }
    if (record.vchunk_id.empty() || record.tenant_id.empty() ||
        record.key.empty() || record.total_size == 0 ||
        !IsKnownSliceSize(record.slice_size_level) ||
        !IsKnownVChunkStatus(record.status)) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (record.slice_count == 0 ||
        record.slice_count > config.max_slice_count ||
        record.slices.size() != record.slice_count || record.row_size == 0 ||
        record.row_size > record.slice_count || record.created_at_ms < 0 ||
        record.last_updated_at_ms < record.created_at_ms) {
        return ErrorCode::INVALID_PARAMS;
    }

    const uint64_t slice_size =
        SliceSizeLevelToBytes(record.slice_size_level);
    uint64_t covered_bytes = 0;
    std::unordered_set<std::string> segments_in_row;
    segments_in_row.reserve(record.row_size);
    for (uint32_t i = 0; i < record.slice_count; ++i) {
        const auto& slice = record.slices[i];
        if (slice.slice_index != i || slice.target_segment_name.empty() ||
            slice.logical_length == 0 ||
            slice.logical_length > slice.allocated_length ||
            slice.allocated_length < slice_size ||
            !IsKnownSliceStatus(slice.status) ||
            slice.retry_count > config.max_slice_retry) {
            return ErrorCode::INVALID_PARAMS;
        }
        if (slice.target_offset >
            std::numeric_limits<uint64_t>::max() - slice.allocated_length) {
            return ErrorCode::INVALID_PARAMS;
        }
        if (covered_bytes >
            std::numeric_limits<uint64_t>::max() - slice.logical_length) {
            return ErrorCode::INVALID_PARAMS;
        }
        covered_bytes += slice.logical_length;

        if (i % record.row_size == 0) {
            segments_in_row.clear();
        }
        if (!segments_in_row.insert(slice.target_segment_name).second) {
            return ErrorCode::INVALID_PARAMS;
        }
    }

    if (covered_bytes != record.total_size) {
        return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::OK;
}

ErrorCode ValidateVChunkTransition(VChunkStatus from, VChunkStatus to) {
    if (!IsKnownVChunkStatus(from) || !IsKnownVChunkStatus(to)) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (from == to) {
        return ErrorCode::OK;
    }
    switch (from) {
        case VChunkStatus::CREATING:
            return (to == VChunkStatus::ACTIVE || to == VChunkStatus::FAILED)
                       ? ErrorCode::OK
                       : ErrorCode::INVALID_PARAMS;
        case VChunkStatus::ACTIVE:
            return (to == VChunkStatus::RELEASING ||
                    to == VChunkStatus::FAILED)
                       ? ErrorCode::OK
                       : ErrorCode::INVALID_PARAMS;
        case VChunkStatus::RELEASING:
            return (to == VChunkStatus::RELEASED ||
                    to == VChunkStatus::FAILED)
                       ? ErrorCode::OK
                       : ErrorCode::INVALID_PARAMS;
        case VChunkStatus::FAILED:
            return to == VChunkStatus::RELEASING ? ErrorCode::OK
                                                 : ErrorCode::INVALID_PARAMS;
        case VChunkStatus::RELEASED:
            return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::INVALID_PARAMS;
}

tl::expected<std::vector<char>, ErrorCode> SerializeVChunkMetadata(
    const VChunkMetadataRecord& record, const VChunkConfig& config) {
    const auto validation = ValidateVChunkMetadata(record, config);
    if (validation != ErrorCode::OK) {
        return tl::make_unexpected(validation);
    }
    auto bytes = struct_pack::serialize(record);
    if (bytes.size() > config.max_metadata_bytes) {
        return tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);
    }
    return bytes;
}

tl::expected<VChunkMetadataRecord, ErrorCode> DeserializeVChunkMetadata(
    const std::vector<char>& bytes, const VChunkConfig& config) {
    if (config.Validate() != ErrorCode::OK || bytes.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (bytes.size() > config.max_metadata_bytes) {
        return tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);
    }

    VChunkMetadataRecord record;
    if (struct_pack::deserialize_to(record, bytes) != struct_pack::errc::ok) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto validation = ValidateVChunkMetadata(record, config);
    if (validation != ErrorCode::OK) {
        return tl::make_unexpected(validation);
    }
    return record;
}

VChunkMetadata::VChunkMetadata(VChunkMetadataRecord record)
    : record_(std::move(record)) {}

VChunkMetadataRecord VChunkMetadata::Snapshot() const {
    SpinLocker guard(&lock_);
    return record_;
}

ErrorCode VChunkMetadata::TransitionTo(VChunkStatus next,
                                       int64_t updated_at_ms) {
    SpinLocker guard(&lock_);
    const auto validation = ValidateVChunkTransition(record_.status, next);
    if (validation != ErrorCode::OK ||
        updated_at_ms < record_.last_updated_at_ms) {
        return ErrorCode::INVALID_PARAMS;
    }
    record_.status = next;
    record_.last_updated_at_ms = updated_at_ms;
    return ErrorCode::OK;
}

}  // namespace mooncake
