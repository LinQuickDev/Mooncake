#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "mutex.h"
#include "types.h"
#include "vchunk_config.h"

namespace mooncake {

inline constexpr uint32_t kVChunkMetadataSchemaVersion = 1;

enum class VCSliceStatus : uint8_t {
    PENDING = 0,
    COMPLETED = 1,
    FAILED = 2,
};

enum class VChunkStatus : uint8_t {
    CREATING = 0,
    ACTIVE = 1,
    RELEASING = 2,
    RELEASED = 3,
    FAILED = 4,
};

struct VCSliceDescriptor {
    uint32_t slice_index{0};
    std::string target_segment_name;
    uint64_t target_offset{0};
    uint32_t logical_length{0};
    uint32_t allocated_length{0};
    VCSliceStatus status{VCSliceStatus::PENDING};
    uint32_t retry_count{0};

    YLT_REFL(VCSliceDescriptor, slice_index, target_segment_name,
             target_offset, logical_length, allocated_length, status,
             retry_count);
};

// Stable wire/storage representation. Runtime-only synchronization and indexes
// intentionally live outside this type.
struct VChunkMetadataRecord {
    uint32_t schema_version{kVChunkMetadataSchemaVersion};
    std::string vchunk_id;
    std::string tenant_id;
    std::string key;
    uint64_t total_size{0};
    uint32_t slice_count{0};
    VCSliceSizeLevel slice_size_level{VCSliceSizeLevel::k4K};
    std::vector<VCSliceDescriptor> slices;
    uint32_t row_size{0};
    VChunkStatus status{VChunkStatus::CREATING};
    int64_t created_at_ms{0};
    int64_t last_updated_at_ms{0};

    YLT_REFL(VChunkMetadataRecord, schema_version, vchunk_id, tenant_id, key,
             total_size, slice_count, slice_size_level, slices, row_size,
             status, created_at_ms, last_updated_at_ms);
};

struct VChunkRuntimeInfo {
    bool enabled{false};
    bool persistent_metadata{false};

    YLT_REFL(VChunkRuntimeInfo, enabled, persistent_metadata);
};

struct VChunkReadLease {
    VChunkMetadataRecord record;
    std::string lease_id;

    YLT_REFL(VChunkReadLease, record, lease_id);
};

ErrorCode ValidateVChunkMetadata(const VChunkMetadataRecord& record,
                                 const VChunkConfig& config);
ErrorCode ValidateVChunkTransition(VChunkStatus from, VChunkStatus to);

tl::expected<std::vector<char>, ErrorCode> SerializeVChunkMetadata(
    const VChunkMetadataRecord& record, const VChunkConfig& config);
tl::expected<VChunkMetadataRecord, ErrorCode> DeserializeVChunkMetadata(
    const std::vector<char>& bytes, const VChunkConfig& config);

class VChunkMetadata {
   public:
    explicit VChunkMetadata(VChunkMetadataRecord record);

    VChunkMetadata(const VChunkMetadata&) = delete;
    VChunkMetadata& operator=(const VChunkMetadata&) = delete;

    VChunkMetadataRecord Snapshot() const;
    ErrorCode TransitionTo(VChunkStatus next, int64_t updated_at_ms);

   private:
    mutable SpinLock lock_;
    VChunkMetadataRecord record_;
};

}  // namespace mooncake
