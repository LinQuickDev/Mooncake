#include "vchunk_metadata.h"

#include <gtest/gtest.h>

#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace mooncake {
namespace {

VChunkMetadataRecord MakeValidRecord() {
    VChunkMetadataRecord record;
    record.vchunk_id = "vchunk-1";
    record.tenant_id = "tenant-1";
    record.key = "key-1";
    record.total_size = 10U * 1024U;
    record.slice_count = 3;
    record.slice_size_level = VCSliceSizeLevel::k4K;
    record.row_size = 2;
    record.status = VChunkStatus::CREATING;
    record.created_at_ms = 100;
    record.last_updated_at_ms = 100;
    record.slices = {
        VCSliceDescriptor{0, "segment-a", 0, 4096, 4096,
                          VCSliceStatus::PENDING, 0},
        VCSliceDescriptor{1, "segment-b", 0, 4096, 4096,
                          VCSliceStatus::PENDING, 0},
        VCSliceDescriptor{2, "segment-a", 4096, 2048, 4096,
                          VCSliceStatus::PENDING, 0},
    };
    return record;
}

TEST(VChunkMetadataTest, AcceptsValidLayout) {
    EXPECT_EQ(ValidateVChunkMetadata(MakeValidRecord(), VChunkConfig{}),
              ErrorCode::OK);
}

TEST(VChunkMetadataTest, RoundTripsStableRecord) {
    const auto original = MakeValidRecord();
    auto serialized = SerializeVChunkMetadata(original, VChunkConfig{});
    ASSERT_TRUE(serialized.has_value());

    auto restored =
        DeserializeVChunkMetadata(serialized.value(), VChunkConfig{});
    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(restored->schema_version, original.schema_version);
    EXPECT_EQ(restored->vchunk_id, original.vchunk_id);
    EXPECT_EQ(restored->tenant_id, original.tenant_id);
    EXPECT_EQ(restored->key, original.key);
    EXPECT_EQ(restored->total_size, original.total_size);
    EXPECT_EQ(restored->slices.size(), original.slices.size());
    EXPECT_EQ(restored->slices.back().logical_length, 2048U);
    EXPECT_EQ(restored->created_at_ms, 100);
}

TEST(VChunkMetadataTest, RejectsUnsupportedSchema) {
    auto record = MakeValidRecord();
    record.schema_version = kVChunkMetadataSchemaVersion + 1;
    EXPECT_EQ(ValidateVChunkMetadata(record, VChunkConfig{}),
              ErrorCode::INVALID_VERSION);
}

TEST(VChunkMetadataTest, RejectsGapsAndDuplicateSegmentsWithinRow) {
    auto record = MakeValidRecord();
    record.slices[1].slice_index = 3;
    EXPECT_EQ(ValidateVChunkMetadata(record, VChunkConfig{}),
              ErrorCode::INVALID_PARAMS);

    record = MakeValidRecord();
    record.slices[1].target_segment_name = "segment-a";
    EXPECT_EQ(ValidateVChunkMetadata(record, VChunkConfig{}),
              ErrorCode::INVALID_PARAMS);
}

TEST(VChunkMetadataTest, RejectsIncorrectCoverageAndAllocation) {
    auto record = MakeValidRecord();
    record.slices.back().logical_length = 1024;
    EXPECT_EQ(ValidateVChunkMetadata(record, VChunkConfig{}),
              ErrorCode::INVALID_PARAMS);

    record = MakeValidRecord();
    record.slices.back().allocated_length = 1024;
    EXPECT_EQ(ValidateVChunkMetadata(record, VChunkConfig{}),
              ErrorCode::INVALID_PARAMS);
}

TEST(VChunkMetadataTest, RejectsOffsetOverflowAndRetryOverflow) {
    auto record = MakeValidRecord();
    record.slices[0].target_offset =
        std::numeric_limits<uint64_t>::max() - 1024;
    EXPECT_EQ(ValidateVChunkMetadata(record, VChunkConfig{}),
              ErrorCode::INVALID_PARAMS);

    record = MakeValidRecord();
    record.slices[0].retry_count = VChunkConfig{}.max_slice_retry + 1;
    EXPECT_EQ(ValidateVChunkMetadata(record, VChunkConfig{}),
              ErrorCode::INVALID_PARAMS);
}

TEST(VChunkMetadataTest, EnforcesMetadataSizeLimit) {
    auto config = VChunkConfig{};
    config.max_metadata_bytes = 8;
    auto serialized = SerializeVChunkMetadata(MakeValidRecord(), config);
    ASSERT_FALSE(serialized.has_value());
    EXPECT_EQ(serialized.error(), ErrorCode::BUFFER_OVERFLOW);
}

TEST(VChunkMetadataTest, RejectsCorruptedBytes) {
    auto serialized =
        SerializeVChunkMetadata(MakeValidRecord(), VChunkConfig{});
    ASSERT_TRUE(serialized.has_value());
    serialized->pop_back();

    auto restored =
        DeserializeVChunkMetadata(serialized.value(), VChunkConfig{});
    EXPECT_FALSE(restored.has_value());
    EXPECT_EQ(restored.error(), ErrorCode::INVALID_PARAMS);
}

TEST(VChunkMetadataTest, RuntimeStateTransitionsAreValidated) {
    VChunkMetadata metadata(MakeValidRecord());
    EXPECT_EQ(metadata.TransitionTo(VChunkStatus::ACTIVE, 200), ErrorCode::OK);
    EXPECT_EQ(metadata.TransitionTo(VChunkStatus::CREATING, 300),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(metadata.TransitionTo(VChunkStatus::RELEASING, 199),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(metadata.TransitionTo(VChunkStatus::RELEASING, 300),
              ErrorCode::OK);
    EXPECT_EQ(metadata.TransitionTo(VChunkStatus::RELEASED, 400),
              ErrorCode::OK);

    const auto snapshot = metadata.Snapshot();
    EXPECT_EQ(snapshot.status, VChunkStatus::RELEASED);
    EXPECT_EQ(snapshot.last_updated_at_ms, 400);
}

}  // namespace
}  // namespace mooncake
