#include "vchunk_transfer_engine.h"

#include <gtest/gtest.h>

#include <array>
#include <string>

namespace mooncake {
namespace {

VChunkMetadataRecord MakeRecord() {
    VChunkMetadataRecord record;
    record.vchunk_id = "id";
    record.tenant_id = "tenant";
    record.key = "key";
    record.total_size = 4096 + 17;
    record.slice_count = 2;
    record.slice_size_level = VCSliceSizeLevel::k4K;
    record.row_size = 2;
    record.status = VChunkStatus::CREATING;
    record.created_at_ms = 1;
    record.last_updated_at_ms = 1;
    record.slices = {
        {0, "a", 1000, 4096, 4096, VCSliceStatus::PENDING, 0},
        {1, "b", 2000, 17, 4096, VCSliceStatus::PENDING, 0}};
    return record;
}

TEST(VChunkTransferEngineTest, BuildsAllRequestsBeforeSubmission) {
    auto record = MakeRecord();
    std::array<char, 4096 + 17> buffer{};
    int resolutions = 0;
    auto requests = BuildVChunkTransferRequests(
        record, buffer.data(), buffer.size(), TransferRequest::WRITE,
        [&](const std::string& name)
            -> tl::expected<SegmentHandle, ErrorCode> {
            ++resolutions;
            return name == "a" ? 11 : 22;
        });
    ASSERT_TRUE(requests.has_value());
    ASSERT_EQ(requests->size(), 2U);
    EXPECT_EQ(resolutions, 2);
    EXPECT_EQ((*requests)[0].source, buffer.data());
    EXPECT_EQ((*requests)[0].target_id, 11U);
    EXPECT_EQ((*requests)[0].target_offset, 1000U);
    EXPECT_EQ((*requests)[0].length, 4096U);
    EXPECT_EQ((*requests)[1].source, buffer.data() + 4096);
    EXPECT_EQ((*requests)[1].target_id, 22U);
    EXPECT_EQ((*requests)[1].length, 17U);
}

TEST(VChunkTransferEngineTest, RejectsAnyUnresolvableSliceAsAWhole) {
    auto record = MakeRecord();
    std::array<char, 4096 + 17> buffer{};
    auto requests = BuildVChunkTransferRequests(
        record, buffer.data(), buffer.size(), TransferRequest::READ,
        [](const std::string& name)
            -> tl::expected<SegmentHandle, ErrorCode> {
            if (name == "b") {
                return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
            }
            return 11;
        });
    ASSERT_FALSE(requests.has_value());
    EXPECT_EQ(requests.error(), ErrorCode::SEGMENT_NOT_FOUND);
}

TEST(VChunkTransferEngineTest, ReusesHandleWithinOneSegment) {
    auto record = MakeRecord();
    record.slices[1].target_segment_name = "a";
    record.row_size = 1;
    std::array<char, 4096 + 17> buffer{};
    int resolutions = 0;
    auto requests = BuildVChunkTransferRequests(
        record, buffer.data(), buffer.size(), TransferRequest::WRITE,
        [&](const std::string&)
            -> tl::expected<SegmentHandle, ErrorCode> {
            ++resolutions;
            return 11;
        });
    ASSERT_TRUE(requests.has_value());
    EXPECT_EQ(resolutions, 1);
}

TEST(VChunkTransferEngineTest, RejectsUnknownMetadataVersion) {
    auto record = MakeRecord();
    record.schema_version = kVChunkMetadataSchemaVersion + 1;
    std::array<char, 4096 + 17> buffer{};
    auto requests = BuildVChunkTransferRequests(
        record, buffer.data(), buffer.size(), TransferRequest::WRITE,
        [](const std::string&)
            -> tl::expected<SegmentHandle, ErrorCode> { return 11; });
    ASSERT_FALSE(requests.has_value());
    EXPECT_EQ(requests.error(), ErrorCode::INVALID_VERSION);
}

}  // namespace
}  // namespace mooncake
