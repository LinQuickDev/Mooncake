#include "vchunk_config.h"

#include <gtest/gtest.h>

namespace mooncake {
namespace {

TEST(VChunkConfigTest, DefaultsAreSafeForProductionOptIn) {
    VChunkConfig config;
    EXPECT_FALSE(config.enabled);
    EXPECT_EQ(config.Validate(), ErrorCode::OK);
    EXPECT_EQ(config.max_slice_count, 4096U);
    EXPECT_EQ(config.max_metadata_bytes, 1024U * 1024U);
    EXPECT_EQ(config.max_creating_objects, 1024U);
    EXPECT_EQ(config.reaper_interval_ms, 1000U);
    EXPECT_EQ(config.reaper_max_scan, 128U);
}

TEST(VChunkConfigTest, RejectsInvalidLimits) {
    VChunkConfig config;
    config.max_slice_count = 0;
    EXPECT_EQ(config.Validate(), ErrorCode::INVALID_PARAMS);

    config = VChunkConfig{};
    config.max_metadata_bytes = 0;
    EXPECT_EQ(config.Validate(), ErrorCode::INVALID_PARAMS);

    config = VChunkConfig{};
    config.creating_timeout_ms = 0;
    EXPECT_EQ(config.Validate(), ErrorCode::INVALID_PARAMS);

    config = VChunkConfig{};
    config.max_creating_objects = 0;
    EXPECT_EQ(config.Validate(), ErrorCode::INVALID_PARAMS);

    config = VChunkConfig{};
    config.reaper_interval_ms = 0;
    EXPECT_EQ(config.Validate(), ErrorCode::INVALID_PARAMS);
}

TEST(VChunkConfigTest, SelectsMemorySliceBoundaries) {
    EXPECT_EQ(SelectVChunkSliceSize(0, false), VCSliceSizeLevel::k4K);
    EXPECT_EQ(SelectVChunkSliceSize(64U * 1024U - 1, false),
              VCSliceSizeLevel::k4K);
    EXPECT_EQ(SelectVChunkSliceSize(64U * 1024U, false),
              VCSliceSizeLevel::k64K);
    EXPECT_EQ(SelectVChunkSliceSize(256U * 1024U - 1, false),
              VCSliceSizeLevel::k64K);
    EXPECT_EQ(SelectVChunkSliceSize(256U * 1024U, false),
              VCSliceSizeLevel::k256K);
    EXPECT_EQ(SelectVChunkSliceSize(1024U * 1024U - 1, false),
              VCSliceSizeLevel::k256K);
    EXPECT_EQ(SelectVChunkSliceSize(1024U * 1024U, false),
              VCSliceSizeLevel::k1M);
}

TEST(VChunkConfigTest, SsdAlwaysUsesFourKiB) {
    EXPECT_EQ(SelectVChunkSliceSize(8U * 1024U * 1024U, true),
              VCSliceSizeLevel::k4K);
}

}  // namespace
}  // namespace mooncake
