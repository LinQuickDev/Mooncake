#include "vchunk_allocation_strategy.h"

#include <gtest/gtest.h>

#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

#include "allocation_strategy.h"
#include "vchunk_test_allocator.h"

namespace mooncake {
namespace {

using test::VChunkTestAllocator;

TEST(VChunkAllocationStrategyTest, AllocatesTransposeRowsAndRollsBack) {
    AllocatorManager manager;
    std::vector<std::shared_ptr<VChunkTestAllocator>> allocators;
    for (size_t i = 0; i < 3; ++i) {
        auto allocator = std::make_shared<VChunkTestAllocator>(
            "segment-" + std::to_string(i), 0x100000000ULL + i * 0x100000,
            64U * 1024U);
        manager.addAllocator(allocator->getSegmentName(), allocator);
        allocators.push_back(std::move(allocator));
    }

    {
        auto result = AllocateVChunk(manager, 9U * 4096U,
                                     VCSliceSizeLevel::k4K);
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result->row_size, 3U);
        ASSERT_EQ(result->allocations.size(), 9U);
        for (size_t row = 0; row < 3; ++row) {
            std::unordered_set<std::string> segments;
            for (size_t column = 0; column < 3; ++column) {
                const auto& allocation =
                    result->allocations[row * 3 + column];
                EXPECT_TRUE(segments.insert(allocation.segment_name).second);
                EXPECT_EQ(allocation.slice_index, row * 3 + column);
                EXPECT_EQ(allocation.logical_length, 4096U);
                EXPECT_EQ(allocation.allocated_length, 4096U);
                EXPECT_NE(allocation.buffer, nullptr);
            }
        }
        for (const auto& allocator : allocators) {
            EXPECT_EQ(allocator->size(), 3U * 4096U);
        }
    }
    for (const auto& allocator : allocators) {
        EXPECT_EQ(allocator->size(), 0U);
    }
}

TEST(VChunkAllocationStrategyTest, TracksShortFinalLogicalSlice) {
    AllocatorManager manager;
    auto allocator = std::make_shared<VChunkTestAllocator>(
        "segment-a", 0x200000000ULL, 64U * 1024U);
    manager.addAllocator("segment-a", allocator);

    auto result =
        AllocateVChunk(manager, 10U * 1024U, VCSliceSizeLevel::k4K);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->allocations.size(), 3U);
    EXPECT_EQ(result->allocations.back().logical_length, 2048U);
    EXPECT_EQ(result->allocations.back().allocated_length, 4096U);
}

TEST(VChunkAllocationStrategyTest, HonorsExcludedSegments) {
    AllocatorManager manager;
    auto first = std::make_shared<VChunkTestAllocator>(
        "segment-a", 0x300000000ULL, 64U * 1024U);
    auto second = std::make_shared<VChunkTestAllocator>(
        "segment-b", 0x400000000ULL, 64U * 1024U);
    manager.addAllocator("segment-a", first);
    manager.addAllocator("segment-b", second);

    auto result = AllocateVChunk(manager, 8192, VCSliceSizeLevel::k4K,
                                 std::set<std::string>{"segment-a"});
    ASSERT_TRUE(result.has_value());
    for (const auto& allocation : result->allocations) {
        EXPECT_EQ(allocation.segment_name, "segment-b");
    }
    EXPECT_EQ(first->size(), 0U);
}

TEST(VChunkAllocationStrategyTest, PartialFailureRollsBackAllBuffers) {
    AllocatorManager manager;
    auto first = std::make_shared<VChunkTestAllocator>(
        "segment-a", 0x500000000ULL, 4096);
    auto second = std::make_shared<VChunkTestAllocator>(
        "segment-b", 0x600000000ULL, 4096);
    manager.addAllocator("segment-a", first);
    manager.addAllocator("segment-b", second);

    auto result =
        AllocateVChunk(manager, 3U * 4096U, VCSliceSizeLevel::k4K);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_EQ(first->size(), 0U);
    EXPECT_EQ(second->size(), 0U);
}

TEST(VChunkAllocationStrategyTest, RejectsInvalidAndEmptyInputs) {
    AllocatorManager manager;
    auto empty = AllocateVChunk(manager, 4096, VCSliceSizeLevel::k4K);
    EXPECT_FALSE(empty.has_value());
    EXPECT_EQ(empty.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    auto zero = AllocateVChunk(manager, 0, VCSliceSizeLevel::k4K);
    EXPECT_FALSE(zero.has_value());
    EXPECT_EQ(zero.error(), ErrorCode::INVALID_PARAMS);
}

}  // namespace
}  // namespace mooncake
