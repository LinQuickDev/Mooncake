#include "vchunk_master_manager.h"

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "allocation_strategy.h"
#include "vchunk_test_allocator.h"

namespace mooncake {
namespace {

using test::VChunkTestAllocator;

struct ManagerFixture {
    AllocatorManager allocators;
    std::shared_ptr<VChunkTestAllocator> first =
        std::make_shared<VChunkTestAllocator>("segment-a", 0x700000000ULL,
                                              1024U * 1024U);
    std::shared_ptr<VChunkTestAllocator> second =
        std::make_shared<VChunkTestAllocator>("segment-b", 0x800000000ULL,
                                              1024U * 1024U);

    ManagerFixture() {
        allocators.addAllocator("segment-a", first);
        allocators.addAllocator("segment-b", second);
    }
};

VChunkConfig EnabledConfig() {
    VChunkConfig config;
    config.enabled = true;
    return config;
}

TEST(VChunkMasterManagerTest, RunsPutGetRemoveLifecycle) {
    ManagerFixture fixture;
    VChunkMasterManager manager(EnabledConfig());
    const TenantId tenant("tenant-a");

    auto created = manager.PutStart(fixture.allocators, tenant, "key", 10U * 1024U,
                                    false, 100);
    ASSERT_TRUE(created.has_value());
    EXPECT_EQ(created->status, VChunkStatus::CREATING);
    EXPECT_EQ(created->slice_count, 3U);
    EXPECT_EQ(created->row_size, 2U);
    EXPECT_FALSE(manager.Get(tenant, "key").has_value());

    EXPECT_EQ(manager.PutEnd(tenant, "key", "wrong-id", 200),
              ErrorCode::INVALID_VERSION);
    EXPECT_EQ(manager.PutEnd(tenant, "key", created->vchunk_id, 200),
              ErrorCode::OK);
    EXPECT_EQ(manager.PutEnd(tenant, "key", created->vchunk_id, 200),
              ErrorCode::OK);

    auto active = manager.Get(tenant, "key");
    ASSERT_TRUE(active.has_value());
    EXPECT_EQ(active->status, VChunkStatus::ACTIVE);
    for (const auto& slice : active->slices) {
        EXPECT_EQ(slice.status, VCSliceStatus::COMPLETED);
    }

    EXPECT_EQ(manager.Remove(tenant, "key", 300), ErrorCode::OK);
    EXPECT_EQ(manager.Remove(tenant, "key", 301), ErrorCode::OK);
    EXPECT_EQ(manager.SizeForTesting(), 0U);
    EXPECT_EQ(fixture.first->size(), 0U);
    EXPECT_EQ(fixture.second->size(), 0U);
}

TEST(VChunkMasterManagerTest, RevokeIsIdempotentAndReleasesBuffers) {
    ManagerFixture fixture;
    VChunkMasterManager manager(EnabledConfig());
    const TenantId tenant("tenant-a");
    auto created = manager.PutStart(fixture.allocators, tenant, "key", 8192,
                                    false, 100);
    ASSERT_TRUE(created.has_value());
    EXPECT_GT(fixture.first->size() + fixture.second->size(), 0U);

    EXPECT_EQ(manager.PutRevoke(tenant, "key", created->vchunk_id),
              ErrorCode::OK);
    EXPECT_EQ(manager.PutRevoke(tenant, "key", created->vchunk_id),
              ErrorCode::OK);
    EXPECT_EQ(fixture.first->size() + fixture.second->size(), 0U);
}

TEST(VChunkMasterManagerTest, ConcurrentPutStartPublishesOneObject) {
    ManagerFixture fixture;
    VChunkMasterManager manager(EnabledConfig());
    const TenantId tenant("tenant-a");
    std::atomic<int> success{0};
    std::atomic<int> already_exists{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&] {
            auto result = manager.PutStart(fixture.allocators, tenant, "key",
                                           8192, false, 100);
            if (result.has_value()) {
                ++success;
            } else if (result.error() == ErrorCode::OBJECT_ALREADY_EXISTS) {
                ++already_exists;
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(success.load(), 1);
    EXPECT_EQ(already_exists.load(), 7);
    EXPECT_EQ(manager.SizeForTesting(), 1U);
}

TEST(VChunkMasterManagerTest, IsolatesSameKeyAcrossTenants) {
    ManagerFixture fixture;
    VChunkMasterManager manager(EnabledConfig());
    auto first = manager.PutStart(fixture.allocators, TenantId("tenant-a"),
                                  "key", 4096, false, 100);
    auto second = manager.PutStart(fixture.allocators, TenantId("tenant-b"),
                                   "key", 4096, false, 100);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_NE(first->vchunk_id, second->vchunk_id);
    EXPECT_EQ(manager.SizeForTesting(), 2U);
}

TEST(VChunkMasterManagerTest, GetAndRemoveAreSerializedSafely) {
    ManagerFixture fixture;
    VChunkMasterManager manager(EnabledConfig());
    const TenantId tenant("tenant-a");
    auto created = manager.PutStart(fixture.allocators, tenant, "key", 8192,
                                    false, 100);
    ASSERT_TRUE(created.has_value());
    ASSERT_EQ(manager.PutEnd(tenant, "key", created->vchunk_id, 200),
              ErrorCode::OK);

    std::atomic<bool> stop{false};
    std::atomic<int> valid_reads{0};
    std::thread reader([&] {
        while (!stop.load()) {
            auto result = manager.Get(tenant, "key");
            if (result.has_value()) {
                EXPECT_EQ(result->status, VChunkStatus::ACTIVE);
                ++valid_reads;
            } else {
                EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
            }
        }
    });
    EXPECT_EQ(manager.Remove(tenant, "key", 300), ErrorCode::OK);
    stop.store(true);
    reader.join();

    EXPECT_EQ(manager.SizeForTesting(), 0U);
    EXPECT_EQ(fixture.first->size() + fixture.second->size(), 0U);
}

TEST(VChunkMasterManagerTest, ReadLeaseDefersBufferReleaseAfterRemove) {
    ManagerFixture fixture;
    VChunkMasterManager manager(EnabledConfig());
    const TenantId tenant("tenant-a");
    auto created = manager.PutStart(fixture.allocators, tenant, "key", 8192,
                                    false, 100);
    ASSERT_TRUE(created.has_value());
    ASSERT_EQ(manager.PutEnd(tenant, "key", created->vchunk_id, 200),
              ErrorCode::OK);

    const auto allocated = fixture.first->size() + fixture.second->size();
    ASSERT_GT(allocated, 0U);
    {
        auto read = manager.AcquireRead(tenant, "key");
        ASSERT_TRUE(read.has_value());
        EXPECT_EQ(manager.Remove(tenant, "key", 300), ErrorCode::OK);
        EXPECT_FALSE(manager.Get(tenant, "key").has_value());
        EXPECT_EQ(fixture.first->size() + fixture.second->size(), allocated);
    }
    EXPECT_EQ(fixture.first->size() + fixture.second->size(), 0U);
}

TEST(VChunkMasterManagerTest, DisabledConfigurationRejectsCreation) {
    ManagerFixture fixture;
    VChunkMasterManager manager(VChunkConfig{});
    auto result = manager.PutStart(fixture.allocators, TenantId::Default(),
                                   "key", 4096, false, 100);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST(VChunkMasterManagerTest, PiercingVersionRejectsSsdSegments) {
    ManagerFixture fixture;
    VChunkMasterManager manager(EnabledConfig());
    auto result = manager.PutStart(fixture.allocators, TenantId::Default(),
                                   "key", 4096, true, 100);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(fixture.first->size() + fixture.second->size(), 0U);
}

}  // namespace
}  // namespace mooncake
