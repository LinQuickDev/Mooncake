#include "master_service.h"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "master_config.h"
#include "types.h"

namespace mooncake {
namespace {

Segment MakeVChunkSegment(const std::string& name, uintptr_t base) {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = name;
    segment.base = base;
    segment.size = 64U * 1024U * 1024U;
    segment.te_endpoint = name;
    return segment;
}

TEST(VChunkMasterServiceTest, BuilderPropagatesVChunkConfiguration) {
    VChunkConfig vchunk_config;
    vchunk_config.enabled = true;
    vchunk_config.max_slice_count = 128;
    auto metadata_store = std::make_shared<InMemoryVChunkMetadataStore>();
    const auto config = MasterServiceConfig::builder()
                            .set_vchunk_config(vchunk_config)
                            .set_vchunk_metadata_store(metadata_store)
                            .build();
    EXPECT_TRUE(config.vchunk_config.enabled);
    EXPECT_EQ(config.vchunk_config.max_slice_count, 128U);
    EXPECT_EQ(config.vchunk_metadata_store, metadata_store);
}

TEST(VChunkMasterServiceTest, DisabledModeRejectsAllControlPlaneOperations) {
    MasterServiceConfig config;
    config.vchunk_config.enabled = false;
    MasterService service(config);
    const TenantId tenant("tenant");

    EXPECT_EQ(service.VChunkPutStart(tenant, "key", 4096, false, 1).error(),
              ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    EXPECT_EQ(service.VChunkPutEnd(tenant, "key", "id", 2),
              ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    EXPECT_EQ(service.VChunkPutRevoke(tenant, "key", "id"),
              ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    EXPECT_EQ(service.GetVChunk(tenant, "key").error(),
              ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    EXPECT_EQ(service.AcquireVChunkRead(tenant, "key").error(),
              ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    EXPECT_EQ(service.RemoveVChunk(tenant, "key", 3),
              ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    EXPECT_EQ(service.ReapExpiredVChunks(4, 1).error(),
              ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

TEST(VChunkMasterServiceTest, PartitionedModeRejectsBeforeSlotViewIsReady) {
    MasterServiceConfig config;
    config.enable_ha = true;
    config.ha_backend_type = "etcd";
    config.submaster_count = 2;
    config.vchunk_config.enabled = true;
    MasterService service(config);

    EXPECT_EQ(service
                  .VChunkPutStart(TenantId("tenant"), "key", 4096, false, 1)
                  .error(),
              ErrorCode::SLOT_NOT_OWNED);
    EXPECT_EQ(service.GetVChunk(TenantId("tenant"), "key").error(),
              ErrorCode::SLOT_NOT_OWNED);
}

TEST(VChunkMasterServiceTest, BackgroundReaperStopsAndCleansExpiredCreating) {
    MasterServiceConfig config;
    config.memory_allocator = BufferAllocatorType::OFFSET;
    config.vchunk_config.enabled = true;
    config.vchunk_config.creating_timeout_ms = 1;
    config.vchunk_config.reaper_interval_ms = 5;
    config.vchunk_config.reaper_max_scan = 4;
    MasterService service(config);
    ASSERT_TRUE(service
                    .MountSegment(
                        MakeVChunkSegment("reaper-segment", 0xE00000000ULL),
                        generate_uuid())
                    .has_value());
    const TenantId tenant("tenant");
    ASSERT_TRUE(
        service.VChunkPutStart(tenant, "expired", 4096, false, 0).has_value());
    for (int i = 0; i < 100; ++i) {
        if (service.GetVChunk(tenant, "expired").error() ==
            ErrorCode::OBJECT_NOT_FOUND) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    EXPECT_EQ(service.GetVChunk(tenant, "expired").error(),
              ErrorCode::OBJECT_NOT_FOUND);
    const auto metrics = service.GetVChunkMetrics();
    EXPECT_EQ(metrics.states[static_cast<size_t>(VChunkStatus::CREATING)], 0U);
    EXPECT_GE(metrics.rollbacks, 1U);
}

TEST(VChunkMasterServiceTest, ExposesIsolatedVChunkControlPlane) {
    MasterServiceConfig config;
    config.memory_allocator = BufferAllocatorType::OFFSET;
    config.vchunk_config.enabled = true;
    MasterService service(config);
    const auto client_id = generate_uuid();
    ASSERT_TRUE(service
                    .MountSegment(
                        MakeVChunkSegment("vchunk-segment-a", 0x900000000ULL),
                        client_id)
                    .has_value());
    ASSERT_TRUE(service
                    .MountSegment(
                        MakeVChunkSegment("vchunk-segment-b", 0xA00000000ULL),
                        client_id)
                    .has_value());

    const TenantId tenant("tenant-a");
    auto created =
        service.VChunkPutStart(tenant, "key", 10U * 1024U, false, 100);
    ASSERT_TRUE(created.has_value());
    EXPECT_EQ(created->row_size, 2U);
    EXPECT_EQ(service.VChunkPutEnd(tenant, "key", created->vchunk_id, 200),
              ErrorCode::OK);

    auto active = service.GetVChunk(tenant, "key");
    ASSERT_TRUE(active.has_value());
    EXPECT_EQ(active->status, VChunkStatus::ACTIVE);
    auto remote_read = service.AcquireVChunkReadLease(tenant, "key", 250);
    ASSERT_TRUE(remote_read.has_value());
    EXPECT_FALSE(remote_read->lease_id.empty());
    EXPECT_EQ(remote_read->record.vchunk_id, created->vchunk_id);
    EXPECT_EQ(service.RemoveVChunk(tenant, "key", 300), ErrorCode::OK);
    EXPECT_EQ(service.ReleaseVChunkReadLease(remote_read->lease_id),
              ErrorCode::OK);
    EXPECT_EQ(service.ReleaseVChunkReadLease(remote_read->lease_id),
              ErrorCode::OK);
    EXPECT_EQ(service.ReleaseVChunkReadLease(""), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(service.GetVChunk(tenant, "key").error(),
              ErrorCode::OBJECT_NOT_FOUND);
}

}  // namespace
}  // namespace mooncake
