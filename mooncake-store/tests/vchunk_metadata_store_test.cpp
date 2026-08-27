#include "vchunk_metadata_store.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "allocation_strategy.h"
#include "master_service.h"
#include "vchunk_master_manager.h"
#include "vchunk_test_allocator.h"

namespace mooncake {
namespace {

class FaultStore final : public VChunkMetadataStore {
   public:
    ErrorCode Put(const VChunkMetadataRecord& record) override {
        if (fail_put) return ErrorCode::ETCD_OPERATION_ERROR;
        for (auto& current : records) {
            if (current.vchunk_id == record.vchunk_id) {
                current = record;
                return ErrorCode::OK;
            }
        }
        records.push_back(record);
        return ErrorCode::OK;
    }
    ErrorCode Remove(const VChunkMetadataRecord& record) override {
        if (fail_remove) return ErrorCode::ETCD_OPERATION_ERROR;
        std::erase_if(records, [&](const auto& current) {
            return current.vchunk_id == record.vchunk_id;
        });
        return ErrorCode::OK;
    }
    tl::expected<std::vector<VChunkMetadataRecord>, ErrorCode> List() override {
        if (fail_list) {
            return tl::make_unexpected(ErrorCode::ETCD_OPERATION_ERROR);
        }
        return records;
    }
    bool IsPersistent() const override { return true; }

    bool fail_put{false};
    bool fail_remove{false};
    bool fail_list{false};
    std::vector<VChunkMetadataRecord> records;
};

struct StoreFixture {
    AllocatorManager allocators;
    std::shared_ptr<test::VChunkTestAllocator> allocator =
        std::make_shared<test::VChunkTestAllocator>(
            "segment", 0xD00000000ULL, 4U * 1024U * 1024U);
    std::shared_ptr<FaultStore> store = std::make_shared<FaultStore>();
    VChunkConfig config;

    StoreFixture() {
        allocators.addAllocator("segment", allocator);
        config.enabled = true;
        config.creating_timeout_ms = 100;
    }
};

TEST(VChunkMetadataStoreTest, UsesStableTenantAndVChunkNamespace) {
    VChunkMetadataRecord record;
    record.tenant_id = "tenant-a";
    record.vchunk_id = "id-a";
    EXPECT_EQ(MakeVChunkMetadataStoreKey(record),
              "/mooncake/vchunk/v1/tenant-a/id-a");
}

TEST(VChunkMetadataStoreTest, PutStartIsNotVisibleWhenDurableWriteFails) {
    StoreFixture fixture;
    fixture.store->fail_put = true;
    VChunkMasterManager manager(fixture.config, fixture.store);
    auto result = manager.PutStart(fixture.allocators, TenantId("tenant"),
                                   "key", 4096, false, 10);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::ETCD_OPERATION_ERROR);
    EXPECT_EQ(manager.SizeForTesting(), 0U);
    EXPECT_EQ(fixture.allocator->size(), 0U);

    fixture.store->fail_put = false;
    EXPECT_TRUE(manager.PutStart(fixture.allocators, TenantId("tenant"),
                                 "key", 4096, false, 11)
                    .has_value());
}

TEST(VChunkMetadataStoreTest, PersistsCreatingThenActiveBeforeVisibility) {
    StoreFixture fixture;
    VChunkMasterManager manager(fixture.config, fixture.store);
    auto created = manager.PutStart(fixture.allocators, TenantId("tenant"),
                                    "key", 4096, false, 10);
    ASSERT_TRUE(created.has_value());
    ASSERT_EQ(fixture.store->records.size(), 1U);
    EXPECT_EQ(fixture.store->records[0].status, VChunkStatus::CREATING);
    ASSERT_EQ(manager.PutEnd(TenantId("tenant"), "key", created->vchunk_id,
                             20),
              ErrorCode::OK);
    EXPECT_EQ(fixture.store->records[0].status, VChunkStatus::ACTIVE);
}

TEST(VChunkMetadataStoreTest, RecoveryRejectsActiveWithoutAllocatorRestore) {
    StoreFixture fixture;
    VChunkMetadataRecord active;
    {
        VChunkMasterManager writer(fixture.config, fixture.store);
        auto created = writer.PutStart(fixture.allocators, TenantId("tenant"),
                                       "active", 4096, false, 10);
        ASSERT_TRUE(created.has_value());
        ASSERT_EQ(writer.PutEnd(TenantId("tenant"), "active",
                                created->vchunk_id, 20),
                  ErrorCode::OK);
        active = *writer.Get(TenantId("tenant"), "active");
    }
    auto expired = active;
    expired.vchunk_id = "expired";
    expired.key = "expired";
    expired.status = VChunkStatus::CREATING;
    expired.last_updated_at_ms = 10;
    for (auto& slice : expired.slices) {
        slice.status = VCSliceStatus::PENDING;
    }
    ASSERT_EQ(fixture.store->Put(expired), ErrorCode::OK);

    VChunkMasterManager recovered(fixture.config, fixture.store);
    EXPECT_EQ(recovered.Recover(200), ErrorCode::REPLICA_IS_GONE);
    EXPECT_EQ(recovered.SizeForTesting(), 0U);
    // Validation is atomic: no incomplete record is removed when an ACTIVE
    // record makes the whole snapshot unsafe to restore.
    ASSERT_EQ(fixture.store->List()->size(), 2U);
}

TEST(VChunkMetadataStoreTest, RecoveryCleansIncompleteWrites) {
    StoreFixture fixture;
    VChunkMasterManager writer(fixture.config, fixture.store);
    ASSERT_TRUE(writer.PutStart(fixture.allocators, TenantId("tenant"),
                                "creating", 4096, false, 10)
                    .has_value());

    VChunkMasterManager recovered(fixture.config, fixture.store);
    EXPECT_EQ(recovered.Recover(20), ErrorCode::OK);
    EXPECT_TRUE(fixture.store->List()->empty());
}

TEST(VChunkMetadataStoreTest, ReaperIsBoundedAndRetryable) {
    StoreFixture fixture;
    VChunkMasterManager manager(fixture.config, fixture.store);
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(manager.PutStart(fixture.allocators, TenantId("tenant"),
                                     "key-" + std::to_string(i), 4096, false,
                                     10)
                        .has_value());
    }
    auto first = manager.ReapExpired(200, 2);
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(*first, 2U);
    EXPECT_EQ(manager.SizeForTesting(), 1U);
    fixture.store->fail_remove = true;
    EXPECT_EQ(manager.ReapExpired(200, 2).error(),
              ErrorCode::ETCD_OPERATION_ERROR);
    EXPECT_EQ(manager.SizeForTesting(), 1U);
    fixture.store->fail_remove = false;
    EXPECT_EQ(*manager.ReapExpired(200, 2), 1U);
    EXPECT_EQ(manager.SizeForTesting(), 0U);
}

TEST(VChunkMetadataStoreTest, RemoveFailureLeavesReleasingObjectRetryable) {
    StoreFixture fixture;
    VChunkMasterManager manager(fixture.config, fixture.store);
    const TenantId tenant("tenant");
    auto created = manager.PutStart(fixture.allocators, tenant, "key", 4096,
                                    false, 10);
    ASSERT_TRUE(created.has_value());
    ASSERT_EQ(manager.PutEnd(tenant, "key", created->vchunk_id, 20),
              ErrorCode::OK);
    fixture.store->fail_remove = true;
    EXPECT_EQ(manager.Remove(tenant, "key", 30),
              ErrorCode::ETCD_OPERATION_ERROR);
    EXPECT_EQ(manager.Get(tenant, "key").error(),
              ErrorCode::REPLICA_IS_NOT_READY);
    fixture.store->fail_remove = false;
    EXPECT_EQ(manager.Remove(tenant, "key", 31), ErrorCode::OK);
    EXPECT_EQ(manager.SizeForTesting(), 0U);
}

TEST(VChunkMetadataStoreTest, StartupRejectsUnavailableStore) {
    auto store = std::make_shared<FaultStore>();
    store->fail_list = true;
    MasterServiceConfig unavailable;
    unavailable.vchunk_config.enabled = true;
    unavailable.vchunk_metadata_store = store;
    EXPECT_THROW(MasterService service(unavailable), std::runtime_error);
}

TEST(VChunkMetadataStoreTest, StartupAllowsVChunkWithHaEnabled) {
    MasterServiceConfig coexistence;
    coexistence.vchunk_config.enabled = true;
    coexistence.enable_ha = true;

    MasterService service(coexistence);
    EXPECT_TRUE(service.GetVChunkRuntimeInfo().enabled);
}

TEST(VChunkMetadataStoreTest, RecoveryRejectsUnknownSchemaVersion) {
    StoreFixture fixture;
    VChunkMetadataRecord record;
    record.schema_version = kVChunkMetadataSchemaVersion + 1;
    record.vchunk_id = "future";
    record.tenant_id = "tenant";
    record.key = "key";
    fixture.store->records.push_back(record);
    VChunkMasterManager manager(fixture.config, fixture.store);
    EXPECT_EQ(manager.Recover(100), ErrorCode::INVALID_VERSION);
    EXPECT_EQ(manager.SizeForTesting(), 0U);
}

}  // namespace
}  // namespace mooncake
