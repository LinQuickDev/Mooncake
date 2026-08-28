#include "partition/partition_router.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "cvm/slot_hash.h"
#include "tenant_id.h"

namespace mooncake::test {
namespace {

cvm::SlotOwner MakeOwner(uint16_t slot, const std::string& primary) {
    cvm::SlotOwner owner;
    owner.slot = slot;
    owner.primary_master_id = primary;
    owner.state = static_cast<int32_t>(cvm::SlotState::kStable);
    return owner;
}

TEST(PartitionRouterTest, LoadAndResolve) {
    partition::PartitionRouter router;
    router.LoadSlotOwners({MakeOwner(10, "m-a"), MakeOwner(20, "m-b")});

    EXPECT_EQ(router.Size(), 2u);
    ASSERT_TRUE(router.ResolveSubmaster(10).has_value());
    EXPECT_EQ(*router.ResolveSubmaster(10), "m-a");
    ASSERT_TRUE(router.ResolveSubmaster(20).has_value());
    EXPECT_EQ(*router.ResolveSubmaster(20), "m-b");
}

TEST(PartitionRouterTest, MissingSlotReturnsNullopt) {
    partition::PartitionRouter router;
    router.LoadSlotOwners({MakeOwner(10, "m-a")});
    EXPECT_FALSE(router.ResolveSubmaster(11).has_value());
}

TEST(PartitionRouterTest, SkipsEmptyPrimary) {
    partition::PartitionRouter router;
    router.LoadSlotOwners({MakeOwner(10, ""), MakeOwner(11, "m-b")});

    EXPECT_EQ(router.Size(), 1u);
    EXPECT_FALSE(router.ResolveSubmaster(10).has_value());
    EXPECT_TRUE(router.ResolveSubmaster(11).has_value());
}

TEST(PartitionRouterTest, OverwritesOnReload) {
    partition::PartitionRouter router;
    router.LoadSlotOwners({MakeOwner(10, "m-a")});
    router.LoadSlotOwners({MakeOwner(10, "m-b"), MakeOwner(20, "m-c")});

    EXPECT_EQ(router.Size(), 2u);
    ASSERT_TRUE(router.ResolveSubmaster(10).has_value());
    EXPECT_EQ(*router.ResolveSubmaster(10), "m-b");
    EXPECT_TRUE(router.ResolveSubmaster(20).has_value());
}

TEST(PartitionRouterTest, RouteUsesKeySlot) {
    partition::PartitionRouter router;
    const std::string key = "route-me";
    const TenantId tenant("tenant-r");
    const uint16_t slot = cvm::KeySlot(tenant, key);
    router.LoadSlotOwners({MakeOwner(slot, "m-target")});

    ASSERT_TRUE(router.Route(tenant, key).has_value());
    EXPECT_EQ(*router.Route(tenant, key), "m-target");
}

TEST(PartitionRouterTest, ClearAndSize) {
    partition::PartitionRouter router;
    router.LoadSlotOwners({MakeOwner(10, "m-a"), MakeOwner(20, "m-b")});
    EXPECT_EQ(router.Size(), 2u);

    router.Clear();
    EXPECT_EQ(router.Size(), 0u);
    EXPECT_FALSE(router.ResolveSubmaster(10).has_value());
}

}  // namespace
}  // namespace mooncake::test
