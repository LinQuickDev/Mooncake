#include "cvm/slot_hash.h"

#include <gtest/gtest.h>

#include <set>
#include <string>
#include <vector>

#include "crc32c.h"
#include "partition/kv_hash_map.h"
#include "tenant_id.h"

namespace mooncake::test {
namespace {

TEST(SlotHashTest, SlotOfUsesLow14Bits) {
    EXPECT_EQ(cvm::SlotOf(0u), 0u);
    EXPECT_EQ(cvm::SlotOf(1u), 1u);
    EXPECT_EQ(cvm::SlotOf(cvm::kSlotCount - 1), cvm::kSlotCount - 1);
    // 第 15 位及以上被丢弃：16384 (2^14) 映射回 0。
    EXPECT_EQ(cvm::SlotOf(cvm::kSlotCount), 0u);
    EXPECT_EQ(cvm::SlotOf(0xFFFFFFFFu), cvm::kSlotMask);
}

TEST(SlotHashTest, KeySlotInRange) {
    const std::vector<std::string> keys = {"", "a", "hello", "key-123",
                                           "a-longer-key-for-coverage"};
    for (const auto& key : keys) {
        EXPECT_LT(cvm::KeySlot(TenantId::Default(), key), cvm::kSlotCount);
        EXPECT_LT(cvm::KeySlot(TenantId("tenant-a"), key), cvm::kSlotCount);
    }
}

TEST(SlotHashTest, KeySlotDeterministic) {
    const std::string key = "deterministic-key";
    const TenantId tenant("tenant-x");
    const uint16_t first = cvm::KeySlot(tenant, key);
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(first, cvm::KeySlot(tenant, key));
    }
}

namespace {
uint16_t ManualDefaultKeySlot(const std::string& key) {
    Crc32c crc;
    crc.Extend(key.data(), key.size());
    return cvm::SlotOf(crc.Final());
}

uint16_t ManualScopedKeySlot(const std::string& tenant,
                             const std::string& key) {
    Crc32c crc;
    crc.Extend(tenant.data(), tenant.size());
    constexpr char kSeparator = '\0';
    crc.Extend(&kSeparator, 1);
    crc.Extend(key.data(), key.size());
    return cvm::SlotOf(crc.Final());
}
}  // namespace

TEST(SlotHashTest, KeySlotMatchesManualCrc) {
    const std::string key = "scope-me";
    // 默认 tenant：slot = hash(user_key)。
    EXPECT_EQ(cvm::KeySlot(TenantId::Default(), key),
              ManualDefaultKeySlot(key));
    // 非默认 tenant：slot = hash(tenant + '\0' + user_key)。
    EXPECT_EQ(cvm::KeySlot(TenantId("tenant-a"), key),
              ManualScopedKeySlot("tenant-a", key));
}

TEST(SlotHashTest, VNodePositionInRangeAndDeterministic) {
    const std::string master = "master-1:10001";
    for (uint16_t vnode = 0; vnode < cvm::kVnodeCount; ++vnode) {
        const uint16_t pos = cvm::VNodePosition(master, vnode);
        EXPECT_LT(pos, cvm::kSlotCount);
        EXPECT_EQ(pos, cvm::VNodePosition(master, vnode));
    }
}

TEST(SlotHashTest, KvHashMapMatchesKeySlot) {
    // client 侧路由（KvHashMap）与 submaster 侧校验（KeySlot）必须用同一份
    // 哈希，否则 key 会路由到错误的 submaster。
    const std::string key = "consistency-key";
    const TenantId tenant("tenant-c");
    EXPECT_EQ(partition::KvHashMap::Compute(tenant, key),
              cvm::KeySlot(tenant, key));
}

TEST(SlotHashTest, ResolveOwnedSlotsOnRingSingleMaster) {
    // n=1 时退化：本机拥有全部 slot。
    const std::vector<std::string> ids = {"m-a"};
    const auto slots = cvm::ResolveOwnedSlotsOnRing(ids, "m-a");
    EXPECT_EQ(slots.size(), static_cast<size_t>(cvm::kSlotCount));
}

TEST(SlotHashTest, ResolveOwnedSlotsOnRingCoversAllSlots) {
    const std::vector<std::string> ids = {"m-a", "m-b", "m-c"};
    std::vector<bool> covered(cvm::kSlotCount, false);
    size_t total = 0;
    for (const auto& id : ids) {
        const auto slots = cvm::ResolveOwnedSlotsOnRing(ids, id);
        for (uint16_t s : slots) {
            EXPECT_FALSE(covered[s]) << "slot " << s << " owned by >1 master";
            covered[s] = true;
            ++total;
        }
    }
    // 每个 slot 恰好归属一个 primary（不重叠 + 全覆盖）。
    EXPECT_EQ(total, static_cast<size_t>(cvm::kSlotCount));
    for (size_t s = 0; s < cvm::kSlotCount; ++s) {
        EXPECT_TRUE(covered[s]) << "slot " << s << " has no owner";
    }
}

TEST(SlotHashTest, ResolveOwnedSlotsOnRingStableOnRemoval) {
    // 一致性哈希环核心性质：移除一个 primary 后，其余 primary 原有 slot 不
    // 丢（只增不减），仅被移除 primary 覆盖的 slot 发生重分配——即「非全员
    // 平移」。
    const std::vector<std::string> ids3 = {"m-a", "m-b", "m-c"};
    const auto a = cvm::ResolveOwnedSlotsOnRing(ids3, "m-a");
    const auto b = cvm::ResolveOwnedSlotsOnRing(ids3, "m-b");

    const std::vector<std::string> ids2 = {"m-a", "m-b"};
    const auto a2 = cvm::ResolveOwnedSlotsOnRing(ids2, "m-a");
    const auto b2 = cvm::ResolveOwnedSlotsOnRing(ids2, "m-b");

    const std::set<uint16_t> a_set(a.begin(), a.end());
    const std::set<uint16_t> a2_set(a2.begin(), a2.end());
    const std::set<uint16_t> b_set(b.begin(), b.end());
    const std::set<uint16_t> b2_set(b2.begin(), b2.end());

    for (uint16_t s : a_set) {
        EXPECT_TRUE(a2_set.count(s)) << "a lost slot " << s << " on removal";
    }
    for (uint16_t s : b_set) {
        EXPECT_TRUE(b2_set.count(s)) << "b lost slot " << s << " on removal";
    }
    // 移除后 a/b 覆盖全量 slot，且总量较移除前增加（接管了被移除者的 slot）。
    EXPECT_EQ(a2_set.size() + b2_set.size(),
              static_cast<size_t>(cvm::kSlotCount));
    EXPECT_GT(a2_set.size() + b2_set.size(), a_set.size() + b_set.size());
}

}  // namespace
}  // namespace mooncake::test
