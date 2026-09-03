#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "crc32c.h"
#include "tenant_id.h"

namespace mooncake {
namespace cvm {

// Number of logical slots, aligned with Redis Cluster (16384 == 2^14).
constexpr uint16_t kSlotCount = 16384;
constexpr uint16_t kSlotMask = kSlotCount - 1;  // 16383

// Number of virtual nodes per master on the consistent-hash ring used for
// dynamic slot ownership. More vnodes yield a more balanced distribution at
// the cost of a larger ring to build/sort on each heartbeat. 128 keeps load
// skew low (~1%) while remaining cheap to recompute.
constexpr uint16_t kVnodeCount = 128;

// Maps a stable hash value to a slot by taking the low 14 bits. This is
// equivalent to `hash % kSlotCount` but cheaper.
inline uint16_t SlotOf(uint32_t hash) {
    return static_cast<uint16_t>(hash & kSlotMask);
}

// Computes the logical slot for a tenant-scoped key.
//
// - Default tenant: slot = hash(user_key).
// - Non-default tenant: slot = hash(tenant + '\0' + user_key), so that keys
//   from different tenants are isolated while remaining stable across
//   processes/compilers (unlike std::hash).
inline uint16_t KeySlot(const TenantId& tenant, const std::string& user_key) {
    Crc32c crc;
    if (!tenant.IsDefault()) {
        crc.Extend(tenant.value().data(), tenant.value().size());
        constexpr char kSeparator = '\0';
        crc.Extend(&kSeparator, 1);
    }
    crc.Extend(user_key.data(), user_key.size());
    return SlotOf(crc.Final());
}

// Position of a master's virtual node on the consistent-hash ring, in
// [0, kSlotCount). Placement depends only on (master_id, vnode_index), so it
// is deterministic across processes/compilers and independent of the current
// master set. The vnode index is encoded as fixed little-endian bytes so the
// hash is stable regardless of host endianness.
inline uint16_t VNodePosition(const std::string& master_id, uint16_t vnode) {
    Crc32c crc;
    crc.Extend(master_id.data(), master_id.size());
    const uint8_t vnode_bytes[2] = {
        static_cast<uint8_t>(vnode & 0xFFu),
        static_cast<uint8_t>((vnode >> 8) & 0xFFu),
    };
    crc.Extend(reinterpret_cast<const char*>(vnode_bytes),
               sizeof(vnode_bytes));
    return SlotOf(crc.Final());
}

// 一致性哈希环分配：给定去重后的 primary master_id 列表 ids 与本机
// master_id，返回本机应拥有的 slot 集合。每个 primary 在环上放置
// kVnodeCount 个虚拟节点，slot 归属「顺时针最近的虚拟节点」。ids 应为
// 排序去重后的 primary 列表，且必须包含 master_id。
//
// 虚拟节点位置只依赖 (master_id, vnode_index)，因此 primary 增删时仅该
// primary 虚拟节点覆盖的 slot（约 1/n）发生迁移，其余 primary 的 slot 保持
// 不变，避免 naive 均分导致的「全员 slot 平移」。
inline std::vector<uint16_t> ResolveOwnedSlotsOnRing(
    const std::vector<std::string>& ids, const std::string& master_id) {
    const size_t n = ids.size();
    if (n <= 1) {
        std::vector<uint16_t> slots;
        slots.reserve(kSlotCount);
        for (uint16_t s = 0; s < kSlotCount; ++s) {
            slots.push_back(s);
        }
        return slots;
    }

    struct VNode {
        uint16_t position;
        size_t owner_index;  // 指向 ids
    };
    std::vector<VNode> ring;
    ring.reserve(n * kVnodeCount);
    for (size_t i = 0; i < n; ++i) {
        for (uint16_t v = 0; v < kVnodeCount; ++v) {
            ring.push_back({VNodePosition(ids[i], v), i});
        }
    }
    // 稳定排序（position 相同按 owner_index）保证跨进程结果一致。
    std::sort(ring.begin(), ring.end(), [](const VNode& a, const VNode& b) {
        if (a.position != b.position) {
            return a.position < b.position;
        }
        return a.owner_index < b.owner_index;
    });

    std::vector<uint16_t> slots;
    slots.reserve(kSlotCount / n + 1);
    for (uint16_t s = 0; s < kSlotCount; ++s) {
        // 环上第一个 position >= s 的虚拟节点（越界则环绕到 ring[0]）。
        auto it = std::lower_bound(
            ring.begin(), ring.end(), s,
            [](const VNode& vn, uint16_t value) { return vn.position < value; });
        if (it == ring.end()) {
            it = ring.begin();
        }
        if (ids[it->owner_index] == master_id) {
            slots.push_back(s);
        }
    }
    return slots;
}

// 一致性哈希环反查：给定去重排序后的 primary master_id 列表 ids 与单个
// slot，返回该 slot 的 owner master_id。规则与 ResolveOwnedSlotsOnRing 一致
// （slot 归属顺时针最近的虚拟节点）。ids 为空返回空串，n == 1 直接返回
// 唯一成员（单主全量接管）。供读路径转发（非 owner → slot owner）使用。
inline std::string ResolveSlotOwnerOnRing(const std::vector<std::string>& ids,
                                          uint16_t slot) {
    const size_t n = ids.size();
    if (n == 0) {
        return {};
    }
    if (n == 1) {
        return ids[0];
    }

    struct VNode {
        uint16_t position;
        size_t owner_index;  // 指向 ids
    };
    std::vector<VNode> ring;
    ring.reserve(n * kVnodeCount);
    for (size_t i = 0; i < n; ++i) {
        for (uint16_t v = 0; v < kVnodeCount; ++v) {
            ring.push_back({VNodePosition(ids[i], v), i});
        }
    }
    // 稳定排序（position 相同按 owner_index）保证跨进程结果一致。
    std::sort(ring.begin(), ring.end(), [](const VNode& a, const VNode& b) {
        if (a.position != b.position) {
            return a.position < b.position;
        }
        return a.owner_index < b.owner_index;
    });

    auto it = std::lower_bound(
        ring.begin(), ring.end(), slot,
        [](const VNode& vn, uint16_t value) { return vn.position < value; });
    if (it == ring.end()) {
        it = ring.begin();
    }
    return ids[it->owner_index];
}

}  // namespace cvm
}  // namespace mooncake
