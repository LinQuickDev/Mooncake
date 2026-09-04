#pragma once

#include <cstdint>
#include <string>

#include "cvm/slot_hash.h"
#include "tenant_id.h"

namespace mooncake {
namespace partition {

// client 侧哈希分片：key → 逻辑 slot（0..16383）。
// 复用 cvm::KeySlot（CRC32C），保证与 submaster 内部 getSlot 使用同一份实现，
// 使 hash 相同的 key 必然落到同一个 submaster。
class KvHashMap {
   public:
    static uint16_t Compute(const TenantId& tenant, const std::string& key) {
        return cvm::KeySlot(tenant, key);
    }
};

}  // namespace partition
}  // namespace mooncake
