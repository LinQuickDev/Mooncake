#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "cvm/cvm_types.h"
#include "mutex.h"
#include "tenant_id.h"
#include "types.h"

namespace mooncake {
namespace partition {

// client 侧路由：应用 SlotOwner 映射，把逻辑 slot 解析为 submaster_id。
// 映射来源为 etcd 快照（snapshot/kv_view，即 KvViewSnapshot），直读 etcd。
class PartitionRouter {
   public:
    // 加载 slot → submaster 映射（覆盖式）。
    void LoadSlotOwners(const std::vector<cvm::SlotOwner>& owners);

    // 直读 etcd 快照（snapshot/kv_view）并刷新映射。
    ErrorCode LoadFromEtcdSnapshot(const std::string& cluster_namespace);

    // slot → submaster_id（primary_master_id）；未命中返回 nullopt。
    std::optional<std::string> ResolveSubmaster(uint16_t slot) const;

    // key → submaster_id（先哈希再路由）；未命中返回 nullopt。
    std::optional<std::string> Route(const TenantId& tenant,
                                     const std::string& key) const;

    void Clear();
    size_t Size() const;

   private:
    mutable SharedMutex mutex_;
    std::unordered_map<uint16_t, std::string> slot_to_submaster_;
};

}  // namespace partition
}  // namespace mooncake
