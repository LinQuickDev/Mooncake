#include "partition/partition_router.h"

#include <utility>

#include <glog/logging.h>

#include "cvm/cvm_keys.h"
#include "etcd_helper.h"
#include "partition/kv_hash_map.h"
#include "ylt/struct_json/json_reader.h"

namespace mooncake {
namespace partition {

void PartitionRouter::LoadSlotOwners(
    const std::vector<cvm::SlotOwner>& owners) {
    std::unordered_map<uint16_t, std::string> next;
    next.reserve(owners.size());
    for (const auto& owner : owners) {
        if (!owner.primary_master_id.empty() &&
            owner.state == static_cast<int32_t>(cvm::SlotState::kStable)) {
            next[owner.slot] = owner.primary_master_id;
        }
    }

    const size_t valid = next.size();
    {
        SharedMutexLocker locker(&mutex_);
        slot_to_submaster_ = std::move(next);
    }
    LOG(INFO) << "PartitionRouter loaded " << valid << " slot->submaster"
              << " entries (input " << owners.size() << " SlotOwner records)";
}

ErrorCode PartitionRouter::LoadFromEtcdSnapshot(
    const std::string& cluster_namespace) {
    const std::string key = cvm::KvViewSnapshotKey(cluster_namespace);
    std::string value;
    EtcdRevisionId revision = 0;
    ErrorCode err = EtcdHelper::Get(key.data(), key.size(), value, revision);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "PartitionRouter read snapshot failed: " << key
                     << " err=" << err;
        return err;
    }

    cvm::KvViewSnapshot snapshot;
    try {
        struct_json::from_json(snapshot, value);
    } catch (const std::exception& e) {
        LOG(ERROR) << "PartitionRouter deserialize snapshot failed: "
                   << e.what();
        return ErrorCode::DESERIALIZE_FAIL;
    }

    LoadSlotOwners(snapshot.slot_owners);
    LOG(INFO) << "PartitionRouter refreshed from snapshot " << key
              << " version=" << snapshot.version;
    return ErrorCode::OK;
}

std::optional<std::string> PartitionRouter::ResolveSubmaster(
    uint16_t slot) const {
    SharedMutexLocker locker(&mutex_, shared_lock);
    auto it = slot_to_submaster_.find(slot);
    if (it == slot_to_submaster_.end()) {
        LOG(WARNING) << "PartitionRouter no submaster for slot " << slot;
        return std::nullopt;
    }
    return it->second;
}

std::optional<std::string> PartitionRouter::Route(
    const TenantId& tenant, const std::string& key) const {
    return ResolveSubmaster(KvHashMap::Compute(tenant, key));
}

void PartitionRouter::Clear() {
    size_t old_size = 0;
    {
        SharedMutexLocker locker(&mutex_);
        old_size = slot_to_submaster_.size();
        slot_to_submaster_.clear();
    }
    LOG(INFO) << "PartitionRouter cleared " << old_size << " entries";
}

size_t PartitionRouter::Size() const {
    SharedMutexLocker locker(&mutex_, shared_lock);
    return slot_to_submaster_.size();
}

}  // namespace partition
}  // namespace mooncake
