#include "cvm/slot_migrator.h"

#include <algorithm>
#include <iterator>
#include <utility>

#include <glog/logging.h>

#include "cvm/etcd_view_store.h"

namespace mooncake {
namespace cvm {

SlotMigrator::SlotMigrator(Config config) : config_(std::move(config)) {}

ErrorCode SlotMigrator::PublishMigrating(uint16_t slot) {
    SlotOwner owner;
    owner.slot = slot;
    owner.primary_master_id = config_.master_id;
    owner.state = static_cast<int32_t>(SlotState::kMigrating);
    owner.migrating_to_master_id = config_.master_id;

    if (config_.lease_id != 0) {
        return EtcdViewStore::SaveSlotOwnerWithLease(config_.cluster_namespace,
                                                     owner, config_.lease_id);
    }
    return EtcdViewStore::SaveSlotOwner(config_.cluster_namespace, owner);
}

ErrorCode SlotMigrator::PublishStable(uint16_t slot) {
    SlotOwner owner;
    owner.slot = slot;
    owner.primary_master_id = config_.master_id;
    owner.state = static_cast<int32_t>(SlotState::kStable);
    // migrating_to_master_id 留空。

    if (config_.lease_id != 0) {
        return EtcdViewStore::SaveSlotOwnerWithLease(config_.cluster_namespace,
                                                     owner, config_.lease_id);
    }
    return EtcdViewStore::SaveSlotOwner(config_.cluster_namespace, owner);
}

ErrorCode SlotMigrator::Reconcile(const std::vector<uint16_t>& owned_slots) {
    std::vector<uint16_t> cur = owned_slots;
    std::vector<uint16_t> prev = last_owned_slots_;
    std::sort(cur.begin(), cur.end());
    std::sort(prev.begin(), prev.end());

    std::vector<uint16_t> gained;
    std::vector<uint16_t> released;
    std::set_difference(cur.begin(), cur.end(), prev.begin(), prev.end(),
                        std::back_inserter(gained));
    std::set_difference(prev.begin(), prev.end(), cur.begin(), cur.end(),
                        std::back_inserter(released));

    ErrorCode last_err = ErrorCode::OK;

    // 释放：先回调清理元数据，再条件删除残留记录（确认仍指向本机才删）。
    for (uint16_t slot : released) {
        if (on_release_) {
            on_release_(slot);
        }
        ErrorCode err = EtcdViewStore::DeleteSlotOwnerIfOwnedBy(
            config_.cluster_namespace, slot, config_.master_id);
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "SlotMigrator release slot " << slot
                         << " failed: " << err;
            last_err = err;
        }
    }
    // 释放成功的聚合记录：仅在 owned 集合变化时打印一条，便于确认交接完成。
    if (!released.empty()) {
        LOG(INFO) << "SlotMigrator released " << released.size()
                  << " slot(s), master_id=" << config_.master_id;
    }

    // 获得：kMigrating -> on_acquire -> kStable 两段式交接。
    for (uint16_t slot : gained) {
        ErrorCode err = PublishMigrating(slot);
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "SlotMigrator publish migrating slot " << slot
                         << " failed: " << err;
            last_err = err;
            continue;
        }
        if (on_acquire_) {
            on_acquire_(slot);
        }
        err = PublishStable(slot);
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "SlotMigrator publish stable slot " << slot
                         << " failed: " << err;
            last_err = err;
        }
    }
    // 获得成功的聚合记录：确认 kMigrating -> kStable 交接已全部落盘。
    if (!gained.empty()) {
        LOG(INFO) << "SlotMigrator acquired " << gained.size()
                  << " slot(s) via kMigrating->kStable, master_id="
                  << config_.master_id;
    }

    // 不变 slot：幂等 reaffirm kStable。
    for (uint16_t slot : cur) {
        if (std::binary_search(gained.begin(), gained.end(), slot)) {
            continue;
        }
        ErrorCode err = PublishStable(slot);
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "SlotMigrator reaffirm slot " << slot
                         << " failed: " << err;
            last_err = err;
        }
    }

    last_owned_slots_ = std::move(cur);
    return last_err;
}

}  // namespace cvm
}  // namespace mooncake
