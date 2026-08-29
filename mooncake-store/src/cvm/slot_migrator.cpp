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
    // fencing：写入前检查 etcd 中现 owner。记录存在即说明原 owner 的
    // lease 仍然有效（slot key 附着在其 lease 上，lease 过期 etcd 会自动
    // 删除记录）——此时绝不抢夺，避免两个 submaster 对同一 slot 反复
    // 互相覆盖（slot 分布震荡不收敛的根因）。仅当记录已消失（原 owner
    // 崩溃/lease 过期/主动释放）时才允许本机接管。
    std::vector<uint16_t> fenced;
    for (uint16_t slot : gained) {
        SlotOwner current;
        ViewVersionId current_version = 0;
        ErrorCode load_err =
            EtcdViewStore::LoadSlotOwner(config_.cluster_namespace, slot,
                                         current, current_version);
        if (load_err == ErrorCode::OK &&
            !current.primary_master_id.empty() &&
            current.primary_master_id != config_.master_id) {
            fenced.push_back(slot);
            continue;
        }
        if (load_err != ErrorCode::OK &&
            load_err != ErrorCode::ETCD_KEY_NOT_EXIST) {
            // etcd 读取异常：保守跳过，待下轮重试，不做盲目覆盖。
            fenced.push_back(slot);
            last_err = load_err;
            LOG(WARNING) << "SlotMigrator fence check slot " << slot
                         << " failed: " << load_err
                         << ", master_id=" << config_.master_id;
            continue;
        }

        ErrorCode err = PublishMigrating(slot);
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "SlotMigrator publish migrating slot " << slot
                         << " failed: " << err;
            last_err = err;
            fenced.push_back(slot);  // 发布失败同样下轮重试
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
    if (!gained.empty()) {
        const size_t acquired = gained.size() - fenced.size();
        if (acquired > 0) {
            // 获得成功的聚合记录：确认 kMigrating -> kStable 交接已全部落盘。
            LOG(INFO) << "SlotMigrator acquired " << acquired
                      << " slot(s) via kMigrating->kStable, master_id="
                      << config_.master_id;
        }
        if (!fenced.empty()) {
            // fencing 拦截汇总：这些 slot 仍由其他存活 master 持有，本机
            // 观察等待，直到对方释放（记录消失）后再接管。
            LOG(INFO) << "SlotMigrator fenced " << fenced.size()
                      << " slot(s) still owned by live peers, master_id="
                      << config_.master_id;
        }
    }

    // 不变 slot：幂等 reaffirm kStable，但仅作为低频安全网（每
    // kReaffirmIntervalCycles 个周期一次）。slot key 附着在 lease 上，
    // keepalive 持续保活即不过期；逐周期重写等值 value 只会线性堆积 etcd
    // MVCC revision（曾以 ~3k put/s 的速率写满 backend 配额）。正常稳态
    // 下 lease 存活即代表所有权持续有效，无需重写。
    constexpr uint64_t kReaffirmIntervalCycles = 12;  // ~60s @ 5s heartbeat
    const bool do_reaffirm = (++reconcile_cycles_ % kReaffirmIntervalCycles) == 0;
    if (do_reaffirm) {
        for (uint16_t slot : cur) {
            // fenced slot（本周期被拦截/发布失败）不能 reaffirm：它仍归
            // 其他存活 master 所有，重写会重新引发覆盖。
            if (std::binary_search(fenced.begin(), fenced.end(), slot)) {
                continue;
            }
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
    }

    // last_owned_slots_ 只记录本周期真正发布成功的 slot；fenced slot 不计入，
    // 下一周期它们重新进入 gained 集合，重新走 fence 检查 → 观察等待。
    if (!fenced.empty()) {
        std::sort(fenced.begin(), fenced.end());
        std::vector<uint16_t> committed;
        committed.reserve(cur.size() - fenced.size());
        std::set_difference(cur.begin(), cur.end(), fenced.begin(),
                            fenced.end(), std::back_inserter(committed));
        last_owned_slots_ = std::move(committed);
    } else {
        last_owned_slots_ = std::move(cur);
    }
    return last_err;
}

}  // namespace cvm
}  // namespace mooncake
