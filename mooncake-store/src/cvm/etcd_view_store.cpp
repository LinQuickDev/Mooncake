#include "cvm/etcd_view_store.h"

#include <chrono>
#include <memory>
#include <sstream>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include "cvm/cvm_keys.h"
#include "etcd_helper.h"
#include "ylt/struct_json/json_reader.h"
#include "ylt/struct_json/json_writer.h"

namespace mooncake {
namespace cvm {

namespace {

// Parses the JSON array returned by EtcdHelper::GetRangeAsJson, which has the
// form [{"key":"...","value":"..."}, ...].
ErrorCode ParseRangeJson(const std::string& json,
                         std::vector<std::pair<std::string, std::string>>& kvs) {
    Json::Value root;
    Json::CharReaderBuilder reader;
    std::string errors;
    std::istringstream stream(json);
    if (!Json::parseFromStream(reader, stream, &root, &errors) ||
        !root.isArray()) {
        LOG(ERROR) << "Failed to parse etcd range JSON: " << errors;
        return ErrorCode::INTERNAL_ERROR;
    }

    kvs.clear();
    kvs.reserve(root.size());
    for (const auto& item : root) {
        if (!item.isObject() || !item["key"].isString() ||
            !item["value"].isString()) {
            return ErrorCode::INTERNAL_ERROR;
        }
        kvs.emplace_back(item["key"].asString(), item["value"].asString());
    }
    return ErrorCode::OK;
}

}  // namespace

// ---- JSON serialization ----

ErrorCode EtcdViewStore::SerializeSlotOwner(const SlotOwner& owner,
                                            std::string& out) {
    try {
        struct_json::to_json(owner, out);
    } catch (const std::exception& e) {
        LOG(ERROR) << "SerializeSlotOwner failed: " << e.what();
        return ErrorCode::SERIALIZE_FAIL;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdViewStore::DeserializeSlotOwner(const std::string& in,
                                              SlotOwner& out) {
    try {
        struct_json::from_json(out, in);
    } catch (const std::exception& e) {
        LOG(ERROR) << "DeserializeSlotOwner failed: " << e.what();
        return ErrorCode::DESERIALIZE_FAIL;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdViewStore::SerializeSegmentOwner(const SegmentOwner& owner,
                                               std::string& out) {
    try {
        struct_json::to_json(owner, out);
    } catch (const std::exception& e) {
        LOG(ERROR) << "SerializeSegmentOwner failed: " << e.what();
        return ErrorCode::SERIALIZE_FAIL;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdViewStore::DeserializeSegmentOwner(const std::string& in,
                                                 SegmentOwner& out) {
    try {
        struct_json::from_json(out, in);
    } catch (const std::exception& e) {
        LOG(ERROR) << "DeserializeSegmentOwner failed: " << e.what();
        return ErrorCode::DESERIALIZE_FAIL;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdViewStore::SerializeMasterRegistration(const MasterRegistration& reg,
                                                     std::string& out) {
    try {
        struct_json::to_json(reg, out);
    } catch (const std::exception& e) {
        LOG(ERROR) << "SerializeMasterRegistration failed: " << e.what();
        return ErrorCode::SERIALIZE_FAIL;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdViewStore::DeserializeMasterRegistration(const std::string& in,
                                                       MasterRegistration& out) {
    try {
        struct_json::from_json(out, in);
    } catch (const std::exception& e) {
        LOG(ERROR) << "DeserializeMasterRegistration failed: " << e.what();
        return ErrorCode::DESERIALIZE_FAIL;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdViewStore::SerializeKvViewSnapshot(const KvViewSnapshot& snapshot,
                                                 std::string& out) {
    try {
        struct_json::to_json(snapshot, out);
    } catch (const std::exception& e) {
        LOG(ERROR) << "SerializeKvViewSnapshot failed: " << e.what();
        return ErrorCode::SERIALIZE_FAIL;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdViewStore::DeserializeKvViewSnapshot(const std::string& in,
                                                   KvViewSnapshot& out) {
    try {
        struct_json::from_json(out, in);
    } catch (const std::exception& e) {
        LOG(ERROR) << "DeserializeKvViewSnapshot failed: " << e.what();
        return ErrorCode::DESERIALIZE_FAIL;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdViewStore::SerializeSegmentViewSnapshot(
    const SegmentViewSnapshot& snapshot, std::string& out) {
    try {
        struct_json::to_json(snapshot, out);
    } catch (const std::exception& e) {
        LOG(ERROR) << "SerializeSegmentViewSnapshot failed: " << e.what();
        return ErrorCode::SERIALIZE_FAIL;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdViewStore::DeserializeSegmentViewSnapshot(const std::string& in,
                                                        SegmentViewSnapshot& out) {
    try {
        struct_json::from_json(out, in);
    } catch (const std::exception& e) {
        LOG(ERROR) << "DeserializeSegmentViewSnapshot failed: " << e.what();
        return ErrorCode::DESERIALIZE_FAIL;
    }
    return ErrorCode::OK;
}

// ---- KV view ----

ErrorCode EtcdViewStore::LoadSlotOwner(const std::string& cluster_namespace,
                                       uint16_t slot, SlotOwner& out,
                                       ViewVersionId& version) {
    const std::string key = SlotOwnerKey(cluster_namespace, slot);
    std::string value;
    ErrorCode err = EtcdHelper::Get(key.data(), key.size(), value, version);
    if (err != ErrorCode::OK) {
        return err;
    }
    return DeserializeSlotOwner(value, out);
}

ErrorCode EtcdViewStore::SaveSlotOwner(const std::string& cluster_namespace,
                                       const SlotOwner& owner) {
    const std::string key = SlotOwnerKey(cluster_namespace, owner.slot);
    std::string value;
    ErrorCode err = SerializeSlotOwner(owner, value);
    if (err != ErrorCode::OK) {
        return err;
    }
    return EtcdHelper::Put(key.data(), key.size(), value.data(), value.size());
}

ErrorCode EtcdViewStore::SaveSlotOwnerWithLease(
    const std::string& cluster_namespace, const SlotOwner& owner,
    EtcdLeaseId lease_id) {
    const std::string key = SlotOwnerKey(cluster_namespace, owner.slot);
    std::string value;
    ErrorCode err = SerializeSlotOwner(owner, value);
    if (err != ErrorCode::OK) {
        return err;
    }
    return EtcdHelper::PutWithLease(key.data(), key.size(), value.data(),
                                    value.size(), lease_id);
}

ErrorCode EtcdViewStore::DeleteSlotOwner(const std::string& cluster_namespace,
                                         uint16_t slot) {
    const std::string key = SlotOwnerKey(cluster_namespace, slot);
    const std::string end = PrefixEnd(key);
    return EtcdHelper::DeleteRange(key.data(), key.size(), end.data(),
                                   end.size());
}

ErrorCode EtcdViewStore::DeleteSlotOwnerIfOwnedBy(
    const std::string& cluster_namespace, uint16_t slot,
    const std::string& master_id) {
    SlotOwner owner;
    ViewVersionId version = 0;
    ErrorCode err = LoadSlotOwner(cluster_namespace, slot, owner, version);
    if (err == ErrorCode::ETCD_KEY_NOT_EXIST) {
        return ErrorCode::OK;  // already gone; nothing to clean up
    }
    if (err != ErrorCode::OK) {
        return err;
    }
    if (owner.primary_master_id != master_id) {
        return ErrorCode::OK;  // now owned by another master; leave it
    }
    return DeleteSlotOwner(cluster_namespace, slot);
}

ErrorCode EtcdViewStore::LoadAllSlotOwners(const std::string& cluster_namespace,
                                           std::vector<SlotOwner>& out,
                                           ViewVersionId& version) {
    out.clear();
    const std::string prefix = KvViewPrefix(cluster_namespace);
    const std::string end = PrefixEnd(prefix);
    std::string json;
    ErrorCode err = EtcdHelper::GetRangeAsJson(prefix.data(), prefix.size(),
                                               end.data(), end.size(),
                                               /*limit=*/0, json, version);
    if (err != ErrorCode::OK) {
        return err;
    }

    std::vector<std::pair<std::string, std::string>> kvs;
    err = ParseRangeJson(json, kvs);
    if (err != ErrorCode::OK) {
        return err;
    }

    out.reserve(kvs.size());
    for (const auto& kv : kvs) {
        SlotOwner owner;
        err = DeserializeSlotOwner(kv.second, owner);
        if (err != ErrorCode::OK) {
            return err;
        }
        out.push_back(std::move(owner));
    }
    return ErrorCode::OK;
}

// ---- Segment view (reserved) ----

ErrorCode EtcdViewStore::LoadSegmentOwner(const std::string& cluster_namespace,
                                          const std::string& segment_id,
                                          SegmentOwner& out,
                                          ViewVersionId& version) {
    const std::string key = SegmentOwnerKey(cluster_namespace, segment_id);
    std::string value;
    ErrorCode err = EtcdHelper::Get(key.data(), key.size(), value, version);
    if (err != ErrorCode::OK) {
        return err;
    }
    return DeserializeSegmentOwner(value, out);
}

ErrorCode EtcdViewStore::SaveSegmentOwner(const std::string& cluster_namespace,
                                          const SegmentOwner& owner) {
    const std::string key =
        SegmentOwnerKey(cluster_namespace, owner.segment_id);
    std::string value;
    ErrorCode err = SerializeSegmentOwner(owner, value);
    if (err != ErrorCode::OK) {
        return err;
    }
    return EtcdHelper::Put(key.data(), key.size(), value.data(), value.size());
}

ErrorCode EtcdViewStore::SaveSegmentOwnerWithLease(
    const std::string& cluster_namespace, const SegmentOwner& owner,
    EtcdLeaseId lease_id) {
    const std::string key =
        SegmentOwnerKey(cluster_namespace, owner.segment_id);
    std::string value;
    ErrorCode err = SerializeSegmentOwner(owner, value);
    if (err != ErrorCode::OK) {
        return err;
    }
    return EtcdHelper::PutWithLease(key.data(), key.size(), value.data(),
                                    value.size(), lease_id);
}

ErrorCode EtcdViewStore::DeleteSegmentOwner(
    const std::string& cluster_namespace, const std::string& segment_id) {
    const std::string key = SegmentOwnerKey(cluster_namespace, segment_id);
    const std::string end = PrefixEnd(key);
    return EtcdHelper::DeleteRange(key.data(), key.size(), end.data(),
                                   end.size());
}

ErrorCode EtcdViewStore::LoadAllSegmentOwners(
    const std::string& cluster_namespace, std::vector<SegmentOwner>& out,
    ViewVersionId& version) {
    out.clear();
    const std::string prefix = SegmentViewPrefix(cluster_namespace);
    const std::string end = PrefixEnd(prefix);
    std::string json;
    ErrorCode err = EtcdHelper::GetRangeAsJson(prefix.data(), prefix.size(),
                                               end.data(), end.size(),
                                               /*limit=*/0, json, version);
    if (err != ErrorCode::OK) {
        return err;
    }

    std::vector<std::pair<std::string, std::string>> kvs;
    err = ParseRangeJson(json, kvs);
    if (err != ErrorCode::OK) {
        return err;
    }

    out.reserve(kvs.size());
    for (const auto& kv : kvs) {
        SegmentOwner owner;
        err = DeserializeSegmentOwner(kv.second, owner);
        if (err != ErrorCode::OK) {
            return err;
        }
        out.push_back(std::move(owner));
    }
    return ErrorCode::OK;
}

// ---- Master registration ----

ErrorCode EtcdViewStore::RegisterMaster(const std::string& cluster_namespace,
                                        const MasterRegistration& reg,
                                        EtcdLeaseId lease_id) {
    const std::string key =
        MasterRegistrationKey(cluster_namespace, reg.master_id);
    std::string value;
    ErrorCode err = SerializeMasterRegistration(reg, value);
    if (err != ErrorCode::OK) {
        return err;
    }
    return EtcdHelper::PutWithLease(key.data(), key.size(), value.data(),
                                    value.size(), lease_id);
}

ErrorCode EtcdViewStore::UpdateMasterRole(const std::string& cluster_namespace,
                                          const std::string& master_id,
                                          MasterRole role,
                                          EtcdLeaseId lease_id) {
    const std::string key = MasterRegistrationKey(cluster_namespace, master_id);

    MasterRegistration reg;
    ViewVersionId version = 0;
    std::string existing;
    ErrorCode err =
        EtcdHelper::Get(key.data(), key.size(), existing, version);
    if (err == ErrorCode::OK) {
        err = DeserializeMasterRegistration(existing, reg);
        if (err != ErrorCode::OK) {
            return err;
        }
    }
    // On read failure (e.g. key missing) fall back to a minimal registration;
    // the caller only flips role on a previously-registered master, so this
    // path is defensive and preserves liveness via the lease.
    reg.master_id = master_id;
    reg.role = static_cast<int32_t>(role);

    std::string value;
    err = SerializeMasterRegistration(reg, value);
    if (err != ErrorCode::OK) {
        return err;
    }
    return EtcdHelper::PutWithLease(key.data(), key.size(), value.data(),
                                    value.size(), lease_id);
}

ErrorCode EtcdViewStore::LoadAllMasters(
    const std::string& cluster_namespace, std::vector<MasterRegistration>& out,
    ViewVersionId& version) {
    out.clear();
    const std::string prefix = MasterRegistrationPrefix(cluster_namespace);
    const std::string end = PrefixEnd(prefix);
    std::string json;
    ErrorCode err = EtcdHelper::GetRangeAsJson(prefix.data(), prefix.size(),
                                               end.data(), end.size(),
                                               /*limit=*/0, json, version);
    if (err != ErrorCode::OK) {
        return err;
    }

    std::vector<std::pair<std::string, std::string>> kvs;
    err = ParseRangeJson(json, kvs);
    if (err != ErrorCode::OK) {
        return err;
    }

    out.reserve(kvs.size());
    for (const auto& kv : kvs) {
        MasterRegistration reg;
        err = DeserializeMasterRegistration(kv.second, reg);
        if (err != ErrorCode::OK) {
            return err;
        }
        out.push_back(std::move(reg));
    }
    return ErrorCode::OK;
}

// ---- Snapshots ----

ErrorCode EtcdViewStore::SaveKvViewSnapshot(const std::string& cluster_namespace,
                                            const KvViewSnapshot& snapshot) {
    const std::string key = KvViewSnapshotKey(cluster_namespace);
    std::string value;
    ErrorCode err = SerializeKvViewSnapshot(snapshot, value);
    if (err != ErrorCode::OK) {
        return err;
    }
    return EtcdHelper::Put(key.data(), key.size(), value.data(), value.size());
}

ErrorCode EtcdViewStore::SaveSegmentViewSnapshot(
    const std::string& cluster_namespace, const SegmentViewSnapshot& snapshot) {
    const std::string key = SegmentViewSnapshotKey(cluster_namespace);
    std::string value;
    ErrorCode err = SerializeSegmentViewSnapshot(snapshot, value);
    if (err != ErrorCode::OK) {
        return err;
    }
    return EtcdHelper::Put(key.data(), key.size(), value.data(), value.size());
}

namespace {
// Content-dedup cache for snapshot writes. `generated_at_ms` changes on every
// build, so a naive "write every sync cycle" put a ~MB value per cycle per
// master and exhausted the etcd backend quota with MVCC revisions. Skip the
// write when the owners content is unchanged since this process last wrote it.
// Process-local: after a content change each master writes once, which is
// acceptable (two puts per change instead of one).
std::mutex g_snapshot_dedup_mutex;
std::unordered_map<std::string, std::string> g_last_snapshot_content;
}  // namespace

ErrorCode EtcdViewStore::BuildAndSaveKvViewSnapshot(
    const std::string& cluster_namespace, ViewVersionId& version) {
    std::vector<SlotOwner> owners;
    ErrorCode err = LoadAllSlotOwners(cluster_namespace, owners, version);
    if (err != ErrorCode::OK) {
        return err;
    }

    // Fingerprint over the owners only (exclude version/generated_at so the
    // steady state produces a stable string).
    std::string content;
    try {
        struct_json::to_json(owners, content);
    } catch (const std::exception& e) {
        LOG(WARNING) << "BuildAndSaveKvViewSnapshot: fingerprint failed: "
                     << e.what();
        content.clear();
    }
    {
        std::lock_guard<std::mutex> lock(g_snapshot_dedup_mutex);
        auto& last = g_last_snapshot_content["kv:" + cluster_namespace];
        if (!content.empty() && last == content) {
            return ErrorCode::OK;  // unchanged, skip the etcd write
        }
        last = content;
    }

    KvViewSnapshot snapshot;
    snapshot.version = version;
    snapshot.generated_at_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    snapshot.slot_owners = std::move(owners);
    err = SaveKvViewSnapshot(cluster_namespace, snapshot);
    if (err != ErrorCode::OK) {
        // 写失败时回滚缓存，下轮重试完整写入。
        std::lock_guard<std::mutex> lock(g_snapshot_dedup_mutex);
        g_last_snapshot_content.erase("kv:" + cluster_namespace);
    }
    return err;
}

ErrorCode EtcdViewStore::BuildAndSaveSegmentViewSnapshot(
    const std::string& cluster_namespace, ViewVersionId& version) {
    std::vector<SegmentOwner> owners;
    ErrorCode err = LoadAllSegmentOwners(cluster_namespace, owners, version);
    if (err != ErrorCode::OK) {
        return err;
    }

    std::string content;
    try {
        struct_json::to_json(owners, content);
    } catch (const std::exception& e) {
        LOG(WARNING) << "BuildAndSaveSegmentViewSnapshot: fingerprint failed: "
                     << e.what();
        content.clear();
    }
    {
        std::lock_guard<std::mutex> lock(g_snapshot_dedup_mutex);
        auto& last = g_last_snapshot_content["segment:" + cluster_namespace];
        if (!content.empty() && last == content) {
            return ErrorCode::OK;  // unchanged, skip the etcd write
        }
        last = content;
    }

    SegmentViewSnapshot snapshot;
    snapshot.version = version;
    snapshot.generated_at_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    snapshot.segment_owners = std::move(owners);
    err = SaveSegmentViewSnapshot(cluster_namespace, snapshot);
    if (err != ErrorCode::OK) {
        std::lock_guard<std::mutex> lock(g_snapshot_dedup_mutex);
        g_last_snapshot_content.erase("segment:" + cluster_namespace);
    }
    return err;
}

// ---- Watch ----

ErrorCode EtcdViewStore::WatchKvView(const std::string& cluster_namespace,
                                     ViewVersionId start_revision, void* ctx,
                                     WatchCallback cb) {
    const std::string prefix = KvViewPrefix(cluster_namespace);
    return EtcdHelper::WatchWithPrefixFromRevision(prefix.data(), prefix.size(),
                                                   start_revision, ctx, cb);
}

ErrorCode EtcdViewStore::CancelWatchKvView(const std::string& cluster_namespace) {
    const std::string prefix = KvViewPrefix(cluster_namespace);
    return EtcdHelper::CancelWatchWithPrefix(prefix.data(), prefix.size());
}

ErrorCode EtcdViewStore::WaitWatchKvViewStopped(
    const std::string& cluster_namespace, int timeout_ms) {
    const std::string prefix = KvViewPrefix(cluster_namespace);
    return EtcdHelper::WaitWatchWithPrefixStopped(prefix.data(), prefix.size(),
                                                  timeout_ms);
}

// ---- Master membership watch ----

ErrorCode EtcdViewStore::WatchMasters(const std::string& cluster_namespace,
                                      ViewVersionId start_revision, void* ctx,
                                      WatchCallback cb) {
    const std::string prefix = MasterRegistrationPrefix(cluster_namespace);
    return EtcdHelper::WatchWithPrefixFromRevision(prefix.data(), prefix.size(),
                                                   start_revision, ctx, cb);
}

ErrorCode EtcdViewStore::CancelWatchMasters(
    const std::string& cluster_namespace) {
    const std::string prefix = MasterRegistrationPrefix(cluster_namespace);
    return EtcdHelper::CancelWatchWithPrefix(prefix.data(), prefix.size());
}

ErrorCode EtcdViewStore::WaitWatchMastersStopped(
    const std::string& cluster_namespace, int timeout_ms) {
    const std::string prefix = MasterRegistrationPrefix(cluster_namespace);
    return EtcdHelper::WaitWatchWithPrefixStopped(prefix.data(), prefix.size(),
                                                  timeout_ms);
}

}  // namespace cvm
}  // namespace mooncake
