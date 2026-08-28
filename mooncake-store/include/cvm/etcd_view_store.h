#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "cvm/cvm_types.h"
#include "types.h"

namespace mooncake {
namespace cvm {

// Serialization + etcd persistence for CVM views, built on EtcdHelper.
//
// All KV view / segment view / master registration records are stored in etcd
// under the CVM key space (see cvm_keys.h). ViewVersionId is the etcd revision
// returned by the read, so consumers can watch from it without gaps.
class EtcdViewStore {
   public:
    // ---- JSON serialization ----
    static ErrorCode SerializeSlotOwner(const SlotOwner& owner,
                                        std::string& out);
    static ErrorCode DeserializeSlotOwner(const std::string& in,
                                          SlotOwner& out);

    static ErrorCode SerializeSegmentOwner(const SegmentOwner& owner,
                                           std::string& out);
    static ErrorCode DeserializeSegmentOwner(const std::string& in,
                                             SegmentOwner& out);

    static ErrorCode SerializeMasterRegistration(const MasterRegistration& reg,
                                                 std::string& out);
    static ErrorCode DeserializeMasterRegistration(const std::string& in,
                                                   MasterRegistration& out);

    static ErrorCode SerializeKvViewSnapshot(const KvViewSnapshot& snapshot,
                                             std::string& out);
    static ErrorCode DeserializeKvViewSnapshot(const std::string& in,
                                               KvViewSnapshot& out);
    static ErrorCode SerializeSegmentViewSnapshot(
        const SegmentViewSnapshot& snapshot, std::string& out);
    static ErrorCode DeserializeSegmentViewSnapshot(const std::string& in,
                                                    SegmentViewSnapshot& out);

    // ---- KV view ----
    static ErrorCode LoadSlotOwner(const std::string& cluster_namespace,
                                   uint16_t slot, SlotOwner& out,
                                   ViewVersionId& version);
    static ErrorCode SaveSlotOwner(const std::string& cluster_namespace,
                                   const SlotOwner& owner);
    static ErrorCode SaveSlotOwnerWithLease(const std::string& cluster_namespace,
                                            const SlotOwner& owner,
                                            EtcdLeaseId lease_id);
    static ErrorCode DeleteSlotOwner(const std::string& cluster_namespace,
                                     uint16_t slot);
    static ErrorCode DeleteSlotOwnerIfOwnedBy(
        const std::string& cluster_namespace, uint16_t slot,
        const std::string& master_id);
    static ErrorCode LoadAllSlotOwners(const std::string& cluster_namespace,
                                       std::vector<SlotOwner>& out,
                                       ViewVersionId& version);

    // ---- Segment view (reserved) ----
    static ErrorCode LoadSegmentOwner(const std::string& cluster_namespace,
                                      const std::string& segment_id,
                                      SegmentOwner& out,
                                      ViewVersionId& version);
    static ErrorCode SaveSegmentOwner(const std::string& cluster_namespace,
                                      const SegmentOwner& owner);
    static ErrorCode SaveSegmentOwnerWithLease(
        const std::string& cluster_namespace, const SegmentOwner& owner,
        EtcdLeaseId lease_id);
    static ErrorCode DeleteSegmentOwner(const std::string& cluster_namespace,
                                        const std::string& segment_id);
    static ErrorCode LoadAllSegmentOwners(const std::string& cluster_namespace,
                                          std::vector<SegmentOwner>& out,
                                          ViewVersionId& version);

    // ---- Master registration ----
    static ErrorCode RegisterMaster(const std::string& cluster_namespace,
                                    const MasterRegistration& reg,
                                    EtcdLeaseId lease_id);
    static ErrorCode UpdateMasterRole(const std::string& cluster_namespace,
                                      const std::string& master_id,
                                      MasterRole role, EtcdLeaseId lease_id);
    static ErrorCode LoadAllMasters(const std::string& cluster_namespace,
                                    std::vector<MasterRegistration>& out,
                                    ViewVersionId& version);

    // ---- Snapshots ----
    // Reads the raw slot/segment records, aggregates them into a point-in-time
    // snapshot and writes it back to etcd. `version` is set to the etcd
    // revision of the raw records that were aggregated.
    static ErrorCode BuildAndSaveKvViewSnapshot(
        const std::string& cluster_namespace, ViewVersionId& version);
    static ErrorCode BuildAndSaveSegmentViewSnapshot(
        const std::string& cluster_namespace, ViewVersionId& version);

    static ErrorCode SaveKvViewSnapshot(const std::string& cluster_namespace,
                                        const KvViewSnapshot& snapshot);
    static ErrorCode SaveSegmentViewSnapshot(
        const std::string& cluster_namespace, const SegmentViewSnapshot& snapshot);

    // ---- Watch ----
    using WatchCallback = void (*)(void*, const char*, size_t, const char*,
                                   size_t, int, int64_t);
    static ErrorCode WatchKvView(const std::string& cluster_namespace,
                                 ViewVersionId start_revision, void* ctx,
                                 WatchCallback cb);
    static ErrorCode CancelWatchKvView(const std::string& cluster_namespace);
    static ErrorCode WaitWatchKvViewStopped(const std::string& cluster_namespace,
                                            int timeout_ms);

    // ---- Master membership watch ----
    // Watches the master registration prefix so member add/remove (e.g. lease
    // expiry) can drive immediate role re-evaluation (P3 failover).
    static ErrorCode WatchMasters(const std::string& cluster_namespace,
                                  ViewVersionId start_revision, void* ctx,
                                  WatchCallback cb);
    static ErrorCode CancelWatchMasters(const std::string& cluster_namespace);
    static ErrorCode WaitWatchMastersStopped(
        const std::string& cluster_namespace, int timeout_ms);
};

}  // namespace cvm
}  // namespace mooncake
