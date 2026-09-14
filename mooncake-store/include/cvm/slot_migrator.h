#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "cvm/cvm_types.h"
#include "types.h"

namespace mooncake {
namespace cvm {

// Drives the slot ownership handoff state machine (P4).
//
// A submaster owning a set of logical slots moves each slot through a two-phase
// transition whenever ownership changes:
//
//   新获得 slot: kMigrating (migrating_to = self) -> on_acquire -> kStable
//   释放   slot: on_release -> DeleteSlotOwnerIfOwnedBy(self)
//   不变   slot: kStable (幂等 reaffirm)
//
// `kMigrating` is visible to clients so they can treat a handoff-in-progress
// slot as "not yet routable" instead of hitting a half-ready new owner.
//
// SlotMigrator only moves the *ownership records*; the actual object-metadata
// materialization/drop is the caller's responsibility via the on_acquire /
// on_release hooks (data bytes always stay in segments).
class SlotMigrator {
   public:
    struct Config {
        std::string cluster_namespace;
        std::string master_id;
        EtcdLeaseId lease_id{0};
    };

    using SlotCallback = std::function<void(uint16_t)>;

    explicit SlotMigrator(Config config);
    ~SlotMigrator() = default;

    SlotMigrator(const SlotMigrator&) = delete;
    SlotMigrator& operator=(const SlotMigrator&) = delete;

    // Hooks invoked on ownership change. on_acquire materializes object
    // metadata for the slot (or is a no-op when the metadata already arrived
    // via the standby-restore path); on_release drops it.
    void SetOnAcquire(SlotCallback cb) { on_acquire_ = std::move(cb); }
    void SetOnRelease(SlotCallback cb) { on_release_ = std::move(cb); }

    // Publishes slot ownership for `owned_slots`. Idempotent; safe to call from
    // the heartbeat thread. Returns the last non-OK error (if any) but keeps
    // going so a single failing slot does not block the rest.
    ErrorCode Reconcile(const std::vector<uint16_t>& owned_slots);

   private:
    ErrorCode PublishMigrating(uint16_t slot);
    ErrorCode PublishStable(uint16_t slot);

    Config config_;
    SlotCallback on_acquire_;
    SlotCallback on_release_;
    std::vector<uint16_t> last_owned_slots_;
    // Reconcile 调用计数，用于把不变 slot 的 reaffirm 降频为低频安全网：
    // slot key 附着在 lease 上（keepalive 保活即不过期），逐周期重写只是
    // 徒增 etcd MVCC revision，曾导致 backend 配额被写满。
    uint64_t reconcile_cycles_{0};
};

}  // namespace cvm
}  // namespace mooncake
