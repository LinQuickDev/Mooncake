#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cvm/cvm_types.h"
#include "cvm/slot_migrator.h"
#include "types.h"

namespace mooncake {
namespace cvm {

// Publishes this submaster's slot ownership to etcd on a fixed interval.
//
// The submaster (a MasterService instance) owns a set of logical slots. It
// periodically writes one `SlotOwner` record per owned slot under
// `/cvm/{ns}/kv_view/slot/{slot:05d}` (see EtcdViewStore::SaveSlotOwner).
// EtcdViewStore later aggregates these raw records into a `KvViewSnapshot`
// that clients read to route keys to the right submaster.
//
// Slot ownership is published on a fixed interval. With a non-zero `lease_id`
// the records are lease-bound and auto-removed when the lease expires (master
// death), which lets the dynamic partition rebalance the freed slots. With
// `lease_id == 0` the mapping is a persistent fact re-affirmed idempotently
// (single-master fallback).
class SlotOwnerHeartbeat {
   public:
    struct Config {
        std::string cluster_namespace;
        std::string master_id;
        std::chrono::milliseconds heartbeat_interval{5000};

        // Slots owned by this submaster. When empty, the submaster owns every
        // logical slot (single-master mode).
        std::vector<uint16_t> owned_slots;

        // Optional resolver that recomputes the owned slot set on every
        // publish. When set, it overrides `owned_slots` and is invoked before
        // each PublishOnce so ownership tracks cluster membership changes
        // (dynamic partition). An empty result means the submaster owns no
        // slot. The callback must be safe to invoke from the heartbeat thread.
        std::function<std::vector<uint16_t>()> dynamic_slot_resolver;

        // Optional etcd lease id. When non-zero, each slot record is written
        // with this lease so it is auto-deleted when the lease expires (master
        // death), letting the dynamic partition rebalance the freed slots.
        // When zero, slot ownership is a persistent fact re-affirmed by each
        // heartbeat (single-master fallback).
        EtcdLeaseId lease_id{0};

        // Optional hooks fired on slot ownership change (P4). on_slot_acquired
        // materializes object metadata for a newly-owned slot (no-op when it
        // already arrived via the standby-restore path); on_slot_released drops
        // it. Leave empty for ownership-publishing-only behavior.
        std::function<void(uint16_t)> on_slot_acquired;
        std::function<void(uint16_t)> on_slot_released;
    };

    explicit SlotOwnerHeartbeat(Config config);
    ~SlotOwnerHeartbeat();

    SlotOwnerHeartbeat(const SlotOwnerHeartbeat&) = delete;
    SlotOwnerHeartbeat& operator=(const SlotOwnerHeartbeat&) = delete;

    ErrorCode Start();
    void Stop();

    // Writes a `SlotOwner` record for every owned slot once. Idempotent; safe
    // to call from the heartbeat thread or externally.
    ErrorCode PublishOnce();

   private:
    void RunLoop();

    Config config_;
    std::vector<uint16_t> owned_slots_;
    // Owns the slot handoff state machine (kMigrating -> kStable / release).
    SlotMigrator migrator_;

    std::atomic<bool> running_{false};
    std::mutex stop_mutex_;
    std::condition_variable stop_cv_;
    std::thread thread_;
};

}  // namespace cvm
}  // namespace mooncake
