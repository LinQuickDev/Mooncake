#include "cvm/slot_owner_heartbeat.h"

#include <algorithm>
#include <iterator>
#include <utility>

#include <glog/logging.h>

#include "cvm/etcd_view_store.h"
#include "cvm/slot_hash.h"

namespace mooncake {
namespace cvm {

namespace {

// Resolves the owned slot list from config. An empty `owned_slots` means the
// submaster owns every logical slot (single-master mode).
std::vector<uint16_t> ResolveOwnedSlots(const SlotOwnerHeartbeat::Config& cfg) {
    if (!cfg.owned_slots.empty()) {
        return cfg.owned_slots;
    }
    std::vector<uint16_t> slots;
    slots.reserve(kSlotCount);
    for (uint16_t slot = 0; slot < kSlotCount; ++slot) {
        slots.push_back(slot);
    }
    return slots;
}

}  // namespace

SlotOwnerHeartbeat::SlotOwnerHeartbeat(Config config)
    : config_(std::move(config)),
      owned_slots_(ResolveOwnedSlots(config_)),
      migrator_(SlotMigrator::Config{config_.cluster_namespace,
                                     config_.master_id,
                                     config_.lease_id}) {
    migrator_.SetOnAcquire(config_.on_slot_acquired);
    migrator_.SetOnRelease(config_.on_slot_released);
}

SlotOwnerHeartbeat::~SlotOwnerHeartbeat() { Stop(); }

ErrorCode SlotOwnerHeartbeat::Start() {
    if (running_.load()) {
        return ErrorCode::OK;
    }
    running_.store(true);
    thread_ = std::thread([this]() { RunLoop(); });
    return ErrorCode::OK;
}

void SlotOwnerHeartbeat::Stop() {
    if (!running_.exchange(false)) {
        return;
    }
    stop_cv_.notify_all();
    if (thread_.joinable()) {
        thread_.join();
    }
}

ErrorCode SlotOwnerHeartbeat::PublishOnce() {
    // Under dynamic partition the owned slot set is recomputed every publish so
    // it follows cluster membership changes; otherwise use the static set.
    std::vector<uint16_t> slots = owned_slots_;
    if (config_.dynamic_slot_resolver) {
        slots = config_.dynamic_slot_resolver();
    }

    // Delegate the per-slot writes to the SlotMigrator state machine: it
    // publishes kMigrating -> kStable for newly-acquired slots, deletes
    // released slots, and re-affirms unchanged slots.
    return migrator_.Reconcile(slots);
}

void SlotOwnerHeartbeat::RunLoop() {
    while (running_.load()) {
        (void)PublishOnce();
        std::unique_lock<std::mutex> lock(stop_mutex_);
        stop_cv_.wait_for(lock, config_.heartbeat_interval,
                          [this] { return !running_.load(); });
    }
}

}  // namespace cvm
}  // namespace mooncake
