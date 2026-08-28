#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "types.h"

namespace mooncake {
namespace cvm {

// ---------------------------------------------------------------------------
// Enumerations
// ---------------------------------------------------------------------------

// Ownership state of a logical KV slot.
enum class SlotState : int32_t {
    kStable = 0,     // Served by primary_master_id.
    kMigrating = 1,  // Handing off to migrating_to_master_id.
};

// Role of a master within the CVM topology.
enum class MasterRole : int32_t {
    kPrimary = 0,
    kStandby = 1,
};

// Ownership state of a segment (segment view is reserved).
enum class SegmentOwnerState : int32_t {
    kStable = 0,
    kTransitioning = 1,
};

// ---------------------------------------------------------------------------
// KV view: slot -> primary master ownership
// ---------------------------------------------------------------------------

// NOTE: enum fields are stored as int32_t so the records serialize/deserialize
// identically across languages and compiler settings.
struct SlotOwner {
    uint16_t slot{0};
    std::string primary_master_id;
    int32_t state{0};  // SlotState
    std::string migrating_to_master_id;  // empty when stable
};
YLT_REFL(SlotOwner, slot, primary_master_id, state, migrating_to_master_id);

// ---------------------------------------------------------------------------
// Segment view: segment -> owner master ownership (reserved)
// ---------------------------------------------------------------------------

struct SegmentOwner {
    std::string segment_id;
    std::string owner_master_id;
    int32_t state{0};  // SegmentOwnerState
};
YLT_REFL(SegmentOwner, segment_id, owner_master_id, state);

// ---------------------------------------------------------------------------
// Master registration: liveness + role, persisted under an etcd lease
// ---------------------------------------------------------------------------

struct MasterRegistration {
    std::string master_id;
    std::string address;  // RPC endpoint, e.g. "host:port"
    int32_t role{0};      // MasterRole
    int64_t registered_at_ms{0};
};
YLT_REFL(MasterRegistration, master_id, address, role, registered_at_ms);

// ---------------------------------------------------------------------------
// Aggregated views (in-memory snapshots)
// ---------------------------------------------------------------------------

struct KvView {
    std::vector<SlotOwner> slot_owners;
};
YLT_REFL(KvView, slot_owners);

struct SegmentView {
    std::vector<SegmentOwner> segment_owners;
};
YLT_REFL(SegmentView, segment_owners);

// ---------------------------------------------------------------------------
// Derived snapshots (persisted back to etcd)
// ---------------------------------------------------------------------------

// Aggregated, point-in-time view of slot ownership. EtcdViewStore builds this
// from the raw slot records and writes it back to etcd so that clients can read
// the whole mapping with a single range get instead of one key per slot.
struct KvViewSnapshot {
    ViewVersionId version{0};       // Etcd revision of the raw records used.
    int64_t generated_at_ms{0};     // Build time (ms since epoch).
    std::vector<SlotOwner> slot_owners;
};
YLT_REFL(KvViewSnapshot, version, generated_at_ms, slot_owners);

// Aggregated, point-in-time view of segment ownership (reserved).
struct SegmentViewSnapshot {
    ViewVersionId version{0};       // Etcd revision of the raw records used.
    int64_t generated_at_ms{0};     // Build time (ms since epoch).
    std::vector<SegmentOwner> segment_owners;
};
YLT_REFL(SegmentViewSnapshot, version, generated_at_ms, segment_owners);

}  // namespace cvm
}  // namespace mooncake
