// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_TRANSPORT_UB_NATIVE_BUDGET_H_
#define TENT_TRANSPORT_UB_NATIVE_BUDGET_H_

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "tent/common/status.h"

namespace mooncake::tent::ub {

// Roles that consume native device capacity. The budget ledger accounts every
// role against one physical device so business Jetties, probe Jetties,
// shared targets and legacy endpoint Jetties can never jointly exceed the
// verified device limit (INV10).
enum class BudgetRole : uint8_t {
    kBusiness = 0,
    kProbe,
    kTarget,
    kLegacy,
    kOther,
};

inline const char* budgetRoleName(BudgetRole role) noexcept {
    switch (role) {
        case BudgetRole::kBusiness:
            return "business";
        case BudgetRole::kProbe:
            return "probe";
        case BudgetRole::kTarget:
            return "target";
        case BudgetRole::kLegacy:
            return "legacy";
        case BudgetRole::kOther:
            return "other";
    }
    return "unknown";
}

// Lifecycle state of one budgeted reservation. The sets are mutually
// exclusive; a reservation occupies exactly one state at a time and state
// transfers never double count (C05).
enum class BudgetObjectState : uint8_t {
    kCreating = 0,  // budget reserved, native object not yet published
    kActive,        // published and serving (IDLE or IN_USE in its pool)
    kRetiring,      // drained/retired, destruction in progress
    kQuarantined,   // destruction unproven or unsafe; keeps its budget
};

inline const char* budgetObjectStateName(BudgetObjectState state) noexcept {
    switch (state) {
        case BudgetObjectState::kCreating:
            return "creating";
        case BudgetObjectState::kActive:
            return "active";
        case BudgetObjectState::kRetiring:
            return "retiring";
        case BudgetObjectState::kQuarantined:
            return "quarantined";
    }
    return "unknown";
}

// Counted native quantities for one reservation. Jetty objects are the
// primary pooled resource in this increment; JFC control credits and
// MR/token/import slots are tracked as separate counters so later modules
// can extend the ledger without changing its shape.
struct BudgetQuantities {
    uint64_t jetties = 0;
    uint64_t jfc_control_credits = 0;
    uint64_t registration_slots = 0;

    bool operator==(const BudgetQuantities&) const = default;

    [[nodiscard]] bool isZero() const noexcept {
        return jetties == 0 && jfc_control_credits == 0 &&
               registration_slots == 0;
    }
};

// Move-only reservation ticket. The holder owns the reservation until it
// transitions it or releases it after proven destruction. Dropping a ticket
// without a release is a bookkeeping leak and is counted as such; it never
// frees budget implicitly because Unknown destruction must keep the budget
// charged (INV09/INV10).
struct BudgetReservation {
    uint64_t reservation_id = 0;
    std::string device_key;
    BudgetRole role = BudgetRole::kOther;
    uint64_t generation = 0;
    BudgetQuantities quantities{};
    BudgetObjectState state = BudgetObjectState::kCreating;

    [[nodiscard]] bool valid() const noexcept { return reservation_id != 0; }
};

struct BudgetRoleStateCount {
    BudgetRole role = BudgetRole::kOther;
    BudgetObjectState state = BudgetObjectState::kCreating;
    uint64_t jetties = 0;
    uint64_t jfc_control_credits = 0;
    uint64_t registration_slots = 0;
};

// Per-device ledger snapshot used by diagnostics, refill decisions and
// tests. total_* sums every role and every state:
//   N(device) = business_active + probe_active + target_active
//             + creating_all + retiring_all + quarantine_all + legacy + other
struct BudgetDeviceSnapshot {
    std::string device_key;
    uint64_t configured_budget_jetties = 0;
    uint64_t replacement_reserve_jetties = 0;
    uint64_t verified_device_limit_jetties = 0;
    uint64_t total_jetties = 0;
    uint64_t active_jetties = 0;
    uint64_t creating_jetties = 0;
    uint64_t retiring_jetties = 0;
    uint64_t quarantined_jetties = 0;
    uint64_t rejected_reservations = 0;
    uint64_t leaked_reservations = 0;
    std::vector<BudgetRoleStateCount> role_states;
};

// Derives the per-device Jetty budget when the operator left
// transports/ub/native_budget/max_jetty_per_device at 0.
//
// NativeBudget::configure() enforces
//   configured_budget + replacement_reserve <= verified_device_limit,
// so a derived budget must keep the reserve *outside* the device's limit.
// Deriving it as the limit itself made every device that reports its own limit
// fail install with a non-zero reserve (observed on a device reporting 65532
// with the default reserve of 2: 65532 + 2 > 65532).
//
// An explicit operator value is authoritative; whether it fits is decided by
// configure(). Without a reported limit the static Serving target is the
// smallest truthful answer.
[[nodiscard]] inline uint64_t deriveNativeBudgetJetties(
    uint64_t verified_device_limit_jetties, uint64_t configured_budget_jetties,
    uint64_t replacement_reserve_jetties,
    uint64_t minimum_jetties) noexcept {
    if (configured_budget_jetties != 0) return configured_budget_jetties;
    if (verified_device_limit_jetties == 0) {
        return minimum_jetties != 0 ? minimum_jetties : 1;
    }
    if (verified_device_limit_jetties <= replacement_reserve_jetties) {
        // A limit this small cannot hold the reserve either; hand the limit
        // back and let configure() reject it with a precise message.
        return verified_device_limit_jetties;
    }
    return verified_device_limit_jetties - replacement_reserve_jetties;
}

// Device-level full-role native budget (C05). One physical device is
// identified by a stable key (normalized device identity + path + EID); all
// EIDs and context incarnations of the same physical device must share one
// ledger entry. The budget enforces:
//
//   N(device) <= configured_budget + replacement_reserve
//                                            <= verified_device_limit
//
// replacement_reserve allows a bounded number of replacement objects while
// retired/quarantined predecessors still hold their budget; it never allows
// unlimited replacement because the reserve itself is finite.
//
// Budget is released only by releaseAfterProvenDestruction(); logical
// failures, timeouts or Unknown deletions never zero the accounting.
class NativeBudget final {
   public:
    NativeBudget() = default;

    NativeBudget(const NativeBudget&) = delete;
    NativeBudget& operator=(const NativeBudget&) = delete;

    // Installs the ledger for one physical device. configured_budget must be
    // a positive integer and configured_budget + replacement_reserve must
    // not exceed verified_device_limit (when the provider reported one);
    // overflow is rejected. Reconfiguring an existing device is rejected so
    // a running ledger cannot be silently widened.
    Status configure(const std::string& device_key,
                     uint64_t verified_device_limit_jetties,
                     uint64_t configured_budget_jetties,
                     uint64_t replacement_reserve_jetties);

    // Atomically reserves budget for one new native object. Returns
    // std::nullopt (defer, never an error path) when the device is unknown
    // or the reservation would exceed configured_budget + reserve, or when
    // the arithmetic overflows. The reservation starts in kCreating.
    [[nodiscard]] std::optional<BudgetReservation> tryReserve(
        const std::string& device_key, BudgetRole role,
        const BudgetQuantities& quantities);

    // Moves a reservation between states. Allowed edges:
    //   kCreating    -> kActive | kQuarantined
    //   kActive      -> kRetiring | kQuarantined
    //   kRetiring    -> kQuarantined
    // Any other edge (backwards, from kQuarantined, unknown reservation) is
    // rejected as InvalidArgument and leaves the ledger unchanged.
    Status transition(BudgetReservation& reservation, BudgetObjectState next);

    // Releases budget after the native destruction has been proven. proof
    // must be true; an unproven destruction keeps the reservation charged
    // (callers quarantine it instead). Only kRetiring and kQuarantined
    // reservations may be released.
    Status releaseAfterProvenDestruction(BudgetReservation& reservation,
                                         bool native_destruction_proven);

    // Records a dropped reservation ticket (destructor path). The budget
    // stays charged and the leak counter increases; this is diagnostics,
    // not a release.
    void recordLeakedReservation(const BudgetReservation& reservation);

    [[nodiscard]] BudgetDeviceSnapshot snapshot(
        const std::string& device_key) const;
    [[nodiscard]] std::vector<std::string> deviceKeys() const;

   private:
    struct LedgerEntry {
        BudgetObjectState state = BudgetObjectState::kCreating;
        BudgetRole role = BudgetRole::kOther;
        BudgetQuantities quantities{};
    };

    struct DeviceLedger {
        uint64_t verified_limit_jetties = 0;
        uint64_t configured_budget_jetties = 0;
        uint64_t replacement_reserve_jetties = 0;
        uint64_t next_generation = 1;
        uint64_t rejected_reservations = 0;
        uint64_t leaked_reservations = 0;
        std::unordered_map<uint64_t, LedgerEntry> entries;
    };

    static uint64_t totalJetties(const DeviceLedger& ledger);
    static void accumulateSnapshot(const DeviceLedger& ledger,
                                   BudgetDeviceSnapshot& snapshot);

    mutable std::mutex mutex_;
    std::unordered_map<std::string, DeviceLedger> devices_;
    std::unordered_map<uint64_t, std::string> reservation_device_;
    uint64_t next_reservation_id_ = 1;
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_NATIVE_BUDGET_H_
