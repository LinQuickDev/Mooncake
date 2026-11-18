// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/native_budget.h"

#include <limits>
#include <utility>

namespace mooncake::tent::ub {

namespace {

bool addWouldOverflow(uint64_t lhs, uint64_t rhs) {
    return lhs > std::numeric_limits<uint64_t>::max() - rhs;
}

bool isValidTransition(BudgetObjectState from, BudgetObjectState to) {
    switch (from) {
        case BudgetObjectState::kCreating:
            return to == BudgetObjectState::kActive ||
                   to == BudgetObjectState::kQuarantined;
        case BudgetObjectState::kActive:
            return to == BudgetObjectState::kRetiring ||
                   to == BudgetObjectState::kQuarantined;
        case BudgetObjectState::kRetiring:
            return to == BudgetObjectState::kQuarantined;
        case BudgetObjectState::kQuarantined:
            return false;
    }
    return false;
}

}  // namespace

Status NativeBudget::configure(const std::string& device_key,
                               uint64_t verified_device_limit_jetties,
                               uint64_t configured_budget_jetties,
                               uint64_t replacement_reserve_jetties) {
    if (device_key.empty()) {
        return Status::InvalidArgument(
            "UB native budget device key must not be empty" LOC_MARK);
    }
    if (configured_budget_jetties == 0) {
        return Status::InvalidArgument(
            "UB native budget must be an explicit positive integer" LOC_MARK);
    }
    if (addWouldOverflow(configured_budget_jetties,
                         replacement_reserve_jetties)) {
        return Status::InvalidArgument(
            "UB native budget plus replacement reserve overflows" LOC_MARK);
    }
    const uint64_t ceiling =
        configured_budget_jetties + replacement_reserve_jetties;
    if (verified_device_limit_jetties != 0 &&
        ceiling > verified_device_limit_jetties) {
        return Status::InvalidArgument(
            "UB native budget exceeds the verified device Jetty "
            "limit" LOC_MARK);
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (devices_.count(device_key) != 0) {
        return Status::InvalidArgument(
            "UB native budget already configured for device " + device_key +
            LOC_MARK);
    }
    DeviceLedger ledger;
    ledger.verified_limit_jetties = verified_device_limit_jetties;
    ledger.configured_budget_jetties = configured_budget_jetties;
    ledger.replacement_reserve_jetties = replacement_reserve_jetties;
    devices_.emplace(device_key, std::move(ledger));
    return Status::OK();
}

std::optional<BudgetReservation> NativeBudget::tryReserve(
    const std::string& device_key, BudgetRole role,
    const BudgetQuantities& quantities) {
    if (quantities.isZero()) return std::nullopt;
    std::lock_guard<std::mutex> lock(mutex_);
    auto device = devices_.find(device_key);
    if (device == devices_.end()) return std::nullopt;

    DeviceLedger& ledger = device->second;
    const uint64_t total = totalJetties(ledger);
    const uint64_t ceiling =
        ledger.configured_budget_jetties + ledger.replacement_reserve_jetties;
    if (addWouldOverflow(total, quantities.jetties) ||
        total + quantities.jetties > ceiling) {
        ++ledger.rejected_reservations;
        return std::nullopt;
    }

    BudgetReservation reservation;
    reservation.reservation_id = next_reservation_id_++;
    reservation.device_key = device_key;
    reservation.role = role;
    reservation.generation = ledger.next_generation++;
    reservation.quantities = quantities;
    reservation.state = BudgetObjectState::kCreating;

    LedgerEntry entry;
    entry.state = reservation.state;
    entry.role = role;
    entry.quantities = quantities;
    ledger.entries.emplace(reservation.reservation_id, std::move(entry));
    reservation_device_.emplace(reservation.reservation_id, device_key);
    return reservation;
}

Status NativeBudget::transition(BudgetReservation& reservation,
                                BudgetObjectState next) {
    if (!reservation.valid()) {
        return Status::InvalidArgument(
            "UB budget transition requires a valid reservation" LOC_MARK);
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto device = devices_.find(reservation.device_key);
    if (device == devices_.end()) {
        return Status::InvalidArgument(
            "UB budget reservation references an unknown device" LOC_MARK);
    }
    auto entry = device->second.entries.find(reservation.reservation_id);
    if (entry == device->second.entries.end()) {
        return Status::InvalidArgument(
            "UB budget reservation was already released" LOC_MARK);
    }
    if (!isValidTransition(entry->second.state, next)) {
        return Status::InvalidArgument(
            std::string("UB budget transition ") +
            budgetObjectStateName(entry->second.state) + " -> " +
            budgetObjectStateName(next) + " is not allowed" LOC_MARK);
    }
    entry->second.state = next;
    reservation.state = next;
    return Status::OK();
}

Status NativeBudget::releaseAfterProvenDestruction(
    BudgetReservation& reservation, bool native_destruction_proven) {
    if (!reservation.valid()) {
        return Status::InvalidArgument(
            "UB budget release requires a valid reservation" LOC_MARK);
    }
    if (!native_destruction_proven) {
        return Status::InvalidArgument(
            "UB budget release requires proven native destruction; keep the "
            "reservation quarantined instead" LOC_MARK);
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto device = devices_.find(reservation.device_key);
    if (device == devices_.end()) {
        return Status::InvalidArgument(
            "UB budget reservation references an unknown device" LOC_MARK);
    }
    auto entry = device->second.entries.find(reservation.reservation_id);
    if (entry == device->second.entries.end()) {
        return Status::InvalidArgument(
            "UB budget reservation was already released" LOC_MARK);
    }
    if (entry->second.state != BudgetObjectState::kRetiring &&
        entry->second.state != BudgetObjectState::kQuarantined) {
        return Status::InvalidArgument(
            "UB budget release requires a retiring or quarantined "
            "reservation" LOC_MARK);
    }
    device->second.entries.erase(entry);
    reservation_device_.erase(reservation.reservation_id);
    reservation.reservation_id = 0;
    return Status::OK();
}

void NativeBudget::recordLeakedReservation(
    const BudgetReservation& reservation) {
    if (!reservation.valid()) return;
    std::lock_guard<std::mutex> lock(mutex_);
    auto device = devices_.find(reservation.device_key);
    if (device == devices_.end()) return;
    ++device->second.leaked_reservations;
}

BudgetDeviceSnapshot NativeBudget::snapshot(
    const std::string& device_key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    BudgetDeviceSnapshot result;
    result.device_key = device_key;
    auto device = devices_.find(device_key);
    if (device == devices_.end()) return result;
    accumulateSnapshot(device->second, result);
    return result;
}

std::vector<std::string> NativeBudget::deviceKeys() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> keys;
    keys.reserve(devices_.size());
    for (const auto& device : devices_) keys.push_back(device.first);
    return keys;
}

uint64_t NativeBudget::totalJetties(const DeviceLedger& ledger) {
    uint64_t total = 0;
    for (const auto& entry : ledger.entries) {
        if (addWouldOverflow(total, entry.second.quantities.jetties)) {
            return std::numeric_limits<uint64_t>::max();
        }
        total += entry.second.quantities.jetties;
    }
    return total;
}

void NativeBudget::accumulateSnapshot(const DeviceLedger& ledger,
                                      BudgetDeviceSnapshot& snapshot) {
    snapshot.configured_budget_jetties = ledger.configured_budget_jetties;
    snapshot.replacement_reserve_jetties = ledger.replacement_reserve_jetties;
    snapshot.verified_device_limit_jetties = ledger.verified_limit_jetties;
    snapshot.rejected_reservations = ledger.rejected_reservations;
    snapshot.leaked_reservations = ledger.leaked_reservations;

    std::unordered_map<uint8_t, BudgetRoleStateCount> by_role_state;
    for (const auto& entry : ledger.entries) {
        const LedgerEntry& record = entry.second;
        snapshot.total_jetties += record.quantities.jetties;
        switch (record.state) {
            case BudgetObjectState::kActive:
                snapshot.active_jetties += record.quantities.jetties;
                break;
            case BudgetObjectState::kCreating:
                snapshot.creating_jetties += record.quantities.jetties;
                break;
            case BudgetObjectState::kRetiring:
                snapshot.retiring_jetties += record.quantities.jetties;
                break;
            case BudgetObjectState::kQuarantined:
                snapshot.quarantined_jetties += record.quantities.jetties;
                break;
        }
        const uint8_t key = (static_cast<uint8_t>(record.role) << 4) |
                            static_cast<uint8_t>(record.state);
        auto& counter = by_role_state[key];
        counter.role = record.role;
        counter.state = record.state;
        counter.jetties += record.quantities.jetties;
        counter.jfc_control_credits += record.quantities.jfc_control_credits;
        counter.registration_slots += record.quantities.registration_slots;
    }
    snapshot.role_states.reserve(by_role_state.size());
    for (auto& entry : by_role_state) {
        snapshot.role_states.push_back(std::move(entry.second));
    }
}

}  // namespace mooncake::tent::ub
