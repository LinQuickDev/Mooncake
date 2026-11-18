// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// JFC credit ledger, ported from the pooling side (M-1). See admission.h for
// why the process-wide TaskPermitLedger is not part of this file (D-6/R-3).

#include "tent/transport/ub/admission.h"

#include <string>

namespace mooncake::tent::ub {

Status JfcCreditLedger::install(size_t jfc_index, uint64_t capacity) {
    if (capacity == 0) {
        return Status::InvalidArgument(
            "UB JFC credit capacity must be an explicit positive "
            "integer" LOC_MARK);
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (jfcs_.count(jfc_index) != 0) {
        return Status::InvalidArgument(
            "UB JFC credit ledger already installed for index " +
            std::to_string(jfc_index) + LOC_MARK);
    }
    JfcCredits credits;
    credits.capacity = capacity;
    jfcs_.emplace(jfc_index, std::move(credits));
    return Status::OK();
}

std::optional<uint64_t> JfcCreditLedger::tryReserve(size_t jfc_index,
                                                    uint64_t credits) {
    if (credits == 0) return std::nullopt;
    std::lock_guard<std::mutex> lock(mutex_);
    auto jfc = jfcs_.find(jfc_index);
    if (jfc == jfcs_.end()) return std::nullopt;
    JfcCredits& record = jfc->second;
    // Two-stage comparison (the pooling side's form): `outstanding + credits`
    // can wrap for a large request, which would let an over-sized reservation
    // through.
    if (credits > record.capacity ||
        record.outstanding > record.capacity - credits) {
        ++record.rejected;
        return std::nullopt;
    }
    const uint64_t token = record.next_token++;
    record.outstanding += credits;
    record.active.emplace(token, credits);
    return token;
}

void JfcCreditLedger::release(size_t jfc_index, uint64_t reservation_token) {
    if (reservation_token == 0) return;
    std::lock_guard<std::mutex> lock(mutex_);
    auto jfc = jfcs_.find(jfc_index);
    if (jfc == jfcs_.end()) return;
    JfcCredits& record = jfc->second;
    auto active = record.active.find(reservation_token);
    if (active == record.active.end()) {
        // Unknown or already released token: harmless for the ledger, but a
        // double release is a defect worth seeing.
        ++record.rejected_releases;
        return;
    }
    const uint64_t credits = active->second;
    record.active.erase(active);
    record.outstanding =
        credits > record.outstanding ? 0 : record.outstanding - credits;
}

uint64_t JfcCreditLedger::outstanding(size_t jfc_index) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto jfc = jfcs_.find(jfc_index);
    return jfc == jfcs_.end() ? 0 : jfc->second.outstanding;
}

uint64_t JfcCreditLedger::capacity(size_t jfc_index) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto jfc = jfcs_.find(jfc_index);
    return jfc == jfcs_.end() ? 0 : jfc->second.capacity;
}

uint64_t JfcCreditLedger::rejectedReservations(size_t jfc_index) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto jfc = jfcs_.find(jfc_index);
    return jfc == jfcs_.end() ? 0 : jfc->second.rejected;
}

uint64_t JfcCreditLedger::rejectedReleases(size_t jfc_index) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto jfc = jfcs_.find(jfc_index);
    return jfc == jfcs_.end() ? 0 : jfc->second.rejected_releases;
}

}  // namespace mooncake::tent::ub
