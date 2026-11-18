// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/path_load_ledger.h"

namespace mooncake::tent::ub {
namespace {

// A ledger must never wrap: one underflowed counter would invert the ranking
// it feeds.
uint64_t saturatingSub(uint64_t lhs, uint64_t rhs) {
    return rhs > lhs ? 0 : lhs - rhs;
}

}  // namespace

void PathLoadLedger::onPublish(const UbPostPath& path, size_t bytes) {
    if (!path.valid()) return;
    // W3 review B4: every access to a row happens under the map lock. Updating
    // the counters outside it let a concurrent finalize observe zero, erase the
    // row and strand this increment on an orphan slot - unmatched finalizes and
    // a permanently poisoned path_load_imbalance, i.e. W3-IN-7 broken. The
    // critical section is two increments, and the posting path already takes
    // one lock in the pool per attempt, so the cost is noise.
    std::lock_guard<std::mutex> lock(mutex_);
    auto& slot = slots_[path];
    if (!slot) slot = std::make_shared<Slot>();
    published_total_.fetch_add(1, std::memory_order_relaxed);
    const uint64_t wrs = slot->wrs.fetch_add(1, std::memory_order_acq_rel) + 1;
    slot->bytes.fetch_add(static_cast<uint64_t>(bytes),
                          std::memory_order_acq_rel);
    uint64_t peak = peak_wrs_.load(std::memory_order_relaxed);
    while (wrs > peak && !peak_wrs_.compare_exchange_weak(
                             peak, wrs, std::memory_order_relaxed,
                             std::memory_order_relaxed)) {
    }
}

void PathLoadLedger::onFinalize(const UbPostPath& path, size_t bytes) {
    if (!path.valid()) return;
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = slots_.find(path);
    if (it == slots_.end()) {
        // Finalize without publish: the quota accounting would already have
        // caught this, so stay balanced instead of creating a negative row.
        ++unmatched_finalize_;
        return;
    }
    finalized_total_.fetch_add(1, std::memory_order_relaxed);
    auto& slot = it->second;
    // Saturating decode: the pair (publish, finalize) normally balances, but a
    // defect must not wrap into a huge load that would poison selection.
    uint64_t current = slot->wrs.load(std::memory_order_acquire);
    while (current != 0 && !slot->wrs.compare_exchange_weak(
                               current, current - 1, std::memory_order_acq_rel,
                               std::memory_order_acquire)) {
    }
    uint64_t current_bytes = slot->bytes.load(std::memory_order_acquire);
    while (current_bytes != 0 &&
           !slot->bytes.compare_exchange_weak(
               current_bytes, saturatingSub(current_bytes, bytes),
               std::memory_order_acq_rel, std::memory_order_acquire)) {
    }
    if (slot->wrs.load(std::memory_order_acquire) == 0 &&
        slot->bytes.load(std::memory_order_acquire) == 0) {
        // Retire the empty row under the same lock publish() takes, so a
        // concurrent publish either already counted on this row or creates a
        // fresh one - never an orphan increment.
        slots_.erase(it);
    }
}

PathLoadLedger::Load PathLoadLedger::load(const UbPostPath& path) const {
    if (!path.valid()) return Load{};
    std::shared_ptr<Slot> slot;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = slots_.find(path);
        if (it == slots_.end()) return Load{};
        slot = it->second;
    }
    return Load{slot->wrs.load(std::memory_order_acquire),
                slot->bytes.load(std::memory_order_acquire)};
}

size_t PathLoadLedger::pathCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return slots_.size();
}

}  // namespace mooncake::tent::ub
