// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// W3d: per-path inflight ledger used only for scoring (design §5.3 / D-11).
//
// The W2 score ranked rails by `endpoint->outstandingWrs()`, which is reset
// whenever the endpoint generation turns over: a fresh endpoint made a busy
// path look idle. This ledger keys on the full UbPostPath (local NIC, segment,
// remote NIC *and* endpoint generation), so a generation change starts a new
// row instead of inheriting or clearing another row's accounting.
//
// It never participates in admission: capacity belongs to quota and leases.
// publish/finalize are called at the same two points as the quota acquire and
// release, so the ledger stays balanced even when an attempt is rolled back
// (W3-IN-7).

#ifndef TENT_TRANSPORT_UB_PATH_LOAD_LEDGER_H_
#define TENT_TRANSPORT_UB_PATH_LOAD_LEDGER_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "tent/transport/ub/slice.h"

namespace mooncake::tent::ub {

class PathLoadLedger {
   public:
    struct Load {
        uint64_t wrs{0};
        uint64_t bytes{0};
    };

    // One record was published for this path (immediately after the post-side
    // commit, before the native post). Both entries take the map lock; see the
    // implementation for why the counters cannot live outside it.
    void onPublish(const UbPostPath& path, size_t bytes);
    // The record's owners were released (same point as the quota release).
    void onFinalize(const UbPostPath& path, size_t bytes);

    [[nodiscard]] Load load(const UbPostPath& path) const;

    // Observation (design §13): peak inflight WRs across all paths and the
    // live imbalance between publish and finalize, which must return to zero
    // once the pool drains.
    [[nodiscard]] uint64_t peakWrs() const {
        return peak_wrs_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t publishedTotal() const {
        return published_total_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t finalizedTotal() const {
        return finalized_total_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] size_t pathCount() const;
    // A finalize without a matching publish. Always zero on a healthy build.
    [[nodiscard]] uint64_t unmatchedFinalize() const {
        return unmatched_finalize_.load(std::memory_order_relaxed);
    }

   private:
    // Counters live in a heap slot owned by the map, so a reader can drop the
    // map lock and still update/read a stable address. The lock only protects
    // slot lookup/creation.
    struct Slot {
        std::atomic<uint64_t> wrs{0};
        std::atomic<uint64_t> bytes{0};
    };

    // Keyed by the full path (generation included), not by its hash: two paths
    // that happen to collide must never share a row (review minor).
    mutable std::mutex mutex_;
    std::unordered_map<UbPostPath, std::shared_ptr<Slot>, UbPostPathHash>
        slots_;
    std::atomic<uint64_t> peak_wrs_{0};
    std::atomic<uint64_t> published_total_{0};
    std::atomic<uint64_t> finalized_total_{0};
    std::atomic<uint64_t> unmatched_finalize_{0};
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_PATH_LOAD_LEDGER_H_
