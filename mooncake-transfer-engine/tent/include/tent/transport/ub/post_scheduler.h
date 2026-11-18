// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// W3b: five-bucket defer registry (design §5.4 / §7.4).
//
// The W2 posting loop handled capacity pressure with a fixed 50us sleep and a
// blind requeue. That wastes CPU and, worse, loses wake-ups: nothing can tell a
// K release from a pool return, so every lane has to guess by time. This file
// replaces the guess with an availability epoch per reason:
//
//   * a lane reads the epoch of the bucket it is about to wait on *before* it
//     evaluates the condition;
//   * a release site bumps exactly the bucket it just unblocked;
//   * a parked item is only re-queued once its bucket advanced past the epoch
//     it was parked with, so a bump between "evaluate" and "park" cannot be
//     lost (the park re-checks the snapshot and bounces the item immediately).
//
// All methods assume the caller holds the scheduler's queue lock. The registry
// owns no lock of its own: parked slices and the business queue must be mutated
// under the same critical section, otherwise a slice could be parked after the
// bucket it waits on was bumped and never woken again.
//
// The payload is a template parameter so this header stays unit-testable
// without linking workers.cpp (the admission test drives it with a plain id).

#ifndef TENT_TRANSPORT_UB_POST_SCHEDULER_H_
#define TENT_TRANSPORT_UB_POST_SCHEDULER_H_

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <vector>

namespace mooncake::tent::ub {

// Why an attempt was deferred (design §5.4, D-9). Every reason names one
// resource whose release bumps its bucket:
//   kTaskPermit       task-level K slot          -> released with the record
//   kQuota            path quota reservation     -> released in onOwners
//   kPoolIdle         pool member / JFC credit   -> released by returnOnce
//   kPathUnavailable  health, generation, claim  -> bumped by PLM transitions
//
// kCredit is deliberately folded into kPoolIdle: the pooling side's tryLease
// returns one TooManyRequests for "stopping", "no member" and "no credit"
// (R-2), so splitting it here would invent a wake-up source that does not
// exist yet (§18-2).
enum class DeferReason : uint8_t {
    kTaskPermit = 0,
    kQuota = 1,
    kPoolIdle = 2,
    kPathUnavailable = 3,
    kCount = 4,
};

inline constexpr size_t kDeferReasonCount =
    static_cast<size_t>(DeferReason::kCount);

inline const char* deferReasonName(DeferReason reason) noexcept {
    switch (reason) {
        case DeferReason::kTaskPermit:
            return "task_permit";
        case DeferReason::kQuota:
            return "quota";
        case DeferReason::kPoolIdle:
            return "pool_idle";
        case DeferReason::kPathUnavailable:
            return "path_unavailable";
        case DeferReason::kCount:
            break;
    }
    return "unknown";
}

// W3a queue-capacity predicate (design §6.1, W3-IN-4). Queued and parked
// slices share one budget, and a Task is admitted all-or-nothing. Kept out of
// the workers class so the boundary can be unit tested directly and so the
// "capacity strictly before markQueued" rule has exactly one home.
[[nodiscard]] inline bool businessQueueAdmits(uint64_t budget, uint64_t queued,
                                              uint64_t parked, uint64_t need) {
    if (need == 0) return true;
    // The invariant queued + parked <= budget holds because every increment
    // passes this check first; clamping defends against a bug elsewhere turning
    // the subtraction below into a huge number.
    const uint64_t used =
        queued > budget - std::min(budget, parked) ? budget : queued + parked;
    if (used >= budget) return false;
    return need <= budget - used;
}

template <typename Payload>
class DeferRegistry {
   public:
    struct Item {
        Payload payload;
        DeferReason reason{DeferReason::kQuota};
        // Bucket epoch observed before the condition was evaluated.
        uint64_t wait_epoch{0};
        // Optional wall-clock deadline for conditions that no release site can
        // bump: a local device in recovery cooldown becomes usable again purely
        // with time. 0 means "only a bucket bump can wake me".
        uint64_t expire_ns{0};
    };

    [[nodiscard]] uint64_t epoch(DeferReason reason) const {
        return buckets_[indexOf(reason)].epoch;
    }

    // Registers one deferred item. Returns false when the bucket already
    // advanced past `item.wait_epoch`, i.e. the resource changed between the
    // evaluation and this call: the caller must re-queue the item immediately
    // instead of parking it, otherwise the wake-up is lost.
    bool park(Item item) {
        auto& bucket = buckets_[indexOf(item.reason)];
        if (item.wait_epoch != bucket.epoch) return false;
        ++bucket.count;
        ++parked_total_;
        bucket.items.push_back(std::move(item));
        return true;
    }

    // Marks one availability change. The epoch is monotonic, so a stale
    // snapshot can never match a later park().
    void bump(DeferReason reason) {
        auto& bucket = buckets_[indexOf(reason)];
        ++bucket.epoch;
        bucket.dirty = true;
    }

    [[nodiscard]] bool hasDirty() const {
        for (const auto& bucket : buckets_) {
            if (bucket.dirty) return true;
        }
        return false;
    }

    [[nodiscard]] size_t parkedTotal() const { return parked_total_; }

    [[nodiscard]] size_t parkedTotal(DeferReason reason) const {
        return buckets_[indexOf(reason)].count;
    }

    // Moves up to `max_items` items that became actionable out of the parked
    // buckets: either the bucket epoch advanced past their snapshot
    // (wait_epoch < current epoch) or their own deadline passed. `wakeups`
    // (optional) receives the per-bucket reflow counts for the
    // defer_epoch_wakeup{reason} metric.
    //
    // Park order is (epoch, deadline) order: every park observes the
    // then-current epoch and sets its deadline relative to the same clock, so
    // the first non-actionable item ends the scan for its bucket. A bucket
    // keeps its dirty flag while stale items are left behind by the batch
    // limit, so the next pass finishes the scan instead of dropping the
    // wake-up.
    size_t drainDirty(std::vector<Payload>& out, size_t max_items,
                      uint64_t now_ns,
                      std::array<uint64_t, kDeferReasonCount>* wakeups) {
        size_t drained = 0;
        // M1: the scan starts at a rotating cursor instead of bucket 0. With a
        // fixed start a hot bucket (typically kTaskPermit under load) could
        // consume the whole batch every pass and starve the others forever.
        for (size_t step = 0; step < kDeferReasonCount; ++step) {
            const size_t index = (next_bucket_ + step) % kDeferReasonCount;
            auto& bucket = buckets_[index];
            // A bucket is scannable when it was bumped, or when its head is a
            // timed defer (no bump will ever come for those).
            const bool timed_head =
                !bucket.items.empty() && bucket.items.front().expire_ns != 0;
            if (!bucket.dirty && !timed_head) continue;
            if (drained >= max_items) break;
            while (!bucket.items.empty() && drained < max_items &&
                   actionable(bucket, bucket.items.front(), now_ns)) {
                Item item = std::move(bucket.items.front());
                bucket.items.pop_front();
                --bucket.count;
                --parked_total_;
                out.push_back(std::move(item.payload));
                ++drained;
                if (wakeups != nullptr) ++(*wakeups)[index];
            }
            bucket.dirty = !bucket.items.empty() &&
                           actionable(bucket, bucket.items.front(), now_ns);
            if (drained >= max_items) {
                // Resume at the next bucket so the truncation cannot keep
                // punishing the same tail.
                next_bucket_ = (index + 1) % kDeferReasonCount;
                return drained;
            }
        }
        // A completed pass also advances the cursor, which keeps the order
        // fair between buckets that are always actionable.
        next_bucket_ = (next_bucket_ + 1) % kDeferReasonCount;
        return drained;
    }

    // True when at least one parked item could be re-queued right now. Used by
    // the posting loop instead of sleeping on a fixed backoff.
    [[nodiscard]] bool hasActionable(uint64_t now_ns) const {
        for (size_t index = 0; index < kDeferReasonCount; ++index) {
            const auto& bucket = buckets_[index];
            if (bucket.items.empty()) continue;
            if (actionable(bucket, bucket.items.front(), now_ns)) return true;
        }
        return false;
    }

    // Shutdown drain: every bucket, every item, ignoring epochs. The caller
    // cancels or re-queues the returned payloads; nothing stays parked across
    // a stop (design §8.3).
    size_t drainAll(std::vector<Payload>& out, size_t max_items) {
        size_t drained = 0;
        for (auto& bucket : buckets_) {
            while (!bucket.items.empty() && drained < max_items) {
                Item item = std::move(bucket.items.front());
                bucket.items.pop_front();
                --bucket.count;
                --parked_total_;
                out.push_back(std::move(item.payload));
                ++drained;
            }
            bucket.dirty = !bucket.items.empty();
            if (drained >= max_items) break;
        }
        return drained;
    }

    // Deepest bucket right now (queue_depth/parked_depth observation).
    [[nodiscard]] size_t deepest() const {
        size_t deepest = 0;
        for (const auto& bucket : buckets_) {
            if (bucket.count > deepest) deepest = bucket.count;
        }
        return deepest;
    }

   private:
    struct Bucket {
        std::deque<Item> items;
        uint64_t epoch{0};
        size_t count{0};
        bool dirty{false};
    };

    // One item is actionable when its bucket moved on since it was parked, or
    // when its own timed deadline passed (no release site can bump those).
    static bool actionable(const Bucket& bucket, const Item& item,
                           uint64_t now_ns) {
        if (item.wait_epoch < bucket.epoch) return true;
        return item.expire_ns != 0 && item.expire_ns <= now_ns;
    }

    static size_t indexOf(DeferReason reason) {
        const size_t index = static_cast<size_t>(reason);
        return index < kDeferReasonCount ? index : 0;
    }

    std::array<Bucket, kDeferReasonCount> buckets_{};
    size_t parked_total_{0};
    // Round-robin start of the next drainDirty() scan (M1).
    size_t next_bucket_{0};
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_POST_SCHEDULER_H_
