// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Per-context send-Jetty pool, merged from the pooling side (`930/Mooncake` @
// a9edee30, M-1) with two W3 additions marked "W3 R-2" below.
//
// This file owns the native send resource of one attempt: the lease. W3's job
// is to hold it, post exactly one WR through it and hand it back exactly once
// (design §8/§10).

#ifndef TENT_TRANSPORT_UB_SEND_JETTY_POOL_H_
#define TENT_TRANSPORT_UB_SEND_JETTY_POOL_H_

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "tent/common/status.h"
#include "tent/transport/ub/admission.h"
#include "tent/transport/ub/jfc.h"
#include "tent/transport/ub/native_budget.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake::tent::ub {

// PoolKey partitions send Jetties by Context and LocalSendProfile (D01).
// Two attempts with different profiles never share a member.
struct PoolKey {
    uint64_t context_incarnation = 0;
    uint16_t trans_mode = 0;
    uint8_t priority = 0;
    uint32_t bind_tp_type_mask = 0;

    bool operator==(const PoolKey&) const = default;
};

struct PoolKeyHash {
    size_t operator()(const PoolKey& key) const noexcept {
        size_t seed = std::hash<uint64_t>{}(key.context_incarnation);
        auto combine = [&seed](size_t value) {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
        };
        combine(std::hash<uint16_t>{}(key.trans_mode));
        combine(std::hash<uint8_t>{}(key.priority));
        combine(std::hash<uint32_t>{}(key.bind_tp_type_mask));
        return seed;
    }
};

// AttemptKey identifies one business attempt of one slice (D01). Retries
// must use a new attempt_generation; a lease is bound to exactly one
// attempt and one native post of exactly one WR (INV03).
struct AttemptKey {
    uint64_t task_id = 0;
    uint64_t slice_id = 0;
    uint64_t attempt_generation = 0;

    bool operator==(const AttemptKey&) const = default;

    [[nodiscard]] bool valid() const noexcept {
        return task_id != 0 && slice_id != 0 && attempt_generation != 0;
    }
};

// PoolLaneKey identifies one pooled member generation. A retired member keeps
// its lane so late completions can be attributed; the generation is never
// reused by a fresh native Jetty (D01/INV15).
//
// Merge note (M-1): the pooling side spells this type `LaneKey`, but this
// baseline's W1 probe contract already froze `LaneKey` as the probe channel
// identity (probe_resources.h) in the same namespace. Only the name differs;
// the shape and the "generation is never reused" rule are unchanged.
struct PoolLaneKey {
    uint32_t jetty_id = 0;
    uint64_t lane_generation = 0;

    bool operator==(const PoolLaneKey&) const = default;
};

enum class PoolMemberState : uint8_t {
    kCreating = 0,
    kIdle,
    kInUse,
    kFaulted,
    kDraining,
    kDeleteReady,
    kDeleting,
    kQuarantined,
    kDestroyed,
};

inline const char* poolMemberStateName(PoolMemberState state) noexcept {
    switch (state) {
        case PoolMemberState::kCreating:
            return "creating";
        case PoolMemberState::kIdle:
            return "idle";
        case PoolMemberState::kInUse:
            return "in_use";
        case PoolMemberState::kFaulted:
            return "faulted";
        case PoolMemberState::kDraining:
            return "draining";
        case PoolMemberState::kDeleteReady:
            return "delete_ready";
        case PoolMemberState::kDeleting:
            return "deleting";
        case PoolMemberState::kQuarantined:
            return "quarantined";
        case PoolMemberState::kDestroyed:
            return "destroyed";
    }
    return "unknown";
}

// One pooled send Jetty. The member keeps its budget reservation until the
// native destruction is proven; state follows the C08 lifecycle
// (IDLE/IN_USE are the static Serving set).
struct PoolMember {
    PoolLaneKey lane{};
    PoolKey pool_key{};
    JettyPtr jetty;
    size_t jfc_index = 0;
    BudgetReservation budget{};
    PoolMemberState state = PoolMemberState::kCreating;
    std::string retire_reason;
};

class SendJettyPool;

// Move-only exclusive lease for one attempt (D03). The lease is returned
// exactly once through SendJettyPool::returnOnce() (normal or retire path).
// The destructor never calls native delete: a dropped lease is a
// responsibility leak, so the pool keeps the member out of circulation,
// retains its budget and counts the leak for diagnostics.
class SliceJettyLease {
   public:
    SliceJettyLease() = default;
    SliceJettyLease(SliceJettyLease&& other) noexcept;
    SliceJettyLease& operator=(SliceJettyLease&& other) noexcept;
    ~SliceJettyLease();

    SliceJettyLease(const SliceJettyLease&) = delete;
    SliceJettyLease& operator=(const SliceJettyLease&) = delete;

    [[nodiscard]] bool valid() const noexcept { return member_ != nullptr; }
    // Safe on an invalid lease: a diagnostic path must never dereference a
    // dropped member (review minor).
    [[nodiscard]] const PoolLaneKey& lane() const noexcept {
        static const PoolLaneKey kNoLane{};
        return member_ ? member_->lane : kNoLane;
    }
    [[nodiscard]] const AttemptKey& attempt() const noexcept {
        return attempt_;
    }
    [[nodiscard]] std::shared_ptr<PoolMember> member() const noexcept {
        return member_;
    }

   private:
    friend class SendJettyPool;
    SliceJettyLease(SendJettyPool* pool, std::shared_ptr<PoolMember> member,
                    AttemptKey attempt)
        : pool_(pool), member_(std::move(member)), attempt_(attempt) {}

    void detach() noexcept {
        pool_ = nullptr;
        member_ = nullptr;
        attempt_ = AttemptKey{};
    }

    SendJettyPool* pool_ = nullptr;
    std::shared_ptr<PoolMember> member_;
    AttemptKey attempt_{};
};

// Lease plus the non-blocking JFC credit reservation for the WR the caller
// is about to post. The credit token must be released on the completion or
// rollback path, exactly once.
struct LeaseWithCredit {
    SliceJettyLease lease;
    size_t jfc_index = 0;
    uint64_t jfc_credit_token = 0;
};

struct SendJettyPoolOptions {
    // Static Serving (IDLE + IN_USE) per PoolKey. The pool never grows
    // beyond it; all-IN_USE therefore defers instead of expanding (C05).
    size_t desired_serving = 0;
    // Bound on concurrent creations per PoolKey so refill cannot storm the
    // provider.
    size_t max_creating = 2;
    JettyOptions jetty_options;
};

struct SendJettyPoolStats {
    size_t idle_members = 0;
    size_t in_use_members = 0;
    size_t creating_members = 0;
    size_t retired_members = 0;
    size_t quarantined_members = 0;
    uint64_t total_leases = 0;
    uint64_t total_returns = 0;
    uint64_t pool_misses = 0;
    uint64_t stop_rejections = 0;
    uint64_t leaked_leases = 0;
    uint64_t created_members = 0;
    uint64_t create_failures = 0;
    uint64_t double_returns = 0;
};

// Per-attempt business send-Jetty pool (C06, INV03/INV04/INV06). One pool
// serves every PoolKey of one native Context. Members are maintained as a
// LIFO stack per PoolKey: returns push_front, leases pop the front (most
// recently returned first), freshly created members are appended at the
// back. JFC credit matching happens inside the LIFO order: the lease scans
// from the front for the newest member whose JFC credit is available this
// round. All provider calls happen outside the pool lock.
class SendJettyPool final {
   public:
    SendJettyPool(std::shared_ptr<UrmaAdapter> adapter,
                  ContextPtr context_handle,
                  std::vector<std::shared_ptr<UbJfc>> jfcs,
                  std::shared_ptr<NativeBudget> budget, std::string device_key,
                  std::shared_ptr<JfcCreditLedger> credits,
                  SendJettyPoolOptions options);

    SendJettyPool(const SendJettyPool&) = delete;
    SendJettyPool& operator=(const SendJettyPool&) = delete;

    // Non-blocking lease for one attempt. Status codes:
    //   OK               lease (and JFC credit) acquired
    //   TooManyRequests  defer: pool/quota/credit insufficient (never a
    //                    retry charge, never a path fault, never a bypass)
    //   InvalidArgument  bad key/attempt or pool not attached
    Status tryLease(const PoolKey& pool_key, const AttemptKey& attempt,
                    LeaseWithCredit& out);

    // Returns a lease exactly once. completion_gate_passed=true (the C07
    // normal gate) pushes the member back to the idle front; false or a
    // stopping pool retires the member permanently. Returns
    // InvalidArgument on a double return.
    Status returnOnce(SliceJettyLease lease, bool completion_gate_passed);

    // Retires the leased member after the first real error (INV07: the
    // member never returns to the pool, even if the path recovers).
    Status retireFaulted(SliceJettyLease lease, const std::string& reason);

    // Stops new leases/creations. Outstanding leases still settle through
    // returnOnce()/retireFaulted(); their members are retired, never reused.
    void beginStop();
    [[nodiscard]] bool stopping() const;

    // Deletes every idle member (shutdown path). Handles whose delete fails
    // stay owned by the pool for the next drain attempt; their budget stays
    // charged until destruction is proven.
    Status drainIdle();

    // Attempts to delete quarantined members (e.g. during shutdown after
    // their responsibilities drained). Successful deletes release budget.
    Status drainQuarantined();

    [[nodiscard]] SendJettyPoolStats stats() const;
    [[nodiscard]] std::vector<PoolMemberState> memberStates() const;
    [[nodiscard]] size_t servingCount(const PoolKey& pool_key) const;

    // W3 R-2: non-blocking pre-screen queries. `servingCount()` alone cannot
    // tell "every member is busy" from "the pool is empty and still creating",
    // so path selection can only pre-filter with these. The authority stays
    // with tryLease(): a positive idleCount still races other attempts, so a
    // miss remains a normal kPoolIdle defer (design §7.1).
    //
    // A richer LeaseMissReason (stopping/idle/credit/budget) is R-2 on the
    // pooling side and is deliberately not invented here; until it lands,
    // credit and pool-idle misses share the kPoolIdle defer bucket (§18-2).
    [[nodiscard]] size_t idleCount(const PoolKey& pool_key) const;
    [[nodiscard]] size_t creatingCount(const PoolKey& pool_key) const;

    // The JFC credit ledger shared by every lease of this pool. Credits are
    // released by the caller (completion or rollback path), never by the pool.
    [[nodiscard]] const std::shared_ptr<JfcCreditLedger>& creditLedger() const {
        return credits_;
    }

   private:
    friend class SliceJettyLease;  // reports dropped leases

    struct PoolBucket {
        std::deque<std::shared_ptr<PoolMember>> idle;  // front = newest
        size_t in_use = 0;
        size_t creating = 0;
    };

    Status createAndPublish(const PoolKey& pool_key);
    void retireMemberLocked(std::shared_ptr<PoolMember> member,
                            const std::string& reason);
    void destroyRetired(std::vector<std::shared_ptr<PoolMember>> members);
    void onLeaseLeaked(const std::shared_ptr<PoolMember>& member);

    const std::shared_ptr<UrmaAdapter> adapter_;
    const ContextPtr context_handle_;
    const std::vector<std::shared_ptr<UbJfc>> jfcs_;
    const std::shared_ptr<NativeBudget> budget_;
    const std::string device_key_;
    const std::shared_ptr<JfcCreditLedger> credits_;
    const SendJettyPoolOptions options_;

    mutable std::mutex mutex_;  // never held across provider calls
    std::unordered_map<PoolKey, PoolBucket, PoolKeyHash> buckets_;
    std::vector<std::shared_ptr<PoolMember>> retired_;
    std::vector<std::shared_ptr<PoolMember>> quarantined_;
    uint64_t next_lane_generation_ = 1;
    size_t next_jfc_rotation_ = 0;
    bool stopping_ = false;

    // Diagnostics counters (protected by mutex_).
    uint64_t total_leases_ = 0;
    uint64_t total_returns_ = 0;
    uint64_t pool_misses_ = 0;
    uint64_t stop_rejections_ = 0;
    uint64_t leaked_leases_ = 0;
    uint64_t created_members_ = 0;
    uint64_t create_failures_ = 0;
    uint64_t double_returns_ = 0;
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_SEND_JETTY_POOL_H_
