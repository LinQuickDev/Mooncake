// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/send_jetty_pool.h"

#include <utility>

namespace mooncake::tent::ub {

SliceJettyLease::SliceJettyLease(SliceJettyLease&& other) noexcept
    : pool_(other.pool_),
      member_(std::move(other.member_)),
      attempt_(other.attempt_) {
    other.pool_ = nullptr;
    other.attempt_ = AttemptKey{};
}

SliceJettyLease& SliceJettyLease::operator=(SliceJettyLease&& other) noexcept {
    if (this != &other) {
        if (valid() && pool_ != nullptr) pool_->onLeaseLeaked(member_);
        pool_ = other.pool_;
        member_ = std::move(other.member_);
        attempt_ = other.attempt_;
        other.pool_ = nullptr;
        other.attempt_ = AttemptKey{};
    }
    return *this;
}

SliceJettyLease::~SliceJettyLease() {
    if (valid() && pool_ != nullptr) pool_->onLeaseLeaked(member_);
}

SendJettyPool::SendJettyPool(std::shared_ptr<UrmaAdapter> adapter,
                             ContextPtr context_handle,
                             std::vector<std::shared_ptr<UbJfc>> jfcs,
                             std::shared_ptr<NativeBudget> budget,
                             std::string device_key,
                             std::shared_ptr<JfcCreditLedger> credits,
                             SendJettyPoolOptions options)
    : adapter_(std::move(adapter)),
      context_handle_(std::move(context_handle)),
      jfcs_(std::move(jfcs)),
      budget_(std::move(budget)),
      device_key_(std::move(device_key)),
      credits_(std::move(credits)),
      options_(options) {}

Status SendJettyPool::tryLease(const PoolKey& pool_key,
                               const AttemptKey& attempt,
                               LeaseWithCredit& out) {
    out = LeaseWithCredit{};
    if (!attempt.valid()) {
        return Status::InvalidArgument(
            "UB send pool lease requires a valid attempt key" LOC_MARK);
    }
    if (adapter_ == nullptr || context_handle_ == nullptr || jfcs_.empty() ||
        budget_ == nullptr || credits_ == nullptr) {
        return Status::InvalidArgument(
            "UB send pool is not fully attached" LOC_MARK);
    }

    for (;;) {
        std::shared_ptr<PoolMember> chosen;
        size_t chosen_jfc = 0;
        uint64_t credit_token = 0;
        bool can_create = false;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopping_) {
                ++stop_rejections_;
                return Status::TooManyRequests(
                    "UB send pool is stopping; defer the attempt" LOC_MARK);
            }
            PoolBucket& bucket = buckets_[pool_key];
            // LIFO scan: newest returned member first; JFC credit matching
            // happens inside the LIFO order (non-blocking reservation).
            for (auto it = bucket.idle.begin(); it != bucket.idle.end(); ++it) {
                auto token = credits_->tryReserve((*it)->jfc_index, 1);
                if (!token.has_value()) continue;
                chosen = std::move(*it);
                chosen_jfc = chosen->jfc_index;
                credit_token = *token;
                bucket.idle.erase(it);
                break;
            }
            if (chosen != nullptr) {
                chosen->state = PoolMemberState::kInUse;
                ++bucket.in_use;
                ++total_leases_;
            } else {
                const size_t serving = bucket.idle.size() + bucket.in_use;
                can_create =
                    serving + bucket.creating < options_.desired_serving &&
                    bucket.creating < options_.max_creating;
                if (can_create) ++bucket.creating;
            }
        }

        if (chosen != nullptr) {
            out.lease = SliceJettyLease(this, std::move(chosen), attempt);
            out.jfc_index = chosen_jfc;
            out.jfc_credit_token = credit_token;
            return Status::OK();
        }

        if (!can_create) {
            std::lock_guard<std::mutex> lock(mutex_);
            ++pool_misses_;
            return Status::TooManyRequests(
                "UB send pool has no leaseable member; defer the "
                "attempt" LOC_MARK);
        }

        // Creation happens outside the pool lock; the slot was claimed
        // above so concurrent leases cannot double count it.
        const Status created = createAndPublish(pool_key);
        if (!created.ok()) {
            std::lock_guard<std::mutex> lock(mutex_);
            ++pool_misses_;
            return Status::TooManyRequests(
                "UB send pool could not create a member this round; defer "
                "the attempt" LOC_MARK);
        }
        // Loop back: the published member is at the LIFO tail and the scan
        // will find it (or another concurrent leaser may have taken it, in
        // which case we re-evaluate deficit/miss).
    }
}

Status SendJettyPool::createAndPublish(const PoolKey& pool_key) {
    // Budget first: a member without device budget must never be created.
    BudgetQuantities quantities;
    quantities.jetties = 1;
    auto reservation =
        budget_->tryReserve(device_key_, BudgetRole::kBusiness, quantities);
    if (!reservation.has_value()) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto bucket = buckets_.find(pool_key);
        if (bucket != buckets_.end() && bucket->second.creating > 0) {
            --bucket->second.creating;
        }
        ++create_failures_;
        return Status::TooManyRequests(
            "UB native budget exhausted for device " + device_key_ + LOC_MARK);
    }

    size_t jfc_index = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        jfc_index = next_jfc_rotation_++ % jfcs_.size();
    }

    JettyPtr jetty;
    const Status created =
        adapter_->createJetty(context_handle_, jfcs_[jfc_index]->handle(),
                              options_.jetty_options, jetty);

    if (!created.ok()) {
        // Provider contract: a failed createJetty may still hand back a
        // partially allocated handle. It was never posted, so one
        // best-effort delete is the natural cleanup; either way the budget
        // reservation is released because no serving object exists.
        if (jetty) (void)adapter_->deleteJetty(jetty);
        budget_->transition(*reservation, BudgetObjectState::kQuarantined);
        budget_->releaseAfterProvenDestruction(*reservation, true);
        std::lock_guard<std::mutex> lock(mutex_);
        auto bucket = buckets_.find(pool_key);
        if (bucket != buckets_.end() && bucket->second.creating > 0) {
            --bucket->second.creating;
        }
        ++create_failures_;
        return created;
    }

    // Re-check under the lock: publish only when the pool is still
    // accepting; otherwise the fresh Jetty is retired (stopped result).
    std::shared_ptr<PoolMember> member;
    bool publish = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto bucket = buckets_.find(pool_key);
        if (bucket != buckets_.end() && bucket->second.creating > 0) {
            --bucket->second.creating;
        }
        if (!stopping_) {
            member = std::make_shared<PoolMember>();
            member->lane = PoolLaneKey{jetty->id(), next_lane_generation_++};
            member->pool_key = pool_key;
            member->jetty = std::move(jetty);
            member->jfc_index = jfc_index;
            member->budget = std::move(*reservation);
            member->state = PoolMemberState::kIdle;
            const Status activated =
                budget_->transition(member->budget, BudgetObjectState::kActive);
            if (activated.ok()) {
                buckets_[pool_key].idle.push_back(member);  // LIFO tail
                ++created_members_;
                publish = true;
            }
        }
    }

    if (publish) return Status::OK();

    // Stopped (or activation failed) after creation: retire the fresh
    // object. It never posted a WR, so deletion proves destruction and the
    // budget can be released.
    if (member != nullptr) {
        reservation = std::move(member->budget);
        budget_->transition(*reservation, BudgetObjectState::kQuarantined);
        adapter_->deleteJetty(member->jetty);
        budget_->releaseAfterProvenDestruction(*reservation, true);
    } else {
        budget_->transition(*reservation, BudgetObjectState::kQuarantined);
        adapter_->deleteJetty(jetty);
        budget_->releaseAfterProvenDestruction(*reservation, true);
    }
    return Status::TooManyRequests(
        "UB send pool stopped while creating a member" LOC_MARK);
}

Status SendJettyPool::returnOnce(SliceJettyLease lease,
                                 bool completion_gate_passed) {
    if (!lease.valid()) {
        return Status::InvalidArgument(
            "UB send pool return requires a valid lease" LOC_MARK);
    }
    auto member = lease.member();
    lease.detach();

    std::vector<std::shared_ptr<PoolMember>> to_destroy;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (member->state != PoolMemberState::kInUse) {
            ++double_returns_;
            return Status::InvalidArgument(
                "UB send pool lease returned twice" LOC_MARK);
        }
        auto bucket = buckets_.find(member->pool_key);
        if (bucket != buckets_.end() && bucket->second.in_use > 0) {
            --bucket->second.in_use;
        }
        ++total_returns_;
        if (completion_gate_passed && !stopping_) {
            member->state = PoolMemberState::kIdle;
            buckets_[member->pool_key].idle.push_front(member);  // LIFO head
        } else {
            retireMemberLocked(member,
                               stopping_ ? "pool stopping" : "gate failed");
            to_destroy = retired_;
            retired_.clear();
        }
    }

    if (!to_destroy.empty()) destroyRetired(std::move(to_destroy));
    return Status::OK();
}

Status SendJettyPool::retireFaulted(SliceJettyLease lease,
                                    const std::string& reason) {
    if (!lease.valid()) {
        return Status::InvalidArgument(
            "UB send pool retire requires a valid lease" LOC_MARK);
    }
    auto member = lease.member();
    lease.detach();

    std::vector<std::shared_ptr<PoolMember>> to_destroy;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (member->state != PoolMemberState::kInUse) {
            ++double_returns_;
            return Status::InvalidArgument(
                "UB send pool lease retired twice" LOC_MARK);
        }
        auto bucket = buckets_.find(member->pool_key);
        if (bucket != buckets_.end() && bucket->second.in_use > 0) {
            --bucket->second.in_use;
        }
        ++total_returns_;
        retireMemberLocked(member, reason.empty() ? "fault" : reason);
        to_destroy = std::move(retired_);
        retired_.clear();
    }

    if (!to_destroy.empty()) destroyRetired(std::move(to_destroy));
    return Status::OK();
}

void SendJettyPool::retireMemberLocked(std::shared_ptr<PoolMember> member,
                                       const std::string& reason) {
    member->state = PoolMemberState::kDraining;
    member->retire_reason = reason;
    // First real error retires permanently (INV07): the member never
    // re-enters the idle stack even if the path recovers.
    retired_.push_back(std::move(member));
}

void SendJettyPool::destroyRetired(
    std::vector<std::shared_ptr<PoolMember>> members) {
    // All native calls happen outside the pool lock.
    for (auto& member : members) {
        member->state = PoolMemberState::kDeleting;
        JettyPtr jetty = std::move(member->jetty);
        const Status deleted = adapter_->deleteJetty(jetty);
        if (deleted.ok()) {
            member->state = PoolMemberState::kDestroyed;
            budget_->transition(member->budget, BudgetObjectState::kRetiring);
            budget_->releaseAfterProvenDestruction(member->budget, true);
        } else {
            // Unknown destruction: keep owner and budget (INV09/INV10).
            // The handle goes back to the member so the next drain attempt
            // can retry the native delete; dropping it here would lose the
            // native ownership and leak the budget.
            member->jetty = std::move(jetty);
            member->state = PoolMemberState::kQuarantined;
            budget_->transition(member->budget,
                                BudgetObjectState::kQuarantined);
            std::lock_guard<std::mutex> lock(mutex_);
            quarantined_.push_back(member);
        }
    }
}

void SendJettyPool::onLeaseLeaked(const std::shared_ptr<PoolMember>& member) {
    // A lease dropped without returnOnce(): responsibility is unknown, so
    // the member is quarantined with its budget retained; it never returns
    // to circulation.
    std::lock_guard<std::mutex> lock(mutex_);
    ++leaked_leases_;
    if (member == nullptr) return;
    if (member->state == PoolMemberState::kInUse) {
        auto bucket = buckets_.find(member->pool_key);
        if (bucket != buckets_.end() && bucket->second.in_use > 0) {
            --bucket->second.in_use;
        }
    }
    member->state = PoolMemberState::kQuarantined;
    budget_->transition(member->budget, BudgetObjectState::kQuarantined);
    quarantined_.push_back(member);
}

void SendJettyPool::beginStop() {
    std::lock_guard<std::mutex> lock(mutex_);
    stopping_ = true;
}

bool SendJettyPool::stopping() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return stopping_;
}

Status SendJettyPool::drainIdle() {
    std::vector<std::shared_ptr<PoolMember>> to_destroy;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& entry : buckets_) {
            for (auto& member : entry.second.idle) {
                member->retire_reason = "drain idle";
                to_destroy.push_back(std::move(member));
            }
            entry.second.idle.clear();
        }
    }
    destroyRetired(std::move(to_destroy));
    return Status::OK();
}

Status SendJettyPool::drainQuarantined() {
    std::vector<std::shared_ptr<PoolMember>> candidates;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        candidates = std::move(quarantined_);
        quarantined_.clear();
    }
    for (auto& member : candidates) {
        if (member->jetty == nullptr) {
            member->state = PoolMemberState::kDestroyed;
            budget_->releaseAfterProvenDestruction(member->budget, true);
            continue;
        }
        JettyPtr jetty = std::move(member->jetty);
        const Status deleted = adapter_->deleteJetty(jetty);
        if (deleted.ok()) {
            member->state = PoolMemberState::kDestroyed;
            budget_->releaseAfterProvenDestruction(member->budget, true);
        } else {
            // Keep the handle owned by the pool for the next drain attempt.
            member->jetty = std::move(jetty);
            std::lock_guard<std::mutex> lock(mutex_);
            quarantined_.push_back(member);
        }
    }
    return Status::OK();
}

SendJettyPoolStats SendJettyPool::stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    SendJettyPoolStats result{};
    for (const auto& entry : buckets_) {
        result.idle_members += entry.second.idle.size();
        result.in_use_members += entry.second.in_use;
        result.creating_members += entry.second.creating;
    }
    result.retired_members = retired_.size();
    result.quarantined_members = quarantined_.size();
    result.total_leases = total_leases_;
    result.total_returns = total_returns_;
    result.pool_misses = pool_misses_;
    result.stop_rejections = stop_rejections_;
    result.leaked_leases = leaked_leases_;
    result.created_members = created_members_;
    result.create_failures = create_failures_;
    result.double_returns = double_returns_;
    return result;
}

std::vector<PoolMemberState> SendJettyPool::memberStates() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<PoolMemberState> states;
    for (const auto& entry : buckets_) {
        for (const auto& member : entry.second.idle) {
            states.push_back(member->state);
        }
        states.insert(states.end(), entry.second.in_use,
                      PoolMemberState::kInUse);
    }
    for (const auto& member : retired_) states.push_back(member->state);
    for (const auto& member : quarantined_) states.push_back(member->state);
    return states;
}

size_t SendJettyPool::servingCount(const PoolKey& pool_key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto bucket = buckets_.find(pool_key);
    if (bucket == buckets_.end()) return 0;
    return bucket->second.idle.size() + bucket->second.in_use;
}

size_t SendJettyPool::idleCount(const PoolKey& pool_key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto bucket = buckets_.find(pool_key);
    if (bucket == buckets_.end()) return 0;
    return bucket->second.idle.size();
}

size_t SendJettyPool::creatingCount(const PoolKey& pool_key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto bucket = buckets_.find(pool_key);
    if (bucket == buckets_.end()) return 0;
    return bucket->second.creating;
}

}  // namespace mooncake::tent::ub
