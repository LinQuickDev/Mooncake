// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// W3a/W3b/W3c unit tests: bounded queue arithmetic, the five-bucket defer
// registry with its epoch wake-up, and the task-level K gate.
//
// These are deliberately free of UbWorkers, URMA and the segment manager: the
// pieces under test are the pure ones (post_scheduler.h and slice.h), so a
// failure here is never a threading artifact of the transport.

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "tent/common/config.h"
#include "tent/transport/ub/params.h"
#include "tent/transport/ub/post_scheduler.h"
#include "tent/transport/ub/slice.h"

namespace mooncake::tent::ub {
namespace {

// ---------------------------------------------------------------------------
// §6.1 all-or-nothing queue arithmetic (W3-IN-4).
TEST(UbAdmissionTest, QueueBudgetCountsQueuedAndParkedTogether) {
    constexpr uint64_t kBudget = 100;
    // A Task that fits exactly is admitted.
    EXPECT_TRUE(businessQueueAdmits(kBudget, 40, 60, 0));
    EXPECT_TRUE(businessQueueAdmits(kBudget, 99, 0, 1));
    // One slice over the budget is rejected as a whole.
    EXPECT_FALSE(businessQueueAdmits(kBudget, 99, 0, 2));
    EXPECT_FALSE(businessQueueAdmits(kBudget, 50, 50, 1));
    // Parked slices are just as much queued work as queued ones.
    EXPECT_FALSE(businessQueueAdmits(kBudget, 0, 100, 1));
    // Nothing to add is always admissible, even at the limit: otherwise a
    // submit() with only terminal slices would report backpressure.
    EXPECT_TRUE(businessQueueAdmits(kBudget, 100, 0, 0));
    // Defensive: a corrupted (over-limit) count must not underflow into a huge
    // window.
    EXPECT_FALSE(businessQueueAdmits(kBudget, 200, 200, 1));
}

// ---------------------------------------------------------------------------
// §12 configuration gate: every W3 key is validated once at install time and
// the pooled profile fails closed.
TEST(UbAdmissionTest, PooledProfileRequiresTheFullKeyMatrix) {
    Config config;
    UbParams params;

    // Defaults are the RC legacy baseline and never request a pool.
    ASSERT_TRUE(UbParams::FromConfig(config, params).ok());
    EXPECT_EQ(params.runtime_profile, UbRuntimeProfile::kRcLegacy);
    EXPECT_FALSE(params.pooledSendPoolRequested());

    // rm_pool without the RM transport mode is rejected.
    config.set("transports/ub/runtime_profile", std::string("rm_pool"));
    config.set("transports/ub/jetty_pool/enable", true);
    EXPECT_FALSE(UbParams::FromConfig(config, params).ok());

    // ... with the RM mode it is accepted.
    config.set("transports/ub/trans_mode", std::string("rm"));
    ASSERT_TRUE(UbParams::FromConfig(config, params).ok());
    EXPECT_TRUE(params.pooledSendPoolRequested());

    // The runtime brake always wins.
    config.set("transports/ub/jetty_pool/force_disable", true);
    EXPECT_FALSE(UbParams::FromConfig(config, params).ok());
    config.set("transports/ub/jetty_pool/force_disable", false);

    // rm_pool does NOT force probe/enabled (M-3): probing stays an independent
    // v1.7 decision.
    EXPECT_FALSE(params.probe.enabled);
    ASSERT_TRUE(UbParams::FromConfig(config, params).ok());

    // A pool switch outside rm_pool is a structural conflict.
    Config legacy;
    legacy.set("transports/ub/jetty_pool/enable", true);
    EXPECT_FALSE(UbParams::FromConfig(legacy, params).ok());

    // rm_legacy needs trans_mode=rm and must not enable the pool.
    Config rm_legacy;
    rm_legacy.set("transports/ub/runtime_profile", std::string("rm_legacy"));
    EXPECT_FALSE(UbParams::FromConfig(rm_legacy, params).ok());
    rm_legacy.set("transports/ub/trans_mode", std::string("rm"));
    ASSERT_TRUE(UbParams::FromConfig(rm_legacy, params).ok());
    EXPECT_FALSE(params.pooledSendPoolRequested());
}

// §6.2: the queue budget must admit one maximal Task, because submit() is
// all-or-nothing and a smaller budget would reject large Tasks forever.
TEST(UbAdmissionTest, QueueBudgetMustAdmitOneMaximalTask) {
    Config config;
    UbParams params;
    config.set("transports/ub/max_slices_per_task", static_cast<int64_t>(64));
    config.set("transports/ub/business_queue_budget", static_cast<int64_t>(63));
    EXPECT_FALSE(UbParams::FromConfig(config, params).ok());
    config.set("transports/ub/business_queue_budget", static_cast<int64_t>(64));
    ASSERT_TRUE(UbParams::FromConfig(config, params).ok());
    EXPECT_EQ(params.business_queue_budget, 64U);
    EXPECT_EQ(params.task_max_inflight_attempts, 4U);
    EXPECT_EQ(params.defer_reflow_batch, 256U);
}

// ---------------------------------------------------------------------------
// §5.4 five buckets with independent epochs (D-9).
TEST(UbAdmissionTest, DeferRegistryKeepsBucketsIndependent) {
    DeferRegistry<uint64_t> registry;
    EXPECT_EQ(registry.epoch(DeferReason::kTaskPermit), 0U);

    ASSERT_TRUE(registry.park({1, DeferReason::kTaskPermit, 0}));
    ASSERT_TRUE(registry.park({2, DeferReason::kQuota, 0}));
    ASSERT_TRUE(registry.park({3, DeferReason::kPoolIdle, 0}));
    ASSERT_TRUE(registry.park({4, DeferReason::kPathUnavailable, 0}));
    EXPECT_EQ(registry.parkedTotal(), 4U);
    EXPECT_EQ(registry.parkedTotal(DeferReason::kQuota), 1U);

    // A K release wakes only the task-permit bucket.
    registry.bump(DeferReason::kTaskPermit);
    std::vector<uint64_t> ready;
    std::array<uint64_t, kDeferReasonCount> wakeups{};
    EXPECT_EQ(registry.drainDirty(ready, 16, 0, &wakeups), 1U);
    ASSERT_EQ(ready.size(), 1U);
    EXPECT_EQ(ready.front(), 1U);
    EXPECT_EQ(wakeups[static_cast<size_t>(DeferReason::kTaskPermit)], 1U);
    EXPECT_EQ(wakeups[static_cast<size_t>(DeferReason::kQuota)], 0U);
    // The other buckets are untouched and still parked.
    EXPECT_EQ(registry.parkedTotal(), 3U);
    // A drained bucket is not dirty any more: a second pass finds nothing.
    ready.clear();
    EXPECT_EQ(registry.drainDirty(ready, 16, 0, nullptr), 0U);
}

// The W2 bug this design exists to remove: a bump between "read the epoch" and
// "park" used to be lost, and the slice then waited for a reflow that could
// never happen. park() must refuse the registration so the caller can re-queue
// immediately.
TEST(UbAdmissionTest, DeferRegistryDetectsSnapshotThatAlreadyAdvanced) {
    DeferRegistry<uint64_t> registry;
    const uint64_t snapshot = registry.epoch(DeferReason::kQuota);
    registry.bump(DeferReason::kQuota);
    EXPECT_FALSE(registry.park({7, DeferReason::kQuota, snapshot}));
    EXPECT_EQ(registry.parkedTotal(), 0U);

    // A fresh snapshot parks normally.
    ASSERT_TRUE(registry.park(
        {8, DeferReason::kQuota, registry.epoch(DeferReason::kQuota)}));
    EXPECT_EQ(registry.parkedTotal(), 1U);
    // Parked against the current epoch: no wake-up until the next bump.
    std::vector<uint64_t> ready;
    EXPECT_EQ(registry.drainDirty(ready, 16, 0, nullptr), 0U);
    EXPECT_FALSE(registry.hasActionable(0));
    registry.bump(DeferReason::kQuota);
    EXPECT_TRUE(registry.hasActionable(0));
}

// Timed defers exist for conditions no release site can bump (a local device in
// recovery cooldown). They must not need a fake bump to be retried.
TEST(UbAdmissionTest, DeferRegistryHonoursTimedDeadlines) {
    DeferRegistry<uint64_t> registry;
    ASSERT_TRUE(registry.park({1, DeferReason::kPathUnavailable, 0, 500}));
    ASSERT_TRUE(registry.park({2, DeferReason::kQuota, 0, 0}));

    // Nothing due yet.
    EXPECT_FALSE(registry.hasActionable(499));
    std::vector<uint64_t> ready;
    EXPECT_EQ(registry.drainDirty(ready, 16, 499, nullptr), 0U);
    // The deadline passes: the timed item is reflowed without any bump, and the
    // unbounded one stays parked.
    EXPECT_TRUE(registry.hasActionable(500));
    ASSERT_EQ(registry.drainDirty(ready, 16, 500, nullptr), 1U);
    ASSERT_EQ(ready.size(), 1U);
    EXPECT_EQ(ready.front(), 1U);
    EXPECT_EQ(registry.parkedTotal(), 1U);
    EXPECT_EQ(registry.parkedTotal(DeferReason::kQuota), 1U);
}

// §18-9: one reflow pass is bounded, and the leftovers keep the bucket dirty so
// the next pass finishes the scan instead of dropping the wake-up.
TEST(UbAdmissionTest, DeferRegistryBatchLimitKeepsPendingWorkDirty) {
    DeferRegistry<uint64_t> registry;
    for (uint64_t i = 0; i < 10; ++i) {
        ASSERT_TRUE(registry.park({i, DeferReason::kPoolIdle, 0}));
    }
    registry.bump(DeferReason::kPoolIdle);
    std::vector<uint64_t> ready;
    EXPECT_EQ(registry.drainDirty(ready, 4, 0, nullptr), 4U);
    EXPECT_EQ(registry.parkedTotal(), 6U);
    EXPECT_TRUE(registry.hasActionable(0));

    ready.clear();
    EXPECT_EQ(registry.drainDirty(ready, 4, 0, nullptr), 4U);
    EXPECT_EQ(registry.parkedTotal(), 2U);
    ready.clear();
    EXPECT_EQ(registry.drainDirty(ready, 4, 0, nullptr), 2U);
    EXPECT_EQ(registry.parkedTotal(), 0U);
    EXPECT_FALSE(registry.hasActionable(0));
}

// §8.3: nothing may stay parked across a stop.
TEST(UbAdmissionTest, DeferRegistryDrainsEveryBucketOnStop) {
    DeferRegistry<uint64_t> registry;
    ASSERT_TRUE(registry.park({1, DeferReason::kTaskPermit, 0}));
    ASSERT_TRUE(registry.park({2, DeferReason::kQuota, 0, 1'000'000}));
    ASSERT_TRUE(registry.park({3, DeferReason::kPoolIdle, 0}));
    ASSERT_TRUE(registry.park({4, DeferReason::kPathUnavailable, 0}));

    std::vector<uint64_t> drained;
    // Bounded like every other pass.
    EXPECT_EQ(registry.drainAll(drained, 2), 2U);
    EXPECT_EQ(registry.parkedTotal(), 2U);
    EXPECT_EQ(registry.drainAll(drained, 2), 2U);
    EXPECT_EQ(registry.parkedTotal(), 0U);
    ASSERT_EQ(drained.size(), 4U);
}

namespace {

UbTask::Ptr makeTask(uint32_t k, size_t slice_count = 4) {
    Request request{};
    request.opcode = Request::WRITE;
    request.length = 16 * slice_count;
    auto task = UbTask::create(request, {}, 0, k);
    for (size_t i = 0; i < slice_count; ++i) {
        task->addSlice(UbSliceSpec{nullptr, i * 16, 16, i * 16, 0});
    }
    task->seal();
    return task;
}

}  // namespace

// W3 review B1: a slice deferred before beginAttempt() is still kQueued;
// requeueAfterDefer() must re-admit it, and must refuse a slice an attempt
// owns.
TEST(UbAdmissionTest, RequeueAfterDeferReAdmitsOnlyUnclaimedSlices) {
    auto task = makeTask(/*k=*/4, /*slice_count=*/1);
    const auto slice = task->slices().front();
    const UbPostPath path{0, 7, 0, 1};

    // Not yet claimed by an attempt: kInitial -> kQueued via markQueued.
    EXPECT_EQ(slice->snapshot().state, UbSliceState::kInitial);
    ASSERT_TRUE(slice->markQueued());
    EXPECT_EQ(slice->snapshot().state, UbSliceState::kQueued);

    // Deferred before beginAttempt(): still kQueued and must stay admissible.
    ASSERT_TRUE(slice->requeueAfterDefer());
    EXPECT_EQ(slice->snapshot().state, UbSliceState::kQueued);

    // A returned attempt (deferAttempt left it kRetryPending) becomes kQueued.
    auto token = slice->beginAttempt(path, 1);
    ASSERT_TRUE(token.has_value());
    ASSERT_TRUE(slice->deferAttempt(*token, 2));
    EXPECT_EQ(slice->snapshot().state, UbSliceState::kRetryPending);
    ASSERT_TRUE(slice->requeueAfterDefer());
    EXPECT_EQ(slice->snapshot().state, UbSliceState::kQueued);

    // While an attempt owns the slice, the defer path must not re-admit it.
    token = slice->beginAttempt(path, 3);
    ASSERT_TRUE(token.has_value());
    EXPECT_EQ(slice->snapshot().state, UbSliceState::kPosting);
    EXPECT_FALSE(slice->requeueAfterDefer());

    // Terminal or canceled slices are never re-admitted.
    (void)slice->requestCancellation(4);
    EXPECT_FALSE(slice->requeueAfterDefer());
}

// W3 review M1: a hot bucket must not starve the others when the reflow batch
// limit truncates a scan.
TEST(UbAdmissionTest, DeferRegistryRotatesSoHotBucketsCannotStarveOthers) {
    DeferRegistry<uint64_t> registry;
    for (uint64_t i = 0; i < 1000; ++i) {
        ASSERT_TRUE(registry.park({i, DeferReason::kTaskPermit, 0}));
    }
    // One item in the last bucket, parked against the same epoch.
    ASSERT_TRUE(registry.park({5000, DeferReason::kPathUnavailable, 0}));
    registry.bump(DeferReason::kTaskPermit);
    registry.bump(DeferReason::kPathUnavailable);

    bool starved = true;
    for (int pass = 0; pass < 8 && starved; ++pass) {
        std::vector<uint64_t> ready;
        registry.drainDirty(ready, 256, 0, nullptr);
        for (uint64_t value : ready) {
            if (value == 5000) starved = false;
        }
    }
    EXPECT_FALSE(starved)
        << "the path bucket was never visited while the K bucket stayed hot";
    EXPECT_EQ(registry.parkedTotal(DeferReason::kPathUnavailable), 0U);
}

// ---------------------------------------------------------------------------
// §7.3 task-level K (D-6). K caps ONE task, so a second task is unaffected.

TEST(UbAdmissionTest, TaskPermitCapsOnlyItsOwnTask) {
    auto task = makeTask(/*k=*/2);
    auto other = makeTask(/*k=*/2);
    EXPECT_TRUE(task->tryAcquireAttemptSlot());
    EXPECT_TRUE(task->tryAcquireAttemptSlot());
    // K is exhausted for this task only.
    EXPECT_FALSE(task->tryAcquireAttemptSlot());
    EXPECT_EQ(task->inflightAttempts(), 2U);
    EXPECT_TRUE(other->tryAcquireAttemptSlot());

    // A release frees exactly one slot, and an unmatched release never
    // underflows into an unlimited window.
    task->releaseAttemptSlot();
    EXPECT_TRUE(task->tryAcquireAttemptSlot());
    EXPECT_FALSE(task->tryAcquireAttemptSlot());
    for (int i = 0; i < 10; ++i) task->releaseAttemptSlot();
    EXPECT_EQ(task->inflightAttempts(), 0U);
    // Unmatched releases never underflow, but they are counted: a double
    // release must be observable (review minor).
    EXPECT_GT(task->slotUnderflows(), 0U);
    EXPECT_TRUE(task->tryAcquireAttemptSlot());
}

TEST(UbAdmissionTest, TaskPermitUnlimitedKeepsLegacyBehavior) {
    // K == 0 is the legacy "no task cap" value used by RC/RM legacy and every
    // pre-W3 caller.
    auto task = makeTask(/*k=*/0);
    for (int i = 0; i < 128; ++i) EXPECT_TRUE(task->tryAcquireAttemptSlot());
    EXPECT_EQ(task->inflightAttempts(), 128U);
    EXPECT_EQ(task->maxInflightAttempts(), 0U);
}

// ---------------------------------------------------------------------------
// §7.3/§8.4 deferAttempt: capacity pressure returns the slice to kRetryPending
// WITHOUT consuming retry budget (W3-IN-3).
TEST(UbAdmissionTest, DeferAttemptDoesNotConsumeRetryBudget) {
    auto task = makeTask(/*k=*/4, /*slice_count=*/1);
    ASSERT_EQ(task->slices().size(), 1U);
    const auto slice = task->slices().front();
    const UbPostPath path{0, 7, 0, 1};

    auto token = slice->beginAttempt(path, 1);
    ASSERT_TRUE(token.has_value());
    EXPECT_EQ(slice->snapshot().state, UbSliceState::kPosting);
    EXPECT_EQ(slice->snapshot().attempt, 1U);

    EXPECT_TRUE(slice->deferAttempt(*token, 2));
    EXPECT_EQ(slice->snapshot().state, UbSliceState::kRetryPending);
    EXPECT_EQ(slice->snapshot().retry_count, 0U);
    // The task never saw a terminal slice: deferral is not a failure.
    EXPECT_EQ(task->transferStatus().s, PENDING);

    // The next attempt is a new generation: the old token is stale and cannot
    // defer the new one.
    auto next = slice->beginAttempt(path, 3);
    ASSERT_TRUE(next.has_value());
    EXPECT_EQ(next->attempt, 2U);
    EXPECT_FALSE(slice->deferAttempt(*token, 4));
    EXPECT_EQ(slice->snapshot().state, UbSliceState::kPosting);
}

TEST(UbAdmissionTest, DeferAttemptOnCanceledSliceTerminalizes) {
    auto task = makeTask(/*k=*/4, /*slice_count=*/1);
    const auto slice = task->slices().front();
    const UbPostPath path{0, 7, 0, 1};
    auto token = slice->beginAttempt(path, 1);
    ASSERT_TRUE(token.has_value());

    // Cancellation loses to the posting claim, so the slice is still kPosting;
    // the deferral is then the only exit and it must not resurrect the slice.
    (void)slice->requestCancellation(2);
    EXPECT_EQ(slice->snapshot().state, UbSliceState::kPosting);
    EXPECT_TRUE(slice->deferAttempt(*token, 3));
    EXPECT_EQ(slice->snapshot().state, UbSliceState::kCanceled);
    EXPECT_EQ(task->transferStatus().s, CANCELED);
}

// submit() counts with queuedEligible() instead of mutating state, so a slice
// that is already terminal must not inflate the budget estimate.
TEST(UbAdmissionTest, QueuedEligibleMatchesTheSetMarkQueuedAccepts) {
    auto task = makeTask(/*k=*/0, /*slice_count=*/3);
    const auto slices = task->slices();
    ASSERT_EQ(slices.size(), 3U);
    for (const auto& slice : slices) EXPECT_TRUE(slice->queuedEligible());

    EXPECT_TRUE(slices[0]->markQueued());
    EXPECT_FALSE(slices[0]->queuedEligible());
    EXPECT_FALSE(slices[0]->markQueued());

    (void)slices[1]->requestCancellation();
    EXPECT_FALSE(slices[1]->queuedEligible());
    EXPECT_FALSE(slices[1]->markQueued());
    // The untouched slice is still a candidate.
    EXPECT_TRUE(slices[2]->queuedEligible());
}

}  // namespace
}  // namespace mooncake::tent::ub
