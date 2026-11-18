// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Completion gate unit tests (W2a).
//
// Design: docs/analyze/tent-ub-w2-completion-gate-design.md §6.6 (interleaving
// matrix) and §11.1 (G-1..G-15). These tests link only completion_record.cpp:
// no workers, no slices, no adapter, no threads unless the case is about
// threads. A recording sink stands in for the data path owner.

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <cstdint>
#include <mutex>
#include <thread>
#include <vector>

#include "tent/transport/ub/completion_record.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake::tent::ub {
namespace {

using gate::advance;
using gate::HandlerRef;
using gate::onFenceFailed;
using gate::onFenceProven;
using gate::onPostReturn;
using gate::onRealCr;
using gate::onTimeout;
using gate::onUnposted;
using gate::readyForNormalReturn;
using gate::Sink;

// Records every callback in order. Thread-safe: the interleaving cases drive it
// from a poller thread and a post thread at the same time.
class RecordingSink final : public Sink {
   public:
    void onSettle(CompletionRecord&, LogicalOutcome outcome) override {
        std::lock_guard<std::mutex> lock(mutex_);
        settles_.push_back(outcome);
    }
    void onOwners(CompletionRecord&, OwnerDisposition disposition) override {
        std::lock_guard<std::mutex> lock(mutex_);
        dispositions_.push_back(disposition);
    }
    void onFault(CompletionRecord&, FaultKind kind) override {
        std::lock_guard<std::mutex> lock(mutex_);
        faults_.push_back(kind);
    }
    void onBlocked(CompletionRecord&, BlockReason reason) override {
        std::lock_guard<std::mutex> lock(mutex_);
        blocked_.push_back(reason);
    }
    void onDuplicate(CompletionRecord&) override {
        std::lock_guard<std::mutex> lock(mutex_);
        ++duplicates_;
    }

    [[nodiscard]] size_t settleCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return settles_.size();
    }
    [[nodiscard]] std::vector<LogicalOutcome> settles() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return settles_;
    }
    [[nodiscard]] std::vector<OwnerDisposition> dispositions() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return dispositions_;
    }
    [[nodiscard]] size_t dispositionCount(OwnerDisposition disposition) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return static_cast<size_t>(std::count(
            dispositions_.begin(), dispositions_.end(), disposition));
    }
    [[nodiscard]] std::vector<FaultKind> faults() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return faults_;
    }
    [[nodiscard]] std::vector<BlockReason> blocked() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return blocked_;
    }
    [[nodiscard]] size_t duplicateCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return duplicates_;
    }
    [[nodiscard]] bool silent() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return settles_.empty() && dispositions_.empty() && faults_.empty();
    }

   private:
    mutable std::mutex mutex_;
    std::vector<LogicalOutcome> settles_;
    std::vector<OwnerDisposition> dispositions_;
    std::vector<FaultKind> faults_;
    std::vector<BlockReason> blocked_;
    size_t duplicates_{0};
};

PostReturn acceptedPost(bool retryable = false) {
    return PostReturn{PostAcceptance::kAccepted, 64, retryable};
}

PostReturn notAcceptedPost(bool retryable = true) {
    return PostReturn{PostAcceptance::kNotAccepted, 0, retryable};
}

WrFacts facts(CompletionOutcome outcome) {
    WrFacts result;
    result.outcome = outcome;
    result.completed_bytes = outcome == CompletionOutcome::kSuccess ? 64 : 0;
    result.native_status = outcome == CompletionOutcome::kSuccess ? 0 : -1;
    return result;
}

WrFacts successFacts() { return facts(CompletionOutcome::kSuccess); }

// ---------------------------------------------------------------------------
// G-1: post returns first, then a SUCCESS completion.
TEST(UbCompletionGateTest, PostReturnThenSuccessReturnsOnce) {
    CompletionRecord record;
    RecordingSink sink;
    GateContext context;

    {
        HandlerRef post(record, sink, context);
        onPostReturn(record, acceptedPost(), context);
    }
    // The post stack is gone but hardware responsibility is still open.
    EXPECT_TRUE(sink.settles().empty());
    EXPECT_TRUE(sink.dispositions().empty());
    EXPECT_EQ(context.stats.snapshot().gate_blocked_by_responsibility, 1U);

    {
        HandlerRef cqe(record, sink, context);
        onRealCr(record, successFacts(), sink, context);
    }

    const auto settles = sink.settles();
    ASSERT_EQ(settles.size(), 1U);
    EXPECT_EQ(settles.front(), LogicalOutcome::kCompleted);
    const auto dispositions = sink.dispositions();
    ASSERT_EQ(dispositions.size(), 1U);
    EXPECT_EQ(dispositions.front(), OwnerDisposition::kReturn);
    EXPECT_EQ(context.stats.snapshot().record_settled, 1U);
    EXPECT_EQ(context.stats.snapshot().record_returned, 1U);
    EXPECT_EQ(context.stats.snapshot().one_shot_violations, 0U);
    EXPECT_EQ(record.handler_refs.load(std::memory_order_acquire), 0);
}

// ---------------------------------------------------------------------------
// G-2: the completion wins the race; nothing is released until the native post
// has returned.
TEST(UbCompletionGateTest, EarlyCqeReleasesOnlyAfterPostReturns) {
    CompletionRecord record;
    RecordingSink sink;
    GateContext context;

    {
        HandlerRef post(record, sink, context);
        {
            HandlerRef cqe(record, sink, context);
            onRealCr(record, successFacts(), sink, context);
            EXPECT_TRUE(sink.silent());
            EXPECT_EQ(context.stats.snapshot().early_cqe, 1U);
        }
        // The post stack still holds its reference. Releasing the completion
        // reference goes from 2 to 1, so the gate does not run at all.
        EXPECT_TRUE(sink.silent());
        EXPECT_EQ(record.handler_refs.load(std::memory_order_acquire), 1);
        onPostReturn(record, acceptedPost(), context);
    }

    ASSERT_EQ(sink.settles().size(), 1U);
    EXPECT_EQ(sink.settles().front(), LogicalOutcome::kCompleted);
    ASSERT_EQ(sink.dispositions().size(), 1U);
    EXPECT_EQ(sink.dispositions().front(), OwnerDisposition::kReturn);
    EXPECT_GE(context.stats.snapshot().early_cqe, 1U);
}

// ---------------------------------------------------------------------------
// G-3: two threads reach "references == 0" at the same time. Both may run
// advance(), but every claim bit still resolves to exactly one winner.
TEST(UbCompletionGateTest, ConcurrentAdvanceDisposesExactlyOnce) {
    for (int round = 0; round < 500; ++round) {
        CompletionRecord record;
        RecordingSink sink;
        GateContext context;
        onPostReturn(record, acceptedPost(), context);
        onRealCr(record, successFacts(), sink, context);
        ASSERT_EQ(record.handler_refs.load(std::memory_order_acquire), 0);

        std::barrier sync(2);
        auto run = [&] {
            sync.arrive_and_wait();
            advance(record, sink, context);
        };
        std::thread first(run);
        std::thread second(run);
        first.join();
        second.join();

        ASSERT_EQ(sink.settleCount(), 1U) << "round " << round;
        ASSERT_EQ(sink.dispositionCount(OwnerDisposition::kReturn), 1U)
            << "round " << round;
        ASSERT_EQ(sink.dispositions().size(), 1U) << "round " << round;
        EXPECT_LE(context.stats.snapshot().gate_claim_race, 1U);
    }
}

// ---------------------------------------------------------------------------
// G-4: a duplicate completion is accounted, never applied a second time.
TEST(UbCompletionGateTest, DuplicateCompletionIsCountedNotReapplied) {
    CompletionRecord record;
    RecordingSink sink;
    GateContext context;

    {
        HandlerRef post(record, sink, context);
        onPostReturn(record, acceptedPost(), context);
        {
            HandlerRef cqe(record, sink, context);
            onRealCr(record, successFacts(), sink, context);
            onRealCr(record, successFacts(), sink, context);
        }
    }
    EXPECT_EQ(sink.duplicateCount(), 1U);
    EXPECT_EQ(context.stats.snapshot().duplicate_cr, 1U);
    EXPECT_EQ(sink.dispositionCount(OwnerDisposition::kReturn), 1U);
    EXPECT_EQ(sink.dispositions().size(), 1U);
}

// ---------------------------------------------------------------------------
// G-5: a provably unposted WR is rejected, not returned and not reported as a
// fault, even if a concurrent scan already marked a (false) timeout.
TEST(UbCompletionGateTest, NotAcceptedRollsBackWithoutFault) {
    CompletionRecord record;
    RecordingSink sink;
    GateContext context;

    {
        HandlerRef post(record, sink, context);
        // A scan may have raced this record; NotAccepted beats the fault path.
        EXPECT_TRUE(onTimeout(record));
        onPostReturn(record, notAcceptedPost(), context);
    }

    ASSERT_EQ(sink.settles().size(), 1U);
    EXPECT_EQ(sink.settles().front(), LogicalOutcome::kCanceled);
    ASSERT_EQ(sink.dispositions().size(), 1U);
    EXPECT_EQ(sink.dispositions().front(), OwnerDisposition::kReject);
    EXPECT_TRUE(sink.faults().empty());
    EXPECT_FALSE(record.fault_reported.load(std::memory_order_acquire));
    EXPECT_TRUE(record.logical_retryable);
    const auto stats = context.stats.snapshot();
    EXPECT_EQ(stats.record_rejected, 1U);
    EXPECT_EQ(stats.record_returned, 0U);
    EXPECT_EQ(stats.record_finalized, 0U);
}

// ---------------------------------------------------------------------------
// G-6: a first real error finalizes the owners exactly once and never returns.
TEST(UbCompletionGateTest, CompletionErrorFinalizesWithoutReturn) {
    CompletionRecord record;
    RecordingSink sink;
    GateContext context;

    {
        HandlerRef post(record, sink, context);
        onPostReturn(record, acceptedPost(), context);
    }
    {
        HandlerRef cqe(record, sink, context);
        onRealCr(record, facts(CompletionOutcome::kRemotePathError), sink,
                 context);
    }

    ASSERT_EQ(sink.settles().size(), 1U);
    EXPECT_EQ(sink.settles().front(), LogicalOutcome::kFailed);
    const auto faults = sink.faults();
    ASSERT_EQ(faults.size(), 1U);
    EXPECT_EQ(faults.front(), FaultKind::kCompletionError);
    ASSERT_EQ(sink.dispositions().size(), 1U);
    EXPECT_EQ(sink.dispositions().front(), OwnerDisposition::kFinalize);
    EXPECT_TRUE(record.fault_seen.load(std::memory_order_acquire));
    EXPECT_FALSE(readyForNormalReturn(record));
    EXPECT_TRUE(record.logical_retryable);
    const auto stats = context.stats.snapshot();
    EXPECT_EQ(stats.record_finalized, 1U);
    EXPECT_EQ(stats.record_returned, 0U);
}

// ---------------------------------------------------------------------------
// G-7: the deadline wins over a late SUCCESS completion (baseline `timed_out`
// branch). The logical result stays TIMEOUT and the owners are finalized.
TEST(UbCompletionGateTest, TimeoutWinsOverLateSuccess) {
    CompletionRecord record;
    RecordingSink sink;
    GateContext context;

    {
        HandlerRef post(record, sink, context);
        onPostReturn(record, acceptedPost(), context);
    }
    ASSERT_TRUE(onTimeout(record));
    {
        HandlerRef cqe(record, sink, context);
        onRealCr(record, successFacts(), sink, context);
    }

    ASSERT_EQ(sink.settles().size(), 1U);
    EXPECT_EQ(sink.settles().front(), LogicalOutcome::kTimeout);
    const auto faults = sink.faults();
    ASSERT_EQ(faults.size(), 1U);
    EXPECT_EQ(faults.front(), FaultKind::kTimeout);
    ASSERT_EQ(sink.dispositions().size(), 1U);
    EXPECT_EQ(sink.dispositions().front(), OwnerDisposition::kFinalize);
    const auto timeout_dispositions = sink.dispositions();
    EXPECT_EQ(std::find(timeout_dispositions.begin(),
                        timeout_dispositions.end(), OwnerDisposition::kReturn),
              timeout_dispositions.end());
    EXPECT_EQ(context.stats.snapshot().record_returned, 0U);
    EXPECT_EQ(context.stats.snapshot().record_finalized, 1U);
}

// ---------------------------------------------------------------------------
// G-8: a fenced timeout must not run while a handler reference is live; it
// settles as soon as the last reference is released.
TEST(UbCompletionGateTest, FencedTimeoutWaitsForHandlerReferences) {
    CompletionRecord record;
    RecordingSink sink;
    GateContext context;

    {
        HandlerRef post(record, sink, context);
        onPostReturn(record, acceptedPost(), context);
        ASSERT_TRUE(onTimeout(record));
        onFenceProven(record, sink, context);
        EXPECT_TRUE(sink.silent());
        EXPECT_EQ(context.stats.snapshot().gate_blocked_by_handler_refs, 1U);
    }

    ASSERT_EQ(sink.settles().size(), 1U);
    EXPECT_EQ(sink.settles().front(), LogicalOutcome::kTimeout);
    ASSERT_EQ(sink.dispositions().size(), 1U);
    EXPECT_EQ(sink.dispositions().front(), OwnerDisposition::kFinalize);
    EXPECT_TRUE(record.fence_proven.load(std::memory_order_acquire));
}

// ---------------------------------------------------------------------------
// G-9 (interleaving row 9): a failed fence revokes the mark, releases nothing
// and leaves the record in flight so a later scan retries the fence.
TEST(UbCompletionGateTest, FailedFenceRevokesTimeoutForRetry) {
    CompletionRecord record;
    RecordingSink sink;
    GateContext context;

    {
        HandlerRef post(record, sink, context);
        onPostReturn(record, acceptedPost(), context);
    }
    ASSERT_TRUE(onTimeout(record));
    onFenceFailed(record);
    EXPECT_FALSE(record.timed_out.load(std::memory_order_acquire));
    EXPECT_FALSE(record.fence_proven.load(std::memory_order_acquire));
    EXPECT_TRUE(sink.silent());
    EXPECT_FALSE(record.disposed());

    // The next scan may mark it again and fence again.
    EXPECT_TRUE(onTimeout(record));
    EXPECT_TRUE(record.timed_out.load(std::memory_order_acquire));
}

// ---------------------------------------------------------------------------
// G-10: the shutdown bottom-out supplies the missing verdict and finalizes once
// (interleaving row 10).
TEST(UbCompletionGateTest, ShutdownBottomOutSettlesFailedOnce) {
    CompletionRecord record;
    RecordingSink sink;
    GateContext context;
    onPostReturn(record, acceptedPost(), context);

    onUnposted(record, RecordDropReason::kShutdown, sink, context);
    ASSERT_EQ(sink.settles().size(), 1U);
    EXPECT_EQ(sink.settles().front(), LogicalOutcome::kFailed);
    ASSERT_EQ(sink.dispositions().size(), 1U);
    EXPECT_EQ(sink.dispositions().front(), OwnerDisposition::kFinalize);
    EXPECT_FALSE(record.logical_retryable);
    EXPECT_EQ(record.orphan_reason.load(std::memory_order_relaxed),
              RecordDropReason::kShutdown);

    // Idempotent: a second call must not settle or release again.
    onUnposted(record, RecordDropReason::kShutdown, sink, context);
    EXPECT_EQ(sink.settleCount(), 1U);
    EXPECT_EQ(sink.dispositions().size(), 1U);
}

// A record already disposed by another exit is not touched by the bottom-out.
TEST(UbCompletionGateTest, ShutdownBottomOutSkipsDisposedRecord) {
    CompletionRecord record;
    RecordingSink sink;
    GateContext context;
    {
        HandlerRef post(record, sink, context);
        onPostReturn(record, acceptedPost(), context);
    }
    {
        HandlerRef cqe(record, sink, context);
        onRealCr(record, successFacts(), sink, context);
    }
    ASSERT_EQ(sink.dispositions().size(), 1U);

    onUnposted(record, RecordDropReason::kShutdown, sink, context);
    EXPECT_EQ(sink.dispositions().size(), 1U);
    EXPECT_EQ(sink.settleCount(), 1U);
}

// ---------------------------------------------------------------------------
// G-11: publication order. The reader never observes the one-shot flag without
// the data that was written before it (IN-5). A delay-injection stand-in for a
// TSan run: the writer publishes immediately, the reader spins on the flag.
TEST(UbCompletionGateTest, PostReturnAndWrFactsArePublishedBeforeTheirFlags) {
    for (int round = 0; round < 200; ++round) {
        {
            CompletionRecord record;
            GateContext context;
            std::atomic<bool> checked{false};
            std::thread reader([&] {
                while (!record.post_returned.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                EXPECT_EQ(record.acceptance.load(std::memory_order_relaxed),
                          PostAcceptance::kAccepted);
                EXPECT_EQ(record.posted_bytes, 4096U);
                EXPECT_TRUE(record.post_retryable);
                checked.store(true, std::memory_order_release);
            });
            onPostReturn(record,
                         PostReturn{PostAcceptance::kAccepted, 4096, true},
                         context);
            reader.join();
            ASSERT_TRUE(checked.load(std::memory_order_acquire));
        }
        {
            CompletionRecord record;
            RecordingSink sink;
            GateContext context;
            std::atomic<bool> checked{false};
            std::thread reader([&] {
                while (!record.wr_terminal.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                EXPECT_EQ(record.terminal.outcome,
                          CompletionOutcome::kEndpointError);
                EXPECT_EQ(record.terminal.completed_bytes, 32U);
                EXPECT_EQ(record.terminal.native_status, -7);
                checked.store(true, std::memory_order_release);
            });
            onRealCr(record,
                     WrFacts{CompletionOutcome::kEndpointError, -7, 32, 0},
                     sink, context);
            reader.join();
            ASSERT_TRUE(checked.load(std::memory_order_acquire));
        }
    }
}

// ---------------------------------------------------------------------------
// G-12: handler-reference discipline (IN-6). A live reference blocks the gate;
// nested scopes nest; the last release advances exactly once; the count never
// goes negative.
TEST(UbCompletionGateTest, HandlerReferencesGateAdvanceExactlyOnce) {
    CompletionRecord record;
    RecordingSink sink;
    GateContext context;
    onPostReturn(record, acceptedPost(), context);
    onRealCr(record, successFacts(), sink, context);

    {
        HandlerRef outer(record, sink, context);
        EXPECT_EQ(record.handler_refs.load(std::memory_order_acquire), 1);
        advance(record, sink, context);
        EXPECT_TRUE(sink.dispositions().empty());
        EXPECT_EQ(context.stats.snapshot().gate_blocked_by_handler_refs, 1U);
        {
            HandlerRef inner(record, sink, context);
            EXPECT_EQ(record.handler_refs.load(std::memory_order_acquire), 2);
        }
        EXPECT_EQ(record.handler_refs.load(std::memory_order_acquire), 1);
        EXPECT_TRUE(sink.dispositions().empty());
    }

    EXPECT_EQ(record.handler_refs.load(std::memory_order_acquire), 0);
    EXPECT_EQ(sink.dispositionCount(OwnerDisposition::kReturn), 1U);
    const auto stats = context.stats.snapshot();
    EXPECT_EQ(stats.handler_ref_leak_on_finalize, 0U);
    EXPECT_EQ(stats.record_returned, 1U);

    // A later advance finds the record already disposed and changes nothing.
    advance(record, sink, context);
    EXPECT_EQ(sink.dispositions().size(), 1U);
}

// ---------------------------------------------------------------------------
// G-13: a one-shot bit written twice is a caller defect: it is counted and the
// record keeps its first value.
TEST(UbCompletionGateTest, SecondPostReturnIsAViolationNotAStateChange) {
    CompletionRecord record;
    RecordingSink sink;
    GateContext context;
    onPostReturn(record, notAcceptedPost(false), context);
    onPostReturn(record, acceptedPost(), context);

    EXPECT_EQ(context.stats.snapshot().one_shot_violations, 1U);
    EXPECT_EQ(record.acceptance.load(std::memory_order_relaxed),
              PostAcceptance::kNotAccepted);
    EXPECT_FALSE(record.post_retryable);
}

// ---------------------------------------------------------------------------
// G-14: a revoked timeout must not swallow the fault side effects that arrive
// afterwards, and that fault batch still runs exactly once.
TEST(UbCompletionGateTest, RevokedTimeoutReportsLaterFaultOnce) {
    CompletionRecord record;
    RecordingSink sink;
    GateContext context;
    {
        HandlerRef post(record, sink, context);
        onPostReturn(record, acceptedPost(), context);
    }
    ASSERT_TRUE(onTimeout(record));
    onFenceFailed(record);
    EXPECT_TRUE(sink.faults().empty());
    EXPECT_FALSE(record.fault_reported.load(std::memory_order_acquire));

    {
        HandlerRef cqe(record, sink, context);
        onRealCr(record, facts(CompletionOutcome::kEndpointError), sink,
                 context);
    }
    const auto faults = sink.faults();
    ASSERT_EQ(faults.size(), 1U);
    EXPECT_EQ(faults.front(), FaultKind::kCompletionError);
    ASSERT_EQ(sink.dispositions().size(), 1U);
    EXPECT_EQ(sink.dispositions().front(), OwnerDisposition::kFinalize);

    // Re-running the gate never reports the fault again (one-shot claim).
    advance(record, sink, context);
    EXPECT_EQ(sink.faults().size(), 1U);
    EXPECT_EQ(sink.settleCount(), 1U);
}

// ---------------------------------------------------------------------------
// G-15: tristate acceptance mapping (D-12). W3 moved the single normalization
// point into the adapter (classifyPostAcceptance) and deleted the W2
// legacyAcceptance() helper, so this covers the identical contract.
TEST(UbCompletionGateTest, TristateAcceptanceMapping) {
    const Status ok = Status::OK();
    const Status failed = Status::InternalError("post failed");

    EXPECT_EQ(classifyPostAcceptance(ok, 1, 1), PostAcceptance::kAccepted);
    // The WR crossed the boundary before the error was reported.
    EXPECT_EQ(classifyPostAcceptance(failed, 1, 1), PostAcceptance::kAccepted);
    // Definite zero acceptance is the only rollback case.
    EXPECT_EQ(classifyPostAcceptance(failed, 0, 1),
              PostAcceptance::kNotAccepted);
    // Success with nothing posted is contradictory: the gate must neither
    // accept nor roll back on a guess, it drains (see
    // UnknownAcceptanceDrainsUntilFence).
    EXPECT_EQ(classifyPostAcceptance(ok, 0, 1), PostAcceptance::kUnknown);
    // An over-reporting adapter is self-contradictory too.
    EXPECT_EQ(classifyPostAcceptance(ok, 2, 1), PostAcceptance::kUnknown);
    EXPECT_EQ(classifyPostAcceptance(failed, 2, 1), PostAcceptance::kUnknown);
    // A partially accepted batch is never a definite rejection.
    EXPECT_EQ(classifyPostAcceptance(failed, 1, 2), PostAcceptance::kUnknown);
    // An empty batch has nothing to wait for.
    EXPECT_EQ(classifyPostAcceptance(ok, 0, 0), PostAcceptance::kAccepted);
}

// ---------------------------------------------------------------------------
// A real CQE on its poller and a fence drain on the timeout/stop thread can
// enter onRealCr for the same record concurrently. Exactly one may apply; the
// others are duplicates, and the applied facts must never be a blend of two
// completions. The distinct facts below make a blended record detectable, and
// the multi-thread shape reliably reproduces the window that a two-thread race
// usually loses to thread-start latency.
TEST(UbCompletionGateTest, ConcurrentFirstCompletionIsAppliedOnce) {
    constexpr int kThreads = 4;
    constexpr int kRounds = 500;
    // Only candidate 0 is a success; the others are errors with a unique
    // native status and byte count so the winner can be identified.
    std::array<WrFacts, kThreads> candidates{};
    for (int i = 0; i < kThreads; ++i) {
        candidates[i] = i == 0 ? successFacts()
                               : facts(CompletionOutcome::kRemotePathError);
        candidates[i].native_status = -i;
        candidates[i].completed_bytes = static_cast<uint32_t>(16 * i);
    }

    for (int round = 0; round < kRounds; ++round) {
        CompletionRecord record;
        RecordingSink sink;
        GateContext context;
        onPostReturn(record, acceptedPost(), context);

        std::barrier sync(kThreads);
        std::vector<std::thread> threads;
        threads.reserve(kThreads);
        for (int i = 0; i < kThreads; ++i) {
            threads.emplace_back([&, i] {
                sync.arrive_and_wait();
                HandlerRef ref(record, sink, context);
                onRealCr(record, candidates[i], sink, context);
            });
        }
        for (auto& thread : threads) thread.join();

        const auto stats = context.stats.snapshot();
        ASSERT_EQ(sink.duplicateCount(), static_cast<size_t>(kThreads - 1))
            << "round " << round;
        ASSERT_EQ(stats.duplicate_cr, static_cast<uint64_t>(kThreads - 1))
            << "round " << round;
        // The post fact existed before any completion, so none is early.
        EXPECT_EQ(stats.early_cqe, 0U);

        // The terminal facts belong to exactly one candidate: a blended record
        // (e.g. a success outcome with an error status) matches none.
        int winner = -1;
        for (int i = 0; i < kThreads; ++i) {
            if (record.terminal.outcome == candidates[i].outcome &&
                record.terminal.native_status == candidates[i].native_status &&
                record.terminal.completed_bytes ==
                    candidates[i].completed_bytes) {
                winner = i;
                break;
            }
        }
        ASSERT_GE(winner, 0) << "round " << round;
        EXPECT_EQ(record.fault_seen.load(std::memory_order_acquire),
                  winner != 0);
        ASSERT_EQ(sink.dispositions().size(), 1U) << "round " << round;
        EXPECT_EQ(sink.dispositions().front(),
                  winner == 0 ? OwnerDisposition::kReturn
                              : OwnerDisposition::kFinalize);
    }
}

// ---------------------------------------------------------------------------
// An acceptance the adapter could not prove (posted_count > 1) must never be
// released on a guess: the gate blocks with its own reason and only the fence
// releases.
TEST(UbCompletionGateTest, UnknownAcceptanceDrainsUntilFence) {
    CompletionRecord record;
    RecordingSink sink;
    GateContext context;
    onPostReturn(record, PostReturn{PostAcceptance::kUnknown, 64, false},
                 context);

    {
        HandlerRef cqe(record, sink, context);
        onRealCr(record, successFacts(), sink, context);
    }
    // The WR provably ended, so the verdict is decided...
    ASSERT_EQ(sink.settleCount(), 1U);
    EXPECT_EQ(sink.settles().front(), LogicalOutcome::kCompleted);
    // ...but no exit may release the owners.
    EXPECT_TRUE(sink.dispositions().empty());
    EXPECT_EQ(context.stats.snapshot().gate_blocked_by_acceptance, 1U);
    EXPECT_EQ(context.stats.snapshot().gate_blocked_by_handler_refs, 0U);

    // A further scan pass keeps blocking for the same reason.
    advance(record, sink, context);
    EXPECT_TRUE(sink.dispositions().empty());
    EXPECT_EQ(context.stats.snapshot().gate_blocked_by_acceptance, 2U);

    // Only the fence gets the record out.
    ASSERT_TRUE(onTimeout(record));
    onFenceProven(record, sink, context);
    ASSERT_EQ(sink.dispositions().size(), 1U);
    EXPECT_EQ(sink.dispositions().front(), OwnerDisposition::kFinalize);
    EXPECT_EQ(context.stats.snapshot().record_returned, 0U);
    EXPECT_EQ(context.stats.snapshot().record_finalized, 1U);
    EXPECT_EQ(record.logical_outcome, LogicalOutcome::kCompleted);
}

// ---------------------------------------------------------------------------
// "Definitely not posted" plus a real completion is a contradiction. The first
// verdict wins, the reject disposition still runs exactly once, and the
// conflict is counted exactly once in either arrival order.
TEST(UbCompletionGateTest, NotAcceptedCompletionConflictIsCounted) {
    {
        // Post first, contradictory completion second.
        CompletionRecord record;
        RecordingSink sink;
        GateContext context;
        onPostReturn(record, notAcceptedPost(false), context);
        {
            HandlerRef cqe(record, sink, context);
            onRealCr(record, successFacts(), sink, context);
        }
        EXPECT_EQ(context.stats.snapshot().not_accepted_with_completion, 1U);
        ASSERT_EQ(sink.settles().size(), 1U);
        EXPECT_EQ(sink.settles().front(), LogicalOutcome::kCanceled);
        ASSERT_EQ(sink.dispositions().size(), 1U);
        EXPECT_EQ(sink.dispositions().front(), OwnerDisposition::kReject);
        EXPECT_EQ(sink.duplicateCount(), 0U);
    }
    {
        // Completion first, contradictory post result second. A real completion
        // is responsibility evidence on its own, so its verdict (COMPLETED) is
        // already claimed when the post result arrives: the first verdict wins
        // and the reject disposition still runs exactly once (upstream §1.2.4).
        CompletionRecord record;
        RecordingSink sink;
        GateContext context;
        {
            HandlerRef cqe(record, sink, context);
            onRealCr(record, successFacts(), sink, context);
        }
        ASSERT_EQ(sink.settles().size(), 1U);
        EXPECT_EQ(sink.settles().front(), LogicalOutcome::kCompleted);
        EXPECT_TRUE(sink.dispositions().empty());

        onPostReturn(record, notAcceptedPost(false), context);
        // The contradiction is counted by the reference join point, not by the
        // entry that completes the pair.
        advance(record, sink, context);
        EXPECT_EQ(context.stats.snapshot().not_accepted_with_completion, 1U);
        EXPECT_EQ(sink.settleCount(), 1U);
        EXPECT_EQ(sink.settles().front(), LogicalOutcome::kCompleted);
        ASSERT_EQ(sink.dispositions().size(), 1U);
        EXPECT_EQ(sink.dispositions().front(), OwnerDisposition::kReject);
        EXPECT_EQ(context.stats.snapshot().gate_claim_race, 0U);
    }
}

// ---------------------------------------------------------------------------
// The contradiction counter must be exact, not "usually one": the post fact and
// the completion may be published on top of each other, so a check inside
// either entry point can miss both ways. Counting happens at the reference
// join point instead, which always observes both. 5000 simultaneous rounds
// would show a miss rate of ~0.14% as ~7 failures if the count regressed.
TEST(UbCompletionGateTest, NotAcceptedCompletionConflictIsExact) {
    constexpr int kRounds = 5000;
    const WrFacts success = successFacts();
    for (int round = 0; round < kRounds; ++round) {
        CompletionRecord record;
        RecordingSink sink;
        GateContext context;

        std::barrier sync(2);
        std::thread post_side([&] {
            HandlerRef ref(record, sink, context);
            sync.arrive_and_wait();
            onPostReturn(record, notAcceptedPost(false), context);
        });
        std::thread completion_side([&, success] {
            HandlerRef ref(record, sink, context);
            sync.arrive_and_wait();
            onRealCr(record, success, sink, context);
        });
        post_side.join();
        completion_side.join();

        ASSERT_EQ(context.stats.snapshot().not_accepted_with_completion, 1U)
            << "round " << round;
        // Exactly one verdict and one disposition, whichever fact was first.
        ASSERT_EQ(sink.settleCount(), 1U) << "round " << round;
        ASSERT_EQ(sink.dispositions().size(), 1U) << "round " << round;
        EXPECT_EQ(sink.dispositions().front(), OwnerDisposition::kReject);
    }
}

// ---------------------------------------------------------------------------
// IN-6: the shutdown bottom-out must not release an owner while a handler still
// touches reusable state. It supplies the verdict and refuses the release; the
// counter makes the defect visible.
TEST(UbCompletionGateTest, ShutdownBottomOutRefusesReleaseWithLiveHandlers) {
    CompletionRecord record;
    RecordingSink sink;
    GateContext context;
    onPostReturn(record, acceptedPost(), context);

    {
        HandlerRef held(record, sink, context);
        onUnposted(record, RecordDropReason::kShutdown, sink, context);
        EXPECT_EQ(sink.settleCount(), 1U);
        EXPECT_TRUE(sink.dispositions().empty());
        EXPECT_FALSE(record.disposed());
        EXPECT_EQ(context.stats.snapshot().handler_ref_leak_on_finalize, 1U);
    }

    // The last release runs advance, which still refuses without evidence.
    EXPECT_TRUE(sink.dispositions().empty());
    EXPECT_EQ(context.stats.snapshot().handler_ref_leak_on_finalize, 1U);
}

// ---------------------------------------------------------------------------
// Extra: the exported predicate and decider match the documented matrix
// (IN-2/IN-3, design §6.2/§6.3).
TEST(UbCompletionGateTest, PredicatesFollowTheDocumentedMatrix) {
    CompletionRecord record;
    RecordingSink sink;
    GateContext context;

    EXPECT_FALSE(readyForNormalReturn(record));
    EXPECT_EQ(gate::decideLogicalOutcome(record), LogicalOutcome::kPending);
    EXPECT_FALSE(record.hasResponsibilityEvidence());

    onPostReturn(record, acceptedPost(), context);
    EXPECT_FALSE(record.hasResponsibilityEvidence());
    EXPECT_FALSE(readyForNormalReturn(record));

    onRealCr(record, successFacts(), sink, context);
    EXPECT_TRUE(record.hasResponsibilityEvidence());
    EXPECT_TRUE(readyForNormalReturn(record));
    EXPECT_EQ(gate::decideLogicalOutcome(record), LogicalOutcome::kCompleted);

    record.fault_seen.store(true, std::memory_order_release);
    EXPECT_FALSE(readyForNormalReturn(record));

    // A timeout mark alone never proves responsibility ended.
    CompletionRecord pending;
    onPostReturn(pending, acceptedPost(), context);
    EXPECT_TRUE(onTimeout(pending));
    EXPECT_FALSE(pending.hasResponsibilityEvidence());
    EXPECT_EQ(gate::decideLogicalOutcome(pending), LogicalOutcome::kTimeout);
    pending.fence_proven.store(true, std::memory_order_release);
    EXPECT_TRUE(pending.hasResponsibilityEvidence());
}

}  // namespace
}  // namespace mooncake::tent::ub
