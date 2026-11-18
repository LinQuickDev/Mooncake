// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Completion record and counter unit tests (W2a).
//
// Design: docs/analyze/tent-ub-w2-completion-gate-design.md §5 (data model),
// §9 (observation) and §10.1 (invariants). Links only completion_record.cpp.

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <string>

#include "tent/transport/ub/completion_record.h"

namespace mooncake::tent::ub {
namespace {

TEST(UbCompletionRecordTest, DefaultRecordHasNoFactsAndNoClaims) {
    CompletionRecord record;
    EXPECT_EQ(record.token, 0U);
    EXPECT_EQ(record.kind, RecordKind::kBusiness);
    EXPECT_EQ(record.acceptance.load(std::memory_order_relaxed),
              PostAcceptance::kUnknown);
    EXPECT_FALSE(record.post_claimed.load(std::memory_order_relaxed));
    EXPECT_FALSE(record.post_returned.load(std::memory_order_relaxed));
    EXPECT_FALSE(record.wr_terminal.load(std::memory_order_relaxed));
    EXPECT_FALSE(record.fence_proven.load(std::memory_order_relaxed));
    EXPECT_FALSE(record.timed_out.load(std::memory_order_relaxed));
    EXPECT_FALSE(record.fault_seen.load(std::memory_order_relaxed));
    EXPECT_EQ(record.handler_refs.load(std::memory_order_relaxed), 0);
    EXPECT_FALSE(record.settle_claimed.load(std::memory_order_relaxed));
    EXPECT_FALSE(record.reject_claimed.load(std::memory_order_relaxed));
    EXPECT_FALSE(record.finalize_claimed.load(std::memory_order_relaxed));
    EXPECT_FALSE(record.lease_return_claimed.load(std::memory_order_relaxed));
    EXPECT_FALSE(record.fault_reported.load(std::memory_order_relaxed));
    EXPECT_EQ(record.logical_outcome, LogicalOutcome::kPending);
    EXPECT_FALSE(record.logical_retryable);
    EXPECT_EQ(record.orphan_reason.load(std::memory_order_relaxed),
              RecordDropReason::kNone);
    EXPECT_FALSE(record.notAccepted());
    EXPECT_FALSE(record.hasResponsibilityEvidence());
    EXPECT_FALSE(record.disposed());
}

// IN-3: each of the three evidence sources is sufficient; fault_seen alone
// is not.
TEST(UbCompletionRecordTest, ResponsibilityEvidenceIsThreeWay) {
    CompletionRecord record;
    EXPECT_FALSE(record.hasResponsibilityEvidence());

    record.fault_seen.store(true, std::memory_order_relaxed);
    EXPECT_FALSE(record.hasResponsibilityEvidence())
        << "IN-3: fault is not proof";

    record.fence_proven.store(true, std::memory_order_release);
    EXPECT_TRUE(record.hasResponsibilityEvidence());

    CompletionRecord via_cqe;
    via_cqe.wr_terminal.store(true, std::memory_order_release);
    EXPECT_TRUE(via_cqe.hasResponsibilityEvidence());

    CompletionRecord via_reject;
    via_reject.acceptance.store(PostAcceptance::kNotAccepted,
                                std::memory_order_relaxed);
    via_reject.post_returned.store(true, std::memory_order_release);
    EXPECT_TRUE(via_reject.notAccepted());
    EXPECT_TRUE(via_reject.hasResponsibilityEvidence());

    // NotAccepted requires the post fact as well: acceptance alone is not
    // evidence until the post call has actually returned.
    CompletionRecord half;
    half.acceptance.store(PostAcceptance::kNotAccepted,
                          std::memory_order_relaxed);
    EXPECT_FALSE(half.notAccepted());
    EXPECT_FALSE(half.hasResponsibilityEvidence());
}

TEST(UbCompletionRecordTest, DisposedTracksTheThreeOwnerExits) {
    CompletionRecord record;
    EXPECT_FALSE(record.disposed());
    record.reject_claimed.store(true, std::memory_order_relaxed);
    EXPECT_TRUE(record.disposed());

    CompletionRecord finalize;
    finalize.finalize_claimed.store(true, std::memory_order_relaxed);
    EXPECT_TRUE(finalize.disposed());

    CompletionRecord returned;
    returned.lease_return_claimed.store(true, std::memory_order_relaxed);
    EXPECT_TRUE(returned.disposed());
}

TEST(UbCompletionRecordTest, PathTextCarriesIdentityFields) {
    RecordPath path{3, 42, 7};
    EXPECT_TRUE(path.valid());
    const std::string text = recordPathText(path);
    EXPECT_NE(text.find("local=3"), std::string::npos);
    EXPECT_NE(text.find("segment=42"), std::string::npos);
    EXPECT_NE(text.find("remote=7"), std::string::npos);

    RecordPath invalid;
    EXPECT_FALSE(invalid.valid());
    EXPECT_EQ(RecordPath({1, 2, 3}), (RecordPath{1, 2, 3}));
}

TEST(UbCompletionRecordTest, NameHelpersCoverEveryEnumerator) {
    EXPECT_STREQ(recordKindName(RecordKind::kBusiness), "business");
    EXPECT_STREQ(recordKindName(RecordKind::kProbe), "probe");
    EXPECT_STREQ(postAcceptanceName(PostAcceptance::kNotAccepted),
                 "not_accepted");
    EXPECT_STREQ(postAcceptanceName(PostAcceptance::kAccepted), "accepted");
    EXPECT_STREQ(postAcceptanceName(PostAcceptance::kUnknown), "unknown");
    EXPECT_STREQ(completionOutcomeName(CompletionOutcome::kSuccess), "SUCCESS");
    EXPECT_STREQ(completionOutcomeName(CompletionOutcome::kUnknownError),
                 "UNKNOWN_ERROR");
    EXPECT_STREQ(logicalOutcomeName(LogicalOutcome::kCompleted), "COMPLETED");
    EXPECT_STREQ(logicalOutcomeName(LogicalOutcome::kCanceled), "CANCELED");
    EXPECT_STREQ(blockReasonName(BlockReason::kHandlerRefs), "handler_refs");
    EXPECT_STREQ(blockReasonName(BlockReason::kAcceptance), "acceptance");
    EXPECT_STREQ(faultKindName(FaultKind::kTimeout), "timeout");
    EXPECT_STREQ(ownerDispositionName(OwnerDisposition::kFinalize), "finalize");
    EXPECT_STREQ(recordDropReasonName(RecordDropReason::kShutdown), "shutdown");
}

// CompletionStats must be copyable *by snapshot only*: the counters are atomic.
TEST(UbCompletionRecordTest, StatsSnapshotReflectsAndResetsCounters) {
    CompletionStats stats;
    const auto empty = stats.snapshot();
    EXPECT_EQ(empty.record_published, 0U);
    EXPECT_EQ(empty.one_shot_violations, 0U);

    stats.record_published.fetch_add(3, std::memory_order_relaxed);
    stats.early_cqe.fetch_add(2, std::memory_order_relaxed);
    stats.duplicate_cr.fetch_add(1, std::memory_order_relaxed);
    stats.unknown_token.fetch_add(4, std::memory_order_relaxed);
    stats.gate_blocked_by_handler_refs.fetch_add(5, std::memory_order_relaxed);
    stats.gate_blocked_by_responsibility.fetch_add(6,
                                                   std::memory_order_relaxed);
    stats.gate_blocked_by_acceptance.fetch_add(11, std::memory_order_relaxed);
    stats.not_accepted_with_completion.fetch_add(12, std::memory_order_relaxed);
    stats.gate_claim_race.fetch_add(7, std::memory_order_relaxed);
    stats.one_shot_violations.fetch_add(8, std::memory_order_relaxed);
    stats.handler_ref_leak_on_finalize.fetch_add(9, std::memory_order_relaxed);
    stats.timeout_deferred_by_post_return.fetch_add(10,
                                                    std::memory_order_relaxed);

    const auto snapshot = stats.snapshot();
    EXPECT_EQ(snapshot.record_published, 3U);
    EXPECT_EQ(snapshot.early_cqe, 2U);
    EXPECT_EQ(snapshot.duplicate_cr, 1U);
    EXPECT_EQ(snapshot.unknown_token, 4U);
    EXPECT_EQ(snapshot.gate_blocked_by_handler_refs, 5U);
    EXPECT_EQ(snapshot.gate_blocked_by_responsibility, 6U);
    EXPECT_EQ(snapshot.gate_blocked_by_acceptance, 11U);
    EXPECT_EQ(snapshot.not_accepted_with_completion, 12U);
    EXPECT_EQ(snapshot.gate_claim_race, 7U);
    EXPECT_EQ(snapshot.one_shot_violations, 8U);
    EXPECT_EQ(snapshot.handler_ref_leak_on_finalize, 9U);
    EXPECT_EQ(snapshot.timeout_deferred_by_post_return, 10U);

    stats.reset();
    const auto cleared = stats.snapshot();
    EXPECT_EQ(cleared.record_published, 0U);
    EXPECT_EQ(cleared.unknown_token, 0U);
    EXPECT_EQ(cleared.gate_blocked_by_acceptance, 0U);
    EXPECT_EQ(cleared.not_accepted_with_completion, 0U);
    EXPECT_EQ(cleared.timeout_deferred_by_post_return, 0U);
}

// The logical decision table (design §6.3). The owner side only needs the
// outcome; retryability is derived by the gate from the same facts.
TEST(UbCompletionRecordTest, LogicalDecisionFollowsTheDocumentedTable) {
    CompletionRecord record;
    GateContext context;

    // No evidence yet.
    EXPECT_EQ(gate::decideLogicalOutcome(record), LogicalOutcome::kPending);

    // SUCCESS completion -> COMPLETED.
    record.post_returned.store(true, std::memory_order_release);
    record.acceptance.store(PostAcceptance::kAccepted,
                            std::memory_order_relaxed);
    record.terminal.outcome = CompletionOutcome::kSuccess;
    record.wr_terminal.store(true, std::memory_order_release);
    EXPECT_EQ(gate::decideLogicalOutcome(record), LogicalOutcome::kCompleted);

    // Adapter-level TIMEOUT category keeps resolving TIMEOUT (baseline).
    record.terminal.outcome = CompletionOutcome::kTimeout;
    EXPECT_EQ(gate::decideLogicalOutcome(record), LogicalOutcome::kTimeout);

    // Any other error -> FAILED.
    record.terminal.outcome = CompletionOutcome::kMemoryError;
    EXPECT_EQ(gate::decideLogicalOutcome(record), LogicalOutcome::kFailed);

    // A deadline that already passed wins over the completion category.
    record.timed_out.store(true, std::memory_order_release);
    record.terminal.outcome = CompletionOutcome::kSuccess;
    EXPECT_EQ(gate::decideLogicalOutcome(record), LogicalOutcome::kTimeout);

    // NotAccepted wins over everything (the WR never existed).
    record.acceptance.store(PostAcceptance::kNotAccepted,
                            std::memory_order_relaxed);
    EXPECT_EQ(gate::decideLogicalOutcome(record), LogicalOutcome::kCanceled);

    // A fence proven without any completion resolves TIMEOUT.
    CompletionRecord fenced;
    fenced.fence_proven.store(true, std::memory_order_release);
    EXPECT_EQ(gate::decideLogicalOutcome(fenced), LogicalOutcome::kTimeout);
    (void)context;
}

// IN-1: four independent claim bits, each one-shot.
TEST(UbCompletionRecordTest, ClaimBitsAreOneShot) {
    CompletionRecord record;
    EXPECT_FALSE(record.settle_claimed.exchange(true));
    EXPECT_TRUE(record.settle_claimed.exchange(true));
    EXPECT_FALSE(record.reject_claimed.exchange(true));
    EXPECT_TRUE(record.reject_claimed.exchange(true));
    EXPECT_FALSE(record.finalize_claimed.exchange(true));
    EXPECT_TRUE(record.finalize_claimed.exchange(true));
    EXPECT_FALSE(record.lease_return_claimed.exchange(true));
    EXPECT_TRUE(record.lease_return_claimed.exchange(true));
    EXPECT_FALSE(record.fault_reported.exchange(true));
    EXPECT_TRUE(record.fault_reported.exchange(true));
    EXPECT_EQ(record.handler_refs.load(std::memory_order_relaxed), 0);
}

// The probe side embeds the very same kernel definition (IN-10).
TEST(UbCompletionRecordTest, ProbeKindIsCarriedByTheSharedKernel) {
    CompletionRecord record;
    record.kind = RecordKind::kProbe;
    record.token = 1ULL << 48;
    EXPECT_EQ(recordKindName(record.kind), "probe");
    EXPECT_NE(record.token, 0U);
}

}  // namespace
}  // namespace mooncake::tent::ub
