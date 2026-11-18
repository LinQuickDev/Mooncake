// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Completion gate implementation (W2). Design:
// docs/analyze/tent-ub-w2-completion-gate-design.md §5/§6/§9.
//
// The gate separates three questions that the legacy `handleCompletion` mixed
// together:
//   1. did this record reach its logical verdict?        (settle claim)
//   2. is hardware responsibility provably over?         (evidence + owners)
//   3. who, if anyone, already disposed of the owners?   (one claim per exit)

#include "tent/transport/ub/completion_record.h"

#include <cstdio>

namespace mooncake::tent::ub {
namespace {

void bump(std::atomic<uint64_t>& counter) {
    counter.fetch_add(1, std::memory_order_relaxed);
}

void bumpBlocked(CompletionStats& stats, BlockReason reason) {
    switch (reason) {
        case BlockReason::kPostReturn:
            bump(stats.gate_blocked_by_post_return);
            return;
        case BlockReason::kCqe:
            bump(stats.gate_blocked_by_cqe);
            return;
        case BlockReason::kResponsibility:
            bump(stats.gate_blocked_by_responsibility);
            return;
        case BlockReason::kHandlerRefs:
            bump(stats.gate_blocked_by_handler_refs);
            return;
        case BlockReason::kFault:
            bump(stats.gate_blocked_by_fault);
            return;
        case BlockReason::kNotAccepted:
            bump(stats.gate_blocked_by_not_accepted);
            return;
        case BlockReason::kAcceptance:
            bump(stats.gate_blocked_by_acceptance);
            return;
    }
}

// IN-3: only proof that the WR can no longer touch memory allows an owner
// release. `fault_seen` alone never qualifies.
bool responsibilityEnded(const CompletionRecord& record) {
    return record.hasResponsibilityEvidence();
}

// Which fault batch this record is entitled to. A timeout takes precedence: a
// record whose deadline passed is billed as a timeout even if its late
// completion was an error (baseline `handleCompletion` checks timed_out first).
FaultKind faultKindOf(const CompletionRecord& record) {
    if (record.timed_out.load(std::memory_order_acquire)) {
        return FaultKind::kTimeout;
    }
    if (record.wr_terminal.load(std::memory_order_acquire) &&
        !record.terminal.success()) {
        return FaultKind::kCompletionError;
    }
    // Fence-proven timeout without a completion.
    return FaultKind::kTimeout;
}

// Why the gate refused the normal return (design §9 gate_blocked_by_*).
BlockReason blockReasonOf(const CompletionRecord& record) {
    if (!record.post_returned.load(std::memory_order_acquire)) {
        return BlockReason::kPostReturn;
    }
    const PostAcceptance acceptance =
        record.acceptance.load(std::memory_order_relaxed);
    if (acceptance == PostAcceptance::kNotAccepted) {
        return BlockReason::kNotAccepted;
    }
    if (!record.wr_terminal.load(std::memory_order_acquire)) {
        return BlockReason::kCqe;
    }
    if (record.fault_seen.load(std::memory_order_acquire) ||
        record.timed_out.load(std::memory_order_acquire)) {
        return BlockReason::kFault;
    }
    if (acceptance != PostAcceptance::kAccepted) {
        // The WR is provably over but the adapter could not prove acceptance:
        // only a fence may release, so this is not a handler-reference block.
        return BlockReason::kAcceptance;
    }
    // Unreachable: with refs == 0 and every conjunct above satisfied,
    // readyForNormalReturn() is true and advance() never asks for a reason.
    // Kept so the function stays total (and so a future predicate change cannot
    // silently mislabel the block).
    return BlockReason::kHandlerRefs;
}

// Retryability of the delivered logical result. Mirrors the baseline
// `retryableStatus()` classification per completion category.
bool logicalRetryable(const CompletionRecord& record, LogicalOutcome outcome) {
    switch (outcome) {
        case LogicalOutcome::kCompleted:
            return false;
        case LogicalOutcome::kTimeout:
            // Baseline resolves every timeout as retryable.
            return true;
        case LogicalOutcome::kCanceled:
            // A post that provably did not cross the native boundary may be
            // retried as the legacy rollback allowed.
            return record.post_retryable;
        case LogicalOutcome::kFailed:
            switch (record.terminal.outcome) {
                case CompletionOutcome::kLocalDeviceError:
                case CompletionOutcome::kRemotePathError:
                case CompletionOutcome::kEndpointError:
                    return true;
                case CompletionOutcome::kMemoryError:
                case CompletionOutcome::kUnknownError:
                case CompletionOutcome::kSuccess:
                case CompletionOutcome::kTimeout:
                    return false;
            }
            return false;
        case LogicalOutcome::kPending:
            return false;
    }
    return false;
}

void decideAndPublish(CompletionRecord& record) {
    const LogicalOutcome outcome = gate::decideLogicalOutcome(record);
    record.logical_outcome = outcome;
    record.logical_retryable = logicalRetryable(record, outcome);
}

}  // namespace

const char* recordKindName(RecordKind kind) {
    switch (kind) {
        case RecordKind::kBusiness:
            return "business";
        case RecordKind::kProbe:
            return "probe";
    }
    return "unknown";
}

const char* postAcceptanceName(PostAcceptance acceptance) {
    switch (acceptance) {
        case PostAcceptance::kNotAccepted:
            return "not_accepted";
        case PostAcceptance::kAccepted:
            return "accepted";
        case PostAcceptance::kUnknown:
            return "unknown";
    }
    return "unknown";
}

const char* completionOutcomeName(CompletionOutcome outcome) {
    switch (outcome) {
        case CompletionOutcome::kSuccess:
            return "SUCCESS";
        case CompletionOutcome::kTimeout:
            return "TIMEOUT";
        case CompletionOutcome::kLocalDeviceError:
            return "LOCAL_DEVICE_ERROR";
        case CompletionOutcome::kRemotePathError:
            return "REMOTE_PATH_ERROR";
        case CompletionOutcome::kEndpointError:
            return "ENDPOINT_ERROR";
        case CompletionOutcome::kMemoryError:
            return "MEMORY_ERROR";
        case CompletionOutcome::kUnknownError:
            return "UNKNOWN_ERROR";
    }
    return "UNKNOWN_ERROR";
}

const char* logicalOutcomeName(LogicalOutcome outcome) {
    switch (outcome) {
        case LogicalOutcome::kPending:
            return "PENDING";
        case LogicalOutcome::kCompleted:
            return "COMPLETED";
        case LogicalOutcome::kFailed:
            return "FAILED";
        case LogicalOutcome::kTimeout:
            return "TIMEOUT";
        case LogicalOutcome::kCanceled:
            return "CANCELED";
    }
    return "PENDING";
}

const char* blockReasonName(BlockReason reason) {
    switch (reason) {
        case BlockReason::kPostReturn:
            return "post_return";
        case BlockReason::kCqe:
            return "cqe";
        case BlockReason::kResponsibility:
            return "responsibility";
        case BlockReason::kHandlerRefs:
            return "handler_refs";
        case BlockReason::kFault:
            return "fault";
        case BlockReason::kNotAccepted:
            return "not_accepted";
        case BlockReason::kAcceptance:
            return "acceptance";
    }
    return "unknown";
}

const char* faultKindName(FaultKind kind) {
    switch (kind) {
        case FaultKind::kNone:
            return "none";
        case FaultKind::kTimeout:
            return "timeout";
        case FaultKind::kCompletionError:
            return "completion_error";
    }
    return "none";
}

const char* ownerDispositionName(OwnerDisposition disposition) {
    switch (disposition) {
        case OwnerDisposition::kReturn:
            return "return";
        case OwnerDisposition::kFinalize:
            return "finalize";
        case OwnerDisposition::kReject:
            return "reject";
    }
    return "unknown";
}

const char* recordDropReasonName(RecordDropReason reason) {
    switch (reason) {
        case RecordDropReason::kNone:
            return "none";
        case RecordDropReason::kTimeout:
            return "timeout";
        case RecordDropReason::kNotAccepted:
            return "not_accepted";
        case RecordDropReason::kCompletionError:
            return "completion_error";
        case RecordDropReason::kShutdown:
            return "shutdown";
    }
    return "none";
}

std::string recordPathText(const RecordPath& path) {
    char buffer[96];
    std::snprintf(buffer, sizeof(buffer), "local=%d segment=%llu remote=%d",
                  path.local_nic,
                  static_cast<unsigned long long>(path.segment_id),
                  path.remote_nic);
    return std::string(buffer);
}

CompletionStatsSnapshot CompletionStats::snapshot() const {
    CompletionStatsSnapshot out;
    out.record_published = record_published.load(std::memory_order_relaxed);
    out.record_settled = record_settled.load(std::memory_order_relaxed);
    out.record_returned = record_returned.load(std::memory_order_relaxed);
    out.record_finalized = record_finalized.load(std::memory_order_relaxed);
    out.record_rejected = record_rejected.load(std::memory_order_relaxed);
    out.early_cqe = early_cqe.load(std::memory_order_relaxed);
    out.duplicate_cr = duplicate_cr.load(std::memory_order_relaxed);
    out.unknown_token = unknown_token.load(std::memory_order_relaxed);
    out.gate_blocked_by_post_return =
        gate_blocked_by_post_return.load(std::memory_order_relaxed);
    out.gate_blocked_by_cqe =
        gate_blocked_by_cqe.load(std::memory_order_relaxed);
    out.gate_blocked_by_responsibility =
        gate_blocked_by_responsibility.load(std::memory_order_relaxed);
    out.gate_blocked_by_handler_refs =
        gate_blocked_by_handler_refs.load(std::memory_order_relaxed);
    out.gate_blocked_by_fault =
        gate_blocked_by_fault.load(std::memory_order_relaxed);
    out.gate_blocked_by_not_accepted =
        gate_blocked_by_not_accepted.load(std::memory_order_relaxed);
    out.gate_blocked_by_acceptance =
        gate_blocked_by_acceptance.load(std::memory_order_relaxed);
    out.gate_claim_race = gate_claim_race.load(std::memory_order_relaxed);
    out.not_accepted_with_completion =
        not_accepted_with_completion.load(std::memory_order_relaxed);
    out.completion_length_mismatch =
        completion_length_mismatch.load(std::memory_order_relaxed);
    out.one_shot_violations =
        one_shot_violations.load(std::memory_order_relaxed);
    out.handler_ref_leak_on_finalize =
        handler_ref_leak_on_finalize.load(std::memory_order_relaxed);
    out.timeout_deferred_by_post_return =
        timeout_deferred_by_post_return.load(std::memory_order_relaxed);
    return out;
}

void CompletionStats::reset() {
    for (std::atomic<uint64_t>* counter : {&record_published,
                                           &record_settled,
                                           &record_returned,
                                           &record_finalized,
                                           &record_rejected,
                                           &early_cqe,
                                           &duplicate_cr,
                                           &unknown_token,
                                           &gate_blocked_by_post_return,
                                           &gate_blocked_by_cqe,
                                           &gate_blocked_by_responsibility,
                                           &gate_blocked_by_handler_refs,
                                           &gate_blocked_by_fault,
                                           &gate_blocked_by_not_accepted,
                                           &gate_blocked_by_acceptance,
                                           &gate_claim_race,
                                           &not_accepted_with_completion,
                                           &one_shot_violations,
                                           &handler_ref_leak_on_finalize,
                                           &timeout_deferred_by_post_return}) {
        counter->store(0, std::memory_order_relaxed);
    }
}

namespace gate {

HandlerRef::HandlerRef(CompletionRecord& record, Sink& sink,
                       GateContext& context)
    : record_(record), sink_(sink), context_(context) {
    record_.handler_refs.fetch_add(1, std::memory_order_acq_rel);
}

HandlerRef::~HandlerRef() {
    if (record_.handler_refs.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        advance(record_, sink_, context_);
    }
}

void onPostReturn(CompletionRecord& record, const PostReturn& result,
                  GateContext& context) {
    // This entry is called exactly once per record by its post call stack.
    // A second call is a defect in the caller, not a race to handle: count it
    // and leave the record untouched.
    if (record.post_returned.load(std::memory_order_acquire)) {
        bump(context.stats.one_shot_violations);
        return;
    }
    // (1) data first, (2) release-store the one-shot bit. Reversing the two
    // would let a reader observe "post returned" while `acceptance` is still
    // undefined (IN-5).
    record.acceptance.store(result.acceptance, std::memory_order_relaxed);
    record.posted_bytes = result.posted_bytes;
    record.post_retryable = result.retryable;
    record.post_returned.store(true, std::memory_order_release);
}

void onRealCr(CompletionRecord& record, const WrFacts& facts, Sink& sink,
              GateContext& context) {
    // One JFC has exactly one poller, but a fence drain runs on the timeout or
    // stop thread while that poller may still consume the same Jetty. Two
    // threads can therefore reach this entry for one record, so the duplicate
    // guard must be a claim, not a load of `wr_terminal`: the loser must not
    // write `terminal` (that would be a data race and could mix in the late
    // category).
    if (record.cr_claimed.exchange(true, std::memory_order_acq_rel)) {
        bump(context.stats.duplicate_cr);
        sink.onDuplicate(record);
        return;
    }
    if (!record.post_returned.load(std::memory_order_acquire)) {
        // Benign and expected: the CQE legally beats the blocking post call.
        // Only the winning completion counts it.
        bump(context.stats.early_cqe);
    }
    // (1) result fields, (2) release-store wr_terminal (IN-5). fault_seen is
    // part of the result, so it is published before the flag as well: any
    // acquirer of wr_terminal then also sees that this was an error.
    record.terminal = facts;
    if (!facts.success()) {
        record.fault_seen.store(true, std::memory_order_release);
        record.orphan_reason.store(RecordDropReason::kCompletionError,
                                   std::memory_order_relaxed);
    }
    record.wr_terminal.store(true, std::memory_order_release);
}

bool onTimeout(CompletionRecord& record) {
    // Provisional: only a successful fence turns this into a proven timeout.
    return !record.timed_out.exchange(true, std::memory_order_acq_rel);
}

void onFenceProven(CompletionRecord& record, Sink& sink, GateContext& context) {
    record.fence_proven.store(true, std::memory_order_release);
    record.orphan_reason.store(RecordDropReason::kTimeout,
                               std::memory_order_relaxed);
    advance(record, sink, context);
}

void onFenceFailed(CompletionRecord& record) {
    // No proof that the old DMA stopped: revoke the provisional mark so the
    // record goes back to "in flight" and a later scan retries the fence. A
    // natural completion may still settle it in the meantime (IN-11).
    record.timed_out.store(false, std::memory_order_release);
}

void onUnposted(CompletionRecord& record, RecordDropReason reason, Sink& sink,
                GateContext& context) {
    record.orphan_reason.store(reason, std::memory_order_relaxed);
    if (!record.settle_claimed.exchange(true, std::memory_order_acq_rel)) {
        record.logical_outcome = LogicalOutcome::kFailed;
        record.logical_retryable = false;
        bump(context.stats.record_settled);
        sink.onSettle(record, LogicalOutcome::kFailed);
    }
    // Any exit that already claimed an owner disposition owns the release; the
    // bottom-out only supplies the missing verdict (§6.9).
    if (record.disposed()) return;
    if (record.handler_refs.load(std::memory_order_acquire) != 0) {
        // IN-6: a handler still touches reusable state, so releasing the owners
        // here would be exactly the "release while the post has not returned"
        // bug W2 exists to remove. The only caller runs after the posting and
        // polling threads were joined, so this is unreachable in the current
        // call graph: refuse the release, keep the record for a later exit, and
        // let the counter surface the defect instead of trading a leak for a
        // use-after-free.
        bump(context.stats.handler_ref_leak_on_finalize);
        return;
    }
    if (record.finalize_claimed.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    bump(context.stats.record_finalized);
    sink.onOwners(record, OwnerDisposition::kFinalize);
}

bool readyForNormalReturn(const CompletionRecord& record) {
    return record.post_returned.load(std::memory_order_acquire) &&
           record.acceptance.load(std::memory_order_relaxed) ==
               PostAcceptance::kAccepted &&
           record.wr_terminal.load(std::memory_order_acquire) &&
           record.terminal.success() &&
           !record.fault_seen.load(std::memory_order_acquire) &&
           !record.timed_out.load(std::memory_order_acquire) &&
           !record.finalize_claimed.load(std::memory_order_acquire) &&
           record.handler_refs.load(std::memory_order_acquire) == 0;
}

LogicalOutcome decideLogicalOutcome(const CompletionRecord& record) {
    // Ordering mirrors the baseline: a record that was already marked timed out
    // resolves as TIMEOUT even if its late completion was an error.
    if (record.notAccepted()) {
        // The WR never crossed the native boundary. Baseline calls this result
        // FAILED, upstream §8 calls the logical state FAILED/NotExecuted; the
        // owner side keeps delivering FAILED (see
        // UbWorkers::deliverSettlement).
        return LogicalOutcome::kCanceled;
    }
    if (record.timed_out.load(std::memory_order_acquire)) {
        return LogicalOutcome::kTimeout;
    }
    if (record.wr_terminal.load(std::memory_order_acquire)) {
        switch (record.terminal.outcome) {
            case CompletionOutcome::kSuccess:
                return LogicalOutcome::kCompleted;
            case CompletionOutcome::kTimeout:
                return LogicalOutcome::kTimeout;
            default:
                return LogicalOutcome::kFailed;
        }
    }
    if (record.fence_proven.load(std::memory_order_acquire)) {
        // Fenced without a completion: nobody can answer, so the attempt is a
        // timeout (baseline probe-lost branch).
        return LogicalOutcome::kTimeout;
    }
    return LogicalOutcome::kPending;
}

void advance(CompletionRecord& record, Sink& sink, GateContext& context) {
    // A handler is still touching reusable state: the post call stack has not
    // returned yet or a settle scope is live. Whoever releases the last
    // reference runs this function again.
    if (record.handler_refs.load(std::memory_order_acquire) != 0) {
        bump(context.stats.gate_blocked_by_handler_refs);
        return;
    }
    // A record that is both provably unposted and completed is a contradiction.
    // Neither entry point can count it reliably: each publishes its own fact
    // and they may land on top of each other, so both may look before the other
    // wrote. Both facts, however, are published *before* their handler
    // reference is released, and the reference count is an acquire/release
    // chain, so the last releaser - the thread that runs this function - sees
    // both. That makes the count exact, once per record.
    if (record.notAccepted() &&
        record.wr_terminal.load(std::memory_order_acquire) &&
        !record.conflict_claimed.exchange(true, std::memory_order_acq_rel)) {
        bump(context.stats.not_accepted_with_completion);
    }

    if (!responsibilityEnded(record)) {
        // No proof yet that the WR cannot touch memory, so there is nothing to
        // settle or release. `fault_seen` alone never qualifies (IN-3).
        bump(context.stats.gate_blocked_by_responsibility);
        sink.onBlocked(record, BlockReason::kResponsibility);
        return;
    }

    // ---- stage one: the logical verdict, decided exactly once --------------
    if (!record.settle_claimed.exchange(true, std::memory_order_acq_rel)) {
        decideAndPublish(record);
        bump(context.stats.record_settled);
        sink.onSettle(record, record.logical_outcome);
    }

    // ---- stage two: three mutually exclusive owner exits -------------------
    if (record.notAccepted()) {
        // Nothing was posted, so nothing can be in flight. A concurrent timeout
        // mark on this record is a false positive and must not be reported as
        // a fault, and a (contradictory) real completion must not leave the
        // accounting wedged: the reject path still runs exactly once, keeping
        // whichever logical verdict was claimed first.
        if (record.reject_claimed.exchange(true, std::memory_order_acq_rel)) {
            bump(context.stats.gate_claim_race);
            return;
        }
        bump(context.stats.record_rejected);
        sink.onOwners(record, OwnerDisposition::kReject);
        return;
    }

    if (record.fault_seen.load(std::memory_order_acquire) ||
        record.timed_out.load(std::memory_order_acquire)) {
        if (!record.fault_reported.exchange(true, std::memory_order_acq_rel)) {
            sink.onFault(record, faultKindOf(record));
        }
        if (record.finalize_claimed.exchange(true, std::memory_order_acq_rel)) {
            bump(context.stats.gate_claim_race);
            return;
        }
        if (record.handler_refs.load(std::memory_order_acquire) != 0) {
            // Defensive: advance() already refused to run with live handlers.
            bump(context.stats.handler_ref_leak_on_finalize);
        }
        bump(context.stats.record_finalized);
        sink.onOwners(record, OwnerDisposition::kFinalize);
        return;
    }

    if (!readyForNormalReturn(record)) {
        const BlockReason reason = blockReasonOf(record);
        bumpBlocked(context.stats, reason);
        sink.onBlocked(record, reason);
        return;
    }
    if (record.lease_return_claimed.exchange(true, std::memory_order_acq_rel)) {
        bump(context.stats.gate_claim_race);
        return;
    }
    bump(context.stats.record_returned);
    sink.onOwners(record, OwnerDisposition::kReturn);
}

}  // namespace gate

}  // namespace mooncake::tent::ub
