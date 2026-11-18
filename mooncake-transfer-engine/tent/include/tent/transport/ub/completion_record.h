// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Shared completion record and completion gate (W2).
//
// Design: docs/analyze/tent-ub-w2-completion-gate-design.md (§5, §6, §9).
// Upstream: docs/analyze/tent-ub-post-poll-threadpool-design.md §7.3/§7.4.
//
// `CompletionRecord` is the "shared kernel": identity scalars plus one-shot
// fact/claim bits. It deliberately contains no business or probe private types,
// so it can be unit tested without workers, slices or the probe link manager
// (`gate::*` and `CompletionRecord` must never include slice.h, workers.h or
// probe_link_manager.h). Each side hangs its own owner fields off it.
//
// Two groups of atomics live here and the difference matters:
//   * status bits answer "did this happen?" (written by one side, read by
//   many);
//   * claim bits answer "who is responsible for doing this, and has it been
//     done?" (CAS/atomic exchange; the loser must return immediately).
//
// Invariants (design §10.1) enforced here:
//   IN-1 every one-shot action happens at most once. Five of them are the
//        record's own outcomes - terminal write / settle / reject / finalize /
//        return - guarded by cr_claimed, settle_claimed, reject_claimed,
//        finalize_claimed and lease_return_claimed; the remaining two claims
//        (fault_reported, conflict_claimed) gate the side-effect batches so a
//        fault or a contradicting record is reported exactly once;
//   IN-2 fault/timeout and normal return are mutually exclusive;
//   IN-3 no owner may be released without responsibility evidence
//        (wr_terminal || fence_proven || a definite NotAccepted post);
//   IN-5 a reader that observes wr_terminal/post_returned also observes the
//        result fields that were published before the release store;
//   IN-7 the logical result is *decided* once at settle claim time and
//        *delivered* later, after the owners were released and handler_refs
//        reached zero;
//   IN-10 probe records reuse this very definition.

#ifndef TENT_TRANSPORT_UB_COMPLETION_RECORD_H_
#define TENT_TRANSPORT_UB_COMPLETION_RECORD_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>

#include "tent/common/status.h"
#include "tent/common/types.h"

namespace mooncake::tent::ub {

// ---------------------------------------------------------------------------
// Identity.
//
// The high 16 bits of a completion token name the namespace (probe design
// §5.3): business tokens are kind 0, probe tokens kind 1, and token 0 stays
// reserved for a native marker.
enum class RecordKind : uint8_t {
    kBusiness = 0,
    kProbe = 1,
};

// Three-state post acceptance (interface 5). W3 sources it from the adapter's
// real tristate result through classifyPostAcceptance() (urma_adapter.h, D-12);
// the W2 heuristic that inferred it from posted_count is gone. kUnknown is
// produced by a self-contradictory adapter; such a record is never released on
// a guess - it drains until a fence proves that responsibility ended
// (upstream §1.2.4 "Unknown 交排空").
enum class PostAcceptance : uint8_t {
    kNotAccepted = 0,
    kAccepted = 1,
    kUnknown = 2,
};

// Result category carried by a real completion. Mirrors the adapter's
// CompletionCategory so the shared kernel does not depend on urma_adapter.h;
// the owner side maps one onto the other.
enum class CompletionOutcome : uint8_t {
    kSuccess = 0,
    kTimeout = 1,
    kLocalDeviceError = 2,
    kRemotePathError = 3,
    kEndpointError = 4,
    kMemoryError = 5,
    kUnknownError = 6,
};

// Logical result of the attempt (three-layer semantics, total design §8).
// kPending only means "not decided yet" and is never delivered.
enum class LogicalOutcome : uint8_t {
    kPending = 0,
    kCompleted = 1,
    kFailed = 2,
    kTimeout = 3,
    kCanceled = 4,
};

// Why the owner of a record was finalized outside the normal path. W2 uses it
// in the shutdown bottom-out; W6 will carry it to the handoff report.
enum class RecordDropReason : uint8_t {
    kNone = 0,
    kTimeout = 1,
    kNotAccepted = 2,
    kCompletionError = 3,
    kShutdown = 4,
};

// Why the gate refused to release the owner (design §9, gate_blocked_by_*).
enum class BlockReason : uint8_t {
    kPostReturn = 0,
    kCqe = 1,
    kResponsibility = 2,
    kHandlerRefs = 3,
    kFault = 4,
    kNotAccepted = 5,
    // The completion proves the WR ended but the adapter could not prove that
    // the post was accepted: no exit may release the owners here, the timeout
    // fence has to do it.
    kAcceptance = 6,
};

// One-shot fault side effect kind (interface 2 / rail health, design §6.3).
enum class FaultKind : uint8_t {
    kNone = 0,
    // Deadline driven: a timeout proved by a fence, or a completion that
    // arrived after the record was already marked timed out.
    kTimeout = 1,
    // A first real completion error (category from `terminal.outcome`).
    kCompletionError = 2,
};

// The three mutually exclusive owner dispositions (design §6.4).
enum class OwnerDisposition : uint8_t {
    kReturn = 0,
    kFinalize = 1,
    kReject = 2,
};

[[nodiscard]] const char* recordKindName(RecordKind kind);
[[nodiscard]] const char* postAcceptanceName(PostAcceptance acceptance);
[[nodiscard]] const char* completionOutcomeName(CompletionOutcome outcome);
[[nodiscard]] const char* logicalOutcomeName(LogicalOutcome outcome);
[[nodiscard]] const char* blockReasonName(BlockReason reason);
[[nodiscard]] const char* faultKindName(FaultKind kind);
[[nodiscard]] const char* ownerDispositionName(OwnerDisposition disposition);
[[nodiscard]] const char* recordDropReasonName(RecordDropReason reason);

// Path identity in the shape both sides share (design §5.4). The fields mirror
// Topology::NicID/SegmentID but stay plain scalars: the kernel knows scalars
// only, and each owner keeps its own conversion function.
struct RecordPath {
    int local_nic{-1};
    SegmentID segment_id{LOCAL_SEGMENT_ID};
    int remote_nic{-1};

    bool operator==(const RecordPath&) const = default;

    [[nodiscard]] bool valid() const {
        return local_nic >= 0 && remote_nic >= 0;
    }
};

// Human-readable path for the shared "record" log line (design §9).
[[nodiscard]] std::string recordPathText(const RecordPath& path);

// Terminal facts of the first real completion. Written once by the wr_terminal
// claimant *before* the release store of `wr_terminal`, so an acquire reader
// always sees initialized fields (IN-5).
struct WrFacts {
    CompletionOutcome outcome{CompletionOutcome::kSuccess};
    int32_t native_status{0};
    uint32_t completed_bytes{0};
    uint64_t completion_ns{0};

    [[nodiscard]] bool success() const {
        return outcome == CompletionOutcome::kSuccess;
    }
};

// Outcome of one native post call, as seen by the posting side. `retryable` is
// the caller's classification of the post status; the gate only forwards it to
// the reject disposition because the legacy rollback resolves the attempt with
// it.
struct PostReturn {
    PostAcceptance acceptance{PostAcceptance::kUnknown};
    size_t posted_bytes{0};
    bool retryable{false};
};

// ---------------------------------------------------------------------------
// The record.
//
// A record is heap allocated and shared (`std::shared_ptr` in the owner's
// registry); `handler_refs` never participates in its lifetime.
struct CompletionRecord {
    // ---- identity: immutable once published -------------------------------
    uint64_t token{0};
    RecordKind kind{RecordKind::kBusiness};
    uint64_t attempt_id{0};
    uint32_t attempt_serial{0};
    RecordPath path{};
    uint64_t jetty_generation{0};
    // Generational anchors. Business leaves them zero in W2; the probe side
    // fills them from its PathVersion (§5.4).
    uint64_t topology_epoch{0};
    uint64_t target_epoch{0};
    uint64_t fault_epoch{0};
    // Opaque owner back-reference. The gate never dereferences it; the sink
    // casts it back to its own record type. It is valid for the duration of a
    // sink callback because the caller that drove the callback holds a strong
    // reference to that owner object.
    void* owner{nullptr};

    // ---- status bits: one writer, many readers -----------------------------
    // This side already claimed the single post right for the attempt. It only
    // prevents a local double post; the authority for "may we post at all"
    // stays with the pooling side claim (W3).
    std::atomic<bool> post_claimed{false};
    std::atomic<bool> post_returned{false};
    std::atomic<PostAcceptance> acceptance{PostAcceptance::kUnknown};
    // First real completion arrived: hardware responsibility ended.
    std::atomic<bool> wr_terminal{false};
    // Synchronous quiesce proved no more DMA can touch this record's buffers.
    std::atomic<bool> fence_proven{false};
    // Logical timeout mark. Provisional until a fence proves the old DMA
    // stopped; a failed fence clears it again (§6.7).
    std::atomic<bool> timed_out{false};
    // A real fault was recorded. This is a logical flag only: alone it is
    // never release evidence (IN-3).
    std::atomic<bool> fault_seen{false};
    // Executors currently touching reusable state (post stack, settle scope).
    // A gate fact, not a lifetime mechanism (D-4).
    std::atomic<int32_t> handler_refs{0};

    // Terminal facts, published before wr_terminal (§5.2).
    WrFacts terminal{};
    // Post-side facts, published before post_returned (§5.2).
    size_t posted_bytes{0};
    bool post_retryable{false};
    std::atomic<RecordDropReason> orphan_reason{RecordDropReason::kNone};

    // ---- claim bits: one-shot actions -------------------------------------
    // Claims the *write right* of `terminal` and `wr_terminal`. A fence drain
    // on the timeout/stop thread and a real CQE on its poller can enter
    // onRealCr for the same record concurrently, so the duplicate check must be
    // a claim and not a load of `wr_terminal`.
    std::atomic<bool> cr_claimed{false};
    std::atomic<bool> settle_claimed{false};
    std::atomic<bool> reject_claimed{false};
    std::atomic<bool> finalize_claimed{false};
    std::atomic<bool> lease_return_claimed{false};
    // At most one thread reports the "provably unposted yet completed"
    // contradiction. Both facts are published before their handler reference is
    // released, so the last releaser is the single join point that sees both.
    std::atomic<bool> conflict_claimed{false};
    std::atomic<bool> fault_reported{false};

    // ---- decided logical result -------------------------------------------
    // Written once by the settle claimant, read by the owner disposition that
    // delivers it. Plain fields are enough: the decision and its delivery run
    // in the same advance() call. They are also readable for diagnostics.
    LogicalOutcome logical_outcome{LogicalOutcome::kPending};
    bool logical_retryable{false};

    // Convenience predicates used by both the gate and the owner side.
    [[nodiscard]] bool notAccepted() const {
        return post_returned.load(std::memory_order_acquire) &&
               acceptance.load(std::memory_order_relaxed) ==
                   PostAcceptance::kNotAccepted;
    }
    [[nodiscard]] bool hasResponsibilityEvidence() const {
        return wr_terminal.load(std::memory_order_acquire) ||
               fence_proven.load(std::memory_order_acquire) || notAccepted();
    }
    [[nodiscard]] bool disposed() const {
        return reject_claimed.load(std::memory_order_acquire) ||
               finalize_claimed.load(std::memory_order_acquire) ||
               lease_return_claimed.load(std::memory_order_acquire);
    }
};

// W3 D-12: `legacyAcceptance()` was deleted. acceptance always arrives from the
// adapter's tristate post through classifyPostAcceptance() (urma_adapter.h), so
// no caller may reconstruct it from posted_count.

// ---------------------------------------------------------------------------
// Counters (design §9). Same shape as ProbeStats: atomics written by the gate
// and copied out by snapshot(). The owner side bumps record_published; the gate
// owns everything else.
struct CompletionStatsSnapshot {
    uint64_t record_published{0};
    uint64_t record_settled{0};
    uint64_t record_returned{0};
    uint64_t record_finalized{0};
    uint64_t record_rejected{0};
    uint64_t early_cqe{0};
    uint64_t duplicate_cr{0};
    uint64_t unknown_token{0};
    uint64_t gate_blocked_by_post_return{0};
    uint64_t gate_blocked_by_cqe{0};
    uint64_t gate_blocked_by_responsibility{0};
    uint64_t gate_blocked_by_handler_refs{0};
    uint64_t gate_blocked_by_fault{0};
    uint64_t gate_blocked_by_not_accepted{0};
    uint64_t gate_blocked_by_acceptance{0};
    uint64_t gate_claim_race{0};
    // A completion arrived for a record whose post reported definite zero
    // acceptance: a contradiction that is isolated (the reject disposition
    // retires the endpoint) and must stay visible.
    uint64_t not_accepted_with_completion{0};
    // W3 D-15: the completion reported a byte count that differs from the
    // submitted length. Recorded only - the W2 settlement predicate and the
    // pool return predicate stay untouched, so this must stay at zero on a
    // healthy build and never changes a verdict.
    uint64_t completion_length_mismatch{0};
    // Invariant counters; always zero on a healthy build.
    uint64_t one_shot_violations{0};
    uint64_t handler_ref_leak_on_finalize{0};
    uint64_t timeout_deferred_by_post_return{0};
};

class CompletionStats {
   public:
    [[nodiscard]] CompletionStatsSnapshot snapshot() const;
    void reset();

    std::atomic<uint64_t> record_published{0};
    std::atomic<uint64_t> record_settled{0};
    std::atomic<uint64_t> record_returned{0};
    std::atomic<uint64_t> record_finalized{0};
    std::atomic<uint64_t> record_rejected{0};
    std::atomic<uint64_t> early_cqe{0};
    std::atomic<uint64_t> duplicate_cr{0};
    std::atomic<uint64_t> unknown_token{0};
    std::atomic<uint64_t> gate_blocked_by_post_return{0};
    std::atomic<uint64_t> gate_blocked_by_cqe{0};
    std::atomic<uint64_t> gate_blocked_by_responsibility{0};
    std::atomic<uint64_t> gate_blocked_by_handler_refs{0};
    std::atomic<uint64_t> gate_blocked_by_fault{0};
    std::atomic<uint64_t> gate_blocked_by_not_accepted{0};
    std::atomic<uint64_t> gate_blocked_by_acceptance{0};
    std::atomic<uint64_t> gate_claim_race{0};
    std::atomic<uint64_t> not_accepted_with_completion{0};
    std::atomic<uint64_t> completion_length_mismatch{0};
    std::atomic<uint64_t> one_shot_violations{0};
    std::atomic<uint64_t> handler_ref_leak_on_finalize{0};
    std::atomic<uint64_t> timeout_deferred_by_post_return{0};
};

// Everything the gate needs from its caller. It owns the counters so the gate
// never reaches for a global and a unit test can drive it single-threaded
// (the design's GateContext, minus the clock: no decision in W2 needs one).
struct GateContext {
    CompletionStats stats{};
};

namespace gate {

// Owner-side side effects. `UbWorkers` implements this; the gate itself only
// decides *when* each callback may run. Callbacks run outside any record lock,
// at most once per claim bit.
class Sink {
   public:
    virtual ~Sink() = default;

    // The logical result was decided and published into the record (never
    // repeated for one record). Observe it; do not deliver it here: delivery
    // waits for onOwners(), so a retry can never be queued before the old
    // owners were released (D-8).
    virtual void onSettle(CompletionRecord& record, LogicalOutcome outcome) = 0;
    // The owner disposition won by this record. Exactly one of kReturn,
    // kFinalize, kReject is ever delivered; it releases the owners, removes the
    // record from the registry and *then* delivers the logical result.
    virtual void onOwners(CompletionRecord& record,
                          OwnerDisposition disposition) = 0;
    // One-shot fault side effects (interface 2 / rail health).
    virtual void onFault(CompletionRecord& record, FaultKind kind) = 0;
    // The gate refused to advance. Purely observational and may fire often.
    virtual void onBlocked(CompletionRecord& record, BlockReason reason) = 0;
    // A second completion for a record that is still registered.
    virtual void onDuplicate(CompletionRecord& record) = 0;
};

// Holds one handler reference and releases it on scope exit. Only the thread
// whose release reaches zero runs advance(), so "the last one out closes the
// door" is explicit and no early return can leak the count (design §6.5).
class HandlerRef {
   public:
    HandlerRef(CompletionRecord& record, Sink& sink, GateContext& context);
    ~HandlerRef();

    HandlerRef(const HandlerRef&) = delete;
    HandlerRef& operator=(const HandlerRef&) = delete;

   private:
    CompletionRecord& record_;
    Sink& sink_;
    GateContext& context_;
};

// Dual entry points. Each publishes its fact fields *before* the release store
// of its one-shot bit (§5.2), and neither one touches handler_refs: the
// reference is owned by the enclosing HandlerRef, which is also what runs
// advance() when the last reference goes away.
void onPostReturn(CompletionRecord& record, const PostReturn& result,
                  GateContext& context);
// Claims the terminal write right with `cr_claimed` before publishing the
// facts, so concurrent completions (a real CQE on its poller plus a fence drain
// on the timeout/stop thread) are applied at most once.
void onRealCr(CompletionRecord& record, const WrFacts& facts, Sink& sink,
              GateContext& context);

// Timeout seam (design §6.7). Returns false when another exit already claimed
// the timeout mark. Marking a timeout deliberately performs no fault side
// effect yet: until a fence proves the old DMA stopped the mark is provisional,
// and a revoked timeout must not swallow the side effects of the real fault
// that may still arrive (IN-11).
[[nodiscard]] bool onTimeout(CompletionRecord& record);
void onFenceProven(CompletionRecord& record, Sink& sink, GateContext& context);
void onFenceFailed(CompletionRecord& record);
// Shutdown bottom-out: settle FAILED if nobody did and finalize the owners if
// nobody did. Idempotent and safe against every other exit (§6.9). The caller
// must have joined the posting and polling executors: with a live handler
// reference this entry refuses to release (IN-6) and counts the defect.
void onUnposted(CompletionRecord& record, RecordDropReason reason, Sink& sink,
                GateContext& context);

// Runs the gate for one record. Public because HandlerRef calls it when the
// last reference goes away; everything else goes through the entries above.
void advance(CompletionRecord& record, Sink& sink, GateContext& context);

// Normal-return predicate (design §6.2). Necessary condition only: the final
// verdict belongs to the pooling side release() in W3.
[[nodiscard]] bool readyForNormalReturn(const CompletionRecord& record);

// The logical result the gate would decide for this record, or kPending when
// there is not enough evidence yet (exposed for tests).
[[nodiscard]] LogicalOutcome decideLogicalOutcome(
    const CompletionRecord& record);

}  // namespace gate

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_COMPLETION_RECORD_H_
