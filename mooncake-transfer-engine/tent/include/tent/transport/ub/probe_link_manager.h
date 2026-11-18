// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// ProbeLinkManager (PLM): the probe-link health state machine, campaign
// bookkeeping, recovery gate and completion settlement. It owns no thread and
// no native resource: the ProbeScheduler drives it, the poller feeds probe
// completions into it, and the pooling side supplies resources through
// interface 4/7.
//
// Design: docs/analyze/tent-ub-probe-link-detail-design.md §5–§10.
//
// Invariants enforced here (design §14.1):
//   1. at most one in-flight probe per local port (including "unknown
//      responsibility" after a timeout);
//   2. at most one task (kQueued/kInflight) per path;
//   3. at most one active campaign per peer;
//   4. token/record are published before the native post, so a completion may
//      legally arrive before ProbePoster::post() returns;
//   5. health changes and fault_epoch increments happen in one critical
//      section and accept current-generation evidence only;
//   6. sinks are called outside the mutex, and record-held segment references
//      are released outside it (handle destructors call into the adapter);
//   9. settled/timed_out are one-shot CAS bits;
//  11. the port slot is generation-scoped: releasing it checks both token and
//      generation, and old-generation orphans never disturb a new slot.

#ifndef TENT_TRANSPORT_UB_PROBE_LINK_MANAGER_H_
#define TENT_TRANSPORT_UB_PROBE_LINK_MANAGER_H_

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "tent/common/status.h"
#include "tent/transport/ub/completion_record.h"
#include "tent/transport/ub/probe_resources.h"
#include "tent/transport/ub/probe_types.h"
#include "tent/transport/ub/slice.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake {
namespace tent {
namespace ub {

// Business evidence classification (design §11.3). Only kPathFault isolates a
// path and creates/merges a campaign; the other kinds never mark the path bad.
enum class BusinessErrorKind : uint8_t {
    kPathFault,
    kContextFailure,
    kLaneError,
    kParamError,
    kUnknown,
};

// Result of ProbePoster::post() as seen by the PLM (design §8.6).
enum class ProbePostOutcome : uint8_t {
    kAccepted,
    kRejected,
    kUnknown,
};

// Probe owner of the shared completion record (design D-9 / W2 §5.5).
//
// `core` is the very same CompletionRecord the business data path uses; the
// probe side only fills its identity and one-shot facts and keeps its own
// fields alongside. `core.finalize_claimed` is the W1 `settled` bit: exactly
// one thread may settle (clean up) this record. `core.timed_out` marks a
// timeout without settling it: resources and the port slot stay pinned (L-4).
// The migration is deliberately mechanical - no probe decision, lock boundary,
// side-effect order or log line changed with it (invariant 10).
struct ProbeRecord {
    CompletionRecord core;
    ProbeKey key;
    // Budget release must still work after the PathVersion row has been
    // dropped (stale settlement, orphans), so the peer is carried alongside
    // the key.
    ProbePeerKey peer;
    uint64_t posted_ns{0};
    uint64_t deadline_ns{0};
    RecordState state{RecordState::kPosting};
    OrphanReason orphan_reason{OrphanReason::kNone};
    // Holding these references keeps the probe source page and the remote
    // probe area alive until the record terminates or is handed off. They are
    // owned by the PLM and must not be read by the scheduler/poster.
    LocalSegmentPtr source;
    RemoteSegmentPtr area;
    uint32_t credit{0};
    // Periodic test-mode probe (probe/test/periodic_all): settlement can skip
    // health application and never applies exponential backoff.
    bool test_probe{false};
};

// One schedulable probe computed by pendingActions() (design §8.1). The handles
// are copies so the scheduler can build a ProbePostTarget without holding the
// PLM lock.
struct ProbeAction {
    PathVersion version;
    ProbePeerKey peer;
    Topology::NicID local_nic{-1};
    uint64_t jetty_generation{0};
    uint64_t area_epoch{0};
    uint64_t due_ns{0};
    bool test{false};
    JettyPtr probe_jetty;
    RemoteJettyRef remote_jetty;
    LocalSegmentPtr source_segment;
    RemoteSegmentPtr area_segment;
    uint64_t area_address{0};
    uint64_t area_length{0};
    uint64_t source_address{0};
    uint64_t source_length{0};
};

struct PathHealthView {
    PathVersion version;
    ProbePeerKey peer;
    LinkHealth health{LinkHealth::kEligibleUnverified};
    uint64_t fault_epoch{0};
    uint64_t consecutive_failures{0};
    uint64_t next_probe_ns{0};
    uint64_t probe_seq{0};
    uint64_t inflight_seq{0};
    TaskState task{TaskState::kIdle};
    bool pending{false};
    PendingReason pending_reason{PendingReason::kNone};
    uint64_t pending_since_ns{0};
};

struct CampaignEntryView {
    PathVersion version;
    EntryState state{EntryState::kQueued};
    uint64_t entry_epoch{0};
};

struct CampaignView {
    uint64_t id{0};
    uint64_t epoch{0};
    ProbePeerKey peer;
    std::vector<CampaignEntryView> entries;
};

// Shutdown accounting (design §9.6). `settled` counts records that terminated
// while draining, `unproven` counts records handed off with unresolved hardware
// responsibility. `drained` is true when nothing had to be handed off.
struct ProbeDrainResult {
    size_t settled{0};
    size_t unproven{0};
    bool drained{true};
};

class ProbeLinkManager final : public ProbeAvailabilitySink {
   public:
    using Clock = std::function<uint64_t()>;
    using WakeCallback = std::function<void()>;

    explicit ProbeLinkManager(ProbeParams params, Clock clock = {});
    ~ProbeLinkManager() override = default;

    ProbeLinkManager(const ProbeLinkManager&) = delete;
    ProbeLinkManager& operator=(const ProbeLinkManager&) = delete;

    // Reporting (interface 2 and the W1 handoff seam) may be provided by the
    // same object that supplies resources, or a narrower sink. The hook mutex
    // makes the setter safe against concurrent emitEffects().
    void setReportSink(ProbeReportSink* sink);
    // Invoked outside the PLM mutex whenever scheduling state changed.
    void setWakeCallback(WakeCallback callback);

    // Interface 7 entry point handed to the provider at wiring time.
    [[nodiscard]] ProbeAvailabilitySink* availabilitySink() { return this; }

    // Interface 4. Builds the health table from a full snapshot; every edge
    // starts ELIGIBLE_UNVERIFIED and no periodic task exists (design §5.4).
    Status initialize(const ProbeGraph& graph);
    [[nodiscard]] bool initialized() const;

    // ---- scheduler seam (design §8.1) -------------------------------------
    // Computes at most one action per local port. Read-only with respect to
    // health; the authoritative claim happens in beginPost().
    [[nodiscard]] std::vector<ProbeAction> pendingActions(uint64_t now_ns);

    // Claims the action: publishes the token and record, aligns the campaign
    // entry epoch, occupies the port slot and the peer budget. Returns nullptr
    // when the action became unschedulable in the meantime.
    [[nodiscard]] std::shared_ptr<ProbeRecord> beginPost(
        const ProbeAction& action, uint64_t token, uint64_t now_ns);

    // design §8.6. kRejected rolls the task slot back and never marks the path
    // bad; kAccepted/kUnknown wait for the completion or timeout.
    void onPostResult(const std::shared_ptr<ProbeRecord>& record,
                      ProbePostOutcome outcome, uint64_t now_ns);

    // Earliest time the scheduler has something to do, or 0 if it can sleep
    // until the next external event (design §8.7).
    [[nodiscard]] uint64_t nextDeadline(uint64_t now_ns) const;

    // ---- timeout scan and shutdown (design §9.4/§9.6) --------------------
    // Marks every record past its deadline exactly once: retires the probe
    // lane, marks the port Pending and moves the record to the orphan table.
    // Resources and the port slot stay pinned (L-4); a late completion only
    // settles the orphan. Called by the scheduler thread before its wait.
    void scanTimeouts(uint64_t now_ns);

    // Stops scheduling new probes and cancels queued campaign entries;
    // in-flight records keep settling (design §13.4). Idempotent.
    void stopPosting();

    // Scheduler lifecycle hook: the scheduler registers "request stop and join
    // me" so drainBeforePollStop can join the scheduler before pollers stop.
    void setSchedulerStopHook(std::function<void()> hook);

    // Prepares shutdown: stops posting, joins the scheduler through the hook,
    // waits (bounded) for in-flight completions, then moves every unresolved
    // record plus the existing orphan table into one handoff snapshot. Must be
    // called from a thread other than the scheduler thread. The sink is called
    // outside the PLM mutex and must acquire the references before returning
    // (acquire-before-release). A zero deadline means now + timeout_ms.
    ProbeDrainResult drainBeforePollStop(uint64_t deadline_ns = 0);

    // High-water mark for the poster's blocking time (design §13.2).
    void notePostBlockUs(uint64_t micros);

    // ---- poller seam (design §9.1/§9.2) -----------------------------------
    // Returns false when the token does not belong to the probe namespace.
    // Called on the poller thread; performs no blocking work and calls no sink
    // while holding the lock.
    [[nodiscard]] bool onProbeCompletion(const Completion& completion,
                                         uint64_t now_ns = 0);

    // ---- business evidence (design §11.3) --------------------------------
    void onBusinessPathSuccess(const UbPostPath& path, uint64_t now_ns = 0);
    void onBusinessPathError(const UbPostPath& path, BusinessErrorKind kind,
                             uint64_t now_ns = 0);
    // A local context/JFC failed: every path on that port becomes DISABLED and
    // is not recovered by probing (design §6.2).
    void onContextUnavailable(Topology::NicID local_nic, uint64_t now_ns = 0);

    // ---- W3 business-routing gate (design §7.1 / D-10) --------------------
    // Read-only queries used by path selection; they never mutate health and
    // never create a campaign.
    //
    // `businessPathUsable` performs subtraction only: HEALTHY and
    // ELIGIBLE_UNVERIFIED pass, ISOLATED/RECOVERING/DISABLED do not, and a path
    // that is not in the probe graph PASSES. Excluding unknown paths would let
    // an incomplete adjacency configuration starve business traffic, which W1
    // deliberately avoided by staying a side channel (W3-IN-6 / RK8).
    //
    // `businessFaultEpoch` is the claim anchor of the admission path: it is
    // snapshotted before the lease and compared after it, so an attempt that
    // was selected before its path became isolated does not post (design §8.2).
    [[nodiscard]] bool businessPathUsable(const UbPostPath& path);
    [[nodiscard]] uint64_t businessFaultEpoch(const UbPostPath& path);

    // ---- interface 7 (design §12.3) --------------------------------------
    // Synchronous, bounded update. A repeated (graph_epoch, event_seq) is
    // acknowledged without being applied. A gap or a malformed payload is
    // rejected WITHOUT advancing the idempotency key, so the same key can be
    // resent and accepted later.
    Status onPoolAvailabilityChange(const ProbeEvent& event) override;

    // ---- observation -----------------------------------------------------
    [[nodiscard]] std::vector<PathHealthView> healthView() const;
    [[nodiscard]] std::vector<CampaignView> campaignView() const;
    [[nodiscard]] ProbeStats stats() const;
    [[nodiscard]] size_t inflightCount() const;
    [[nodiscard]] size_t orphanCount() const;
    [[nodiscard]] uint64_t graphEpoch() const;
    [[nodiscard]] const ProbeParams& params() const { return params_; }

   private:
    struct PathRuntime {
        ProbePeerKey peer;
        LinkHealth health{LinkHealth::kEligibleUnverified};
        uint64_t fault_epoch{0};
        uint64_t consecutive_failures{0};
        uint64_t success_count{0};
        // Per-path monotonic sequence: log correlation plus the task-slot
        // ownership assertion used by releaseTaskSlot().
        uint64_t probe_seq{0};
        uint64_t inflight_seq{0};
        uint64_t backoff_ns{0};
        uint64_t next_probe_ns{0};
        // Test-mode cadence (probe/test/periodic_all); independent of health
        // and backoff.
        uint64_t test_due_ns{0};
        TaskState task{TaskState::kIdle};
        bool pending{false};
        PendingReason pending_reason{PendingReason::kNone};
        uint64_t pending_since_ns{0};
    };

    struct PortRuntime {
        ProbePortSupply supply;
        uint64_t generation{0};
        // The slot only describes the current generation. A lane retirement
        // leaves it occupied (no resend); an interface-7 replacement resets it
        // and moves old in-flight records to the orphan table.
        uint64_t slot_token{0};
        uint64_t slot_generation{0};
        bool handle_valid{true};
        uint64_t broken_since_ns{0};
        size_t rr_cursor{0};
        uint32_t inflight{0};
        // Deterministic round-robin order for this port.
        std::vector<PathVersion> paths;
    };

    struct CampaignEntry {
        EntryState state{EntryState::kQueued};
        uint64_t entry_epoch{1};
    };

    struct Campaign {
        uint64_t id{0};
        uint64_t epoch{1};
        ProbePeerKey peer;
        std::map<PathVersion, CampaignEntry> entries;
    };

    // Side effects collected in the critical section and emitted after it.
    struct Effects {
        bool retire_lane{false};
        LaneKey lane;
        uint64_t generation{0};
        uint64_t error_epoch{0};
        PathVersion path;
        Status reason;
        bool bump{false};
    };

    enum class StaleReason : uint8_t {
        kNone,
        kEpoch,
        kGeneration,
        kResource,
    };

    using PortMap = std::map<Topology::NicID, PortRuntime>;
    using PathMap = std::map<PathVersion, PathRuntime>;
    using EdgeMap = std::map<PathVersion, ProbeEdgeSupply>;
    using CampaignMap = std::map<ProbePeerKey, Campaign>;
    using RecordMap = std::map<uint64_t, std::shared_ptr<ProbeRecord>>;

    uint64_t normalizedNow(uint64_t now_ns) const;
    PortRuntime* findPortLocked(Topology::NicID local_nic);
    PathRuntime* findPathLocked(const PathVersion& version);
    ProbeEdgeSupply* findEdgeLocked(const PathVersion& version);

    void emitEffects(const Effects& effects);

    // State helpers. setHealthLocked() owns the path_state counters and the
    // availability bump.
    void setHealthLocked(PathRuntime& runtime, LinkHealth health,
                         Effects& effects);
    void publishHealthyLocked(PathRuntime& runtime, Effects& effects);
    static bool externalGateOk(const ProbeEdgeSupply& edge);
    uint64_t backoffDelayLocked(uint64_t consecutive_failures) const;

    // Task/health/campaign eligibility ignoring the next_probe deadline.
    bool timeEligibleLocked(const PathVersion& version,
                            const PathRuntime& runtime) const;
    bool dueLocked(const PathVersion& version, const PathRuntime& runtime,
                   uint64_t now_ns) const;
    bool schedulableLocked(const PathVersion& version,
                           const PathRuntime& runtime, uint64_t now_ns) const;
    bool testEligibleLocked(const PathRuntime& runtime, uint64_t now_ns) const;
    bool testSchedulableLocked(const PathRuntime& runtime,
                               uint64_t now_ns) const;
    bool peerBudgetAvailableLocked(const ProbePeerKey& peer) const;
    ProbeAction makeActionLocked(const PortRuntime& port,
                                 const PathVersion& version,
                                 const PathRuntime& runtime) const;

    // Settlement pieces (design §9.2).
    void releaseSlotLocked(PortRuntime& port, const ProbeRecord& record);
    void releaseBudgetLocked(const ProbePeerKey& peer);
    void releaseTaskSlotLocked(ProbeRecord& record);
    void campaignProgressLocked(const ProbeKey& key, EntryState outcome);
    void exitFinishedCampaignsLocked();
    void applyLocked(const Completion& completion, const PathVersion& version,
                     PathRuntime& runtime, const ProbeEdgeSupply& edge,
                     PortRuntime& port, uint64_t now_ns, bool test_probe,
                     Effects& effects);
    StaleReason staleReasonLocked(const ProbeRecord& record);
    void moveToOrphansLocked(const std::shared_ptr<ProbeRecord>& record,
                             OrphanReason reason, uint64_t now_ns);

    // Campaign helpers (design §7.2).
    void mergeOrCreateCampaignLocked(const ProbePeerKey& peer, uint64_t now_ns);
    void cancelCampaignEntriesLocked(const PathVersion& version);

    // Interface-7 application (design §12.3).
    void removePathLocked(const PathVersion& version);
    void markPortPendingLocked(PortRuntime& port, PendingReason reason,
                               uint64_t now_ns);
    void clearPortPendingLocked(PortRuntime& port);
    void laneRetireLocked(PortRuntime& port, uint64_t now_ns,
                          const PathVersion& path, const Status& reason,
                          Effects& effects);
    bool validateEventPayloadLocked(const ProbeEvent& event) const;
    Status applyEventLocked(const ProbeEvent& event, uint64_t now_ns,
                            Effects& effects);
    Status upsertEdgeLocked(const ProbeEdgeSupply& edge);

    // Maps business evidence onto a health row (design L-7). When the
    // provider published an endpoint_generation anchor and the evidence
    // carries a different one, the evidence is unknown: faults fall back to
    // the rail's current version (and are counted), successes are discarded
    // because they must not promote a path they do not belong to.
    std::optional<PathVersion> resolveVersionLocked(
        const UbPostPath& evidence, bool require_matching_generation);

    mutable std::mutex mutex_;
    // Guards the externally installed hooks (report sink, wake callback and
    // scheduler stop hook) so they can be replaced/cleared while another
    // thread emits effects.
    mutable std::mutex hook_mutex_;
    ProbeParams params_;
    Clock clock_;
    ProbeReportSink* report_{nullptr};
    WakeCallback wake_;
    std::function<void()> scheduler_stop_;
    // Woken when the in-flight table loses an entry, for bounded drain waits.
    std::condition_variable inflight_cv_;
    // False after stopPosting(): no new probe may be claimed or created.
    std::atomic<bool> accepting_{true};

    bool initialized_{false};
    uint64_t graph_epoch_{0};
    // Interface-7 idempotency watermark inside graph_epoch_.
    uint64_t last_applied_seq_{0};

    PortMap ports_;
    PathMap paths_;
    // W3 M4: (local_nic, segment, remote_nic) -> current PathVersion.
    // Maintained by every row creation/removal so the business routing gate
    // answers in O(1) instead of scanning every path under the PLM lock.
    std::unordered_map<ProbePathKey, PathVersion, ProbePathKeyHash> path_index_;
    EdgeMap edges_;
    CampaignMap campaigns_;
    RecordMap inflight_;
    // Tokens that were moved out of the normal settlement path. They still
    // hold their segment references (and never their port slot/budget).
    RecordMap orphans_;
    std::map<ProbePeerKey, uint32_t> peer_inflight_;

    uint64_t campaign_seq_{0};
    uint64_t lane_error_epoch_{0};

    ProbeStats stats_;
};

}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_PROBE_LINK_MANAGER_H_
