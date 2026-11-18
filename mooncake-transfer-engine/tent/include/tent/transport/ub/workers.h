// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_TRANSPORT_UB_WORKERS_H_
#define TENT_TRANSPORT_UB_WORKERS_H_

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "tent/common/status.h"
#include "tent/runtime/segment_manager.h"
#include "tent/runtime/topology.h"
#include "tent/transport/ub/buffers.h"
#include "tent/transport/ub/completion_record.h"
#include "tent/transport/ub/context.h"
#include "tent/transport/ub/entity_event.h"
#include "tent/transport/ub/params.h"
#include "tent/transport/ub/path_load_ledger.h"
#include "tent/transport/ub/post_scheduler.h"
#include "tent/transport/ub/probe_link_manager.h"
#include "tent/transport/ub/quota.h"
#include "tent/transport/ub/rail_monitor.h"
#include "tent/transport/ub/send_jetty_pool.h"
#include "tent/transport/ub/slice.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake::tent::ub {

class UbEndpoint;

struct EndpointResolveRequest {
    UbContextPtr local_context;
    SegmentID remote_segment_id{LOCAL_SEGMENT_ID};
    const SegmentDesc* remote_segment{nullptr};
    Topology::NicID remote_topology_id{-1};
    uint64_t segment_generation{0};
};

using EndpointResolver = std::function<Status(const EndpointResolveRequest&,
                                              std::shared_ptr<UbEndpoint>&)>;
using EndpointRetirer = std::function<void(const std::shared_ptr<UbEndpoint>&)>;
using LocalDeviceFailureHandler =
    std::function<Status(Topology::NicID local_topology_id)>;

// Lexicographic path score. Capacity is a hard preference, topology locality
// remains the primary policy choice, and live pressure breaks ties before
// learned bandwidth. The deterministic tail makes concurrent workers agree on
// an order while retry_order rotates otherwise-equivalent rails on retry.
struct UbPathSelectionScore {
    bool quota_available{false};
    size_t topology_rank{Topology::DevicePriorityRanks};
    double normalized_inflight{1.0};
    double normalized_quota_wrs{1.0};
    // W3 D-11 renamed these from endpoint_outstanding_{wrs,bytes}: the value no
    // longer comes from the endpoint (which resets on every generation change)
    // but from the per-path in-flight ledger. The rename makes the source
    // change impossible to miss in review.
    uint64_t path_inflight_wrs{0};
    uint64_t path_inflight_bytes{0};
    bool has_bandwidth_sample{false};
    double ewma_bandwidth_bytes_per_second{-1.0};
    size_t retry_order{0};
    Topology::NicID local_topology_id{-1};
    int remote_device_id{-1};
};

[[nodiscard]] inline bool betterUbPathScore(
    const UbPathSelectionScore& lhs, const UbPathSelectionScore& rhs) noexcept {
    if (lhs.quota_available != rhs.quota_available) return lhs.quota_available;
    if (lhs.topology_rank != rhs.topology_rank)
        return lhs.topology_rank < rhs.topology_rank;
    if (lhs.normalized_inflight != rhs.normalized_inflight)
        return lhs.normalized_inflight < rhs.normalized_inflight;
    if (lhs.normalized_quota_wrs != rhs.normalized_quota_wrs)
        return lhs.normalized_quota_wrs < rhs.normalized_quota_wrs;
    if (lhs.path_inflight_wrs != rhs.path_inflight_wrs) {
        return lhs.path_inflight_wrs < rhs.path_inflight_wrs;
    }
    if (lhs.path_inflight_bytes != rhs.path_inflight_bytes) {
        return lhs.path_inflight_bytes < rhs.path_inflight_bytes;
    }
    if (lhs.has_bandwidth_sample != rhs.has_bandwidth_sample)
        return lhs.has_bandwidth_sample;
    if (lhs.has_bandwidth_sample && lhs.ewma_bandwidth_bytes_per_second !=
                                        rhs.ewma_bandwidth_bytes_per_second) {
        return lhs.ewma_bandwidth_bytes_per_second >
               rhs.ewma_bandwidth_bytes_per_second;
    }
    if (lhs.retry_order != rhs.retry_order)
        return lhs.retry_order < rhs.retry_order;
    if (lhs.local_topology_id != rhs.local_topology_id)
        return lhs.local_topology_id < rhs.local_topology_id;
    return lhs.remote_device_id < rhs.remote_device_id;
}

// Orders pre-scored rails for lazy endpoint resolution. `local_topology_ids`
// holds each rail's local device in pre-score order, and the returned values
// are indices into it.
//
// Pass one takes the highest-scoring rail of every local device, so the
// candidates that actually get resolved span devices instead of piling onto
// whichever device ranks best. Pass two appends the remaining rails, so a
// caller that still needs candidates - a single-device topology, or a rail that
// failed to resolve - can fill its floor without attempting a rail twice.
[[nodiscard]] inline std::vector<size_t> ubResolutionAttemptOrder(
    const std::vector<Topology::NicID>& local_topology_ids) {
    const size_t count = local_topology_ids.size();
    std::vector<bool> taken(count, false);
    std::vector<size_t> order;
    order.reserve(count);
    for (size_t index = 0; index < count; ++index) {
        const auto id = local_topology_ids[index];
        bool known_device = false;
        for (size_t earlier : order) {
            if (local_topology_ids[earlier] == id) {
                known_device = true;
                break;
            }
        }
        if (known_device) continue;
        order.push_back(index);
        taken[index] = true;
    }
    for (size_t index = 0; index < count; ++index) {
        if (!taken[index]) order.push_back(index);
    }
    return order;
}

// W3 business-path counters (design §13). The values are cumulative except
// queue_depth/parked_depth, which are sampled at snapshot() time.
struct UbBusinessStatsSnapshot {
    uint64_t queue_depth{0};
    uint64_t parked_depth{0};
    uint64_t submit_backpressure{0};
    uint64_t submit_accepted{0};
    std::array<uint64_t, kDeferReasonCount> defer_pending{};
    std::array<uint64_t, kDeferReasonCount> defer_epoch_wakeup{};
    uint64_t defer_reflow_repark{0};
    uint64_t defer_bucket_snapshot_retry{0};
    uint64_t reflow_requeued{0};
    uint64_t reflow_cancelled{0};
    uint64_t task_K_wait{0};
    uint64_t pool_lease_attempts{0};
    uint64_t pool_lease_misses{0};
    uint64_t pool_stop_rejections{0};
    uint64_t pool_hard_errors{0};
    uint64_t claim_after_isolation{0};
    uint64_t post_on_isolated_path{0};
    uint64_t pooled_return{0};
    uint64_t pooled_retire{0};
    uint64_t pool_return_double{0};
    uint64_t path_inflight_peak{0};
    uint64_t path_load_imbalance{0};
};

// One dedicated probe completion queue handed over by the pooling side. The
// handle stays owned by the provider; workers only polls it (design §11.1).
struct ProbeJfcSlot {
    Topology::NicID local_nic{-1};
    JfcPtr jfc;
};

// Native UB scheduler and owner of the business half of every completion
// record. Posting lanes own request selection and URMA post; poller lanes own
// completion dispatch. A monotonic numeric token, never a raw UbSlice pointer,
// crosses the adapter boundary.
//
// `gate::Sink` is the completion gate's side-effect outlet (design D-11): the
// gate decides *when* each action may run, this class performs the resource
// release, the health reporting and the slice delivery.
class UbWorkers final : public gate::Sink {
   public:
    UbWorkers(std::shared_ptr<UrmaAdapter> adapter,
              std::vector<UbContextPtr> contexts,
              std::shared_ptr<Topology> local_topology,
              SegmentManager* segment_manager, UbBufferManager* buffers,
              RailMonitor* rail_monitor, QuotaManager* quota, UbParams params,
              EndpointResolver endpoint_resolver,
              EndpointRetirer endpoint_retirer = {},
              LocalDeviceFailureHandler local_device_failure_handler = {},
              ProbeLinkManager* probe_manager = nullptr,
              std::vector<ProbeJfcSlot> probe_jfcs = {});
    ~UbWorkers();

    UbWorkers(const UbWorkers&) = delete;
    UbWorkers& operator=(const UbWorkers&) = delete;

    Status start();
    Status stop();
    Status submit(const UbTask::Ptr& task, uint64_t device_mask = ~0ULL);
    Status cancel(const UbTask::Ptr& task);

    [[nodiscard]] bool running() const noexcept {
        return accepting_.load(std::memory_order_acquire);
    }
    [[nodiscard]] size_t queuedCount() const;
    // Deferred (parked) slices: part of the same bounded budget as the queue
    // (design §6.1), reported separately for observation.
    [[nodiscard]] size_t parkedCount() const;
    // Number of records still registered (in-flight owner accounting).
    [[nodiscard]] size_t inflightCount() const;
    // W3 admission/defer/pool counters (design §13).
    [[nodiscard]] UbBusinessStatsSnapshot businessStats() const;
    [[nodiscard]] uint64_t queueBudget() const noexcept {
        return params_.business_queue_budget;
    }
    // Completion gate counters (design §9).
    [[nodiscard]] CompletionStatsSnapshot completionStats() const {
        return gate_ctx_.stats.snapshot();
    }
    [[nodiscard]] const CountingEntityEventSink& entityEvents() const {
        return entity_events_;
    }

   private:
    struct PendingSlice {
        // Keep the task alive until every queued/in-flight slice reaches a
        // terminal state. Callers may release the sub-batch handle before the
        // asynchronous data path has drained.
        UbTask::Ptr task;
        UbSlice::Ptr slice;
        uint64_t device_mask{~0ULL};
        int priority{PRIO_HIGH};
        SegmentID target_id{LOCAL_SEGMENT_ID};
        Request::OpCode opcode{Request::READ};
        // How often this slice has been parked so far. Used only to count
        // defer_reflow_repark: a reflowed slice that has to wait again means a
        // wake-up source and its condition disagree (design §13).
        uint32_t deferrals{0};
    };
    struct Route;
    struct BusinessRecord;
    // Availability epochs of every defer bucket, sampled once before the
    // admission conditions are evaluated (design §7.4).
    struct DeferEpochs {
        uint64_t task_permit{0};
        uint64_t quota{0};
        uint64_t pool_idle{0};
        uint64_t path{0};
    };
    // Why selection failed and when to re-evaluate it.
    struct DeferPlan {
        DeferReason reason{DeferReason::kQuota};
        // Non-zero for conditions that only time can resolve (a device in
        // recovery cooldown). Every other reason is bumped by its release site.
        uint64_t expire_ns{0};
    };
    struct LocalDeviceCandidate {
        Topology::NicID id{-1};
        size_t topology_rank{Topology::DevicePriorityRanks};
    };

    // popPending distinguishes "took an item", "the deferred set has wake-ups
    // to reflow" and "posting stopped" so the posting loop never sleeps on a
    // resource change it could act on (design §7.4).
    enum class PopResult : uint8_t {
        kPending,
        kReflow,
        kStopped,
    };

    void postingLoop(size_t worker_index);
    void pollingLoop(size_t poller_index);
    PopResult popPending(PendingSlice& pending);
    void enqueueRetry(const PendingSlice& pending);
    // Parks one slice against the availability epoch that was read *before*
    // the failing condition was evaluated (design §7.4). Never blocks and never
    // sleeps: the bucket bump is the only wake-up source.
    void deferPending(const PendingSlice& pending, DeferReason reason,
                      uint64_t wait_epoch, uint64_t expire_ns = 0);
    // Moves stale parked slices back to the business queue. Returns the number
    // of reflowed items.
    size_t reflowDeferred();
    // Must be called with queue_mutex_ held.
    void bumpLocked(DeferReason reason);
    void processPending(const PendingSlice& pending, size_t worker_index);
    // Returns a copy of the deferred slice with its deferral counter advanced
    // (and the repark observation recorded).
    [[nodiscard]] PendingSlice withDeferralCount(const PendingSlice& pending);
    // Periodic, rate-limited business summary (production observation outlet).
    void logBusinessSummary(uint64_t now_ns);
    // W3 pooled tail of processPending: K -> lease(+credit) -> claim snapshot
    // -> claim review -> commit -> publish -> tristate post (design §8.1).
    void processPendingPooled(
        const PendingSlice& pending, size_t worker_index, const Route& route,
        const std::shared_ptr<UbEndpoint>& endpoint, const UbPostPath& path,
        const QuotaReservation& reservation, const LocalSegmentRef& local,
        const ImportedSegmentRef& remote, const UbAttemptToken& attempt);
    // W2 tail of processPending, kept byte-for-byte for pool == nullptr.
    void postLegacy(const PendingSlice& pending, size_t worker_index,
                    const std::shared_ptr<UbEndpoint>& endpoint,
                    const UbPostPath& path, const QuotaReservation& reservation,
                    const LocalSegmentRef& local,
                    const ImportedSegmentRef& remote,
                    const UbAttemptToken& attempt);
    std::shared_ptr<BusinessRecord> makeRecord(
        const PendingSlice& pending,
        const std::shared_ptr<UbEndpoint>& endpoint, const UbPostPath& path,
        const QuotaReservation& reservation, const LocalSegmentRef& local,
        const ImportedSegmentRef& remote, const UbAttemptToken& attempt);
    // The W3 claim review (design §8.2, R-1 transition shape): pool stop plus
    // the fault-epoch snapshot comparison. Deliberately lock-free and
    // allocation-free: it runs immediately before the native post (W3-IN-8).
    [[nodiscard]] bool claimPost(const std::shared_ptr<SendJettyPool>& pool,
                                 const UbPostPath& path,
                                 uint64_t fault_epoch_snapshot);
    [[nodiscard]] static PoolKey poolKeyFor(
        const std::shared_ptr<UbEndpoint>& endpoint);
    [[nodiscard]] DeferEpochs readDeferEpochs() const;
    // The W3 mode gate (D-2): non-null only when the transport attached a pool
    // to this endpoint's context.
    [[nodiscard]] std::shared_ptr<SendJettyPool> pooledPoolFor(
        const std::shared_ptr<UbEndpoint>& endpoint) const;
    // Cheap pool pre-filter for path selection (design §7.1 rule 2). A false
    // answer only means "try another candidate"; tryLease stays authoritative.
    [[nodiscard]] bool poolCandidateUsable(
        const std::shared_ptr<UbEndpoint>& endpoint) const;
    [[nodiscard]] static WorkRequest makeWorkRequest(
        const PendingSlice& pending, uint64_t token,
        const LocalSegmentRef& local, const ImportedSegmentRef& remote);
    Status buildRoute(const PendingSlice& pending, Route& route);
    Status chooseAndResolveEndpoint(const PendingSlice& pending, Route& route,
                                    std::shared_ptr<UbEndpoint>& endpoint,
                                    UbPostPath& path,
                                    QuotaReservation& reservation,
                                    DeferPlan* plan = nullptr);
    // Namespace first: probe tokens are routed to the PLM before the business
    // table can silently drop them (design §11.2). `local_nic`/`jfc_index`
    // identify the polling slot for the interface-8 reports; the fence/stop
    // drains pass the record's local NIC and no JFC index.
    bool dispatchCompletion(const Completion& completion, int local_nic = -1,
                            uint64_t jfc_index = 0);
    bool pollBusinessSlot(size_t slot_index);
    bool pollProbeSlot(size_t slot_index);
    void handleLocalDeviceFailure(const UbContextPtr& context);
    void progressLocalDeviceFailure(const UbContextPtr& context);
    void scanTimeouts();
    static WrFacts makeWrFacts(const Completion& completion);
    [[nodiscard]] std::shared_ptr<BusinessRecord> findRecord(
        uint64_t token) const;
    void releaseOwnerResources(BusinessRecord& record,
                               OwnerDisposition disposition);
    void eraseRecord(uint64_t token, const BusinessRecord* record);
    void deliverSettlement(BusinessRecord& record);
    void recordSuccessEvidence(BusinessRecord& record);

    // gate::Sink. All of them run on the thread that won the corresponding
    // claim, outside the registry lock.
    void onSettle(CompletionRecord& record, LogicalOutcome outcome) override;
    void onOwners(CompletionRecord& record,
                  OwnerDisposition disposition) override;
    void onFault(CompletionRecord& record, FaultKind kind) override;
    void onBlocked(CompletionRecord& record, BlockReason reason) override;
    void onDuplicate(CompletionRecord& record) override;
    void rememberEndpointDrain(const std::shared_ptr<UbEndpoint>& endpoint);
    void forgetEndpointDrain(const std::shared_ptr<UbEndpoint>& endpoint);
    void failUnposted(const PendingSlice& pending,
                      TransferStatusEnum outcome = FAILED);
    uint64_t nextCompletionToken();
    std::vector<LocalDeviceCandidate> orderedLocalDevices(
        const PendingSlice& pending) const;
    static std::vector<Topology::NicID> orderedRemoteDevices(
        const SegmentDesc& segment, const BufferDesc& buffer);

    std::shared_ptr<UrmaAdapter> adapter_;
    std::vector<UbContextPtr> contexts_;
    std::unordered_map<Topology::NicID, UbContextPtr> context_by_topology_id_;
    std::shared_ptr<Topology> local_topology_;
    SegmentManager* segment_manager_;
    UbBufferManager* buffers_;
    RailMonitor* rail_monitor_;
    QuotaManager* quota_;
    const UbParams params_;
    EndpointResolver endpoint_resolver_;
    EndpointRetirer endpoint_retirer_;
    LocalDeviceFailureHandler local_device_failure_handler_;
    // Serializes endpoint-store retirement progress across JFC pollers. The
    // handler temporarily moves quarantined endpoints out of the store while
    // retrying cleanup, so a second poller must not observe a false empty
    // barrier and reactivate the context early.
    mutable std::mutex local_device_failure_mutex_;

    mutable std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::array<std::deque<PendingSlice>, PRIO_LOW + 1> queues_;
    // Slices currently in `queues_`, kept incrementally so submit() can apply
    // the all-or-nothing budget check without walking every queue (design
    // §6.1/§6.3).
    size_t queued_{0};
    // W3 defer buckets. Always mutated under queue_mutex_ together with the
    // queue, so "park" and "bump" are ordered (see post_scheduler.h).
    DeferRegistry<PendingSlice> defer_;

    mutable std::mutex records_mutex_;
    mutable std::mutex endpoint_drain_mutex_;
    // Kept under its original name: stop() still waits on it for the registry
    // to drain.
    std::condition_variable inflight_cv_;
    // Keyed by completion token. Renamed from `inflight_` because W5 adds a
    // second, deadline-ordered index next to it.
    std::unordered_map<uint64_t, std::shared_ptr<BusinessRecord>> records_;
    // Endpoints whose ERROR transition has not yet reached its native flush
    // fence remain owned here even if every logical token completes naturally.
    std::unordered_map<uint64_t, std::shared_ptr<UbEndpoint>>
        draining_endpoints_;

    struct BusinessSlot {
        std::shared_ptr<UbJfc> jfc;
        UbContextPtr context;
    };
    struct ProbeSlot {
        JfcPtr jfc;
        UbContextPtr context;
    };
    std::vector<BusinessSlot> business_slots_;
    std::vector<ProbeSlot> probe_slots_;
    ProbeLinkManager* probe_manager_{nullptr};
    // W3 per-path in-flight ledger (scoring only, D-11).
    PathLoadLedger path_load_;

    // W3 business counters. Cumulative except the two sampled depths.
    std::atomic<uint64_t> submit_backpressure_{0};
    std::atomic<uint64_t> submit_accepted_{0};
    std::array<std::atomic<uint64_t>, kDeferReasonCount> defer_pending_{};
    std::array<std::atomic<uint64_t>, kDeferReasonCount> defer_epoch_wakeup_{};
    std::atomic<uint64_t> defer_reflow_repark_{0};
    std::atomic<uint64_t> defer_bucket_snapshot_retry_{0};
    std::atomic<uint64_t> reflow_requeued_{0};
    std::atomic<uint64_t> reflow_cancelled_{0};
    std::atomic<uint64_t> task_K_wait_{0};
    std::atomic<uint64_t> pool_lease_attempts_{0};
    std::atomic<uint64_t> pool_lease_misses_{0};
    std::atomic<uint64_t> pool_stop_rejections_{0};
    std::atomic<uint64_t> pool_hard_errors_{0};
    std::atomic<uint64_t> claim_after_isolation_{0};
    std::atomic<uint64_t> post_on_isolated_path_{0};
    std::atomic<uint64_t> pooled_return_{0};
    std::atomic<uint64_t> pooled_retire_{0};
    // Only one poller per interval emits the periodic business summary.
    std::atomic<uint64_t> last_business_log_ns_{0};

    std::vector<std::thread> posting_threads_;
    std::vector<std::thread> polling_threads_;
    std::atomic<bool> accepting_{false};
    std::atomic<bool> posting_{false};
    std::atomic<bool> polling_{false};
    std::atomic<bool> timeout_scans_enabled_{false};
    std::atomic<uint64_t> next_token_{1};

    // Completion gate plumbing. Declared last so every member it touches during
    // construction is already initialized.
    GateContext gate_ctx_;
    CountingEntityEventSink entity_events_;
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_WORKERS_H_
