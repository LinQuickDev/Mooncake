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
// ProbeLinkManager unit tests for the W1a exit matrix (design §14.2):
//   #1  state-machine transitions and their side effects,
//   #9  token namespace,
//   #11 parameter-class business errors never isolate a path,
//   #13 business-error idempotency while isolated,
// plus the recovery gate and the generation-scoped port slot.

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "tent/transport/ub/probe_link_manager.h"
#include "ub_probe_test_support.h"

namespace mooncake {
namespace tent {
namespace ub {
namespace {

using namespace probe_test;

constexpr uint64_t kIntervalNs = 1000ULL * 1000 * 1000;

TEST(UbProbeLinkManagerTest, InitializesEligibleAndBusinessSuccessPromotes) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    RecordingReportSink reports;
    plm.setReportSink(&reports);

    auto graph = makeGraph({makePortSupply(0, 1), makePortSupply(1, 1)},
                           {makeEdgeSupply(0, 10, 0), makeEdgeSupply(0, 10, 1),
                            makeEdgeSupply(1, 20, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    EXPECT_TRUE(plm.initialized());
    EXPECT_EQ(plm.graphEpoch(), 1U);

    const auto view = plm.healthView();
    ASSERT_EQ(view.size(), 3U);
    for (const auto& entry : view) {
        EXPECT_EQ(entry.health, LinkHealth::kEligibleUnverified);
        EXPECT_EQ(entry.task, TaskState::kIdle);
        EXPECT_FALSE(entry.pending);
    }
    EXPECT_EQ(plm.stats().path_state_eligible, 3U);

    const PathVersion v00{ProbePathKey{0, 10, 0}, 1, 1};
    plm.onBusinessPathSuccess(makeEvidence(0, 10, 0), clock.now());
    ASSERT_TRUE(hasHealth(plm, v00));
    EXPECT_EQ(healthOf(plm, v00).health, LinkHealth::kHealthy);
    EXPECT_EQ(plm.stats().path_state_healthy, 1U);

    // A repeated success on a healthy path is not a second publication.
    plm.onBusinessPathSuccess(makeEvidence(0, 10, 0), clock.now());
    EXPECT_EQ(plm.stats().path_state_healthy, 1U);

    // The probe namespace is claimed first: a business token is not a probe,
    // and an unknown probe token is accounted instead of crashing.
    EXPECT_FALSE(plm.onProbeCompletion(makeCompletion(42), clock.now()));
    EXPECT_EQ(plm.stats().probe_unknown_token, 0U);
    EXPECT_TRUE(plm.onProbeCompletion(makeCompletion(makeProbeToken(999)),
                                      clock.now()));
    EXPECT_EQ(plm.stats().probe_unknown_token, 1U);
}

TEST(UbProbeLinkManagerTest, BusinessSuccessNeverClearsIsolation) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());

    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};
    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    ASSERT_EQ(healthOf(plm, version).health, LinkHealth::kIsolated);

    // An old success arriving after a newer fault must not undo the fault.
    plm.onBusinessPathSuccess(makeEvidence(0, 10, 0), clock.now());
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kIsolated);
}

TEST(UbProbeLinkManagerTest, HealthyPathFaultIsolatesWithNewEpoch) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};

    plm.onBusinessPathSuccess(makeEvidence(0, 10, 0), clock.now());
    ASSERT_EQ(healthOf(plm, version).health, LinkHealth::kHealthy);

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    const auto health = healthOf(plm, version);
    EXPECT_EQ(health.health, LinkHealth::kIsolated);
    EXPECT_EQ(health.fault_epoch, 1U);
    EXPECT_EQ(health.next_probe_ns, clock.now());
    EXPECT_EQ(plm.stats().path_state_isolated, 1U);
    EXPECT_EQ(plm.campaignView().size(), 1U);
}

TEST(UbProbeLinkManagerTest, CompletionBeforePostReturnIsSettledOnce) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    auto probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());

    // The CQE may legally beat the post call's return (invariant 4). The
    // record must be settleable already, and the late "accepted" result must
    // not settle it a second time.
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(probe->token), clock.now()));
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kHealthy);
    EXPECT_EQ(plm.inflightCount(), 0U);

    plm.onPostResult(probe->record, ProbePostOutcome::kAccepted, clock.now());
    EXPECT_EQ(plm.stats().probe_posted, 1U);
    EXPECT_EQ(plm.stats().probe_post_conflict, 0U);
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kHealthy);
}

TEST(UbProbeLinkManagerTest, ParameterAndLaneBusinessErrorsNeverIsolate) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());

    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};
    for (const auto kind :
         {BusinessErrorKind::kParamError, BusinessErrorKind::kLaneError,
          BusinessErrorKind::kUnknown}) {
        plm.onBusinessPathError(makeEvidence(0, 10, 0), kind, clock.now());
        EXPECT_EQ(healthOf(plm, version).health,
                  LinkHealth::kEligibleUnverified);
    }
    EXPECT_TRUE(plm.campaignView().empty());
    const auto stats = plm.stats();
    EXPECT_EQ(stats.business_param_error, 1U);
    EXPECT_EQ(stats.business_lane_error, 1U);
    EXPECT_EQ(stats.business_unknown_error, 1U);
    EXPECT_EQ(stats.campaign_created, 0U);
    EXPECT_EQ(stats.business_error_while_isolated, 0U);
}

TEST(UbProbeLinkManagerTest, BusinessFaultIsIdempotentWhileIsolated) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0),
                                                    makeEdgeSupply(0, 10, 1)});
    ASSERT_TRUE(plm.initialize(graph).ok());

    const PathVersion first{ProbePathKey{0, 10, 0}, 1, 1};
    const PathVersion second{ProbePathKey{0, 10, 1}, 1, 1};

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    auto isolated = healthOf(plm, first);
    EXPECT_EQ(isolated.health, LinkHealth::kIsolated);
    EXPECT_EQ(isolated.fault_epoch, 1U);
    EXPECT_EQ(isolated.task, TaskState::kQueued);
    EXPECT_EQ(isolated.next_probe_ns, clock.now());

    auto campaigns = plm.campaignView();
    ASSERT_EQ(campaigns.size(), 1U);
    EXPECT_EQ(campaigns[0].epoch, 1U);
    ASSERT_EQ(campaigns[0].entries.size(), 2U);
    EXPECT_EQ(campaignEntryOf(campaigns[0], first)->state, EntryState::kQueued);
    EXPECT_EQ(campaignEntryOf(campaigns[0], second)->state,
              EntryState::kQueued);

    // Without business filtering the same isolated path keeps failing. That
    // must not raise its fault epoch or merge the campaign (design §6.3-3),
    // otherwise every in-flight probe would go stale forever.
    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    EXPECT_EQ(healthOf(plm, first).fault_epoch, 1U);
    campaigns = plm.campaignView();
    ASSERT_EQ(campaigns.size(), 1U);
    EXPECT_EQ(campaigns[0].epoch, 1U);
    EXPECT_EQ(plm.stats().business_error_while_isolated, 1U);

    // A fault on another path of the same peer merges the campaign, and the
    // queued entry is re-armed under the new epoch.
    plm.onBusinessPathError(makeEvidence(0, 10, 1),
                            BusinessErrorKind::kPathFault, clock.now());
    EXPECT_EQ(healthOf(plm, second).health, LinkHealth::kIsolated);
    campaigns = plm.campaignView();
    ASSERT_EQ(campaigns.size(), 1U);
    EXPECT_EQ(campaigns[0].epoch, 2U);
    EXPECT_EQ(campaignEntryOf(campaigns[0], first)->entry_epoch, 2U);
    EXPECT_EQ(campaignEntryOf(campaigns[0], second)->entry_epoch, 2U);
    EXPECT_EQ(plm.stats().campaign_merged, 1U);
}

TEST(UbProbeLinkManagerTest, ProbeSuccessRecoversOnlyWhenGatesPass) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph =
        makeGraph({makePortSupply(0, 1)},
                  {makeEdgeSupply(0, 10, 0, 1, 1, 1, /*gates_ok=*/false)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    ASSERT_EQ(healthOf(plm, version).health, LinkHealth::kIsolated);

    uint64_t token_seq = 0;
    auto posted = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(posted.has_value());
    EXPECT_TRUE(
        plm.onProbeCompletion(makeCompletion(posted->token), clock.now()));

    auto health = healthOf(plm, version);
    EXPECT_EQ(health.health, LinkHealth::kRecovering);
    EXPECT_EQ(health.next_probe_ns, clock.now() + kIntervalNs);
    EXPECT_EQ(health.task, TaskState::kIdle);
    EXPECT_EQ(plm.stats().probe_gate_blocked_by_external, 1U);
    // Campaign exit is driven by entry terminal states, not by RECOVERING.
    EXPECT_TRUE(plm.campaignView().empty());
    EXPECT_EQ(plm.stats().campaign_exited, 1U);

    ProbeEvent gate_event;
    gate_event.kind = ProbeEventKind::kGateChanged;
    gate_event.local_nic = 0;
    gate_event.graph_epoch = 1;
    gate_event.event_seq = 1;
    gate_event.gate = GateBits{true, true, true};
    ASSERT_TRUE(plm.onPoolAvailabilityChange(gate_event).ok());

    health = healthOf(plm, version);
    EXPECT_EQ(health.health, LinkHealth::kHealthy);
    EXPECT_EQ(health.next_probe_ns, 0U);
    EXPECT_EQ(plm.stats().path_state_healthy, 1U);
}

TEST(UbProbeLinkManagerTest, LaneErrorRetiresLaneOnceAndMarksPortPending) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    RecordingReportSink reports;
    plm.setReportSink(&reports);
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0),
                                                    makeEdgeSupply(0, 10, 1)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion first{ProbePathKey{0, 10, 0}, 1, 1};
    const PathVersion second{ProbePathKey{0, 10, 1}, 1, 1};

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    auto posted = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(posted.has_value());
    ASSERT_EQ(posted->record->key.version, first);

    ASSERT_TRUE(plm.onProbeCompletion(
        makeCompletion(posted->token, CompletionCategory::ENDPOINT_ERROR),
        clock.now()));

    // A lane/configuration error must not mark the path bad (L-3), but the
    // whole port becomes Pending and the lane is reported exactly once.
    for (const auto& version : {first, second}) {
        const auto health = healthOf(plm, version);
        EXPECT_EQ(health.health, version == first
                                     ? LinkHealth::kIsolated
                                     : LinkHealth::kEligibleUnverified);
        EXPECT_TRUE(health.pending);
        EXPECT_EQ(health.pending_reason, PendingReason::kLaneRetired);
    }
    ASSERT_EQ(reports.retire_calls.size(), 1U);
    EXPECT_EQ(reports.retire_calls[0].jetty_generation, 1U);
    EXPECT_EQ(reports.retire_calls[0].lane.local_nic, 0);
    EXPECT_EQ(plm.stats().probe_failure_lane, 1U);
    EXPECT_EQ(plm.inflightCount(), 0U);

    // Duplicate CQE: claimed already, so it only shows up as an unknown token;
    // it must not report the lane twice.
    EXPECT_TRUE(plm.onProbeCompletion(
        makeCompletion(posted->token, CompletionCategory::ENDPOINT_ERROR),
        clock.now()));
    EXPECT_EQ(reports.retire_calls.size(), 1U);
    EXPECT_EQ(plm.stats().probe_unknown_token, 1U);
    // A broken lane posts nothing until interface 7 replaces the handle.
    EXPECT_TRUE(plm.pendingActions(clock.now()).empty());
}

TEST(UbProbeLinkManagerTest, ContextUnavailableDisablesPortAndCancelsCampaign) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    RecordingReportSink reports;
    plm.setReportSink(&reports);
    auto graph = makeGraph({makePortSupply(0, 1), makePortSupply(1, 1)},
                           {makeEdgeSupply(0, 10, 0), makeEdgeSupply(0, 10, 1),
                            makeEdgeSupply(1, 20, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    ASSERT_EQ(plm.campaignView().size(), 1U);

    plm.onContextUnavailable(0, clock.now());
    for (const auto& entry : plm.healthView()) {
        if (entry.version.path.local_nic != 0) continue;
        EXPECT_EQ(entry.health, LinkHealth::kDisabled);
        EXPECT_TRUE(entry.pending);
        EXPECT_EQ(entry.pending_reason, PendingReason::kContextFailure);
        EXPECT_EQ(entry.task, TaskState::kIdle);
    }
    EXPECT_TRUE(plm.campaignView().empty());
    EXPECT_EQ(plm.stats().path_state_disabled, 2U);
    EXPECT_EQ(plm.stats().campaign_entry_canceled, 2U);
    // A context failure is reported through interface 9, not interface 2.
    EXPECT_TRUE(reports.retire_calls.empty());

    // A DISABLED path is not recovered by probing: further business errors are
    // counted, not turned into isolation or a new campaign.
    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    EXPECT_EQ(plm.stats().business_error_while_disabled, 1U);
    EXPECT_TRUE(plm.campaignView().empty());
}

TEST(UbProbeLinkManagerTest, StaleCompletionReleasesSlotWithoutChangingHealth) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0),
                                                    makeEdgeSupply(0, 10, 1)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion first{ProbePathKey{0, 10, 0}, 1, 1};
    const PathVersion second{ProbePathKey{0, 10, 1}, 1, 1};

    // Fault on the second path starts a campaign that verifies both.
    plm.onBusinessPathError(makeEvidence(0, 10, 1),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    auto in_flight = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(in_flight.has_value());
    ASSERT_EQ(in_flight->record->key.version, first);
    ASSERT_EQ(plm.campaignView()[0].epoch, 1U);

    // A newer fault on the in-flight path bumps its fault epoch, so the
    // outstanding success can no longer publish health.
    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    EXPECT_EQ(healthOf(plm, first).health, LinkHealth::kIsolated);
    EXPECT_EQ(healthOf(plm, first).fault_epoch, 1U);
    auto campaigns = plm.campaignView();
    ASSERT_EQ(campaigns.size(), 1U);
    EXPECT_EQ(campaigns[0].epoch, 2U);
    EXPECT_EQ(campaignEntryOf(campaigns[0], first)->state,
              EntryState::kInflight);
    EXPECT_EQ(campaignEntryOf(campaigns[0], first)->entry_epoch, 1U);

    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(in_flight->token), clock.now()));
    EXPECT_EQ(plm.stats().probe_stale, 1U);
    // A stale success is not current evidence, so it is not counted as one.
    EXPECT_EQ(plm.stats().probe_success, 0U);
    EXPECT_EQ(healthOf(plm, first).health, LinkHealth::kIsolated);
    // The task slot is released by probe_seq even though the result was stale,
    // and the matching campaign entry converges to a terminal state.
    EXPECT_EQ(healthOf(plm, first).inflight_seq, 0U);
    EXPECT_EQ(healthOf(plm, first).task, TaskState::kIdle);
    campaigns = plm.campaignView();
    ASSERT_EQ(campaigns.size(), 1U);
    EXPECT_EQ(campaigns[0].epoch, 2U);
    EXPECT_EQ(campaignEntryOf(campaigns[0], first)->state,
              EntryState::kCanceled);
    EXPECT_EQ(campaignEntryOf(campaigns[0], second)->state,
              EntryState::kQueued);

    // The next campaign attempt is the other path; it recovers normally.
    auto next = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(next.has_value());
    EXPECT_EQ(next->record->key.version, second);
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(next->token), clock.now()));
    EXPECT_EQ(healthOf(plm, second).health, LinkHealth::kHealthy);
    EXPECT_TRUE(plm.campaignView().empty());
}

TEST(UbProbeLinkManagerTest,
     GenerationReplacementOrphansInflightAndResetsSlot) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    RecordingReportSink reports;
    plm.setReportSink(&reports);
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    auto old_probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(old_probe.has_value());
    EXPECT_EQ(plm.inflightCount(), 1U);

    ProbeEvent replaced;
    replaced.kind = ProbeEventKind::kProbeHandleReplaced;
    replaced.local_nic = 0;
    replaced.graph_epoch = 1;
    replaced.event_seq = 1;
    replaced.port = makePortSupply(0, 2);
    ASSERT_TRUE(plm.onPoolAvailabilityChange(replaced).ok());

    // Old generation becomes an orphan (references retained), the slot is
    // reset for the new generation, and the isolated path probes immediately.
    EXPECT_EQ(plm.orphanCount(), 1U);
    EXPECT_EQ(plm.inflightCount(), 0U);
    auto health = healthOf(plm, version);
    EXPECT_EQ(health.health, LinkHealth::kIsolated);
    EXPECT_FALSE(health.pending);
    EXPECT_EQ(health.next_probe_ns, clock.now());
    // Orphan transfer does not report interface 2.
    EXPECT_TRUE(reports.retire_calls.empty());

    auto new_probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(new_probe.has_value());
    EXPECT_NE(new_probe->token, old_probe->token);
    EXPECT_EQ(plm.inflightCount(), 1U);

    // A late completion of the old generation only settles the orphan; it must
    // not disturb the new generation's slot or health.
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(old_probe->token), clock.now()));
    EXPECT_EQ(plm.orphanCount(), 0U);
    EXPECT_EQ(plm.stats().probe_orphan_settled, 1U);
    EXPECT_EQ(plm.inflightCount(), 1U);
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kIsolated);

    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(new_probe->token), clock.now()));
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kHealthy);
    EXPECT_EQ(plm.stats().probe_slot_anomaly, 0U);
}

TEST(UbProbeLinkManagerTest, PeerBudgetGatesCrossPortScheduling) {
    TestClock clock;
    auto params = defaultParams();
    params.peer_budget = 1;
    ProbeLinkManager plm(params, clock.clock());
    auto graph =
        makeGraph({makePortSupply(0, 1), makePortSupply(1, 1)},
                  {makeEdgeSupply(0, 10, 0), makeEdgeSupply(1, 10, 1)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion first{ProbePathKey{0, 10, 0}, 1, 1};
    const PathVersion second{ProbePathKey{1, 10, 1}, 1, 1};

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    auto actions = plm.pendingActions(clock.now());
    ASSERT_EQ(actions.size(), 2U);

    uint64_t token_seq = 0;
    auto first_probe =
        plm.beginPost(actions[0], makeProbeToken(++token_seq), clock.now());
    ASSERT_NE(first_probe, nullptr);
    // The peer budget is shared across ports, so the second port cannot claim
    // another in-flight probe to the same peer.
    auto blocked =
        plm.beginPost(actions[1], makeProbeToken(++token_seq), clock.now());
    EXPECT_EQ(blocked, nullptr);
    EXPECT_EQ(plm.stats().probe_skipped_peer_budget, 0U);

    // A lane error cancels the first entry and frees the budget; the second
    // path is then schedulable and can recover.
    ASSERT_TRUE(plm.onProbeCompletion(
        makeCompletion(first_probe->core.token,
                       CompletionCategory::ENDPOINT_ERROR),
        clock.now()));
    auto retry = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(retry.has_value());
    EXPECT_EQ(retry->record->key.version, second);
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(retry->token), clock.now()));
    EXPECT_EQ(healthOf(plm, second).health, LinkHealth::kHealthy);
    // The lane error did not mark the first path bad; it stays isolated from
    // its original business fault.
    EXPECT_EQ(healthOf(plm, first).health, LinkHealth::kIsolated);
}

TEST(UbProbeLinkManagerTest, HandleReplacedPreservesBackoff) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const uint64_t now = clock.now();

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, now);
    uint64_t token_seq = 0;
    auto probe = postNext(plm, token_seq, now);
    ASSERT_TRUE(probe.has_value());
    ASSERT_TRUE(plm.onProbeCompletion(
        makeCompletion(probe->token, CompletionCategory::REMOTE_PATH_ERROR),
        now));
    ASSERT_EQ(healthOf(plm, probe->record->key.version).health,
              LinkHealth::kIsolated);

    // The lane was retired; a replacement event must not erase the backoff.
    ProbeEvent event;
    event.kind = ProbeEventKind::kProbeHandleReplaced;
    event.local_nic = 0;
    event.graph_epoch = plm.graphEpoch();
    event.event_seq = 1;
    event.port = makePortSupply(0, 2);
    ASSERT_TRUE(plm.onPoolAvailabilityChange(event).ok());

    EXPECT_TRUE(plm.pendingActions(now).empty());
    clock.advance(1000ULL * 1000 * 1000);
    EXPECT_FALSE(plm.pendingActions(clock.now()).empty());
}

}  // namespace
}  // namespace ub
}  // namespace tent
}  // namespace mooncake
