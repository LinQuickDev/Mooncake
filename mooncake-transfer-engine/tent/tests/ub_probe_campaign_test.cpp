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
// Campaign, generation and stale-result tests for the W1a exit matrix
// (design §14.2): #2 campaign lifecycle, #3 stale three-state, #8 post result
// rollback, #14 entry convergence, #17 merge x in-flight. The interface-7
// delivery contract (§12.3) is exercised here as well.

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

TEST(UbProbeCampaignTest, ConvergesThroughGateBlockedRecovery) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph =
        makeGraph({makePortSupply(0, 1)},
                  {makeEdgeSupply(0, 10, 0, 1, 1, 1, /*gates_ok=*/false),
                   makeEdgeSupply(0, 10, 1, 1, 1, 1, /*gates_ok=*/false)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion first{ProbePathKey{0, 10, 0}, 1, 1};
    const PathVersion second{ProbePathKey{0, 10, 1}, 1, 1};

    // Both paths of the peer are faulty: the second error merges the campaign
    // and re-arms the queued entries under the merged epoch.
    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    plm.onBusinessPathError(makeEvidence(0, 10, 1),
                            BusinessErrorKind::kPathFault, clock.now());
    ASSERT_EQ(plm.campaignView().size(), 1U);
    ASSERT_EQ(healthOf(plm, first).health, LinkHealth::kIsolated);
    ASSERT_EQ(healthOf(plm, second).health, LinkHealth::kIsolated);

    uint64_t token_seq = 0;
    auto first_probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(first_probe.has_value());
    ASSERT_EQ(first_probe->record->key.version, first);
    // Permanent gate failure: the path proves liveness but stays RECOVERING.
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(first_probe->token), clock.now()));
    EXPECT_EQ(healthOf(plm, first).health, LinkHealth::kRecovering);

    auto second_probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(second_probe.has_value());
    ASSERT_EQ(second_probe->record->key.version, second);
    ASSERT_TRUE(plm.onProbeCompletion(makeCompletion(second_probe->token),
                                      clock.now()));
    EXPECT_EQ(healthOf(plm, second).health, LinkHealth::kRecovering);

    // Exit only needs terminal entries. RECOVERING must not hold the campaign
    // open (L-2); the paths keep probing on their own interval.
    EXPECT_TRUE(plm.campaignView().empty());
    EXPECT_EQ(plm.stats().campaign_exited, 1U);
    EXPECT_EQ(plm.stats().campaign_entry_ok, 2U);
    EXPECT_EQ(healthOf(plm, first).next_probe_ns, clock.now() + kIntervalNs);

    // A newer fault while RECOVERING isolates again and opens a new campaign.
    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    EXPECT_EQ(healthOf(plm, first).health, LinkHealth::kIsolated);
    EXPECT_EQ(healthOf(plm, first).fault_epoch, 2U);
    EXPECT_EQ(healthOf(plm, second).health, LinkHealth::kRecovering);
    EXPECT_EQ(plm.campaignView().size(), 1U);
    EXPECT_EQ(plm.stats().campaign_created, 2U);
}

TEST(UbProbeCampaignTest, MergeRequeuesNonInflightEntriesAtNewEpoch) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0),
                                                    makeEdgeSupply(0, 10, 1)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion first{ProbePathKey{0, 10, 0}, 1, 1};
    const PathVersion second{ProbePathKey{0, 10, 1}, 1, 1};

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    auto in_flight = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(in_flight.has_value());
    ASSERT_EQ(in_flight->record->key.version, first);
    ASSERT_EQ(in_flight->record->key.campaign_epoch, 1U);

    // A second fault on another path of the same peer merges the campaign. The
    // in-flight verification keeps its old epoch and is skipped; queued entries
    // move to the new epoch (design §7.3).
    plm.onBusinessPathError(makeEvidence(0, 10, 1),
                            BusinessErrorKind::kPathFault, clock.now());
    auto campaigns = plm.campaignView();
    ASSERT_EQ(campaigns.size(), 1U);
    EXPECT_EQ(campaigns[0].epoch, 2U);
    EXPECT_EQ(campaignEntryOf(campaigns[0], first)->state,
              EntryState::kInflight);
    EXPECT_EQ(campaignEntryOf(campaigns[0], first)->entry_epoch, 1U);
    EXPECT_EQ(campaignEntryOf(campaigns[0], second)->state,
              EntryState::kQueued);
    EXPECT_EQ(campaignEntryOf(campaigns[0], second)->entry_epoch, 2U);

    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(in_flight->token), clock.now()));
    EXPECT_EQ(healthOf(plm, first).health, LinkHealth::kHealthy);
    EXPECT_EQ(plm.stats().campaign_entry_ok, 1U);
    // The requeued entry still holds the campaign open.
    EXPECT_EQ(plm.campaignView().size(), 1U);

    auto second_probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(second_probe.has_value());
    ASSERT_EQ(second_probe->record->key.version, second);
    ASSERT_EQ(second_probe->record->key.campaign_epoch, 2U);
    ASSERT_TRUE(plm.onProbeCompletion(makeCompletion(second_probe->token),
                                      clock.now()));
    EXPECT_EQ(healthOf(plm, second).health, LinkHealth::kHealthy);
    EXPECT_TRUE(plm.campaignView().empty());
    EXPECT_EQ(plm.stats().campaign_entry_ok, 2U);
    EXPECT_EQ(plm.stats().campaign_exited, 1U);

    // A new business fault after convergence opens a new campaign; the old,
    // already-terminated campaign cannot be revived.
    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    auto renewed = plm.campaignView();
    ASSERT_EQ(renewed.size(), 1U);
    EXPECT_GT(renewed[0].id, 1U);
    EXPECT_EQ(renewed[0].epoch, 1U);
    EXPECT_EQ(plm.stats().campaign_created, 2U);
}

TEST(UbProbeCampaignTest, RemovedEdgeCancelsQueuedEntry) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0),
                                                    makeEdgeSupply(0, 10, 1)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion first{ProbePathKey{0, 10, 0}, 1, 1};
    const PathVersion second{ProbePathKey{0, 10, 1}, 1, 1};
    EventDriver events(plm);

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    ASSERT_EQ(plm.campaignView()[0].entries.size(), 2U);

    ProbeEvent removed;
    removed.kind = ProbeEventKind::kEdgeRemoved;
    removed.version = second;
    ASSERT_TRUE(events.send(removed).ok());
    EXPECT_FALSE(hasHealth(plm, second));
    EXPECT_EQ(plm.stats().campaign_entry_canceled, 1U);

    auto campaigns = plm.campaignView();
    ASSERT_EQ(campaigns.size(), 1U);
    // Terminal entries are retained for accounting until the campaign exits,
    // so the removed version is still visible as canceled.
    ASSERT_EQ(campaigns[0].entries.size(), 2U);
    EXPECT_EQ(campaignEntryOf(campaigns[0], second)->state,
              EntryState::kCanceled);
    EXPECT_EQ(campaignEntryOf(campaigns[0], first)->state, EntryState::kQueued);

    uint64_t token_seq = 0;
    auto probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());
    EXPECT_EQ(probe->record->key.version, first);
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(probe->token), clock.now()));
    EXPECT_TRUE(plm.campaignView().empty());
    EXPECT_EQ(healthOf(plm, first).health, LinkHealth::kHealthy);
}

TEST(UbProbeCampaignTest, TargetEpochChangeReplacesVersionAndStalesInflight) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion old_version{ProbePathKey{0, 10, 0}, 1, 1};
    const PathVersion new_version{ProbePathKey{0, 10, 0}, 1, 2};
    EventDriver events(plm);

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    auto old_probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(old_probe.has_value());

    // target_epoch is part of the primary key: the old row is deleted by its
    // (path, topology_epoch) prefix and the event only carries the new version.
    ProbeEvent changed;
    changed.kind = ProbeEventKind::kTargetEpochChanged;
    changed.edge = makeEdgeSupply(0, 10, 0, 1, 2, 1);
    ASSERT_TRUE(events.send(changed).ok());

    EXPECT_FALSE(hasHealth(plm, old_version));
    ASSERT_TRUE(hasHealth(plm, new_version));
    EXPECT_EQ(healthOf(plm, new_version).health,
              LinkHealth::kEligibleUnverified);
    // The old campaign entry was canceled, so the campaign converged even
    // though its in-flight record is still outstanding.
    EXPECT_TRUE(plm.campaignView().empty());
    EXPECT_EQ(plm.inflightCount(), 1U);
    EXPECT_EQ(plm.stats().campaign_entry_canceled, 1U);

    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(old_probe->token), clock.now()));
    EXPECT_EQ(plm.stats().probe_stale, 1U);
    EXPECT_EQ(plm.inflightCount(), 0U);
    EXPECT_EQ(healthOf(plm, new_version).health,
              LinkHealth::kEligibleUnverified);
    // A fresh version starts with no periodic task.
    EXPECT_FALSE(postNext(plm, token_seq, clock.now()).has_value());
}

TEST(UbProbeCampaignTest, PostRejectionRollsEntryBackToQueued) {
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

    plm.onPostResult(probe->record, ProbePostOutcome::kRejected, clock.now());
    auto health = healthOf(plm, version);
    EXPECT_EQ(health.health, LinkHealth::kIsolated);
    EXPECT_EQ(health.fault_epoch, 1U);
    // A rejected post closes the campaign entry instead of requeueing it.
    EXPECT_EQ(health.task, TaskState::kIdle);
    EXPECT_EQ(health.inflight_seq, 0U);
    EXPECT_EQ(health.next_probe_ns, clock.now() + 100ULL * 1000 * 1000);
    EXPECT_EQ(plm.inflightCount(), 0U);
    EXPECT_EQ(plm.stats().probe_post_rejected, 1U);
    // Not accepted is not a probe: no completion may follow.
    EXPECT_EQ(plm.stats().probe_scheduled, 1U);
    EXPECT_EQ(plm.stats().probe_posted, 0U);
    EXPECT_TRUE(plm.campaignView().empty());
    const uint64_t seq_after_rejection = health.probe_seq;
    EXPECT_TRUE(plm.pendingActions(clock.now()).empty());

    // The next campaign is driven by a new event after the interval, not by
    // an immediate rejected-post retry.
    clock.advance(kIntervalNs);
    EXPECT_EQ(healthOf(plm, version).probe_seq, seq_after_rejection);
    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    auto retry = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(retry.has_value());
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(retry->token), clock.now()));
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kHealthy);
    EXPECT_TRUE(plm.campaignView().empty());
}

TEST(UbProbeCampaignTest, RejectionAfterCompletionOnlyRetiresLane) {
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
    auto probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());
    plm.onPostResult(probe->record, ProbePostOutcome::kAccepted, clock.now());
    // The real completion wins; then the provider contradicts itself and says
    // the WR was never accepted. Health must not be rolled back (design §8.6).
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(probe->token), clock.now()));
    ASSERT_EQ(healthOf(plm, version).health, LinkHealth::kHealthy);
    EXPECT_EQ(plm.stats().probe_posted, 1U);

    plm.onPostResult(probe->record, ProbePostOutcome::kRejected, clock.now());
    EXPECT_EQ(plm.stats().probe_post_conflict, 1U);
    EXPECT_EQ(plm.stats().probe_post_rejected, 0U);
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kHealthy);
    EXPECT_TRUE(healthOf(plm, version).pending);
    ASSERT_EQ(reports.retire_calls.size(), 1U);
    EXPECT_EQ(plm.inflightCount(), 0U);
    EXPECT_EQ(plm.stats().probe_slot_anomaly, 0U);
}

TEST(UbProbeCampaignTest, InterfaceEventContractIsIdempotentAndGapSafe) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());

    ProbeEvent gate;
    gate.kind = ProbeEventKind::kGateChanged;
    gate.local_nic = 0;
    gate.graph_epoch = 1;
    gate.event_seq = 1;
    gate.gate = GateBits{true, true, true};
    ASSERT_TRUE(plm.onPoolAvailabilityChange(gate).ok());
    // Same key again: acknowledged, not re-applied.
    ASSERT_TRUE(plm.onPoolAvailabilityChange(gate).ok());
    EXPECT_EQ(plm.stats().probe_event_duplicate, 1U);

    // Sequence 3 is a gap while 2 is outstanding: rejected, key not advanced.
    gate.event_seq = 3;
    EXPECT_FALSE(plm.onPoolAvailabilityChange(gate).ok());
    EXPECT_EQ(plm.stats().probe_event_gap, 1U);

    gate.event_seq = 2;
    ASSERT_TRUE(plm.onPoolAvailabilityChange(gate).ok());

    // Malformed payload at the expected key: rejected and not applied.
    ProbeEvent malformed;
    malformed.kind = ProbeEventKind::kEdgeAdded;
    malformed.graph_epoch = 1;
    malformed.event_seq = 3;
    EXPECT_FALSE(plm.onPoolAvailabilityChange(malformed).ok());
    EXPECT_EQ(plm.stats().probe_event_malformed, 1U);
    EXPECT_FALSE(plm.onPoolAvailabilityChange(malformed).ok());

    // Because rejection does not advance the key, the corrected event with the
    // same key is accepted and takes effect.
    const PathVersion added{ProbePathKey{0, 10, 1}, 1, 1};
    ProbeEvent corrected;
    corrected.kind = ProbeEventKind::kEdgeAdded;
    corrected.graph_epoch = 1;
    corrected.event_seq = 3;
    corrected.edge = makeEdgeSupply(0, 10, 1);
    ASSERT_TRUE(plm.onPoolAvailabilityChange(corrected).ok());
    ASSERT_TRUE(hasHealth(plm, added));
    EXPECT_EQ(healthOf(plm, added).health, LinkHealth::kEligibleUnverified);

    // A new graph epoch must start at sequence 1; gaps are still rejected.
    ProbeEvent next_epoch;
    next_epoch.kind = ProbeEventKind::kGateChanged;
    next_epoch.local_nic = 0;
    next_epoch.graph_epoch = 2;
    next_epoch.event_seq = 2;
    next_epoch.gate = GateBits{true, true, true};
    EXPECT_FALSE(plm.onPoolAvailabilityChange(next_epoch).ok());
    next_epoch.event_seq = 1;
    ASSERT_TRUE(plm.onPoolAvailabilityChange(next_epoch).ok());
    EXPECT_EQ(plm.graphEpoch(), 2U);

    // An event from the superseded epoch is acknowledged but not applied.
    gate.graph_epoch = 1;
    gate.event_seq = 3;
    ASSERT_TRUE(plm.onPoolAvailabilityChange(gate).ok());
    EXPECT_EQ(plm.stats().probe_event_stale_epoch, 1U);
    EXPECT_EQ(plm.graphEpoch(), 2U);
}

TEST(UbProbeCampaignTest, GenerationMismatchFallsBackForFaultsOnly) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto first_edge = makeEdgeSupply(0, 10, 0);
    first_edge.endpoint_generation = 7;
    auto second_edge = makeEdgeSupply(0, 20, 1);
    second_edge.endpoint_generation = 7;
    auto graph = makeGraph({makePortSupply(0, 1)}, {first_edge, second_edge});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion first{ProbePathKey{0, 10, 0}, 1, 1};
    const PathVersion second{ProbePathKey{0, 20, 1}, 1, 1};

    // A fault whose generation does not map is still attributed to the rail's
    // current version (L-7: never lose a fault), and it is counted.
    plm.onBusinessPathError(makeEvidence(0, 10, 0, /*endpoint_generation=*/9),
                            BusinessErrorKind::kPathFault, clock.now());
    EXPECT_EQ(healthOf(plm, first).health, LinkHealth::kIsolated);
    EXPECT_EQ(plm.stats().probe_evidence_unmapped, 1U);

    // A success with a mismatched generation must not promote a version it does
    // not belong to.
    plm.onBusinessPathSuccess(makeEvidence(0, 20, 1, 9), clock.now());
    EXPECT_EQ(healthOf(plm, second).health, LinkHealth::kEligibleUnverified);
    EXPECT_EQ(plm.stats().probe_evidence_unmapped, 2U);

    // Matching evidence is accepted normally.
    plm.onBusinessPathSuccess(makeEvidence(0, 20, 1, 7), clock.now());
    EXPECT_EQ(healthOf(plm, second).health, LinkHealth::kHealthy);
    EXPECT_EQ(plm.stats().probe_evidence_unmapped, 2U);
}

TEST(UbProbeCampaignTest, PortRevokedWaitsForReplacementWithoutReporting) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    RecordingReportSink reports;
    plm.setReportSink(&reports);
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};
    EventDriver events(plm);

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    ASSERT_FALSE(plm.pendingActions(clock.now()).empty());

    ProbeEvent revoked;
    revoked.kind = ProbeEventKind::kPortRevoked;
    revoked.local_nic = 0;
    ASSERT_TRUE(events.send(revoked).ok());
    auto health = healthOf(plm, version);
    EXPECT_TRUE(health.pending);
    EXPECT_EQ(health.pending_reason, PendingReason::kPortRevoked);
    EXPECT_EQ(health.health, LinkHealth::kIsolated);
    EXPECT_TRUE(plm.pendingActions(clock.now()).empty());
    // The queued campaign entry is blocked on resources, not on the scheduler.
    EXPECT_EQ(plm.stats().campaign_waiting_for_event, 1U);
    // The pooling side revoked on purpose: no interface-2 report.
    EXPECT_TRUE(reports.retire_calls.empty());

    ProbeEvent replaced;
    replaced.kind = ProbeEventKind::kProbeHandleReplaced;
    replaced.local_nic = 0;
    replaced.port = makePortSupply(0, 2);
    ASSERT_TRUE(events.send(replaced).ok());
    health = healthOf(plm, version);
    EXPECT_FALSE(health.pending);
    EXPECT_EQ(health.next_probe_ns, clock.now());
    EXPECT_FALSE(plm.pendingActions(clock.now()).empty());
    EXPECT_EQ(plm.stats().campaign_waiting_for_event, 0U);
}

TEST(UbProbeCampaignTest, RetiredHandleCompletionCannotPublishHealth) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};
    EventDriver events(plm);

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    auto probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());

    // Defensive injection for matrix #17: the handle is retired while the WR is
    // still outstanding, then a real CQE arrives for it. `handle_valid` must
    // keep the evidence from publishing health on the dead handle.
    ProbeEvent revoked;
    revoked.kind = ProbeEventKind::kPortRevoked;
    revoked.local_nic = 0;
    ASSERT_TRUE(events.send(revoked).ok());
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(probe->token), clock.now()));

    EXPECT_EQ(plm.stats().probe_stale, 1U);
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kIsolated);
    EXPECT_EQ(healthOf(plm, version).inflight_seq, 0U);
    EXPECT_EQ(plm.inflightCount(), 0U);
    EXPECT_TRUE(plm.campaignView().empty());
}

TEST(UbProbeCampaignTest, RoundRobinDoesNotStarveOtherPeers) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph =
        makeGraph({makePortSupply(0, 1)},
                  {makeEdgeSupply(0, 10, 0, 1, 1, 1, /*gates_ok=*/false),
                   makeEdgeSupply(0, 20, 1, 1, 1, 1, /*gates_ok=*/false)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion first{ProbePathKey{0, 10, 0}, 1, 1};
    const PathVersion second{ProbePathKey{0, 20, 1}, 1, 1};

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    plm.onBusinessPathError(makeEvidence(0, 20, 1),
                            BusinessErrorKind::kPathFault, clock.now());

    uint64_t token_seq = 0;
    auto first_probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(first_probe.has_value());
    EXPECT_EQ(first_probe->record->key.version, first);

    // One in-flight probe per port: the second peer waits for the slot.
    EXPECT_TRUE(plm.pendingActions(clock.now()).empty());
    clock.advance(1);
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(first_probe->token), clock.now()));
    auto second_probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(second_probe.has_value());
    EXPECT_EQ(second_probe->record->key.version, second);

    clock.advance(1);
    ASSERT_TRUE(plm.onProbeCompletion(makeCompletion(second_probe->token),
                                      clock.now()));
    // RECOVERING paths re-probe on their interval; the round-robin cursor
    // resumes at the first peer instead of pinning one path.
    clock.set(clock.now() + kIntervalNs);
    auto resumed = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(resumed.has_value());
    EXPECT_EQ(resumed->record->key.version, first);
}

}  // namespace
}  // namespace ub
}  // namespace tent
}  // namespace mooncake
