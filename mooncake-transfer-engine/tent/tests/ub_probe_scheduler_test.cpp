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
// ProbeScheduler and shutdown tests for the W1b exit matrix (design §14.2):
//   #4  per-port serialisation, ports in parallel, peer budget
//   #5  backoff growth/cap/reset
//   #6  completion timeout, port recovery through a new generation
//   #7  gate blocking (area epoch evidence)
//   #8  post result mapping (accepted / rejected / unknown)
//   #10 shutdown drain, handoff and idempotent stop
//   #16 orphan handoff on shutdown
//
// Virtual-clock tests drive ProbeLinkManager directly (timeout/backoff/drain
// transitions). Threaded tests exercise the scheduler with a real clock and a
// fake poster; only orchestration is real-time, so the timing choices stay
// coarse (interval 1s, timeout 500ms).

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <thread>
#include <vector>

#include "tent/transport/ub/probe_link_manager.h"
#include "tent/transport/ub/probe_scheduler.h"
#include "ub_probe_test_support.h"

namespace mooncake {
namespace tent {
namespace ub {
namespace {

using namespace probe_test;

constexpr uint64_t kIntervalNs = 1000ULL * 1000 * 1000;

PathVersion versionOf(Topology::NicID local_nic, SegmentID peer,
                      Topology::NicID remote_nic) {
    return PathVersion{ProbePathKey{local_nic, peer, remote_nic}, 1, 1};
}

TEST(UbProbeSchedulerTest, PostsCampaignProbeAndSettlesSuccess) {
    ProbeLinkManager plm(defaultParams());
    RecordingReportSink reports;
    plm.setReportSink(&reports);
    FakeProbePoster poster;
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    ProbeScheduler scheduler(plm, poster);
    scheduler.start();
    EXPECT_TRUE(scheduler.running());
    const auto version = versionOf(0, 10, 0);

    // Let the thread park on its deadline wait: the wake-up path must be
    // race-free, not just "the first loop iteration happens to see the work".
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault);
    ASSERT_TRUE(poster.waitForCalls(1));

    const auto call = poster.lastCall();
    EXPECT_TRUE(isProbeToken(call.token));
    EXPECT_EQ(call.jetty_generation, 1U);
    EXPECT_EQ(call.native_jetty_id, 100U);
    EXPECT_EQ(call.source_address, graph.ports[0].source_address);
    EXPECT_EQ(call.source_length, 1U);
    EXPECT_EQ(call.area_address, graph.edges[0].area_address);
    EXPECT_EQ(call.area_length, 1U);
    EXPECT_EQ(plm.inflightCount(), 1U);

    ASSERT_TRUE(plm.onProbeCompletion(makeCompletion(call.token)));
    ASSERT_TRUE(waitUntil(
        [&] { return healthOf(plm, version).health == LinkHealth::kHealthy; }));
    EXPECT_EQ(plm.stats().probe_posted, 1U);

    scheduler.notePostingStopped();
    const auto drain = plm.drainBeforePollStop();
    EXPECT_TRUE(drain.drained);
    EXPECT_EQ(drain.unproven, 0U);
    EXPECT_FALSE(scheduler.running());
    // Nothing posts after the posting side was closed.
    EXPECT_EQ(poster.callCount(), 1U);
}

TEST(UbProbeSchedulerTest, RejectionRetriesWithoutMarkingPathBad) {
    ProbeLinkManager plm(defaultParams());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const auto version = versionOf(0, 10, 0);
    FakeProbePoster poster(FakePostResult::kRejected);
    ProbeScheduler scheduler(plm, poster);
    scheduler.start();

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault);
    ASSERT_TRUE(poster.waitForCalls(1));
    ASSERT_TRUE(waitUntil([&] {
        return plm.stats().probe_post_rejected >= 1 && plm.inflightCount() == 0;
    }));
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kIsolated);
    EXPECT_EQ(healthOf(plm, version).fault_epoch, 1U);
    // Rejection closes the campaign entry; the task slot is released and the
    // rejected post must not be retried in a tight loop.
    EXPECT_EQ(healthOf(plm, version).task, TaskState::kIdle);
    EXPECT_TRUE(plm.campaignView().empty());
    const uint64_t seq_after_rejection = healthOf(plm, version).probe_seq;
    EXPECT_EQ(poster.callCount(), 1U);
    EXPECT_EQ(healthOf(plm, version).probe_seq, seq_after_rejection);

    // A later business event starts a new campaign and a fresh accepted post.
    poster.setResult(FakePostResult::kAccepted);
    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault);
    ASSERT_TRUE(poster.waitForCalls(2));
    ASSERT_TRUE(waitUntil([&] {
        return plm.inflightCount() == 1 && plm.stats().probe_posted >= 1;
    }));
    ASSERT_TRUE(plm.onProbeCompletion(makeCompletion(poster.lastCall().token)));
    ASSERT_TRUE(waitUntil(
        [&] { return healthOf(plm, version).health == LinkHealth::kHealthy; }));

    plm.stopPosting();
    scheduler.stop();
}

TEST(UbProbeSchedulerTest, UnknownPostIsTreatedAsAccepted) {
    ProbeLinkManager plm(defaultParams());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const auto version = versionOf(0, 10, 0);
    FakeProbePoster poster(FakePostResult::kUnknown);
    ProbeScheduler scheduler(plm, poster);
    scheduler.start();

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault);
    ASSERT_TRUE(poster.waitForCalls(1));
    ASSERT_TRUE(waitUntil([&] { return plm.stats().probe_post_unknown >= 1; }));
    EXPECT_EQ(plm.inflightCount(), 1U);

    // Unknown is never resent: no second post while the record waits.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(poster.callCount(), 1U);

    ASSERT_TRUE(plm.onProbeCompletion(makeCompletion(poster.lastCall().token)));
    ASSERT_TRUE(waitUntil(
        [&] { return healthOf(plm, version).health == LinkHealth::kHealthy; }));
    plm.stopPosting();
    scheduler.stop();
}

TEST(UbProbeSchedulerTest, PortsProbeInParallelButOneAtATime) {
    ProbeLinkManager plm(defaultParams());
    auto graph =
        makeGraph({makePortSupply(0, 1), makePortSupply(1, 1)},
                  {makeEdgeSupply(0, 10, 0), makeEdgeSupply(1, 20, 1)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const auto first = versionOf(0, 10, 0);
    const auto second = versionOf(1, 20, 1);
    FakeProbePoster poster;
    ProbeScheduler scheduler(plm, poster);
    scheduler.start();

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault);
    plm.onBusinessPathError(makeEvidence(1, 20, 1),
                            BusinessErrorKind::kPathFault);
    ASSERT_TRUE(poster.waitForCalls(2));

    const auto calls = poster.calls();
    ASSERT_EQ(calls.size(), 2U);
    EXPECT_NE(calls[0].native_jetty_id, calls[1].native_jetty_id);
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    // One in-flight probe per port; no third action can be claimed.
    EXPECT_EQ(poster.callCount(), 2U);
    EXPECT_EQ(plm.inflightCount(), 2U);

    for (const auto& call : calls) {
        ASSERT_TRUE(plm.onProbeCompletion(makeCompletion(call.token)));
    }
    ASSERT_TRUE(waitUntil([&] {
        return healthOf(plm, first).health == LinkHealth::kHealthy &&
               healthOf(plm, second).health == LinkHealth::kHealthy;
    }));
    plm.stopPosting();
    scheduler.stop();
}

TEST(UbProbeSchedulerTest, TimeoutRetiresLaneKeepsSlotAndRecoversOnNewHandle) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    RecordingReportSink reports;
    plm.setReportSink(&reports);
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const auto version = versionOf(0, 10, 0);

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    auto probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());

    clock.set(clock.now() + plm.params().timeoutNs());
    plm.scanTimeouts(clock.now());
    EXPECT_EQ(plm.stats().probe_timeout_unknown, 1U);
    EXPECT_EQ(plm.inflightCount(), 0U);
    EXPECT_EQ(plm.orphanCount(), 1U);
    ASSERT_EQ(reports.retire_calls.size(), 1U);
    auto health = healthOf(plm, version);
    EXPECT_EQ(health.health, LinkHealth::kIsolated);
    EXPECT_TRUE(health.pending);
    EXPECT_EQ(health.pending_reason, PendingReason::kLaneRetired);
    EXPECT_TRUE(plm.campaignView().empty());

    // A second scan must not double-process the same record.
    plm.scanTimeouts(clock.now());
    EXPECT_EQ(plm.stats().probe_timeout_unknown, 1U);
    EXPECT_EQ(reports.retire_calls.size(), 1U);

    // A late success for the timed-out probe is not evidence.
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(probe->token), clock.now()));
    EXPECT_EQ(plm.orphanCount(), 0U);
    EXPECT_EQ(plm.stats().probe_orphan_settled, 1U);
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kIsolated);

    // The new handle generation resets the slot and resumes probing.
    EventDriver events(plm);
    ProbeEvent replaced;
    replaced.kind = ProbeEventKind::kProbeHandleReplaced;
    replaced.local_nic = 0;
    replaced.port = makePortSupply(0, 2);
    ASSERT_TRUE(events.send(replaced).ok());
    EXPECT_FALSE(healthOf(plm, version).pending);
    EXPECT_EQ(healthOf(plm, version).next_probe_ns, clock.now());

    auto retry = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(retry.has_value());
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(retry->token), clock.now()));
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kHealthy);
}

TEST(UbProbeSchedulerTest, BackoffGrowsCapsAndResetsOnSuccess) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    RecordingReportSink reports;
    plm.setReportSink(&reports);
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const auto version = versionOf(0, 10, 0);
    EventDriver events(plm);
    uint64_t token_seq = 0;

    auto replace_handle = [&](uint64_t generation) {
        ProbeEvent event;
        event.kind = ProbeEventKind::kProbeHandleReplaced;
        event.local_nic = 0;
        event.port = makePortSupply(0, generation);
        EXPECT_TRUE(events.send(event).ok());
    };

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    EXPECT_EQ(healthOf(plm, version).next_probe_ns, clock.now());

    // interval = 1000ms, backoff_max = 4000ms.
    auto probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());
    ASSERT_TRUE(plm.onProbeCompletion(
        makeCompletion(probe->token, CompletionCategory::REMOTE_PATH_ERROR),
        clock.now()));
    auto health = healthOf(plm, version);
    EXPECT_EQ(health.consecutive_failures, 1U);
    EXPECT_EQ(health.next_probe_ns, clock.now() + kIntervalNs);
    EXPECT_EQ(reports.retire_calls.size(), 1U);

    replace_handle(2);
    clock.advance(kIntervalNs);
    probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());
    // An ACK timeout carries a completion; it is a path fault too, but is
    // counted separately.
    ASSERT_TRUE(plm.onProbeCompletion(
        makeCompletion(probe->token, CompletionCategory::TIMEOUT),
        clock.now()));
    health = healthOf(plm, version);
    EXPECT_EQ(health.consecutive_failures, 2U);
    EXPECT_EQ(health.next_probe_ns, clock.now() + 2 * kIntervalNs);
    EXPECT_EQ(plm.stats().probe_ack_timeout, 1U);

    replace_handle(3);
    clock.advance(2 * kIntervalNs);
    probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());
    ASSERT_TRUE(plm.onProbeCompletion(
        makeCompletion(probe->token, CompletionCategory::REMOTE_PATH_ERROR),
        clock.now()));
    health = healthOf(plm, version);
    EXPECT_EQ(health.consecutive_failures, 3U);
    EXPECT_EQ(health.next_probe_ns, clock.now() + 4 * kIntervalNs);

    // Fourth failure would be 8 * interval, but backoff_max caps it at 4s.
    replace_handle(4);
    clock.advance(4 * kIntervalNs);
    probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());
    ASSERT_TRUE(plm.onProbeCompletion(
        makeCompletion(probe->token, CompletionCategory::REMOTE_PATH_ERROR),
        clock.now()));
    health = healthOf(plm, version);
    EXPECT_EQ(health.consecutive_failures, 4U);
    EXPECT_EQ(health.next_probe_ns, clock.now() + 4 * kIntervalNs);
    EXPECT_EQ(reports.retire_calls.size(), 4U);

    // Success clears failures and backoff and stops the periodic task.
    replace_handle(5);
    clock.advance(4 * kIntervalNs);
    probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(probe->token), clock.now()));
    health = healthOf(plm, version);
    EXPECT_EQ(health.health, LinkHealth::kHealthy);
    EXPECT_EQ(health.consecutive_failures, 0U);
    EXPECT_EQ(health.next_probe_ns, 0U);
}

TEST(UbProbeSchedulerTest, StaleAreaEvidenceBlocksRecovery) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const auto version = versionOf(0, 10, 0);
    EventDriver events(plm);
    uint64_t token_seq = 0;

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    auto probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());

    // A new probe area replaces the page while the WR is outstanding, so the
    // old completion is stale evidence (gate #4).
    ProbeEvent area;
    area.kind = ProbeEventKind::kAreaReplaced;
    area.local_nic = 0;
    area.edge = makeEdgeSupply(0, 10, 0, 1, 1, /*area_epoch=*/2);
    ASSERT_TRUE(events.send(area).ok());
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(probe->token), clock.now()));

    EXPECT_EQ(plm.stats().probe_stale, 1U);
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kIsolated);
    // Stale campaign evidence must not make the path immediately due again.
    EXPECT_EQ(healthOf(plm, version).next_probe_ns, clock.now() + kIntervalNs);
    EXPECT_EQ(healthOf(plm, version).task, TaskState::kIdle);
    EXPECT_TRUE(plm.campaignView().empty());
    const uint64_t seq_after_stale = healthOf(plm, version).probe_seq;
    EXPECT_FALSE(postNext(plm, token_seq, clock.now()).has_value());
    EXPECT_EQ(healthOf(plm, version).probe_seq, seq_after_stale);

    clock.advance(kIntervalNs);
    probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());
    EXPECT_EQ(probe->record->key.area_epoch, 2U);
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(probe->token), clock.now()));
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kHealthy);
}

TEST(UbProbeSchedulerTest, NextDeadlineCoversInflightAndDelayedProbes) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph =
        makeGraph({makePortSupply(0, 1)},
                  {makeEdgeSupply(0, 10, 0, 1, 1, 1, /*gates_ok=*/false)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const auto version = versionOf(0, 10, 0);

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    auto probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());
    // While a record is in flight its deadline is the next wake-up.
    EXPECT_EQ(plm.nextDeadline(clock.now()), probe->record->deadline_ns);

    // A gate-blocked success leaves RECOVERING with a future periodic probe;
    // nextDeadline must still schedule that wake-up, not report "idle".
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(probe->token), clock.now()));
    ASSERT_EQ(healthOf(plm, version).health, LinkHealth::kRecovering);
    EXPECT_EQ(plm.nextDeadline(clock.now()), clock.now() + kIntervalNs);
}

TEST(UbProbeSchedulerTest, ShutdownDrainHandsOffUnprovenRecords) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    RecordingReportSink reports;
    plm.setReportSink(&reports);
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const auto version = versionOf(0, 10, 0);

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    auto probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());
    EXPECT_EQ(plm.inflightCount(), 1U);

    plm.stopPosting();
    EXPECT_TRUE(plm.pendingActions(clock.now()).empty());
    const auto result = plm.drainBeforePollStop(clock.now());
    EXPECT_EQ(result.settled, 0U);
    EXPECT_EQ(result.unproven, 1U);
    EXPECT_FALSE(result.drained);
    EXPECT_EQ(plm.inflightCount(), 0U);
    EXPECT_EQ(plm.orphanCount(), 0U);
    ASSERT_EQ(reports.handoffs.size(), 1U);
    const auto& handoff = reports.handoffs[0];
    EXPECT_EQ(handoff.token, probe->token);
    EXPECT_EQ(handoff.key.version, version);
    EXPECT_EQ(handoff.orphan_reason, OrphanReason::kShutdown);
    EXPECT_FALSE(handoff.timed_out);
    EXPECT_NE(handoff.source, nullptr);
    EXPECT_NE(handoff.area, nullptr);
    EXPECT_EQ(handoff.lane.local_nic, 0);
    EXPECT_EQ(handoff.lane.jetty_generation, 1U);
    EXPECT_EQ(plm.stats().probe_orphan_handed_off, 1U);
    EXPECT_EQ(plm.stats().probe_drain_unproven, 1U);

    // A completion after handoff has no local record any more; it is only an
    // unknown token and must not double-release anything.
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(probe->token), clock.now()));
    EXPECT_EQ(plm.stats().probe_unknown_token, 1U);

    // Shutdown accepts no new evidence: no campaign, no health change.
    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kIsolated);
    EXPECT_TRUE(plm.campaignView().empty());

    const auto second = plm.drainBeforePollStop(clock.now());
    EXPECT_TRUE(second.drained);
    EXPECT_EQ(second.unproven, 0U);
}

TEST(UbProbeSchedulerTest, ShutdownDrainIncludesExistingTimeouts) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    RecordingReportSink reports;
    plm.setReportSink(&reports);
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    auto probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());
    clock.set(clock.now() + plm.params().timeoutNs());
    plm.scanTimeouts(clock.now());
    ASSERT_EQ(plm.orphanCount(), 1U);

    plm.stopPosting();
    const auto result = plm.drainBeforePollStop(clock.now());
    EXPECT_EQ(result.unproven, 1U);
    ASSERT_EQ(reports.handoffs.size(), 1U);
    EXPECT_TRUE(reports.handoffs[0].timed_out);
    EXPECT_EQ(reports.handoffs[0].orphan_reason, OrphanReason::kTimeout);
    EXPECT_EQ(plm.orphanCount(), 0U);
}

TEST(UbProbeSchedulerTest, ShutdownDrainWaitsForCompletionBeforeHandoff) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    RecordingReportSink reports;
    plm.setReportSink(&reports);
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const auto version = versionOf(0, 10, 0);

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    auto probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());

    // The poller keeps running during the drain and settles the record.
    std::thread completer([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        const bool settled =
            plm.onProbeCompletion(makeCompletion(probe->token), clock.now());
        EXPECT_TRUE(settled);
    });
    const auto result = plm.drainBeforePollStop(clock.now() + 5'000'000'000ULL);
    completer.join();

    EXPECT_EQ(result.settled, 1U);
    EXPECT_EQ(result.unproven, 0U);
    EXPECT_TRUE(result.drained);
    EXPECT_TRUE(reports.handoffs.empty());
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kHealthy);
}

}  // namespace
}  // namespace ub
}  // namespace tent
}  // namespace mooncake
