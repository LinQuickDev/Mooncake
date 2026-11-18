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
// Test-mode (probe/test/periodic_all) scheduling and settlement: all links
// are probed on a fixed cadence, campaign/periodic work keeps priority, and
// apply_health=false is a pure link check.

#include <gtest/gtest.h>

#include <algorithm>
#include <vector>

#include "tent/transport/ub/probe_link_manager.h"
#include "ub_probe_test_support.h"

namespace mooncake {
namespace tent {
namespace ub {
namespace {

using namespace probe_test;

constexpr uint64_t kTestIntervalNs = 5ULL * 1000 * 1000 * 1000;

ProbeParams testParams(bool apply_health = true) {
    auto params = defaultParams();
    params.test_periodic_all = true;
    params.test_interval_ms = 5;
    params.test_apply_health = apply_health;
    return params;
}

TEST(UbProbeTestModeTest, ProbesHealthyPathsOnFixedCadence) {
    TestClock clock;
    ProbeLinkManager plm(testParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};
    plm.onBusinessPathSuccess(makeEvidence(0, 10, 0), clock.now());
    ASSERT_EQ(healthOf(plm, version).health, LinkHealth::kHealthy);

    uint64_t token_seq = 0;
    auto first = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(plm.stats().probe_test_post, 1U);
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(first->token), clock.now()));
    EXPECT_EQ(plm.stats().probe_test_ok, 1U);
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kHealthy);

    // The next sweep is not due yet, and HEALTHY paths have no periodic task.
    EXPECT_TRUE(plm.pendingActions(clock.now()).empty());

    clock.advance(kTestIntervalNs);
    const auto actions = plm.pendingActions(clock.now());
    ASSERT_EQ(actions.size(), 1U);
    EXPECT_TRUE(actions[0].test);
    EXPECT_EQ(actions[0].version, version);
}

TEST(UbProbeTestModeTest, CampaignKeepsPriorityOverTestSweep) {
    TestClock clock;
    ProbeLinkManager plm(testParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    const auto actions = plm.pendingActions(clock.now());
    ASSERT_EQ(actions.size(), 1U);
    EXPECT_FALSE(actions[0].test);
}

TEST(UbProbeTestModeTest, ApplyHealthFalseNeverChangesHealthOrRetiresLane) {
    TestClock clock;
    ProbeLinkManager plm(testParams(/*apply_health=*/false), clock.clock());
    RecordingReportSink reports;
    plm.setReportSink(&reports);
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};

    uint64_t token_seq = 0;
    auto probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());
    ASSERT_TRUE(plm.onProbeCompletion(
        makeCompletion(probe->token, CompletionCategory::REMOTE_PATH_ERROR),
        clock.now()));

    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kEligibleUnverified);
    EXPECT_EQ(plm.stats().probe_test_fail, 1U);
    EXPECT_EQ(plm.stats().probe_failure_path, 0U);
    EXPECT_TRUE(reports.retire_calls.empty());

    clock.advance(kTestIntervalNs);
    auto next = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(next.has_value());
    EXPECT_EQ(plm.stats().probe_test_post, 2U);
}

TEST(UbProbeTestModeTest, RejectedTestPostDoesNotIsolate) {
    TestClock clock;
    ProbeLinkManager plm(testParams(), clock.clock());
    RecordingReportSink reports;
    plm.setReportSink(&reports);
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};

    uint64_t token_seq = 0;
    auto probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());
    plm.onPostResult(probe->record, ProbePostOutcome::kRejected, clock.now());

    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kEligibleUnverified);
    EXPECT_EQ(plm.stats().probe_post_rejected, 1U);
    EXPECT_TRUE(reports.retire_calls.empty());
    // Rejection advances the test cadence rather than retrying immediately.
    EXPECT_TRUE(plm.pendingActions(clock.now()).empty());
    clock.advance(kTestIntervalNs);
    EXPECT_FALSE(plm.pendingActions(clock.now()).empty());
}

TEST(UbProbeTestModeTest, MemoryErrorIsolatesWithoutLaneRebuild) {
    TestClock clock;
    ProbeLinkManager plm(testParams(), clock.clock());
    RecordingReportSink reports;
    plm.setReportSink(&reports);
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};

    plm.onBusinessPathSuccess(makeEvidence(0, 10, 0), clock.now());
    ASSERT_EQ(healthOf(plm, version).health, LinkHealth::kHealthy);

    uint64_t token_seq = 0;
    clock.advance(kTestIntervalNs);
    auto probe = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(probe.has_value());
    ASSERT_TRUE(plm.onProbeCompletion(
        makeCompletion(probe->token, CompletionCategory::MEMORY_ERROR),
        clock.now()));
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kIsolated);
    EXPECT_EQ(plm.stats().probe_area_config_error, 1U);
    EXPECT_EQ(plm.stats().probe_test_fail, 1U);
    EXPECT_TRUE(reports.retire_calls.empty());

    clock.advance(kTestIntervalNs);
    auto recovered = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(recovered.has_value());
    ASSERT_TRUE(
        plm.onProbeCompletion(makeCompletion(recovered->token), clock.now()));
    EXPECT_EQ(healthOf(plm, version).health, LinkHealth::kHealthy);
    EXPECT_TRUE(reports.retire_calls.empty());
}

TEST(UbProbeTestModeTest, CampaignMemoryErrorDoesNotRebuildLane) {
    TestClock clock;
    ProbeLinkManager plm(testParams(), clock.clock());
    RecordingReportSink reports;
    plm.setReportSink(&reports);
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};

    plm.onBusinessPathSuccess(makeEvidence(0, 10, 0), clock.now());
    ASSERT_EQ(healthOf(plm, version).health, LinkHealth::kHealthy);
    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());

    uint64_t token_seq = 0;
    auto campaign = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(campaign.has_value());
    ASSERT_TRUE(campaign->record != nullptr);
    EXPECT_FALSE(campaign->record->test_probe);
    ASSERT_TRUE(plm.onProbeCompletion(
        makeCompletion(campaign->token, CompletionCategory::MEMORY_ERROR),
        clock.now()));
    EXPECT_EQ(plm.stats().probe_area_config_error, 1U);
    EXPECT_TRUE(reports.retire_calls.empty());
    EXPECT_TRUE(std::ranges::none_of(
        plm.pendingActions(clock.now()),
        [](const ProbeAction& action) { return !action.test; }));
}

TEST(UbProbeTestModeTest, RejectedCampaignDoesNotSpinProbeSeq) {
    TestClock clock;
    ProbeLinkManager plm(testParams(), clock.clock());
    RecordingReportSink reports;
    plm.setReportSink(&reports);
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const PathVersion version{ProbePathKey{0, 10, 0}, 1, 1};

    plm.onBusinessPathError(makeEvidence(0, 10, 0),
                            BusinessErrorKind::kPathFault, clock.now());
    uint64_t token_seq = 0;
    auto campaign = postNext(plm, token_seq, clock.now());
    ASSERT_TRUE(campaign.has_value());
    const uint64_t seq_before = healthOf(plm, version).probe_seq;
    plm.onPostResult(campaign->record, ProbePostOutcome::kRejected,
                     clock.now());
    EXPECT_EQ(healthOf(plm, version).probe_seq, seq_before);
    EXPECT_TRUE(std::ranges::none_of(
        plm.pendingActions(clock.now()),
        [](const ProbeAction& action) { return !action.test; }));
    EXPECT_TRUE(reports.retire_calls.empty());
}

}  // namespace
}  // namespace ub
}  // namespace tent
}  // namespace mooncake
