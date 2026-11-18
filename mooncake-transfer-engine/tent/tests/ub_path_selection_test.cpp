// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// W3d unit tests: the PLM business routing gate (design §7.1/D-10) and the
// per-path in-flight ledger that replaces the endpoint counter as the scoring
// source (design §5.3/D-11).

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "tent/transport/ub/path_load_ledger.h"
#include "tent/transport/ub/probe_link_manager.h"
#include "tent/transport/ub/workers.h"
#include "ub_probe_test_support.h"

namespace mooncake::tent::ub {
namespace {

using namespace probe_test;

// ---------------------------------------------------------------------------
// §5.3 PathLoadLedger.
TEST(UbPathSelectionTest, LedgerBalancesPublishAndFinalize) {
    PathLoadLedger ledger;
    const UbPostPath path{0, 11, 1, 7};
    EXPECT_EQ(ledger.load(path).wrs, 0U);

    ledger.onPublish(path, 4096);
    ledger.onPublish(path, 8192);
    EXPECT_EQ(ledger.load(path).wrs, 2U);
    EXPECT_EQ(ledger.load(path).bytes, 12288U);
    EXPECT_EQ(ledger.peakWrs(), 2U);

    ledger.onFinalize(path, 4096);
    EXPECT_EQ(ledger.load(path).wrs, 1U);
    EXPECT_EQ(ledger.load(path).bytes, 8192U);
    ledger.onFinalize(path, 8192);
    // W3-IN-7: balanced again, and the peak is a high-water mark, not a level.
    EXPECT_EQ(ledger.load(path).wrs, 0U);
    EXPECT_EQ(ledger.load(path).bytes, 0U);
    EXPECT_EQ(ledger.publishedTotal(), 2U);
    EXPECT_EQ(ledger.finalizedTotal(), 2U);
    EXPECT_EQ(ledger.unmatchedFinalize(), 0U);
    EXPECT_EQ(ledger.peakWrs(), 2U);
}

// The endpoint generation participates in the key, so a replacement endpoint
// must neither inherit nor clear its predecessor's load (the exact failure mode
// of `endpoint->outstandingWrs()` this design replaces).
TEST(UbPathSelectionTest, LedgerKeysOnGeneration) {
    PathLoadLedger ledger;
    const UbPostPath old_path{0, 11, 1, 7};
    const UbPostPath new_path{0, 11, 1, 8};
    ledger.onPublish(old_path, 1024);
    ledger.onPublish(old_path, 1024);
    EXPECT_EQ(ledger.load(old_path).wrs, 2U);
    // A read never creates a row: the new generation simply has no load.
    EXPECT_EQ(ledger.load(new_path).wrs, 0U);
    EXPECT_EQ(ledger.pathCount(), 1U);

    ledger.onPublish(new_path, 512);
    EXPECT_EQ(ledger.pathCount(), 2U);
    EXPECT_EQ(ledger.load(old_path).wrs, 2U);
    EXPECT_EQ(ledger.load(new_path).wrs, 1U);

    // A finalize on a row that was never published is recorded, never applied
    // to another generation's counters.
    ledger.onFinalize(UbPostPath{0, 11, 1, 9}, 512);
    EXPECT_EQ(ledger.load(old_path).wrs, 2U);
    EXPECT_EQ(ledger.load(new_path).wrs, 1U);
    EXPECT_EQ(ledger.unmatchedFinalize(), 1U);
}

TEST(UbPathSelectionTest, LedgerDrivesTheScoreTieBreak) {
    const UbPostPath calm{0, 11, 1, 7};
    const UbPostPath busy{0, 11, 2, 7};
    PathLoadLedger ledger;
    for (int i = 0; i < 4; ++i) ledger.onPublish(busy, 4096);

    auto score_of = [&](const UbPostPath& path) {
        const auto load = ledger.load(path);
        UbPathSelectionScore score;
        score.quota_available = true;
        score.topology_rank = 0;
        score.normalized_inflight = 0.5;
        score.normalized_quota_wrs = 0.5;
        score.path_inflight_wrs = load.wrs;
        score.path_inflight_bytes = load.bytes;
        score.retry_order = 0;
        score.local_topology_id = path.local_topology_id;
        score.remote_device_id = path.remote_device_id;
        return score;
    };
    // Same topology and same quota pressure: only the ledger separates them,
    // and the loaded path must lose.
    EXPECT_TRUE(betterUbPathScore(score_of(calm), score_of(busy)));
    EXPECT_FALSE(betterUbPathScore(score_of(busy), score_of(calm)));
}

// ---------------------------------------------------------------------------
// §7.1 rule 1: health is a pure subtraction and an unknown path is usable.
TEST(UbPathSelectionTest, HealthGateBlocksOnlyExplicitlyUnusablePaths) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0),
                                                    makeEdgeSupply(0, 10, 1)});
    ASSERT_TRUE(plm.initialize(graph).ok());

    const UbPostPath verified{0, 10, 0, 5};
    // ELIGIBLE_UNVERIFIED is usable: W1 deliberately treats "not yet proven
    // bad" as usable, and so must selection (D-10).
    EXPECT_TRUE(plm.businessPathUsable(verified));
    EXPECT_EQ(plm.businessFaultEpoch(verified), 0U);

    // A path the probe graph does not model is still usable, and counted
    // separately: an incomplete adjacency configuration must never starve
    // business traffic (W3-IN-6 / RK8).
    const UbPostPath unknown_path{9, 99, 99, 5};
    EXPECT_TRUE(plm.businessPathUsable(unknown_path));
    EXPECT_EQ(plm.businessFaultEpoch(unknown_path), 0U);
    EXPECT_EQ(plm.stats().business_path_not_in_graph, 1U);

    // A real path fault isolates the path and bumps its fault epoch, which is
    // the claim anchor the admission path compares after the lease.
    plm.onBusinessPathError(verified, BusinessErrorKind::kPathFault,
                            clock.now());
    EXPECT_FALSE(plm.businessPathUsable(verified));
    EXPECT_EQ(plm.businessFaultEpoch(verified), 1U);
    EXPECT_EQ(plm.stats().business_path_health_blocked, 1U);

    // A second error on an already isolated path is idempotent: the epoch must
    // not keep advancing, or every claim snapshot would be stale forever.
    plm.onBusinessPathError(verified, BusinessErrorKind::kPathFault,
                            clock.now());
    EXPECT_EQ(plm.businessFaultEpoch(verified), 1U);
}

// The parameter-class business errors never isolate, so they must not change
// the routing verdict either.
TEST(UbPathSelectionTest, ParamErrorKeepsThePathUsable) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const UbPostPath path{0, 10, 0, 5};
    plm.onBusinessPathError(path, BusinessErrorKind::kParamError, clock.now());
    EXPECT_TRUE(plm.businessPathUsable(path));
    EXPECT_EQ(plm.businessFaultEpoch(path), 0U);
}

TEST(UbPathSelectionTest, ContextFailureDisablesEveryPathOnThePort) {
    TestClock clock;
    ProbeLinkManager plm(defaultParams(), clock.clock());
    auto graph = makeGraph({makePortSupply(0, 1)}, {makeEdgeSupply(0, 10, 0)});
    ASSERT_TRUE(plm.initialize(graph).ok());
    const UbPostPath path{0, 10, 0, 5};
    EXPECT_TRUE(plm.businessPathUsable(path));
    plm.onContextUnavailable(0, clock.now());
    EXPECT_FALSE(plm.businessPathUsable(path));
}

// Rail pause is no longer a selection input (§7.2): the score has no pause
// field at all and the two `continue`s that consulted it were removed from
// chooseAndResolveEndpoint(). A rail that keeps failing is now excluded by the
// PLM health gate (covered above) or loses on the ledger - there is no third
// mechanism left to test here.

// The ledger row is retired once it drains, so a long-lived process does not
// keep one row per (path, generation) forever (review minor).
TEST(UbPathSelectionTest, LedgerPrunesDrainedRows) {
    PathLoadLedger ledger;
    const UbPostPath path{0, 11, 1, 7};
    ledger.onPublish(path, 1024);
    EXPECT_EQ(ledger.pathCount(), 1U);
    ledger.onFinalize(path, 1024);
    EXPECT_EQ(ledger.pathCount(), 0U);
    EXPECT_EQ(ledger.load(path).wrs, 0U);
    EXPECT_EQ(ledger.unmatchedFinalize(), 0U);
    // The totals are cumulative observations, not row counts.
    EXPECT_EQ(ledger.publishedTotal(), 1U);
    EXPECT_EQ(ledger.finalizedTotal(), 1U);
    // A publish after the prune simply creates a fresh row.
    ledger.onPublish(path, 512);
    EXPECT_EQ(ledger.pathCount(), 1U);
    EXPECT_EQ(ledger.load(path).wrs, 1U);
    EXPECT_EQ(ledger.peakWrs(), 1U);
}

// W3 review B4: publish/finalize update the row under the map lock. When the
// counters were updated outside it, a concurrent finalize could observe zero,
// erase the row and strand a publish increment on an orphan slot - unmatched
// finalizes plus a permanently poisoned imbalance (W3-IN-7 broken).
TEST(UbPathSelectionTest, LedgerStaysBalancedUnderConcurrentPublishFinalize) {
    PathLoadLedger ledger;
    const UbPostPath path{0, 11, 1, 7};
    constexpr int kThreads = 8;
    constexpr int kPerThread = 20000;
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&ledger, &path] {
            for (int j = 0; j < kPerThread; ++j) {
                ledger.onPublish(path, 64);
                ledger.onFinalize(path, 64);
            }
        });
    }
    for (auto& thread : threads) thread.join();

    EXPECT_EQ(ledger.publishedTotal(), ledger.finalizedTotal());
    EXPECT_EQ(ledger.unmatchedFinalize(), 0U);
    EXPECT_EQ(ledger.load(path).wrs, 0U);
    EXPECT_EQ(ledger.load(path).bytes, 0U);
    EXPECT_EQ(ledger.pathCount(), 0U);
    EXPECT_GE(ledger.peakWrs(), 1U);
}

}  // namespace
}  // namespace mooncake::tent::ub
