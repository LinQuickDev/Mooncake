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
// P1 tests for the standalone probe resource provider: local resource
// creation/release, dedicated vs shared JFC ownership, rollback on invalid
// peer edges, and the P1 handoff/retire recording behavior.

#include <gtest/gtest.h>
#include <unistd.h>

#include <memory>
#include <vector>

#include "tent/transport/ub/probe_provider_standalone.h"
#include "ub_fake_urma.h"

namespace mooncake {
namespace tent {
namespace ub {
namespace {

using namespace fake;

ProbeParams makeProbeParams() {
    ProbeParams params;
    params.enabled = true;
    params.jfc_depth = 64;
    return params;
}

class StubPeerDirectory final : public ProbePeerDirectory {
   public:
    explicit StubPeerDirectory(std::vector<ProbeEdgeSupply> edges)
        : edges_(std::move(edges)) {}

    Status snapshotEdges(std::vector<ProbeEdgeSupply>& edges) override {
        edges = edges_;
        return Status::OK();
    }

   private:
    std::vector<ProbeEdgeSupply> edges_;
};

ProbeEdgeSupply makeEdge(Topology::NicID local_nic, SegmentID peer_id,
                         Topology::NicID remote_nic) {
    ProbeEdgeSupply edge;
    edge.version =
        PathVersion{ProbePathKey{local_nic, peer_id, remote_nic}, 1, 1};
    edge.peer = ProbePeerKey{peer_id, "peer"};
    edge.area_segment = std::make_shared<FakeRemoteSegment>(
        0xA00000, 4096, makeDescriptor(0xA00000, 4096));
    edge.area_address = 0xA00000;
    edge.area_length = 1;
    edge.area_epoch = 1;
    edge.remote_jetty = RemoteJettyInfo{"eid", 401, 501, 0xACFE};
    edge.node_gate_ok = true;
    edge.port_gate_ok = true;
    edge.profile_gate_ok = true;
    return edge;
}

TEST(UbProbeStandaloneProviderTest, DedicatedSnapshotCreatesAndReleases) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    ContextPtr context;
    ASSERT_TRUE(adapter->openContext(fakeDevice(), context).ok());

    StubPeerDirectory directory({makeEdge(0, 42, 0)});
    auto params = makeProbeParams();
    StandaloneProbeResourceProvider provider(
        adapter, params, {ProbeLocalPortConfig{0, context, nullptr, 1}},
        &directory);

    ProbeGraph graph;
    ASSERT_TRUE(provider.snapshot(graph).ok());
    EXPECT_TRUE(provider.initialized());
    EXPECT_EQ(graph.graph_epoch, 1U);
    ASSERT_EQ(graph.ports.size(), 1U);
    ASSERT_EQ(graph.edges.size(), 1U);

    const auto& port = graph.ports[0];
    EXPECT_EQ(port.local_nic, 0);
    EXPECT_EQ(port.jetty_generation, 1U);
    EXPECT_NE(port.probe_jetty, nullptr);
    EXPECT_NE(port.probe_jfc, nullptr);
    EXPECT_NE(port.source_segment, nullptr);
    EXPECT_EQ(port.source_length, 1U);
    EXPECT_EQ(port.source_generation, 1U);
    EXPECT_EQ(port.control_credit, 1U);
    EXPECT_EQ(port.peer_budget, params.peer_budget);
    EXPECT_EQ(adapter->liveJfcCount(), 1U);
    EXPECT_EQ(adapter->liveJettyCount(), 1U);
    EXPECT_EQ(adapter->lastJettyTransportMode(), JettyTransportMode::kRm);
    // Source page plus the local probe area page peers write into.
    EXPECT_EQ(adapter->liveLocalSegmentCount(), 2U);

    const auto areas = provider.localAreaViews();
    ASSERT_EQ(areas.size(), 1U);
    EXPECT_EQ(areas[0].local_nic, 0);
    EXPECT_EQ(areas[0].generation, 1U);
    EXPECT_EQ(areas[0].length % static_cast<uint64_t>(::getpagesize()), 0U);
    EXPECT_EQ(areas[0].address % static_cast<uint64_t>(::getpagesize()), 0U);
    EXPECT_EQ(areas[0].token, 0xACFEU);
    EXPECT_EQ(areas[0].epoch, 1U);
    EXPECT_FALSE(areas[0].descriptor.hex.empty());

    const auto views = provider.portViews();
    ASSERT_EQ(views.size(), 1U);
    EXPECT_EQ(views[0].local_nic, 0);
    EXPECT_EQ(views[0].generation, 1U);
    EXPECT_EQ(views[0].native_jetty_id, port.probe_jetty->id());
    EXPECT_EQ(views[0].source_address, port.source_address);
    EXPECT_EQ(views[0].source_generation, 1U);
    EXPECT_TRUE(views[0].owns_jfc);

    ProbeGraph second;
    EXPECT_TRUE(provider.snapshot(second).IsInvalidArgument());

    ASSERT_TRUE(provider.shutdown().ok());
    ASSERT_TRUE(provider.shutdown().ok());
    EXPECT_TRUE(provider.portViews().empty());
    EXPECT_FALSE(provider.initialized());
    EXPECT_EQ(adapter->liveJfcCount(), 0U);
    EXPECT_EQ(adapter->liveJettyCount(), 0U);
    EXPECT_EQ(adapter->liveLocalSegmentCount(), 0U);

    EXPECT_TRUE(adapter->closeContext(context).ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeStandaloneProviderTest, SharedModeBorrowsBusinessJfc) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    ContextPtr context;
    ASSERT_TRUE(adapter->openContext(fakeDevice(), context).ok());

    JfcPtr business_jfc;
    ASSERT_TRUE(adapter->createJfc(context, JfcOptions{}, business_jfc).ok());
    ASSERT_EQ(adapter->liveJfcCount(), 1U);

    auto params = makeProbeParams();
    params.jfc_mode = ProbeJfcMode::kShared;
    StubPeerDirectory directory({makeEdge(0, 42, 0)});
    StandaloneProbeResourceProvider provider(
        adapter, params, {ProbeLocalPortConfig{0, context, business_jfc, 1}},
        &directory);

    ProbeGraph graph;
    ASSERT_TRUE(provider.snapshot(graph).ok());
    ASSERT_EQ(graph.ports.size(), 1U);
    EXPECT_EQ(graph.ports[0].probe_jfc, nullptr);
    EXPECT_EQ(adapter->liveJfcCount(), 1U);
    EXPECT_EQ(adapter->liveJettyCount(), 1U);
    EXPECT_EQ(adapter->liveLocalSegmentCount(), 2U);

    ASSERT_TRUE(provider.shutdown().ok());
    // The business JFC is borrowed: the provider must not delete it.
    EXPECT_EQ(adapter->liveJfcCount(), 1U);
    EXPECT_EQ(adapter->liveJettyCount(), 0U);
    EXPECT_EQ(adapter->liveLocalSegmentCount(), 0U);

    ASSERT_TRUE(adapter->deleteJfc(business_jfc).ok());
    EXPECT_EQ(adapter->liveJfcCount(), 0U);
    EXPECT_TRUE(adapter->closeContext(context).ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeStandaloneProviderTest, InvalidEdgeRollsBackCreatedResources) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    ContextPtr context;
    ASSERT_TRUE(adapter->openContext(fakeDevice(), context).ok());

    // Edge references local port 99, which is not configured.
    StubPeerDirectory directory({makeEdge(99, 42, 0)});
    StandaloneProbeResourceProvider provider(
        adapter, makeProbeParams(),
        {ProbeLocalPortConfig{0, context, nullptr, 1}}, &directory);

    ProbeGraph graph;
    const auto status = provider.snapshot(graph);
    EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
    EXPECT_FALSE(provider.initialized());
    EXPECT_TRUE(provider.portViews().empty());
    EXPECT_EQ(adapter->liveJfcCount(), 0U);
    EXPECT_EQ(adapter->liveJettyCount(), 0U);
    EXPECT_EQ(adapter->liveLocalSegmentCount(), 0U);

    ASSERT_TRUE(provider.shutdown().ok());
    EXPECT_TRUE(adapter->closeContext(context).ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeStandaloneProviderTest, RejectsMissingOrDuplicateLocalPorts) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    ContextPtr context;
    ASSERT_TRUE(adapter->openContext(fakeDevice(), context).ok());

    StubPeerDirectory directory({makeEdge(0, 42, 0)});
    {
        StandaloneProbeResourceProvider empty(adapter, makeProbeParams(), {},
                                              &directory);
        ProbeGraph graph;
        EXPECT_TRUE(empty.snapshot(graph).IsInvalidArgument());
    }
    {
        StandaloneProbeResourceProvider duplicate(
            adapter, makeProbeParams(),
            {ProbeLocalPortConfig{0, context, nullptr, 1},
             ProbeLocalPortConfig{0, context, nullptr, 1}},
            &directory);
        ProbeGraph graph;
        EXPECT_TRUE(duplicate.snapshot(graph).IsInvalidArgument());
    }
    {
        StandaloneProbeResourceProvider null_context(
            adapter, makeProbeParams(),
            {ProbeLocalPortConfig{0, nullptr, nullptr, 1}}, &directory);
        ProbeGraph graph;
        EXPECT_TRUE(null_context.snapshot(graph).IsInvalidArgument());
    }
    EXPECT_EQ(adapter->liveJfcCount(), 0U);
    EXPECT_EQ(adapter->liveJettyCount(), 0U);
    EXPECT_TRUE(adapter->closeContext(context).ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeStandaloneProviderTest, RecordsRetireAndHandoffUntilShutdown) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    ContextPtr context;
    ASSERT_TRUE(adapter->openContext(fakeDevice(), context).ok());

    StubPeerDirectory directory({makeEdge(0, 42, 0)});
    StandaloneProbeResourceProvider provider(
        adapter, makeProbeParams(),
        {ProbeLocalPortConfig{0, context, nullptr, 1}}, &directory);
    ProbeGraph graph;
    ASSERT_TRUE(provider.snapshot(graph).ok());

    const PathVersion version{ProbePathKey{0, 42, 0}, 1, 1};
    provider.retireProbeLane(LaneKey{0, 1234, 1}, 1, 7, version,
                             Status::InternalError("injected retire"));
    EXPECT_EQ(provider.retireCalls(), 1U);
    // P1 does not rebuild the lane; the resources remain until shutdown.
    EXPECT_EQ(adapter->liveJettyCount(), 1U);

    ProbeHandoff handoff;
    handoff.token = makeProbeToken(1);
    handoff.lane = LaneKey{0, 1234, 1};
    handoff.key.version = version;
    handoff.orphan_reason = OrphanReason::kTimeout;
    handoff.source = std::make_shared<FakeLocalSegment>(0xB00000, 4096);
    handoff.area = std::make_shared<FakeRemoteSegment>(
        0xA00000, 4096, makeDescriptor(0xA00000, 4096));
    provider.handoffUnproven(handoff);
    EXPECT_EQ(provider.quarantineCount(), 1U);

    ASSERT_TRUE(provider.shutdown().ok());
    EXPECT_EQ(provider.quarantineCount(), 0U);
    EXPECT_EQ(adapter->liveJfcCount(), 0U);
    EXPECT_EQ(adapter->liveJettyCount(), 0U);
    EXPECT_EQ(adapter->liveLocalSegmentCount(), 0U);

    EXPECT_TRUE(adapter->closeContext(context).ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

}  // namespace
}  // namespace ub
}  // namespace tent
}  // namespace mooncake
