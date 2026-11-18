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
// P4 tests: provider-side lane retire/rebuild, interface-7 event emission,
// quiesced completion dispatch and retired-page quarantine.

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "tent/transport/ub/probe_provider_standalone.h"
#include "ub_fake_urma.h"

namespace mooncake {
namespace tent {
namespace ub {
namespace {

using namespace fake;

class StaticDirectory final : public ProbePeerDirectory {
   public:
    explicit StaticDirectory(std::vector<ProbeEdgeSupply> edges)
        : edges_(std::move(edges)) {}

    Status snapshotEdges(std::vector<ProbeEdgeSupply>& edges) override {
        edges = edges_;
        return Status::OK();
    }

   private:
    std::vector<ProbeEdgeSupply> edges_;
};

class RecordingAvailabilitySink final : public ProbeAvailabilitySink {
   public:
    Status onPoolAvailabilityChange(const ProbeEvent& event) override {
        events.push_back(event);
        return Status::OK();
    }

    std::vector<ProbeEvent> events;
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

ProbeParams makeProbeParams() {
    ProbeParams params;
    params.enabled = true;
    params.jfc_depth = 64;
    return params;
}

struct Fixture {
    std::shared_ptr<FakeUrmaAdapter> adapter =
        std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ContextPtr context;
    StaticDirectory directory;

    Fixture() : directory({makeEdge(0, 42, 0)}) {}

    bool initialize() {
        if (!adapter->initialize().ok()) return false;
        return adapter->openContext(fakeDevice(), context).ok();
    }
};

TEST(UbProbeRebuildTest, RebuildsLaneAndEmitsHandleReplaced) {
    Fixture fixture;
    ASSERT_TRUE(fixture.initialize());
    StandaloneProbeResourceProvider provider(
        fixture.adapter, makeProbeParams(),
        {ProbeLocalPortConfig{0, fixture.context, nullptr, 1}},
        &fixture.directory);

    ProbeGraph graph;
    ASSERT_TRUE(provider.snapshot(graph).ok());
    ASSERT_EQ(provider.portViews().size(), 1U);
    const uint32_t old_jetty_id = provider.portViews()[0].native_jetty_id;
    const uint64_t old_generation = provider.portViews()[0].generation;

    RecordingAvailabilitySink sink;
    provider.setAvailabilitySink(&sink);
    std::vector<Completion> completions;
    provider.setCompletionSink([&](const Completion& completion) {
        completions.push_back(completion);
        return Status::OK();
    });

    const uint64_t token = makeProbeToken(3);
    fixture.adapter->pushPendingCompletion(graph.ports[0].probe_jetty, token);
    provider.retireProbeLane(LaneKey{0, old_jetty_id, 1}, old_generation, 7,
                             graph.edges[0].version,
                             Status::InternalError("injected lane error"));
    ASSERT_TRUE(provider.waitForRebuilds(2000));

    ASSERT_EQ(provider.portViews().size(), 1U);
    EXPECT_EQ(provider.portViews()[0].generation, old_generation + 1);
    EXPECT_NE(provider.portViews()[0].native_jetty_id, old_jetty_id);
    EXPECT_EQ(fixture.adapter->liveJettyCount(), 1U);
    EXPECT_EQ(fixture.adapter->liveJfcCount(), 1U);
    // Current source+area pages plus the retired source page. The area page
    // and probe Jetty identity stay stable across the rebuild.
    EXPECT_EQ(fixture.adapter->liveLocalSegmentCount(), 3U);
    EXPECT_EQ(provider.retiredPageCount(), 1U);
    EXPECT_EQ(provider.localAreaViews()[0].epoch, old_generation);

    ASSERT_EQ(completions.size(), 1U);
    EXPECT_EQ(completions[0].token, token);
    ASSERT_EQ(sink.events.size(), 1U);
    const auto& event = sink.events[0];
    EXPECT_EQ(event.kind, ProbeEventKind::kProbeHandleReplaced);
    EXPECT_EQ(event.local_nic, 0);
    EXPECT_EQ(event.graph_epoch, 1U);
    EXPECT_EQ(event.event_seq, 1U);
    ASSERT_TRUE(event.port.has_value());
    EXPECT_EQ(event.port->jetty_generation, old_generation + 1);
    EXPECT_EQ(event.port->probe_jetty->id(),
              provider.portViews()[0].native_jetty_id);
    EXPECT_EQ(event.port->source_generation, old_generation + 1);

    // A repeated report for the old generation is stale and must not rebuild
    // again.
    provider.retireProbeLane(LaneKey{0, old_jetty_id, 1}, old_generation, 7,
                             graph.edges[0].version,
                             Status::InternalError("duplicate"));
    ASSERT_TRUE(provider.waitForRebuilds(2000));
    EXPECT_EQ(sink.events.size(), 1U);
    EXPECT_EQ(provider.portViews()[0].generation, old_generation + 1);

    // The old source page stays quarantined until shutdown, even when a
    // handoff record references it.
    ProbeHandoff handoff;
    handoff.token = token;
    handoff.lane = LaneKey{0, old_jetty_id, 1};
    handoff.key.version = graph.edges[0].version;
    handoff.orphan_reason = OrphanReason::kTimeout;
    handoff.source = graph.ports[0].source_segment;
    handoff.area = graph.edges[0].area_segment;
    provider.handoffUnproven(handoff);
    EXPECT_EQ(provider.quarantineCount(), 1U);
    EXPECT_EQ(provider.retiredPageCount(), 1U);
    EXPECT_EQ(fixture.adapter->liveLocalSegmentCount(), 3U);

    ASSERT_TRUE(provider.shutdown().ok());
    EXPECT_EQ(provider.quarantineCount(), 0U);
    EXPECT_EQ(provider.retiredPageCount(), 0U);
    EXPECT_EQ(fixture.adapter->liveJfcCount(), 0U);
    EXPECT_EQ(fixture.adapter->liveJettyCount(), 0U);
    EXPECT_EQ(fixture.adapter->liveLocalSegmentCount(), 0U);
    EXPECT_TRUE(fixture.adapter->closeContext(fixture.context).ok());
    EXPECT_TRUE(fixture.adapter->shutdown().ok());
}

TEST(UbProbeRebuildTest, RetriesAfterQuiesceFailure) {
    Fixture fixture;
    ASSERT_TRUE(fixture.initialize());
    StandaloneProbeResourceProvider provider(
        fixture.adapter, makeProbeParams(),
        {ProbeLocalPortConfig{0, fixture.context, nullptr, 1}},
        &fixture.directory);
    ProbeGraph graph;
    ASSERT_TRUE(provider.snapshot(graph).ok());
    const uint32_t old_jetty_id = provider.portViews()[0].native_jetty_id;
    const uint64_t old_generation = provider.portViews()[0].generation;

    RecordingAvailabilitySink sink;
    provider.setAvailabilitySink(&sink);
    provider.setCompletionSink([](const Completion&) { return Status::OK(); });

    fixture.adapter->failNextQuiesce();
    provider.retireProbeLane(LaneKey{0, old_jetty_id, 1}, old_generation, 7,
                             graph.edges[0].version,
                             Status::InternalError("first attempt"));
    ASSERT_TRUE(provider.waitForRebuilds(2000));
    EXPECT_EQ(provider.portViews()[0].generation, old_generation);
    EXPECT_TRUE(sink.events.empty());
    EXPECT_EQ(fixture.adapter->liveJettyCount(), 1U);
    EXPECT_EQ(provider.retiredPageCount(), 0U);

    provider.retireProbeLane(LaneKey{0, old_jetty_id, 1}, old_generation, 7,
                             graph.edges[0].version,
                             Status::InternalError("retry"));
    ASSERT_TRUE(provider.waitForRebuilds(2000));
    EXPECT_EQ(provider.portViews()[0].generation, old_generation + 1);
    ASSERT_EQ(sink.events.size(), 1U);
    EXPECT_EQ(fixture.adapter->liveJettyCount(), 1U);
    EXPECT_EQ(provider.retiredPageCount(), 1U);

    ASSERT_TRUE(provider.shutdown().ok());
    EXPECT_TRUE(fixture.adapter->closeContext(fixture.context).ok());
    EXPECT_TRUE(fixture.adapter->shutdown().ok());
}

TEST(UbProbeRebuildTest, RefusesRebuildWithoutSinks) {
    Fixture fixture;
    ASSERT_TRUE(fixture.initialize());
    StandaloneProbeResourceProvider provider(
        fixture.adapter, makeProbeParams(),
        {ProbeLocalPortConfig{0, fixture.context, nullptr, 1}},
        &fixture.directory);
    ProbeGraph graph;
    ASSERT_TRUE(provider.snapshot(graph).ok());
    const uint32_t old_jetty_id = provider.portViews()[0].native_jetty_id;
    const uint64_t old_generation = provider.portViews()[0].generation;

    provider.retireProbeLane(LaneKey{0, old_jetty_id, 1}, old_generation, 7,
                             graph.edges[0].version,
                             Status::InternalError("no sinks"));
    ASSERT_TRUE(provider.waitForRebuilds(2000));
    EXPECT_EQ(provider.retireCalls(), 1U);
    EXPECT_EQ(provider.portViews()[0].generation, old_generation);
    EXPECT_EQ(provider.retiredPageCount(), 0U);
    EXPECT_EQ(fixture.adapter->liveJettyCount(), 1U);

    ASSERT_TRUE(provider.shutdown().ok());
    EXPECT_TRUE(fixture.adapter->closeContext(fixture.context).ok());
    EXPECT_TRUE(fixture.adapter->shutdown().ok());
}

}  // namespace
}  // namespace ub
}  // namespace tent
}  // namespace mooncake
