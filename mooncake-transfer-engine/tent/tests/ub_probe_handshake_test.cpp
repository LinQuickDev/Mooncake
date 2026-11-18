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
// P2 tests: probe peer descriptor validation, probe-area import/caching and
// the integration with the standalone provider (one peer, one port).

#include <gtest/gtest.h>

#include <memory>
#include <utility>
#include <vector>

#include "tent/transport/ub/probe_peer_handshake.h"
#include "tent/transport/ub/probe_provider_standalone.h"
#include "ub_fake_urma.h"

namespace mooncake {
namespace tent {
namespace ub {
namespace {

using namespace fake;

class StubPeerSource final : public ProbePeerDescriptorSource {
   public:
    explicit StubPeerSource(std::vector<ProbePeerDescriptor> descriptors)
        : descriptors_(std::move(descriptors)) {}

    Status snapshot(std::vector<ProbePeerDescriptor>& descriptors) override {
        descriptors = descriptors_;
        return Status::OK();
    }

    void set(std::vector<ProbePeerDescriptor> descriptors) {
        descriptors_ = std::move(descriptors);
    }

   private:
    std::vector<ProbePeerDescriptor> descriptors_;
};

ProbePeerDescriptor makePeer(
    Topology::NicID local_nic, SegmentID peer_id, Topology::NicID remote_nic,
    uint64_t area_address = 0xA00000,
    uint32_t schema_version = kProbePeerDescriptorSchemaVersion) {
    ProbePeerDescriptor descriptor;
    descriptor.schema_version = schema_version;
    descriptor.local_nic = local_nic;
    descriptor.peer_segment_id = peer_id;
    descriptor.remote_nic = remote_nic;
    descriptor.peer_nic_path = "peer@ub:fake0:eid0";
    descriptor.area_descriptor = makeDescriptor(area_address, 4096);
    descriptor.area_length = 1;
    descriptor.area_epoch = 1;
    descriptor.area_token = 0xACFE;
    descriptor.remote_jetty = RemoteJettyInfo{"eid", 401, 501, 0xACFE};
    descriptor.node_gate_ok = true;
    descriptor.port_gate_ok = true;
    descriptor.profile_gate_ok = true;
    return descriptor;
}

ProbeParams makeProbeParams() {
    ProbeParams params;
    params.enabled = true;
    params.jfc_depth = 64;
    return params;
}

TEST(UbProbeHandshakeTest, ImportsAndCachesPeerProbeArea) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    ContextPtr context;
    ASSERT_TRUE(adapter->openContext(fakeDevice(), context).ok());

    StubPeerSource source({makePeer(0, 42, 0)});
    HandshakeProbePeerDirectory directory(adapter, {{0, context}}, &source);

    std::vector<ProbeEdgeSupply> edges;
    ASSERT_TRUE(directory.snapshotEdges(edges).ok());
    ASSERT_EQ(edges.size(), 1U);
    EXPECT_EQ(edges[0].version.path.local_nic, 0);
    EXPECT_EQ(edges[0].version.path.segment_id, 42U);
    EXPECT_EQ(edges[0].version.path.remote_nic, 0);
    EXPECT_EQ(edges[0].version.topology_epoch, 1U);
    EXPECT_EQ(edges[0].version.target_epoch, 1U);
    EXPECT_EQ(edges[0].area_address, 0xA00000U);
    EXPECT_EQ(edges[0].area_length, 1U);
    EXPECT_EQ(edges[0].area_epoch, 1U);
    EXPECT_TRUE(edges[0].node_gate_ok);
    EXPECT_TRUE(edges[0].port_gate_ok);
    EXPECT_TRUE(edges[0].profile_gate_ok);
    EXPECT_EQ(edges[0].remote_jetty.id, 401U);
    EXPECT_EQ(directory.importCount(), 1U);
    EXPECT_EQ(directory.cachedEdgeCount(), 1U);
    EXPECT_EQ(adapter->liveRemoteSegmentCount(), 1U);

    // Same descriptor on the next snapshot reuses the import.
    edges.clear();
    ASSERT_TRUE(directory.snapshotEdges(edges).ok());
    EXPECT_EQ(edges.size(), 1U);
    EXPECT_EQ(directory.importCount(), 1U);
    EXPECT_EQ(adapter->liveRemoteSegmentCount(), 1U);

    ASSERT_TRUE(directory.shutdown().ok());
    EXPECT_TRUE(directory.shutdown().ok());
    EXPECT_EQ(directory.cachedEdgeCount(), 0U);
    EXPECT_EQ(adapter->liveRemoteSegmentCount(), 0U);

    EXPECT_TRUE(adapter->closeContext(context).ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeHandshakeTest, SkipsUnsupportedPeerSchema) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    ContextPtr context;
    ASSERT_TRUE(adapter->openContext(fakeDevice(), context).ok());

    StubPeerSource source(
        {makePeer(0, 41, 0, 0xA00000, /*schema=*/2), makePeer(0, 42, 0)});
    HandshakeProbePeerDirectory directory(adapter, {{0, context}}, &source);

    std::vector<ProbeEdgeSupply> edges;
    ASSERT_TRUE(directory.snapshotEdges(edges).ok());
    ASSERT_EQ(edges.size(), 1U);
    EXPECT_EQ(edges[0].version.path.segment_id, 42U);
    EXPECT_EQ(directory.unsupportedPeerCount(), 1U);
    EXPECT_EQ(directory.importCount(), 1U);

    ASSERT_TRUE(directory.shutdown().ok());
    EXPECT_TRUE(adapter->closeContext(context).ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeHandshakeTest, ImportFailureIsCountedAndSkipped) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    ContextPtr context;
    ASSERT_TRUE(adapter->openContext(fakeDevice(), context).ok());

    auto bad = makePeer(0, 42, 0);
    bad.area_descriptor.hex = "not-a-descriptor";
    StubPeerSource source({bad});
    HandshakeProbePeerDirectory directory(adapter, {{0, context}}, &source);

    std::vector<ProbeEdgeSupply> edges;
    ASSERT_TRUE(directory.snapshotEdges(edges).ok());
    EXPECT_TRUE(edges.empty());
    EXPECT_EQ(directory.importFailureCount(), 1U);
    EXPECT_EQ(adapter->liveRemoteSegmentCount(), 0U);

    ASSERT_TRUE(directory.shutdown().ok());
    EXPECT_TRUE(adapter->closeContext(context).ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeHandshakeTest, ChangedAreaIsReimportedAndOldSegmentReleased) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    ContextPtr context;
    ASSERT_TRUE(adapter->openContext(fakeDevice(), context).ok());

    StubPeerSource source({makePeer(0, 42, 0, 0xA00000)});
    HandshakeProbePeerDirectory directory(adapter, {{0, context}}, &source);
    std::vector<ProbeEdgeSupply> edges;
    ASSERT_TRUE(directory.snapshotEdges(edges).ok());
    ASSERT_EQ(edges.size(), 1U);
    EXPECT_EQ(edges[0].area_address, 0xA00000U);
    EXPECT_EQ(directory.importCount(), 1U);
    EXPECT_EQ(adapter->liveRemoteSegmentCount(), 1U);

    source.set({makePeer(0, 42, 0, 0xB00000)});
    edges.clear();
    ASSERT_TRUE(directory.snapshotEdges(edges).ok());
    ASSERT_EQ(edges.size(), 1U);
    EXPECT_EQ(edges[0].area_address, 0xB00000U);
    EXPECT_EQ(directory.importCount(), 2U);
    // The old import was released before the new one was cached.
    EXPECT_EQ(adapter->liveRemoteSegmentCount(), 1U);

    ASSERT_TRUE(directory.shutdown().ok());
    EXPECT_EQ(adapter->liveRemoteSegmentCount(), 0U);
    EXPECT_TRUE(adapter->closeContext(context).ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeHandshakeTest, UnknownLocalPortIsRejected) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    ContextPtr context;
    ASSERT_TRUE(adapter->openContext(fakeDevice(), context).ok());

    StubPeerSource source({makePeer(7, 42, 0)});
    HandshakeProbePeerDirectory directory(adapter, {{0, context}}, &source);
    std::vector<ProbeEdgeSupply> edges;
    EXPECT_TRUE(directory.snapshotEdges(edges).IsInvalidArgument());

    ASSERT_TRUE(directory.shutdown().ok());
    EXPECT_TRUE(adapter->closeContext(context).ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeHandshakeTest, BuildsProviderGraphWithOnePeerAndPort) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    ContextPtr context;
    ASSERT_TRUE(adapter->openContext(fakeDevice(), context).ok());

    StubPeerSource source({makePeer(0, 42, 0)});
    HandshakeProbePeerDirectory directory(adapter, {{0, context}}, &source);
    StandaloneProbeResourceProvider provider(
        adapter, makeProbeParams(),
        {ProbeLocalPortConfig{0, context, nullptr, 1}}, &directory);

    ProbeGraph graph;
    ASSERT_TRUE(provider.snapshot(graph).ok());
    ASSERT_EQ(graph.ports.size(), 1U);
    ASSERT_EQ(graph.edges.size(), 1U);
    EXPECT_EQ(graph.edges[0].version.path.segment_id, 42U);
    EXPECT_EQ(graph.edges[0].area_address, 0xA00000U);
    ASSERT_EQ(provider.localAreaViews().size(), 1U);
    EXPECT_EQ(provider.localAreaViews()[0].local_nic, 0);
    EXPECT_FALSE(provider.localAreaViews()[0].descriptor.hex.empty());
    EXPECT_EQ(adapter->liveLocalSegmentCount(), 2U);
    EXPECT_EQ(adapter->liveRemoteSegmentCount(), 1U);

    // Transport teardown order: provider first, then the peer directory.
    ASSERT_TRUE(provider.shutdown().ok());
    ASSERT_TRUE(directory.shutdown().ok());
    EXPECT_EQ(adapter->liveLocalSegmentCount(), 0U);
    EXPECT_EQ(adapter->liveRemoteSegmentCount(), 0U);
    EXPECT_TRUE(adapter->closeContext(context).ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

}  // namespace
}  // namespace ub
}  // namespace tent
}  // namespace mooncake
