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
// v1.7 self-owned probe resources: physical-device discovery, bond rejection,
// device filtering, stable port indices and source-page-size validation.

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

DeviceInfo fakeDeviceNamed(const std::string& native_name) {
    auto device = fakeDevice();
    device.native_device_name = native_name;
    device.topology_name = "ub:" + native_name + ":eid0";
    return device;
}

TEST(UbProbeDiscoveryTest, DiscoversPhysicalDeviceAndBuildsGraph) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    StaticDirectory directory({makeEdge(0, 42, 0)});
    StandaloneProbeResourceProvider provider(adapter, makeProbeParams());

    ASSERT_TRUE(provider.discover().ok());
    ASSERT_EQ(provider.contexts().size(), 1U);
    provider.setRemoteDirectory(&directory);

    ProbeGraph graph;
    ASSERT_TRUE(provider.snapshot(graph).ok());
    ASSERT_EQ(graph.ports.size(), 1U);
    ASSERT_EQ(graph.edges.size(), 1U);
    EXPECT_EQ(graph.ports[0].local_nic, 0);
    EXPECT_EQ(provider.portViews().size(), 1U);

    ASSERT_TRUE(provider.shutdown().ok());
    EXPECT_TRUE(provider.contexts().empty());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeDiscoveryTest, SkipsBondingDevicesAndUsesPhysicalDevice) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    adapter->setDiscoveredDevices({fakeDeviceNamed("bonding0"), fakeDevice()});

    StandaloneProbeResourceProvider provider(adapter, makeProbeParams());
    ASSERT_TRUE(provider.discover().ok());
    ASSERT_EQ(provider.contexts().size(), 1U);
    EXPECT_NE(provider.contexts().find(0), provider.contexts().end());
    ASSERT_TRUE(provider.shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeDiscoveryTest, RejectsOnlyBondingDevices) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    adapter->setDiscoveredDevices({fakeDeviceNamed("bonding0")});

    StandaloneProbeResourceProvider provider(adapter, makeProbeParams());
    const auto status = provider.discover();
    EXPECT_TRUE(status.IsDeviceNotFound()) << status.ToString();
    ASSERT_TRUE(provider.shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeDiscoveryTest, AppliesDeviceFilterAndUsesSelectedPort) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    adapter->setDiscoveredDevices(
        {fakeDeviceNamed("fake0"), fakeDeviceNamed("fake1")});
    StaticDirectory directory({makeEdge(0, 42, 0)});

    auto params = makeProbeParams();
    params.device_filter = {"fake1"};
    StandaloneProbeResourceProvider provider(adapter, params);
    ASSERT_TRUE(provider.discover().ok());
    ASSERT_EQ(provider.contexts().size(), 1U);
    EXPECT_NE(provider.contexts().find(0), provider.contexts().end());
    provider.setRemoteDirectory(&directory);

    ProbeGraph graph;
    ASSERT_TRUE(provider.snapshot(graph).ok());
    ASSERT_EQ(graph.ports.size(), 1U);
    EXPECT_EQ(graph.ports[0].local_nic, 0);
    EXPECT_EQ(provider.portViews()[0].local_nic, 0);

    ASSERT_TRUE(provider.shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeDiscoveryTest, PrepareLocalThenSnapshotReusesResources) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    StaticDirectory directory({makeEdge(0, 42, 0)});
    StandaloneProbeResourceProvider provider(adapter, makeProbeParams());

    ASSERT_TRUE(provider.discover().ok());
    ASSERT_TRUE(provider.prepareLocal().ok());
    ASSERT_EQ(provider.portViews().size(), 1U);
    EXPECT_EQ(adapter->liveJfcCount(), 1U);
    EXPECT_EQ(adapter->liveJettyCount(), 1U);
    EXPECT_EQ(adapter->liveLocalSegmentCount(), 2U);
    EXPECT_FALSE(provider.portViews()[0].device_eid.empty());

    provider.setRemoteDirectory(&directory);
    ProbeGraph graph;
    ASSERT_TRUE(provider.snapshot(graph).ok());
    EXPECT_EQ(graph.ports.size(), 1U);
    EXPECT_EQ(adapter->liveJfcCount(), 1U);
    EXPECT_EQ(adapter->liveJettyCount(), 1U);
    EXPECT_EQ(adapter->liveLocalSegmentCount(), 2U);

    ASSERT_TRUE(provider.shutdown().ok());
    EXPECT_EQ(adapter->liveJfcCount(), 0U);
    EXPECT_EQ(adapter->liveJettyCount(), 0U);
    EXPECT_EQ(adapter->liveLocalSegmentCount(), 0U);
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeDiscoveryTest, ResolvesBusinessTopologyNicId) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    StaticDirectory directory({makeEdge(100, 42, 0)});

    StandaloneProbeResourceProvider provider(
        adapter, makeProbeParams(),
        [](const std::string& name) -> Topology::NicID {
            return name == "ub:fake0:eid0" ? 100 : -1;
        });
    ASSERT_TRUE(provider.discover().ok());
    ASSERT_EQ(provider.contexts().size(), 1U);
    EXPECT_NE(provider.contexts().find(100), provider.contexts().end());
    provider.setRemoteDirectory(&directory);

    ProbeGraph graph;
    ASSERT_TRUE(provider.snapshot(graph).ok());
    ASSERT_EQ(graph.ports.size(), 1U);
    EXPECT_EQ(graph.ports[0].local_nic, 100);
    EXPECT_EQ(provider.portViews()[0].local_nic, 100);

    ASSERT_TRUE(provider.shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeDiscoveryTest, DiscoversAllEidsWithoutFilter) {
    auto eid0 = fakeDevice();
    auto eid1 = fakeDevice();
    eid1.eid_index = 1;
    eid1.eid = "0001:0000:0000:0000:0000:0000:0000:0001";
    eid1.topology_name = "ub:fake0:eid1";
    auto adapter = std::make_shared<FakeUrmaAdapter>(eid0);
    adapter->setDiscoveredDevices({eid0, eid1});
    ASSERT_TRUE(adapter->initialize().ok());

    StandaloneProbeResourceProvider provider(adapter, makeProbeParams());
    ASSERT_TRUE(provider.discover().ok());
    ASSERT_TRUE(provider.prepareLocal().ok());
    const auto views = provider.portViews();
    ASSERT_EQ(views.size(), 2U);
    EXPECT_EQ(views[0].topology_name, "ub:fake0:eid0");
    EXPECT_EQ(views[1].topology_name, "ub:fake0:eid1");

    ASSERT_TRUE(provider.shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeDiscoveryTest, NativeFilterSelectsAllEids) {
    auto eid0 = fakeDevice();
    auto eid1 = fakeDevice();
    eid1.eid_index = 1;
    eid1.eid = "0001:0000:0000:0000:0000:0000:0000:0001";
    eid1.topology_name = "ub:fake0:eid1";
    auto adapter = std::make_shared<FakeUrmaAdapter>(eid0);
    adapter->setDiscoveredDevices({eid0, eid1});
    ASSERT_TRUE(adapter->initialize().ok());

    auto params = makeProbeParams();
    params.device_filter = {"fake0"};
    StandaloneProbeResourceProvider provider(adapter, params);
    ASSERT_TRUE(provider.discover().ok());
    ASSERT_TRUE(provider.prepareLocal().ok());
    const auto views = provider.portViews();
    ASSERT_EQ(views.size(), 2U);
    EXPECT_EQ(views[0].topology_name, "ub:fake0:eid0");
    EXPECT_EQ(views[1].topology_name, "ub:fake0:eid1");

    ASSERT_TRUE(provider.shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeDiscoveryTest, SelectsExplicitEid) {
    auto eid0 = fakeDevice();
    auto eid1 = fakeDevice();
    eid1.eid_index = 1;
    eid1.eid = "0001:0000:0000:0000:0000:0000:0000:0001";
    eid1.topology_name = "ub:fake0:eid1";
    auto adapter = std::make_shared<FakeUrmaAdapter>(eid0);
    adapter->setDiscoveredDevices({eid0, eid1});
    ASSERT_TRUE(adapter->initialize().ok());

    auto params = makeProbeParams();
    params.device_filter = {"fake0:eid1"};
    StaticDirectory directory({makeEdge(0, 42, 0)});
    StandaloneProbeResourceProvider provider(adapter, params);
    ASSERT_TRUE(provider.discover().ok());
    provider.setRemoteDirectory(&directory);

    ProbeGraph graph;
    ASSERT_TRUE(provider.snapshot(graph).ok());
    ASSERT_EQ(graph.ports.size(), 1U);
    const auto views = provider.portViews();
    ASSERT_EQ(views.size(), 1U);
    EXPECT_EQ(views[0].topology_name, "ub:fake0:eid1");
    EXPECT_EQ(views[0].device_eid, eid1.eid);

    ASSERT_TRUE(provider.shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeDiscoveryTest, RejectsUnmatchedFilter) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto params = makeProbeParams();
    params.device_filter = {"does-not-exist"};

    StandaloneProbeResourceProvider provider(adapter, params);
    const auto status = provider.discover();
    EXPECT_TRUE(status.IsDeviceNotFound()) << status.ToString();
    ASSERT_TRUE(provider.shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbeDiscoveryTest, RejectsMisalignedSourcePageSize) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto params = makeProbeParams();
    params.source_page_size = 1234;
    StandaloneProbeResourceProvider provider(adapter, params);
    const auto status = provider.discover();
    EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
    ASSERT_TRUE(provider.shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

}  // namespace
}  // namespace ub
}  // namespace tent
}  // namespace mooncake
