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
// P3 tests: the probe poster builds a one-byte RM WRITE with an explicit
// per-WR remote target and passes the source/area segments through.

#include <gtest/gtest.h>

#include <memory>

#include "tent/transport/ub/probe_poster_urma.h"
#include "ub_fake_urma.h"

namespace mooncake {
namespace tent {
namespace ub {
namespace {

using namespace fake;

TEST(UbProbePosterTest, PostsOneByteWriteWithPerWrTarget) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    ContextPtr context;
    ASSERT_TRUE(adapter->openContext(fakeDevice(), context).ok());

    JfcPtr jfc;
    ASSERT_TRUE(adapter->createJfc(context, JfcOptions{}, jfc).ok());
    JettyOptions jetty_options;
    jetty_options.transport_mode = JettyTransportMode::kRm;
    JettyPtr jetty;
    ASSERT_TRUE(adapter->createJetty(context, jfc, jetty_options, jetty).ok());

    ProbePostTarget target;
    target.probe_jetty = jetty;
    target.jetty_generation = 1;
    target.remote_jetty = RemoteJettyInfo{"eid", 401, 501, 0xACFE};
    target.source_segment = std::make_shared<FakeLocalSegment>(0x100000, 4096);
    target.area_segment = std::make_shared<FakeRemoteSegment>(
        0xA00000, 4096, makeDescriptor(0xA00000, 4096));

    ProbeWorkRequest request;
    request.token = makeProbeToken(7);
    request.source_address = 0x100000;
    request.source_length = 1;
    request.area_address = 0xA00000;
    request.area_length = 1;

    UrmaProbePoster poster(adapter);
    size_t posted = 0;
    ASSERT_TRUE(poster.post(target, request, posted).ok());
    EXPECT_EQ(posted, 1U);
    EXPECT_TRUE(adapter->lastPostHadTarget());
    EXPECT_EQ(adapter->lastPostTarget().id, 401U);
    EXPECT_EQ(adapter->lastPostTarget().uasid, 501U);
    EXPECT_EQ(adapter->lastPostTarget().eid, "eid");

    const auto write = adapter->lastPost();
    EXPECT_EQ(write.operation, Operation::WRITE);
    EXPECT_EQ(write.length, 1U);
    EXPECT_EQ(write.token, makeProbeToken(7));
    EXPECT_EQ(write.local_address, 0x100000U);
    EXPECT_EQ(write.remote_address, 0xA00000U);
    EXPECT_NE(write.local_segment, nullptr);
    EXPECT_NE(write.remote_segment, nullptr);
    EXPECT_EQ(adapter->lastJettyTransportMode(), JettyTransportMode::kRm);

    ASSERT_TRUE(adapter->resetJetty(jetty).ok());
    ASSERT_TRUE(adapter->deleteJetty(jetty).ok());
    ASSERT_TRUE(adapter->deleteJfc(jfc).ok());
    EXPECT_TRUE(adapter->closeContext(context).ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbProbePosterTest, RejectsMissingSourceSegment) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    ContextPtr context;
    ASSERT_TRUE(adapter->openContext(fakeDevice(), context).ok());
    JfcPtr jfc;
    ASSERT_TRUE(adapter->createJfc(context, JfcOptions{}, jfc).ok());
    JettyPtr jetty;
    ASSERT_TRUE(adapter->createJetty(context, jfc, JettyOptions{}, jetty).ok());

    ProbePostTarget target;
    target.probe_jetty = jetty;
    target.area_segment = std::make_shared<FakeRemoteSegment>(
        0xA00000, 4096, makeDescriptor(0xA00000, 4096));
    ProbeWorkRequest request;
    request.token = makeProbeToken(1);
    request.source_address = 0x100000;
    request.source_length = 1;
    request.area_address = 0xA00000;
    request.area_length = 1;

    UrmaProbePoster poster(adapter);
    size_t posted = 1;
    EXPECT_TRUE(poster.post(target, request, posted).IsInvalidArgument());
    EXPECT_EQ(posted, 0U);

    ASSERT_TRUE(adapter->deleteJetty(jetty).ok());
    ASSERT_TRUE(adapter->deleteJfc(jfc).ok());
    EXPECT_TRUE(adapter->closeContext(context).ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

}  // namespace
}  // namespace ub
}  // namespace tent
}  // namespace mooncake
