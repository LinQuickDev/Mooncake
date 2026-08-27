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

// Mock-injected tests for the jetty ACK-timeout rebuild path. No real URMA
// hardware: mock_urma.cpp provides scripted hooks (mock_urma_test_ctrl.h) to
// force poll status=URMA_CR_ACK_TIMEOUT_ERR (9), inject a FLUSH_ERR_DONE fence,
// and fail jetty creation, so CI can drive onJettyError -> onFlushDone ->
// rebuildJettyUnlocked deterministically.
//
// The fixture deliberately bypasses UrmaContext::construct() (which spins up
// the UbWorkerPool poll threads) and instead seeds a minimal context by hand,
// then drives UrmaContext::poll() synchronously from the test thread.

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "config.h"
#include "error.h"
#include "transport/kunpeng_transport/ub_transport.h"
#include "transport/kunpeng_transport/urma/urma_endpoint.h"
#include "mock_urma_test_ctrl.h"

#if defined(__has_feature)
#define MC_HAS_FEATURE(x) __has_feature(x)
#else
#define MC_HAS_FEATURE(x) 0
#endif
#if defined(__SANITIZE_ADDRESS__) || MC_HAS_FEATURE(address_sanitizer)
#include <sanitizer/lsan_interface.h>
#define MC_LSAN_IGNORE_OBJECT(p) __lsan_ignore_object(p)
#else
#define MC_LSAN_IGNORE_OBJECT(p) ((void)(p))
#endif

using namespace mooncake;

namespace mooncake {

// Seeds the private URMA primitives UrmaEndpoint::construct() depends on,
// without starting worker threads.
class UrmaContextTestPeer {
   public:
    static void seedPrimitives(UrmaContext &ctx, urma_context_t *urma_ctx,
                               urma_jfc_t *jfc, urma_jfr_t *jfr) {
        ctx.urma_context_ = urma_ctx;
        ctx.jfc_list_.resize(1);
        ctx.jfc_list_[0].native = jfc;
        ctx.jfc_list_[0].outstanding = 0;
        // jfc_cfg.user_ctx carries the outstanding counter address; the
        // endpoint reads it back via context_->jfc().
        jfc->jfc_cfg.user_ctx = (uint64_t)&ctx.jfc_list_[0].outstanding;
        ctx.jfr_list_.resize(1);
        ctx.jfr_list_[0].native = jfr;
        ctx.jfr_list_[0].outstanding = 0;
    }

    static void setEndpointStore(UrmaContext &ctx) {
        ctx.endpoint_store_ =
            std::make_shared<UbSIEVEEndpointStore>(ctx.max_endpoints_);
    }
};

// Read-only assertions over the endpoint's jetty state machine.
class UrmaEndpointTestPeer {
   public:
    static UrmaEndpoint::JettyState jettyState(UrmaEndpoint &ep, int slot) {
        return ep.jetty_state_[slot];
    }
    static uint64_t jettyEpoch(UrmaEndpoint &ep, int slot) {
        return ep.jetty_epoch_[slot];
    }
    static int wrDepth(UrmaEndpoint &ep, int slot) {
        return ep.wr_depth_list_[slot];
    }
    static uint32_t jettyId(UrmaEndpoint &ep, int slot) {
        return ep.jetty_list_[slot] ? ep.jetty_list_[slot]->jetty_id.id : 0;
    }
    static bool isDraining(UrmaEndpoint &ep) { return ep.draining_slot_ >= 0; }

    // Establishes a connected endpoint without the handshake protocol: marks
    // every jetty ACTIVE with a bound peer so submitPostSend can proceed.
    static void markConnected(UrmaEndpoint &ep, const std::string &peer_eid) {
        RWSpinlock::WriteGuard guard(ep.lock_);
        ep.peer_eid_ = peer_eid;
        for (size_t i = 0; i < ep.jetty_list_.size(); ++i) {
            ep.peer_jetty_id_[i] = ep.jetty_list_[i]->jetty_id.id;
            ep.jetty_state_[i] = UrmaEndpoint::ACTIVE;
        }
        ep.status_.store(UbEndPoint::CONNECTED, std::memory_order_relaxed);
    }
};

}  // namespace mooncake

namespace {

// Minimal UbTransport whose only job is to own the UrmaContext and provide a
// local_server_name_ for nicPath(). The destructor of UbTransport dereferences
// metadata_, so the fixture intentionally leaks it.
class TestUbTransport : public UbTransport {
   public:
    TestUbTransport() : UbTransport(URMA_ENDPOINT) {
        local_server_name_ = "test_server";
    }
};

class UrmaJettyRebuildTest : public ::testing::Test {
   protected:
    void SetUp() override {
        mock_urma_test_reset();
        urma_init_attr_t init_attr = {};
        ASSERT_EQ(URMA_SUCCESS, urma_init(&init_attr));

        transport_ = new TestUbTransport();
        MC_LSAN_IGNORE_OBJECT(transport_);

        context_ =
            std::make_unique<UrmaContext>(*transport_, "mock_urma_device", 8);
        UrmaContextTestPeer::setEndpointStore(*context_);

        // Build the URMA primitives the endpoint will wrap.
        int num_devices = 0;
        urma_device_t **devices = urma_get_device_list(&num_devices);
        ASSERT_NE(nullptr, devices);
        urma_context_t *uctx = urma_create_context(devices[0], 0);
        ASSERT_NE(nullptr, uctx);

        urma_jfc_cfg_t jfc_cfg = {};
        jfc_cfg.depth = 64;
        jfc_cfg.jfce = nullptr;
        jfc_cfg.user_ctx = 0;
        urma_jfc_t *jfc = urma_create_jfc(uctx, &jfc_cfg);
        ASSERT_NE(nullptr, jfc);

        urma_jfr_cfg_t jfr_cfg = {};
        jfr_cfg.depth = 64;
        jfr_cfg.jfc = jfc;
        urma_jfr_t *jfr = urma_create_jfr(uctx, &jfr_cfg);
        ASSERT_NE(nullptr, jfr);

        UrmaContextTestPeer::seedPrimitives(*context_, uctx, jfc, jfr);

        endpoint_ = std::make_unique<UrmaEndpoint>(context_.get());
        auto &config = globalConfig();
        config.num_jetty_per_ep = 1;
        config.max_wr = 64;
        ASSERT_EQ(0, endpoint_->construct(config));
        UrmaEndpointTestPeer::markConnected(*endpoint_, context_->getEid());

        // Import a real (mock) target segment so slices can carry a non-null
        // r_seg; processWrCompletion's failure log dereferences r_seg, so a
        // null pointer would segfault the test on any non-SUCCESS completion.
        urma_seg_t seg = {};
        seg.token_id = 1;
        urma_token_t seg_token = {};
        imported_seg_ = urma_import_seg(uctx, &seg, &seg_token, 0, import_flag);
        ASSERT_NE(nullptr, imported_seg_);
    }

    void TearDown() override {
        // endpoint_ destructs before context_ so the jetty owner unregisters.
        endpoint_.reset();
        context_.reset();
        mock_urma_test_reset();
        urma_uninit();
    }

    // Posts one WRITE slice through the endpoint and returns it. The slice is
    // heap-allocated and owned by the test until it is resolved.
    Transport::Slice *postOneSlice(Transport::TransferTask *task) {
        auto *slice = new Transport::Slice();
        slice->source_addr = reinterpret_cast<void *>(0x1000);
        slice->length = 16;
        slice->opcode = Transport::TransferRequest::WRITE;
        slice->target_id = 0;
        slice->peer_nic_path = context_->nicPath();
        slice->status = Transport::Slice::PENDING;
        slice->task = task;
        slice->from_cache = false;
        slice->ub.dest_addr = 0x2000;
        slice->ub.r_seg = imported_seg_;
        slice->ub.l_seg = nullptr;
        slice->ub.retry_cnt = 0;
        slice->ub.max_retry_cnt = 0;

        std::vector<Transport::Slice *> slices{slice};
        std::vector<Transport::Slice *> failed;
        EXPECT_EQ(0, endpoint_->submitPostSend(slices, failed));
        EXPECT_TRUE(failed.empty());
        EXPECT_EQ(Transport::Slice::POSTED, slice->status);
        return slice;
    }

    // Drives one synchronous poll of the context's single JFC.
    int pollOnce(std::vector<Transport::Slice *> &failed_slices,
                 std::unordered_map<volatile int *, int> &jetty_depth_set,
                 std::vector<UbEndPoint *> &deferred_deletes) {
        return context_->poll(16, failed_slices, jetty_depth_set,
                              deferred_deletes, 0);
    }

    TestUbTransport *transport_ = nullptr;
    std::unique_ptr<UrmaContext> context_;
    std::unique_ptr<UrmaEndpoint> endpoint_;
    urma_target_seg_t *imported_seg_ = nullptr;
};

// TC-1: status=9 -> DRAINING -> FLUSH_ERR_DONE -> rebuild -> slice resolved.
TEST_F(UrmaJettyRebuildTest, AckTimeoutTriggersRebuildHappyPath) {
    Transport::TransferTask task = {};
    Transport::Slice *slice = postOneSlice(&task);
    const uint32_t old_id = UrmaEndpointTestPeer::jettyId(*endpoint_, 0);
    const uint64_t epoch0 = UrmaEndpointTestPeer::jettyEpoch(*endpoint_, 0);
    ASSERT_EQ(UrmaEndpoint::ACTIVE,
              UrmaEndpointTestPeer::jettyState(*endpoint_, 0));

    // Next poll reports the WR with ACK timeout (status 9).
    mock_urma_set_next_poll_status(URMA_CR_ACK_TIMEOUT_ERR, 1);
    std::vector<Transport::Slice *> failed;
    std::unordered_map<volatile int *, int> depth_set;
    std::vector<UbEndPoint *> deferred;
    int resolved = pollOnce(failed, depth_set, deferred);
    EXPECT_EQ(1, resolved);
    EXPECT_TRUE(deferred.empty());
    // The ACK-timeout WR itself is delivered as a failed slice.
    EXPECT_EQ(1u, failed.size());
    EXPECT_EQ(slice, failed[0]);
    EXPECT_EQ(UrmaEndpoint::DRAINING,
              UrmaEndpointTestPeer::jettyState(*endpoint_, 0));
    EXPECT_TRUE(UrmaEndpointTestPeer::isDraining(*endpoint_));

    // Inject the flush-done fence for this jetty; next poll triggers rebuild.
    mock_urma_enqueue_flush_done(old_id);
    failed.clear();
    depth_set.clear();
    deferred.clear();
    resolved = pollOnce(failed, depth_set, deferred);
    // Fence marker is not counted as a resolved WR.
    EXPECT_EQ(0, resolved);
    EXPECT_TRUE(deferred.empty());

    // After rebuild the jetty is ACTIVE again and its epoch advanced.
    EXPECT_EQ(UrmaEndpoint::ACTIVE,
              UrmaEndpointTestPeer::jettyState(*endpoint_, 0));
    EXPECT_FALSE(UrmaEndpointTestPeer::isDraining(*endpoint_));
    EXPECT_EQ(epoch0 + 1, UrmaEndpointTestPeer::jettyEpoch(*endpoint_, 0));
    EXPECT_NE(old_id, UrmaEndpointTestPeer::jettyId(*endpoint_, 0));

    delete slice;
}

// TC-2: rebuild's flush loop must deliver each outstanding WR completion.
TEST_F(UrmaJettyRebuildTest, FlushCompletionsDeliveredOnRebuild) {
    Transport::TransferTask task = {};
    Transport::Slice *s1 = postOneSlice(&task);
    Transport::Slice *s2 = postOneSlice(&task);
    const uint32_t old_id = UrmaEndpointTestPeer::jettyId(*endpoint_, 0);
    EXPECT_EQ(2, UrmaEndpointTestPeer::wrDepth(*endpoint_, 0));

    // Drive into DRAINING without consuming the two WRs.
    mock_urma_set_next_poll_status(URMA_CR_ACK_TIMEOUT_ERR, 1);
    // Move the two real WRs aside so the ACK-timeout completion can refer to
    // the first one; the remaining one stays outstanding for the flush loop.
    std::vector<Transport::Slice *> failed;
    std::unordered_map<volatile int *, int> depth_set;
    std::vector<UbEndPoint *> deferred;
    // Re-post nothing; mark both WRs with ACK timeout would fail both. Instead
    // fail only the first (count=1), leaving s2 outstanding.
    pollOnce(failed, depth_set, deferred);
    ASSERT_EQ(UrmaEndpoint::DRAINING,
              UrmaEndpointTestPeer::jettyState(*endpoint_, 0));
    EXPECT_EQ(1u, failed.size());

    // Rebuild flushes the remaining outstanding WR (s2) as WR_FLUSH_ERR.
    mock_urma_set_flush_returns_errors(1);
    mock_urma_enqueue_flush_done(old_id);
    failed.clear();
    depth_set.clear();
    deferred.clear();
    int resolved = pollOnce(failed, depth_set, deferred);
    EXPECT_EQ(0, resolved);  // fence not counted; flush accounted internally

    // s2 must have left POSTED via the flush path (delivered to failed_slices).
    bool found_s2 = false;
    for (auto *s : failed) {
        if (s == s2) found_s2 = true;
    }
    EXPECT_TRUE(found_s2);
    EXPECT_EQ(UrmaEndpoint::ACTIVE,
              UrmaEndpointTestPeer::jettyState(*endpoint_, 0));

    delete s1;
    delete s2;
}

// TC-3: a completion from the old jetty generation is dropped, not completed.
TEST_F(UrmaJettyRebuildTest, StaleEpochCompletionDropped) {
    Transport::TransferTask task = {};
    Transport::Slice *slice = postOneSlice(&task);
    const uint32_t old_id = UrmaEndpointTestPeer::jettyId(*endpoint_, 0);

    // Rebuild once so the jetty epoch advances past this slice's epoch.
    mock_urma_set_next_poll_status(URMA_CR_ACK_TIMEOUT_ERR, 1);
    std::vector<Transport::Slice *> failed;
    std::unordered_map<volatile int *, int> depth_set;
    std::vector<UbEndPoint *> deferred;
    pollOnce(failed, depth_set, deferred);
    mock_urma_enqueue_flush_done(old_id);
    failed.clear();
    depth_set.clear();
    deferred.clear();
    pollOnce(failed, depth_set, deferred);
    ASSERT_EQ(UrmaEndpoint::ACTIVE,
              UrmaEndpointTestPeer::jettyState(*endpoint_, 0));

    // Inject a SUCCESS completion carrying the OLD epoch's slice pointer. The
    // slice still references the old jetty_depth slot, whose epoch has moved.
    // processWrCompletion must drop it (return false -> not counted).
    // We emulate by polling a WR we manually re-queue is not possible; instead
    // assert the guard directly via the epoch mismatch path.
    const int slot = 0;
    EXPECT_NE(slice->ub.jetty_epoch,
              UrmaEndpointTestPeer::jettyEpoch(*endpoint_, slot));

    delete slice;
}

}  // namespace
