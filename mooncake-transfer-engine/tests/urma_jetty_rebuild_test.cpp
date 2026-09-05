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

    // Forces the next checkDrainTimeout to consider the drain already timed
    // out, without waiting the real 3s kJettyDrainTimeoutNs.
    static void forceDrainTimeout(UrmaEndpoint &ep) { ep.drain_start_ns_ = 1; }

    static urma_jetty_t *jettyHandle(UrmaEndpoint &ep, int slot) {
        return ep.jetty_list_[slot];
    }

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
    // Withhold s2 from poll so it stays outstanding on the jetty and can only
    // be completed by the rebuild flush path below.
    mock_urma_withhold_next_post(1);
    Transport::Slice *s2 = postOneSlice(&task);
    const uint32_t old_id = UrmaEndpointTestPeer::jettyId(*endpoint_, 0);
    EXPECT_EQ(2, UrmaEndpointTestPeer::wrDepth(*endpoint_, 0));

    // Drive into DRAINING: only s1 is polled (status 9); s2 stays outstanding.
    mock_urma_set_next_poll_status(URMA_CR_ACK_TIMEOUT_ERR, 1);
    std::vector<Transport::Slice *> failed;
    std::unordered_map<volatile int *, int> depth_set;
    std::vector<UbEndPoint *> deferred;
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
    // The fence marker itself is not counted, but s2's flush-driven
    // completion increments poll's resolved_wr_count via rebuildJettyUnlocked.
    EXPECT_EQ(1, resolved);

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

// TC-3: a completion from the old jetty generation is dropped by poll, not
// completed. A WR withheld from poll survives a rebuild untouched (the
// flush loop produces nothing when no flush-error script is armed), so its
// CQE is still sitting in the JFC queue afterwards, carrying the old slice
// as user_ctx and stamped with the pre-rebuild epoch. When poll delivers
// that CQE after the epoch bump, processWrCompletion must drop it: not
// counted as resolved, not re-delivered as failed, no depth accounting —
// otherwise the slice would complete a second time.
TEST_F(UrmaJettyRebuildTest, StaleEpochCompletionDropped) {
    Transport::TransferTask task = {};
    mock_urma_withhold_next_post(1);
    Transport::Slice *stale = postOneSlice(&task);
    const uint64_t old_epoch = UrmaEndpointTestPeer::jettyEpoch(*endpoint_, 0);
    const uint32_t old_id = UrmaEndpointTestPeer::jettyId(*endpoint_, 0);

    // A second WR whose completion carries status=9 drives the jetty into
    // DRAINING; the withheld `stale` stays outstanding on the old jetty.
    Transport::Slice *trigger = postOneSlice(&task);
    mock_urma_set_next_poll_status(URMA_CR_ACK_TIMEOUT_ERR, 1);
    std::vector<Transport::Slice *> failed;
    std::unordered_map<volatile int *, int> depth_set;
    std::vector<UbEndPoint *> deferred;
    pollOnce(failed, depth_set, deferred);
    ASSERT_EQ(UrmaEndpoint::DRAINING,
              UrmaEndpointTestPeer::jettyState(*endpoint_, 0));

    // Fence arrives; rebuild completes without consuming `stale` (no
    // flush-error script armed). The epoch advances past `stale`'s stamp.
    mock_urma_enqueue_flush_done(old_id);
    failed.clear();
    depth_set.clear();
    deferred.clear();
    pollOnce(failed, depth_set, deferred);
    ASSERT_EQ(UrmaEndpoint::ACTIVE,
              UrmaEndpointTestPeer::jettyState(*endpoint_, 0));
    ASSERT_NE(old_epoch, UrmaEndpointTestPeer::jettyEpoch(*endpoint_, 0));

    // Release the withheld WR so urma_poll_jfc delivers its completion, then
    // poll for real: SUCCESS, user_ctx=`stale`, stamped with the pre-rebuild
    // epoch. processWrCompletion must drop it.
    mock_urma_unwithhold_all();
    failed.clear();
    depth_set.clear();
    deferred.clear();
    const int resolved = pollOnce(failed, depth_set, deferred);
    EXPECT_EQ(0, resolved);
    EXPECT_TRUE(failed.empty());
    EXPECT_TRUE(depth_set.empty());
    EXPECT_EQ(Transport::Slice::POSTED, stale->status);

    // Ownership was retained (the drop path never calls markSuccess, which
    // would self-delete the slice), so deleting it here is safe.
    delete stale;
    delete trigger;
}

// TC-4: when rebuild's urma_delete_jetty fails, the old jetty handle is kept
// (marked REBUILDING_FAILED) so deconstruct can retry the delete instead of
// leaking it, and the endpoint is deferred for deletion rather than torn down
// inside poll.
TEST_F(UrmaJettyRebuildTest, RebuildFailureKeepsJettyHandle) {
    Transport::TransferTask task = {};
    Transport::Slice *slice = postOneSlice(&task);
    const uint32_t old_id = UrmaEndpointTestPeer::jettyId(*endpoint_, 0);
    urma_jetty_t *old_handle = UrmaEndpointTestPeer::jettyHandle(*endpoint_, 0);
    ASSERT_NE(nullptr, old_handle);

    // Drive into DRAINING.
    mock_urma_set_next_poll_status(URMA_CR_ACK_TIMEOUT_ERR, 1);
    std::vector<Transport::Slice *> failed;
    std::unordered_map<volatile int *, int> depth_set;
    std::vector<UbEndPoint *> deferred;
    pollOnce(failed, depth_set, deferred);
    ASSERT_EQ(UrmaEndpoint::DRAINING,
              UrmaEndpointTestPeer::jettyState(*endpoint_, 0));

    // Make rebuild's delete step fail (urma_delete_jetty returns an error and
    // keeps the jetty allocated), then inject the fence so onFlushDone runs
    // rebuildJettyUnlocked into that delete-failure branch.
    mock_urma_fail_next_delete_jetty();
    mock_urma_enqueue_flush_done(old_id);
    failed.clear();
    depth_set.clear();
    deferred.clear();
    pollOnce(failed, depth_set, deferred);

    // The rebuild failed at delete: the endpoint is deferred for deletion, and
    // the old jetty handle is preserved (not nulled) so it can be retried.
    ASSERT_FALSE(deferred.empty());
    EXPECT_EQ(static_cast<UbEndPoint *>(endpoint_.get()), deferred.back());
    EXPECT_EQ(UrmaEndpoint::REBUILDING_FAILED,
              UrmaEndpointTestPeer::jettyState(*endpoint_, 0));
    EXPECT_EQ(old_handle, UrmaEndpointTestPeer::jettyHandle(*endpoint_, 0));

    delete slice;
}

// TC-5: a drain timeout flushes the jetty and delivers each residual WR as
// failed (failed_slices), bringing the slot depth accounting back to zero,
// instead of leaving slices stuck when the flush-done fence never arrives.
TEST_F(UrmaJettyRebuildTest, DrainTimeoutDeliversResidualWriters) {
    Transport::TransferTask task = {};
    // Residual WR withheld from poll so it stays outstanding into the timeout.
    mock_urma_withhold_next_post(1);
    Transport::Slice *stuck = postOneSlice(&task);
    EXPECT_EQ(1, UrmaEndpointTestPeer::wrDepth(*endpoint_, 0));

    // A second WR that polls out with status 9 drives the jetty into DRAINING.
    // `stuck` is withheld so this poll consumes only the trigger completion.
    Transport::Slice *trigger = postOneSlice(&task);
    mock_urma_set_next_poll_status(URMA_CR_ACK_TIMEOUT_ERR, 1);
    std::vector<Transport::Slice *> failed;
    std::unordered_map<volatile int *, int> depth_set;
    std::vector<UbEndPoint *> deferred;
    pollOnce(failed, depth_set, deferred);
    ASSERT_EQ(UrmaEndpoint::DRAINING,
              UrmaEndpointTestPeer::jettyState(*endpoint_, 0));

    // No flush-done fence arrives; force the drain timeout. The timeout path
    // must flush the jetty and deliver `stuck` as a failed completion.
    UrmaEndpointTestPeer::forceDrainTimeout(*endpoint_);
    mock_urma_set_flush_returns_errors(1);
    failed.clear();
    depth_set.clear();
    deferred.clear();
    context_->checkJettyDrainTimeouts(depth_set, failed, deferred);

    // `stuck` was delivered to failed_slices, depth accounted, and the endpoint
    // is deferred for deletion (no flush-done fence to rebuild from).
    bool found_stuck = false;
    for (auto *s : failed) {
        if (s == stuck) found_stuck = true;
    }
    EXPECT_TRUE(found_stuck);
    EXPECT_FALSE(deferred.empty());

    delete stuck;
    delete trigger;
}

// TC-6: drain-timeout recovery must NOT be part of poll(). If poll() delivered
// recovered WRs in its failed vector without counting them in its return
// value, the worker pool's num_success = resolved - failed would go negative
// and permanently disable the RNIC-dead protection. The contract is: poll()
// resolves only completions it polled; recovery happens in the separate
// checkJettyDrainTimeouts() pass afterwards.
TEST_F(UrmaJettyRebuildTest, DrainTimeoutRecoveryExcludedFromPoll) {
    Transport::TransferTask task = {};
    mock_urma_withhold_next_post(1);
    Transport::Slice *stuck = postOneSlice(&task);

    Transport::Slice *trigger = postOneSlice(&task);
    mock_urma_set_next_poll_status(URMA_CR_ACK_TIMEOUT_ERR, 1);
    std::vector<Transport::Slice *> failed;
    std::unordered_map<volatile int *, int> depth_set;
    std::vector<UbEndPoint *> deferred;
    pollOnce(failed, depth_set, deferred);
    ASSERT_EQ(UrmaEndpoint::DRAINING,
              UrmaEndpointTestPeer::jettyState(*endpoint_, 0));

    // Force the timeout, then poll again WITHOUT the separate recovery pass:
    // poll() must resolve nothing and deliver no failures for the residual
    // (withheld) WR, even though its jetty has already timed out.
    UrmaEndpointTestPeer::forceDrainTimeout(*endpoint_);
    mock_urma_set_flush_returns_errors(1);
    failed.clear();
    depth_set.clear();
    deferred.clear();
    int resolved = pollOnce(failed, depth_set, deferred);
    EXPECT_EQ(0, resolved);
    EXPECT_TRUE(failed.empty());
    EXPECT_TRUE(deferred.empty());

    // The recovery pass is what delivers the residual WR.
    context_->checkJettyDrainTimeouts(depth_set, failed, deferred);
    bool found_stuck = false;
    for (auto *s : failed) {
        if (s == stuck) found_stuck = true;
    }
    EXPECT_TRUE(found_stuck);

    delete stuck;
    delete trigger;
}

}  // namespace
