// Copyright 2024 KVCache.AI
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

#ifndef URMA_ENDPOINT_H
#define URMA_ENDPOINT_H
#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include "common.h"
#include "config.h"
#include "urma_api.h"
#include "transport/kunpeng_transport/ub_context.h"
#include "transport/kunpeng_transport/ub_endpoint.h"

namespace mooncake {
struct UrmaJFC {
    UrmaJFC() : native(nullptr), outstanding(0) {}

    urma_jfc_t* native;
    volatile int outstanding;
};

struct UrmaJFR {
    UrmaJFR() : native(nullptr), outstanding(0) {}

    urma_jfr_t* native;
    volatile int outstanding;
};

static urma_import_seg_flag_t import_flag = {
    .bs = {.cacheable = URMA_NON_CACHEABLE,
           .access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC,
           .mapping = URMA_SEG_NOMAP,
           .reserved = 0}};

// define the UrmaContext class
class UrmaEndpoint;
// Test hook: lets gtest fixtures seed a minimal UrmaContext (jfc/jfr/urma
// context) without spinning up the worker pool that full construct() starts.
class UrmaContextTestPeer;

class UrmaContext : public UbContext {
    friend class UrmaEndpoint;
    friend class UrmaContextTestPeer;

   public:
    UrmaContext(UbTransport& engine, std::string device_name,
                int max_endpoints);
    ~UrmaContext();
    int registerMemoryRegion(uint64_t va, size_t length) override;
    int unregisterMemoryRegion(uint64_t va) override;
    int doProcessContextEvents() override;
    void* retrieveRemoteSeg(const std::string& value) override;
    int poll(int num_entries, std::vector<Transport::Slice*>& failed_slices,
             std::unordered_map<volatile int*, int>& jetty_depth_set,
             std::vector<UbEndPoint*>& deferred_deletes,
             int jfc_index) override;
    volatile int* outstandingCount(int jfc_index) override;
    int submitPostSend(
        const std::vector<Transport::Slice*>& slice_list) override;
    int buildLocalBufferDesc(uint64_t addr,
                             UbTransport::BufferDesc& buffer_desc) override;
    void* localSegWithIndex(unsigned value) override;
    int jfcCount() override;
    int getAsyncFd() override;
    std::string getEid() override;
    std::string toString() override;
    std::shared_ptr<UbEndPoint> makeEndpoint() override;
    std::string eid() const;
    std::string eid(urma_eid_t eid);
    bool transEidFromString(const std::string& eid_str, urma_eid_t& eid);
    urma_jfc_t* jfc();
    urma_jfr_t* jfr();
    urma_jfce_t* JFCE();
    static bool uninit();
    static bool init();

    void registerJettyOwner(uint32_t jetty_id, UrmaEndpoint* endpoint,
                            int slot);
    void unregisterJettyOwner(uint32_t jetty_id);
    bool findJettyOwner(uint32_t jetty_id, UrmaEndpoint** endpoint, int* slot);
    void addDrainingEndpoint(UrmaEndpoint* endpoint);
    void removeDrainingEndpoint(UrmaEndpoint* endpoint);
    void checkJettyDrainTimeouts(
        std::unordered_map<volatile int*, int>& jetty_depth_set,
        std::vector<Transport::Slice*>& failed_slices,
        std::vector<UbEndPoint*>& deferred_deletes) override;

   private:
    int construct(GlobalConfig& config) override;
    int deconstruct() override;
    int openDevice(const std::string& device_name, uint8_t port,
                   int& eid_index) override;

    urma_target_seg_t* seg(uint64_t addr);

    std::vector<urma_seg_t*>& remote_seg_list() { return remote_seg_list_; }

    std::vector<urma_target_seg_t*>& imported_seg_list() {
        return imported_seg_list_;
    }

    std::vector<urma_target_seg_t*>& local_tseg_list() {
        return local_tseg_list_;
    }

    void updateUrmaGlobalConfig(urma_device_attr_t& device_attr) {
        auto& config = globalConfig();
        if (config.max_ep_per_ctx * config.num_jetty_per_ep >
            (size_t)device_attr.dev_cap.max_jetty) {
            config.max_ep_per_ctx =
                device_attr.dev_cap.max_jetty / config.num_jetty_per_ep;
        }
        if (config.num_jfc_per_ctx > (size_t)device_attr.dev_cap.max_jfc) {
            config.num_jfc_per_ctx = device_attr.dev_cap.max_jfc;
        }
    }

   private:
    std::vector<UrmaJFC> jfc_list_;
    urma_token_t urma_token = {.token = 0xACFE};
    urma_context_t* urma_context_ = nullptr;
    // ibv_pd *pd_ = nullptr;
    uint64_t max_seg_size{};
    urma_mtu active_mtu_;
    urma_eid_t eid_{};
    urma_device_attr_t dev_attr_{};

    int eid_index_ = -1;
    int active_speed_ = -1;

    RWSpinlock seg_region_lock_;
    std::vector<std::pair<urma_target_seg_t*, uint64_t>> seg_region_list_;
    std::vector<urma_target_seg_t*> local_tseg_list_;
    std::vector<urma_seg_t*> remote_seg_list_;
    std::vector<urma_target_seg_t*> imported_seg_list_;

    std::vector<UrmaJFR> jfr_list_;

    size_t num_JFCE_ = 0;
    urma_jfce_t** jfce_ = nullptr;

    std::vector<std::thread> background_thread_;
    std::atomic<bool> threads_running_;

    std::atomic<int> next_jfce_index_;
    std::atomic<int> next_jfce_vector_index_;
    std::atomic<int> next_jfc_list_index_;
    std::atomic<int> next_jfr_list_index_;
    std::vector<urma_jfc_t*> jfc_r_list_;

    urma_import_seg_flag_t import_flag_ = mooncake::import_flag;
    std::unordered_map<std::string, urma_target_seg_t*> import_tseg_map;

    RWSpinlock jetty_owner_lock_;
    struct JettyOwner {
        UrmaEndpoint* endpoint = nullptr;
        int slot = -1;
    };
    std::unordered_map<uint32_t, JettyOwner> jetty_owner_map_;
    std::unordered_set<UrmaEndpoint*> draining_endpoints_;
};

// Test hook for asserting the jetty state machine without touching private
// members directly from the test body.
class UrmaEndpointTestPeer;

// define the UrmaEndpoint class
class UrmaEndpoint : public UbEndPoint {
    // UrmaContext::poll drives the jetty state machine via processWrCompletion.
    friend class UrmaContext;
    friend class UrmaEndpointTestPeer;

   public:
    // PENDING_DRAIN: the slot hit ACK timeout while another jetty of this
    // endpoint was draining/rebuilding. It is excluded from post selection
    // and drained once the in-flight rebuild completes (rebuilds are
    // serialized per endpoint).
    enum JettyState {
        ACTIVE = 0,
        DRAINING = 1,
        REBUILDING = 2,
        PENDING_DRAIN = 3,
        // Rebuild failed after the old jetty was already torn down
        // (unbind/unimport) but could not be fully deleted or replaced. The
        // old handle is kept in jetty_list_[slot] so deconstruct can retry
        // urma_delete_jetty instead of leaking it. Never selected for post.
        REBUILDING_FAILED = 4
    };

    UrmaEndpoint(UrmaContext* context)
        : context_(context), jfc_outstanding_(nullptr) {}

    int construct(GlobalConfig& config) override;

    int deconstruct() override;

    void setPeerNicPath(const std::string& peer_nic_path) override;

    int setupConnectionsByActive() override;

    int setupConnectionsByPassive(const HandShakeDesc& peer_desc,
                                  HandShakeDesc& local_desc) override;

    bool hasOutstandingSlice() const override;

    int submitPostSend(
        std::vector<Transport::Slice*>& slice_list,
        std::vector<Transport::Slice*>& failed_slice_list) override;

    const std::string toString() const override;

    // Called from UrmaContext::poll on ACK timeout / flush-done / drain
    // timeout.
    void onJettyError(int slot, std::vector<UbEndPoint*>& deferred_deletes);
    void onFlushDone(int slot,
                     std::unordered_map<volatile int*, int>& jetty_depth_set,
                     std::vector<Transport::Slice*>& failed_slices,
                     std::vector<UbEndPoint*>& deferred_deletes,
                     int& resolved_wr_count);
    void checkDrainTimeout(
        std::unordered_map<volatile int*, int>& jetty_depth_set,
        std::vector<Transport::Slice*>& failed_slices,
        std::vector<UbEndPoint*>& deferred_deletes);

    int findSlotByDepth(volatile int* depth) const;

   private:
    void disconnectUnlocked() override;

   private:
    std::vector<uint32_t> JettyNum() const;

    int doSetupConnection(const std::string& peer_eid,
                          std::vector<uint32_t> peer_jetty_num_list,
                          std::string* reply_msg = nullptr);

    int doSetupConnection(int qp_index, const std::string& peer_eid,
                          uint32_t peer_jetty_num,
                          std::string* reply_msg = nullptr);

    bool hasNonActiveJettyUnlocked() const;
    int selectActiveJettyUnlocked();
    // Transitions `slot` to DRAINING via urma_modify_jetty(ERROR) and arms
    // the flush-done wait. Caller must hold lock_. Returns ERR_ENDPOINT if
    // modify failed (caller should fall back to deleting the endpoint).
    int startDrainUnlocked(int slot);
    int rebuildJettyUnlocked(
        int slot, std::unordered_map<volatile int*, int>& jetty_depth_set,
        std::vector<Transport::Slice*>& failed_slices,
        std::vector<UbEndPoint*>& deferred_deletes, int& resolved_wr_count);

    // Delivers one WR completion to the normal success/failure path.
    // Returns true when the completion resolved a live WR (and must be counted
    // in the JFC outstanding accounting). Returns false for completions from a
    // stale jetty generation, which were already resolved during the rebuild
    // flush and must be dropped entirely. When allow_error_trigger is false
    // (the caller already holds lock_ while rebuilding/draining), an
    // ACK_TIMEOUT completion is delivered as a plain failure without
    // re-entering onJettyError.
    bool processWrCompletion(
        urma_cr_t& cr, std::unordered_map<volatile int*, int>& jetty_depth_set,
        std::vector<Transport::Slice*>& failed_slices,
        std::vector<UbEndPoint*>& deferred_deletes, int jfc_index,
        bool allow_error_trigger);

    int recreateJettyUnlocked(int slot, urma_jfc_t* reuse_jfc,
                              urma_jfr_t* reuse_jfr);

    static constexpr uint64_t kJettyDrainTimeoutNs = 3000000000ull;  // 3s

   private:
    UrmaContext* context_;
    urma_token_t urma_token = {.token = 0xACFE};
    std::vector<urma_jetty_t*> jetty_list_;
    volatile int* wr_depth_list_;
    int max_wr_depth_;
    volatile int* jfc_outstanding_;
    std::unordered_map<urma_jetty_t*, urma_target_jetty_t*> imported_jetty_map_;

    std::vector<JettyState> jetty_state_;
    std::unordered_map<uint32_t, int> jetty_id_map_;
    std::vector<uint32_t> peer_jetty_id_;
    std::string peer_eid_;
    uint64_t drain_start_ns_ = 0;
    int draining_slot_ = -1;
    std::vector<uint64_t> jetty_epoch_;
};
}  // namespace mooncake
#endif  // URMA_ENDPOINT_H
