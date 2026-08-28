#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cvm/cvm_types.h"
#include "mutex.h"
#include "types.h"

namespace mooncake {
namespace cvm {

class CvmServiceDelegate;
class CvmHttpServer;

// In-process control plane for the CVM (Cache View Master).
//
// Responsibilities (P1 skeleton):
//   - Register this master under an etcd lease (liveness + role).
//   - Load and continuously watch the KV view (slot -> primary master).
//   - Expose local read access to the cached view (OwnsSlot / GetKvView).
//
// The full slot-migration / failover / segment-view state machines are added
// in later phases.
class CvmController {
   public:
    struct Config {
        std::string cluster_namespace;
        std::string master_id;
        std::string address;  // RPC endpoint of this master.
        MasterRole role = MasterRole::kPrimary;
        int64_t registration_lease_ttl_sec = 3;

        // CvmHttpServer（外部 HTTP 接口）配置；http_port == 0 表示不启动。
        std::string http_host = "0.0.0.0";
        uint16_t http_port = 0;

        // 集群中允许同时 serving 的 submaster 上限（名额协调，先到先得）。
        // 排名前 submaster_count 个 master 为 kPrimary，其余降级为 kStandby。
        uint32_t submaster_count = 1;

        // 视图快照生成（SyncOnce）的调度周期。
        std::chrono::milliseconds sync_interval{5000};
    };

    explicit CvmController(Config config);
    ~CvmController();

    CvmController(const CvmController&) = delete;
    CvmController& operator=(const CvmController&) = delete;

    void SetDelegate(CvmServiceDelegate* delegate);

    // 当前角色（随名额协调动态变化）。
    MasterRole GetCurrentRole() const { return current_role_.load(); }

    // 排名第一（先到先得）的 primary 的 RPC 地址，作为 standby 的单源回放
    // 目标。当成员列表为空或本机即为排名第一的 primary 时返回空字符串。
    std::string GetPrimaryAddress();

    // 排名前 submaster_count 的 primary（submaster）成员（含 master_id 与
    // address），作为 standby 的多源回放目标。排除本机；成员列表为空或本机
    // 覆盖全部 primary 名额时返回空列表。
    std::vector<MasterRegistration> GetPrimaryPeers();

    // standby 动态绑定（2c）：按「本 standby 负责的 slot 区间」过滤出拥有这些
    // slot 的 primary，作为回放源（而非回放全部 primary）。本机为 primary 或
    // 无法确定负责区间时返回空列表。
    std::vector<MasterRegistration> GetBindingSources();

    ErrorCode Start();
    void Stop();

    // 调度 EtcdViewStore 聚合原始记录 -> 生成视图快照 -> 回写 etcd。
    ErrorCode SyncOnce();

    // 把最新视图快照路径推送给 CvmHttpServer。
    void PushViewPaths();

    // etcd lease id backing this master's registration. Callers may reuse it
    // for their own records (slot/segment ownership) so they share the same
    // lifecycle and are auto-removed on master death. 0 until Start() succeeds.
    EtcdLeaseId GetLeaseId() const { return lease_id_; }

    // Whether this master currently owns `slot` as primary.
    bool OwnsSlot(uint16_t slot) const;

    // Snapshot of the cached KV view and its version.
    std::vector<SlotOwner> GetKvView() const;
    ViewVersionId GetKvViewVersion() const;

   private:
    struct WatchState {
        std::mutex mutex;
        std::condition_variable cv;
        bool dirty = false;
        bool broken = false;
    };

    Config config_;
    CvmServiceDelegate* delegate_ = nullptr;

    mutable SharedMutex view_mutex_;
    std::unordered_map<uint16_t, SlotOwner> kv_view_;
    ViewVersionId kv_view_version_{0};

    EtcdLeaseId lease_id_{0};
    std::atomic<bool> running_{false};
    std::atomic<bool> watch_armed_{false};
    std::atomic<bool> masters_watch_armed_{false};

    // 当前角色（随名额协调动态变化）；初始为启动配置的 role。
    std::atomic<MasterRole> current_role_{MasterRole::kPrimary};

    std::unique_ptr<WatchState> watch_state_;
    std::unique_ptr<WatchState> masters_watch_state_;
    std::thread watch_thread_;
    std::thread masters_watch_thread_;
    std::thread keepalive_thread_;
    std::thread sync_thread_;
    std::thread membership_thread_;

    std::mutex sync_mutex_;
    std::condition_variable sync_cv_;

    std::mutex membership_mutex_;
    std::condition_variable membership_cv_;

    // 视图类型 -> etcd 快照路径（"kv_view"/"segment_view"）。
    std::unordered_map<std::string, std::string> view_paths_;
    std::unique_ptr<CvmHttpServer> http_server_;

    ErrorCode RefreshKvView();
    void CancelWatchAndWait();
    void WatchLoop();
    void MastersWatchLoop();
    void KeepaliveLoop();
    void SyncLoop();
    void MembershipLoop();
    // 重算并回写角色（先到先得排名）；membership 轮询与 masters watch 回调
    // 共用，用 CAS 去重，保证并发下仅一次迁移与通知。
    void ReconcileRole();
    MasterRole ComputeDesiredRole();
    // 加载所有存活 master 并按「先到先得」排序（registered_at_ms 升序，
    // tie-break master_id 字典序）。失败返回 false。
    bool LoadRankedMembers(std::vector<MasterRegistration>& out);

    static void WatchCallback(void* ctx, const char* key, size_t key_size,
                              const char* value, size_t value_size,
                              int event_type, int64_t mod_revision);
};

}  // namespace cvm
}  // namespace mooncake
