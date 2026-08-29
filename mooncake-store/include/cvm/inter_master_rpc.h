#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "cvm/cvm_types.h"
#include "rpc_client_io_context.h"
#include "rpc_types.h"
#include "types.h"

namespace mooncake {

class WrappedMasterService;

namespace cvm {

// Inter-master RPC client for the CVM multi-submaster topology (plan B:
// forward allocation requests to the segment-owning submaster).
//
// Discovery is fully etcd-driven (loose coupling): a background thread
// periodically reloads the master registration table
// (/cvm/<ns>/masters/<master_id>) and maintains a master_id -> RPC address
// mapping. One coro_rpc client pool is cached per target address, so
// repeated forwarding reuses established connections.
//
// This first phase provides the handshake RPC used to verify the channel;
// forwarding methods (e.g. PutStart allocation forwarding to the segment
// owner) build on the same invoke_rpc core.
class InterMasterRpcClient {
   public:
    // Refresh period for the etcd-driven member table. Matches the CVM
    // membership/sync cadence (5s) so member add/remove is picked up with
    // comparable latency.
    static constexpr int kRefreshIntervalMs = 5000;

    // Upper bound for the pending broadcast-free queue (eviction storms
    // must not grow it unboundedly; dropped entries only leak handles until
    // the segment is unmounted).
    static constexpr size_t kMaxBroadcastFreeQueueSize = 100000;

    InterMasterRpcClient();

    ~InterMasterRpcClient();

    InterMasterRpcClient(const InterMasterRpcClient&) = delete;
    InterMasterRpcClient& operator=(const InterMasterRpcClient&) = delete;

    // Starts the member-refresh loop. `cluster_namespace` selects the CVM
    // namespace in etcd; empty disables the refresh thread (manual updates
    // only). `self_master_id` is excluded from peer handshakes. The etcd
    // client must already be connected (the supervisor's CvmController
    // connects it before serve).
    ErrorCode Start(const std::string& cluster_namespace,
                    const std::string& self_master_id = "");

    void Stop();

    // Replaces the member table (master_id -> address). Safe to call from
    // any thread; drops addresses of members that disappeared.
    void UpdateMembers(const std::vector<MasterRegistration>& members);

    // Current member table snapshot.
    std::vector<MasterRegistration> GetMembers() const;

    // Resolves the RPC address of `master_id` from the cached member table.
    std::optional<std::string> ResolveAddress(
        const std::string& master_id) const;

    // Handshakes the target submaster: returns its identity + ownership
    // summary. Used both for channel verification and liveness probing.
    tl::expected<InterMasterHandshakeResponse, ErrorCode> Handshake(
        const std::string& master_id);

    // Handshakes every known member except `self_master_id` (logging-only
    // helper for startup verification). Returns the number of successes.
    size_t HandshakeAll(const std::string& self_master_id);

    // ----- Allocation forwarding (CVM plan B phase 2) -----

    // Asks the target submaster (segment owner) to allocate memory replicas
    // in its locally mounted segments. When `preferred_segments` is
    // non-empty the allocation is strict: only those segments are used.
    // The peer keeps the real handles alive; the caller only materializes
    // dummy-allocator replicas from the returned descriptors.
    tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
    AllocateReplicas(const std::string& master_id, const std::string& tenant_id,
                     const std::string& key, uint64_t slice_length,
                     uint64_t replica_num,
                     const std::vector<std::string>& preferred_segments);

    // Frees replicas previously allocated via AllocateReplicas on the
    // target submaster. Returns true when a keepalive entry was found.
    tl::expected<bool, ErrorCode> FreeReplicas(const std::string& master_id,
                                               const std::string& tenant_id,
                                               const std::string& key);

    // Fire-and-forget: broadcast the free to every known peer. Used when the
    // slot owner erases an object whose handles live on a segment-owning
    // peer (it does not track which peer, so all peers are asked; only the
    // owner of the keepalive entry reacts).
    void EnqueueBroadcastFree(const std::string& tenant_id,
                              const std::string& key);

    // ----- Read forwarding (CVM plan B phase 2) -----

    // Forwards a single-key GetReplicaList to the slot-owning submaster.
    // The target performs a local query only (no re-forward), so an
    // inconsistent view terminates the forward chain at the first hop.
    tl::expected<GetReplicaListResponse, ErrorCode> GetReplicaList(
        const std::string& master_id, const std::string& key,
        const std::string& tenant_id);

    // Forwards a batch GetReplicaList to the slot-owning submaster.
    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(const std::string& master_id,
                        const std::vector<std::string>& keys,
                        const std::string& tenant_id);

   private:
    // Generic sync RPC invocation against the pool of the target address.
    // Defined in the .cpp (needs the complete WrappedMasterService type).
    template <auto ServiceMethod, typename ReturnType, typename... Args>
    tl::expected<ReturnType, ErrorCode> invoke_rpc(const std::string& address,
                                                   Args&&... args);

    // Refreshes the member table from etcd and handshakes newly joined peers
    // (channel verification + connection warmup).
    void RefreshLoop();

    // Drains the broadcast-free queue: for each task, asks every known peer
    // to free the keepalive entry for (tenant, key) if it holds one.
    void FreeLoop();

    struct FreeTask {
        std::string tenant_id;
        std::string key;
        int attempts{0};
    };

    mutable std::mutex members_mutex_;
    std::unordered_map<std::string, std::string> members_;  // id -> address

    std::string cluster_namespace_;
    std::string self_master_id_;
    std::atomic<bool> running_{false};
    std::thread refresh_thread_;
    std::thread free_thread_;
    std::mutex cv_mutex_;
    std::condition_variable cv_;

    std::mutex free_queue_mutex_;
    std::condition_variable free_cv_;
    std::deque<FreeTask> free_queue_;

    // Per-address cached coro_rpc client pools (see RpcClientPool).
    RpcClientPool pool_accessor_;
};

}  // namespace cvm
}  // namespace mooncake
