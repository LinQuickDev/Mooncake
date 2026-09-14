#include "cvm/inter_master_rpc.h"

#include <algorithm>
#include <chrono>

#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>
#include <glog/logging.h>

#include "cvm/etcd_view_store.h"
#include "master_client.h"
#include "rpc_service.h"
#include "store_rpc_client_io_context.h"

namespace mooncake {
namespace cvm {

namespace {

std::string JoinStrings(const std::vector<std::string>& parts) {
    std::string out;
    for (const auto& p : parts) {
        if (!out.empty()) {
            out += ", ";
        }
        out += p;
    }
    return out;
}

}  // namespace

InterMasterRpcClient::InterMasterRpcClient()
    : pool_accessor_(GetStoreRpcClientIoContextPool(),
                     detail::MakeMasterRpcClientPoolConfig()) {}

InterMasterRpcClient::~InterMasterRpcClient() { Stop(); }

ErrorCode InterMasterRpcClient::Start(const std::string& cluster_namespace,
                                      const std::string& self_master_id) {
    if (running_.load()) {
        return ErrorCode::OK;
    }
    if (cluster_namespace.empty()) {
        LOG(WARNING)
            << "InterMasterRpcClient: no cluster namespace, refresh thread "
               "not started (manual member updates only)";
        return ErrorCode::INVALID_PARAMS;
    }
    cluster_namespace_ = cluster_namespace;
    self_master_id_ = self_master_id;
    running_.store(true);
    refresh_thread_ = std::thread([this] { RefreshLoop(); });
    free_thread_ = std::thread([this] { FreeLoop(); });
    LOG(INFO) << "InterMasterRpcClient started: cluster_namespace="
              << cluster_namespace_ << ", self=" << self_master_id_
              << ", refresh_interval_ms=" << kRefreshIntervalMs;
    return ErrorCode::OK;
}

void InterMasterRpcClient::Stop() {
    if (!running_.exchange(false)) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(cv_mutex_);
        cv_.notify_all();
    }
    {
        std::lock_guard<std::mutex> lock(free_queue_mutex_);
        free_cv_.notify_all();
    }
    if (refresh_thread_.joinable()) {
        refresh_thread_.join();
    }
    if (free_thread_.joinable()) {
        free_thread_.join();
    }
    LOG(INFO) << "InterMasterRpcClient stopped";
}

void InterMasterRpcClient::UpdateMembers(
    const std::vector<MasterRegistration>& members) {
    std::unordered_map<std::string, std::string> next;
    next.reserve(members.size());
    for (const auto& m : members) {
        if (m.master_id.empty() || m.address.empty()) {
            continue;
        }
        next[m.master_id] = m.address;
    }

    std::lock_guard<std::mutex> lock(members_mutex_);
    if (next != members_) {
        std::vector<std::string> joined, left;
        for (const auto& [id, addr] : next) {
            if (members_.find(id) == members_.end()) {
                joined.push_back(id + "@" + addr);
            }
        }
        for (const auto& [id, addr] : members_) {
            if (next.find(id) == next.end()) {
                left.push_back(id + "@" + addr);
            }
        }
        members_ = std::move(next);
        LOG(INFO) << "InterMasterRpc members updated: total=" << members_.size()
                  << ", joined=[" << JoinStrings(joined)
                  << "], left=[" << JoinStrings(left) << "]";
    }
}

std::vector<MasterRegistration> InterMasterRpcClient::GetMembers() const {
    std::lock_guard<std::mutex> lock(members_mutex_);
    std::vector<MasterRegistration> out;
    out.reserve(members_.size());
    for (const auto& [id, addr] : members_) {
        MasterRegistration reg;
        reg.master_id = id;
        reg.address = addr;
        out.push_back(std::move(reg));
    }
    return out;
}

std::optional<std::string> InterMasterRpcClient::ResolveAddress(
    const std::string& master_id) const {
    std::lock_guard<std::mutex> lock(members_mutex_);
    auto it = members_.find(master_id);
    if (it == members_.end()) {
        return std::nullopt;
    }
    return it->second;
}

tl::expected<InterMasterHandshakeResponse, ErrorCode>
InterMasterRpcClient::Handshake(const std::string& master_id) {
    auto address = ResolveAddress(master_id);
    if (!address) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return invoke_rpc<&WrappedMasterService::InterMasterHandshake,
                      InterMasterHandshakeResponse>(*address);
}

size_t InterMasterRpcClient::HandshakeAll(const std::string& self_master_id) {
    auto members = GetMembers();
    size_t ok_count = 0;
    for (const auto& m : members) {
        if (m.master_id == self_master_id) {
            continue;
        }
        auto result = Handshake(m.master_id);
        if (result.has_value()) {
            ++ok_count;
            LOG(INFO) << "InterMasterRpc handshake ok: peer="
                      << result.value().master_id
                      << " address=" << m.address
                      << " lease_id=" << result.value().lease_id
                      << " owned_slots=" << result.value().owned_slot_count
                      << " version=" << result.value().version;
        } else {
            LOG(WARNING) << "InterMasterRpc handshake failed: peer="
                         << m.master_id << " address=" << m.address
                         << " error=" << toString(result.error());
        }
    }
    return ok_count;
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
InterMasterRpcClient::AllocateReplicas(
    const std::string& master_id, const std::string& tenant_id,
    const std::string& key, uint64_t slice_length, uint64_t replica_num,
    const std::vector<std::string>& preferred_segments) {
    auto address = ResolveAddress(master_id);
    if (!address) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return invoke_rpc<&WrappedMasterService::InterMasterAllocateReplicas,
                      std::vector<Replica::Descriptor>>(
        *address, tenant_id, key, slice_length, replica_num,
        preferred_segments);
}

tl::expected<bool, ErrorCode> InterMasterRpcClient::FreeReplicas(
    const std::string& master_id, const std::string& tenant_id,
    const std::string& key) {
    auto address = ResolveAddress(master_id);
    if (!address) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return invoke_rpc<&WrappedMasterService::InterMasterFreeReplicas, bool>(
        *address, tenant_id, key);
}

tl::expected<GetReplicaListResponse, ErrorCode>
InterMasterRpcClient::GetReplicaList(const std::string& master_id,
                                     const std::string& key,
                                     const std::string& tenant_id) {
    auto address = ResolveAddress(master_id);
    if (!address) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return invoke_rpc<&WrappedMasterService::InterMasterGetReplicaList,
                      GetReplicaListResponse>(*address, key, tenant_id);
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
InterMasterRpcClient::BatchGetReplicaList(
    const std::string& master_id, const std::vector<std::string>& keys,
    const std::string& tenant_id) {
    auto address = ResolveAddress(master_id);
    if (!address) {
        std::vector<tl::expected<GetReplicaListResponse, ErrorCode>> errs(
            keys.size(), tl::make_unexpected(ErrorCode::INVALID_PARAMS));
        return errs;
    }
    // RPC 返回值本身已是逐 key 的 expected 向量；外层只关心传输层失败。
    auto result = invoke_rpc<
        &WrappedMasterService::InterMasterBatchGetReplicaList,
        std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>>(
        *address, keys, tenant_id);
    if (!result.has_value()) {
        return std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>(
            keys.size(), tl::make_unexpected(result.error()));
    }
    return std::move(result.value());
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
InterMasterRpcClient::PutStart(const std::string& master_id,
                               const UUID& client_id, const std::string& key,
                               const std::string& tenant_id,
                               uint64_t slice_length,
                               const ReplicateConfig& config) {
    auto address = ResolveAddress(master_id);
    if (!address) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return invoke_rpc<&WrappedMasterService::InterMasterPutStart,
                      std::vector<Replica::Descriptor>>(
        *address, client_id, key, tenant_id, slice_length, config);
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
InterMasterRpcClient::UpsertStart(const std::string& master_id,
                                  const UUID& client_id, const std::string& key,
                                  const std::string& tenant_id,
                                  uint64_t slice_length,
                                  const ReplicateConfig& config) {
    auto address = ResolveAddress(master_id);
    if (!address) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return invoke_rpc<&WrappedMasterService::InterMasterUpsertStart,
                      std::vector<Replica::Descriptor>>(
        *address, client_id, key, tenant_id, slice_length, config);
}

void InterMasterRpcClient::EnqueueBroadcastFree(const std::string& tenant_id,
                                                 const std::string& key) {
    if (!running_.load()) {
        VLOG(1) << "InterMasterRpc: broadcast free dropped (not running): key="
                << key;
        return;
    }
    {
        std::lock_guard<std::mutex> lock(free_queue_mutex_);
        // Bound the queue so a free storm cannot grow it unboundedly; the
        // entries cover handle reclamation, so dropping only leaks until the
        // segment is unmounted.
        if (free_queue_.size() >= kMaxBroadcastFreeQueueSize) {
            LOG(WARNING) << "InterMasterRpc: broadcast free queue full ("
                         << free_queue_.size() << "), dropping key=" << key;
            return;
        }
        free_queue_.push_back(FreeTask{tenant_id, key, 0});
    }
    free_cv_.notify_one();
}

void InterMasterRpcClient::FreeLoop() {
    constexpr int kMaxFreeAttempts = 20;  // ~10s when members not yet known
    while (running_.load()) {
        FreeTask task;
        {
            std::unique_lock<std::mutex> lock(free_queue_mutex_);
            free_cv_.wait(lock, [this] {
                return !running_.load() || !free_queue_.empty();
            });
            if (!running_.load()) {
                return;
            }
            task = std::move(free_queue_.front());
            free_queue_.pop_front();
        }

        auto members = GetMembers();
        if (members.empty()) {
            // Member table not populated yet (startup race): retry after a
            // short delay, bounded by kMaxFreeAttempts.
            if (++task.attempts < kMaxFreeAttempts) {
                {
                    std::lock_guard<std::mutex> lock(free_queue_mutex_);
                    free_queue_.push_back(std::move(task));
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            } else {
                LOG(WARNING)
                    << "InterMasterRpc: broadcast free gave up (no members): "
                    << "key=" << task.key;
            }
            continue;
        }

        for (const auto& member : members) {
            if (member.master_id.empty() || member.master_id == self_master_id_) {
                continue;
            }
            auto result = FreeReplicas(member.master_id, task.tenant_id,
                                       task.key);
            // Not-found (false) is the common case for non-owner peers.
            if (result.has_value() && result.value()) {
                LOG(INFO) << "InterMasterRpc: freed remote replicas on "
                         << member.master_id << " for key=" << task.key;
            } else if (!result.has_value()) {
                VLOG(1) << "InterMasterRpc: free failed on "
                        << member.master_id << " for key=" << task.key
                        << " error=" << toString(result.error());
            }
        }
    }
}

void InterMasterRpcClient::RefreshLoop() {
    while (running_.load()) {
        std::vector<MasterRegistration> masters;
        ViewVersionId version = 0;
        ErrorCode rc = EtcdViewStore::LoadAllMasters(cluster_namespace_,
                                                    masters, version);
        if (rc == ErrorCode::OK) {
            // Snapshot the previous table to detect newly joined peers.
            std::unordered_map<std::string, std::string> previous;
            {
                std::lock_guard<std::mutex> lock(members_mutex_);
                previous = members_;
            }
            UpdateMembers(masters);

            // Handshake newly joined peers: verifies the inter-master
            // channel and warms up the connection pools.
            std::vector<std::string> joined;
            for (const auto& m : masters) {
                if (m.master_id.empty() || m.master_id == self_master_id_) {
                    continue;
                }
                if (previous.find(m.master_id) == previous.end()) {
                    joined.push_back(m.master_id);
                }
            }
            for (const auto& peer_id : joined) {
                auto result = Handshake(peer_id);
                if (result.has_value()) {
                    LOG(INFO) << "InterMasterRpc handshake ok (new peer): peer="
                              << result.value().master_id
                              << " lease_id=" << result.value().lease_id
                              << " owned_slots="
                              << result.value().owned_slot_count
                              << " version=" << result.value().version;
                } else {
                    LOG(WARNING)
                        << "InterMasterRpc handshake failed (new peer): peer="
                        << peer_id << " error=" << toString(result.error());
                }
            }
        } else {
            LOG(WARNING) << "InterMasterRpc: failed to load masters from "
                            "etcd: "
                         << toString(rc);
        }
        std::unique_lock<std::mutex> lock(cv_mutex_);
        cv_.wait_for(lock, std::chrono::milliseconds(kRefreshIntervalMs),
                     [this] { return !running_.load(); });
    }
}

template <auto ServiceMethod, typename ReturnType, typename... Args>
tl::expected<ReturnType, ErrorCode> InterMasterRpcClient::invoke_rpc(
    const std::string& address, Args&&... args) {
    auto pool = pool_accessor_.GetOrCreateClientPool(address);
    auto rpc_result = async_simple::coro::syncAwait(
        [&]() -> async_simple::coro::Lazy<
                   tl::expected<ReturnType, ErrorCode>> {
            auto ret = co_await pool->send_request(
                [&](coro_io::client_reuse_hint,
                    coro_rpc::coro_rpc_client& client) {
                    return client.send_request<ServiceMethod>(
                        std::forward<Args>(args)...);
                });
            if (!ret.has_value()) {
                LOG(ERROR) << "InterMasterRpc: no available client for "
                           << address;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            auto result = co_await std::move(ret.value());
            if (!result) {
                if (result.error().code == coro_rpc::errc::timed_out) {
                    co_return tl::make_unexpected(ErrorCode::RPC_TIMEOUT);
                }
                LOG(ERROR) << "InterMasterRpc call failed on " << address
                           << ": " << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            if constexpr (std::is_void_v<ReturnType>) {
                result->result();
                co_return tl::expected<ReturnType, ErrorCode>{};
            } else {
                co_return result->result();
            }
        }());
    return rpc_result;
}

// Explicit instantiation is not required: invoke_rpc is only used within
// this translation unit (Handshake). Future forwarding methods live here too.

}  // namespace cvm
}  // namespace mooncake
