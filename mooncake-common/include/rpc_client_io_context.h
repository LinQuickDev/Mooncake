#pragma once

#include <cstdint>
#include <chrono>
#include <csignal>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <ylt/coro_io/client_pool.hpp>
#include <ylt/coro_io/io_context_pool.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

namespace mooncake {

std::shared_ptr<coro_io::io_context_pool> CreateRpcClientIoContextPool(
    uint32_t thread_count);

template <typename PoolTag>
coro_io::io_context_pool& GetRpcClientIoContextPool(uint32_t thread_count) {
    static const auto io_pool = CreateRpcClientIoContextPool(thread_count);
    return *io_pool;
}

/**
 * A client pool accessor that caches one client pool per target address, so
 * callers that alternate between several targets (e.g. submaster routing)
 * reuse existing connections instead of recreating a pool on every switch.
 * The most recently selected pool is also exposed via GetClientPool().
 */
class RpcClientPool {
   public:
    using ClientPool = coro_io::client_pool<coro_rpc::coro_rpc_client>;
    using PoolConfig = ClientPool::pool_config;

    explicit RpcClientPool(coro_io::io_context_pool& io_context_pool,
                           PoolConfig config = {})
        : io_context_pool_(io_context_pool), config_(std::move(config)) {
        // Explicit target selection supersedes background recovery of an old
        // host; the pool's own connect/retry still applies per request.
        config_.host_alive_detect_duration = std::chrono::seconds(0);
    }

    std::shared_ptr<ClientPool> GetOrCreateClientPool(
        std::string_view address) {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        std::string addr(address);
        auto it = pools_.find(addr);
        if (it == pools_.end()) {
            auto pool = ClientPool::create(addr, config_, io_context_pool_);
            it = pools_.emplace(addr, std::move(pool)).first;
        }
        address_ = std::move(addr);
        client_pool_ = it->second;
        return client_pool_;
    }

    std::shared_ptr<ClientPool> GetClientPool() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return client_pool_;
    }

    std::string GetAddress() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return address_;
    }

   private:
    mutable std::shared_mutex mutex_;
    coro_io::io_context_pool& io_context_pool_;
    PoolConfig config_;
    std::string address_;
    std::shared_ptr<ClientPool> client_pool_;
    // Cache of client pools keyed by target address, so switching back and
    // forth between submaster addresses reuses existing connections instead of
    // recreating the pool on every switch.
    std::unordered_map<std::string, std::shared_ptr<ClientPool>> pools_;
};

}  // namespace mooncake
