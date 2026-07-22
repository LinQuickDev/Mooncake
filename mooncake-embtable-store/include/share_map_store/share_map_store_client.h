#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "emb_types.h"
#include "share_map_store/share_map_store_rpc_service.h"
#include "share_map_store/share_map_store_rpc_types.h"
#include "real_client.h"
#include "client_buffer.hpp"
#include "ylt/coro_rpc/impl/coro_rpc_client.hpp"

namespace embtable {

// ShareMapStoreClient is the RPC client used by Bucket/EmbTable to call a
// remote ShareMapStore service. It owns one registered transfer buffer and
// sub-allocates non-overlapping result regions from it.
//
// Lifecycle of a remote query:
//   1. Client allocates a slice from its registered transfer buffer.
//   2. RPC sends that slice's endpoint/address/capacity as control metadata.
//   3. Remote service writes the packed result directly into the slice by TE.
//   4. Client parses the packed buffer into StringViews.
//   5. BufferHandle is kept alive (stored in outBuffers) so StringViews
//      remain valid until the caller is done.
class ShareMapStoreClient {
   public:
    explicit ShareMapStoreClient(std::shared_ptr<mooncake::RealClient> client)
        : client_(std::move(client)) {}

    ~ShareMapStoreClient();

    Status Init(uint64_t transferBufferSize);

    // Query a single bucket on the remote node. On success:
    //   - buffers[i] points to value data (or empty if not found)
    //   - bufferHandles keeps the backing memory alive
    Status QueryData(
        const std::string& rpcEndpoint, const std::string& bucketKey,
        uint64_t valueSize, const std::vector<uint64_t>& keys,
        std::vector<StringView>& buffers,
        std::vector<std::shared_ptr<mooncake::BufferHandle>>& bufferHandles);

    // Batch query multiple buckets on the same remote node.
    Status BatchQueryData(
        const std::string& rpcEndpoint,
        const std::vector<std::string>& bucketKeys, uint64_t valueSize,
        const std::vector<std::vector<uint64_t>>& keysPerBucket,
        std::vector<std::vector<StringView>>& buffersPerBucket,
        std::vector<std::shared_ptr<mooncake::BufferHandle>>& bufferHandles);

    // Publish key/value pairs to a bucket on the remote node.
    Status Publish(const std::string& rpcEndpoint, const std::string& bucketKey,
                   uint64_t valueSize, const std::vector<uint64_t>& keys,
                   const std::vector<StringView>& values);

    // Build index for a bucket on the remote node.
    Status BuildIndex(const std::string& rpcEndpoint,
                      const std::string& bucketKey);

    // Establish or reuse a connection to an EmbTable RPC endpoint.
    Status CheckEndpoint(const std::string& rpcEndpoint);

   private:
    struct RpcClientSlot {
        std::unique_ptr<coro_rpc::coro_rpc_client> client;
        bool inUse = false;
    };

    class RpcClientLease {
       public:
        RpcClientLease(ShareMapStoreClient* owner,
                       std::shared_ptr<RpcClientSlot> slot)
            : owner_(owner), slot_(std::move(slot)) {}
        RpcClientLease(RpcClientLease&& other) noexcept
            : owner_(other.owner_), slot_(std::move(other.slot_)) {
            other.owner_ = nullptr;
        }
        RpcClientLease(const RpcClientLease&) = delete;
        RpcClientLease& operator=(const RpcClientLease&) = delete;
        RpcClientLease& operator=(RpcClientLease&&) = delete;
        ~RpcClientLease();

        coro_rpc::coro_rpc_client* get() const {
            return slot_ ? slot_->client.get() : nullptr;
        }

       private:
        ShareMapStoreClient* owner_ = nullptr;
        std::shared_ptr<RpcClientSlot> slot_;
    };

    // Lease an exclusive coro_rpc_client for one in-flight request. The pool
    // grows with endpoint concurrency and reuses idle connections.
    std::optional<RpcClientLease> AcquireClient(const std::string& rpcEndpoint);
    void ReleaseClient(const std::shared_ptr<RpcClientSlot>& slot);

    // Parse a packed single-bucket result buffer (from get_buffer) into
    // StringViews. Layout: for each key [1B found flag][valueSize bytes data].
    Status ParseResultBuffer(void* data, uint64_t dataSize, uint64_t valueSize,
                             size_t numKeys, std::vector<StringView>& buffers,
                             std::shared_ptr<mooncake::BufferHandle> handle);

    Status ParseAggregatedBuffer(
        void* data, uint64_t dataSize, uint64_t valueSize,
        const std::vector<std::string>& bucketKeys,
        const std::vector<std::vector<uint64_t>>& keysPerBucket,
        std::vector<std::vector<StringView>>& buffersPerBucket,
        std::shared_ptr<mooncake::BufferHandle> handle);

    std::shared_ptr<mooncake::BufferHandle> AllocateTransferBuffer(
        uint64_t size);

    std::shared_ptr<mooncake::RealClient> client_;
    uint64_t transferBufferSize_ = 0;
    bool transferBufferRegistered_ = false;
    std::shared_ptr<mooncake::ClientBufferAllocator> transferAllocator_;
    // Logical TE segment name under which transferAllocator_ is published in
    // metadata. This is not the independently allocated TE RPC mapping port.
    std::string transferSegmentEndpoint_;
    // coro_rpc_client is not safe for concurrent calls. Each active request
    // leases a distinct slot while idle connections remain cached.
    std::unordered_map<std::string, std::vector<std::shared_ptr<RpcClientSlot>>>
        clientCache_;
    std::mutex cacheMutex_;
};

}  // namespace embtable
