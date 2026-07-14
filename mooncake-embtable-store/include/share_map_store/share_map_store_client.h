#pragma once

#include <cstdint>
#include <memory>
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
// remote ShareMapStore service. It manages coro_rpc_client connections and
// reads temp result objects from Mooncake Store via TE read (get_buffer).
//
// Lifecycle of a remote query:
//   1. Client sends QueryDataRequest via coro_rpc to the remote service.
//   2. Remote service queries local ShareMap, packs results, writes to a
//      temp Mooncake Store object (TE write), returns the object key.
//   3. Client reads the temp object via get_buffer (TE read) → BufferHandle.
//   4. Client parses the packed buffer into StringViews.
//   5. Client removes the temp object.
//   6. BufferHandle is kept alive (stored in outBuffers) so StringViews
//      remain valid until the caller is done.
class ShareMapStoreClient {
   public:
    ShareMapStoreClient(std::shared_ptr<mooncake::RealClient> client,
                        const std::string& localHostname)
        : client_(std::move(client)), localHostname_(localHostname) {}

    ~ShareMapStoreClient() = default;

    // Query a single bucket on the remote node. On success:
    //   - buffers[i] points to value data (or empty if not found)
    //   - bufferHandles keeps the backing memory alive
    Status QueryData(const std::string& rpcEndpoint,
                     const std::string& bucketKey,
                     uint64_t valueSize,
                     const std::vector<uint64_t>& keys,
                     std::vector<StringView>& buffers,
                     std::vector<std::shared_ptr<mooncake::BufferHandle>>&
                         bufferHandles);

    // Batch query multiple buckets on the same remote node.
    Status BatchQueryData(
        const std::string& rpcEndpoint,
        const std::vector<std::string>& bucketKeys,
        uint64_t valueSize,
        const std::vector<std::vector<uint64_t>>& keysPerBucket,
        std::vector<std::vector<StringView>>& buffersPerBucket,
        std::vector<std::shared_ptr<mooncake::BufferHandle>>& bufferHandles);

    // Publish key/value pairs to a bucket on the remote node.
    Status Publish(const std::string& rpcEndpoint,
                   const std::string& bucketKey,
                   uint64_t valueSize,
                   const std::vector<uint64_t>& keys,
                   const std::vector<StringView>& values);

    // Build index for a bucket on the remote node.
    Status BuildIndex(const std::string& rpcEndpoint,
                      const std::string& bucketKey);

    // Check whether a bucket's data is stored locally (on this node) by
    // querying the bucket's ShareMapMeta ShareObject key. Returns true if a
    // replica is on local memory. Also returns the owning hostname (from
    // transport_endpoint_) for remote buckets.
    bool IsBucketLocal(const std::string& bucketKey,
                       std::string& ownerHostname);

    // Extract hostname from a transport_endpoint string like "host:port".
    static std::string ExtractHostname(const std::string& endpoint);

   private:
    // Get or create a coro_rpc_client for the given endpoint.
    coro_rpc::coro_rpc_client* GetClient(const std::string& rpcEndpoint);

    // Parse a packed single-bucket result buffer (from get_buffer) into
    // StringViews. Layout: for each key [1B found flag][valueSize bytes data].
    void ParseResultBuffer(
        void* data, uint64_t dataSize, uint64_t valueSize,
        size_t numKeys, const std::vector<int8_t>& foundFlags,
        std::vector<StringView>& buffers,
        std::shared_ptr<mooncake::BufferHandle> handle);

    // Parse an aggregated multi-bucket result buffer (from a single
    // get_buffer) into per-bucket StringViews. Layout:
    //   [bucketCount(8B)]
    //   for each bucket: [bucketKeyLen(4B)][bucketKey][keyCount(8B)]
    //                    for each key: [1B found flag][valueSize bytes data]
    // Returns a map from bucketKey -> (StringViews for that bucket's keys).
    void ParseAggregatedBuffer(
        void* data, uint64_t dataSize, uint64_t valueSize,
        const std::vector<std::string>& bucketKeys,
        const std::vector<std::vector<int8_t>>& foundFlagsPerBucket,
        std::vector<std::vector<StringView>>& buffersPerBucket,
        std::shared_ptr<mooncake::BufferHandle> handle);

    std::shared_ptr<mooncake::RealClient> client_;
    std::string localHostname_;
    // Cache of coro_rpc_client per endpoint.
    std::unordered_map<std::string, std::unique_ptr<coro_rpc::coro_rpc_client>>
        clientCache_;
    std::mutex cacheMutex_;
};

}  // namespace embtable
