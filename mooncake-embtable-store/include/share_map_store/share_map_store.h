#pragma once

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "share_object/share_map.h"
#include "emb_types.h"
#include "client_buffer.hpp"
#include "real_client.h"

namespace embtable {

// DeploymentConfig captures the parameters ShareMapStore needs to connect to
// the Mooncake master and (optionally) expose an RPC endpoint.
struct DeploymentConfig {
    std::string masterAddress = "127.0.0.1:50051";
    std::string protocol = "tcp";
    std::string deviceNames;
    std::string metadataServer = "http://127.0.0.1:8080/metadata";
    // Mooncake Store segment and client-local allocator sizes.
    uint64_t globalSegmentSize = 16ull * 1024 * 1024;
    uint64_t localBufferSize = 16ull * 1024 * 1024;
    // RPC service port for EmbTableClient to reach this ShareMapStore.
    // Used only when enableEmbTableRpc is true.
    uint16_t rpcPort = 0;
    // Enable the EmbTable/ShareMapStore RPC services. Keep false for
    // co-process deployments; the standalone storage node enables it.
    bool enableEmbTableRpc = false;
    // Registered staging buffer used by the ShareMapStore RPC service for
    // direct Transfer Engine writes to remote EmbTable clients.
    uint64_t transferBufferSize = 64ull * 1024 * 1024;
    // Number of independently registered staging buffers available to
    // concurrent RPC handlers. A value of zero is treated as one.
    uint32_t transferBufferCount = 4;
    // Default per-bucket ShareObject size (design doc 8.2).
    uint64_t shareObjectSize = 64ull * 1024 * 1024;
};

// ShareMapStore is the middle management layer (design doc 4.2):
//   - owns a Mooncake RealClient
//   - manages a collection of ShareMap objects keyed by bucketKey
//   - exposes Publish / QueryData / BuildIndex operations
//
// The RPC server (coro_rpc) registration is performed by EmbTableClient;
// ShareMapStore itself stays transport-agnostic so it can be unit-tested
// without a running RPC stack.
class ShareMapStore {
   public:
    explicit ShareMapStore(DeploymentConfig config,
                           std::string localHostname = "");

    ~ShareMapStore();

    ShareMapStore(const ShareMapStore&) = delete;
    ShareMapStore& operator=(const ShareMapStore&) = delete;

    // Initialize the underlying Mooncake RealClient. Must be called before
    // any Publish/QueryData/BuildIndex call.
    Status Init();

    // Publish a batch of key/value pairs into the bucket identified by
    // bucketKey. Values must all be exactly valueSize bytes. Creates the
    // ShareMap on first use.
    Status Publish(const std::string& bucketKey, uint64_t valueSize,
                   const std::vector<uint64_t>& keys,
                   const std::vector<StringView>& values);

    // Batch lookup against a bucket. On success buffers[i] points to the
    // value for keys[i] (or empty if not found).
    Status QueryData(const std::string& bucketKey,
                     const std::vector<uint64_t>& keys,
                     std::vector<StringView>& buffers);

    // Build the perfect-hash index for a bucket and Publish all backing
    // ShareObjects. The bucket becomes read-only afterwards.
    Status BuildIndex(const std::string& bucketKey);

    // Import an already-published ShareMap from Mooncake Store (other nodes).
    Status Import(const std::string& bucketKey);

    // Access an existing ShareMap (nullptr if absent).
    std::shared_ptr<ShareMap> GetShareMap(const std::string& bucketKey);

    // Query data, pack it in the service's registered transfer buffer, and
    // write it directly to a registered remote client buffer through TE.
    // The result buffer layout is:
    //   for each key: [1-byte found flag][valueSize bytes data]
    Status QueryDataToBuffer(const std::string& bucketKey,
                             const std::vector<uint64_t>& keys,
                             uint64_t valueSize,
                             const std::string& targetEndpoint,
                             uint64_t targetAddress, uint64_t targetCapacity,
                             uint64_t& transferredSize,
                             std::vector<int8_t>& foundFlags);

    // Batch query: query multiple buckets on this node, pack all results into
    // ONE aggregated buffer, and write it directly to the client's registered
    // buffer with one TE transfer. Layout:
    //   [bucketCount(8B)]
    //   for each bucket: [bucketKeyLen(4B)][bucketKey][keyCount(8B)]
    //                    for each key: [1B found flag][valueSize bytes data]
    Status BatchQueryDataToBuffer(
        const std::vector<std::string>& bucketKeys,
        const std::vector<std::vector<uint64_t>>& keysPerBucket,
        uint64_t valueSize, const std::string& targetEndpoint,
        uint64_t targetAddress, uint64_t targetCapacity,
        uint64_t& transferredSize,
        std::vector<std::vector<int8_t>>& foundFlagsPerBucket);

    // Expose the underlying Mooncake client (used by RPC service and Bucket).
    std::shared_ptr<mooncake::RealClient> GetClient() const {
        return realClient_;
    }
    std::shared_ptr<mooncake::RealClient> GetRealClient() const {
        return realClient_;
    }

    const DeploymentConfig& Config() const { return config_; }
    bool IsInitialized() const { return initialized_; }

   private:
    struct TransferBufferSlot {
        std::shared_ptr<mooncake::ClientBufferAllocator> allocator;
        bool registered = false;
        bool inUse = false;
    };

    size_t AcquireTransferBuffer();
    void ReleaseTransferBuffer(size_t index);

    // Get-or-create a ShareMap for the given bucket. valueSize is required
    // when creating a new ShareMap; passing 0 keeps any existing ShareMap's
    // valueSize and returns an error if the bucket does not exist.
    Status getOrCreateShareMap(const std::string& bucketKey, uint64_t valueSize,
                               std::shared_ptr<ShareMap>& out);

    DeploymentConfig config_;
    std::string localHostname_;
    std::shared_ptr<mooncake::RealClient> realClient_;
    std::unordered_map<std::string, std::shared_ptr<ShareMap>> shareMaps_;
    mutable std::mutex mutex_;
    std::vector<TransferBufferSlot> transferBuffers_;
    std::mutex transferBufferMutex_;
    std::condition_variable transferBufferCv_;
    bool initialized_ = false;
};

}  // namespace embtable
