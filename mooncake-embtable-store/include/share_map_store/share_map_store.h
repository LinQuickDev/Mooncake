#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "share_object/share_map.h"
#include "emb_types.h"
#include "real_client.h"

namespace embtable {

// DeploymentConfig captures the parameters ShareMapStore needs to connect to
// the Mooncake master and (optionally) expose an RPC endpoint.
struct DeploymentConfig {
    std::string masterAddress = "127.0.0.1:50051";
    std::string protocol = "tcp";
    std::string deviceNames;
    std::string metadataServer = "http://127.0.0.1:8080/metadata";
    // RPC service port for EmbTableClient to reach this ShareMapStore.
    // 0 disables the RPC server (local-only mode).
    uint16_t rpcPort = 0;
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
    explicit ShareMapStore(DeploymentConfig config);

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

    // Query data and write the packed result buffer to a temporary Mooncake
    // Store object. Used by the RPC service so remote callers can fetch the
    // packed buffer via TE read (get_buffer). The result buffer layout is:
    //   for each key: [1-byte found flag][valueSize bytes data]
    // resultObjectSize is set to keys.size() * (1 + valueSize).
    Status QueryDataToStore(const std::string& bucketKey,
                            const std::vector<uint64_t>& keys,
                            uint64_t valueSize,
                            std::string& resultObjectKey,
                            uint64_t& resultObjectSize,
                            std::vector<int8_t>& foundFlags);

    // Batch query: query multiple buckets on this node, pack all results
    // into ONE aggregated buffer, and write it to a single temporary Mooncake
    // Store object (one TE write). Layout:
    //   [bucketCount(8B)]
    //   for each bucket: [bucketKeyLen(4B)][bucketKey][keyCount(8B)]
    //                    for each key: [1B found flag][valueSize bytes data]
    // Each entry in resultObjectKeys/foundFlagsPerBucket is also filled so
    // the client knows how to parse the per-bucket segments.
    Status BatchQueryDataToStore(
        const std::vector<std::string>& bucketKeys,
        const std::vector<std::vector<uint64_t>>& keysPerBucket,
        uint64_t valueSize,
        std::vector<std::string>& resultObjectKeys,
        std::vector<uint64_t>& resultObjectSizes,
        std::vector<std::vector<int8_t>>& foundFlagsPerBucket);

    // Expose the underlying Mooncake client (used by RPC service and Bucket).
    std::shared_ptr<mooncake::RealClient> GetClient() const {
        return realClient_;
    }

    const DeploymentConfig& Config() const { return config_; }
    bool IsInitialized() const { return initialized_; }

   private:
    // Get-or-create a ShareMap for the given bucket. valueSize is required
    // when creating a new ShareMap; passing 0 keeps any existing ShareMap's
    // valueSize and returns an error if the bucket does not exist.
    Status getOrCreateShareMap(const std::string& bucketKey,
                               uint64_t valueSize,
                               std::shared_ptr<ShareMap>& out);

    DeploymentConfig config_;
    std::shared_ptr<mooncake::RealClient> realClient_;
    std::unordered_map<std::string, std::shared_ptr<ShareMap>> shareMaps_;
    mutable std::mutex mutex_;
    bool initialized_ = false;
};

}  // namespace embtable
