#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "emb_table/emb_table_meta.h"
#include "share_map_store/share_map_store.h"
#include "share_map_store/share_map_store_client.h"
#include "emb_types.h"
#include "real_client.h"
#include "client_buffer.hpp"

namespace embtable {

// Bucket is a value-size-homogeneous data aggregation unit (design doc 4.1).
// It holds a local write buffer for batched inserts and delegates to
// ShareMapStore for Publish / QueryData / BuildIndex.
//
// Node awareness:
//   - On first Find/Flush/BuildIndex, the bucket determines whether its data
//     is stored locally or on a remote node by querying bucket meta.
//   - If local, it calls the local ShareMapStore directly.
//   - If remote, it uses ShareMapStoreClient to make RPC calls to the remote
//     ShareMapStore service.
class Bucket {
   public:
    Bucket(BucketInfo info,
           std::shared_ptr<ShareMapStore> shareMapStore,
           std::shared_ptr<mooncake::RealClient> realClient,
           std::shared_ptr<ShareMapStoreClient> shareMapStoreClient = nullptr,
           const std::string& localHostname = "",
           uint16_t shareMapStoreRpcPort = 0);

    // Append key/value pairs to the local buffer. All values must be
    // info_.valueSize bytes. Sets wouldFlush=true when the local buffer
    // reaches the configured capacity threshold (design doc 4.1.4).
    Status Insert(const std::vector<uint64_t>& keys,
                  const std::vector<StringView>& values,
                  bool& wouldFlush);

    // Convenience overload (ignores wouldFlush signal).
    Status Insert(const std::vector<uint64_t>& keys,
                  const std::vector<StringView>& values);

    // Flush the local buffer to ShareMapStore::Publish, then clear it.
    Status Flush();

    // Whether the local buffer has reached the capacity threshold.
    bool IsFull() const;

    // Query already-published data via ShareMapStore::QueryData.
    // Keeps bufferHandles alive so returned StringViews remain valid.
    Status Find(const std::vector<uint64_t>& keys,
                std::vector<StringView>& buffers,
                std::vector<std::shared_ptr<mooncake::BufferHandle>>&
                    bufferHandles);

    // Build the perfect-hash index for this bucket (flushes first).
    Status BuildIndex();

    BucketInfo Info() const;
    const std::string& BucketKey() const { return bucketKey_; }
    uint64_t PendingCount() const;
    uint64_t FlushThreshold() const;
    void SetFlushThreshold(uint64_t threshold);

    // Resolve the current owner from bucket-meta replica placement. This is
    // intentionally re-evaluated so bucket relocation is observed.
    Status ResolveLocality();

    // Whether this bucket's data is stored locally on this node.
    bool IsLocal() const;
    std::string RpcEndpoint() const;

   private:
    void InvalidateLocality();

    BucketInfo info_;
    std::string bucketKey_;
    std::shared_ptr<ShareMapStore> shareMapStore_;
    std::shared_ptr<mooncake::RealClient> realClient_;
    std::shared_ptr<ShareMapStoreClient> shareMapStoreClient_;
    std::string localHostname_;
    uint16_t shareMapStoreRpcPort_ = 0;

    // Local write buffer for batched inserts (design doc 4.1.4 Insert flow).
    std::vector<uint64_t> localKeys_;
    std::vector<std::string> localValues_;
    // Capacity threshold: when localKeys_.size() >= flushThreshold_, the
    // bucket should be flushed. Default 4096 (design doc 4.1.4).
    uint64_t flushThreshold_ = 4096;

    mutable std::mutex bufferMutex_;
    mutable std::mutex flushMutex_;
    mutable std::mutex localityMutex_;
    bool isLocal_ = true;
    bool localityResolved_ = false;
    std::chrono::steady_clock::time_point localityResolvedAt_{};
};

}  // namespace embtable
