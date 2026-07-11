#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "embtable/emb_table/emb_table_meta.h"
#include "embtable/share_map_store/share_map_store.h"
#include "embtable/types.h"
#include "real_client.h"

namespace embtable {

// Bucket is a value-size-homogeneous data aggregation unit (design doc 4.1).
// It holds a local write buffer for batched inserts and delegates to
// ShareMapStore for Publish / QueryData / BuildIndex.
class Bucket {
   public:
    Bucket(BucketInfo info,
           std::shared_ptr<ShareMapStore> shareMapStore,
           std::shared_ptr<mooncake::RealClient> realClient);

    // Append key/value pairs to the local buffer. All values must be
    // info_.valueSize bytes.
    Status Insert(const std::vector<uint64_t>& keys,
                  const std::vector<StringView>& values);

    // Flush the local buffer to ShareMapStore::Publish, then clear it.
    Status Flush();

    // Query already-published data via ShareMapStore::QueryData.
    Status Find(const std::vector<uint64_t>& keys,
                std::vector<StringView>& buffers);

    // Build the perfect-hash index for this bucket (flushes first).
    Status BuildIndex();

    const BucketInfo& Info() const { return info_; }
    const std::string& BucketKey() const { return bucketKey_; }
    uint64_t PendingCount() const { return localKeys_.size(); }

   private:
    BucketInfo info_;
    std::string bucketKey_;
    std::shared_ptr<ShareMapStore> shareMapStore_;
    std::shared_ptr<mooncake::RealClient> realClient_;

    // Local write buffer for batched inserts (design doc 4.1.4 Insert flow).
    std::vector<uint64_t> localKeys_;
    std::vector<std::string> localValues_;
};

}  // namespace embtable
