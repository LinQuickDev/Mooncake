#pragma once

#include <atomic>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "share_object/index_object.h"
#include "share_object/share_map_meta.h"
#include "share_object/vector_object.h"
#include "emb_types.h"
#include "real_client.h"

namespace embtable {

// ShareMap is a HashMap over a single Bucket's data, composed of:
//   - a VectorObject for keys (elemSize = sizeof(uint64_t))
//   - a VectorObject for values (elemSize = valueSize)
//   - an IndexObject (perfect hash) for O(1) lookups
//   - a ShareMapMeta recording the backing ShareObjects
//
// Lifecycle (design doc section 8.6):
//   Unpublished -- Insert() appends key/value records (linear-scan Lookup).
//   BuildIndex() -- builds PHF, Publishes all ShareObjects, sets Published.
//   Published   -- read-only; Insert() returns kIndexBuilt.
class ShareMap {
   public:
    ShareMap(const std::string& bucketKey, uint64_t valueSize,
             std::shared_ptr<mooncake::RealClient> realClient,
             uint64_t shareObjectSize = 64ull * 1024 * 1024);

    // Append key/value records. Fails after BuildIndex().
    Status Insert(const std::vector<uint64_t>& keys,
                  const std::vector<StringView>& values);

    // Batch lookup. In Published mode uses the PHF; otherwise linear scan.
    Status Lookup(const std::vector<uint64_t>& keys,
                  std::vector<StringView>& buffers) const;

    // Build the PHF index and Publish all backing ShareObjects. Transitions
    // the ShareMap to read-only Published state.
    Status BuildIndex();

    // Reconstruct a Published ShareMap from Mooncake Store (other nodes).
    Status Import();

    ShareMapMeta* GetMeta() const { return meta_.get(); }
    uint64_t ValueSize() const { return valueSize_; }
    uint64_t Size() const { return size_.load(std::memory_order_acquire); }
    bool IsPublished() const {
        return published_.load(std::memory_order_acquire);
    }

   private:
    Status linearLookup(const std::vector<uint64_t>& keys,
                        std::vector<StringView>& buffers) const;

    std::string bucketKey_;
    uint64_t valueSize_;
    std::unique_ptr<VectorObject> keyVec_;
    std::unique_ptr<VectorObject> valueVec_;
    std::unique_ptr<IndexObject> indexObj_;
    std::unique_ptr<ShareMapMeta> meta_;
    std::shared_ptr<mooncake::RealClient> realClient_;
    std::atomic<uint64_t> size_{0};
    std::atomic<bool> published_{false};
    std::atomic<bool> inconsistent_{false};
    mutable std::shared_mutex rwMutex_;
};

}  // namespace embtable
