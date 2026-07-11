#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "embtable/emb_table/emb_table_bucket.h"
#include "embtable/emb_table/emb_table_meta.h"
#include "embtable/share_map_store/share_map_store.h"
#include "embtable/types.h"
#include "real_client.h"

namespace embtable {

// EmbTable is the logical embedding table (design doc 4.1). It shards data
// across numBuckets_ buckets by hashing the key, and delegates each bucket's
// storage to ShareMapStore.
class EmbTable {
   public:
    EmbTable(const std::string& tableName, uint32_t numBuckets,
             uint64_t valueSize,
             std::shared_ptr<ShareMapStore> shareMapStore,
             std::shared_ptr<mooncake::RealClient> realClient);

    // Initialize EmbTableMeta (create or query) and pre-create buckets.
    Status Init(bool createNew);

    // Insert key/value pairs (routed to per-key buckets). Flushes each
    // touched bucket afterwards.
    Status Insert(const std::vector<uint64_t>& keys,
                  const std::vector<StringView>& values);

    // Batch find across all buckets. Values are returned in the same order
    // as keys; missing keys yield empty StringViews.
    Status Find(const std::vector<uint64_t>& keys,
                std::vector<StringView>& buffers);

    // Build the perfect-hash index on every bucket (flushes first).
    Status BuildIndex();

    // Load is not implemented in the first version (design doc 8.x).
    Status Load(const std::vector<std::string>& keyFiles,
                const std::vector<std::string>& valueFiles,
                const std::string& format);

    // Delete is not supported after BuildIndex (read-only).
    Status Delete(const std::vector<uint64_t>& keys);

    const std::string& TableName() const { return tableName_; }
    uint32_t NumBuckets() const { return numBuckets_; }
    uint64_t ValueSize() const { return valueSize_; }
    const TableMetaInfo& MetaInfo() const;
    std::shared_ptr<Bucket> GetBucket(uint32_t index);

   private:
    uint32_t RouteToBucket(uint64_t key) const;

    std::string tableName_;
    uint32_t numBuckets_;
    uint64_t valueSize_;
    std::shared_ptr<ShareMapStore> shareMapStore_;
    std::shared_ptr<mooncake::RealClient> realClient_;
    std::shared_ptr<EmbTableMeta> meta_;
    std::vector<std::shared_ptr<Bucket>> buckets_;
};

}  // namespace embtable
