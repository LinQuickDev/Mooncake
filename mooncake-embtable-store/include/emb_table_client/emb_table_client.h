#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "emb_table/emb_table.h"
#include "share_map_store/share_map_store.h"
#include "emb_types.h"
#include "real_client.h"

namespace embtable {

// EmbTableClient is the top-level facade (design doc 3.1, 4.x). In
// co-located deployment it owns an EmbTable + ShareMapStore + RealClient
// directly; in disaggregated deployment a DummyClient variant forwards to a
// remote EmbTableClient via SHM/RPC.
//
// This first version implements the co-located (local) path. RPC forwarding
// is tracked as a follow-up (design doc 8.x).
class EmbTableClient {
   public:
    struct Options {
        std::string tableName;
        uint32_t numBuckets = 16;
        uint64_t valueSize = 0;        // mandatory: bytes per value
        DeploymentConfig deployment;
        bool createNew = true;         // create table meta on Init (vs query)
    };

    explicit EmbTableClient(Options options);

    ~EmbTableClient();

    EmbTableClient(const EmbTableClient&) = delete;
    EmbTableClient& operator=(const EmbTableClient&) = delete;

    // Initialize ShareMapStore, RealClient, and EmbTable.
    Status Init();

    // Insert key/value pairs. Routes to per-key buckets and flushes.
    Status Insert(const std::vector<uint64_t>& keys,
                  const std::vector<StringView>& values);

    // Batch find. buffers[i] is the value for keys[i] (empty if missing).
    Status Find(const std::vector<uint64_t>& keys,
                std::vector<StringView>& buffers);

    // Build the perfect-hash index on all buckets (read-only afterwards).
    Status BuildIndex();

    const std::string& TableName() const { return options_.tableName; }
    uint32_t NumBuckets() const;
    uint64_t ValueSize() const;

   private:
    Options options_;
    std::shared_ptr<mooncake::RealClient> realClient_;
    std::shared_ptr<ShareMapStore> shareMapStore_;
    std::shared_ptr<EmbTable> embTable_;
};

}  // namespace embtable
