#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "emb_table/emb_table.h"
#include "share_map_store/share_map_store.h"
#include "share_map_store/share_map_store_client.h"
#include "share_map_store/share_map_store_rpc_service.h"
#include "emb_types.h"
#include "real_client.h"
#include "client_buffer.hpp"

namespace embtable {

// EmbTableClient is the top-level facade (design doc 3.1, 4.x). In
// co-located deployment it owns an EmbTable + ShareMapStore + RealClient
// directly; in disaggregated deployment a DummyClient variant forwards to a
// remote EmbTableClient via SHM/RPC.
//
// This version supports both the local path and the RPC path:
//   - On Init, it starts a ShareMapStore RPC service (if rpcPort > 0) so
//     other nodes can query this node's buckets via coro_rpc.
//   - It creates a ShareMapStoreClient used by EmbTable/Bucket to call
//     remote ShareMapStore services when a bucket lives on another node.
class EmbTableClient {
   public:
    struct Options {
        std::string tableName;
        uint32_t numBuckets = 16;
        uint64_t valueSize = 0;        // mandatory: bytes per value
        DeploymentConfig deployment;
        bool createNew = true;         // create table meta on Init (vs query)
        // Local hostname (used for node-locality detection). If empty,
        // detected from the RealClient.
        std::string localHostname;
        // RPC server thread count for ShareMapStore service.
        size_t rpcThreads = 4;
    };

    explicit EmbTableClient(Options options);

    ~EmbTableClient();

    EmbTableClient(const EmbTableClient&) = delete;
    EmbTableClient& operator=(const EmbTableClient&) = delete;

    // Initialize ShareMapStore, RealClient, ShareMapStoreClient, RPC service
    // (if configured), and EmbTable.
    Status Init();

    // Insert key/value pairs. Routes to per-key buckets and flushes.
    Status Insert(const std::vector<uint64_t>& keys,
                  const std::vector<StringView>& values);

    // Batch find. buffers[i] is the value for keys[i] (empty if missing).
    Status Find(const std::vector<uint64_t>& keys,
                std::vector<StringView>& buffers);

    // Thread-safe result-lifetime variant. The caller owns bufferHandles and
    // must keep it alive while consuming the returned StringViews.
    Status Find(const std::vector<uint64_t>& keys,
                std::vector<StringView>& buffers,
                std::vector<std::shared_ptr<mooncake::BufferHandle>>&
                    bufferHandles);

    // Build the perfect-hash index on all buckets (read-only afterwards).
    Status BuildIndex();

    // Load key/value pairs from local files. Parses each (keyFile, valueFile)
    // pair, extracts uint64_t keys and valueSize-byte values, and calls
    // Insert in batches. Flushes all buckets at the end.
    Status Load(const std::vector<std::string>& keyFiles,
                const std::vector<std::string>& valueFiles,
                const std::string& format);

    const std::string& TableName() const { return options_.tableName; }
    uint32_t NumBuckets() const;
    uint64_t ValueSize() const;

    // Access underlying components (for advanced use / testing).
    ShareMapStore& GetShareMapStore() { return *shareMapStore_; }
    std::shared_ptr<ShareMapStoreClient> GetShareMapStoreClient() const {
        return shareMapStoreClient_;
    }

   private:
    Options options_;
    std::shared_ptr<mooncake::RealClient> realClient_;
    std::shared_ptr<ShareMapStore> shareMapStore_;
    std::shared_ptr<ShareMapStoreClient> shareMapStoreClient_;
    std::shared_ptr<EmbTable> embTable_;
    // RPC service (kept alive for the lifetime of the client).
    std::unique_ptr<ShareMapStoreRpcService> rpcService_;
    std::unique_ptr<coro_rpc::coro_rpc_server> rpcServer_;
    std::thread rpcThread_;
    std::string localHostname_;
};

}  // namespace embtable
