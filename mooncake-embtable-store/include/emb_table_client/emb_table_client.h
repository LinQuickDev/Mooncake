#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "emb_table/emb_table.h"
#include "emb_table_client/emb_table_rpc_service.h"
#include "share_map_store/share_map_store.h"
#include "share_map_store/share_map_store_client.h"
#include "share_map_store/share_map_store_rpc_service.h"
#include "emb_types.h"
#include "real_client.h"
#include "client_buffer.hpp"

namespace embtable {

// EmbTableClient is the top-level facade and table registry. It owns one
// ShareMapStore/RealClient pair and lazily opens multiple EmbTable instances
// by table name. A co-process client may select one table through Options;
// the standalone service starts without a table and exposes DDL/data RPCs.
//
// This version supports both the local path and the RPC path:
//   - On Init, it starts EmbTable and ShareMapStore RPC services only when
//     enableEmbTableRpc is true. DummyClient uses EmbTable RPC + shared
//     memory, while other nodes use ShareMapStore RPC + Transfer Engine.
//   - It creates a ShareMapStoreClient used by EmbTable/Bucket to call
//     remote ShareMapStore services when a bucket lives on another node.
class EmbTableClient {
   public:
    struct Options {
        std::string tableName;
        uint32_t numBuckets = 16;
        uint64_t valueSize = 0;  // required only when createNew is true
        DeploymentConfig deployment;
        bool createNew = true;  // create/open tableName on Init
        // Local hostname (used for node-locality detection). If empty,
        // detected from the RealClient.
        std::string localHostname;
        // RPC server thread count for EmbTable and ShareMapStore services.
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
    Status Find(
        const std::vector<uint64_t>& keys, std::vector<StringView>& buffers,
        std::vector<std::shared_ptr<mooncake::BufferHandle>>& bufferHandles);

    // Build the perfect-hash index on all buckets (read-only afterwards).
    Status BuildIndex();

    // Load key/value pairs from local files. Parses each (keyFile, valueFile)
    // pair, extracts uint64_t keys and valueSize-byte values, and calls
    // Insert in batches. Flushes all buckets at the end.
    Status Load(const std::vector<std::string>& keyFiles,
                const std::vector<std::string>& valueFiles,
                const std::string& format);

    // Table-scoped operations used by the RPC service and multi-table users.
    Status Insert(const std::string& tableName,
                  const std::vector<uint64_t>& keys,
                  const std::vector<StringView>& values);
    Status Find(
        const std::string& tableName, const std::vector<uint64_t>& keys,
        std::vector<StringView>& buffers,
        std::vector<std::shared_ptr<mooncake::BufferHandle>>& bufferHandles);
    Status BuildIndex(const std::string& tableName);
    Status GetTableInfo(const std::string& tableName, TableMetaInfo& info);

    // DDL. Drop removes table/bucket metadata and the in-process registry
    // entry. Physical ShareMap objects are reclaimed by store eviction/GC.
    Status CreateTable(const std::string& tableName, uint32_t numBuckets,
                       uint64_t valueSize);
    Status AlterTable(const std::string& tableName, uint32_t numBuckets,
                      uint64_t valueSize);
    Status DeleteTable(const std::string& tableName);

    const std::string& TableName() const { return options_.tableName; }
    uint32_t NumBuckets() const;
    uint64_t ValueSize() const;

    // Access underlying components (for advanced use / testing).
    ShareMapStore& GetShareMapStore() { return *shareMapStore_; }
    std::shared_ptr<ShareMapStoreClient> GetShareMapStoreClient() const {
        return shareMapStoreClient_;
    }

   private:
    Status GetOrLoadTable(const std::string& tableName,
                          std::shared_ptr<EmbTable>& table);
    Status OpenTable(const std::string& tableName, uint32_t numBuckets,
                     uint64_t valueSize, bool createNew,
                     std::shared_ptr<EmbTable>& table);

    Options options_;
    std::shared_ptr<mooncake::RealClient> realClient_;
    std::shared_ptr<ShareMapStore> shareMapStore_;
    std::shared_ptr<ShareMapStoreClient> shareMapStoreClient_;
    std::shared_ptr<EmbTable> embTable_;
    // RPC services (kept alive for the lifetime of the client).
    std::unique_ptr<ShareMapStoreRpcService> shareMapRpcService_;
    std::unique_ptr<EmbTableRpcService> embTableRpcService_;
    std::unique_ptr<coro_rpc::coro_rpc_server> rpcServer_;
    std::thread rpcThread_;
    std::string localHostname_;
    mutable std::mutex tablesMutex_;
    std::unordered_map<std::string, std::shared_ptr<EmbTable>> tables_;
};

}  // namespace embtable
