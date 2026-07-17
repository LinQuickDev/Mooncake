#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "client_buffer.hpp"
#include "emb_table_client/emb_table_client.h"
#include "emb_table_client/emb_table_rpc_service.h"
#include "emb_types.h"
#include "ylt/coro_rpc/coro_rpc_client.hpp"

namespace embtable {

// User-side client for disaggregated deployment. RPC transports keys and
// control metadata; values and query results move through shared memory.
class EmbTableDummyClient {
   public:
    struct Options {
        std::string rpcEndpoint = "127.0.0.1:50055";
        uint64_t sharedMemorySize = 64ull * 1024 * 1024;
    };

    explicit EmbTableDummyClient(Options options);

    // Compatibility constructor. It derives the endpoint and SHM size from
    // EmbTableClient options but does not instantiate a local EmbTableClient.
    explicit EmbTableDummyClient(EmbTableClient::Options options);

    ~EmbTableDummyClient();

    EmbTableDummyClient(const EmbTableDummyClient&) = delete;
    EmbTableDummyClient& operator=(const EmbTableDummyClient&) = delete;

    Status Init();

    Status Insert(const std::vector<uint64_t>& keys,
                  const std::vector<StringView>& values);

    // Returned StringViews reference this client's shared-memory region and
    // remain valid until the next Find call on the same thread.
    Status Find(const std::vector<uint64_t>& keys,
                std::vector<StringView>& buffers);

    Status BuildIndex();

    uint64_t ValueSize() const { return valueSize_; }

   private:
    std::shared_ptr<mooncake::BufferHandle> AllocateSharedBuffer(uint64_t size);
    void CleanupSharedMemory();

    Options options_;
    std::unique_ptr<coro_rpc::coro_rpc_client> rpcClient_;
    int shmFd_ = -1;
    void* shmBase_ = nullptr;
    std::string shmName_;
    bool sharedMemoryRegistered_ = false;
    uint64_t valueSize_ = 0;
    std::shared_ptr<mooncake::ClientBufferAllocator> shmAllocator_;
};

}  // namespace embtable
