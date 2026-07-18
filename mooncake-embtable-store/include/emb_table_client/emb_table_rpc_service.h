#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "emb_table_client/emb_table_rpc_types.h"
#include "ylt/coro_rpc/coro_rpc_server.hpp"

namespace embtable {

class EmbTableClient;

class EmbTableRpcService {
   public:
    explicit EmbTableRpcService(EmbTableClient& client) : client_(client) {}
    ~EmbTableRpcService();

    void RegisterHandlers(coro_rpc::coro_rpc_server& server) {
        server
            .register_handler<&EmbTableRpcService::HandleRegisterSharedMemory>(
                this);
        server.register_handler<
            &EmbTableRpcService::HandleUnregisterSharedMemory>(this);
        server.register_handler<&EmbTableRpcService::HandleGetInfo>(this);
        server.register_handler<&EmbTableRpcService::HandleInsert>(this);
        server.register_handler<&EmbTableRpcService::HandleFind>(this);
        server.register_handler<&EmbTableRpcService::HandleBuildIndex>(this);
        server.register_handler<&EmbTableRpcService::HandleCreateTable>(this);
        server.register_handler<&EmbTableRpcService::HandleAlterTable>(this);
        server.register_handler<&EmbTableRpcService::HandleDeleteTable>(this);
    }

    EmbTableStatusResponse HandleRegisterSharedMemory(
        const RegisterEmbTableShmRequest& req);
    EmbTableStatusResponse HandleUnregisterSharedMemory(
        const UnregisterEmbTableShmRequest& req);
    EmbTableInfoResponse HandleGetInfo(const EmbTableInfoRequest& req);
    EmbTableStatusResponse HandleInsert(const EmbTableInsertRequest& req);
    EmbTableFindResponse HandleFind(const EmbTableFindRequest& req);
    EmbTableStatusResponse HandleBuildIndex(
        const EmbTableBuildIndexRequest& req);
    EmbTableStatusResponse HandleCreateTable(const EmbTableCreateRequest& req);
    EmbTableStatusResponse HandleAlterTable(const EmbTableAlterRequest& req);
    EmbTableStatusResponse HandleDeleteTable(const EmbTableDeleteRequest& req);

   private:
    struct SharedMemoryMapping;

    std::shared_ptr<SharedMemoryMapping> ResolveSharedMemory(
        const std::string& name);

    EmbTableClient& client_;
    std::mutex mappingsMutex_;
    std::unordered_map<std::string, std::shared_ptr<SharedMemoryMapping>>
        mappings_;
};

}  // namespace embtable
