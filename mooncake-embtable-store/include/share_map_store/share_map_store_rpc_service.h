#pragma once

#include <memory>
#include <string>

#include "share_map_store/share_map_store.h"
#include "share_map_store/share_map_store_rpc_types.h"
#include "ylt/coro_rpc/impl/coro_rpc_server.hpp"

namespace embtable {

// ShareMapStoreRpcService wraps a ShareMapStore with coro_rpc handlers. RPC
// carries control metadata; query payloads move through registered TE buffers.
//
// Usage:
//   ShareMapStoreRpcService service(store);
//   coro_rpc::coro_rpc_server server(threads, port);
//   service.RegisterHandlers(server);
//   server.start();
class ShareMapStoreRpcService {
   public:
    explicit ShareMapStoreRpcService(ShareMapStore& store) : store_(store) {}

    // Register all RPC handlers on the given coro_rpc_server.
    void RegisterHandlers(coro_rpc::coro_rpc_server& server) {
        server.register_handler<&ShareMapStoreRpcService::HandleQueryData>(
            this);
        server.register_handler<
            &ShareMapStoreRpcService::HandleBatchQueryData>(this);
        server.register_handler<&ShareMapStoreRpcService::HandlePublish>(this);
        server.register_handler<
            &ShareMapStoreRpcService::HandleBuildIndex>(this);
    }

    // ---- RPC handlers (called by coro_rpc) ----

    QueryDataResponse HandleQueryData(const QueryDataRequest& req);
    BatchQueryDataResponse HandleBatchQueryData(
        const BatchQueryDataRequest& req);
    PublishResponse HandlePublish(const PublishRequest& req);
    BuildIndexResponse HandleBuildIndex(const BuildIndexRequest& req);

   private:
    ShareMapStore& store_;
};

}  // namespace embtable
