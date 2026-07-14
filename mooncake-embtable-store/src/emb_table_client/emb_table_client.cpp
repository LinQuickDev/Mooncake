#include "emb_table_client/emb_table_client.h"

#include <glog/logging.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>

#include "ylt/coro_rpc/impl/coro_rpc_server.hpp"

namespace embtable {

namespace {
// Get the local hostname (short form). Used for node-locality detection.
std::string getLocalHostname() {
    char buf[256] = {0};
    if (gethostname(buf, sizeof(buf)) == 0) {
        return std::string(buf);
    }
    return "127.0.0.1";
}
}  // namespace

EmbTableClient::EmbTableClient(Options options)
    : options_(std::move(options)) {}

EmbTableClient::~EmbTableClient() {
    // Stop the RPC server before destroying the service.
    if (rpcServer_) {
        rpcServer_->stop();
        rpcServer_.reset();
    }
    rpcService_.reset();
}

Status EmbTableClient::Init() {
    if (options_.valueSize == 0) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "valueSize must be > 0");
    }
    if (options_.tableName.empty()) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "tableName must be non-empty");
    }
    localHostname_ =
        options_.localHostname.empty() ? getLocalHostname()
                                       : options_.localHostname;

    shareMapStore_ = std::make_shared<ShareMapStore>(options_.deployment);
    auto s = shareMapStore_->Init();
    if (!s.IsOk()) return s;

    // Use the ShareMapStore's underlying client for metadata operations.
    realClient_ = shareMapStore_->GetClient();
    if (!realClient_) {
        // Fallback: create a dedicated client.
        realClient_ = std::make_shared<mooncake::RealClient>();
        int ret = realClient_->setup_real(
            localHostname_, options_.deployment.metadataServer,
            1024 * 1024 * 16, 1024 * 1024 * 16,
            options_.deployment.protocol, options_.deployment.deviceNames,
            options_.deployment.masterAddress);
        if (ret != 0) {
            return Status::Error(
                ErrorCode::kInternal,
                "EmbTableClient RealClient setup_real failed");
        }
    }

    // Create the ShareMapStoreClient for remote RPC calls.
    shareMapStoreClient_ =
        std::make_shared<ShareMapStoreClient>(realClient_, localHostname_);

    // Start the ShareMapStore RPC service (if a port is configured).
    if (options_.deployment.rpcPort != 0) {
        rpcService_ = std::make_unique<ShareMapStoreRpcService>(*shareMapStore_);
        rpcServer_ = std::make_unique<coro_rpc::coro_rpc_server>(
            options_.rpcThreads, options_.deployment.rpcPort);
        rpcService_->RegisterHandlers(*rpcServer_);
        // Start the server in a background thread.
        // coro_rpc_server::start() blocks; we run it asynchronously.
        std::thread([this]() {
            LOG(INFO) << "ShareMapStore RPC service listening on port "
                      << options_.deployment.rpcPort;
            rpcServer_->start();
        }).detach();
    }

    embTable_ = std::make_shared<EmbTable>(
        options_.tableName, options_.numBuckets, options_.valueSize,
        shareMapStore_, realClient_, shareMapStoreClient_, localHostname_);
    return embTable_->Init(options_.createNew);
}

Status EmbTableClient::Insert(const std::vector<uint64_t>& keys,
                              const std::vector<StringView>& values) {
    if (!embTable_) {
        return Status::Error(ErrorCode::kInternal, "not initialized");
    }
    return embTable_->Insert(keys, values);
}

Status EmbTableClient::Find(const std::vector<uint64_t>& keys,
                            std::vector<StringView>& buffers) {
    if (!embTable_) {
        return Status::Error(ErrorCode::kInternal, "not initialized");
    }
    return embTable_->Find(keys, buffers);
}

Status EmbTableClient::BuildIndex() {
    if (!embTable_) {
        return Status::Error(ErrorCode::kInternal, "not initialized");
    }
    return embTable_->BuildIndex();
}

Status EmbTableClient::Load(const std::vector<std::string>& keyFiles,
                            const std::vector<std::string>& valueFiles,
                            const std::string& format) {
    if (!embTable_) {
        return Status::Error(ErrorCode::kInternal, "not initialized");
    }
    return embTable_->Load(keyFiles, valueFiles, format);
}

uint32_t EmbTableClient::NumBuckets() const {
    return embTable_ ? embTable_->NumBuckets() : 0;
}

uint64_t EmbTableClient::ValueSize() const {
    return embTable_ ? embTable_->ValueSize() : options_.valueSize;
}

}  // namespace embtable
