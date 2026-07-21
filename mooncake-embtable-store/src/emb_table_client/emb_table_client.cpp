#include "emb_table_client/emb_table_client.h"

#include <glog/logging.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <thread>
#include <unistd.h>

#include "ylt/coro_rpc/impl/coro_rpc_server.hpp"
#include "embtable_perf.h"

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
    }
    if (rpcThread_.joinable()) rpcThread_.join();
    rpcServer_.reset();
    embTableRpcService_.reset();
    shareMapRpcService_.reset();
}

Status EmbTableClient::Init() {
    if (options_.createNew && !options_.tableName.empty() &&
        options_.valueSize == 0) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "valueSize must be > 0 when creating a table");
    }
    localHostname_ = options_.localHostname.empty() ? getLocalHostname()
                                                    : options_.localHostname;

    shareMapStore_ =
        std::make_shared<ShareMapStore>(options_.deployment, localHostname_);
    auto s = shareMapStore_->Init();
    if (!s.IsOk()) return s;

    // ShareMapStore is the sole owner responsible for RealClient setup.
    realClient_ = shareMapStore_->GetRealClient();
    if (!realClient_) {
        return Status::Error(ErrorCode::kInternal,
                             "ShareMapStore did not initialize RealClient");
    }

    // Create the ShareMapStoreClient for remote RPC calls.
    shareMapStoreClient_ = std::make_shared<ShareMapStoreClient>(realClient_);
    s = shareMapStoreClient_->Init(options_.deployment.transferBufferSize);
    if (!s.IsOk()) return s;

    // Co-process callers may select a default table. A standalone storage
    // node starts tableless and opens tables lazily through DDL/data RPCs.
    if (!options_.tableName.empty()) {
        s = OpenTable(options_.tableName, options_.numBuckets,
                      options_.valueSize, options_.createNew, embTable_);
        if (!s.IsOk()) return s;
    }

    if (options_.deployment.enableEmbTableRpc) {
        if (options_.deployment.rpcPort == 0) {
            return Status::Error(ErrorCode::kInvalidArgument,
                                 "RPC is enabled but rpcPort is zero");
        }
        shareMapRpcService_ =
            std::make_unique<ShareMapStoreRpcService>(*shareMapStore_);
        embTableRpcService_ = std::make_unique<EmbTableRpcService>(*this);
        rpcServer_ = std::make_unique<coro_rpc::coro_rpc_server>(
            options_.rpcThreads, options_.deployment.rpcPort);
        shareMapRpcService_->RegisterHandlers(*rpcServer_);
        embTableRpcService_->RegisterHandlers(*rpcServer_);
        // Start the server in a background thread.
        // coro_rpc_server::start() blocks; we run it asynchronously.
        rpcThread_ = std::thread([this]() {
            LOG(INFO) << "EmbTable RPC service listening on port "
                      << options_.deployment.rpcPort;
            auto ec = rpcServer_->start();
            if (ec) {
                LOG(ERROR) << "EmbTable RPC service stopped with error: "
                           << ec.message();
            }
        });
    }

    return Status::OK();
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
    UbDiag::PerfPoint point(PerfKey::EMB_RD_CLIENT_FIND_TOTAL,
                            UbDiag::PerfLevel::SUB_SYSTEM);
    point.Start();
    if (!embTable_) {
        auto status =
            Status::Error(ErrorCode::kInternal, "not initialized");
        point.End(status.code());
        return status;
    }
    auto status = embTable_->Find(keys, buffers);
    point.End(status.IsOk() ? 0 : status.code());
    return status;
}

Status EmbTableClient::Find(
    const std::vector<uint64_t>& keys, std::vector<StringView>& buffers,
    std::vector<std::shared_ptr<mooncake::BufferHandle>>& bufferHandles) {
    UbDiag::PerfPoint point(PerfKey::EMB_RD_CLIENT_FIND_TOTAL,
                            UbDiag::PerfLevel::SUB_SYSTEM);
    point.Start();
    if (!embTable_) {
        auto status =
            Status::Error(ErrorCode::kInternal, "not initialized");
        point.End(status.code());
        return status;
    }
    auto status = embTable_->Find(keys, buffers, bufferHandles);
    point.End(status.IsOk() ? 0 : status.code());
    return status;
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

Status EmbTableClient::OpenTable(const std::string& tableName,
                                 uint32_t numBuckets, uint64_t valueSize,
                                 bool createNew,
                                 std::shared_ptr<EmbTable>& table) {
    if (tableName.empty()) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "tableName must be non-empty");
    }
    if (!shareMapStore_ || !realClient_ || !shareMapStoreClient_) {
        return Status::Error(ErrorCode::kInternal,
                             "EmbTableClient is not initialized");
    }
    if (createNew && (numBuckets == 0 || valueSize == 0)) {
        return Status::Error(
            ErrorCode::kInvalidArgument,
            "numBuckets and valueSize must be non-zero when creating a table");
    }

    {
        std::lock_guard<std::mutex> lock(tablesMutex_);
        auto it = tables_.find(tableName);
        if (it != tables_.end()) {
            if (createNew) {
                return Status::Error(ErrorCode::kAlreadyExists,
                                     "table already exists: " + tableName);
            }
            table = it->second;
            return Status::OK();
        }
    }

    auto opened = std::make_shared<EmbTable>(
        tableName, numBuckets, valueSize, shareMapStore_, realClient_,
        shareMapStoreClient_, localHostname_, options_.deployment.rpcPort);
    auto status = opened->Init(createNew);
    if (!status.IsOk()) return status;

    std::lock_guard<std::mutex> lock(tablesMutex_);
    auto [it, inserted] = tables_.emplace(tableName, opened);
    table = inserted ? std::move(opened) : it->second;
    return Status::OK();
}

Status EmbTableClient::GetOrLoadTable(const std::string& tableName,
                                      std::shared_ptr<EmbTable>& table) {
    return OpenTable(tableName, /*numBuckets=*/1, /*valueSize=*/0,
                     /*createNew=*/false, table);
}

Status EmbTableClient::Insert(const std::string& tableName,
                              const std::vector<uint64_t>& keys,
                              const std::vector<StringView>& values) {
    std::shared_ptr<EmbTable> table;
    auto status = GetOrLoadTable(tableName, table);
    return status.IsOk() ? table->Insert(keys, values) : status;
}

Status EmbTableClient::Find(
    const std::string& tableName, const std::vector<uint64_t>& keys,
    std::vector<StringView>& buffers,
    std::vector<std::shared_ptr<mooncake::BufferHandle>>& bufferHandles) {
    UbDiag::PerfPoint point(PerfKey::EMB_RD_CLIENT_FIND_TOTAL,
                            UbDiag::PerfLevel::SUB_SYSTEM);
    point.Start();
    std::shared_ptr<EmbTable> table;
    auto status = GetOrLoadTable(tableName, table);
    if (status.IsOk()) {
        status = table->Find(keys, buffers, bufferHandles);
    }
    point.End(status.IsOk() ? 0 : status.code());
    return status;
}

Status EmbTableClient::BuildIndex(const std::string& tableName) {
    std::shared_ptr<EmbTable> table;
    auto status = GetOrLoadTable(tableName, table);
    return status.IsOk() ? table->BuildIndex() : status;
}

Status EmbTableClient::GetTableInfo(const std::string& tableName,
                                    TableMetaInfo& info) {
    std::shared_ptr<EmbTable> table;
    auto status = GetOrLoadTable(tableName, table);
    if (!status.IsOk()) return status;
    info = table->MetaInfo();
    return Status::OK();
}

Status EmbTableClient::CreateTable(const std::string& tableName,
                                   uint32_t numBuckets, uint64_t valueSize) {
    std::shared_ptr<EmbTable> table;
    return OpenTable(tableName, numBuckets, valueSize, /*createNew=*/true,
                     table);
}

Status EmbTableClient::AlterTable(const std::string& tableName,
                                  uint32_t numBuckets, uint64_t valueSize) {
    if (numBuckets == 0 || valueSize == 0) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "numBuckets and valueSize must be non-zero");
    }
    TableMetaInfo info;
    auto status = GetTableInfo(tableName, info);
    if (!status.IsOk()) return status;
    if (info.bucketNum == numBuckets && info.dimSize == valueSize) {
        return Status::OK();
    }
    return Status::Error(
        ErrorCode::kNotSupported,
        "online AlterTable requires bucket re-sharding/value migration");
}

Status EmbTableClient::DeleteTable(const std::string& tableName) {
    std::shared_ptr<EmbTable> table;
    auto status = GetOrLoadTable(tableName, table);
    if (!status.IsOk()) return status;
    status = table->Drop();
    if (!status.IsOk()) return status;

    std::lock_guard<std::mutex> lock(tablesMutex_);
    auto it = tables_.find(tableName);
    if (it != tables_.end() && it->second == table) tables_.erase(it);
    if (embTable_ == table) embTable_.reset();
    return Status::OK();
}

uint32_t EmbTableClient::NumBuckets() const {
    return embTable_ ? embTable_->NumBuckets() : 0;
}

uint64_t EmbTableClient::ValueSize() const {
    return embTable_ ? embTable_->ValueSize() : options_.valueSize;
}

}  // namespace embtable
