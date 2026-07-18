#include "emb_table_client/emb_table_dummy_client.h"

#include <atomic>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <new>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_map>

#include <async_simple/coro/SyncAwait.h>

namespace embtable {

namespace {

std::atomic<uint64_t> gSharedMemorySequence{0};

thread_local std::unordered_map<const EmbTableDummyClient*,
                                std::shared_ptr<mooncake::BufferHandle>>
    gFindBuffers;

EmbTableDummyClient::Options MakeDummyOptions(
    const EmbTableClient::Options& options) {
    EmbTableDummyClient::Options dummyOptions;
    const std::string host =
        options.localHostname.empty() ? "127.0.0.1" : options.localHostname;
    dummyOptions.rpcEndpoint =
        host + ":" + std::to_string(options.deployment.rpcPort);
    dummyOptions.tableName = options.tableName;
    dummyOptions.sharedMemorySize = options.deployment.transferBufferSize;
    return dummyOptions;
}

Status FromResponse(const EmbTableStatusResponse& response,
                    const std::string& operation) {
    if (response.statusCode == 0) return Status::OK();
    return Status::Error(static_cast<ErrorCode>(response.statusCode),
                         operation + " failed: " + response.errorMsg);
}

}  // namespace

EmbTableDummyClient::EmbTableDummyClient(Options options)
    : options_(std::move(options)) {}

EmbTableDummyClient::EmbTableDummyClient(EmbTableClient::Options options)
    : EmbTableDummyClient(MakeDummyOptions(options)) {}

EmbTableDummyClient::~EmbTableDummyClient() {
    gFindBuffers.erase(this);
    CleanupSharedMemory();
}

Status EmbTableDummyClient::Init() {
    if (rpcClient_) return Status::OK();
    if (options_.rpcEndpoint.empty()) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "RPC endpoint must be non-empty");
    }
    if (!options_.tableName.empty() &&
        (options_.sharedMemorySize == 0 ||
         options_.sharedMemorySize >
             static_cast<uint64_t>(std::numeric_limits<off_t>::max()) ||
         options_.sharedMemorySize >
             static_cast<uint64_t>(std::numeric_limits<size_t>::max()))) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "invalid DummyClient options");
    }

    rpcClient_ = std::make_unique<coro_rpc::coro_rpc_client>();
    auto connectError = async_simple::coro::syncAwait(
        rpcClient_->connect(options_.rpcEndpoint));
    if (connectError) {
        rpcClient_.reset();
        return Status::Error(
            ErrorCode::kIOError,
            "RPC connect failed: " + std::string(connectError.message()));
    }

    if (options_.tableName.empty()) return Status::OK();

    EmbTableInfoRequest infoRequest;
    infoRequest.tableName = options_.tableName;
    auto infoResult = async_simple::coro::syncAwait(
        rpcClient_->call<&EmbTableRpcService::HandleGetInfo>(infoRequest));
    if (!infoResult) {
        const std::string error = infoResult.error().msg;
        CleanupSharedMemory();
        return Status::Error(ErrorCode::kIOError,
                             "GetInfo RPC failed: " + error);
    }
    if (infoResult.value().statusCode != 0) {
        const auto response = infoResult.value();
        CleanupSharedMemory();
        return Status::Error(static_cast<ErrorCode>(response.statusCode),
                             "GetInfo failed: " + response.errorMsg);
    }
    if (infoResult.value().valueSize == 0) {
        CleanupSharedMemory();
        return Status::Error(ErrorCode::kInternal,
                             "GetInfo returned an invalid value size");
    }
    valueSize_ = infoResult.value().valueSize;

    shmName_ = "/mooncake_embtable_" + std::to_string(getpid()) + "_" +
               std::to_string(gSharedMemorySequence.fetch_add(1));
    shmFd_ = shm_open(shmName_.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    if (shmFd_ < 0) {
        CleanupSharedMemory();
        return Status::Error(
            ErrorCode::kIOError,
            "shm_open failed: " + std::string(std::strerror(errno)));
    }
    if (ftruncate(shmFd_, static_cast<off_t>(options_.sharedMemorySize)) != 0) {
        const std::string error = std::strerror(errno);
        CleanupSharedMemory();
        return Status::Error(ErrorCode::kIOError, "ftruncate failed: " + error);
    }
    shmBase_ = mmap(nullptr, static_cast<size_t>(options_.sharedMemorySize),
                    PROT_READ | PROT_WRITE, MAP_SHARED, shmFd_, 0);
    if (shmBase_ == MAP_FAILED) {
        shmBase_ = nullptr;
        const std::string error = std::strerror(errno);
        CleanupSharedMemory();
        return Status::Error(ErrorCode::kIOError, "mmap failed: " + error);
    }

    try {
        shmAllocator_ = mooncake::ClientBufferAllocator::create(
            shmBase_, options_.sharedMemorySize);
    } catch (const std::bad_alloc&) {
        CleanupSharedMemory();
        return Status::Error(ErrorCode::kInternal,
                             "failed to create shared memory allocator");
    }

    RegisterEmbTableShmRequest request;
    request.shmName = shmName_;
    request.shmSize = options_.sharedMemorySize;
    auto result = async_simple::coro::syncAwait(
        rpcClient_->call<&EmbTableRpcService::HandleRegisterSharedMemory>(
            request));
    if (!result) {
        const std::string error = result.error().msg;
        CleanupSharedMemory();
        return Status::Error(ErrorCode::kIOError,
                             "shared memory registration RPC failed: " + error);
    }
    Status status = FromResponse(result.value(), "shared memory registration");
    if (!status.IsOk()) {
        CleanupSharedMemory();
        return status;
    }
    sharedMemoryRegistered_ = true;

    // Both processes hold a mapping now. Remove the global name so a crashed
    // DummyClient cannot leave a persistent POSIX SHM object behind.
    shm_unlink(shmName_.c_str());
    return Status::OK();
}

std::shared_ptr<mooncake::BufferHandle>
EmbTableDummyClient::AllocateSharedBuffer(uint64_t size) {
    if (!shmAllocator_ || size == 0 || size > options_.sharedMemorySize) {
        return nullptr;
    }
    auto allocation = shmAllocator_->allocate(static_cast<size_t>(size));
    if (!allocation) return nullptr;
    return std::make_shared<mooncake::BufferHandle>(
        std::move(allocation.value()));
}

Status EmbTableDummyClient::Insert(const std::vector<uint64_t>& keys,
                                   const std::vector<StringView>& values) {
    if (!rpcClient_ || !sharedMemoryRegistered_ || options_.tableName.empty()) {
        return Status::Error(ErrorCode::kInternal, "not initialized");
    }
    if (keys.size() != values.size()) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "keys/values size mismatch");
    }
    if (keys.empty()) return Status::OK();

    uint64_t dataSize = 0;
    if (!CheckedMultiply(keys.size(), valueSize_, dataSize)) {
        return Status::Error(ErrorCode::kOutOfRange,
                             "Insert data size overflow");
    }
    auto handle = AllocateSharedBuffer(dataSize);
    if (!handle) {
        return Status::Error(ErrorCode::kBufferFull,
                             "shared memory allocator exhausted");
    }
    char* target = static_cast<char*>(handle->ptr());
    for (size_t i = 0; i < values.size(); ++i) {
        if (values[i].size() != valueSize_) {
            return Status::Error(ErrorCode::kInvalidArgument,
                                 "inconsistent value sizes");
        }
        std::memcpy(target + i * valueSize_, values[i].data(), valueSize_);
    }

    EmbTableInsertRequest request;
    request.tableName = options_.tableName;
    request.keys = keys;
    request.shmName = shmName_;
    request.dataOffset = static_cast<uint64_t>(
        static_cast<char*>(handle->ptr()) - static_cast<char*>(shmBase_));
    request.dataSize = dataSize;
    auto result = async_simple::coro::syncAwait(
        rpcClient_->call<&EmbTableRpcService::HandleInsert>(request));
    if (!result) {
        return Status::Error(ErrorCode::kIOError,
                             "Insert RPC failed: " + result.error().msg);
    }
    return FromResponse(result.value(), "Insert");
}

Status EmbTableDummyClient::Find(const std::vector<uint64_t>& keys,
                                 std::vector<StringView>& buffers) {
    buffers.clear();
    if (!rpcClient_ || !sharedMemoryRegistered_ || options_.tableName.empty()) {
        return Status::Error(ErrorCode::kInternal, "not initialized");
    }
    if (keys.empty()) return Status::OK();

    // Release this thread's previous result before allocating its next slot.
    gFindBuffers.erase(this);
    uint64_t targetSize = 0;
    if (valueSize_ == std::numeric_limits<uint64_t>::max() ||
        !CheckedMultiply(keys.size(), valueSize_ + 1, targetSize)) {
        return Status::Error(ErrorCode::kOutOfRange,
                             "Find result size overflow");
    }
    auto handle = AllocateSharedBuffer(targetSize);
    if (!handle) {
        return Status::Error(ErrorCode::kBufferFull,
                             "shared memory allocator exhausted");
    }

    EmbTableFindRequest request;
    request.tableName = options_.tableName;
    request.keys = keys;
    request.shmName = shmName_;
    request.targetOffset = static_cast<uint64_t>(
        static_cast<char*>(handle->ptr()) - static_cast<char*>(shmBase_));
    request.targetCapacity = handle->size();
    auto result = async_simple::coro::syncAwait(
        rpcClient_->call<&EmbTableRpcService::HandleFind>(request));
    if (!result) {
        return Status::Error(ErrorCode::kIOError,
                             "Find RPC failed: " + result.error().msg);
    }
    const auto& response = result.value();
    if (response.statusCode != 0) {
        return Status::Error(static_cast<ErrorCode>(response.statusCode),
                             "Find failed: " + response.errorMsg);
    }
    if (response.transferredSize != targetSize ||
        response.transferredSize > handle->size() ||
        response.transferredSize % keys.size() != 0) {
        return Status::Error(ErrorCode::kOutOfRange,
                             "invalid Find shared memory response size");
    }

    const uint64_t entrySize = response.transferredSize / keys.size();
    if (entrySize <= 1) {
        return Status::Error(ErrorCode::kInternal, "invalid Find entry size");
    }
    if (entrySize != valueSize_ + 1) {
        return Status::Error(ErrorCode::kInternal,
                             "Find entry size does not match table metadata");
    }
    const char* data = static_cast<const char*>(handle->ptr());
    buffers.resize(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        const char* entry = data + i * entrySize;
        if (entry[0] != 0) {
            buffers[i] = StringView(entry + 1, valueSize_);
        }
    }
    gFindBuffers[this] = std::move(handle);
    return Status::OK();
}

Status EmbTableDummyClient::BuildIndex() {
    if (!rpcClient_ || options_.tableName.empty()) {
        return Status::Error(ErrorCode::kInternal, "not initialized");
    }
    EmbTableBuildIndexRequest request;
    request.tableName = options_.tableName;
    auto result = async_simple::coro::syncAwait(
        rpcClient_->call<&EmbTableRpcService::HandleBuildIndex>(request));
    if (!result) {
        return Status::Error(ErrorCode::kIOError,
                             "BuildIndex RPC failed: " + result.error().msg);
    }
    return FromResponse(result.value(), "BuildIndex");
}

Status EmbTableDummyClient::CreateTable(const std::string& tableName,
                                        uint32_t numBuckets,
                                        uint64_t valueSize) {
    if (!rpcClient_) {
        return Status::Error(ErrorCode::kInternal, "not initialized");
    }
    EmbTableCreateRequest request;
    request.tableName = tableName;
    request.numBuckets = numBuckets;
    request.valueSize = valueSize;
    auto result = async_simple::coro::syncAwait(
        rpcClient_->call<&EmbTableRpcService::HandleCreateTable>(request));
    if (!result) {
        return Status::Error(ErrorCode::kIOError,
                             "CreateTable RPC failed: " + result.error().msg);
    }
    return FromResponse(result.value(), "CreateTable");
}

Status EmbTableDummyClient::AlterTable(const std::string& tableName,
                                       uint32_t numBuckets,
                                       uint64_t valueSize) {
    if (!rpcClient_) {
        return Status::Error(ErrorCode::kInternal, "not initialized");
    }
    EmbTableAlterRequest request;
    request.tableName = tableName;
    request.numBuckets = numBuckets;
    request.valueSize = valueSize;
    auto result = async_simple::coro::syncAwait(
        rpcClient_->call<&EmbTableRpcService::HandleAlterTable>(request));
    if (!result) {
        return Status::Error(ErrorCode::kIOError,
                             "AlterTable RPC failed: " + result.error().msg);
    }
    return FromResponse(result.value(), "AlterTable");
}

Status EmbTableDummyClient::DeleteTable(const std::string& tableName) {
    if (!rpcClient_) {
        return Status::Error(ErrorCode::kInternal, "not initialized");
    }
    EmbTableDeleteRequest request;
    request.tableName = tableName;
    auto result = async_simple::coro::syncAwait(
        rpcClient_->call<&EmbTableRpcService::HandleDeleteTable>(request));
    if (!result) {
        return Status::Error(ErrorCode::kIOError,
                             "DeleteTable RPC failed: " + result.error().msg);
    }
    return FromResponse(result.value(), "DeleteTable");
}

void EmbTableDummyClient::CleanupSharedMemory() {
    if (sharedMemoryRegistered_ && rpcClient_) {
        UnregisterEmbTableShmRequest request;
        request.shmName = shmName_;
        (void)async_simple::coro::syncAwait(
            rpcClient_->call<&EmbTableRpcService::HandleUnregisterSharedMemory>(
                request));
    }
    sharedMemoryRegistered_ = false;
    valueSize_ = 0;
    shmAllocator_.reset();
    if (shmBase_) {
        munmap(shmBase_, static_cast<size_t>(options_.sharedMemorySize));
        shmBase_ = nullptr;
    }
    if (shmFd_ >= 0) {
        close(shmFd_);
        shmFd_ = -1;
    }
    if (!shmName_.empty()) {
        shm_unlink(shmName_.c_str());
        shmName_.clear();
    }
    rpcClient_.reset();
}

}  // namespace embtable
