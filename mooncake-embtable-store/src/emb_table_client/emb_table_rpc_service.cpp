#include "emb_table_client/emb_table_rpc_service.h"

#include <chrono>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

#include "emb_table_client/emb_table_client.h"
#include "embtable_perf.h"

namespace embtable {

namespace {

uint64_t MonotonicNowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

EmbTableStatusResponse ToResponse(const Status& status) {
    EmbTableStatusResponse response;
    if (!status.IsOk()) {
        response.statusCode = status.code();
        response.errorMsg = status.msg();
    }
    return response;
}

bool IsValidShmName(const std::string& name) {
    return name.size() > 1 && name.front() == '/' &&
           name.find('/', 1) == std::string::npos;
}

}  // namespace

struct EmbTableRpcService::SharedMemoryMapping {
    void* base = nullptr;
    uint64_t size = 0;

    ~SharedMemoryMapping() {
        if (base && size != 0) {
            munmap(base, static_cast<size_t>(size));
        }
    }
};

EmbTableRpcService::~EmbTableRpcService() {
    std::lock_guard<std::mutex> lock(mappingsMutex_);
    mappings_.clear();
}

std::shared_ptr<EmbTableRpcService::SharedMemoryMapping>
EmbTableRpcService::ResolveSharedMemory(const std::string& name) {
    std::lock_guard<std::mutex> lock(mappingsMutex_);
    auto it = mappings_.find(name);
    return it == mappings_.end() ? nullptr : it->second;
}

EmbTableStatusResponse EmbTableRpcService::HandleRegisterSharedMemory(
    const RegisterEmbTableShmRequest& req) {
    UbDiag::PerfPoint point(PerfKey::EMB_RPC_REGISTER_SHM_TOTAL,
                            UbDiag::PerfLevel::KEY_MODULE);
    point.Start();
    auto finish = [&point](EmbTableStatusResponse response) {
        point.End(response.statusCode);
        return response;
    };
    if (!IsValidShmName(req.shmName) || req.shmSize == 0 ||
        req.shmSize > std::numeric_limits<size_t>::max()) {
        return finish(ToResponse(Status::Error(
            ErrorCode::kInvalidArgument, "invalid shared memory descriptor")));
    }

    {
        std::lock_guard<std::mutex> lock(mappingsMutex_);
        auto it = mappings_.find(req.shmName);
        if (it != mappings_.end()) {
            if (it->second->size == req.shmSize) {
                return finish(EmbTableStatusResponse{});
            }
            return finish(ToResponse(Status::Error(
                ErrorCode::kAlreadyExists,
                "shared memory name already registered with another size")));
        }
    }

    int fd = shm_open(req.shmName.c_str(), O_RDWR, 0);
    if (fd < 0) {
        return finish(ToResponse(Status::Error(
            ErrorCode::kIOError,
            "shm_open failed: " + std::string(std::strerror(errno)))));
    }
    struct stat statBuffer{};
    if (fstat(fd, &statBuffer) != 0 || statBuffer.st_size < 0 ||
        static_cast<uint64_t>(statBuffer.st_size) < req.shmSize) {
        close(fd);
        return finish(ToResponse(Status::Error(
            ErrorCode::kInvalidArgument,
            "shared memory object is smaller than requested mapping")));
    }
    void* base = mmap(nullptr, static_cast<size_t>(req.shmSize),
                      PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (base == MAP_FAILED) {
        return finish(ToResponse(
            Status::Error(ErrorCode::kIOError,
                          "mmap failed: " + std::string(std::strerror(errno)))));
    }

    auto mapping = std::make_shared<SharedMemoryMapping>();
    mapping->base = base;
    mapping->size = req.shmSize;
    {
        std::lock_guard<std::mutex> lock(mappingsMutex_);
        auto [it, inserted] = mappings_.emplace(req.shmName, mapping);
        if (!inserted && it->second->size != req.shmSize) {
            return finish(ToResponse(
                Status::Error(ErrorCode::kAlreadyExists,
                              "shared memory name concurrently registered")));
        }
    }
    return finish(EmbTableStatusResponse{});
}

EmbTableStatusResponse EmbTableRpcService::HandleUnregisterSharedMemory(
    const UnregisterEmbTableShmRequest& req) {
    UbDiag::PerfPoint point(PerfKey::EMB_RPC_UNREGISTER_SHM_TOTAL,
                            UbDiag::PerfLevel::KEY_MODULE);
    point.Start();
    std::lock_guard<std::mutex> lock(mappingsMutex_);
    mappings_.erase(req.shmName);
    point.End(0);
    return EmbTableStatusResponse{};
}

EmbTableInfoResponse EmbTableRpcService::HandleGetInfo(
    const EmbTableInfoRequest& req) {
    UbDiag::PerfPoint point(PerfKey::EMB_RPC_GET_INFO_TOTAL,
                            UbDiag::PerfLevel::KEY_MODULE);
    point.Start();
    EmbTableInfoResponse response;
    auto finish = [&point](EmbTableInfoResponse result) {
        point.End(result.statusCode);
        return result;
    };
    TableMetaInfo info;
    auto status = client_.GetTableInfo(req.tableName, info);
    if (!status.IsOk()) {
        response.statusCode = status.code();
        response.errorMsg = status.msg();
        return finish(std::move(response));
    }
    response.valueSize = info.dimSize;
    response.numBuckets = static_cast<uint32_t>(info.bucketNum);
    return finish(std::move(response));
}

EmbTableStatusResponse EmbTableRpcService::HandleInsert(
    const EmbTableInsertRequest& req) {
    UbDiag::PerfPoint point(PerfKey::EMB_RPC_INSERT_TOTAL,
                            UbDiag::PerfLevel::KEY_MODULE);
    point.Start();
    auto finish = [&point](EmbTableStatusResponse response) {
        point.End(response.statusCode);
        return response;
    };
    TableMetaInfo info;
    auto status = client_.GetTableInfo(req.tableName, info);
    if (!status.IsOk()) return finish(ToResponse(status));
    const uint64_t valueSize = info.dimSize;
    uint64_t expectedSize = 0;
    if (!CheckedMultiply(req.keys.size(), valueSize, expectedSize) ||
        req.dataSize != expectedSize) {
        return finish(ToResponse(Status::Error(
            ErrorCode::kInvalidArgument, "invalid Insert shared memory size")));
    }
    if (req.keys.empty()) return finish(EmbTableStatusResponse{});

    UbDiag::PerfPoint resolvePoint(PerfKey::EMB_RD_DUMMY_RPC_SHM_RESOLVE,
                                   UbDiag::PerfLevel::MODULE);
    resolvePoint.Start();
    auto mapping = ResolveSharedMemory(req.shmName);
    resolvePoint.End(mapping ? 0
                             : static_cast<int>(ErrorCode::kNotFound));
    if (!mapping ||
        !IsRangeValid(req.dataOffset, req.dataSize, mapping->size)) {
        return finish(ToResponse(Status::Error(
            ErrorCode::kOutOfRange,
            "Insert shared memory range is invalid")));
    }
    const char* data = static_cast<const char*>(mapping->base) + req.dataOffset;
    std::vector<StringView> values;
    values.reserve(req.keys.size());
    for (size_t i = 0; i < req.keys.size(); ++i) {
        values.emplace_back(data + i * valueSize, valueSize);
    }
    return finish(ToResponse(client_.Insert(req.tableName, req.keys, values)));
}

EmbTableFindResponse EmbTableRpcService::HandleFind(
    const EmbTableFindRequest& req) {
    EmbTableFindResponse response;
    response.requestId = req.requestId;
    response.handlerEnterNs = MonotonicNowNs();
    UbDiag::PerfPoint totalPoint(PerfKey::EMB_RD_DUMMY_RPC_HANDLE_TOTAL,
                                 UbDiag::PerfLevel::KEY_MODULE);
    totalPoint.Start();
    auto finish = [&response]() {
        response.handlerExitNs = MonotonicNowNs();
        return response;
    };
    TableMetaInfo info;
    UbDiag::PerfPoint metaPoint(PerfKey::EMB_RD_DUMMY_RPC_META,
                                UbDiag::PerfLevel::MODULE);
    metaPoint.Start();
    auto infoStatus = client_.GetTableInfo(req.tableName, info);
    metaPoint.End(infoStatus.IsOk() ? 0 : infoStatus.code());
    if (!infoStatus.IsOk()) {
        response.statusCode = infoStatus.code();
        response.errorMsg = infoStatus.msg();
        totalPoint.End(response.statusCode);
        return finish();
    }
    const uint64_t valueSize = info.dimSize;
    uint64_t entrySize = valueSize + 1;
    uint64_t requiredSize = 0;
    if (entrySize == 0 ||
        !CheckedMultiply(req.keys.size(), entrySize, requiredSize) ||
        req.targetCapacity < requiredSize) {
        response.statusCode = static_cast<int32_t>(ErrorCode::kOutOfRange);
        response.errorMsg = "Find shared memory capacity is insufficient";
        totalPoint.End(response.statusCode);
        return finish();
    }
    if (req.keys.empty()) {
        totalPoint.End(0);
        return finish();
    }

    UbDiag::PerfPoint resolvePoint(PerfKey::EMB_RD_DUMMY_RPC_SHM_RESOLVE,
                                   UbDiag::PerfLevel::MODULE);
    resolvePoint.Start();
    auto mapping = ResolveSharedMemory(req.shmName);
    resolvePoint.End(mapping ? 0
                             : static_cast<int>(ErrorCode::kNotFound));
    if (!mapping ||
        !IsRangeValid(req.targetOffset, requiredSize, mapping->size)) {
        response.statusCode = static_cast<int32_t>(ErrorCode::kOutOfRange);
        response.errorMsg = "Find shared memory range is invalid";
        totalPoint.End(response.statusCode);
        return finish();
    }

    std::vector<StringView> values;
    std::vector<std::shared_ptr<mooncake::BufferHandle>> handles;
    UbDiag::PerfPoint findPoint(PerfKey::EMB_RD_DUMMY_RPC_CORE_FIND,
                                UbDiag::PerfLevel::KEY_MODULE);
    findPoint.Start();
    Status status = client_.Find(req.tableName, req.keys, values, handles);
    findPoint.End(status.IsOk() ? 0 : status.code());
    if (!status.IsOk()) {
        response.statusCode = status.code();
        response.errorMsg = status.msg();
        totalPoint.End(response.statusCode);
        return finish();
    }

    UbDiag::PerfPoint packPoint(PerfKey::EMB_RD_DUMMY_RPC_SHM_PACK,
                                UbDiag::PerfLevel::MODULE);
    packPoint.Start();
    char* target = static_cast<char*>(mapping->base) + req.targetOffset;
    for (size_t i = 0; i < req.keys.size(); ++i) {
        char* entry = target + i * entrySize;
        if (i < values.size() && values[i].data() &&
            values[i].size() >= valueSize) {
            entry[0] = 1;
            std::memcpy(entry + 1, values[i].data(), valueSize);
        } else {
            entry[0] = 0;
            std::memset(entry + 1, 0, valueSize);
        }
    }
    packPoint.End(0);
    response.transferredSize = requiredSize;
    totalPoint.End(0);
    return finish();
}

EmbTableStatusResponse EmbTableRpcService::HandleBuildIndex(
    const EmbTableBuildIndexRequest& req) {
    UbDiag::PerfPoint point(PerfKey::EMB_RPC_BUILD_INDEX_TOTAL,
                            UbDiag::PerfLevel::KEY_MODULE);
    point.Start();
    auto response = ToResponse(client_.BuildIndex(req.tableName));
    point.End(response.statusCode);
    return response;
}

EmbTableStatusResponse EmbTableRpcService::HandleCreateTable(
    const EmbTableCreateRequest& req) {
    UbDiag::PerfPoint point(PerfKey::EMB_RPC_CREATE_TABLE_TOTAL,
                            UbDiag::PerfLevel::KEY_MODULE);
    point.Start();
    auto response = ToResponse(
        client_.CreateTable(req.tableName, req.numBuckets, req.valueSize));
    point.End(response.statusCode);
    return response;
}

EmbTableStatusResponse EmbTableRpcService::HandleAlterTable(
    const EmbTableAlterRequest& req) {
    UbDiag::PerfPoint point(PerfKey::EMB_RPC_ALTER_TABLE_TOTAL,
                            UbDiag::PerfLevel::KEY_MODULE);
    point.Start();
    auto response = ToResponse(
        client_.AlterTable(req.tableName, req.numBuckets, req.valueSize));
    point.End(response.statusCode);
    return response;
}

EmbTableStatusResponse EmbTableRpcService::HandleDeleteTable(
    const EmbTableDeleteRequest& req) {
    UbDiag::PerfPoint point(PerfKey::EMB_RPC_DELETE_TABLE_TOTAL,
                            UbDiag::PerfLevel::KEY_MODULE);
    point.Start();
    auto response = ToResponse(client_.DeleteTable(req.tableName));
    point.End(response.statusCode);
    return response;
}

}  // namespace embtable
