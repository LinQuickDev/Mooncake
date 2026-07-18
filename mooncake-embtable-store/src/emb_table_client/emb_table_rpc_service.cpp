#include "emb_table_client/emb_table_rpc_service.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "emb_table_client/emb_table_client.h"

namespace embtable {

namespace {

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
    if (!IsValidShmName(req.shmName) || req.shmSize == 0 ||
        req.shmSize > std::numeric_limits<size_t>::max()) {
        return ToResponse(Status::Error(ErrorCode::kInvalidArgument,
                                        "invalid shared memory descriptor"));
    }

    {
        std::lock_guard<std::mutex> lock(mappingsMutex_);
        auto it = mappings_.find(req.shmName);
        if (it != mappings_.end()) {
            if (it->second->size == req.shmSize) {
                return EmbTableStatusResponse{};
            }
            return ToResponse(Status::Error(
                ErrorCode::kAlreadyExists,
                "shared memory name already registered with another size"));
        }
    }

    int fd = shm_open(req.shmName.c_str(), O_RDWR, 0);
    if (fd < 0) {
        return ToResponse(Status::Error(
            ErrorCode::kIOError,
            "shm_open failed: " + std::string(std::strerror(errno))));
    }
    struct stat statBuffer{};
    if (fstat(fd, &statBuffer) != 0 || statBuffer.st_size < 0 ||
        static_cast<uint64_t>(statBuffer.st_size) < req.shmSize) {
        close(fd);
        return ToResponse(Status::Error(
            ErrorCode::kInvalidArgument,
            "shared memory object is smaller than requested mapping"));
    }
    void* base = mmap(nullptr, static_cast<size_t>(req.shmSize),
                      PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (base == MAP_FAILED) {
        return ToResponse(
            Status::Error(ErrorCode::kIOError,
                          "mmap failed: " + std::string(std::strerror(errno))));
    }

    auto mapping = std::make_shared<SharedMemoryMapping>();
    mapping->base = base;
    mapping->size = req.shmSize;
    {
        std::lock_guard<std::mutex> lock(mappingsMutex_);
        auto [it, inserted] = mappings_.emplace(req.shmName, mapping);
        if (!inserted && it->second->size != req.shmSize) {
            return ToResponse(
                Status::Error(ErrorCode::kAlreadyExists,
                              "shared memory name concurrently registered"));
        }
    }
    return EmbTableStatusResponse{};
}

EmbTableStatusResponse EmbTableRpcService::HandleUnregisterSharedMemory(
    const UnregisterEmbTableShmRequest& req) {
    std::lock_guard<std::mutex> lock(mappingsMutex_);
    mappings_.erase(req.shmName);
    return EmbTableStatusResponse{};
}

EmbTableInfoResponse EmbTableRpcService::HandleGetInfo(
    const EmbTableInfoRequest& req) {
    EmbTableInfoResponse response;
    TableMetaInfo info;
    auto status = client_.GetTableInfo(req.tableName, info);
    if (!status.IsOk()) {
        response.statusCode = status.code();
        response.errorMsg = status.msg();
        return response;
    }
    response.valueSize = info.dimSize;
    response.numBuckets = static_cast<uint32_t>(info.bucketNum);
    return response;
}

EmbTableStatusResponse EmbTableRpcService::HandleInsert(
    const EmbTableInsertRequest& req) {
    TableMetaInfo info;
    auto status = client_.GetTableInfo(req.tableName, info);
    if (!status.IsOk()) return ToResponse(status);
    const uint64_t valueSize = info.dimSize;
    uint64_t expectedSize = 0;
    if (!CheckedMultiply(req.keys.size(), valueSize, expectedSize) ||
        req.dataSize != expectedSize) {
        return ToResponse(Status::Error(ErrorCode::kInvalidArgument,
                                        "invalid Insert shared memory size"));
    }
    if (req.keys.empty()) return EmbTableStatusResponse{};

    auto mapping = ResolveSharedMemory(req.shmName);
    if (!mapping ||
        !IsRangeValid(req.dataOffset, req.dataSize, mapping->size)) {
        return ToResponse(Status::Error(
            ErrorCode::kOutOfRange, "Insert shared memory range is invalid"));
    }
    const char* data = static_cast<const char*>(mapping->base) + req.dataOffset;
    std::vector<StringView> values;
    values.reserve(req.keys.size());
    for (size_t i = 0; i < req.keys.size(); ++i) {
        values.emplace_back(data + i * valueSize, valueSize);
    }
    return ToResponse(client_.Insert(req.tableName, req.keys, values));
}

EmbTableFindResponse EmbTableRpcService::HandleFind(
    const EmbTableFindRequest& req) {
    EmbTableFindResponse response;
    TableMetaInfo info;
    auto infoStatus = client_.GetTableInfo(req.tableName, info);
    if (!infoStatus.IsOk()) {
        response.statusCode = infoStatus.code();
        response.errorMsg = infoStatus.msg();
        return response;
    }
    const uint64_t valueSize = info.dimSize;
    uint64_t entrySize = valueSize + 1;
    uint64_t requiredSize = 0;
    if (entrySize == 0 ||
        !CheckedMultiply(req.keys.size(), entrySize, requiredSize) ||
        req.targetCapacity < requiredSize) {
        response.statusCode = static_cast<int32_t>(ErrorCode::kOutOfRange);
        response.errorMsg = "Find shared memory capacity is insufficient";
        return response;
    }
    if (req.keys.empty()) return response;

    auto mapping = ResolveSharedMemory(req.shmName);
    if (!mapping ||
        !IsRangeValid(req.targetOffset, requiredSize, mapping->size)) {
        response.statusCode = static_cast<int32_t>(ErrorCode::kOutOfRange);
        response.errorMsg = "Find shared memory range is invalid";
        return response;
    }

    std::vector<StringView> values;
    std::vector<std::shared_ptr<mooncake::BufferHandle>> handles;
    Status status = client_.Find(req.tableName, req.keys, values, handles);
    if (!status.IsOk()) {
        response.statusCode = status.code();
        response.errorMsg = status.msg();
        return response;
    }

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
    response.transferredSize = requiredSize;
    return response;
}

EmbTableStatusResponse EmbTableRpcService::HandleBuildIndex(
    const EmbTableBuildIndexRequest& req) {
    return ToResponse(client_.BuildIndex(req.tableName));
}

EmbTableStatusResponse EmbTableRpcService::HandleCreateTable(
    const EmbTableCreateRequest& req) {
    return ToResponse(
        client_.CreateTable(req.tableName, req.numBuckets, req.valueSize));
}

EmbTableStatusResponse EmbTableRpcService::HandleAlterTable(
    const EmbTableAlterRequest& req) {
    return ToResponse(
        client_.AlterTable(req.tableName, req.numBuckets, req.valueSize));
}

EmbTableStatusResponse EmbTableRpcService::HandleDeleteTable(
    const EmbTableDeleteRequest& req) {
    return ToResponse(client_.DeleteTable(req.tableName));
}

}  // namespace embtable
