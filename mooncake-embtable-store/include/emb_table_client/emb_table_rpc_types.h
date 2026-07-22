#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "ylt/reflection/user_reflect_macro.hpp"

namespace embtable {

// EmbTable RPC carries only control data. Insert values and Find results are
// exchanged through a POSIX shared-memory region registered by DummyClient.
struct RegisterEmbTableShmRequest {
    std::string shmName;
    uint64_t shmSize = 0;
};
YLT_REFL(RegisterEmbTableShmRequest, shmName, shmSize);

struct UnregisterEmbTableShmRequest {
    std::string shmName;
};
YLT_REFL(UnregisterEmbTableShmRequest, shmName);

struct EmbTableStatusResponse {
    int32_t statusCode = 0;
    std::string errorMsg;
};
YLT_REFL(EmbTableStatusResponse, statusCode, errorMsg);

struct EmbTableInfoRequest {
    std::string tableName;
};
YLT_REFL(EmbTableInfoRequest, tableName);

struct EmbTableInfoResponse {
    int32_t statusCode = 0;
    std::string errorMsg;
    uint64_t valueSize = 0;
    uint32_t numBuckets = 0;
};
YLT_REFL(EmbTableInfoResponse, statusCode, errorMsg, valueSize, numBuckets);

struct EmbTableInsertRequest {
    std::string tableName;
    std::vector<uint64_t> keys;
    std::string shmName;
    uint64_t dataOffset = 0;
    uint64_t dataSize = 0;
};
YLT_REFL(EmbTableInsertRequest, tableName, keys, shmName, dataOffset, dataSize);

struct EmbTableFindRequest {
    uint64_t requestId = 0;
    std::string tableName;
    std::vector<uint64_t> keys;
    std::string shmName;
    uint64_t targetOffset = 0;
    uint64_t targetCapacity = 0;
};
YLT_REFL(EmbTableFindRequest, requestId, tableName, keys, shmName, targetOffset,
         targetCapacity);

struct EmbTableFindResponse {
    uint64_t requestId = 0;
    uint64_t handlerEnterNs = 0;
    uint64_t handlerExitNs = 0;
    int32_t statusCode = 0;
    std::string errorMsg;
    uint64_t transferredSize = 0;
};
YLT_REFL(EmbTableFindResponse, requestId, handlerEnterNs, handlerExitNs,
         statusCode, errorMsg, transferredSize);

struct EmbTableBuildIndexRequest {
    std::string tableName;
};
YLT_REFL(EmbTableBuildIndexRequest, tableName);

struct EmbTableCreateRequest {
    std::string tableName;
    uint32_t numBuckets = 0;
    uint64_t valueSize = 0;
};
YLT_REFL(EmbTableCreateRequest, tableName, numBuckets, valueSize);

struct EmbTableAlterRequest {
    std::string tableName;
    uint32_t numBuckets = 0;
    uint64_t valueSize = 0;
};
YLT_REFL(EmbTableAlterRequest, tableName, numBuckets, valueSize);

struct EmbTableDeleteRequest {
    std::string tableName;
};
YLT_REFL(EmbTableDeleteRequest, tableName);

}  // namespace embtable
