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
    uint8_t reserved = 0;
};
YLT_REFL(EmbTableInfoRequest, reserved);

struct EmbTableInfoResponse {
    int32_t statusCode = 0;
    std::string errorMsg;
    uint64_t valueSize = 0;
    uint32_t numBuckets = 0;
};
YLT_REFL(EmbTableInfoResponse, statusCode, errorMsg, valueSize, numBuckets);

struct EmbTableInsertRequest {
    std::vector<uint64_t> keys;
    std::string shmName;
    uint64_t dataOffset = 0;
    uint64_t dataSize = 0;
};
YLT_REFL(EmbTableInsertRequest, keys, shmName, dataOffset, dataSize);

struct EmbTableFindRequest {
    std::vector<uint64_t> keys;
    std::string shmName;
    uint64_t targetOffset = 0;
    uint64_t targetCapacity = 0;
};
YLT_REFL(EmbTableFindRequest, keys, shmName, targetOffset, targetCapacity);

struct EmbTableFindResponse {
    int32_t statusCode = 0;
    std::string errorMsg;
    uint64_t transferredSize = 0;
};
YLT_REFL(EmbTableFindResponse, statusCode, errorMsg, transferredSize);

struct EmbTableBuildIndexRequest {
    uint8_t reserved = 0;
};
YLT_REFL(EmbTableBuildIndexRequest, reserved);

}  // namespace embtable
