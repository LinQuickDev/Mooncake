#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "ylt/reflection/user_reflect_macro.hpp"

namespace embtable {

// Query data for a single bucket. The RPC carries control metadata only.
// The service packs results in its registered transfer buffer and uses the
// Transfer Engine to write directly into the registered client buffer.
struct QueryDataRequest {
    std::string bucketKey;
    std::vector<uint64_t> keys;
    uint64_t valueSize = 0;
    std::string targetEndpoint;
    uint64_t targetAddress = 0;
    uint64_t targetCapacity = 0;
};
YLT_REFL(QueryDataRequest, bucketKey, keys, valueSize, targetEndpoint,
         targetAddress, targetCapacity);

struct QueryDataResponse {
    int32_t statusCode = 0;
    std::string errorMsg;
    uint64_t transferredSize = 0;
    // Per-key found flags (1 = found, 0 = not found).
    std::vector<int8_t> foundFlags;
};
YLT_REFL(QueryDataResponse, statusCode, errorMsg, transferredSize, foundFlags);

// Batch query: multiple buckets on the same remote node in one control RPC
// and one aggregated TE data transfer.
struct BatchQueryEntry {
    std::string bucketKey;
    std::vector<uint64_t> keys;
};
YLT_REFL(BatchQueryEntry, bucketKey, keys);

struct BatchQueryDataRequest {
    std::vector<BatchQueryEntry> entries;
    uint64_t valueSize = 0;
    std::string targetEndpoint;
    uint64_t targetAddress = 0;
    uint64_t targetCapacity = 0;
};
YLT_REFL(BatchQueryDataRequest, entries, valueSize, targetEndpoint,
         targetAddress, targetCapacity);

struct BatchQueryDataResponse {
    int32_t statusCode = 0;
    std::string errorMsg;
    std::vector<QueryDataResponse> responses;
};
YLT_REFL(BatchQueryDataResponse, statusCode, errorMsg, responses);

// Publish request: insert key/value pairs into a bucket on the remote node.
struct PublishRequest {
    std::string bucketKey;
    uint64_t valueSize = 0;
    std::vector<uint64_t> keys;
    // Concatenated values: keys.size() * valueSize bytes.
    std::string valuesData;
};
YLT_REFL(PublishRequest, bucketKey, valueSize, keys, valuesData);

struct PublishResponse {
    int32_t statusCode = 0;
    std::string errorMsg;
};
YLT_REFL(PublishResponse, statusCode, errorMsg);

// BuildIndex request: build PHF for a bucket on the remote node.
struct BuildIndexRequest {
    std::string bucketKey;
};
YLT_REFL(BuildIndexRequest, bucketKey);

struct BuildIndexResponse {
    int32_t statusCode = 0;
    std::string errorMsg;
};
YLT_REFL(BuildIndexResponse, statusCode, errorMsg);

}  // namespace embtable
