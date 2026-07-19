#include "share_map_store/share_map_store_rpc_service.h"

#include <cstring>
#include <limits>
#include <unordered_set>
#include <glog/logging.h>

namespace embtable {

QueryDataResponse ShareMapStoreRpcService::HandleQueryData(
    const QueryDataRequest& req) {
    QueryDataResponse resp;
    if (req.bucketKey.empty() || req.valueSize == 0 ||
        req.targetEndpoint.empty() || req.targetAddress == 0) {
        resp.statusCode = static_cast<int32_t>(ErrorCode::kInvalidArgument);
        resp.errorMsg = "invalid query request";
        return resp;
    }
    uint64_t entrySize = 0;
    uint64_t expectedSize = 0;
    if (!CheckedAdd(1, req.valueSize, entrySize) ||
        !CheckedMultiply(req.keys.size(), entrySize, expectedSize) ||
        expectedSize > req.targetCapacity) {
        resp.statusCode = static_cast<int32_t>(ErrorCode::kOutOfRange);
        resp.errorMsg = "query request size exceeds target capacity";
        return resp;
    }
    Status s = store_.QueryDataToBuffer(req.bucketKey, req.keys, req.valueSize,
                                        req.targetEndpoint, req.targetAddress,
                                        req.targetCapacity,
                                        resp.transferredSize, resp.foundFlags);
    if (!s.IsOk()) {
        resp.statusCode = s.code();
        resp.errorMsg = s.msg();
    }
    return resp;
}

BatchQueryDataResponse ShareMapStoreRpcService::HandleBatchQueryData(
    const BatchQueryDataRequest& req) {
    BatchQueryDataResponse resp;
    if (req.entries.empty()) return resp;
    if (req.valueSize == 0 || req.targetEndpoint.empty() ||
        req.targetAddress == 0) {
        resp.statusCode = static_cast<int32_t>(ErrorCode::kInvalidArgument);
        resp.errorMsg = "invalid batch query request";
        return resp;
    }
    std::unordered_set<std::string> bucketSet;
    for (const auto& entry : req.entries) {
        if (entry.bucketKey.empty() ||
            !bucketSet.insert(entry.bucketKey).second) {
            resp.statusCode = static_cast<int32_t>(ErrorCode::kInvalidArgument);
            resp.errorMsg = "duplicate or empty bucket key";
            return resp;
        }
    }

    // Aggregate all bucket queries into ONE buffer + ONE direct TE write.
    std::vector<std::string> bucketKeys;
    std::vector<std::vector<uint64_t>> keysPerBucket;
    bucketKeys.reserve(req.entries.size());
    keysPerBucket.reserve(req.entries.size());
    for (const auto& entry : req.entries) {
        bucketKeys.push_back(entry.bucketKey);
        keysPerBucket.push_back(entry.keys);
    }

    uint64_t transferredSize = 0;
    std::vector<std::vector<int8_t>> foundFlagsPerBucket;
    Status s = store_.BatchQueryDataToBuffer(
        bucketKeys, keysPerBucket, req.valueSize, req.targetEndpoint,
        req.targetAddress, req.targetCapacity, transferredSize,
        foundFlagsPerBucket);
    if (!s.IsOk()) {
        resp.statusCode = s.code();
        resp.errorMsg = s.msg();
        return resp;
    }

    if (foundFlagsPerBucket.size() != req.entries.size()) {
        resp.statusCode = static_cast<int32_t>(ErrorCode::kInternal);
        resp.errorMsg = "batch query returned mismatched bucket count";
        return resp;
    }
    // Return one control response per bucket; all data already resides in the
    // single registered client buffer.
    resp.responses.reserve(req.entries.size());
    for (size_t i = 0; i < req.entries.size(); ++i) {
        QueryDataResponse single;
        single.transferredSize = transferredSize;
        if (i < foundFlagsPerBucket.size()) {
            single.foundFlags = std::move(foundFlagsPerBucket[i]);
        }
        resp.responses.push_back(std::move(single));
    }
    return resp;
}

PublishResponse ShareMapStoreRpcService::HandlePublish(
    const PublishRequest& req) {
    PublishResponse resp;
    if (req.bucketKey.empty() || req.valueSize == 0) {
        resp.statusCode = static_cast<int32_t>(ErrorCode::kInvalidArgument);
        resp.errorMsg = "invalid publish request";
        return resp;
    }
    uint64_t expectedSize = 0;
    if (!CheckedMultiply(req.keys.size(), req.valueSize, expectedSize) ||
        expectedSize != req.valuesData.size()) {
        resp.statusCode = static_cast<int32_t>(ErrorCode::kInvalidArgument);
        resp.errorMsg = "publish valuesData size mismatch";
        return resp;
    }
    if (req.keys.empty()) return resp;

    std::vector<StringView> values;
    values.reserve(req.keys.size());
    const char* data = req.valuesData.data();
    for (size_t i = 0; i < req.keys.size(); ++i) {
        uint64_t offset = 0;
        if (!CheckedMultiply(i, req.valueSize, offset) ||
            !IsRangeValid(offset, req.valueSize, req.valuesData.size())) {
            resp.statusCode = static_cast<int32_t>(ErrorCode::kOutOfRange);
            resp.errorMsg = "publish value range is invalid";
            return resp;
        }
        values.emplace_back(data + offset, req.valueSize);
    }
    Status s = store_.Publish(req.bucketKey, req.valueSize, req.keys, values);
    if (!s.IsOk()) {
        resp.statusCode = s.code();
        resp.errorMsg = s.msg();
    }
    return resp;
}

BuildIndexResponse ShareMapStoreRpcService::HandleBuildIndex(
    const BuildIndexRequest& req) {
    BuildIndexResponse resp;
    Status s = store_.BuildIndex(req.bucketKey);
    if (!s.IsOk()) {
        resp.statusCode = s.code();
        resp.errorMsg = s.msg();
    }
    return resp;
}

}  // namespace embtable
