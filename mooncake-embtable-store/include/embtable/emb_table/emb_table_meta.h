#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "embtable/types.h"
#include "real_client.h"
#include "ylt/reflection/user_reflect_macro.hpp"

namespace embtable {

// TableMetaInfo records the static configuration of an EmbTable (design doc
// 4.1.3). It is serialized via ylt::struct_json and stored in Mooncake Store
// so all nodes can QueryTableMeta.
struct TableMetaInfo {
    std::string tableKey;
    int tableIndex = 0;
    std::string tableName;
    uint64_t dimSize = 0;          // value size in bytes (8B-512B)
    uint64_t tableCapacity = 0;
    uint64_t bucketNum = 0;
    HashFunctionType hashType = HashFunctionType::kXxHash;
    uint64_t bucketCapacity = 0;
};
YLT_REFL(TableMetaInfo, tableKey, tableIndex, tableName, dimSize,
         tableCapacity, bucketNum, hashType, bucketCapacity);

struct BucketInfo {
    std::string bucketKey;
    uint64_t valueSize = 0;        // 8B-512B
    uint64_t capacity = 0;
    uint64_t currentSize = 0;
    std::string tableKey;
};
YLT_REFL(BucketInfo, bucketKey, valueSize, capacity, currentSize, tableKey);

struct ReplicaInfo {
    std::string nodeId;
    std::string endpoint;
    bool isLocal = false;
};
YLT_REFL(ReplicaInfo, nodeId, endpoint, isLocal);

// EmbTableMeta manages creation/query/update of TableMetaInfo stored in
// Mooncake Store (design doc 4.1).
class EmbTableMeta {
   public:
    explicit EmbTableMeta(std::shared_ptr<mooncake::RealClient> realClient);

    // Create the TableMetaInfo object in Mooncake Store. Fails if it already
    // exists.
    Status CreateTableMeta(const TableMetaInfo& params);

    // Query an existing TableMetaInfo by tableKey.
    Status QueryTableMeta(const std::string& tableKey, TableMetaInfo& meta);

    // Update an existing TableMetaInfo (e.g. after adding buckets).
    Status UpdateTableMeta(const TableMetaInfo& meta);

    const TableMetaInfo& GetLocalMeta() const { return metaInfo_; }

   private:
    TableMetaInfo metaInfo_;
    std::shared_ptr<mooncake::RealClient> realClient_;
};

}  // namespace embtable
