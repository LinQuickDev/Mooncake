#include "emb_table/emb_table_meta.h"

#include <glog/logging.h>
#include <span>
#include <exception>
#include "ylt/struct_json/json_reader.h"
#include "ylt/struct_json/json_writer.h"

namespace embtable {

namespace {
std::string metaKey(const std::string& tableKey) {
    return tableKey + "_tablemeta";
}

std::string bucketMetaKey(const std::string& bucketKey) {
    return bucketKey + "_bucketmeta";
}

Status ValidateTableMeta(const TableMetaInfo& meta) {
    if (meta.tableKey.empty() || meta.tableName.empty() || meta.dimSize == 0 ||
        meta.bucketNum == 0 || meta.bucketCapacity == 0 ||
        meta.tableCapacity == 0) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "invalid table metadata fields");
    }
    uint64_t expectedCapacity = 0;
    if (!CheckedMultiply(meta.bucketNum, meta.bucketCapacity,
                         expectedCapacity) ||
        meta.tableCapacity < expectedCapacity) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "invalid table metadata capacity");
    }
    return Status::OK();
}

Status ValidateBucketMeta(const BucketInfo& info) {
    if (info.bucketKey.empty() || info.tableKey.empty() ||
        info.valueSize == 0 || info.capacity == 0 ||
        info.currentSize > info.capacity) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "invalid bucket metadata fields");
    }
    return Status::OK();
}
}  // namespace

EmbTableMeta::EmbTableMeta(std::shared_ptr<mooncake::RealClient> realClient)
    : realClient_(std::move(realClient)) {}

Status EmbTableMeta::CreateTableMeta(const TableMetaInfo& params) {
    auto validation = ValidateTableMeta(params);
    if (!validation.IsOk()) return validation;
    if (!realClient_) {
        return Status::Error(ErrorCode::kInternal,
                             "RealClient is not initialized");
    }
    const int exists = realClient_->isExist(metaKey(params.tableKey));
    if (exists > 0) {
        return Status::Error(ErrorCode::kAlreadyExists,
                             "table already exists: " + params.tableName);
    }
    if (exists < 0) {
        return Status::Error(ErrorCode::kIOError,
                             "failed to check table metadata existence");
    }
    // Serialize to JSON.
    std::string json;
    struct_json::to_json(params, json);

    mooncake::ReplicateConfig config;
    int ret = realClient_->put(metaKey(params.tableKey),
                               std::span<const char>(json.data(), json.size()),
                               config);
    if (ret != 0) {
        return Status::Error(ErrorCode::kIOError,
                             "put failed for table meta: " + params.tableKey);
    }
    metaInfo_ = params;
    return Status::OK();
}

Status EmbTableMeta::QueryTableMeta(const std::string& tableKey,
                                    TableMetaInfo& meta) {
    if (tableKey.empty() || !realClient_) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "invalid table metadata query");
    }
    auto handle = realClient_->get_buffer(metaKey(tableKey));
    if (!handle) {
        return Status::Error(ErrorCode::kNotFound,
                             "table meta not found: " + tableKey);
    }
    std::string buf(static_cast<const char*>(handle->ptr()), handle->size());
    try {
        TableMetaInfo parsed;
        struct_json::from_json(parsed, buf);
        auto validation = ValidateTableMeta(parsed);
        if (!validation.IsOk()) return validation;
        if (parsed.tableKey != tableKey) {
            return Status::Error(ErrorCode::kInvalidArgument,
                                 "table metadata key mismatch");
        }
        meta = std::move(parsed);
    } catch (const std::exception& e) {
        return Status::Error(
            ErrorCode::kInvalidArgument,
            "invalid table metadata JSON: " + std::string(e.what()));
    }
    metaInfo_ = meta;
    return Status::OK();
}

Status EmbTableMeta::UpdateTableMeta(const TableMetaInfo& meta) {
    if (meta.tableKey.empty()) {
        return Status::Error(ErrorCode::kInvalidArgument, "empty tableKey");
    }
    std::string json;
    struct_json::to_json(meta, json);
    mooncake::ReplicateConfig config;
    int ret = realClient_->put(metaKey(meta.tableKey),
                               std::span<const char>(json.data(), json.size()),
                               config);
    if (ret != 0) {
        return Status::Error(
            ErrorCode::kIOError,
            "put (update) failed for table meta: " + meta.tableKey);
    }
    metaInfo_ = meta;
    return Status::OK();
}

Status EmbTableMeta::DeleteTableMeta(const std::string& tableKey) {
    if (tableKey.empty() || !realClient_) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "invalid table metadata delete");
    }
    if (realClient_->remove(metaKey(tableKey)) != 0) {
        return Status::Error(ErrorCode::kIOError,
                             "remove failed for table meta: " + tableKey);
    }
    return Status::OK();
}

Status EmbTableMeta::CreateBucketMeta(const BucketInfo& info) {
    auto validation = ValidateBucketMeta(info);
    if (!validation.IsOk()) return validation;
    std::string json;
    struct_json::to_json(info, json);
    mooncake::ReplicateConfig config;
    int ret = realClient_->put(bucketMetaKey(info.bucketKey),
                               std::span<const char>(json.data(), json.size()),
                               config);
    if (ret != 0) {
        return Status::Error(ErrorCode::kIOError,
                             "put failed for bucket meta: " + info.bucketKey);
    }
    return Status::OK();
}

Status EmbTableMeta::QueryBucketMeta(const std::string& bucketKey,
                                     BucketInfo& info) {
    if (bucketKey.empty() || !realClient_) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "invalid bucket metadata query");
    }
    auto handle = realClient_->get_buffer(bucketMetaKey(bucketKey));
    if (!handle) {
        return Status::Error(ErrorCode::kNotFound,
                             "bucket meta not found: " + bucketKey);
    }
    std::string buf(static_cast<const char*>(handle->ptr()), handle->size());
    try {
        BucketInfo parsed;
        struct_json::from_json(parsed, buf);
        auto validation = ValidateBucketMeta(parsed);
        if (!validation.IsOk()) return validation;
        if (parsed.bucketKey != bucketKey) {
            return Status::Error(ErrorCode::kInvalidArgument,
                                 "bucket metadata key mismatch");
        }
        info = std::move(parsed);
    } catch (const std::exception& e) {
        return Status::Error(
            ErrorCode::kInvalidArgument,
            "invalid bucket metadata JSON: " + std::string(e.what()));
    }
    return Status::OK();
}

Status EmbTableMeta::DeleteBucketMeta(const std::string& bucketKey) {
    if (bucketKey.empty() || !realClient_) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "invalid bucket metadata delete");
    }
    if (realClient_->remove(bucketMetaKey(bucketKey)) != 0) {
        return Status::Error(ErrorCode::kIOError,
                             "remove failed for bucket meta: " + bucketKey);
    }
    return Status::OK();
}

}  // namespace embtable
