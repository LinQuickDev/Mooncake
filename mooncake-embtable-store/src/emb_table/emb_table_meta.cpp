#include "emb_table/emb_table_meta.h"

#include <glog/logging.h>
#include <span>
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
}  // namespace

EmbTableMeta::EmbTableMeta(std::shared_ptr<mooncake::RealClient> realClient)
    : realClient_(std::move(realClient)) {}

Status EmbTableMeta::CreateTableMeta(const TableMetaInfo& params) {
    if (params.tableKey.empty()) {
        return Status::Error(ErrorCode::kInvalidArgument, "empty tableKey");
    }
    // Serialize to JSON.
    std::string json;
    struct_json::to_json(params, json);

    mooncake::ReplicateConfig config;
    int ret = realClient_->put(
        metaKey(params.tableKey),
        std::span<const char>(json.data(), json.size()), config);
    if (ret != 0) {
        return Status::Error(ErrorCode::kIOError,
                             "put failed for table meta: " + params.tableKey);
    }
    metaInfo_ = params;
    return Status::OK();
}

Status EmbTableMeta::QueryTableMeta(const std::string& tableKey,
                                    TableMetaInfo& meta) {
    if (tableKey.empty()) {
        return Status::Error(ErrorCode::kInvalidArgument, "empty tableKey");
    }
    auto handle = realClient_->get_buffer(metaKey(tableKey));
    if (!handle) {
        return Status::Error(ErrorCode::kNotFound,
                             "table meta not found: " + tableKey);
    }
    std::string buf(static_cast<const char*>(handle->ptr()), handle->size());
    struct_json::from_json(meta, buf);
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
    int ret = realClient_->put(
        metaKey(meta.tableKey),
        std::span<const char>(json.data(), json.size()), config);
    if (ret != 0) {
        return Status::Error(ErrorCode::kIOError,
                             "put (update) failed for table meta: " + meta.tableKey);
    }
    metaInfo_ = meta;
    return Status::OK();
}

Status EmbTableMeta::CreateBucketMeta(const BucketInfo& info) {
    if (info.bucketKey.empty()) {
        return Status::Error(ErrorCode::kInvalidArgument, "empty bucketKey");
    }
    std::string json;
    struct_json::to_json(info, json);
    mooncake::ReplicateConfig config;
    int ret = realClient_->put(
        bucketMetaKey(info.bucketKey),
        std::span<const char>(json.data(), json.size()), config);
    if (ret != 0) {
        return Status::Error(ErrorCode::kIOError,
                             "put failed for bucket meta: " + info.bucketKey);
    }
    return Status::OK();
}

Status EmbTableMeta::QueryBucketMeta(const std::string& bucketKey,
                                     BucketInfo& info) {
    if (bucketKey.empty()) {
        return Status::Error(ErrorCode::kInvalidArgument, "empty bucketKey");
    }
    auto handle = realClient_->get_buffer(bucketMetaKey(bucketKey));
    if (!handle) {
        return Status::Error(ErrorCode::kNotFound,
                             "bucket meta not found: " + bucketKey);
    }
    std::string buf(static_cast<const char*>(handle->ptr()), handle->size());
    struct_json::from_json(info, buf);
    return Status::OK();
}

}  // namespace embtable
