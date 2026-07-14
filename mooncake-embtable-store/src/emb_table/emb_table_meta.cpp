#include "emb_table/emb_table_meta.h"

#include <glog/logging.h>
#include "ylt/struct_json/json_reader.h"
#include "ylt/struct_json/json_writer.h"

namespace embtable {

namespace {
constexpr size_t kMetaObjectSize = 1ull * 1024 * 1024;  // 1 MiB

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

    // put_from into Mooncake Store.
    int ret = realClient_->put_from(metaKey(params.tableKey), json.data(),
                                    json.size());
    if (ret != 0) {
        return Status::Error(ErrorCode::kIOError,
                             "put_from failed for table meta: " + params.tableKey);
    }
    metaInfo_ = params;
    return Status::OK();
}

Status EmbTableMeta::QueryTableMeta(const std::string& tableKey,
                                    TableMetaInfo& meta) {
    if (tableKey.empty()) {
        return Status::Error(ErrorCode::kInvalidArgument, "empty tableKey");
    }
    std::string buf(kMetaObjectSize, '\0');
    int ret = realClient_->get_into(metaKey(tableKey), buf.data(), buf.size());
    if (ret != 0) {
        return Status::Error(ErrorCode::kNotFound,
                             "table meta not found: " + tableKey);
    }
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
    int ret = realClient_->put_from(metaKey(meta.tableKey), json.data(),
                                    json.size());
    if (ret != 0) {
        return Status::Error(ErrorCode::kIOError,
                             "put_from (update) failed for table meta: " + meta.tableKey);
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
    int ret = realClient_->put_from(bucketMetaKey(info.bucketKey), json.data(),
                                    json.size());
    if (ret != 0) {
        return Status::Error(ErrorCode::kIOError,
                             "put_from failed for bucket meta: " + info.bucketKey);
    }
    return Status::OK();
}

Status EmbTableMeta::QueryBucketMeta(const std::string& bucketKey,
                                     BucketInfo& info) {
    if (bucketKey.empty()) {
        return Status::Error(ErrorCode::kInvalidArgument, "empty bucketKey");
    }
    std::string buf(kMetaObjectSize, '\0');
    int ret =
        realClient_->get_into(bucketMetaKey(bucketKey), buf.data(), buf.size());
    if (ret != 0) {
        return Status::Error(ErrorCode::kNotFound,
                             "bucket meta not found: " + bucketKey);
    }
    struct_json::from_json(info, buf);
    return Status::OK();
}

}  // namespace embtable
