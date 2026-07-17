#include "share_object/share_map_meta.h"

#include <glog/logging.h>
#include "ylt/struct_json/json_reader.h"
#include "ylt/struct_json/json_writer.h"

namespace embtable {

ShareMapMeta::ShareMapMeta(const std::string& bucketKey,
                           std::shared_ptr<mooncake::RealClient> realClient,
                           size_t metaCapacity)
    : bucketKey_(bucketKey),
      metaObject_(bucketKey + "_meta", metaCapacity, std::move(realClient)) {}

Status ShareMapMeta::AddObjectInfo(const ObjectInfo& info) {
    objectInfos_.push_back(info);
    return Status::OK();
}

Status ShareMapMeta::Serialize() {
    ShareMapMetaPayload payload{bucketKey_, valueSize_, totalSize_, objectInfos_};
    std::string json;
    struct_json::to_json(payload, json);
    // Layout: [8-byte length][json bytes]
    uint64_t jsonLen = json.size();
    if (sizeof(uint64_t) + jsonLen > metaObject_.Size()) {
        return Status::Error(ErrorCode::kBufferFull,
                             "meta payload exceeds ShareObject capacity");
    }
    auto s = metaObject_.Create();
    if (!s.IsOk()) return s;
    s = metaObject_.Write(0, &jsonLen, sizeof(uint64_t));
    if (!s.IsOk()) return s;
    s = metaObject_.Write(sizeof(uint64_t), json.data(), jsonLen);
    if (!s.IsOk()) return s;
    return metaObject_.Publish();
}

Status ShareMapMeta::Deserialize() {
    auto s = metaObject_.Create();
    if (!s.IsOk()) return s;
    s = metaObject_.Import();
    if (!s.IsOk()) return s;
    // Read the length prefix, then parse exactly jsonLen bytes (not the whole
    // ShareObject, which is padded with zeros and would corrupt JSON parsing).
    uint64_t jsonLen = 0;
    s = metaObject_.Read(0, sizeof(uint64_t), &jsonLen);
    if (!s.IsOk()) return s;
    if (jsonLen == 0 || jsonLen > metaObject_.Size() - sizeof(uint64_t)) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "invalid meta payload length: " +
                                 std::to_string(jsonLen));
    }
    std::string raw(sizeof(uint64_t) + jsonLen, '\0');
    s = metaObject_.Read(0, raw.size(), raw.data());
    if (!s.IsOk()) return s;
    ShareMapMetaPayload payload;
    struct_json::from_json(payload, raw.substr(sizeof(uint64_t)));
    bucketKey_ = std::move(payload.bucketKey);
    valueSize_ = payload.valueSize;
    totalSize_ = payload.totalSize;
    objectInfos_ = std::move(payload.objectInfos);
    return Status::OK();
}

}  // namespace embtable
