#include "embtable/share_object/share_map_meta.h"

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
    if (json.size() > metaObject_.Size()) {
        return Status::Error(ErrorCode::kBufferFull,
                             "meta payload exceeds ShareObject capacity");
    }
    auto s = metaObject_.Create();
    if (!s.IsOk()) return s;
    s = metaObject_.Write(0, json.data(), json.size());
    if (!s.IsOk()) return s;
    return metaObject_.Publish();
}

Status ShareMapMeta::Deserialize() {
    auto s = metaObject_.Create();
    if (!s.IsOk()) return s;
    s = metaObject_.Import();
    if (!s.IsOk()) return s;
    std::string raw(static_cast<const char*>(metaObject_.Data()),
                    metaObject_.Size());
    ShareMapMetaPayload payload;
    struct_json::from_json(payload, raw);
    bucketKey_ = std::move(payload.bucketKey);
    valueSize_ = payload.valueSize;
    totalSize_ = payload.totalSize;
    objectInfos_ = std::move(payload.objectInfos);
    return Status::OK();
}

}  // namespace embtable
