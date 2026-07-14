#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "share_object/share_object.h"
#include "emb_types.h"
#include "real_client.h"
#include "ylt/reflection/user_reflect_macro.hpp"

namespace embtable {

// Metadata describing one backing ShareObject within a ShareMap.
struct ObjectInfo {
    std::string key;
    uint64_t size = 0;
    uint64_t offset = 0;
    std::string type;  // "keys" | "values" | "index" | "meta"
};
YLT_REFL(ObjectInfo, key, size, offset, type);

// Wrapper holding the serialized meta payload (design doc section 8.10).
struct ShareMapMetaPayload {
    std::string bucketKey;
    uint64_t valueSize = 0;
    uint64_t totalSize = 0;
    std::vector<ObjectInfo> objectInfos;
};
YLT_REFL(ShareMapMetaPayload, bucketKey, valueSize, totalSize, objectInfos);

// ShareMapMeta records the ShareObjects used by a ShareMap (key vector, value
// vector, index) so that other nodes can Import and reconstruct the ShareMap.
// Serialized via ylt::struct_json (design doc section 8.10).
class ShareMapMeta {
   public:
    ShareMapMeta(const std::string& bucketKey,
                 std::shared_ptr<mooncake::RealClient> realClient,
                 size_t metaCapacity = 4ull * 1024 * 1024);

    Status AddObjectInfo(const ObjectInfo& info);

    // Serialize the in-memory ObjectInfo list into the backing ShareObject and
    // Publish it.
    Status Serialize();

    // Pull the meta ShareObject and deserialize the ObjectInfo list.
    Status Deserialize();

    const std::vector<ObjectInfo>& GetObjectInfos() const {
        return objectInfos_;
    }
    void SetValueSize(uint64_t valueSize) { valueSize_ = valueSize; }
    uint64_t GetValueSize() const { return valueSize_; }
    void SetTotalSize(uint64_t totalSize) { totalSize_ = totalSize; }
    uint64_t GetTotalSize() const { return totalSize_; }

   private:
    std::string bucketKey_;
    uint64_t valueSize_ = 0;
    uint64_t totalSize_ = 0;
    std::vector<ObjectInfo> objectInfos_;
    ShareObject metaObject_;
};

}  // namespace embtable
