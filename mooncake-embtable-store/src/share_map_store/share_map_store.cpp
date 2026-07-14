#include "share_map_store/share_map_store.h"

#include <glog/logging.h>

namespace embtable {

ShareMapStore::ShareMapStore(DeploymentConfig config)
    : config_(std::move(config)) {}

ShareMapStore::~ShareMapStore() = default;

Status ShareMapStore::Init() {
    if (initialized_) return Status::OK();
    realClient_ = std::make_shared<mooncake::RealClient>();
    int ret = realClient_->setup_real(
        /*local_hostname=*/"", config_.metadataServer,
        /*global_segment_size=*/1024 * 1024 * 16,
        /*local_buffer_size=*/1024 * 1024 * 16,
        config_.protocol, config_.deviceNames,
        config_.masterAddress);
    if (ret != 0) {
        return Status::Error(ErrorCode::kInternal,
                             "RealClient setup_real failed: " + config_.masterAddress);
    }
    initialized_ = true;
    return Status::OK();
}

Status ShareMapStore::getOrCreateShareMap(const std::string& bucketKey,
                                          uint64_t valueSize,
                                          std::shared_ptr<ShareMap>& out) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = shareMaps_.find(bucketKey);
    if (it != shareMaps_.end()) {
        out = it->second;
        return Status::OK();
    }
    if (valueSize == 0) {
        return Status::Error(ErrorCode::kNotFound,
                             "ShareMap not found and valueSize==0: " + bucketKey);
    }
    auto sm = std::make_shared<ShareMap>(bucketKey, valueSize, realClient_,
                                         config_.shareObjectSize);
    shareMaps_.emplace(bucketKey, sm);
    out = sm;
    return Status::OK();
}

Status ShareMapStore::Publish(const std::string& bucketKey, uint64_t valueSize,
                              const std::vector<uint64_t>& keys,
                              const std::vector<StringView>& values) {
    if (!initialized_) {
        return Status::Error(ErrorCode::kInternal, "ShareMapStore not initialized");
    }
    std::shared_ptr<ShareMap> sm;
    auto s = getOrCreateShareMap(bucketKey, valueSize, sm);
    if (!s.IsOk()) return s;
    return sm->Insert(keys, values);
}

Status ShareMapStore::QueryData(const std::string& bucketKey,
                                const std::vector<uint64_t>& keys,
                                std::vector<StringView>& buffers) {
    if (!initialized_) {
        return Status::Error(ErrorCode::kInternal, "ShareMapStore not initialized");
    }
    std::shared_ptr<ShareMap> sm;
    auto s = getOrCreateShareMap(bucketKey, 0, sm);
    if (!s.IsOk()) return s;
    return sm->Lookup(keys, buffers);
}

Status ShareMapStore::BuildIndex(const std::string& bucketKey) {
    if (!initialized_) {
        return Status::Error(ErrorCode::kInternal, "ShareMapStore not initialized");
    }
    std::shared_ptr<ShareMap> sm;
    auto s = getOrCreateShareMap(bucketKey, 0, sm);
    if (!s.IsOk()) return s;
    return sm->BuildIndex();
}

Status ShareMapStore::Import(const std::string& bucketKey) {
    if (!initialized_) {
        return Status::Error(ErrorCode::kInternal, "ShareMapStore not initialized");
    }
    std::shared_ptr<ShareMap> sm;
    auto s = getOrCreateShareMap(bucketKey, 1, sm);
    if (!s.IsOk()) return s;
    return sm->Import();
}

std::shared_ptr<ShareMap> ShareMapStore::GetShareMap(
    const std::string& bucketKey) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = shareMaps_.find(bucketKey);
    if (it == shareMaps_.end()) return nullptr;
    return it->second;
}

}  // namespace embtable
