#include "share_map_store/share_map_store.h"

#include <atomic>
#include <cstring>
#include <span>
#include <string>

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

Status ShareMapStore::QueryDataToStore(const std::string& bucketKey,
                                       const std::vector<uint64_t>& keys,
                                       uint64_t valueSize,
                                       std::string& resultObjectKey,
                                       uint64_t& resultObjectSize,
                                       std::vector<int8_t>& foundFlags) {
    if (!initialized_) {
        return Status::Error(ErrorCode::kInternal,
                             "ShareMapStore not initialized");
    }
    if (keys.empty()) {
        resultObjectSize = 0;
        return Status::OK();
    }
    // 1. Local query
    std::vector<StringView> buffers;
    auto s = QueryData(bucketKey, keys, buffers);
    // If local ShareMap is missing, try to Import it from Mooncake Store.
    if (!s.IsOk()) {
        if (s.code() == static_cast<int>(ErrorCode::kNotFound)) {
            s = Import(bucketKey);
            if (s.IsOk()) s = QueryData(bucketKey, keys, buffers);
        }
        if (!s.IsOk()) return s;
    }

    // 2. Pack results: [1-byte found flag][valueSize bytes data]
    uint64_t entrySize = 1 + valueSize;
    resultObjectSize = keys.size() * entrySize;
    std::string packed;
    packed.resize(resultObjectSize);
    foundFlags.resize(keys.size(), 0);

    for (size_t i = 0; i < keys.size(); ++i) {
        char* dest = &packed[i * entrySize];
        if (i < buffers.size() && buffers[i].data() != nullptr &&
            buffers[i].size() >= valueSize) {
            foundFlags[i] = 1;
            dest[0] = static_cast<char>(1);
            std::memcpy(dest + 1, buffers[i].data(), valueSize);
        } else {
            dest[0] = static_cast<char>(0);
            std::memset(dest + 1, 0, valueSize);
        }
    }

    // 3. Write packed buffer to a temporary Mooncake Store object (TE write).
    static std::atomic<uint64_t> counter{0};
    resultObjectKey = "embtable_qresult_" + bucketKey + "_" +
                      std::to_string(counter.fetch_add(1));
    mooncake::ReplicateConfig config;
    int ret = realClient_->put(
        resultObjectKey,
        std::span<const char>(packed.data(), packed.size()), config);
    if (ret != 0) {
        return Status::Error(ErrorCode::kIOError,
                             "put temp result object failed: " + resultObjectKey);
    }
    return Status::OK();
}

Status ShareMapStore::BatchQueryDataToStore(
    const std::vector<std::string>& bucketKeys,
    const std::vector<std::vector<uint64_t>>& keysPerBucket,
    uint64_t valueSize,
    std::vector<std::string>& resultObjectKeys,
    std::vector<uint64_t>& resultObjectSizes,
    std::vector<std::vector<int8_t>>& foundFlagsPerBucket) {
    if (!initialized_) {
        return Status::Error(ErrorCode::kInternal,
                             "ShareMapStore not initialized");
    }
    resultObjectKeys.clear();
    resultObjectSizes.clear();
    foundFlagsPerBucket.clear();
    if (bucketKeys.empty()) return Status::OK();

    const size_t bucketCount = bucketKeys.size();
    const uint64_t entrySize = 1 + valueSize;

    // Phase 1: Query each bucket locally and collect results.
    std::vector<std::vector<StringView>> allBuffers(bucketCount);
    for (size_t i = 0; i < bucketCount; ++i) {
        const auto& bucketKey = bucketKeys[i];
        const auto& keys = keysPerBucket[i];
        if (keys.empty()) {
            foundFlagsPerBucket.emplace_back();
            continue;
        }
        std::vector<StringView> buffers;
        auto s = QueryData(bucketKey, keys, buffers);
        if (!s.IsOk()) {
            if (s.code() == static_cast<int>(ErrorCode::kNotFound)) {
                s = Import(bucketKey);
                if (s.IsOk()) s = QueryData(bucketKey, keys, buffers);
            }
            if (!s.IsOk()) {
                LOG(WARNING) << "BatchQueryDataToStore: query failed for "
                             << bucketKey << ": " << s.msg();
            }
        }
        allBuffers[i] = std::move(buffers);
    }

    // Phase 2: Pack all results into ONE aggregated buffer.
    // Layout:
    //   [bucketCount(8B)]
    //   for each bucket:
    //     [bucketKeyLen(4B)][bucketKey bytes][keyCount(8B)]
    //     for each key: [1B found flag][valueSize bytes data]
    std::string packed;
    // Reserve header.
    packed.resize(sizeof(uint64_t));  // bucketCount placeholder
    auto* header = reinterpret_cast<uint64_t*>(&packed[0]);
    *header = bucketCount;

    foundFlagsPerBucket.resize(bucketCount);
    for (size_t i = 0; i < bucketCount; ++i) {
        const auto& bucketKey = bucketKeys[i];
        const auto& keys = keysPerBucket[i];
        const auto& buffers = allBuffers[i];
        const uint64_t keyCount = keys.size();

        // bucketKeyLen (4B) + bucketKey bytes + keyCount (8B)
        uint32_t keyLen = static_cast<uint32_t>(bucketKey.size());
        size_t curSize = packed.size();
        packed.resize(curSize + sizeof(uint32_t) + keyLen + sizeof(uint64_t));
        std::memcpy(&packed[curSize], &keyLen, sizeof(uint32_t));
        curSize += sizeof(uint32_t);
        std::memcpy(&packed[curSize], bucketKey.data(), keyLen);
        curSize += keyLen;
        std::memcpy(&packed[curSize], &keyCount, sizeof(uint64_t));

        // per-key entries
        foundFlagsPerBucket[i].assign(keyCount, 0);
        if (keyCount == 0) continue;
        size_t entriesStart = packed.size();
        packed.resize(entriesStart + keyCount * entrySize);
        for (uint64_t k = 0; k < keyCount; ++k) {
            char* dest = &packed[entriesStart + k * entrySize];
            if (k < buffers.size() && buffers[k].data() != nullptr &&
                buffers[k].size() >= valueSize) {
                foundFlagsPerBucket[i][k] = 1;
                dest[0] = static_cast<char>(1);
                std::memcpy(dest + 1, buffers[k].data(), valueSize);
            } else {
                dest[0] = static_cast<char>(0);
                std::memset(dest + 1, 0, valueSize);
            }
        }
    }

    // Phase 3: Single TE write for the entire aggregated buffer.
    static std::atomic<uint64_t> batchCounter{0};
    std::string aggregatedKey = "embtable_batch_qresult_" +
                                std::to_string(batchCounter.fetch_add(1));
    mooncake::ReplicateConfig config;
    int ret = realClient_->put(
        aggregatedKey,
        std::span<const char>(packed.data(), packed.size()), config);
    if (ret != 0) {
        return Status::Error(
            ErrorCode::kIOError,
            "put aggregated batch result object failed: " + aggregatedKey);
    }
    // All buckets share the same aggregated object key; the client parses
    // segments from the single buffer. We still record per-bucket sizes
    // (computed from keyCount) so callers can validate.
    resultObjectKeys.assign(bucketCount, aggregatedKey);
    resultObjectSizes.resize(bucketCount);
    for (size_t i = 0; i < bucketCount; ++i) {
        resultObjectSizes[i] = keysPerBucket[i].size() * entrySize;
    }
    return Status::OK();
}

}  // namespace embtable
