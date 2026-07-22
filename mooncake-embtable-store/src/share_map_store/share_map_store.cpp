#include "share_map_store/share_map_store.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <new>
#include <string>

#include <glog/logging.h>

#include "embtable_perf.h"

namespace embtable {

ShareMapStore::ShareMapStore(DeploymentConfig config, std::string localHostname)
    : config_(std::move(config)), localHostname_(std::move(localHostname)) {}

ShareMapStore::~ShareMapStore() {
    for (auto& slot : transferBuffers_) {
        if (slot.registered && slot.allocator && realClient_) {
            realClient_->unregister_buffer(slot.allocator->getBase());
        }
    }
}

Status ShareMapStore::Init() {
    if (initialized_) return Status::OK();
    realClient_ = std::make_shared<mooncake::RealClient>();
    int ret = realClient_->setup_real(
        localHostname_, config_.metadataServer, config_.globalSegmentSize,
        config_.localBufferSize, config_.protocol, config_.deviceNames,
        config_.masterAddress);
    if (ret != 0) {
        return Status::Error(
            ErrorCode::kInternal,
            "RealClient setup_real failed: " + config_.masterAddress);
    }
    if (config_.transferBufferSize == 0) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "transferBufferSize must be > 0");
    }
    const uint32_t bufferCount =
        std::max<uint32_t>(1, config_.transferBufferCount);
    transferBuffers_.reserve(bufferCount);
    for (uint32_t i = 0; i < bufferCount; ++i) {
        TransferBufferSlot slot;
        try {
            slot.allocator = mooncake::ClientBufferAllocator::create(
                config_.transferBufferSize, config_.protocol);
        } catch (const std::bad_alloc&) {
            for (auto& registeredSlot : transferBuffers_) {
                if (registeredSlot.registered && registeredSlot.allocator) {
                    realClient_->unregister_buffer(
                        registeredSlot.allocator->getBase());
                }
            }
            transferBuffers_.clear();
            return Status::Error(
                ErrorCode::kInternal,
                "failed to allocate ShareMapStore transfer buffer");
        }
        if (!slot.allocator || !slot.allocator->getBase() ||
            realClient_->register_buffer(slot.allocator->getBase(),
                                         config_.transferBufferSize) != 0) {
            for (auto& registeredSlot : transferBuffers_) {
                if (registeredSlot.registered && registeredSlot.allocator) {
                    realClient_->unregister_buffer(
                        registeredSlot.allocator->getBase());
                }
            }
            transferBuffers_.clear();
            return Status::Error(
                ErrorCode::kIOError,
                "failed to register ShareMapStore transfer buffer");
        }
        slot.registered = true;
        transferBuffers_.push_back(std::move(slot));
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
        return Status::Error(
            ErrorCode::kNotFound,
            "ShareMap not found and valueSize==0: " + bucketKey);
    }
    auto sm = std::make_shared<ShareMap>(bucketKey, valueSize, realClient_,
                                         config_.shareObjectSize,
                                         config_.phfLookupConcurrency);
    shareMaps_.emplace(bucketKey, sm);
    out = sm;
    return Status::OK();
}

Status ShareMapStore::Publish(const std::string& bucketKey, uint64_t valueSize,
                              const std::vector<uint64_t>& keys,
                              const std::vector<StringView>& values) {
    if (!initialized_) {
        return Status::Error(ErrorCode::kInternal,
                             "ShareMapStore not initialized");
    }
    std::shared_ptr<ShareMap> sm;
    auto s = getOrCreateShareMap(bucketKey, valueSize, sm);
    if (!s.IsOk()) return s;
    return sm->Insert(keys, values);
}

Status ShareMapStore::QueryData(const std::string& bucketKey,
                                const std::vector<uint64_t>& keys,
                                std::vector<StringView>& buffers) {
    UbDiag::PerfPoint totalPoint(PerfKey::EMB_RD_STORE_QUERY_TOTAL,
                                 UbDiag::PerfLevel::KEY_MODULE);
    totalPoint.Start();
    if (!initialized_) {
        auto status = Status::Error(ErrorCode::kInternal,
                                    "ShareMapStore not initialized");
        totalPoint.End(status.code());
        return status;
    }
    std::shared_ptr<ShareMap> sm;
    UbDiag::PerfPoint getMapPoint(PerfKey::EMB_RD_STORE_GET_SHARE_MAP,
                                  UbDiag::PerfLevel::MODULE);
    getMapPoint.Start();
    auto s = getOrCreateShareMap(bucketKey, 0, sm);
    getMapPoint.End(s.IsOk() ? 0 : s.code());
    if (!s.IsOk()) {
        totalPoint.End(s.code());
        return s;
    }
    s = sm->Lookup(keys, buffers);
    totalPoint.End(s.IsOk() ? 0 : s.code());
    return s;
}

Status ShareMapStore::BuildIndex(const std::string& bucketKey) {
    if (!initialized_) {
        return Status::Error(ErrorCode::kInternal,
                             "ShareMapStore not initialized");
    }
    std::shared_ptr<ShareMap> sm;
    auto s = getOrCreateShareMap(bucketKey, 0, sm);
    if (!s.IsOk()) return s;
    return sm->BuildIndex();
}

Status ShareMapStore::Import(const std::string& bucketKey) {
    if (!initialized_) {
        return Status::Error(ErrorCode::kInternal,
                             "ShareMapStore not initialized");
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

Status ShareMapStore::QueryDataToBuffer(
    const std::string& bucketKey, const std::vector<uint64_t>& keys,
    uint64_t valueSize, const std::string& targetEndpoint,
    uint64_t targetAddress, uint64_t targetCapacity, uint64_t& transferredSize,
    std::vector<int8_t>& foundFlags) {
    UbDiag::PerfPoint totalPoint(PerfKey::EMB_RD_SERVER_QUERY_TOTAL,
                                 UbDiag::PerfLevel::KEY_MODULE);
    totalPoint.Start();
    auto finish = [&totalPoint](Status status) {
        totalPoint.End(status.IsOk() ? 0 : status.code());
        return status;
    };
    if (!initialized_) {
        return finish(Status::Error(ErrorCode::kInternal,
                                    "ShareMapStore not initialized"));
    }
    transferredSize = 0;
    if (keys.empty()) {
        return finish(Status::OK());
    }
    if (targetEndpoint.empty() || targetAddress == 0) {
        return finish(Status::Error(ErrorCode::kInvalidArgument,
                                    "invalid target transfer buffer"));
    }
    // 1. Local query
    std::vector<StringView> buffers;
    UbDiag::PerfPoint lookupPoint(PerfKey::EMB_RD_SERVER_LOCAL_LOOKUP,
                                  UbDiag::PerfLevel::KEY_MODULE);
    lookupPoint.Start();
    auto s = QueryData(bucketKey, keys, buffers);
    lookupPoint.End(s.IsOk() ? 0 : s.code());
    // If local ShareMap is missing, try to Import it from Mooncake Store.
    if (!s.IsOk()) {
        if (s.code() == static_cast<int>(ErrorCode::kNotFound)) {
            UbDiag::PerfPoint importPoint(PerfKey::EMB_RD_SERVER_IMPORT,
                                          UbDiag::PerfLevel::KEY_MODULE);
            importPoint.Start();
            s = Import(bucketKey);
            importPoint.End(s.IsOk() ? 0 : s.code());
            if (s.IsOk()) {
                UbDiag::PerfPoint retryPoint(
                    PerfKey::EMB_RD_SERVER_LOCAL_LOOKUP,
                    UbDiag::PerfLevel::KEY_MODULE);
                retryPoint.Start();
                s = QueryData(bucketKey, keys, buffers);
                retryPoint.End(s.IsOk() ? 0 : s.code());
            }
        }
        if (!s.IsOk()) return finish(s);
    }

    // 2. Pack results: [1-byte found flag][valueSize bytes data]
    UbDiag::PerfPoint packPoint(PerfKey::EMB_RD_SERVER_PACK,
                                UbDiag::PerfLevel::MODULE);
    packPoint.Start();
    if (valueSize == 0) {
        auto status = Status::Error(ErrorCode::kInvalidArgument,
                                    "valueSize must be > 0");
        packPoint.End(status.code());
        return finish(status);
    }
    uint64_t entrySize = 0;
    if (!CheckedAdd(1, valueSize, entrySize)) {
        auto status = Status::Error(ErrorCode::kOutOfRange,
                                    "query entry size overflows");
        packPoint.End(status.code());
        return finish(status);
    }
    if (!CheckedMultiply(keys.size(), entrySize, transferredSize)) {
        auto status = Status::Error(ErrorCode::kOutOfRange,
                                    "query result size overflows");
        packPoint.End(status.code());
        return finish(status);
    }
    if (transferredSize > targetCapacity || transferBuffers_.empty() ||
        transferredSize > config_.transferBufferSize) {
        auto status = Status::Error(
            ErrorCode::kOutOfRange,
            "query result exceeds registered transfer buffer capacity");
        packPoint.End(status.code());
        return finish(status);
    }

    UbDiag::PerfPoint bufferPoint(PerfKey::EMB_RD_SERVER_BUFFER_WAIT,
                                  UbDiag::PerfLevel::MODULE);
    bufferPoint.Start();
    const size_t bufferIndex = AcquireTransferBuffer();
    bufferPoint.End(bufferIndex == std::numeric_limits<size_t>::max()
                        ? static_cast<int>(ErrorCode::kInternal)
                        : 0);
    if (bufferIndex == std::numeric_limits<size_t>::max()) {
        auto status = Status::Error(
            ErrorCode::kInternal,
            "no ShareMapStore transfer buffer available");
        packPoint.End(status.code());
        return finish(status);
    }
    auto& transferAllocator = transferBuffers_[bufferIndex].allocator;
    auto* transferBuffer = static_cast<char*>(transferAllocator->getBase());
    foundFlags.resize(keys.size(), 0);

    for (size_t i = 0; i < keys.size(); ++i) {
        char* dest = transferBuffer + i * entrySize;
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
    packPoint.End(0);

    // 3. Data plane: write directly into the caller's registered buffer.
    UbDiag::PerfPoint transferPoint(PerfKey::EMB_RD_SERVER_TE_WRITE,
                                    UbDiag::PerfLevel::KEY_MODULE);
    transferPoint.Start();
    int ret = realClient_->subTransferTask(transferBuffer, transferredSize,
                                           targetEndpoint, targetAddress);
    transferPoint.End(ret);
    ReleaseTransferBuffer(bufferIndex);
    if (ret != 0) {
        return finish(Status::Error(
            ErrorCode::kIOError,
            "Transfer Engine query result write failed: ret=" +
                std::to_string(ret) + ", target=" + targetEndpoint +
                ", size=" + std::to_string(transferredSize)));
    }
    return finish(Status::OK());
}

Status ShareMapStore::BatchQueryDataToBuffer(
    const std::vector<std::string>& bucketKeys,
    const std::vector<std::vector<uint64_t>>& keysPerBucket, uint64_t valueSize,
    const std::string& targetEndpoint, uint64_t targetAddress,
    uint64_t targetCapacity, uint64_t& transferredSize,
    std::vector<std::vector<int8_t>>& foundFlagsPerBucket) {
    UbDiag::PerfPoint totalPoint(PerfKey::EMB_RD_SERVER_BATCH_TOTAL,
                                 UbDiag::PerfLevel::KEY_MODULE);
    totalPoint.Start();
    auto finish = [&totalPoint](Status status) {
        totalPoint.End(status.IsOk() ? 0 : status.code());
        return status;
    };
    if (!initialized_) {
        return finish(Status::Error(ErrorCode::kInternal,
                                    "ShareMapStore not initialized"));
    }
    transferredSize = 0;
    foundFlagsPerBucket.clear();
    if (bucketKeys.empty()) return finish(Status::OK());
    if (bucketKeys.size() != keysPerBucket.size()) {
        return finish(Status::Error(
            ErrorCode::kInvalidArgument,
            "bucketKeys/keysPerBucket size mismatch"));
    }
    if (targetEndpoint.empty() || targetAddress == 0) {
        return finish(Status::Error(ErrorCode::kInvalidArgument,
                                    "invalid target transfer buffer"));
    }
    if (valueSize == 0) {
        return finish(Status::Error(ErrorCode::kInvalidArgument,
                                    "valueSize must be > 0"));
    }
    uint64_t entrySize = 0;
    if (!CheckedAdd(1, valueSize, entrySize)) {
        return finish(Status::Error(ErrorCode::kOutOfRange,
                                    "batch query entry size overflows"));
    }

    const size_t bucketCount = bucketKeys.size();
    // Phase 1: Query each bucket locally and collect results.
    std::vector<std::vector<StringView>> allBuffers(bucketCount);
    UbDiag::PerfPoint lookupPoint(PerfKey::EMB_RD_SERVER_LOCAL_LOOKUP,
                                  UbDiag::PerfLevel::KEY_MODULE);
    lookupPoint.Start();
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
                UbDiag::PerfPoint importPoint(PerfKey::EMB_RD_SERVER_IMPORT,
                                              UbDiag::PerfLevel::KEY_MODULE);
                importPoint.Start();
                s = Import(bucketKey);
                importPoint.End(s.IsOk() ? 0 : s.code());
                if (s.IsOk()) s = QueryData(bucketKey, keys, buffers);
            }
            if (!s.IsOk()) {
                lookupPoint.End(s.code());
                return finish(s);
            }
        }
        allBuffers[i] = std::move(buffers);
    }
    lookupPoint.End(0);

    // Phase 2: Pack all results into ONE aggregated buffer.
    // Layout:
    //   [bucketCount(8B)]
    //   for each bucket:
    //     [bucketKeyLen(4B)][bucketKey bytes][keyCount(8B)]
    //     for each key: [1B found flag][valueSize bytes data]
    UbDiag::PerfPoint packPoint(PerfKey::EMB_RD_SERVER_PACK,
                                UbDiag::PerfLevel::MODULE);
    packPoint.Start();
    uint64_t requiredSize = sizeof(uint64_t);
    for (size_t i = 0; i < bucketCount; ++i) {
        if (bucketKeys[i].size() > std::numeric_limits<uint32_t>::max()) {
            auto status = Status::Error(ErrorCode::kOutOfRange,
                                        "bucket key is too large");
            packPoint.End(status.code());
            return finish(status);
        }
        uint64_t keyBytes = 0;
        uint64_t entriesBytes = 0;
        if (!CheckedAdd(sizeof(uint32_t), bucketKeys[i].size(), keyBytes) ||
            !CheckedAdd(keyBytes, sizeof(uint64_t), keyBytes) ||
            !CheckedMultiply(keysPerBucket[i].size(), entrySize,
                             entriesBytes) ||
            !CheckedAdd(keyBytes, entriesBytes, keyBytes) ||
            !CheckedAdd(requiredSize, keyBytes, requiredSize)) {
            auto status = Status::Error(
                ErrorCode::kOutOfRange,
                "batch query result size overflows");
            packPoint.End(status.code());
            return finish(status);
        }
    }
    if (requiredSize > targetCapacity || transferBuffers_.empty() ||
        requiredSize > config_.transferBufferSize) {
        auto status = Status::Error(
            ErrorCode::kOutOfRange,
            "batch query result exceeds registered transfer buffer capacity");
        packPoint.End(status.code());
        return finish(status);
    }

    UbDiag::PerfPoint bufferPoint(PerfKey::EMB_RD_SERVER_BUFFER_WAIT,
                                  UbDiag::PerfLevel::MODULE);
    bufferPoint.Start();
    const size_t bufferIndex = AcquireTransferBuffer();
    bufferPoint.End(bufferIndex == std::numeric_limits<size_t>::max()
                        ? static_cast<int>(ErrorCode::kInternal)
                        : 0);
    if (bufferIndex == std::numeric_limits<size_t>::max()) {
        auto status = Status::Error(
            ErrorCode::kInternal,
            "no ShareMapStore transfer buffer available");
        packPoint.End(status.code());
        return finish(status);
    }
    auto& transferAllocator = transferBuffers_[bufferIndex].allocator;
    auto* transferBuffer = static_cast<char*>(transferAllocator->getBase());
    char* writePtr = transferBuffer;
    uint64_t bucketCountValue = bucketCount;
    std::memcpy(writePtr, &bucketCountValue, sizeof(uint64_t));
    writePtr += sizeof(uint64_t);

    foundFlagsPerBucket.resize(bucketCount);
    for (size_t i = 0; i < bucketCount; ++i) {
        const auto& bucketKey = bucketKeys[i];
        const auto& keys = keysPerBucket[i];
        const auto& buffers = allBuffers[i];
        const uint64_t keyCount = keys.size();

        // bucketKeyLen (4B) + bucketKey bytes + keyCount (8B)
        uint32_t keyLen = static_cast<uint32_t>(bucketKey.size());
        std::memcpy(writePtr, &keyLen, sizeof(uint32_t));
        writePtr += sizeof(uint32_t);
        std::memcpy(writePtr, bucketKey.data(), keyLen);
        writePtr += keyLen;
        std::memcpy(writePtr, &keyCount, sizeof(uint64_t));
        writePtr += sizeof(uint64_t);

        // per-key entries
        foundFlagsPerBucket[i].assign(keyCount, 0);
        if (keyCount == 0) continue;
        for (uint64_t k = 0; k < keyCount; ++k) {
            char* dest = writePtr;
            if (k < buffers.size() && buffers[k].data() != nullptr &&
                buffers[k].size() >= valueSize) {
                foundFlagsPerBucket[i][k] = 1;
                dest[0] = static_cast<char>(1);
                std::memcpy(dest + 1, buffers[k].data(), valueSize);
            } else {
                dest[0] = static_cast<char>(0);
                std::memset(dest + 1, 0, valueSize);
            }
            writePtr += entrySize;
        }
    }
    packPoint.End(0);

    // Phase 3: Single direct TE write for the entire aggregated buffer.
    transferredSize = requiredSize;
    UbDiag::PerfPoint transferPoint(PerfKey::EMB_RD_SERVER_TE_WRITE,
                                    UbDiag::PerfLevel::KEY_MODULE);
    transferPoint.Start();
    int ret = realClient_->subTransferTask(transferBuffer, transferredSize,
                                           targetEndpoint, targetAddress);
    transferPoint.End(ret);
    ReleaseTransferBuffer(bufferIndex);
    if (ret != 0) {
        return finish(Status::Error(
            ErrorCode::kIOError,
            "Transfer Engine batch query write failed: ret=" +
                std::to_string(ret) + ", target=" + targetEndpoint +
                ", size=" + std::to_string(transferredSize)));
    }
    return finish(Status::OK());
}

size_t ShareMapStore::AcquireTransferBuffer() {
    std::unique_lock<std::mutex> lock(transferBufferMutex_);
    if (transferBuffers_.empty()) {
        return std::numeric_limits<size_t>::max();
    }
    transferBufferCv_.wait(lock, [this]() {
        for (const auto& slot : transferBuffers_) {
            if (!slot.inUse) return true;
        }
        return false;
    });
    for (size_t i = 0; i < transferBuffers_.size(); ++i) {
        if (!transferBuffers_[i].inUse) {
            transferBuffers_[i].inUse = true;
            return i;
        }
    }
    return std::numeric_limits<size_t>::max();
}

void ShareMapStore::ReleaseTransferBuffer(size_t index) {
    {
        std::lock_guard<std::mutex> lock(transferBufferMutex_);
        if (index < transferBuffers_.size()) {
            transferBuffers_[index].inUse = false;
        }
    }
    transferBufferCv_.notify_one();
}

}  // namespace embtable
