#include "share_map_store/share_map_store_client.h"

#include <cstring>
#include <limits>
#include <new>
#include <unordered_map>

#include <async_simple/coro/SyncAwait.h>
#include <glog/logging.h>

namespace embtable {

namespace {

Status FromRemoteStatus(int32_t statusCode, const std::string& operation,
                        const std::string& message) {
    if (statusCode <= static_cast<int32_t>(ErrorCode::kOk) ||
        statusCode > static_cast<int32_t>(ErrorCode::kNotSupported)) {
        return Status::Error(ErrorCode::kInternal,
                             operation + " returned invalid status " +
                                 std::to_string(statusCode) + ": " + message);
    }
    return Status::Error(static_cast<ErrorCode>(statusCode),
                         operation + " failed: " + message);
}

}  // namespace

ShareMapStoreClient::~ShareMapStoreClient() {
    if (transferBufferRegistered_ && transferAllocator_ && client_) {
        client_->unregister_buffer(transferAllocator_->getBase());
    }
    transferAllocator_.reset();
}

Status ShareMapStoreClient::Init(uint64_t transferBufferSize) {
    if (!client_ || transferBufferSize == 0) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "invalid ShareMapStoreClient transfer buffer");
    }
    try {
        transferAllocator_ = mooncake::ClientBufferAllocator::create(
            transferBufferSize, client_->protocol);
    } catch (const std::bad_alloc&) {
        return Status::Error(
            ErrorCode::kInternal,
            "failed to allocate ShareMapStoreClient transfer buffer");
    }
    if (!transferAllocator_ || !transferAllocator_->getBase()) {
        transferAllocator_.reset();
        return Status::Error(
            ErrorCode::kInternal,
            "failed to allocate ShareMapStoreClient transfer buffer");
    }
    if (client_->register_buffer(transferAllocator_->getBase(),
                                 transferBufferSize) != 0) {
        transferAllocator_.reset();
        return Status::Error(
            ErrorCode::kIOError,
            "failed to register ShareMapStoreClient transfer buffer");
    }
    transferSegmentEndpoint_ = client_->get_segment_endpoint();
    if (transferSegmentEndpoint_.empty()) {
        client_->unregister_buffer(transferAllocator_->getBase());
        transferAllocator_.reset();
        return Status::Error(
            ErrorCode::kInternal,
            "ShareMapStoreClient transfer segment endpoint is empty");
    }
    transferBufferSize_ = transferBufferSize;
    transferBufferRegistered_ = true;
    LOG(INFO) << "ShareMapStoreClient transfer buffer registered: segment="
              << transferSegmentEndpoint_ << ", address="
              << reinterpret_cast<uint64_t>(transferAllocator_->getBase())
              << ", size=" << transferBufferSize_;
    return Status::OK();
}

std::shared_ptr<mooncake::BufferHandle>
ShareMapStoreClient::AllocateTransferBuffer(uint64_t size) {
    if (!transferAllocator_ || size == 0 || size > transferBufferSize_) {
        return nullptr;
    }
    auto allocation = transferAllocator_->allocate(size);
    if (!allocation.has_value()) {
        return nullptr;
    }
    return std::make_shared<mooncake::BufferHandle>(
        std::move(allocation.value()));
}

ShareMapStoreClient::RpcClientLease::~RpcClientLease() {
    if (owner_ && slot_) owner_->ReleaseClient(slot_);
}

void ShareMapStoreClient::ReleaseClient(
    const std::shared_ptr<RpcClientSlot>& slot) {
    std::lock_guard<std::mutex> lock(cacheMutex_);
    if (slot) slot->inUse = false;
}

std::optional<ShareMapStoreClient::RpcClientLease>
ShareMapStoreClient::AcquireClient(const std::string& rpcEndpoint) {
    std::lock_guard<std::mutex> lock(cacheMutex_);
    auto& slots = clientCache_[rpcEndpoint];
    for (auto& slot : slots) {
        if (!slot->inUse) {
            slot->inUse = true;
            return RpcClientLease(this, slot);
        }
    }

    auto slot = std::make_shared<RpcClientSlot>();
    slot->client = std::make_unique<coro_rpc::coro_rpc_client>();
    slot->inUse = true;
    // coro_rpc::err_code: operator bool() returns true on error.
    auto ec = async_simple::coro::syncAwait(slot->client->connect(rpcEndpoint));
    if (ec) {
        LOG(ERROR) << "Failed to connect to ShareMapStore RPC endpoint: "
                   << rpcEndpoint << ", error: " << ec.message();
        return std::nullopt;
    }
    slots.push_back(slot);
    return RpcClientLease(this, std::move(slot));
}

Status ShareMapStoreClient::ParseResultBuffer(
    void* data, uint64_t dataSize, uint64_t valueSize, size_t numKeys,
    std::vector<StringView>& buffers,
    std::shared_ptr<mooncake::BufferHandle> handle) {
    (void)handle;
    if (!data || valueSize == 0) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "invalid single query response metadata");
    }
    uint64_t entrySize = 0;
    uint64_t expectedSize = 0;
    if (!CheckedAdd(1, valueSize, entrySize) ||
        !CheckedMultiply(numKeys, entrySize, expectedSize) ||
        dataSize != expectedSize) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "single query response size mismatch");
    }
    buffers.assign(numKeys, {});
    const char* ptr = static_cast<const char*>(data);
    for (size_t i = 0; i < numKeys; ++i) {
        uint64_t offset = 0;
        if (!CheckedMultiply(i, entrySize, offset) ||
            !IsRangeValid(offset, entrySize, dataSize)) {
            return Status::Error(ErrorCode::kInvalidArgument,
                                 "single query entry is out of range");
        }
        const auto found = static_cast<uint8_t>(ptr[offset]);
        if (found > 1) {
            return Status::Error(ErrorCode::kInvalidArgument,
                                 "invalid found flag in transfer buffer");
        }
        if (found != 0) {
            buffers[i] = StringView(ptr + offset + 1, valueSize);
        }
    }
    return Status::OK();
}

Status ShareMapStoreClient::ParseAggregatedBuffer(
    void* data, uint64_t dataSize, uint64_t valueSize,
    const std::vector<std::string>& bucketKeys,
    const std::vector<std::vector<uint64_t>>& keysPerBucket,
    std::vector<std::vector<StringView>>& buffersPerBucket,
    std::shared_ptr<mooncake::BufferHandle> handle) {
    (void)handle;
    if (!data || valueSize == 0 || bucketKeys.size() != keysPerBucket.size() ||
        dataSize < sizeof(uint64_t)) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "invalid batch query response metadata");
    }
    std::unordered_map<std::string, size_t> keyToIdx;
    for (size_t i = 0; i < bucketKeys.size(); ++i) {
        if (!keyToIdx.emplace(bucketKeys[i], i).second) {
            return Status::Error(ErrorCode::kInvalidArgument,
                                 "duplicate requested bucket key");
        }
    }
    uint64_t entrySize = 0;
    if (!CheckedAdd(1, valueSize, entrySize)) {
        return Status::Error(ErrorCode::kOutOfRange,
                             "batch entry size overflows");
    }
    buffersPerBucket.assign(bucketKeys.size(), {});
    const char* ptr = static_cast<const char*>(data);
    uint64_t bucketCount = 0;
    std::memcpy(&bucketCount, ptr, sizeof(bucketCount));
    ptr += sizeof(bucketCount);
    uint64_t remaining = dataSize - sizeof(bucketCount);
    if (bucketCount != bucketKeys.size()) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "batch bucket count mismatch");
    }
    std::vector<bool> seen(bucketKeys.size(), false);
    for (uint64_t b = 0; b < bucketCount; ++b) {
        if (remaining < sizeof(uint32_t)) {
            return Status::Error(ErrorCode::kInvalidArgument,
                                 "truncated batch bucket key length");
        }
        uint32_t keyLen = 0;
        std::memcpy(&keyLen, ptr, sizeof(keyLen));
        ptr += sizeof(keyLen);
        remaining -= sizeof(keyLen);
        if (remaining < keyLen + sizeof(uint64_t)) {
            return Status::Error(ErrorCode::kInvalidArgument,
                                 "truncated batch bucket header");
        }
        std::string key(ptr, keyLen);
        ptr += keyLen;
        remaining -= keyLen;
        uint64_t keyCount = 0;
        std::memcpy(&keyCount, ptr, sizeof(keyCount));
        ptr += sizeof(keyCount);
        remaining -= sizeof(keyCount);
        auto it = keyToIdx.find(key);
        if (it == keyToIdx.end() || seen[it->second]) {
            return Status::Error(ErrorCode::kInvalidArgument,
                                 "unknown or duplicate batch bucket");
        }
        const size_t idx = it->second;
        seen[idx] = true;
        if (keyCount != keysPerBucket[idx].size()) {
            return Status::Error(
                ErrorCode::kInvalidArgument,
                "batch key count mismatch: bucket=" + key +
                    ", expected=" + std::to_string(keysPerBucket[idx].size()) +
                    ", actual=" + std::to_string(keyCount) + ", buffer=" +
                    std::to_string(reinterpret_cast<uint64_t>(data)));
        }
        uint64_t payloadSize = 0;
        if (!CheckedMultiply(keyCount, entrySize, payloadSize) ||
            payloadSize > remaining) {
            return Status::Error(ErrorCode::kInvalidArgument,
                                 "truncated batch bucket payload");
        }
        buffersPerBucket[idx].assign(keyCount, {});
        for (uint64_t k = 0; k < keyCount; ++k) {
            const auto found = static_cast<uint8_t>(ptr[0]);
            if (found > 1) {
                return Status::Error(ErrorCode::kInvalidArgument,
                                     "invalid found flag in batch transfer "
                                     "buffer");
            }
            if (found != 0)
                buffersPerBucket[idx][k] = StringView(ptr + 1, valueSize);
            ptr += entrySize;
        }
        remaining -= payloadSize;
    }
    if (remaining != 0) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "trailing bytes in batch query response");
    }
    return Status::OK();
}

Status ShareMapStoreClient::QueryData(
    const std::string& rpcEndpoint, const std::string& bucketKey,
    uint64_t valueSize, const std::vector<uint64_t>& keys,
    std::vector<StringView>& buffers,
    std::vector<std::shared_ptr<mooncake::BufferHandle>>& bufferHandles) {
    buffers.clear();
    if (keys.empty()) return Status::OK();

    auto rpcClient = AcquireClient(rpcEndpoint);
    if (!rpcClient) {
        return Status::Error(ErrorCode::kInternal,
                             "RPC client connect failed: " + rpcEndpoint);
    }

    QueryDataRequest req;
    req.bucketKey = bucketKey;
    req.keys = keys;
    req.valueSize = valueSize;
    if (valueSize == 0) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "valueSize must be > 0");
    }
    uint64_t entrySize = 0;
    uint64_t expectedSize = 0;
    if (!CheckedAdd(1, valueSize, entrySize) ||
        !CheckedMultiply(keys.size(), entrySize, expectedSize)) {
        return Status::Error(ErrorCode::kOutOfRange,
                             "query response size overflows");
    }
    auto handle = AllocateTransferBuffer(expectedSize);
    if (!handle) {
        return Status::Error(ErrorCode::kOutOfRange,
                             "client transfer buffer exhausted");
    }
    req.targetEndpoint = transferSegmentEndpoint_;
    req.targetAddress = reinterpret_cast<uint64_t>(handle->ptr());
    req.targetCapacity = handle->size();

    auto result = async_simple::coro::syncAwait(
        rpcClient->get()->call<&ShareMapStoreRpcService::HandleQueryData>(req));
    if (!result) {
        return Status::Error(ErrorCode::kInternal,
                             "RPC call failed: " + result.error().msg);
    }
    const auto& resp = result.value();
    if (resp.statusCode != 0) {
        return FromRemoteStatus(resp.statusCode, "Remote query", resp.errorMsg);
    }

    if (resp.transferredSize == 0) {
        return Status::OK();
    }
    if (resp.transferredSize > handle->size()) {
        return Status::Error(ErrorCode::kOutOfRange,
                             "remote query exceeded target buffer");
    }

    auto parseStatus =
        ParseResultBuffer(handle->ptr(), resp.transferredSize, valueSize,
                          keys.size(), buffers, handle);
    if (!parseStatus.IsOk()) return parseStatus;
    bufferHandles.push_back(handle);
    return Status::OK();
}

Status ShareMapStoreClient::BatchQueryData(
    const std::string& rpcEndpoint, const std::vector<std::string>& bucketKeys,
    uint64_t valueSize, const std::vector<std::vector<uint64_t>>& keysPerBucket,
    std::vector<std::vector<StringView>>& buffersPerBucket,
    std::vector<std::shared_ptr<mooncake::BufferHandle>>& bufferHandles) {
    buffersPerBucket.clear();
    if (bucketKeys.empty()) return Status::OK();
    if (bucketKeys.size() != keysPerBucket.size()) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "bucketKeys/keysPerBucket size mismatch");
    }
    if (valueSize == 0) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "valueSize must be > 0");
    }

    uint64_t entrySize = 0;
    if (!CheckedAdd(1, valueSize, entrySize)) {
        return Status::Error(ErrorCode::kOutOfRange,
                             "batch entry size overflows");
    }
    uint64_t expectedSize = sizeof(uint64_t);
    for (size_t i = 0; i < bucketKeys.size(); ++i) {
        if (bucketKeys[i].empty() ||
            bucketKeys[i].size() > std::numeric_limits<uint32_t>::max()) {
            return Status::Error(ErrorCode::kInvalidArgument,
                                 "invalid bucket key");
        }
        uint64_t entryBytes = 0;
        uint64_t bucketBytes = 0;
        if (!CheckedMultiply(keysPerBucket[i].size(), entrySize, entryBytes) ||
            !CheckedAdd(sizeof(uint32_t), bucketKeys[i].size(), bucketBytes) ||
            !CheckedAdd(bucketBytes, sizeof(uint64_t), bucketBytes) ||
            !CheckedAdd(bucketBytes, entryBytes, bucketBytes) ||
            !CheckedAdd(expectedSize, bucketBytes, expectedSize)) {
            return Status::Error(ErrorCode::kOutOfRange,
                                 "batch query size overflows");
        }
    }

    auto rpcClient = AcquireClient(rpcEndpoint);
    if (!rpcClient) {
        return Status::Error(ErrorCode::kInternal,
                             "RPC client connect failed: " + rpcEndpoint);
    }

    BatchQueryDataRequest req;
    req.valueSize = valueSize;
    req.entries.reserve(bucketKeys.size());
    for (size_t i = 0; i < bucketKeys.size(); ++i) {
        BatchQueryEntry entry;
        entry.bucketKey = bucketKeys[i];
        entry.keys = keysPerBucket[i];
        req.entries.push_back(std::move(entry));
    }

    auto handle = AllocateTransferBuffer(expectedSize);
    if (!handle) {
        return Status::Error(ErrorCode::kOutOfRange,
                             "client transfer buffer exhausted");
    }
    req.targetEndpoint = transferSegmentEndpoint_;
    req.targetAddress = reinterpret_cast<uint64_t>(handle->ptr());
    req.targetCapacity = handle->size();

    auto result = async_simple::coro::syncAwait(
        rpcClient->get()->call<&ShareMapStoreRpcService::HandleBatchQueryData>(
            req));
    if (!result) {
        return Status::Error(ErrorCode::kInternal,
                             "RPC batch call failed: " + result.error().msg);
    }
    const auto& resp = result.value();
    if (resp.statusCode != 0) {
        return FromRemoteStatus(resp.statusCode, "Remote batch query",
                                resp.errorMsg);
    }
    if (resp.responses.size() != bucketKeys.size()) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "remote batch response count mismatch");
    }

    for (size_t i = 0; i < resp.responses.size(); ++i) {
        if (resp.responses[i].foundFlags.size() != keysPerBucket[i].size()) {
            return Status::Error(
                ErrorCode::kInvalidArgument,
                "remote batch control key count mismatch: bucket=" +
                    bucketKeys[i] + ", expected=" +
                    std::to_string(keysPerBucket[i].size()) + ", actual=" +
                    std::to_string(resp.responses[i].foundFlags.size()));
        }
    }

    const uint64_t transferredSize =
        resp.responses.empty() ? 0 : resp.responses.front().transferredSize;
    for (const auto& response : resp.responses) {
        if (response.transferredSize != transferredSize) {
            return Status::Error(ErrorCode::kInvalidArgument,
                                 "remote batch transferred size mismatch");
        }
    }
    if (transferredSize > handle->size()) {
        return Status::Error(ErrorCode::kOutOfRange,
                             "remote batch query exceeded target buffer");
    }
    auto parseStatus = ParseAggregatedBuffer(
        handle->ptr(), transferredSize, valueSize, bucketKeys, keysPerBucket,
        buffersPerBucket, handle);
    if (!parseStatus.IsOk()) return parseStatus;
    bufferHandles.push_back(handle);
    return Status::OK();
}

Status ShareMapStoreClient::Publish(const std::string& rpcEndpoint,
                                    const std::string& bucketKey,
                                    uint64_t valueSize,
                                    const std::vector<uint64_t>& keys,
                                    const std::vector<StringView>& values) {
    if (keys.empty()) return Status::OK();
    if (bucketKey.empty() || valueSize == 0 || values.size() != keys.size()) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "invalid publish arguments");
    }
    uint64_t dataSize = 0;
    if (!CheckedMultiply(keys.size(), valueSize, dataSize) ||
        dataSize > std::numeric_limits<size_t>::max()) {
        return Status::Error(ErrorCode::kOutOfRange,
                             "publish payload size overflows");
    }
    for (const auto& value : values) {
        if (value.size() != valueSize) {
            return Status::Error(ErrorCode::kInvalidArgument,
                                 "publish value size mismatch");
        }
    }

    auto rpcClient = AcquireClient(rpcEndpoint);
    if (!rpcClient) {
        return Status::Error(ErrorCode::kInternal,
                             "RPC client connect failed: " + rpcEndpoint);
    }

    PublishRequest req;
    req.bucketKey = bucketKey;
    req.valueSize = valueSize;
    req.keys = keys;
    req.valuesData.resize(static_cast<size_t>(dataSize));
    for (size_t i = 0; i < values.size(); ++i) {
        uint64_t offset = 0;
        if (!CheckedMultiply(i, valueSize, offset) ||
            !IsRangeValid(offset, valueSize, dataSize)) {
            return Status::Error(ErrorCode::kOutOfRange,
                                 "publish value range is invalid");
        }
        std::memcpy(req.valuesData.data() + offset, values[i].data(),
                    static_cast<size_t>(valueSize));
    }

    auto result = async_simple::coro::syncAwait(
        rpcClient->get()->call<&ShareMapStoreRpcService::HandlePublish>(req));
    if (!result) {
        return Status::Error(ErrorCode::kInternal,
                             "RPC call failed: " + result.error().msg);
    }
    if (result.value().statusCode != 0) {
        return FromRemoteStatus(result.value().statusCode, "Remote publish",
                                result.value().errorMsg);
    }
    return Status::OK();
}

Status ShareMapStoreClient::BuildIndex(const std::string& rpcEndpoint,
                                       const std::string& bucketKey) {
    auto rpcClient = AcquireClient(rpcEndpoint);
    if (!rpcClient) {
        return Status::Error(ErrorCode::kInternal,
                             "RPC client connect failed: " + rpcEndpoint);
    }

    BuildIndexRequest req;
    req.bucketKey = bucketKey;

    auto result = async_simple::coro::syncAwait(
        rpcClient->get()->call<&ShareMapStoreRpcService::HandleBuildIndex>(
            req));
    if (!result) {
        return Status::Error(ErrorCode::kInternal,
                             "RPC call failed: " + result.error().msg);
    }
    if (result.value().statusCode != 0) {
        return FromRemoteStatus(result.value().statusCode, "Remote build index",
                                result.value().errorMsg);
    }
    return Status::OK();
}

}  // namespace embtable
