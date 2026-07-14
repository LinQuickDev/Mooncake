#include "share_map_store/share_map_store_client.h"

#include <cstring>
#include <thread>
#include <unordered_map>

#include <async_simple/coro/SyncAwait.h>
#include <glog/logging.h>
#include "replica.h"
#include "client_service.h"

namespace embtable {

std::string ShareMapStoreClient::ExtractHostname(
    const std::string& endpoint) {
    // transport_endpoint_ looks like "hostname:port" or "ip:port".
    auto pos = endpoint.rfind(':');
    if (pos == std::string::npos) return endpoint;
    return endpoint.substr(0, pos);
}

bool ShareMapStoreClient::IsBucketLocal(const std::string& bucketKey,
                                        std::string& ownerHostname) {
    ownerHostname.clear();
    if (!client_) return false;

    // Query the bucket meta object's replica info via Mooncake Client
    // (design doc 4.3 — Query bucket meta replica to localize the owning
    // node). The bucket meta object key is bucketKey + "_bucketmeta".
    std::string bucketMetaKey = bucketKey + "_bucketmeta";
    auto queryResult = client_->batch_query({bucketMetaKey});
    if (queryResult.empty()) return false;

    auto& first = queryResult[0];
    if (!first.has_value()) return false;
    const auto& replicas = first.value().replicas;
    if (replicas.empty()) return false;

    // Check if any replica is on local memory using the Client helper.
    // PyClient holds a shared_ptr<mooncake::Client> client_ member.
    if (!client_->client_) return false;
    for (const auto& replica : replicas) {
        if (client_->client_->IsReplicaOnLocalMemory(replica)) {
            ownerHostname = localHostname_;
            return true;
        }
    }

    // Not local: extract the owning host from the first memory replica's
    // transport_endpoint_ ("host:port").
    for (const auto& replica : replicas) {
        if (replica.is_memory_replica()) {
            const auto& mem_desc =
                std::get<mooncake::MemoryDescriptor>(replica.descriptor_variant);
            const auto& endpoint = mem_desc.buffer_descriptor.transport_endpoint_;
            if (!endpoint.empty()) {
                ownerHostname = ExtractHostname(endpoint);
                return false;
            }
        }
    }
    return false;
}

coro_rpc::coro_rpc_client* ShareMapStoreClient::GetClient(
    const std::string& rpcEndpoint) {
    std::lock_guard<std::mutex> lock(cacheMutex_);
    auto it = clientCache_.find(rpcEndpoint);
    if (it != clientCache_.end()) return it->second.get();

    auto client = std::make_unique<coro_rpc::coro_rpc_client>();
    // coro_rpc::err_code: operator bool() returns true on error.
    auto ec = async_simple::coro::syncAwait(client->connect(rpcEndpoint));
    if (ec) {
        LOG(ERROR) << "Failed to connect to ShareMapStore RPC endpoint: "
                   << rpcEndpoint << ", error: " << ec.message();
        return nullptr;
    }
    auto* ptr = client.get();
    clientCache_[rpcEndpoint] = std::move(client);
    return ptr;
}

void ShareMapStoreClient::ParseResultBuffer(
    void* data, uint64_t dataSize, uint64_t valueSize,
    size_t numKeys, const std::vector<int8_t>& foundFlags,
    std::vector<StringView>& buffers,
    std::shared_ptr<mooncake::BufferHandle> handle) {
    buffers.resize(numKeys);
    if (!data || dataSize == 0 || valueSize == 0) return;

    const char* ptr = static_cast<const char*>(data);
    uint64_t entrySize = 1 + valueSize;
    for (size_t i = 0; i < numKeys; ++i) {
        if (i < foundFlags.size() && foundFlags[i]) {
            const char* entry = ptr + i * entrySize;
            buffers[i] = StringView(entry + 1, valueSize);
        } else {
            buffers[i] = StringView();
        }
    }
    // NOTE: handle is kept alive by the caller (bufferHandles) so StringViews
    // remain valid.
    (void)handle;
}

void ShareMapStoreClient::ParseAggregatedBuffer(
    void* data, uint64_t dataSize, uint64_t valueSize,
    const std::vector<std::string>& bucketKeys,
    const std::vector<std::vector<int8_t>>& foundFlagsPerBucket,
    std::vector<std::vector<StringView>>& buffersPerBucket,
    std::shared_ptr<mooncake::BufferHandle> handle) {
    buffersPerBucket.assign(bucketKeys.size(), {});
    if (!data || dataSize < sizeof(uint64_t) || valueSize == 0) return;

    const char* ptr = static_cast<const char*>(data);
    const char* end = ptr + dataSize;
    uint64_t bucketCount = 0;
    std::memcpy(&bucketCount, ptr, sizeof(uint64_t));
    ptr += sizeof(uint64_t);

    const uint64_t entrySize = 1 + valueSize;
    // Build a map from bucketKey -> index for fast lookup.
    std::unordered_map<std::string, size_t> keyToIdx;
    for (size_t i = 0; i < bucketKeys.size(); ++i) {
        keyToIdx[bucketKeys[i]] = i;
    }

    for (uint64_t b = 0; b < bucketCount && ptr + sizeof(uint32_t) <= end;
         ++b) {
        uint32_t keyLen = 0;
        std::memcpy(&keyLen, ptr, sizeof(uint32_t));
        ptr += sizeof(uint32_t);
        if (ptr + keyLen + sizeof(uint64_t) > end) break;
        std::string bkey(ptr, keyLen);
        ptr += keyLen;
        uint64_t keyCount = 0;
        std::memcpy(&keyCount, ptr, sizeof(uint64_t));
        ptr += sizeof(uint64_t);

        auto it = keyToIdx.find(bkey);
        if (it == keyToIdx.end()) {
            // Unknown bucket; skip its entries.
            ptr += keyCount * entrySize;
            continue;
        }
        size_t idx = it->second;
        auto& buffers = buffersPerBucket[idx];
        buffers.resize(keyCount);
        const auto& flags =
            idx < foundFlagsPerBucket.size() ? foundFlagsPerBucket[idx]
                                             : std::vector<int8_t>{};
        for (uint64_t k = 0; k < keyCount; ++k) {
            if (ptr + entrySize > end) break;
            if (k < flags.size() && flags[k]) {
                buffers[k] = StringView(ptr + 1, valueSize);
            } else {
                buffers[k] = StringView();
            }
            ptr += entrySize;
        }
    }
    (void)handle;
}

Status ShareMapStoreClient::QueryData(
    const std::string& rpcEndpoint, const std::string& bucketKey,
    uint64_t valueSize, const std::vector<uint64_t>& keys,
    std::vector<StringView>& buffers,
    std::vector<std::shared_ptr<mooncake::BufferHandle>>& bufferHandles) {
    buffers.clear();
    if (keys.empty()) return Status::OK();

    auto* rpcClient = GetClient(rpcEndpoint);
    if (!rpcClient) {
        return Status::Error(ErrorCode::kInternal,
                             "RPC client connect failed: " + rpcEndpoint);
    }

    QueryDataRequest req;
    req.bucketKey = bucketKey;
    req.keys = keys;
    req.valueSize = valueSize;

    auto result = async_simple::coro::syncAwait(
        rpcClient->call<&ShareMapStoreRpcService::HandleQueryData>(req));
    if (!result) {
        return Status::Error(ErrorCode::kInternal,
                             "RPC call failed: " + result.error().msg);
    }
    const auto& resp = result.value();
    if (resp.statusCode != 0) {
        return Status::Error(ErrorCode::kInternal,
                             "Remote query failed: " + resp.errorMsg);
    }

    // Read temp object via TE read (get_buffer).
    if (resp.resultObjectKey.empty()) {
        return Status::OK();
    }
    auto handle = client_->get_buffer(resp.resultObjectKey);
    if (!handle) {
        return Status::Error(ErrorCode::kIOError,
                             "get_buffer failed: " + resp.resultObjectKey);
    }

    ParseResultBuffer(handle->ptr(), handle->size(), valueSize,
                      keys.size(), resp.foundFlags, buffers, handle);
    bufferHandles.push_back(handle);

    // Clean up temp object.
    client_->remove(resp.resultObjectKey);
    return Status::OK();
}

Status ShareMapStoreClient::BatchQueryData(
    const std::string& rpcEndpoint,
    const std::vector<std::string>& bucketKeys, uint64_t valueSize,
    const std::vector<std::vector<uint64_t>>& keysPerBucket,
    std::vector<std::vector<StringView>>& buffersPerBucket,
    std::vector<std::shared_ptr<mooncake::BufferHandle>>& bufferHandles) {
    buffersPerBucket.clear();
    if (bucketKeys.empty()) return Status::OK();

    auto* rpcClient = GetClient(rpcEndpoint);
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

    auto result = async_simple::coro::syncAwait(
        rpcClient->call<&ShareMapStoreRpcService::HandleBatchQueryData>(req));
    if (!result) {
        return Status::Error(ErrorCode::kInternal,
                             "RPC batch call failed: " + result.error().msg);
    }
    const auto& resp = result.value();
    if (resp.statusCode != 0) {
        return Status::Error(ErrorCode::kInternal,
                             "Remote batch query failed: " + resp.errorMsg);
    }

    buffersPerBucket.assign(bucketKeys.size(), {});
    if (resp.responses.empty()) return Status::OK();

    // Detect aggregated mode: all responses share the same resultObjectKey.
    // In that case we do a single get_buffer (TE read) and parse segments.
    const auto& firstKey = resp.responses[0].resultObjectKey;
    bool aggregated = !firstKey.empty();
    for (const auto& r : resp.responses) {
        if (r.resultObjectKey != firstKey) {
            aggregated = false;
            break;
        }
    }

    if (aggregated) {
        // Single get_buffer for the whole aggregated buffer.
        auto handle = client_->get_buffer(firstKey);
        if (!handle) {
            for (size_t i = 0; i < bucketKeys.size(); ++i) {
                buffersPerBucket[i].resize(keysPerBucket[i].size());
            }
            return Status::Error(
                ErrorCode::kIOError,
                "get_buffer failed for aggregated result: " + firstKey);
        }
        std::vector<std::vector<int8_t>> flagsPerBucket;
        flagsPerBucket.reserve(resp.responses.size());
        for (const auto& r : resp.responses) {
            flagsPerBucket.push_back(r.foundFlags);
        }
        ParseAggregatedBuffer(handle->ptr(), handle->size(), valueSize,
                              bucketKeys, flagsPerBucket, buffersPerBucket,
                              handle);
        bufferHandles.push_back(handle);
        client_->remove(firstKey);
        return Status::OK();
    }

    // Fallback: per-bucket get_buffer (non-aggregated legacy path).
    for (size_t i = 0; i < resp.responses.size() && i < bucketKeys.size(); ++i) {
        const auto& single = resp.responses[i];
        auto& buffers = buffersPerBucket[i];
        if (single.statusCode != 0 || single.resultObjectKey.empty()) {
            buffers.resize(keysPerBucket[i].size());
            continue;
        }
        auto handle = client_->get_buffer(single.resultObjectKey);
        if (!handle) {
            buffers.resize(keysPerBucket[i].size());
            continue;
        }
        ParseResultBuffer(handle->ptr(), handle->size(), valueSize,
                          keysPerBucket[i].size(), single.foundFlags, buffers,
                          handle);
        bufferHandles.push_back(handle);
        client_->remove(single.resultObjectKey);
    }
    return Status::OK();
}

Status ShareMapStoreClient::Publish(
    const std::string& rpcEndpoint, const std::string& bucketKey,
    uint64_t valueSize, const std::vector<uint64_t>& keys,
    const std::vector<StringView>& values) {
    if (keys.empty()) return Status::OK();

    auto* rpcClient = GetClient(rpcEndpoint);
    if (!rpcClient) {
        return Status::Error(ErrorCode::kInternal,
                             "RPC client connect failed: " + rpcEndpoint);
    }

    PublishRequest req;
    req.bucketKey = bucketKey;
    req.valueSize = valueSize;
    req.keys = keys;
    req.valuesData.resize(keys.size() * valueSize);
    for (size_t i = 0; i < keys.size(); ++i) {
        if (i < values.size() && values[i].size() >= valueSize) {
            std::memcpy(&req.valuesData[i * valueSize], values[i].data(),
                        valueSize);
        }
    }

    auto result = async_simple::coro::syncAwait(
        rpcClient->call<&ShareMapStoreRpcService::HandlePublish>(req));
    if (!result) {
        return Status::Error(ErrorCode::kInternal,
                             "RPC call failed: " + result.error().msg);
    }
    if (result.value().statusCode != 0) {
        return Status::Error(ErrorCode::kInternal,
                             "Remote publish failed: " + result.value().errorMsg);
    }
    return Status::OK();
}

Status ShareMapStoreClient::BuildIndex(const std::string& rpcEndpoint,
                                       const std::string& bucketKey) {
    auto* rpcClient = GetClient(rpcEndpoint);
    if (!rpcClient) {
        return Status::Error(ErrorCode::kInternal,
                             "RPC client connect failed: " + rpcEndpoint);
    }

    BuildIndexRequest req;
    req.bucketKey = bucketKey;

    auto result = async_simple::coro::syncAwait(
        rpcClient->call<&ShareMapStoreRpcService::HandleBuildIndex>(req));
    if (!result) {
        return Status::Error(ErrorCode::kInternal,
                             "RPC call failed: " + result.error().msg);
    }
    if (result.value().statusCode != 0) {
        return Status::Error(ErrorCode::kInternal,
                             "Remote build index failed: " +
                                 result.value().errorMsg);
    }
    return Status::OK();
}

}  // namespace embtable
