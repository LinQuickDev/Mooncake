#include "emb_table/emb_table.h"

#include <glog/logging.h>
#include <xxhash.h>

#include <algorithm>
#include <fstream>
#include <future>
#include <limits>
#include <unordered_map>

namespace embtable {

EmbTable::EmbTable(const std::string& tableName, uint32_t numBuckets,
                   uint64_t valueSize,
                   std::shared_ptr<ShareMapStore> shareMapStore,
                   std::shared_ptr<mooncake::RealClient> realClient,
                   std::shared_ptr<ShareMapStoreClient> shareMapStoreClient,
                   const std::string& localHostname,
                   uint16_t shareMapStoreRpcPort)
    : tableName_(tableName),
      numBuckets_(numBuckets > 0 ? numBuckets : 1),
      valueSize_(valueSize),
      shareMapStore_(std::move(shareMapStore)),
      realClient_(std::move(realClient)),
      shareMapStoreClient_(std::move(shareMapStoreClient)),
      localHostname_(localHostname),
      shareMapStoreRpcPort_(shareMapStoreRpcPort),
      meta_(std::make_shared<EmbTableMeta>(realClient_)) {
    buckets_.reserve(numBuckets_);
}

uint32_t EmbTable::RouteToBucket(uint64_t key) const {
    // xxHash-based routing (design doc 8.4 default).
    uint64_t h = XXH64(&key, sizeof(key), 0);
    return static_cast<uint32_t>(h % numBuckets_);
}

Status EmbTable::Init(bool createNew) {
    if (valueSize_ == 0) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "valueSize must be non-zero");
    }
    uint64_t tableCapacity = 0;
    if (!CheckedMultiply(numBuckets_, 1024, tableCapacity)) {
        return Status::Error(ErrorCode::kOutOfRange,
                             "table capacity overflows");
    }
    TableMetaInfo info;
    info.tableKey = tableName_ + "_meta";
    info.tableName = tableName_;
    info.dimSize = valueSize_;
    info.bucketNum = numBuckets_;
    info.hashType = HashFunctionType::kXxHash;
    info.tableCapacity = tableCapacity;  // placeholder default
    info.bucketCapacity = 1024;

    Status s;
    if (createNew) {
        s = meta_->CreateTableMeta(info);
    } else {
        s = meta_->QueryTableMeta(info.tableKey, info);
        if (!s.IsOk()) return s;
        if (info.tableName != tableName_ || info.bucketNum == 0 ||
            info.dimSize == 0 || info.bucketCapacity == 0) {
            return Status::Error(ErrorCode::kInvalidArgument,
                                 "invalid persisted table metadata");
        }
        numBuckets_ = static_cast<uint32_t>(info.bucketNum);
        valueSize_ = info.dimSize;
    }
    if (!s.IsOk()) return s;
    buckets_.clear();
    // For newly created tables, register bucket meta in Mooncake Store so
    // other nodes can locate the owning node from bucket-meta replica
    // placement (design doc 4.3).
    for (uint32_t i = 0; i < numBuckets_; ++i) {
        BucketInfo bi;
        bi.tableKey = info.tableKey;
        bi.bucketKey = tableName_ + "_bucket_" + std::to_string(i);
        bi.valueSize = valueSize_;
        bi.capacity = info.bucketCapacity;
        bi.currentSize = 0;
        if (createNew && shareMapStoreRpcPort_ != 0) {
            bi.rpcEndpoint =
                localHostname_ + ":" + std::to_string(shareMapStoreRpcPort_);
        } else if (!createNew) {
            BucketInfo storedInfo;
            auto queryStatus = meta_->QueryBucketMeta(bi.bucketKey, storedInfo);
            if (!queryStatus.IsOk()) return queryStatus;
            if (storedInfo.tableKey != info.tableKey ||
                storedInfo.bucketKey != bi.bucketKey ||
                storedInfo.valueSize != valueSize_ ||
                storedInfo.capacity == 0 ||
                storedInfo.currentSize > storedInfo.capacity) {
                return Status::Error(ErrorCode::kInvalidArgument,
                                     "invalid persisted bucket metadata");
            }
            bi = std::move(storedInfo);
        }
        buckets_.push_back(std::make_shared<Bucket>(
            bi, shareMapStore_, realClient_, shareMapStoreClient_,
            localHostname_, shareMapStoreRpcPort_));
        if (createNew) {
            auto bs = meta_->CreateBucketMeta(bi);
            if (!bs.IsOk()) return bs;
        }
    }
    return Status::OK();
}

Status EmbTable::Insert(const std::vector<uint64_t>& keys,
                        const std::vector<StringView>& values) {
    std::shared_lock<std::shared_mutex> lifecycleLock(lifecycleMutex_);
    if (keys.size() != values.size()) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "keys/values size mismatch");
    }
    // Group by bucket index for batched inserts.
    std::unordered_map<uint32_t,
                       std::pair<std::vector<uint64_t>, std::vector<StringView>>>
        grouped;
    for (size_t i = 0; i < keys.size(); ++i) {
        uint32_t bidx = RouteToBucket(keys[i]);
        auto& entry = grouped[bidx];
        entry.first.push_back(keys[i]);
        entry.second.push_back(values[i]);
    }
    // Insert into each touched bucket's local buffer; flush only the buckets
    // whose buffer reached the capacity threshold (design doc 4.1.4).
    for (auto& [bidx, kv] : grouped) {
        bool wouldFlush = false;
        auto s = buckets_[bidx]->Insert(kv.first, kv.second, wouldFlush);
        if (!s.IsOk()) return s;
        if (wouldFlush) {
            s = buckets_[bidx]->Flush();
            if (!s.IsOk()) return s;
        }
    }
    return Status::OK();
}

Status EmbTable::Find(const std::vector<uint64_t>& keys,
                      std::vector<StringView>& buffers,
                      std::vector<std::shared_ptr<mooncake::BufferHandle>>&
                          bufferHandles) {
    std::shared_lock<std::shared_mutex> lifecycleLock(lifecycleMutex_);
    buffers.clear();
    buffers.resize(keys.size());
    bufferHandles.clear();
    if (keys.empty()) return Status::OK();

    // Step 1: Group keys by target bucket index.
    std::unordered_map<uint32_t, std::vector<size_t>> indexGroups;
    for (size_t i = 0; i < keys.size(); ++i) {
        uint32_t bidx = RouteToBucket(keys[i]);
        indexGroups[bidx].push_back(i);
    }

    // Step 2: For each bucket, resolve locality (local vs. remote node).
    // Group remote buckets by rpcEndpoint so we can batch RPC calls to the
    // same remote node (design doc 4.3 — aggregate by compute node).
    //
    // Structure: rpcEndpoint -> list of (bucketIdx, indices, bucketKeys)
    struct RemoteBucketTask {
        uint32_t bucketIdx;
        std::vector<size_t> indices;
        std::vector<uint64_t> keys;
    };
    std::unordered_map<std::string, std::vector<RemoteBucketTask>>
        remoteGroups;
    // Local tasks: bucketIdx -> (indices, keys)
    std::vector<std::pair<uint32_t, std::vector<size_t>>> localTasks;
    std::unordered_map<uint32_t, std::vector<uint64_t>> localKeys;

    for (auto& [bidx, indices] : indexGroups) {
        // Force locality resolution on the bucket.
        auto& bucket = buckets_[bidx];
        auto s = bucket->Flush();  // ensure pending data is flushed first
        if (!s.IsOk()) {
            return s;
        }

        // Bucket owns locality resolution so routing and endpoint construction
        // are consistent across Find, Flush, and BuildIndex.
        auto localityStatus = bucket->ResolveLocality();
        if (!localityStatus.IsOk()) {
            return localityStatus;
        }
        bool isLocal = bucket->IsLocal();
        std::string endpoint = bucket->RpcEndpoint();

        std::vector<uint64_t> bkeys;
        bkeys.reserve(indices.size());
        for (auto idx : indices) bkeys.push_back(keys[idx]);

        if (isLocal) {
            localTasks.emplace_back(bidx, indices);
            localKeys[bidx] = std::move(bkeys);
        } else {
            if (endpoint.empty()) {
                return Status::Error(
                    ErrorCode::kNotFound,
                    "remote bucket endpoint is empty: " + bucket->BucketKey());
            }
            remoteGroups[endpoint].push_back(
                RemoteBucketTask{bidx, indices, std::move(bkeys)});
        }
    }

    // Step 3: Execute local queries (direct ShareMapStore::QueryData).
    for (auto& [bidx, indices] : localTasks) {
        std::vector<StringView> bucketVals;
        std::vector<std::shared_ptr<mooncake::BufferHandle>> tmpHandles;
        auto s = buckets_[bidx]->Find(localKeys[bidx], bucketVals, tmpHandles);
        if (!s.IsOk()) {
            return s;
        }
        for (size_t j = 0; j < indices.size() && j < bucketVals.size(); ++j) {
            buffers[indices[j]] = bucketVals[j];
        }
        // Keep handles alive (though local queries usually don't need them).
        bufferHandles.insert(bufferHandles.end(),
                             std::make_move_iterator(tmpHandles.begin()),
                             std::make_move_iterator(tmpHandles.end()));
    }

    // Step 4: Execute remote queries grouped by endpoint. Each endpoint is
    // queried independently so fan-out latency is bounded by the slowest
    // remote node. A single endpoint stays inline to avoid creating a worker
    // thread for the common one-node case.
    struct RemoteQueryResult {
        Status status;
        std::string endpoint;
        std::vector<RemoteBucketTask> tasks;
        std::vector<std::vector<StringView>> buffersPerBucket;
        std::vector<std::shared_ptr<mooncake::BufferHandle>> handles;
    };

    auto queryRemote = [this](std::string endpoint,
                              std::vector<RemoteBucketTask> tasks) {
        RemoteQueryResult result;
        result.status = Status::OK();
        result.endpoint = endpoint;
        result.tasks = std::move(tasks);
        if (!shareMapStoreClient_) {
            result.status = Status::Error(
                ErrorCode::kInternal,
                "remote bucket requires ShareMapStoreClient: " + endpoint);
            return result;
        }

        if (result.tasks.size() == 1) {
            auto& task = result.tasks[0];
            std::vector<StringView> bucketVals;
            result.status = shareMapStoreClient_->QueryData(
                endpoint, buckets_[task.bucketIdx]->BucketKey(), valueSize_,
                task.keys, bucketVals, result.handles);
            if (result.status.IsOk()) {
                result.buffersPerBucket.push_back(std::move(bucketVals));
            }
            return result;
        }

        std::vector<std::string> bucketKeys;
        std::vector<std::vector<uint64_t>> keysPerBucket;
        bucketKeys.reserve(result.tasks.size());
        keysPerBucket.reserve(result.tasks.size());
        for (const auto& task : result.tasks) {
            bucketKeys.push_back(buckets_[task.bucketIdx]->BucketKey());
            keysPerBucket.push_back(task.keys);
        }
        result.status = shareMapStoreClient_->BatchQueryData(
            endpoint, bucketKeys, valueSize_, keysPerBucket,
            result.buffersPerBucket, result.handles);
        return result;
    };

    auto mergeRemote = [&](RemoteQueryResult result) -> Status {
        if (!result.status.IsOk()) {
            LOG(WARNING) << "Remote query failed for endpoint "
                         << result.endpoint << ": " << result.status.msg();
            return result.status;
        }
        for (size_t t = 0;
             t < result.tasks.size() && t < result.buffersPerBucket.size(); ++t) {
            const auto& task = result.tasks[t];
            const auto& bucketVals = result.buffersPerBucket[t];
            for (size_t j = 0;
                 j < task.indices.size() && j < bucketVals.size(); ++j) {
                buffers[task.indices[j]] = bucketVals[j];
            }
        }
        bufferHandles.insert(bufferHandles.end(),
                             std::make_move_iterator(result.handles.begin()),
                             std::make_move_iterator(result.handles.end()));
        return Status::OK();
    };

    if (remoteGroups.size() == 1) {
        auto it = remoteGroups.begin();
        auto s = mergeRemote(queryRemote(it->first, std::move(it->second)));
        if (!s.IsOk()) return s;
    } else if (!remoteGroups.empty()) {
        std::vector<std::future<RemoteQueryResult>> futures;
        futures.reserve(remoteGroups.size());
        for (auto& [endpoint, tasks] : remoteGroups) {
            futures.emplace_back(std::async(
                std::launch::async, queryRemote, endpoint, std::move(tasks)));
        }
        for (auto& future : futures) {
            auto s = mergeRemote(future.get());
            if (!s.IsOk()) return s;
        }
    }

    return Status::OK();
}

Status EmbTable::Find(const std::vector<uint64_t>& keys,
                      std::vector<StringView>& buffers) {
    // StringView does not carry ownership. Keep the remote handles isolated
    // per calling thread so concurrent callers do not invalidate one another's
    // result storage. For independent lifetimes, use the explicit handles
    // overload.
    thread_local std::unordered_map<
        const EmbTable*,
        std::vector<std::shared_ptr<mooncake::BufferHandle>>>
        threadFindHandles;
    auto& handles = threadFindHandles[this];
    handles.clear();
    return Find(keys, buffers, handles);
}

Status EmbTable::BuildIndex() {
    std::unique_lock<std::shared_mutex> lifecycleLock(lifecycleMutex_);
    for (auto& bucket : buckets_) {
        auto s = bucket->BuildIndex();
        if (!s.IsOk()) return s;
    }
    return Status::OK();
}

Status EmbTable::Load(const std::vector<std::string>& keyFiles,
                      const std::vector<std::string>& valueFiles,
                      const std::string& format) {
    if (keyFiles.size() != valueFiles.size()) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "keyFiles/valueFiles size mismatch");
    }
    if (format != "binary" && format != "text") {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "unsupported format: " + format +
                                 " (only 'binary' or 'text')");
    }

    const uint64_t valueSize = valueSize_;
    const size_t batchSize = 1024;  // keys per Insert batch

    for (size_t fi = 0; fi < keyFiles.size(); ++fi) {
        // Read key file: binary file of uint64_t keys.
        std::ifstream kf(keyFiles[fi], std::ios::binary);
        if (!kf.is_open()) {
            return Status::Error(ErrorCode::kIOError,
                                 "cannot open key file: " + keyFiles[fi]);
        }
        std::vector<uint64_t> keys;
        kf.seekg(0, std::ios::end);
        auto sz = kf.tellg();
        kf.seekg(0, std::ios::beg);
        if (sz % sizeof(uint64_t) != 0) {
            return Status::Error(
                ErrorCode::kInvalidArgument,
                "key file size not multiple of sizeof(uint64_t): " +
                    keyFiles[fi]);
        }
        keys.resize(sz / sizeof(uint64_t));
        kf.read(reinterpret_cast<char*>(keys.data()), sz);
        kf.close();

        // Read value file: binary file of valueSize-byte values.
        std::ifstream vf(valueFiles[fi], std::ios::binary);
        if (!vf.is_open()) {
            return Status::Error(ErrorCode::kIOError,
                                 "cannot open value file: " + valueFiles[fi]);
        }
        vf.seekg(0, std::ios::end);
        auto vsz = vf.tellg();
        vf.seekg(0, std::ios::beg);
        if (vsz != static_cast<std::streamoff>(keys.size() * valueSize)) {
            return Status::Error(
                ErrorCode::kInvalidArgument,
                "value file size mismatch with key count: " + valueFiles[fi]);
        }
        std::string valuesData(vsz, '\0');
        vf.read(&valuesData[0], vsz);
        vf.close();

        // Insert in batches.
        for (size_t start = 0; start < keys.size(); start += batchSize) {
            size_t end = std::min(start + batchSize, keys.size());
            std::vector<uint64_t> batchKeys(keys.begin() + start,
                                            keys.begin() + end);
            std::vector<StringView> batchVals;
            batchVals.reserve(end - start);
            for (size_t i = 0; i < batchKeys.size(); ++i) {
                const char* p = valuesData.data() + (start + i) * valueSize;
                batchVals.emplace_back(p, valueSize);
            }
            auto s = Insert(batchKeys, batchVals);
            if (!s.IsOk()) return s;
        }
    }

    // Flush all buckets after loading is complete.
    for (auto& bucket : buckets_) {
        auto s = bucket->Flush();
        if (!s.IsOk()) return s;
    }
    return Status::OK();
}

Status EmbTable::Delete(const std::vector<uint64_t>& keys) {
    return Status::Error(ErrorCode::kNotSupported,
                         "Delete not supported (read-only after BuildIndex)");
}

const TableMetaInfo& EmbTable::MetaInfo() const {
    return meta_->GetLocalMeta();
}

std::shared_ptr<Bucket> EmbTable::GetBucket(uint32_t index) {
    if (index >= buckets_.size()) return nullptr;
    return buckets_[index];
}

}  // namespace embtable
