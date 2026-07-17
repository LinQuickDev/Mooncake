#include "emb_table/emb_table_bucket.h"

#include <glog/logging.h>

#include "client_service.h"
#include "replica.h"

namespace embtable {

namespace {

std::string ExtractHostname(const std::string& endpoint) {
    auto pos = endpoint.rfind(':');
    return pos == std::string::npos ? endpoint : endpoint.substr(0, pos);
}

bool IsSameHost(const std::string& lhs, const std::string& rhs) {
    if (lhs.empty() || rhs.empty()) return false;
    if (lhs == rhs) return true;
    return ExtractHostname(lhs) == ExtractHostname(rhs);
}

}  // namespace

Bucket::Bucket(BucketInfo info,
               std::shared_ptr<ShareMapStore> shareMapStore,
               std::shared_ptr<mooncake::RealClient> realClient,
               std::shared_ptr<ShareMapStoreClient> shareMapStoreClient,
               const std::string& localHostname,
               uint16_t shareMapStoreRpcPort)
    : info_(std::move(info)),
      bucketKey_(info_.bucketKey),
      shareMapStore_(std::move(shareMapStore)),
      realClient_(std::move(realClient)),
      shareMapStoreClient_(std::move(shareMapStoreClient)),
      localHostname_(localHostname),
      shareMapStoreRpcPort_(shareMapStoreRpcPort) {}

Status Bucket::ResolveLocality() {
    std::lock_guard<std::mutex> localityLock(localityMutex_);
    const auto now = std::chrono::steady_clock::now();
    if (localityResolved_ &&
        now - localityResolvedAt_ < std::chrono::seconds(1)) {
        return Status::OK();
    }

    std::string ownerHost;
    bool replicaResolved = false;

    if (realClient_ && realClient_->client_) {
        auto queryResult =
            realClient_->batch_query({bucketKey_ + "_bucketmeta"});
        if (!queryResult.empty() && queryResult[0].has_value()) {
            const auto& replicas = queryResult[0].value().replicas;
            for (const auto& replica : replicas) {
                if (realClient_->client_->IsReplicaOnLocalMemory(replica)) {
                    isLocal_ = true;
                    replicaResolved = true;
                    ownerHost = localHostname_;
                    break;
                }
            }
            if (!replicaResolved) {
                for (const auto& replica : replicas) {
                    if (!replica.is_memory_replica()) continue;
                    const auto& endpoint =
                        replica.get_memory_descriptor()
                            .buffer_descriptor.transport_endpoint_;
                    if (!endpoint.empty()) {
                        ownerHost = ExtractHostname(endpoint);
                        isLocal_ = IsSameHost(ownerHost, localHostname_);
                        replicaResolved = true;
                        break;
                    }
                }
            }
        }
    }

    if (!replicaResolved && !info_.rpcEndpoint.empty()) {
        ownerHost = ExtractHostname(info_.rpcEndpoint);
        isLocal_ = IsSameHost(ownerHost, localHostname_) ||
                   ownerHost == "127.0.0.1" || ownerHost == "localhost";
        replicaResolved = true;
    }

    if (!replicaResolved) {
        // Local-only deployments have no RPC routing requirement.
        if (!shareMapStoreClient_ || shareMapStoreRpcPort_ == 0) {
            isLocal_ = true;
            localityResolved_ = true;
            localityResolvedAt_ = now;
            return Status::OK();
        }
        return Status::Error(ErrorCode::kNotFound,
                             "cannot resolve bucket owner: " + bucketKey_);
    }

    if (!isLocal_) {
        if (ownerHost.empty() || shareMapStoreRpcPort_ == 0) {
            return Status::Error(
                ErrorCode::kInvalidArgument,
                "remote bucket has no ShareMapStore RPC endpoint: " +
                    bucketKey_);
        }
        info_.rpcEndpoint =
            ownerHost + ":" + std::to_string(shareMapStoreRpcPort_);
    } else if (info_.rpcEndpoint.empty() && shareMapStoreRpcPort_ != 0) {
        info_.rpcEndpoint =
            localHostname_ + ":" + std::to_string(shareMapStoreRpcPort_);
    }
    localityResolved_ = true;
    localityResolvedAt_ = now;
    return Status::OK();
}

void Bucket::InvalidateLocality() {
    std::lock_guard<std::mutex> lock(localityMutex_);
    localityResolved_ = false;
}

Status Bucket::Insert(const std::vector<uint64_t>& keys,
                      const std::vector<StringView>& values,
                      bool& wouldFlush) {
    wouldFlush = false;
    if (keys.size() != values.size()) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "keys/values size mismatch");
    }
    for (const auto& v : values) {
        if (v.size() != info_.valueSize) {
            return Status::Error(ErrorCode::kInvalidArgument,
                                 "value size != bucket valueSize");
        }
    }
    std::lock_guard<std::mutex> lock(bufferMutex_);
    // Inserts always go to the local write buffer; they are flushed later
    // when the buffer is full (design doc 4.1.4).
    localKeys_.reserve(localKeys_.size() + keys.size());
    localValues_.reserve(localValues_.size() + values.size());
    localKeys_.insert(localKeys_.end(), keys.begin(), keys.end());
    for (const auto& v : values) {
        localValues_.emplace_back(v.data(), v.size());
    }
    wouldFlush = localKeys_.size() >= flushThreshold_;
    return Status::OK();
}

Status Bucket::Insert(const std::vector<uint64_t>& keys,
                      const std::vector<StringView>& values) {
    bool unused = false;
    return Insert(keys, values, unused);
}

Status Bucket::Flush() {
    // Serialize flushes for this bucket. A concurrent Find must not observe an
    // empty pending buffer while another thread is still publishing the batch
    // that it already removed from the buffer.
    std::lock_guard<std::mutex> flushLock(flushMutex_);
    std::vector<uint64_t> keys;
    std::vector<std::string> values;
    {
        std::lock_guard<std::mutex> lock(bufferMutex_);
        if (localKeys_.empty()) return Status::OK();
        keys.swap(localKeys_);
        values.swap(localValues_);
    }

    // Resolve locality to decide whether to flush locally or remotely.
    auto s = ResolveLocality();
    if (!s.IsOk()) {
        std::lock_guard<std::mutex> lock(bufferMutex_);
        localKeys_.insert(localKeys_.begin(), keys.begin(), keys.end());
        localValues_.insert(localValues_.begin(), values.begin(), values.end());
        return s;
    }

    std::vector<StringView> views;
    views.reserve(values.size());
    for (const auto& v : values) {
        views.emplace_back(v);
    }

    const bool local = IsLocal();
    const auto endpoint = RpcEndpoint();
    const auto valueSize = Info().valueSize;
    if (local || !shareMapStoreClient_) {
        s = shareMapStore_->Publish(bucketKey_, valueSize, keys, views);
    } else {
        s = shareMapStoreClient_->Publish(endpoint, bucketKey_, valueSize, keys,
                                          views);
    }
    if (!s.IsOk()) {
        InvalidateLocality();
        std::lock_guard<std::mutex> lock(bufferMutex_);
        localKeys_.insert(localKeys_.begin(), keys.begin(), keys.end());
        localValues_.insert(localValues_.begin(), values.begin(), values.end());
        return s;
    }
    {
        std::lock_guard<std::mutex> lock(localityMutex_);
        info_.currentSize += keys.size();
    }
    return Status::OK();
}

Status Bucket::Find(const std::vector<uint64_t>& keys,
                    std::vector<StringView>& buffers,
                    std::vector<std::shared_ptr<mooncake::BufferHandle>>&
                        bufferHandles) {
    auto s = ResolveLocality();
    if (!s.IsOk()) return s;

    const bool local = IsLocal();
    const auto endpoint = RpcEndpoint();
    if (local || !shareMapStoreClient_) {
        // Local query: ShareMap returns StringViews into ShareObject memory,
        // so no BufferHandle is needed (memory is managed by ShareMap).
        return shareMapStore_->QueryData(bucketKey_, keys, buffers);
    }
    // Remote query via RPC; the returned buffer is held by bufferHandles.
    s = shareMapStoreClient_->QueryData(endpoint, bucketKey_, Info().valueSize,
                                        keys, buffers, bufferHandles);
    if (!s.IsOk()) InvalidateLocality();
    return s;
}

Status Bucket::BuildIndex() {
    auto s = Flush();
    if (!s.IsOk()) return s;

    s = ResolveLocality();
    if (!s.IsOk()) return s;

    const bool local = IsLocal();
    const auto endpoint = RpcEndpoint();
    if (local || !shareMapStoreClient_) {
        return shareMapStore_->BuildIndex(bucketKey_);
    }
    s = shareMapStoreClient_->BuildIndex(endpoint, bucketKey_);
    if (!s.IsOk()) InvalidateLocality();
    return s;
}

BucketInfo Bucket::Info() const {
    std::lock_guard<std::mutex> lock(localityMutex_);
    return info_;
}

uint64_t Bucket::PendingCount() const {
    std::lock_guard<std::mutex> lock(bufferMutex_);
    return localKeys_.size();
}

bool Bucket::IsFull() const {
    std::lock_guard<std::mutex> lock(bufferMutex_);
    return localKeys_.size() >= flushThreshold_;
}

uint64_t Bucket::FlushThreshold() const {
    std::lock_guard<std::mutex> lock(bufferMutex_);
    return flushThreshold_;
}

void Bucket::SetFlushThreshold(uint64_t threshold) {
    std::lock_guard<std::mutex> lock(bufferMutex_);
    flushThreshold_ = threshold;
}

bool Bucket::IsLocal() const {
    std::lock_guard<std::mutex> lock(localityMutex_);
    return isLocal_;
}

std::string Bucket::RpcEndpoint() const {
    std::lock_guard<std::mutex> lock(localityMutex_);
    return info_.rpcEndpoint;
}

}  // namespace embtable
