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
    return Status::OK();
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
    // Inserts always go to the local write buffer; they are flushed later
    // when the buffer is full (design doc 4.1.4).
    localKeys_.insert(localKeys_.end(), keys.begin(), keys.end());
    for (const auto& v : values) {
        localValues_.emplace_back(v.data(), v.size());
    }
    wouldFlush = IsFull();
    return Status::OK();
}

Status Bucket::Insert(const std::vector<uint64_t>& keys,
                      const std::vector<StringView>& values) {
    bool unused = false;
    return Insert(keys, values, unused);
}

Status Bucket::Flush() {
    if (localKeys_.empty()) return Status::OK();

    // Resolve locality to decide whether to flush locally or remotely.
    auto s = ResolveLocality();
    if (!s.IsOk()) return s;

    std::vector<StringView> views;
    views.reserve(localValues_.size());
    for (const auto& v : localValues_) {
        views.emplace_back(v);
    }

    if (isLocal_ || !shareMapStoreClient_) {
        s = shareMapStore_->Publish(bucketKey_, info_.valueSize, localKeys_,
                                    views);
    } else {
        s = shareMapStoreClient_->Publish(info_.rpcEndpoint, bucketKey_,
                                          info_.valueSize, localKeys_, views);
    }
    if (!s.IsOk()) return s;
    info_.currentSize += localKeys_.size();
    localKeys_.clear();
    localValues_.clear();
    return Status::OK();
}

Status Bucket::Find(const std::vector<uint64_t>& keys,
                    std::vector<StringView>& buffers,
                    std::vector<std::shared_ptr<mooncake::BufferHandle>>&
                        bufferHandles) {
    auto s = ResolveLocality();
    if (!s.IsOk()) return s;

    if (isLocal_ || !shareMapStoreClient_) {
        // Local query: ShareMap returns StringViews into ShareObject memory,
        // so no BufferHandle is needed (memory is managed by ShareMap).
        return shareMapStore_->QueryData(bucketKey_, keys, buffers);
    }
    // Remote query via RPC; the returned buffer is held by bufferHandles.
    return shareMapStoreClient_->QueryData(info_.rpcEndpoint, bucketKey_,
                                           info_.valueSize, keys, buffers,
                                           bufferHandles);
}

Status Bucket::BuildIndex() {
    auto s = Flush();
    if (!s.IsOk()) return s;

    s = ResolveLocality();
    if (!s.IsOk()) return s;

    if (isLocal_ || !shareMapStoreClient_) {
        return shareMapStore_->BuildIndex(bucketKey_);
    }
    return shareMapStoreClient_->BuildIndex(info_.rpcEndpoint, bucketKey_);
}

}  // namespace embtable
