#include "emb_table/emb_table_bucket.h"

#include <glog/logging.h>

namespace embtable {

Bucket::Bucket(BucketInfo info,
               std::shared_ptr<ShareMapStore> shareMapStore,
               std::shared_ptr<mooncake::RealClient> realClient,
               std::shared_ptr<ShareMapStoreClient> shareMapStoreClient,
               const std::string& localHostname)
    : info_(std::move(info)),
      bucketKey_(info_.bucketKey),
      shareMapStore_(std::move(shareMapStore)),
      realClient_(std::move(realClient)),
      shareMapStoreClient_(std::move(shareMapStoreClient)),
      localHostname_(localHostname) {}

Status Bucket::resolveLocality() {
    if (localityResolved_) return Status::OK();
    localityResolved_ = true;

    // If rpcEndpoint is set in BucketInfo, use it directly.
    if (!info_.rpcEndpoint.empty()) {
        // If endpoint matches local hostname, treat as local.
        auto host = ShareMapStoreClient::ExtractHostname(info_.rpcEndpoint);
        if (host == localHostname_ || host == "127.0.0.1" ||
            host == "localhost" || host.empty()) {
            isLocal_ = true;
        } else {
            isLocal_ = false;
        }
        return Status::OK();
    }

    // Otherwise, query the bucket's ShareObject key via Mooncake Client
    // to determine where the replica lives (design doc 4.3).
    if (shareMapStoreClient_) {
        std::string ownerHost;
        bool isLocal =
            shareMapStoreClient_->IsBucketLocal(bucketKey_, ownerHost);
        isLocal_ = isLocal;
        if (!isLocal && !ownerHost.empty()) {
            // Use default ShareMapStore RPC port from design doc.
            info_.rpcEndpoint = ownerHost + ":50055";
        }
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
    auto s = resolveLocality();
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
    auto s = resolveLocality();
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

    s = resolveLocality();
    if (!s.IsOk()) return s;

    if (isLocal_ || !shareMapStoreClient_) {
        return shareMapStore_->BuildIndex(bucketKey_);
    }
    return shareMapStoreClient_->BuildIndex(info_.rpcEndpoint, bucketKey_);
}

}  // namespace embtable
