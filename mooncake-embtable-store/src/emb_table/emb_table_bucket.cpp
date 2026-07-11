#include "embtable/emb_table/emb_table_bucket.h"

#include <glog/logging.h>

namespace embtable {

Bucket::Bucket(BucketInfo info,
               std::shared_ptr<ShareMapStore> shareMapStore,
               std::shared_ptr<mooncake::RealClient> realClient)
    : info_(std::move(info)),
      bucketKey_(info_.bucketKey),
      shareMapStore_(std::move(shareMapStore)),
      realClient_(std::move(realClient)) {}

Status Bucket::Insert(const std::vector<uint64_t>& keys,
                      const std::vector<StringView>& values) {
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
    localKeys_.insert(localKeys_.end(), keys.begin(), keys.end());
    for (const auto& v : values) {
        localValues_.emplace_back(v.data(), v.size());
    }
    return Status::OK();
}

Status Bucket::Flush() {
    if (localKeys_.empty()) return Status::OK();
    std::vector<StringView> views;
    views.reserve(localValues_.size());
    for (const auto& v : localValues_) {
        views.emplace_back(v);
    }
    auto s = shareMapStore_->Publish(bucketKey_, info_.valueSize, localKeys_,
                                     views);
    if (!s.IsOk()) return s;
    info_.currentSize += localKeys_.size();
    localKeys_.clear();
    localValues_.clear();
    return Status::OK();
}

Status Bucket::Find(const std::vector<uint64_t>& keys,
                    std::vector<StringView>& buffers) {
    return shareMapStore_->QueryData(bucketKey_, keys, buffers);
}

Status Bucket::BuildIndex() {
    auto s = Flush();
    if (!s.IsOk()) return s;
    return shareMapStore_->BuildIndex(bucketKey_);
}

}  // namespace embtable
