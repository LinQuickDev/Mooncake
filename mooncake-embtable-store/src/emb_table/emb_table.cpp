#include "emb_table/emb_table.h"

#include <glog/logging.h>
#include <xxhash.h>

#include <unordered_map>

namespace embtable {

EmbTable::EmbTable(const std::string& tableName, uint32_t numBuckets,
                   uint64_t valueSize,
                   std::shared_ptr<ShareMapStore> shareMapStore,
                   std::shared_ptr<mooncake::RealClient> realClient)
    : tableName_(tableName),
      numBuckets_(numBuckets > 0 ? numBuckets : 1),
      valueSize_(valueSize),
      shareMapStore_(std::move(shareMapStore)),
      realClient_(std::move(realClient)),
      meta_(std::make_shared<EmbTableMeta>(realClient_)) {
    buckets_.reserve(numBuckets_);
}

uint32_t EmbTable::RouteToBucket(uint64_t key) const {
    // xxHash-based routing (design doc 8.4 default).
    uint64_t h = XXH64(&key, sizeof(key), 0);
    return static_cast<uint32_t>(h % numBuckets_);
}

Status EmbTable::Init(bool createNew) {
    TableMetaInfo info;
    info.tableKey = tableName_ + "_meta";
    info.tableName = tableName_;
    info.dimSize = valueSize_;
    info.bucketNum = numBuckets_;
    info.hashType = HashFunctionType::kXxHash;
    info.tableCapacity = numBuckets_ * 1024;  // placeholder default
    info.bucketCapacity = 1024;

    Status s;
    if (createNew) {
        s = meta_->CreateTableMeta(info);
    } else {
        s = meta_->QueryTableMeta(info.tableKey, info);
    }
    if (!s.IsOk()) return s;

    // Pre-create bucket objects (local handles; ShareMap created lazily).
    for (uint32_t i = 0; i < numBuckets_; ++i) {
        BucketInfo bi;
        bi.tableKey = info.tableKey;
        bi.bucketKey = tableName_ + "_bucket_" + std::to_string(i);
        bi.valueSize = valueSize_;
        bi.capacity = info.bucketCapacity;
        bi.currentSize = 0;
        buckets_.push_back(std::make_shared<Bucket>(bi, shareMapStore_,
                                                     realClient_));
    }
    return Status::OK();
}

Status EmbTable::Insert(const std::vector<uint64_t>& keys,
                        const std::vector<StringView>& values) {
    if (keys.size() != values.size()) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "keys/values size mismatch");
    }
    // Group by bucket index for batched flush.
    std::unordered_map<uint32_t,
                       std::pair<std::vector<uint64_t>, std::vector<StringView>>>
        grouped;
    for (size_t i = 0; i < keys.size(); ++i) {
        uint32_t bidx = RouteToBucket(keys[i]);
        auto& entry = grouped[bidx];
        entry.first.push_back(keys[i]);
        entry.second.push_back(values[i]);
    }
    for (auto& [bidx, kv] : grouped) {
        auto s = buckets_[bidx]->Insert(kv.first, kv.second);
        if (!s.IsOk()) return s;
    }
    // Flush each touched bucket.
    for (auto& [bidx, kv] : grouped) {
        auto s = buckets_[bidx]->Flush();
        if (!s.IsOk()) return s;
    }
    return Status::OK();
}

Status EmbTable::Find(const std::vector<uint64_t>& keys,
                      std::vector<StringView>& buffers) {
    buffers.clear();
    buffers.resize(keys.size());
    // Group by bucket for batched queries.
    std::unordered_map<uint32_t, std::vector<size_t>> indexGroups;
    for (size_t i = 0; i < keys.size(); ++i) {
        uint32_t bidx = RouteToBucket(keys[i]);
        indexGroups[bidx].push_back(i);
    }
    for (auto& [bidx, indices] : indexGroups) {
        std::vector<uint64_t> bucketKeys;
        bucketKeys.reserve(indices.size());
        for (auto idx : indices) bucketKeys.push_back(keys[idx]);
        std::vector<StringView> bucketVals;
        auto s = buckets_[bidx]->Find(bucketKeys, bucketVals);
        if (!s.IsOk()) return s;
        for (size_t j = 0; j < indices.size(); ++j) {
            buffers[indices[j]] = bucketVals[j];
        }
    }
    return Status::OK();
}

Status EmbTable::BuildIndex() {
    for (auto& bucket : buckets_) {
        auto s = bucket->BuildIndex();
        if (!s.IsOk()) return s;
    }
    return Status::OK();
}

Status EmbTable::Load(const std::vector<std::string>& keyFiles,
                      const std::vector<std::string>& valueFiles,
                      const std::string& format) {
    return Status::Error(ErrorCode::kNotSupported,
                         "EmbTable::Load not implemented yet");
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
