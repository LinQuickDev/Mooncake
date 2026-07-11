#include "embtable/share_object/share_map.h"

#include <glog/logging.h>
#include <unordered_map>

namespace embtable {

ShareMap::ShareMap(const std::string& bucketKey, uint64_t valueSize,
                   std::shared_ptr<mooncake::RealClient> realClient,
                   uint64_t shareObjectSize)
    : bucketKey_(bucketKey),
      valueSize_(valueSize),
      realClient_(realClient) {
    if (valueSize_ == 0) valueSize_ = 1;
    keyVec_ = std::make_unique<VectorObject>(bucketKey + "_keys",
                                             sizeof(uint64_t), realClient_,
                                             shareObjectSize);
    valueVec_ = std::make_unique<VectorObject>(bucketKey + "_values",
                                               valueSize_, realClient_,
                                               shareObjectSize);
    indexObj_ = std::make_unique<IndexObject>(bucketKey + "_idx", realClient_);
    meta_ = std::make_unique<ShareMapMeta>(bucketKey, realClient_);
}

Status ShareMap::Insert(const std::vector<uint64_t>& keys,
                        const std::vector<StringView>& values) {
    if (published_.load(std::memory_order_acquire)) {
        return Status::Error(ErrorCode::kIndexBuilt,
                             "ShareMap is read-only after BuildIndex");
    }
    if (keys.size() != values.size()) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "keys/values size mismatch");
    }
    std::unique_lock<std::shared_mutex> lock(rwMutex_);
    for (size_t i = 0; i < keys.size(); ++i) {
        if (values[i].size() != valueSize_) {
            return Status::Error(ErrorCode::kInvalidArgument,
                                 "value size != valueSize");
        }
        uint64_t kIdx = 0, vIdx = 0;
        auto s = keyVec_->Append(&keys[i], sizeof(uint64_t), kIdx);
        if (!s.IsOk()) return s;
        s = valueVec_->Append(values[i].data(), values[i].size(), vIdx);
        if (!s.IsOk()) return s;
        if (kIdx != vIdx) {
            return Status::Error(ErrorCode::kInternal,
                                 "key/value index divergence");
        }
        size_.fetch_add(1, std::memory_order_acq_rel);
    }
    return Status::OK();
}

Status ShareMap::linearLookup(const std::vector<uint64_t>& keys,
                              std::vector<StringView>& buffers) const {
    // Build an in-memory map from key -> index by scanning the key vector.
    uint64_t total = size_.load(std::memory_order_acquire);
    std::unordered_map<uint64_t, uint64_t> index;
    index.reserve(total);
    for (uint64_t i = 0; i < total; ++i) {
        StringView kv;
        auto s = keyVec_->Get(i, kv);
        if (!s.IsOk()) return s;
        uint64_t k;
        std::memcpy(&k, kv.data(), sizeof(uint64_t));
        index.emplace(k, i);
    }
    buffers.clear();
    buffers.reserve(keys.size());
    for (auto k : keys) {
        auto it = index.find(k);
        if (it == index.end()) {
            buffers.emplace_back();
        } else {
            StringView v;
            auto s = valueVec_->Get(it->second, v);
            if (!s.IsOk()) {
                buffers.emplace_back();
            } else {
                buffers.push_back(v);
            }
        }
    }
    return Status::OK();
}

Status ShareMap::Lookup(const std::vector<uint64_t>& keys,
                        std::vector<StringView>& buffers) const {
    std::shared_lock<std::shared_mutex> lock(rwMutex_);
    if (!published_.load(std::memory_order_acquire)) {
        return linearLookup(keys, buffers);
    }
    buffers.clear();
    buffers.reserve(keys.size());
    for (auto k : keys) {
        uint64_t idx = 0;
        auto s = indexObj_->Lookup(k, idx);
        if (!s.IsOk()) {
            buffers.emplace_back();
            continue;
        }
        StringView v;
        s = valueVec_->Get(idx, v);
        if (!s.IsOk()) {
            buffers.emplace_back();
        } else {
            buffers.push_back(v);
        }
    }
    return Status::OK();
}

Status ShareMap::BuildIndex() {
    std::unique_lock<std::shared_mutex> lock(rwMutex_);
    if (published_.load(std::memory_order_acquire)) {
        return Status::Error(ErrorCode::kIndexBuilt, "already published");
    }
    uint64_t total = size_.load(std::memory_order_acquire);
    // 1. Export all keys into a local vector.
    std::vector<uint64_t> keys;
    auto s = keyVec_->ExportToVector(keys);
    if (!s.IsOk()) return s;
    if (keys.size() != total) {
        return Status::Error(ErrorCode::kInternal, "key count mismatch");
    }
    // 2. Build the PHF.
    s = indexObj_->Build(keys);
    if (!s.IsOk()) return s;
    // 3. Publish the backing ShareObjects (keys, values, index, meta).
    s = keyVec_->PublishAll();
    if (!s.IsOk()) return s;
    s = valueVec_->PublishAll();
    if (!s.IsOk()) return s;
    s = indexObj_->Export();
    if (!s.IsOk()) return s;
    // 4. Record meta and publish.
    ObjectInfo keyInfo{bucketKey_ + "__keys", sizeof(uint64_t), 0, "keys"};
    ObjectInfo valInfo{bucketKey_ + "_values", valueSize_, 0, "values"};
    ObjectInfo idxInfo{bucketKey_ + "_idx", 0, 0, "index"};
    meta_->SetValueSize(valueSize_);
    meta_->SetTotalSize(total);
    meta_->AddObjectInfo(keyInfo);
    meta_->AddObjectInfo(valInfo);
    meta_->AddObjectInfo(idxInfo);
    s = meta_->Serialize();
    if (!s.IsOk()) return s;
    published_.store(true, std::memory_order_release);
    return Status::OK();
}

Status ShareMap::Import() {
    std::unique_lock<std::shared_mutex> lock(rwMutex_);
    auto s = meta_->Deserialize();
    if (!s.IsOk()) return s;
    valueSize_ = meta_->GetValueSize();
    // Reconstruct key/value/index from Mooncake Store.
    // Note: VectorObject currently does not expose Import; for cross-node
    // reads the PHF index and value-vector are the primary path. We import
    // the index here so Lookup works; key/vector Import is a future
    // enhancement tracked separately.
    s = indexObj_->Import();
    if (!s.IsOk()) {
        LOG(WARNING) << "IndexObject Import failed: " << s.msg()
                     << " (linear scan fallback will be unavailable)";
    }
    size_.store(meta_->GetTotalSize(), std::memory_order_release);
    published_.store(true, std::memory_order_release);
    return Status::OK();
}

}  // namespace embtable
