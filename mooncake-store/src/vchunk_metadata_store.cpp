#include "vchunk_metadata_store.h"

#include <utility>

namespace mooncake {

std::string MakeVChunkMetadataStoreKey(const VChunkMetadataRecord& record) {
    return std::string(kVChunkMetadataNamespace) + "/" + record.tenant_id +
           "/" + record.vchunk_id;
}

ErrorCode InMemoryVChunkMetadataStore::Put(
    const VChunkMetadataRecord& record) {
    std::lock_guard<std::mutex> guard(mutex_);
    records_[MakeVChunkMetadataStoreKey(record)] = record;
    return ErrorCode::OK;
}

ErrorCode InMemoryVChunkMetadataStore::Remove(
    const VChunkMetadataRecord& record) {
    std::lock_guard<std::mutex> guard(mutex_);
    records_.erase(MakeVChunkMetadataStoreKey(record));
    return ErrorCode::OK;
}

tl::expected<std::vector<VChunkMetadataRecord>, ErrorCode>
InMemoryVChunkMetadataStore::List() {
    std::lock_guard<std::mutex> guard(mutex_);
    std::vector<VChunkMetadataRecord> result;
    result.reserve(records_.size());
    for (const auto& [_, record] : records_) {
        result.push_back(record);
    }
    return result;
}

}  // namespace mooncake
