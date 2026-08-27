#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "types.h"
#include "vchunk_metadata.h"

namespace mooncake {

inline constexpr char kVChunkMetadataNamespace[] = "/mooncake/vchunk/v1";

std::string MakeVChunkMetadataStoreKey(const VChunkMetadataRecord& record);

class VChunkMetadataStore {
   public:
    virtual ~VChunkMetadataStore() = default;
    virtual ErrorCode Put(const VChunkMetadataRecord& record) = 0;
    virtual ErrorCode Remove(const VChunkMetadataRecord& record) = 0;
    virtual tl::expected<std::vector<VChunkMetadataRecord>, ErrorCode> List() = 0;
    virtual bool IsPersistent() const = 0;
};

class InMemoryVChunkMetadataStore final : public VChunkMetadataStore {
   public:
    ErrorCode Put(const VChunkMetadataRecord& record) override;
    ErrorCode Remove(const VChunkMetadataRecord& record) override;
    tl::expected<std::vector<VChunkMetadataRecord>, ErrorCode> List() override;
    bool IsPersistent() const override { return false; }

   private:
    std::mutex mutex_;
    std::unordered_map<std::string, VChunkMetadataRecord> records_;
};

// Persistent metadata backend for distributed validation. A scoped object
// index is stored separately from the vchunk-id record so concurrent masters
// cannot both create the same logical object.
class EtcdVChunkMetadataStore final : public VChunkMetadataStore {
   public:
    EtcdVChunkMetadataStore(std::string endpoints, VChunkConfig config,
                            std::string cluster_id = "default");

    ErrorCode Put(const VChunkMetadataRecord& record) override;
    ErrorCode Remove(const VChunkMetadataRecord& record) override;
    tl::expected<std::vector<VChunkMetadataRecord>, ErrorCode> List() override;
    bool IsPersistent() const override { return true; }

    ErrorCode connection_error() const { return connection_error_; }

   private:
    std::string endpoints_;
    VChunkConfig config_;
    std::string namespace_prefix_;
    ErrorCode connection_error_{ErrorCode::OK};
};

}  // namespace mooncake
