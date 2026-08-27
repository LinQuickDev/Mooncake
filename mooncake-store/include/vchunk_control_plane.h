#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include <ylt/util/tl/expected.hpp>

#include "tenant_id.h"
#include "types.h"
#include "vchunk_metadata.h"

namespace mooncake {

class MasterClient;
class MasterService;

struct VChunkControlPlaneRead {
    VChunkMetadataRecord record;
    // Local control planes retain the allocator-backed read handle here.
    // RPC control planes leave it empty; remote lifetime is governed by the
    // master's object lifecycle.
    std::shared_ptr<void> lifetime;
};

class VChunkControlPlane {
   public:
    virtual ~VChunkControlPlane() = default;

    virtual tl::expected<VChunkMetadataRecord, ErrorCode> PutStart(
        const TenantId& tenant_id, const std::string& key, uint64_t total_size,
        int64_t now_ms) = 0;
    virtual ErrorCode PutEnd(const TenantId& tenant_id, const std::string& key,
                             const std::string& vchunk_id,
                             int64_t now_ms) = 0;
    virtual ErrorCode PutRevoke(const TenantId& tenant_id,
                                const std::string& key,
                                const std::string& vchunk_id) = 0;
    virtual tl::expected<VChunkControlPlaneRead, ErrorCode> Get(
        const TenantId& tenant_id, const std::string& key) = 0;
    virtual ErrorCode Remove(const TenantId& tenant_id,
                             const std::string& key, int64_t now_ms) = 0;
};

class LocalVChunkControlPlane final : public VChunkControlPlane {
   public:
    explicit LocalVChunkControlPlane(MasterService& master) : master_(master) {}

    tl::expected<VChunkMetadataRecord, ErrorCode> PutStart(
        const TenantId&, const std::string&, uint64_t, int64_t) override;
    ErrorCode PutEnd(const TenantId&, const std::string&, const std::string&,
                     int64_t) override;
    ErrorCode PutRevoke(const TenantId&, const std::string&,
                        const std::string&) override;
    tl::expected<VChunkControlPlaneRead, ErrorCode> Get(
        const TenantId&, const std::string&) override;
    ErrorCode Remove(const TenantId&, const std::string&, int64_t) override;

   private:
    MasterService& master_;
};

class RpcVChunkControlPlane final : public VChunkControlPlane {
   public:
    explicit RpcVChunkControlPlane(MasterClient& master) : master_(master) {}

    tl::expected<VChunkMetadataRecord, ErrorCode> PutStart(
        const TenantId&, const std::string&, uint64_t, int64_t) override;
    ErrorCode PutEnd(const TenantId&, const std::string&, const std::string&,
                     int64_t) override;
    ErrorCode PutRevoke(const TenantId&, const std::string&,
                        const std::string&) override;
    tl::expected<VChunkControlPlaneRead, ErrorCode> Get(
        const TenantId&, const std::string&) override;
    ErrorCode Remove(const TenantId&, const std::string&, int64_t) override;

   private:
    MasterClient& master_;
};

}  // namespace mooncake
