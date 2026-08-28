#include "vchunk_control_plane.h"

#include "master_client.h"
#include "master_service.h"

namespace mooncake {

namespace {

class RpcReadLeaseGuard {
   public:
    RpcReadLeaseGuard(MasterClient& master, std::string tenant_id,
                      std::string key, std::string lease_id)
        : master_(master),
          tenant_id_(std::move(tenant_id)),
          key_(std::move(key)),
          lease_id_(std::move(lease_id)) {}

    ~RpcReadLeaseGuard() {
        (void)master_.ReleaseVChunkReadLease(tenant_id_, key_, lease_id_);
    }

   private:
    MasterClient& master_;
    std::string tenant_id_;
    std::string key_;
    std::string lease_id_;
};

}  // namespace

tl::expected<VChunkMetadataRecord, ErrorCode>
LocalVChunkControlPlane::PutStart(const TenantId& tenant_id,
                                  const std::string& key, uint64_t total_size,
                                  int64_t now_ms) {
    return master_.VChunkPutStart(tenant_id, key, total_size, false, now_ms);
}

ErrorCode LocalVChunkControlPlane::PutEnd(const TenantId& tenant_id,
                                          const std::string& key,
                                          const std::string& vchunk_id,
                                          int64_t now_ms) {
    return master_.VChunkPutEnd(tenant_id, key, vchunk_id, now_ms);
}

ErrorCode LocalVChunkControlPlane::PutRevoke(const TenantId& tenant_id,
                                             const std::string& key,
                                             const std::string& vchunk_id) {
    return master_.VChunkPutRevoke(tenant_id, key, vchunk_id);
}

tl::expected<VChunkControlPlaneRead, ErrorCode> LocalVChunkControlPlane::Get(
    const TenantId& tenant_id, const std::string& key) {
    auto handle = master_.AcquireVChunkRead(tenant_id, key);
    if (!handle) return tl::unexpected(handle.error());
    auto lifetime = std::make_shared<VChunkMasterManager::ReadHandle>(
        std::move(*handle));
    return VChunkControlPlaneRead{lifetime->record(), std::move(lifetime)};
}

ErrorCode LocalVChunkControlPlane::Remove(const TenantId& tenant_id,
                                          const std::string& key,
                                          int64_t now_ms) {
    return master_.RemoveVChunk(tenant_id, key, now_ms);
}

tl::expected<VChunkMetadataRecord, ErrorCode>
RpcVChunkControlPlane::PutStart(const TenantId& tenant_id,
                                const std::string& key, uint64_t total_size,
                                int64_t now_ms) {
    return master_.VChunkPutStart(tenant_id.value(), key, total_size, now_ms);
}

ErrorCode RpcVChunkControlPlane::PutEnd(const TenantId& tenant_id,
                                        const std::string& key,
                                        const std::string& vchunk_id,
                                        int64_t now_ms) {
    auto result =
        master_.VChunkPutEnd(tenant_id.value(), key, vchunk_id, now_ms);
    return result ? ErrorCode::OK : result.error();
}

ErrorCode RpcVChunkControlPlane::PutRevoke(const TenantId& tenant_id,
                                           const std::string& key,
                                           const std::string& vchunk_id) {
    auto result = master_.VChunkPutRevoke(tenant_id.value(), key, vchunk_id);
    return result ? ErrorCode::OK : result.error();
}

tl::expected<VChunkControlPlaneRead, ErrorCode> RpcVChunkControlPlane::Get(
    const TenantId& tenant_id, const std::string& key) {
    auto record = master_.GetVChunk(tenant_id.value(), key);
    if (!record) return tl::unexpected(record.error());
    auto lifetime = std::make_shared<RpcReadLeaseGuard>(
        master_, tenant_id.value(), key, record->lease_id);
    return VChunkControlPlaneRead{std::move(record->record),
                                  std::move(lifetime)};
}

ErrorCode RpcVChunkControlPlane::Remove(const TenantId& tenant_id,
                                        const std::string& key,
                                        int64_t now_ms) {
    auto result = master_.RemoveVChunk(tenant_id.value(), key, now_ms);
    return result ? ErrorCode::OK : result.error();
}

}  // namespace mooncake
