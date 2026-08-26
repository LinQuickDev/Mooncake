#include "vchunk_client.h"

namespace mooncake {

VChunkClient::VChunkClient(bool enabled, MasterService& master,
                           VChunkDataPlane& data_plane,
                           VChunkLegacyPath& legacy,
                           std::chrono::milliseconds timeout, NowMs now_ms)
    : enabled_(enabled),
      master_(master),
      data_plane_(data_plane),
      legacy_(legacy),
      timeout_(timeout),
      now_ms_(std::move(now_ms)) {}

ErrorCode VChunkClient::Put(const TenantId& tenant_id, const std::string& key,
                            const void* source, size_t length) {
    if (!enabled_) {
        return legacy_.Put(tenant_id, key, source, length);
    }
    if (!source || length == 0 || timeout_.count() <= 0 || !now_ms_) {
        return ErrorCode::INVALID_PARAMS;
    }
    auto created = master_.VChunkPutStart(tenant_id, key, length, false,
                                           now_ms_());
    if (!created) {
        return created.error();
    }
    const auto deadline = Clock::now() + timeout_;
    const auto transfer =
        data_plane_.Write(*created, source, length, deadline);
    if (transfer != ErrorCode::OK) {
        const auto revoke = master_.VChunkPutRevoke(
            tenant_id, key, created->vchunk_id);
        return revoke == ErrorCode::OK ? transfer : revoke;
    }
    const auto end = master_.VChunkPutEnd(tenant_id, key, created->vchunk_id,
                                          now_ms_());
    if (end != ErrorCode::OK) {
        master_.VChunkPutRevoke(tenant_id, key, created->vchunk_id);
    }
    return end;
}

ErrorCode VChunkClient::Get(const TenantId& tenant_id, const std::string& key,
                            void* destination, size_t length) {
    if (!enabled_) {
        return legacy_.Get(tenant_id, key, destination, length);
    }
    if (!destination || timeout_.count() <= 0) {
        return ErrorCode::INVALID_PARAMS;
    }
    auto read = master_.AcquireVChunkRead(tenant_id, key);
    if (!read) {
        return read.error();
    }
    if (read->record().total_size != length) {
        return ErrorCode::INVALID_PARAMS;
    }
    return data_plane_.Read(read->record(), destination, length,
                            Clock::now() + timeout_);
}

ErrorCode VChunkClient::Remove(const TenantId& tenant_id,
                               const std::string& key) {
    if (!enabled_) {
        return legacy_.Remove(tenant_id, key);
    }
    if (!now_ms_) {
        return ErrorCode::INVALID_PARAMS;
    }
    return master_.RemoveVChunk(tenant_id, key, now_ms_());
}

}  // namespace mooncake
