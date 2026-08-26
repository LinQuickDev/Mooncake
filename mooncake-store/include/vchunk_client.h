#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>

#include <ylt/util/tl/expected.hpp>

#include "master_service.h"
#include "tenant_id.h"
#include "types.h"
#include "vchunk_metadata.h"

namespace mooncake {

class VChunkDataPlane {
   public:
    virtual ~VChunkDataPlane() = default;
    virtual ErrorCode Write(const VChunkMetadataRecord& record,
                            const void* source, size_t length,
                            std::chrono::steady_clock::time_point deadline) = 0;
    virtual ErrorCode Read(const VChunkMetadataRecord& record, void* destination,
                           size_t length,
                           std::chrono::steady_clock::time_point deadline) = 0;
};

class VChunkLegacyPath {
   public:
    virtual ~VChunkLegacyPath() = default;
    virtual ErrorCode Put(const TenantId&, const std::string&, const void*,
                          size_t) = 0;
    virtual ErrorCode Get(const TenantId&, const std::string&, void*, size_t) = 0;
    virtual ErrorCode Remove(const TenantId&, const std::string&) = 0;
};

// In-process control-plane adapter used until the isolated vchunk RPC is added.
// It preserves the wire compatibility of the community client path.
class VChunkClient {
   public:
    using Clock = std::chrono::steady_clock;
    using NowMs = std::function<int64_t()>;

    VChunkClient(bool enabled, MasterService& master, VChunkDataPlane& data_plane,
                 VChunkLegacyPath& legacy, std::chrono::milliseconds timeout,
                 NowMs now_ms);

    ErrorCode Put(const TenantId& tenant_id, const std::string& key,
                  const void* source, size_t length);
    ErrorCode Get(const TenantId& tenant_id, const std::string& key,
                  void* destination, size_t length);
    ErrorCode Remove(const TenantId& tenant_id, const std::string& key);

   private:
    bool enabled_;
    MasterService& master_;
    VChunkDataPlane& data_plane_;
    VChunkLegacyPath& legacy_;
    std::chrono::milliseconds timeout_;
    NowMs now_ms_;
};

}  // namespace mooncake
