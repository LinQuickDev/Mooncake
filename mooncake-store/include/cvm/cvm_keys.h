#pragma once

#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>

namespace mooncake {
namespace cvm {

// Root prefix for all CVM keys in etcd.
inline constexpr std::string_view kCvmRootPrefix = "/cvm/";

// End key for an etcd range scan over [prefix, PrefixEnd(prefix)).
inline std::string PrefixEnd(std::string prefix) {
    for (int i = static_cast<int>(prefix.size()) - 1; i >= 0; --i) {
        unsigned char c = static_cast<unsigned char>(prefix[i]);
        if (c < 0xFF) {
            prefix[i] = static_cast<char>(c + 1);
            prefix.resize(i + 1);
            return prefix;
        }
    }
    return std::string(1, '\0');
}

// "/cvm/<namespace>/"
inline std::string CvmNamespaceRoot(const std::string& cluster_namespace) {
    return std::string(kCvmRootPrefix) + cluster_namespace + "/";
}

// "/cvm/<namespace>/kv_view/"
inline std::string KvViewPrefix(const std::string& cluster_namespace) {
    return CvmNamespaceRoot(cluster_namespace) + "kv_view/";
}

// "/cvm/<namespace>/kv_view/slot/<slot:05d>"
inline std::string SlotOwnerKey(const std::string& cluster_namespace,
                                uint16_t slot) {
    char buf[16];
    std::snprintf(buf, sizeof(buf), "%05u", static_cast<unsigned>(slot));
    return KvViewPrefix(cluster_namespace) + "slot/" + buf;
}

// "/cvm/<namespace>/slot_meta/"
inline std::string SlotMetadataExportPrefix(
    const std::string& cluster_namespace) {
    return CvmNamespaceRoot(cluster_namespace) + "slot_meta/";
}

// "/cvm/<namespace>/slot_meta/<slot:05d>"
// Binary (struct_pack) value holding the object metadata exported by the
// previous live primary owner during a slot handoff.
inline std::string SlotMetadataExportKey(const std::string& cluster_namespace,
                                         uint16_t slot) {
    char buf[16];
    std::snprintf(buf, sizeof(buf), "%05u", static_cast<unsigned>(slot));
    return SlotMetadataExportPrefix(cluster_namespace) + buf;
}

// "/cvm/<namespace>/segment_view/"
inline std::string SegmentViewPrefix(const std::string& cluster_namespace) {
    return CvmNamespaceRoot(cluster_namespace) + "segment_view/";
}

// "/cvm/<namespace>/segment_view/<segment_id>"
inline std::string SegmentOwnerKey(const std::string& cluster_namespace,
                                   const std::string& segment_id) {
    return SegmentViewPrefix(cluster_namespace) + segment_id;
}

// "/cvm/<namespace>/masters/"
inline std::string MasterRegistrationPrefix(
    const std::string& cluster_namespace) {
    return CvmNamespaceRoot(cluster_namespace) + "masters/";
}

// "/cvm/<namespace>/masters/<master_id>"
inline std::string MasterRegistrationKey(const std::string& cluster_namespace,
                                         const std::string& master_id) {
    return MasterRegistrationPrefix(cluster_namespace) + master_id;
}

// "/cvm/<namespace>/snapshot/"
inline std::string SnapshotPrefix(const std::string& cluster_namespace) {
    return CvmNamespaceRoot(cluster_namespace) + "snapshot/";
}

// "/cvm/<namespace>/snapshot/kv_view"
inline std::string KvViewSnapshotKey(const std::string& cluster_namespace) {
    return SnapshotPrefix(cluster_namespace) + "kv_view";
}

// "/cvm/<namespace>/snapshot/segment_view"
inline std::string SegmentViewSnapshotKey(const std::string& cluster_namespace) {
    return SnapshotPrefix(cluster_namespace) + "segment_view";
}

}  // namespace cvm
}  // namespace mooncake
