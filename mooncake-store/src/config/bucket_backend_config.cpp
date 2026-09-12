#include "config/bucket_backend_config.h"

#include <glog/logging.h>

#include <string>

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

bool BucketBackendConfig::Validate() const {
    if (bucket_keys_limit <= 0) {
        LOG(ERROR) << "BucketBackendConfig: bucket_keys_limit must > 0";
        return false;
    }
    if (bucket_size_limit <= 0) {
        LOG(ERROR) << "BucketBackendConfig: bucket_size_limit must > 0";
        return false;
    }
    return true;
}

BucketBackendConfig BucketBackendConfig::FromEnvironment() {
    BucketBackendConfig config;
    using Variables = BucketBackendEnvironmentVariables;

    config.disable_ssd_eviction =
        Environ::ReadOr(Variables::MOONCAKE_OFFLOAD_DISABLE_SSD_EVICTION,
                        config.disable_ssd_eviction);
    config.gc_enable = Environ::ReadOr(
        Variables::MOONCAKE_OFFLOAD_BUCKET_GC_ENABLE, config.gc_enable);
    config.gc_interval_ms =
        Environ::ReadOr(Variables::MOONCAKE_OFFLOAD_BUCKET_GC_INTERVAL_MS,
                        config.gc_interval_ms);
    config.gc_deleted_ratio =
        Environ::ReadOr(Variables::MOONCAKE_OFFLOAD_BUCKET_GC_DELETED_RATIO,
                        config.gc_deleted_ratio);
    config.gc_high_watermark_ratio = Environ::ReadOr(
        Variables::MOONCAKE_OFFLOAD_BUCKET_GC_HIGH_WATERMARK_RATIO,
        config.gc_high_watermark_ratio);
    config.gc_max_buckets_per_round = Environ::ReadOr(
        Variables::MOONCAKE_OFFLOAD_BUCKET_GC_MAX_BUCKETS_PER_ROUND,
        config.gc_max_buckets_per_round);
    config.gc_merge_enable =
        Environ::ReadOr(Variables::MOONCAKE_OFFLOAD_BUCKET_GC_MERGE_ENABLE,
                        config.gc_merge_enable);

    config.bucket_keys_limit =
        Environ::ReadOr(Variables::MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT,
                        config.bucket_keys_limit);

    config.bucket_size_limit =
        Environ::ReadOr(Variables::MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES,
                        config.bucket_size_limit);

    config.max_total_size = Environ::ReadOr(
        Variables::MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE,
        Environ::ReadOr(Variables::MOONCAKE_BUCKET_MAX_TOTAL_SIZE,
                        config.max_total_size));

    config.max_physical_bytes =
        Environ::ReadOr(Variables::MOONCAKE_OFFLOAD_BUCKET_MAX_PHYSICAL_BYTES,
                        config.max_physical_bytes);

    config.disk_scan_cache_ms =
        Environ::ReadOr(Variables::MOONCAKE_OFFLOAD_BUCKET_DISK_SCAN_CACHE_MS,
                        config.disk_scan_cache_ms);

    const std::string policy = Environ::ReadOr(
        Variables::MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY,
        Environ::ReadOr(Variables::MOONCAKE_BUCKET_EVICTION_POLICY,
                        std::string{"fifo"}));
    if (policy == "fifo") {
        config.eviction_policy = BucketEvictionPolicy::FIFO;
    } else if (policy == "lru") {
        config.eviction_policy = BucketEvictionPolicy::LRU;
    } else {
        config.eviction_policy = BucketEvictionPolicy::NONE;
    }

    return config;
}

}  // namespace mooncake
