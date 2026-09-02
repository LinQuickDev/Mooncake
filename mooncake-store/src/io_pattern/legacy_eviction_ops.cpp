#include "io_pattern/legacy_eviction_ops.h"

#include <algorithm>

namespace mooncake::io_pattern {

EvictionPlan LegacyEvictionOps::Evaluate(const PolicyContext& context,
                                         CacheTier tier,
                                         uint64_t target_bytes) const {
    EvictionPlan plan{.source_tier = tier, .target_bytes = target_bytes};
    if (!strategy_ || target_bytes == 0) return plan;
    std::lock_guard lock(mutex_);
    for (const auto& key : context.snapshot.keys) {
        if (key.replica_tiers & CacheTierBit(tier)) {
            strategy_->AddKey(key.object.tenant_id.MakeScopedKey(key.object.key));
        }
    }
    uint64_t bytes = 0;
    while (bytes < target_bytes) {
        const auto scoped = strategy_->EvictKey();
        if (scoped.empty()) break;
        auto [tenant, key] = TenantId::ParseScopedKey(scoped);
        auto it = std::find_if(context.snapshot.keys.begin(), context.snapshot.keys.end(),
                               [&](const KeyMetrics& value) {
                                   return value.object.tenant_id == tenant &&
                                          value.object.key == key;
                               });
        if (it == context.snapshot.keys.end()) continue;
        plan.candidates.push_back({it->object, it->block_size, 0.0F});
        bytes += it->block_size;
    }
    return plan;
}

}  // namespace mooncake::io_pattern
