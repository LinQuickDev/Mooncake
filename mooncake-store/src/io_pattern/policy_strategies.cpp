#include "io_pattern/policy_strategies.h"

#include <algorithm>
#include <limits>
#include <unordered_map>

namespace mooncake::io_pattern {
namespace {

const KeyPattern* FindPattern(const ObjectRef& object,
                              const PatternResult& result) {
    const auto it = std::find_if(
        result.keys.begin(), result.keys.end(),
        [&object](const KeyPattern& pattern) { return pattern.object == object; });
    return it == result.keys.end() ? nullptr : &*it;
}

const KeyMetrics* FindMetrics(const ObjectRef& object,
                              const IoPatternSnapshot& snapshot) {
    const auto it = std::find_if(
        snapshot.keys.begin(), snapshot.keys.end(),
        [&object](const KeyMetrics& key) { return key.object == object; });
    return it == snapshot.keys.end() ? nullptr : &*it;
}

// Whether the object already has a replica deeper in the storage ladder, which
// is what makes evicting this copy safe. Compared by TierDepth rather than by
// enum value: kLocalDisk is declared last for wire compatibility but sits
// directly below host memory, so index order is not storage order.
bool HasLowerTierReplica(const KeyMetrics& key, CacheTier tier) {
    const int depth = TierDepth(tier);
    for (const CacheTier candidate : kAllCacheTiers) {
        if (TierDepth(candidate) <= depth) {
            continue;
        }
        if (key.replica_tiers & CacheTierBit(candidate)) {
            return true;
        }
    }
    return false;
}

CacheTier TierDownTarget(CacheTier source, TierDownMode mode) {
    if (source == CacheTier::kL3NofSsd) return CacheTier::kL3NofSsd;
    if (mode == TierDownMode::kSkipHost && source == CacheTier::kL0Hbm) {
        return CacheTier::kL2Segment;
    }
    // Prefix-affinity keeps the immediate next tier as the placement target;
    // callers may co-locate grouped prefixes within that tier. kLocalDisk is not
    // reachable from here: it is appended last for wire compatibility, and the
    // MEMORY -> LOCAL_DISK demotion is selected explicitly by the tier-down
    // driver rather than derived from this ladder.
    return static_cast<CacheTier>(static_cast<uint8_t>(source) + 1);
}

}  // namespace

EvictionPlan ScoreBasedEvictionOps::Evaluate(const PolicyContext& context,
                                              CacheTier tier,
                                              uint64_t target_bytes) const {
    EvictionPlan plan{.source_tier = tier, .target_bytes = target_bytes};
    // The driver owns the action: a tier-down pass selects the same victims an
    // eviction pass would, and only the executor's action differs. Deriving it
    // from target_tier is not possible -- TierDownTarget() returns source+1 for
    // L0/L1/L2 alike, so every candidate would look like a demotion.
    const EvictionAction action = context.tier_down
                                      ? EvictionAction::kTierDown
                                      : EvictionAction::kEvict;
    if (context.tier_down) {
        plan.tier_down_target_bytes = target_bytes;
    }
    // A demotion copies the key down and keeps its MEMORY replica, so a key that
    // already has a replica below this tier has nothing left to copy. It must be
    // excluded or the driver spins forever on the same victims: the key stays an
    // L1 candidate, the scorer prefers lower-replica-backed victims for eviction
    // (see lower_replica_weight below), and every cycle would spend its whole
    // budget re-copying the same keys while the rest of the cold set is never
    // paved. Eviction has no such problem -- it removes the replica, so the next
    // cycle picks fresh victims -- which is why this exclusion is tier-down only.
    const auto is_already_paved = [&context, tier](const KeyMetrics& key) {
        return context.tier_down && HasLowerTierReplica(key, tier);
    };
    uint64_t max_block_size = 0;
    uint32_t max_other_replicas = 0;
    for (const auto& key : context.snapshot.keys) {
        if ((key.replica_tiers & CacheTierBit(tier)) == 0 || key.pinned) {
            continue;
        }
        if (context.min_idle_time_us != 0 &&
            key.idle_time_us < context.min_idle_time_us) {
            continue;
        }
        if (is_already_paved(key)) {
            continue;
        }
        max_block_size = std::max(max_block_size, key.block_size);
        max_other_replicas = std::max(max_other_replicas,
                                      key.other_replica_count);
    }
    for (const auto& key : context.snapshot.keys) {
        if ((key.replica_tiers & CacheTierBit(tier)) == 0 || key.pinned) {
            continue;
        }
        if (context.min_idle_time_us != 0 &&
            key.idle_time_us < context.min_idle_time_us) {
            continue;
        }
        if (is_already_paved(key)) {
            continue;
        }
        const auto* pattern = FindPattern(key.object, context.analysis);
        if (pattern == nullptr) {
            continue;
        }
        const float normalized_block = max_block_size == 0
                                           ? 0.0F
                                           : static_cast<float>(key.block_size) /
                                                 static_cast<float>(max_block_size);
        const float normalized_other_replicas =
            max_other_replicas == 0
                ? 0.0F
                : static_cast<float>(key.other_replica_count) /
                      static_cast<float>(max_other_replicas);
        float score = 0.0F;
        switch (tier) {
            case CacheTier::kL0Hbm:
                score = config_.idle_weight * pattern->idle_score -
                        config_.frequency_weight * pattern->frequency_score -
                        config_.prefix_weight * pattern->prefix_score -
                        config_.recompute_weight * pattern->recompute_score;
                break;
            case CacheTier::kL1Host:
            // Host-local SSD is scored like host memory: idle time dominates,
            // and an existing deeper replica makes reclaiming it safe.
            case CacheTier::kLocalDisk:
            case CacheTier::kL2Segment:
                score = config_.idle_weight * pattern->idle_score -
                        config_.frequency_weight * pattern->frequency_score +
                        config_.lower_replica_weight *
                            (HasLowerTierReplica(key, tier) ? 1.0F : 0.0F) -
                        config_.prefix_weight * pattern->prefix_score -
                        config_.recompute_weight * pattern->recompute_score;
                break;
            case CacheTier::kL3NofSsd:
                score = config_.idle_weight * pattern->idle_score *
                            normalized_block -
                        config_.frequency_weight * pattern->frequency_score -
                        config_.recompute_weight * pattern->recompute_score +
                        config_.other_replica_weight * normalized_other_replicas;
                break;
        }
        plan.candidates.push_back(
            EvictionCandidate{.object = key.object,
                              .bytes = key.block_size,
                              .score = score,
                              .target_tier = TierDownTarget(
                                  tier, config_.tier_down_mode),
                              .action = action});
    }
    std::sort(plan.candidates.begin(), plan.candidates.end(),
              [](const EvictionCandidate& lhs, const EvictionCandidate& rhs) {
                  return lhs.score > rhs.score;
              });
    if (target_bytes == 0) {
        plan.candidates.clear();
        return plan;
    }
    if (config_.max_candidates != 0 &&
        plan.candidates.size() > config_.max_candidates) {
        plan.candidates.resize(config_.max_candidates);
    }
    uint64_t selected_bytes = 0;
    auto end = plan.candidates.begin();
    while (end != plan.candidates.end() && selected_bytes < target_bytes) {
        selected_bytes =
            end->bytes > std::numeric_limits<uint64_t>::max() - selected_bytes
                ? std::numeric_limits<uint64_t>::max()
                : selected_bytes + end->bytes;
        ++end;
    }
    plan.candidates.erase(end, plan.candidates.end());
    return plan;
}

AdmissionResult PrefixMatchAdmissionOps::Evaluate(
    const ObjectRef& object, CacheTier target_tier,
    const PolicyContext& context) const {
    AdmissionResult result{.object = object, .target_tier = target_tier};
    const auto* key = FindMetrics(object, context.snapshot);
    if (key == nullptr) {
        return result;
    }
    if (target_tier == CacheTier::kL0Hbm) {
        result.decision = key->match_length >= config_.hbm_match_length
                              ? AdmissionDecision::kAdmit
                              : AdmissionDecision::kRejectPrefix;
        const auto threshold = std::max(1U, config_.hbm_match_length);
        result.confidence = key->match_length == 0
                                ? 0.0F
                                : std::min(1.0F, static_cast<float>(
                                                     key->match_length) /
                                                     threshold);
        return result;
    }
    result.decision = key->access_count_window >= config_.frequency_threshold
                          ? AdmissionDecision::kAdmit
                          : AdmissionDecision::kRejectFrequency;
    const bool target_over_watermark = std::any_of(
        context.snapshot.storage.begin(), context.snapshot.storage.end(),
        [&](const StorageMetric& metric) {
            return metric.tier == target_tier &&
                   metric.memory_used_ratio >= config_.max_memory_used_ratio;
        });
    if (result.decision == AdmissionDecision::kAdmit &&
        target_over_watermark) {
        result.decision = AdmissionDecision::kRejectWatermark;
    }
    result.confidence =
        result.decision == AdmissionDecision::kAdmit ? 1.0F : 0.0F;
    return result;
}

PrefetchPlan TraceBasedPrefetchOps::Evaluate(
    const PolicyContext& context, const TraceHistory& trace) const {
    PrefetchPlan plan{.strategy = config_.strategy,
                      .timeout_us = config_.timeout_us};
    std::unordered_map<ObjectRef, bool, ObjectRefHash> seen;
    for (const auto& event : trace.events) {
        if (!event.is_hit ||
            event.match_length <= config_.match_length_threshold ||
            seen.contains(event.object)) {
            continue;
        }
        const auto* key = FindMetrics(event.object, context.snapshot);
        if (key == nullptr) {
            continue;
        }
        PrefetchCandidate candidate;
        candidate.object = event.object;
        candidate.bytes = key->block_size;
        const auto* pattern = FindPattern(event.object, context.analysis);
        candidate.confidence = pattern == nullptr ? 0.0F : pattern->confidence;
        if (candidate.confidence < config_.minimum_confidence) {
            continue;
        }
        // Only one hop is real: the store promotes a LOCAL_DISK replica into
        // host memory. There is no L3 -> L2 or L2 -> L1 mover, so reporting a
        // fabricated kL2Segment target made every candidate look executable
        // while the handler could only ever promote a local-disk source -- a
        // NoF-only object therefore produced a plan the executor had to reject.
        if (key->replica_tiers & CacheTierBit(CacheTier::kLocalDisk)) {
            candidate.source_tier = CacheTier::kLocalDisk;
            candidate.target_tier = CacheTier::kL1Host;
        } else {
            continue;
        }
        candidate.priority = static_cast<float>(event.match_length);
        plan.candidates.push_back(candidate);
        seen.emplace(event.object, true);
        if (config_.max_candidates != 0 &&
            plan.candidates.size() >= config_.max_candidates) {
            break;
        }
    }
    return plan;
}

}  // namespace mooncake::io_pattern
