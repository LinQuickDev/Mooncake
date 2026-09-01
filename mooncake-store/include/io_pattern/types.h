#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "tenant_id.h"

namespace mooncake::io_pattern {

enum class CacheTier : uint8_t {
    kL0Hbm = 0,
    kL1Host = 1,
    kL2Segment = 2,
    kL3NofSsd = 3,
};

using CacheTierMask = uint8_t;

constexpr CacheTierMask CacheTierBit(CacheTier tier) {
    return static_cast<CacheTierMask>(1U << static_cast<uint8_t>(tier));
}

enum class IoOperation : uint8_t {
    kGet,
    kPut,
    kTierUp,
    kTierDown,
};

enum class CacheLayout : uint8_t {
    kUnknown,
    kLayerFirst,
    kPageFirst,
    kPageFirstDirect,
    kPageBased,
    kHmaMultiGroup,
};

enum class StorageGcState : uint8_t {
    kUnknown,
    kIdle,
    kRunning,
    kStalled,
};

enum class WorkloadType : uint8_t {
    kUnknown,
    kCodeAgent,
    kGenerativeRecommendation,
    kMultiTurnConversation,
    kMixed,
};

struct ObjectRef {
    TenantId tenant_id;
    std::string key;

    bool operator==(const ObjectRef&) const = default;
};

struct InferenceMetrics {
    ObjectRef object;
    std::string session_id;
    CacheLayout layout{CacheLayout::kUnknown};
    uint32_t layout_group{0};
    uint32_t prefix_depth{0};
    uint32_t prefix_fanout{0};
    uint32_t match_length{0};
    uint32_t continuous_prefix_length{0};
    uint32_t token_count{0};
    float recompute_cost{0.0F};
    uint8_t request_priority{0};
};

struct AccessRecord {
    ObjectRef object;
    uint64_t observed_at_ns{0};
    uint64_t block_size{0};
    uint64_t latency_us{0};
    CacheTier tier{CacheTier::kL2Segment};
    IoOperation operation{IoOperation::kGet};
    bool is_hit{false};
};

struct StorageMetric {
    std::string source_id;
    uint64_t observed_at_ns{0};
    CacheTier tier{CacheTier::kL2Segment};
    StorageGcState gc_state{StorageGcState::kUnknown};
    uint64_t read_bandwidth_bytes_per_sec{0};
    uint64_t write_bandwidth_bytes_per_sec{0};
    uint64_t read_latency_us{0};
    uint64_t write_latency_us{0};
    uint64_t used_bytes{0};
    uint64_t capacity_bytes{0};
    uint64_t rpc_latency_us{0};
    float memory_used_ratio{0.0F};
};

struct KeyMetrics {
    ObjectRef object;
    uint64_t last_access_time_ns{0};
    uint64_t access_count_window{0};
    uint64_t idle_time_us{0};
    uint64_t block_size{0};
    uint64_t transfer_eta_us{0};
    uint32_t token_count{0};
    uint32_t prefix_depth{0};
    uint32_t prefix_fanout{0};
    uint32_t match_length{0};
    uint32_t continuous_prefix_length{0};
    uint32_t other_replica_count{0};
    uint32_t write_batch_size{0};
    uint32_t write_frequency{0};
    uint64_t write_object_size{0};
    float recompute_cost{0.0F};
    float overwrite_ratio{0.0F};
    CacheTierMask replica_tiers{0};
    CacheLayout layout{CacheLayout::kUnknown};
    uint32_t layout_group{0};
    uint8_t request_priority{0};
    bool active{false};
    bool pinned{false};
    bool ssd_replica_exists{false};
    bool write_burst{false};
};

struct IoPatternSnapshot {
    uint64_t generated_at_ns{0};
    std::vector<KeyMetrics> keys;
    std::vector<StorageMetric> storage;
};

struct KeyPattern {
    ObjectRef object;
    float confidence{0.0F};
    float frequency_score{0.0F};
    float idle_score{0.0F};
    float prefix_score{0.0F};
    float recompute_score{0.0F};
    float transfer_roi{0.0F};
    bool migration_safe{false};
};

struct PatternResult {
    WorkloadType workload_type{WorkloadType::kUnknown};
    float workload_confidence{0.0F};
    std::vector<KeyPattern> keys;
};

struct PolicyContext {
    IoPatternSnapshot snapshot;
    PatternResult analysis;
};

struct TraceEvent {
    ObjectRef object;
    uint64_t observed_at_ns{0};
    uint32_t match_length{0};
    bool is_hit{false};
};

struct TraceHistory {
    std::vector<TraceEvent> events;
};

enum class PrefetchStrategy : uint8_t {
    kBestEffort,
    kTimeout,
    kWaitComplete,
};

struct PrefetchCandidate {
    ObjectRef object;
    CacheTier source_tier{CacheTier::kL3NofSsd};
    CacheTier target_tier{CacheTier::kL2Segment};
    uint64_t bytes{0};
    float priority{0.0F};
    float confidence{0.0F};
};

struct PrefetchPlan {
    PrefetchStrategy strategy{PrefetchStrategy::kBestEffort};
    uint64_t timeout_us{0};
    std::vector<PrefetchCandidate> candidates;
};

struct EvictionCandidate {
    ObjectRef object;
    uint64_t bytes{0};
    float score{0.0F};
};

struct EvictionPlan {
    CacheTier source_tier{CacheTier::kL0Hbm};
    uint64_t target_bytes{0};
    std::vector<EvictionCandidate> candidates;
};

enum class AdmissionDecision : uint8_t {
    kAdmit,
    kRejectFrequency,
    kRejectWatermark,
    kRejectPrefix,
    kDefer,
};

struct AdmissionResult {
    ObjectRef object;
    CacheTier target_tier{CacheTier::kL2Segment};
    AdmissionDecision decision{AdmissionDecision::kDefer};
    float confidence{0.0F};
};

}  // namespace mooncake::io_pattern
