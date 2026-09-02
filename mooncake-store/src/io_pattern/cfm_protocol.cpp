#include "io_pattern/cfm_protocol.h"

#include <cstring>
#include <limits>
#include <type_traits>

namespace mooncake::io_pattern {
namespace {

constexpr char kWireVersion[] = "CFM2";
constexpr size_t kMaxWireStringBytes = 16 * 1024 * 1024;
constexpr size_t kMaxWirePayloadBytes = 64 * 1024 * 1024;
constexpr uint32_t kMaxWireRecords = 1'000'000;

template <typename T>
void Append(std::string& out, T value) {
    static_assert(std::is_trivially_copyable_v<T>);
    const auto* bytes = reinterpret_cast<const char*>(&value);
    out.append(bytes, sizeof(value));
}

template <typename T>
bool Read(const std::string& input, size_t& offset, T& value) {
    static_assert(std::is_trivially_copyable_v<T>);
    if (input.size() - offset < sizeof(value)) return false;
    std::memcpy(&value, input.data() + offset, sizeof(value));
    offset += sizeof(value);
    return true;
}

void AppendString(std::string& out, const std::string& value) {
    const auto size = static_cast<uint32_t>(value.size());
    Append(out, size);
    out.append(value);
}

bool ReadString(const std::string& input, size_t& offset, std::string& value) {
    uint32_t size = 0;
    if (!Read(input, offset, size) || size > kMaxWireStringBytes ||
        input.size() - offset < size) {
        return false;
    }
    value.assign(input.data() + offset, size);
    offset += size;
    return true;
}

void AppendObject(std::string& out, const ObjectRef& object) {
    AppendString(out, object.tenant_id.value());
    AppendString(out, object.key);
}

bool ReadObject(const std::string& input, size_t& offset, ObjectRef& object) {
    std::string tenant;
    if (!ReadString(input, offset, tenant) || !ReadString(input, offset, object.key)) {
        return false;
    }
    object.tenant_id = TenantId(std::move(tenant));
    return true;
}

template <typename Enum>
void AppendEnum(std::string& out, Enum value) {
    Append(out, static_cast<uint8_t>(value));
}

template <typename Enum>
bool ReadEnum(const std::string& input, size_t& offset, Enum& value) {
    uint8_t raw = 0;
    if (!Read(input, offset, raw)) return false;
    value = static_cast<Enum>(raw);
    return true;
}

void AppendPrefetchPlan(std::string& out, const PrefetchPlan& plan) {
    AppendEnum(out, plan.strategy);
    Append(out, plan.timeout_us);
    Append(out, static_cast<uint32_t>(plan.candidates.size()));
    for (const auto& candidate : plan.candidates) {
        AppendObject(out, candidate.object);
        AppendEnum(out, candidate.source_tier);
        AppendEnum(out, candidate.target_tier);
        Append(out, candidate.bytes);
        Append(out, candidate.priority);
        Append(out, candidate.confidence);
    }
}

bool ReadPrefetchPlan(const std::string& input, size_t& offset, PrefetchPlan& plan) {
    uint32_t count = 0;
    if (!ReadEnum(input, offset, plan.strategy) || !Read(input, offset, plan.timeout_us) ||
        !Read(input, offset, count) || count > kMaxWireRecords) {
        return false;
    }
    plan.candidates.clear();
    plan.candidates.reserve(count);
    for (uint32_t index = 0; index < count; ++index) {
        PrefetchCandidate candidate;
        if (!ReadObject(input, offset, candidate.object) ||
            !ReadEnum(input, offset, candidate.source_tier) ||
            !ReadEnum(input, offset, candidate.target_tier) ||
            !Read(input, offset, candidate.bytes) ||
            !Read(input, offset, candidate.priority) ||
            !Read(input, offset, candidate.confidence)) {
            return false;
        }
        plan.candidates.push_back(std::move(candidate));
    }
    return true;
}

void AppendStorageMetric(std::string& out, const StorageMetric& storage) {
    AppendString(out, storage.source_id);
    Append(out, storage.observed_at_ns);
    AppendEnum(out, storage.tier);
    AppendEnum(out, storage.gc_state);
    Append(out, storage.read_bandwidth_bytes_per_sec);
    Append(out, storage.write_bandwidth_bytes_per_sec);
    Append(out, storage.read_latency_us);
    Append(out, storage.write_latency_us);
    Append(out, storage.used_bytes);
    Append(out, storage.capacity_bytes);
    Append(out, storage.rpc_latency_us);
    Append(out, storage.memory_used_ratio);
}

bool ReadStorageMetric(const std::string& input, size_t& offset,
                       StorageMetric& storage) {
    return ReadString(input, offset, storage.source_id) &&
           Read(input, offset, storage.observed_at_ns) &&
           ReadEnum(input, offset, storage.tier) &&
           ReadEnum(input, offset, storage.gc_state) &&
           Read(input, offset, storage.read_bandwidth_bytes_per_sec) &&
           Read(input, offset, storage.write_bandwidth_bytes_per_sec) &&
           Read(input, offset, storage.read_latency_us) &&
           Read(input, offset, storage.write_latency_us) &&
           Read(input, offset, storage.used_bytes) &&
           Read(input, offset, storage.capacity_bytes) &&
           Read(input, offset, storage.rpc_latency_us) &&
           Read(input, offset, storage.memory_used_ratio);
}

void AppendHeader(std::string& out, char type) {
    out.append(kWireVersion, sizeof(kWireVersion) - 1);
    out.push_back(type);
}

bool ReadHeader(const std::string& input, size_t& offset, char& type) {
    if (input.size() < sizeof(kWireVersion) || input.size() > kMaxWirePayloadBytes ||
        input.compare(0, sizeof(kWireVersion) - 1, kWireVersion) != 0) {
        return false;
    }
    offset = sizeof(kWireVersion) - 1;
    return Read(input, offset, type);
}

}  // namespace

std::string CfmBinaryCodec::EncodeSnapshot(const IoPatternSnapshot& snapshot) const {
    std::string output;
    AppendHeader(output, 'S');
    Append(output, snapshot.generated_at_ns);
    Append(output, static_cast<uint32_t>(snapshot.keys.size()));
    for (const auto& key : snapshot.keys) {
        AppendObject(output, key.object);
        AppendString(output, key.session_id);
        Append(output, key.last_access_time_ns);
        Append(output, key.access_count_window);
        Append(output, key.idle_time_us);
        Append(output, key.block_size);
        Append(output, key.transfer_eta_us);
        Append(output, key.token_count);
        Append(output, key.prefix_depth);
        Append(output, key.prefix_fanout);
        Append(output, key.match_length);
        Append(output, key.continuous_prefix_length);
        Append(output, key.other_replica_count);
        Append(output, key.write_batch_size);
        Append(output, key.write_frequency);
        Append(output, key.write_object_size);
        Append(output, key.recompute_cost);
        Append(output, key.overwrite_ratio);
        Append(output, key.replica_tiers);
        AppendEnum(output, key.layout);
        Append(output, key.layout_group);
        Append(output, key.request_priority);
        Append(output, key.active);
        Append(output, key.pinned);
        Append(output, key.ssd_replica_exists);
        Append(output, key.write_burst);
    }
    Append(output, static_cast<uint32_t>(snapshot.storage.size()));
    for (const auto& storage : snapshot.storage) {
        AppendStorageMetric(output, storage);
    }
    return output;
}

std::string CfmBinaryCodec::EncodePrefetch(const PrefetchPlan& plan) const {
    std::string output;
    AppendHeader(output, 'P');
    AppendPrefetchPlan(output, plan);
    return output;
}

std::string CfmBinaryCodec::EncodeMetricBatch(const MetricBatch& batch) const {
    std::string output;
    AppendHeader(output, 'M');
    Append(output, static_cast<uint32_t>(batch.inference.size()));
    for (const auto& metric : batch.inference) {
        AppendObject(output, metric.object);
        AppendString(output, metric.session_id);
        Append(output, metric.prefix_depth);
        Append(output, metric.prefix_fanout);
        Append(output, metric.match_length);
        Append(output, metric.continuous_prefix_length);
        Append(output, metric.token_count);
        Append(output, metric.recompute_cost);
        Append(output, metric.request_priority);
        AppendEnum(output, metric.layout);
        Append(output, metric.layout_group);
    }
    Append(output, static_cast<uint32_t>(batch.accesses.size()));
    for (const auto& access : batch.accesses) {
        AppendObject(output, access.object);
        Append(output, access.observed_at_ns);
        Append(output, access.block_size);
        Append(output, access.latency_us);
        AppendEnum(output, access.tier);
        AppendEnum(output, access.operation);
        Append(output, access.is_hit);
        Append(output, access.write_batch_size);
        Append(output, access.overwrite);
    }
    Append(output, static_cast<uint32_t>(batch.storage.size()));
    for (const auto& storage : batch.storage) {
        AppendStorageMetric(output, storage);
    }
    return output;
}

std::string CfmBinaryCodec::EncodePolicy(const PolicyCommand& command) const {
    std::string output;
    if (const auto* eviction = std::get_if<EvictionPlan>(&command)) {
        AppendHeader(output, 'E');
        AppendEnum(output, eviction->source_tier);
        Append(output, eviction->target_bytes);
        Append(output, static_cast<uint32_t>(eviction->candidates.size()));
        for (const auto& candidate : eviction->candidates) {
            AppendObject(output, candidate.object);
            Append(output, candidate.bytes);
            Append(output, candidate.score);
            AppendEnum(output, candidate.target_tier);
        }
    } else if (const auto* prefetch = std::get_if<PrefetchPlan>(&command)) {
        AppendHeader(output, 'P');
        AppendPrefetchPlan(output, *prefetch);
    } else {
        const auto& admission = std::get<AdmissionResult>(command);
        AppendHeader(output, 'A');
        AppendObject(output, admission.object);
        AppendEnum(output, admission.target_tier);
        AppendEnum(output, admission.decision);
        Append(output, admission.confidence);
    }
    return output;
}

std::optional<PolicyCommand> CfmBinaryCodec::DecodePolicy(
    const std::string& payload) const {
    size_t offset = 0;
    char type = 0;
    if (!ReadHeader(payload, offset, type)) return std::nullopt;
    if (type == 'P') {
        PrefetchPlan plan;
        return ReadPrefetchPlan(payload, offset, plan) && offset == payload.size()
                   ? std::optional<PolicyCommand>(std::move(plan))
                   : std::nullopt;
    }
    if (type == 'E') {
        EvictionPlan plan;
        uint32_t count = 0;
        if (!ReadEnum(payload, offset, plan.source_tier) ||
            !Read(payload, offset, plan.target_bytes) ||
            !Read(payload, offset, count) || count > kMaxWireRecords) {
            return std::nullopt;
        }
        plan.candidates.reserve(count);
        for (uint32_t index = 0; index < count; ++index) {
            EvictionCandidate candidate;
            if (!ReadObject(payload, offset, candidate.object) ||
                !Read(payload, offset, candidate.bytes) ||
                !Read(payload, offset, candidate.score) ||
                !ReadEnum(payload, offset, candidate.target_tier)) {
                return std::nullopt;
            }
            plan.candidates.push_back(std::move(candidate));
        }
        return offset == payload.size()
                   ? std::optional<PolicyCommand>(std::move(plan))
                   : std::nullopt;
    }
    if (type == 'A') {
        AdmissionResult result;
        if (!ReadObject(payload, offset, result.object) ||
            !ReadEnum(payload, offset, result.target_tier) ||
            !ReadEnum(payload, offset, result.decision) ||
            !Read(payload, offset, result.confidence) || offset != payload.size()) {
            return std::nullopt;
        }
        return result;
    }
    return std::nullopt;
}

std::optional<IoPatternSnapshot> CfmBinaryCodec::DecodeSnapshot(
    const std::string& payload) const {
    size_t offset = 0;
    char type = 0;
    IoPatternSnapshot snapshot;
    uint32_t key_count = 0;
    if (!ReadHeader(payload, offset, type) || type != 'S' ||
        !Read(payload, offset, snapshot.generated_at_ns) ||
        !Read(payload, offset, key_count) || key_count > kMaxWireRecords) {
        return std::nullopt;
    }
    snapshot.keys.reserve(key_count);
    for (uint32_t index = 0; index < key_count; ++index) {
        KeyMetrics key;
        if (!ReadObject(payload, offset, key.object) ||
            !ReadString(payload, offset, key.session_id) ||
            !Read(payload, offset, key.last_access_time_ns) ||
            !Read(payload, offset, key.access_count_window) ||
            !Read(payload, offset, key.idle_time_us) ||
            !Read(payload, offset, key.block_size) ||
            !Read(payload, offset, key.transfer_eta_us) ||
            !Read(payload, offset, key.token_count) ||
            !Read(payload, offset, key.prefix_depth) ||
            !Read(payload, offset, key.prefix_fanout) ||
            !Read(payload, offset, key.match_length) ||
            !Read(payload, offset, key.continuous_prefix_length) ||
            !Read(payload, offset, key.other_replica_count) ||
            !Read(payload, offset, key.write_batch_size) ||
            !Read(payload, offset, key.write_frequency) ||
            !Read(payload, offset, key.write_object_size) ||
            !Read(payload, offset, key.recompute_cost) ||
            !Read(payload, offset, key.overwrite_ratio) ||
            !Read(payload, offset, key.replica_tiers) ||
            !ReadEnum(payload, offset, key.layout) ||
            !Read(payload, offset, key.layout_group) ||
            !Read(payload, offset, key.request_priority) ||
            !Read(payload, offset, key.active) || !Read(payload, offset, key.pinned) ||
            !Read(payload, offset, key.ssd_replica_exists) ||
            !Read(payload, offset, key.write_burst)) {
            return std::nullopt;
        }
        snapshot.keys.push_back(std::move(key));
    }
    uint32_t storage_count = 0;
    if (!Read(payload, offset, storage_count) || storage_count > kMaxWireRecords) {
        return std::nullopt;
    }
    snapshot.storage.reserve(storage_count);
    for (uint32_t index = 0; index < storage_count; ++index) {
        StorageMetric storage;
        if (!ReadStorageMetric(payload, offset, storage)) return std::nullopt;
        snapshot.storage.push_back(std::move(storage));
    }
    return offset == payload.size() ? std::optional<IoPatternSnapshot>(std::move(snapshot))
                                    : std::nullopt;
}

std::optional<MetricBatch> CfmBinaryCodec::DecodeMetricBatch(
    const std::string& payload) const {
    size_t offset = 0;
    char type = 0;
    MetricBatch batch;
    uint32_t count = 0;
    if (!ReadHeader(payload, offset, type) || type != 'M' ||
        !Read(payload, offset, count) || count > kMaxWireRecords) {
        return std::nullopt;
    }
    batch.inference.reserve(count);
    for (uint32_t index = 0; index < count; ++index) {
        InferenceMetrics metric;
        if (!ReadObject(payload, offset, metric.object) ||
            !ReadString(payload, offset, metric.session_id) ||
            !Read(payload, offset, metric.prefix_depth) ||
            !Read(payload, offset, metric.prefix_fanout) ||
            !Read(payload, offset, metric.match_length) ||
            !Read(payload, offset, metric.continuous_prefix_length) ||
            !Read(payload, offset, metric.token_count) ||
            !Read(payload, offset, metric.recompute_cost) ||
            !Read(payload, offset, metric.request_priority) ||
            !ReadEnum(payload, offset, metric.layout) ||
            !Read(payload, offset, metric.layout_group)) return std::nullopt;
        batch.inference.push_back(std::move(metric));
    }
    if (!Read(payload, offset, count) || count > kMaxWireRecords) return std::nullopt;
    batch.accesses.reserve(count);
    for (uint32_t index = 0; index < count; ++index) {
        AccessRecord access;
        if (!ReadObject(payload, offset, access.object) ||
            !Read(payload, offset, access.observed_at_ns) ||
            !Read(payload, offset, access.block_size) ||
            !Read(payload, offset, access.latency_us) ||
            !ReadEnum(payload, offset, access.tier) ||
            !ReadEnum(payload, offset, access.operation) ||
            !Read(payload, offset, access.is_hit) ||
            !Read(payload, offset, access.write_batch_size) ||
            !Read(payload, offset, access.overwrite)) return std::nullopt;
        batch.accesses.push_back(std::move(access));
    }
    if (!Read(payload, offset, count) || count > kMaxWireRecords) return std::nullopt;
    batch.storage.reserve(count);
    for (uint32_t index = 0; index < count; ++index) {
        StorageMetric storage;
        if (!ReadStorageMetric(payload, offset, storage)) return std::nullopt;
        batch.storage.push_back(std::move(storage));
    }
    return offset == payload.size() ? std::optional<MetricBatch>(std::move(batch))
                                    : std::nullopt;
}

}  // namespace mooncake::io_pattern
