#include "io_pattern/collector_impl.h"

#include <algorithm>
#include <chrono>
#include <utility>

namespace mooncake::io_pattern {
namespace {
uint64_t NowNs() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                     std::chrono::steady_clock::now().time_since_epoch())
                                     .count());
}
}

void IoPatternCollectorImpl::ReportInferenceMetrics(
    const InferenceMetrics& metrics) {
    std::lock_guard lock(mutex_);
    if (reporter_ && !reporter_->Enqueue(metrics)) ++dropped_;
    if (!key_metrics_.contains(metrics.object) && config_.max_total_keys != 0 &&
        key_metrics_.size() >= config_.max_total_keys) {
        degraded_ = true;
        ++dropped_;
        return;
    }
    if (!key_metrics_.contains(metrics.object) &&
        config_.max_keys_per_tenant != 0 &&
        tenant_key_counts_[metrics.object.tenant_id] >=
            config_.max_keys_per_tenant) {
        ++dropped_;
        return;
    }
    if (!key_metrics_.contains(metrics.object))
        ++tenant_key_counts_[metrics.object.tenant_id];
    auto& value = key_metrics_[metrics.object];
    value.object = metrics.object;
    value.session_id = metrics.session_id;
    value.layout = metrics.layout;
    value.layout_group = metrics.layout_group;
    value.prefix_depth = metrics.prefix_depth;
    value.prefix_fanout = metrics.prefix_fanout;
    value.match_length = metrics.match_length;
    value.continuous_prefix_length = metrics.continuous_prefix_length;
    value.token_count = metrics.token_count;
    value.recompute_cost = metrics.recompute_cost;
    value.request_priority = metrics.request_priority;
}

void IoPatternCollectorImpl::RecordAccess(const std::string& key,
                                          const AccessRecord& record) {
    std::lock_guard lock(mutex_);
    if (reporter_ && !reporter_->EnqueueAccess(record)) ++dropped_;
    ObjectRef object = record.object;
    if (!key.empty()) object.key = key;
    if (!key_metrics_.contains(object) && config_.max_total_keys != 0 &&
        key_metrics_.size() >= config_.max_total_keys) {
        degraded_ = true;
        ++dropped_;
        return;
    }
    if (!key_metrics_.contains(object) && config_.max_keys_per_tenant != 0 &&
        tenant_key_counts_[object.tenant_id] >= config_.max_keys_per_tenant) {
        ++dropped_;
        return;
    }
    if (!key_metrics_.contains(object)) ++tenant_key_counts_[object.tenant_id];
    auto& value = key_metrics_[object];
    value.object = object;
    ++value.access_count_window;
    value.last_access_time_ns =
        std::max(value.last_access_time_ns, record.observed_at_ns);
    value.block_size = std::max(value.block_size, record.block_size);
    value.replica_tiers |= CacheTierBit(record.tier);
    value.active = value.active || record.is_hit;
    if (record.operation == IoOperation::kPut) {
        ++write_counts_[object];
        if (record.overwrite) ++overwrite_counts_[object];
        value.write_frequency =
            static_cast<uint32_t>(std::min<uint64_t>(write_counts_[object],
                                                     UINT32_MAX));
        value.write_batch_size =
            std::max(value.write_batch_size, record.write_batch_size);
        value.write_object_size = std::max(value.write_object_size,
                                           record.block_size);
        value.overwrite_ratio =
            static_cast<float>(overwrite_counts_[object]) /
            static_cast<float>(write_counts_[object]);
        value.write_burst = record.write_batch_size >= 16;
    }
}

void IoPatternCollectorImpl::RecordStorageMetric(const StorageMetric& metric) {
    std::lock_guard lock(mutex_);
    if (reporter_ && !reporter_->EnqueueStorage(metric)) ++dropped_;
    StorageMetricKey key{metric.source_id, metric.tier};
    auto it = storage_metrics_.find(key);
    if (it == storage_metrics_.end() ||
        metric.observed_at_ns >= it->second.observed_at_ns) {
        storage_metrics_[std::move(key)] = metric;
    }
}

void IoPatternCollectorImpl::MergeSnapshot(const IoPatternSnapshot& snapshot) {
    std::lock_guard lock(mutex_);
    for (const auto& metrics : snapshot.keys) {
        if (!key_metrics_.contains(metrics.object) &&
            config_.max_total_keys != 0 &&
            key_metrics_.size() >= config_.max_total_keys) {
            degraded_ = true;
            ++dropped_;
            continue;
        }
        if (!key_metrics_.contains(metrics.object) &&
            config_.max_keys_per_tenant != 0 &&
            tenant_key_counts_[metrics.object.tenant_id] >=
                config_.max_keys_per_tenant) {
            ++dropped_;
            continue;
        }
        if (!key_metrics_.contains(metrics.object)) {
            ++tenant_key_counts_[metrics.object.tenant_id];
        }
        key_metrics_[metrics.object] = metrics;
    }
    for (const auto& metric : snapshot.storage) {
        StorageMetricKey key{metric.source_id, metric.tier};
        auto it = storage_metrics_.find(key);
        if (it == storage_metrics_.end() ||
            metric.observed_at_ns >= it->second.observed_at_ns) {
            storage_metrics_[std::move(key)] = metric;
        }
    }
}

IoPatternSnapshot IoPatternCollectorImpl::GetSnapshot() const {
    std::lock_guard lock(mutex_);
    IoPatternSnapshot snapshot;
    snapshot.generated_at_ns = NowNs();
    snapshot.keys.reserve(key_metrics_.size());
    for (const auto& [object, metrics] : key_metrics_) {
        (void)object;
        auto copy = metrics;
        if (copy.last_access_time_ns != 0 &&
            snapshot.generated_at_ns > copy.last_access_time_ns) {
            copy.idle_time_us =
                (snapshot.generated_at_ns - copy.last_access_time_ns) / 1000;
        }
        snapshot.keys.push_back(std::move(copy));
    }
    snapshot.storage.reserve(storage_metrics_.size());
    for (const auto& [key, metric] : storage_metrics_) {
        (void)key;
        snapshot.storage.push_back(metric);
    }
    std::sort(snapshot.keys.begin(), snapshot.keys.end(),
              [](const KeyMetrics& a, const KeyMetrics& b) {
                  if (a.object.tenant_id != b.object.tenant_id)
                      return a.object.tenant_id < b.object.tenant_id;
                  return a.object.key < b.object.key;
              });
    return snapshot;
}

uint64_t IoPatternCollectorImpl::dropped() const {
    std::lock_guard lock(mutex_);
    return dropped_;
}

bool IoPatternCollectorImpl::degraded() const {
    std::lock_guard lock(mutex_);
    return degraded_;
}

bool IoPatternCollectorImpl::FlushReports() {
    std::shared_ptr<IoPatternReporter> reporter;
    {
        std::lock_guard lock(mutex_);
        reporter = reporter_;
    }
    return !reporter || reporter->Flush();
}

}  // namespace mooncake::io_pattern
