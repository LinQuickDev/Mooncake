#include "io_pattern/collector_impl.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <utility>

namespace mooncake::io_pattern {
namespace {
uint64_t NowNs() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                     std::chrono::steady_clock::now().time_since_epoch())
                                     .count());
}
}

void IoPatternCollectorImpl::ApplyAccessWindow(const ObjectRef& object,
                                               uint64_t now_ns,
                                               KeyMetrics& metrics) const {
    metrics.access_count_window = 0;
    metrics.write_frequency = 0;
    metrics.overwrite_ratio = 0.0F;
    metrics.write_batch_size = 0;
    metrics.write_burst = false;
    const auto it = access_windows_.find(object);
    if (it == access_windows_.end()) return;

    const uint64_t cutoff = config_.access_window_ns != 0 &&
                                    now_ns > config_.access_window_ns
                                ? now_ns - config_.access_window_ns
                                : 0;
    uint64_t writes = 0;
    uint64_t overwrites = 0;
    for (const auto& bucket : it->second) {
        if (config_.access_window_ns != 0 && bucket.observed_at_ns < cutoff) {
            continue;
        }
        metrics.access_count_window += bucket.access_count;
        writes += bucket.write_count;
        overwrites += bucket.overwrite_count;
        metrics.write_batch_size =
            std::max(metrics.write_batch_size, bucket.max_write_batch_size);
    }
    metrics.write_frequency =
        static_cast<uint32_t>(std::min<uint64_t>(
            writes, std::numeric_limits<uint32_t>::max()));
    if (writes != 0) {
        metrics.overwrite_ratio = static_cast<float>(overwrites) /
                                  static_cast<float>(writes);
    }
    metrics.write_burst = metrics.write_batch_size >= 16;
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
    const uint64_t observed_at_ns =
        record.observed_at_ns == 0
            ? (config_.now_ns ? config_.now_ns() : NowNs())
            : record.observed_at_ns;
    auto& window = access_windows_[object];
    const uint64_t bucket_ns = std::max<uint64_t>(1, config_.access_bucket_ns);
    const uint64_t bucket_timestamp =
        observed_at_ns - (observed_at_ns % bucket_ns);
    const uint64_t reference_ns =
        window.empty() ? observed_at_ns
                       : std::max(observed_at_ns,
                                  window.back().observed_at_ns);
    AccessWindowBucket bucket{.observed_at_ns = bucket_timestamp,
                              .access_count = 1};
    if (record.operation == IoOperation::kPut) {
        bucket.write_count = 1;
        bucket.overwrite_count = record.overwrite ? 1 : 0;
        bucket.max_write_batch_size = record.write_batch_size;
    }
    const auto position = std::lower_bound(
        window.begin(), window.end(), bucket_timestamp,
        [](const AccessWindowBucket& existing, uint64_t timestamp) {
            return existing.observed_at_ns < timestamp;
        });
    if (position != window.end() &&
        position->observed_at_ns == bucket_timestamp) {
        position->access_count += bucket.access_count;
        position->write_count += bucket.write_count;
        position->overwrite_count += bucket.overwrite_count;
        position->max_write_batch_size = std::max(
            position->max_write_batch_size, bucket.max_write_batch_size);
    } else {
        window.insert(position, bucket);
    }
    const uint64_t cutoff = config_.access_window_ns != 0 &&
                                    reference_ns > config_.access_window_ns
                                ? reference_ns - config_.access_window_ns
                                : 0;
    while (!window.empty() && config_.access_window_ns != 0 &&
           window.front().observed_at_ns < cutoff) {
        window.pop_front();
    }
    while (config_.max_access_buckets_per_key != 0 &&
           window.size() > config_.max_access_buckets_per_key) {
        window.pop_front();
    }
    value.last_access_time_ns =
        std::max(value.last_access_time_ns, observed_at_ns);
    value.block_size = std::max(value.block_size, record.block_size);
    value.replica_tiers |= CacheTierBit(record.tier);
    value.active = value.active || record.is_hit;
    if (record.operation == IoOperation::kPut) {
        value.write_object_size = std::max(value.write_object_size,
                                           record.block_size);
    }
    ApplyAccessWindow(object, observed_at_ns, value);
}

void IoPatternCollectorImpl::RecordStorageMetric(const StorageMetric& metric) {
    std::lock_guard lock(mutex_);
    if (reporter_) {
        reporter_->UpdateLoad(metric.memory_used_ratio, metric.rpc_latency_us);
        if (!reporter_->EnqueueStorage(metric)) ++dropped_;
    }
    StorageMetricKey key{metric.source_id, metric.tier};
    auto it = storage_metrics_.find(key);
    if (it == storage_metrics_.end() ||
        metric.observed_at_ns >= it->second.observed_at_ns) {
        storage_metrics_[std::move(key)] = metric;
    }
}

void IoPatternCollectorImpl::MergeSnapshot(const IoPatternSnapshot& snapshot) {
    std::lock_guard lock(mutex_);
    const uint64_t received_at_ns = config_.now_ns ? config_.now_ns() : NowNs();
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
        auto& window = access_windows_[metrics.object];
        window.clear();
        window.push_back({.observed_at_ns = received_at_ns,
                          .access_count = metrics.access_count_window,
                          .write_count = metrics.write_frequency,
                          .overwrite_count = static_cast<uint64_t>(
                              metrics.overwrite_ratio *
                              static_cast<float>(metrics.write_frequency)),
                          .max_write_batch_size = metrics.write_batch_size});
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
    snapshot.generated_at_ns = config_.now_ns ? config_.now_ns() : NowNs();
    snapshot.keys.reserve(key_metrics_.size());
    for (const auto& [object, metrics] : key_metrics_) {
        auto copy = metrics;
        ApplyAccessWindow(object, snapshot.generated_at_ns, copy);
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

void IoPatternCollectorImpl::StopReports() {
    std::shared_ptr<IoPatternReporter> reporter;
    {
        std::lock_guard lock(mutex_);
        reporter = reporter_;
    }
    if (reporter) reporter->Stop();
}

}  // namespace mooncake::io_pattern
