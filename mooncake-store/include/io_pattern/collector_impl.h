#pragma once

#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "collector.h"
#include "reporter.h"

namespace mooncake::io_pattern {

// Production collector that aggregates observations by tenant and object.
// It owns only metrics state; reporting to CFM is intentionally external.
class IoPatternCollectorImpl final : public IoPatternCollector {
   public:
    struct Config {
        size_t max_keys_per_tenant{0};
        size_t max_total_keys{0};
        uint64_t access_window_ns{60'000'000'000ULL};
        uint64_t access_bucket_ns{1'000'000'000ULL};
        size_t max_access_buckets_per_key{64};
        std::function<uint64_t()> now_ns;
    };

    explicit IoPatternCollectorImpl(Config config = {},
                                    std::shared_ptr<IoPatternReporter> reporter =
                                        nullptr)
        : config_(config), reporter_(std::move(reporter)) {}
    void ReportInferenceMetrics(const InferenceMetrics& metrics) override;
    void RecordAccess(const std::string& key,
                      const AccessRecord& record) override;
    void RecordStorageMetric(const StorageMetric& metric) override;
    // Ingests a CFM snapshot without replaying it through the asynchronous
    // reporter. The sender is already the reporting side of that pipeline.
    void MergeSnapshot(const IoPatternSnapshot& snapshot);
    IoPatternSnapshot GetSnapshot() const override;
    uint64_t dropped() const;
    bool degraded() const;
    bool FlushReports();

   private:
    struct StorageMetricKey {
        std::string source_id;
        CacheTier tier{CacheTier::kL2Segment};
        bool operator==(const StorageMetricKey&) const = default;
    };
    struct StorageMetricKeyHash {
        size_t operator()(const StorageMetricKey& key) const noexcept {
            return std::hash<std::string>{}(key.source_id) ^
                   (static_cast<size_t>(key.tier) << 1);
        }
    };
    struct AccessWindowBucket {
        uint64_t observed_at_ns{0};
        uint64_t access_count{0};
        uint64_t write_count{0};
        uint64_t overwrite_count{0};
        uint32_t max_write_batch_size{0};
    };

    void ApplyAccessWindow(const ObjectRef& object, uint64_t now_ns,
                           KeyMetrics& metrics) const;

    mutable std::mutex mutex_;
    Config config_;
    std::shared_ptr<IoPatternReporter> reporter_;
    uint64_t dropped_{0};
    bool degraded_{false};
    std::unordered_map<ObjectRef, KeyMetrics, ObjectRefHash> key_metrics_;
    std::unordered_map<ObjectRef, std::deque<AccessWindowBucket>, ObjectRefHash>
        access_windows_;
    std::unordered_map<TenantId, size_t, TenantIdHash> tenant_key_counts_;
    std::unordered_map<StorageMetricKey, StorageMetric, StorageMetricKeyHash>
        storage_metrics_;
};

}  // namespace mooncake::io_pattern
