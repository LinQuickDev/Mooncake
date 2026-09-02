#pragma once

#include <mutex>
#include <unordered_map>
#include <memory>
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

    mutable std::mutex mutex_;
    Config config_;
    std::shared_ptr<IoPatternReporter> reporter_;
    uint64_t dropped_{0};
    bool degraded_{false};
    std::unordered_map<ObjectRef, KeyMetrics, ObjectRefHash> key_metrics_;
    std::unordered_map<ObjectRef, uint64_t, ObjectRefHash> write_counts_;
    std::unordered_map<ObjectRef, uint64_t, ObjectRefHash> overwrite_counts_;
    std::unordered_map<TenantId, size_t, TenantIdHash> tenant_key_counts_;
    std::unordered_map<StorageMetricKey, StorageMetric, StorageMetricKeyHash>
        storage_metrics_;
};

}  // namespace mooncake::io_pattern
