#pragma once

#include <cstddef>
#include <chrono>
#include <functional>
#include <mutex>
#include <vector>
#include <thread>
#include <condition_variable>
#include <unordered_map>

#include "types.h"

namespace mooncake::io_pattern {

struct MetricBatch {
    std::vector<InferenceMetrics> inference;
    std::vector<AccessRecord> accesses;
    std::vector<StorageMetric> storage;
};

using MetricBatchSink = std::function<bool(const MetricBatch&)>;

class MetricBatchTransport {
   public:
    virtual ~MetricBatchTransport() = default;
    virtual bool Send(const MetricBatch& batch) = 0;
};

// Bounded, non-blocking metric batching. Transport and RPC ownership remain
// with the supplied sink.
class IoPatternReporter final {
   public:
    explicit IoPatternReporter(size_t capacity, MetricBatchSink sink,
                               size_t per_tenant_capacity = 0);
    ~IoPatternReporter();

    void Start();
    void Stop();

    bool Enqueue(InferenceMetrics metrics);
    bool EnqueueAccess(AccessRecord record);
    bool EnqueueStorage(StorageMetric metric);
    bool Flush();

    size_t pending() const;
    uint64_t dropped() const;
    uint64_t reported() const;
    void UpdateLoad(float memory_used_ratio, uint64_t rpc_latency_us);
    std::chrono::milliseconds RecommendedFlushInterval() const;

   private:
    bool EnqueueImpl(std::function<void(MetricBatch&)> append,
                     const TenantId& tenant);
    std::chrono::milliseconds RecommendedFlushIntervalLocked() const;

    const size_t capacity_;
    const MetricBatchSink sink_;
    const size_t per_tenant_capacity_;
    mutable std::mutex mutex_;
    MetricBatch batch_;
    uint64_t dropped_{0};
    uint64_t reported_{0};
    std::condition_variable condition_;
    std::thread worker_;
    bool running_{false};
    float memory_used_ratio_{0.0F};
    uint64_t rpc_latency_us_{0};
    std::unordered_map<TenantId, size_t, TenantIdHash> tenant_pending_;
};

}  // namespace mooncake::io_pattern
