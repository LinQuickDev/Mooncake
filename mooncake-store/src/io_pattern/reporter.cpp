#include "io_pattern/reporter.h"

#include <utility>

namespace mooncake::io_pattern {

IoPatternReporter::IoPatternReporter(size_t capacity, MetricBatchSink sink,
                                     size_t per_tenant_capacity)
    : capacity_(capacity),
      sink_(std::move(sink)),
      per_tenant_capacity_(per_tenant_capacity) {}

IoPatternReporter::~IoPatternReporter() { Stop(); }

void IoPatternReporter::Start() {
    std::lock_guard lock(mutex_);
    if (running_) return;
    running_ = true;
    worker_ = std::thread([this] {
        std::unique_lock lock(mutex_);
        while (running_) {
            const auto interval = RecommendedFlushIntervalLocked();
            condition_.wait_for(lock, interval, [this] { return !running_; });
            if (!running_) break;
            lock.unlock();
            Flush();
            lock.lock();
        }
    });
}

void IoPatternReporter::Stop() {
    {
        std::lock_guard lock(mutex_);
        if (!running_) return;
        running_ = false;
    }
    condition_.notify_all();
    if (worker_.joinable()) worker_.join();
    Flush();
}

bool IoPatternReporter::Enqueue(InferenceMetrics metrics) {
    const auto tenant = metrics.object.tenant_id;
    return EnqueueImpl(
        [value = std::move(metrics)](MetricBatch& batch) {
            batch.inference.push_back(value);
        },
        tenant);
}

bool IoPatternReporter::EnqueueAccess(AccessRecord record) {
    const auto tenant = record.object.tenant_id;
    return EnqueueImpl(
        [value = std::move(record)](MetricBatch& batch) {
            batch.accesses.push_back(value);
        },
        tenant);
}

bool IoPatternReporter::EnqueueStorage(StorageMetric metric) {
    return EnqueueImpl([value = std::move(metric)](MetricBatch& batch) {
        batch.storage.push_back(value);
    }, TenantId::Default());
}

bool IoPatternReporter::EnqueueImpl(std::function<void(MetricBatch&)> append,
                                    const TenantId& tenant) {
    std::lock_guard lock(mutex_);
    const size_t size = batch_.inference.size() + batch_.accesses.size() +
                        batch_.storage.size();
    if (!sink_ || size >= capacity_) {
        ++dropped_;
        return false;
    }
    if (per_tenant_capacity_ != 0 &&
        tenant_pending_[tenant] >= per_tenant_capacity_) {
        ++dropped_;
        return false;
    }
    append(batch_);
    ++tenant_pending_[tenant];
    condition_.notify_one();
    return true;
}

bool IoPatternReporter::Flush() {
    MetricBatch outgoing;
    {
        std::lock_guard lock(mutex_);
        if (batch_.inference.empty() && batch_.accesses.empty() &&
            batch_.storage.empty()) {
            return true;
        }
        outgoing = std::move(batch_);
        batch_ = {};
        tenant_pending_.clear();
    }
    if (!sink_(outgoing)) {
        std::lock_guard lock(mutex_);
        ++dropped_;
        return false;
    }
    std::lock_guard lock(mutex_);
    reported_ += outgoing.inference.size() + outgoing.accesses.size() +
                 outgoing.storage.size();
    return true;
}

size_t IoPatternReporter::pending() const {
    std::lock_guard lock(mutex_);
    return batch_.inference.size() + batch_.accesses.size() +
           batch_.storage.size();
}

uint64_t IoPatternReporter::dropped() const {
    std::lock_guard lock(mutex_);
    return dropped_;
}

uint64_t IoPatternReporter::reported() const {
    std::lock_guard lock(mutex_);
    return reported_;
}

std::chrono::milliseconds IoPatternReporter::RecommendedFlushInterval() const {
    std::lock_guard lock(mutex_);
    return RecommendedFlushIntervalLocked();
}

void IoPatternReporter::UpdateLoad(float memory_used_ratio,
                                   uint64_t rpc_latency_us) {
    std::lock_guard lock(mutex_);
    memory_used_ratio_ = memory_used_ratio;
    rpc_latency_us_ = rpc_latency_us;
    condition_.notify_one();
}

std::chrono::milliseconds
IoPatternReporter::RecommendedFlushIntervalLocked() const {
    if (memory_used_ratio_ >= 0.95F || rpc_latency_us_ > 100'000) {
        return std::chrono::milliseconds(1000);
    }
    if (memory_used_ratio_ >= 0.80F) return std::chrono::milliseconds(500);
    if (memory_used_ratio_ >= 0.50F) return std::chrono::milliseconds(200);
    return std::chrono::milliseconds(100);
}

}  // namespace mooncake::io_pattern
