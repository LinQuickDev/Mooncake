#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

#include "vchunk_metadata.h"

namespace mooncake {

enum class VChunkOperation : uint8_t { PUT = 0, GET = 1, REMOVE = 2 };

struct VChunkMetricsSnapshot {
    std::array<uint64_t, 3> requests{};
    std::array<uint64_t, 3> successes{};
    std::array<uint64_t, 3> latency_us{};
    uint64_t slices{0};
    uint64_t segment_participations{0};
    std::array<uint64_t, 4> slice_size_distribution{};
    uint64_t allocation_failures{0};
    uint64_t transfer_failures{0};
    uint64_t timeouts{0};
    uint64_t retries{0};
    uint64_t rollbacks{0};
    uint64_t metadata_bytes{0};
    std::array<uint64_t, 5> states{};
};

class VChunkMetrics {
   public:
    void Observe(VChunkOperation operation, bool success, uint64_t latency_us);
    void AddSlices(uint64_t count) { slices_.fetch_add(count); }
    void ObserveLayout(const VChunkMetadataRecord& record);
    void AddAllocationFailure() { ++allocation_failures_; }
    void AddTransferFailure() { ++transfer_failures_; }
    void AddTimeout() { ++timeouts_; }
    void AddRetry() { ++retries_; }
    void AddRollback() { ++rollbacks_; }
    void AddMetadataBytes(uint64_t bytes) { metadata_bytes_.fetch_add(bytes); }
    void SetStateCount(VChunkStatus state, uint64_t count);
    VChunkMetricsSnapshot Snapshot() const;

   private:
    std::array<std::atomic<uint64_t>, 3> requests_{};
    std::array<std::atomic<uint64_t>, 3> successes_{};
    std::array<std::atomic<uint64_t>, 3> latency_us_{};
    std::atomic<uint64_t> slices_{0};
    std::atomic<uint64_t> segment_participations_{0};
    std::array<std::atomic<uint64_t>, 4> slice_size_distribution_{};
    std::atomic<uint64_t> allocation_failures_{0};
    std::atomic<uint64_t> transfer_failures_{0};
    std::atomic<uint64_t> timeouts_{0};
    std::atomic<uint64_t> retries_{0};
    std::atomic<uint64_t> rollbacks_{0};
    std::atomic<uint64_t> metadata_bytes_{0};
    std::array<std::atomic<uint64_t>, 5> states_{};
};

}  // namespace mooncake
