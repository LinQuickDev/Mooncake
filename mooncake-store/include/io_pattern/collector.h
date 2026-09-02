#pragma once

#include "io_pattern/types.h"

namespace mooncake::io_pattern {

// Collects non-blocking, already-aggregated observations from data paths.
class IoPatternCollector {
   public:
    virtual ~IoPatternCollector() = default;

    // Implementations must not block the caller on RPC or storage I/O.
    virtual void ReportInferenceMetrics(const InferenceMetrics& metrics) = 0;
    virtual void RecordAccess(const std::string& key,
                              const AccessRecord& record) = 0;
    virtual void RecordStorageMetric(const StorageMetric& metric) = 0;
    virtual IoPatternSnapshot GetSnapshot() const = 0;
};

}  // namespace mooncake::io_pattern
