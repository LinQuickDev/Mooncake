#pragma once

#include "io_pattern/types.h"

namespace mooncake::io_pattern {

class IoPatternCollector {
   public:
    virtual ~IoPatternCollector() = default;

    virtual void ReportInferenceMetrics(const InferenceMetrics& metrics) = 0;
    virtual void RecordAccess(const AccessRecord& record) = 0;
    virtual void RecordStorageMetric(const StorageMetric& metric) = 0;
    virtual IoPatternSnapshot GetSnapshot() const = 0;
};

}  // namespace mooncake::io_pattern
