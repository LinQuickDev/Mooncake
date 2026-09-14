#include "io_pattern/resilient_analyzer.h"

namespace mooncake::io_pattern {

PatternResult ResilientAnalyzer::Analyze(
    const IoPatternSnapshot& snapshot) const {
    if (!primary_) return Fallback();
    try {
        auto result = primary_->Analyze(snapshot);
        RecordSuccess(result);
        return result;
    } catch (...) {
        RecordFailure();
        return Fallback();
    }
}

WorkloadType ResilientAnalyzer::DetectWorkloadType(
    const IoPatternSnapshot& snapshot) const {
    return Analyze(snapshot).workload_type;
}

float ResilientAnalyzer::CalculateConfidence(
    const ObjectRef& object, const IoPatternSnapshot& snapshot) const {
    const auto result = Analyze(snapshot);
    for (const auto& key : result.keys) {
        if (key.object == object) return key.confidence;
    }
    return 0.0F;
}

void ResilientAnalyzer::RecordFailure() const {
    std::lock_guard lock(mutex_);
    ++failures_;
    if (failure_threshold_ != 0 && failures_ >= failure_threshold_)
        degraded_ = true;
}

void ResilientAnalyzer::RecordSuccess(const PatternResult& result) const {
    std::lock_guard lock(mutex_);
    last_result_ = result;
    failures_ = 0;
    degraded_ = false;
}

PatternResult ResilientAnalyzer::Fallback() const {
    std::lock_guard lock(mutex_);
    if (!last_result_.keys.empty() || last_result_.workload_type != WorkloadType::kUnknown)
        return last_result_;
    PatternResult result;
    result.workload_type = WorkloadType::kMixed;
    return result;
}

bool ResilientAnalyzer::degraded() const {
    std::lock_guard lock(mutex_);
    return degraded_;
}

size_t ResilientAnalyzer::failures() const {
    std::lock_guard lock(mutex_);
    return failures_;
}

PatternResult ResilientAnalyzer::FallbackResult() const { return Fallback(); }

}  // namespace mooncake::io_pattern
