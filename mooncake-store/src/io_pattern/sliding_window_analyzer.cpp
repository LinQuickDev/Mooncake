#include "io_pattern/sliding_window_analyzer.h"

#include <algorithm>
#include <vector>

namespace mooncake::io_pattern {
namespace {
template <typename T>
T Percentile(std::vector<T> values, size_t rank) {
    if (values.empty()) return 0;
    std::sort(values.begin(), values.end());
    return values[std::min(rank, values.size() - 1)];
}
}

void SlidingWindowAnalyzer::Append(const IoPatternSnapshot& snapshot) const {
    std::lock_guard lock(mutex_);
    if (history_.empty() ||
        history_.back().generated_at_ns != snapshot.generated_at_ns) {
        history_.push_back(snapshot);
    }
    const uint64_t cutoff = snapshot.generated_at_ns > window_ns_
                                ? snapshot.generated_at_ns - window_ns_
                                : 0;
    while (!history_.empty() && history_.front().generated_at_ns < cutoff)
        history_.pop_front();
}

IoPatternSnapshot SlidingWindowAnalyzer::Aggregate(
    const IoPatternSnapshot& current) const {
    Append(current);
    std::lock_guard lock(mutex_);
    IoPatternSnapshot aggregate = current;
    aggregate.keys.clear();
    for (const auto& snapshot : history_) {
        aggregate.keys.insert(aggregate.keys.end(), snapshot.keys.begin(),
                              snapshot.keys.end());
    }
    return aggregate;
}

PatternResult SlidingWindowAnalyzer::Analyze(
    const IoPatternSnapshot& snapshot) const {
    const auto aggregate = Aggregate(snapshot);
    auto result = analyzer_.Analyze(aggregate);
    return result.workload_type == WorkloadType::kMixed
               ? kmeans_.Analyze(aggregate)
               : result;
}

WorkloadType SlidingWindowAnalyzer::DetectWorkloadType(
    const IoPatternSnapshot& snapshot) const {
    return Analyze(snapshot).workload_type;
}

float SlidingWindowAnalyzer::CalculateConfidence(
    const ObjectRef& object, const IoPatternSnapshot& snapshot) const {
    const auto result = Analyze(snapshot);
    const auto it = std::find_if(result.keys.begin(), result.keys.end(),
                                 [&object](const KeyPattern& key) {
                                     return key.object == object;
                                 });
    return it == result.keys.end() ? 0.0F : it->confidence;
}

WorkloadFeatureStats SlidingWindowAnalyzer::FeatureStats() const {
    std::lock_guard lock(mutex_);
    std::vector<uint32_t> tokens, fanouts, matches, frequencies;
    std::vector<uint64_t> blocks;
    for (const auto& snapshot : history_) {
        for (const auto& key : snapshot.keys) {
            tokens.push_back(key.token_count);
            fanouts.push_back(key.prefix_fanout);
            matches.push_back(key.match_length);
            frequencies.push_back(static_cast<uint32_t>(key.access_count_window));
            blocks.push_back(key.block_size);
        }
    }
    const auto p90 = [](size_t size) { return size == 0 ? 0 : (size * 9) / 10; };
    WorkloadFeatureStats stats;
    stats.samples = tokens.size();
    stats.token_median = Percentile(tokens, tokens.size() / 2);
    stats.token_p90 = Percentile(tokens, p90(tokens.size()));
    stats.fanout_p90 = Percentile(fanouts, p90(fanouts.size()));
    stats.block_median = Percentile(blocks, blocks.size() / 2);
    stats.block_p90 = Percentile(blocks, p90(blocks.size()));
    stats.match_p90 = Percentile(matches, p90(matches.size()));
    stats.frequency_median = Percentile(frequencies, frequencies.size() / 2);
    return stats;
}

}  // namespace mooncake::io_pattern
