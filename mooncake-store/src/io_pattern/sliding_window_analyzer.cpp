#include "io_pattern/sliding_window_analyzer.h"

#include <algorithm>
#include <unordered_map>
#include <vector>

namespace mooncake::io_pattern {
namespace {
template <typename T>
T Percentile(std::vector<T> values, size_t rank) {
    if (values.empty()) return 0;
    std::sort(values.begin(), values.end());
    return values[std::min(rank, values.size() - 1)];
}

std::vector<KeyMetrics> LatestKeys(
    const std::deque<IoPatternSnapshot>& history) {
    std::unordered_map<ObjectRef, KeyMetrics, ObjectRefHash> latest;
    for (const auto& snapshot : history) {
        for (const auto& key : snapshot.keys) latest[key.object] = key;
    }
    std::vector<KeyMetrics> keys;
    keys.reserve(latest.size());
    for (auto& [object, key] : latest) {
        (void)object;
        keys.push_back(std::move(key));
    }
    std::sort(keys.begin(), keys.end(), [](const KeyMetrics& left,
                                           const KeyMetrics& right) {
        if (left.object.tenant_id != right.object.tenant_id) {
            return left.object.tenant_id < right.object.tenant_id;
        }
        return left.object.key < right.object.key;
    });
    return keys;
}
}

void SlidingWindowAnalyzer::Append(const IoPatternSnapshot& snapshot) const {
    std::lock_guard lock(mutex_);
    IoPatternSnapshot bounded = snapshot;
    if (max_history_keys_ != 0 &&
        bounded.keys.size() > max_history_keys_) {
        std::sort(bounded.keys.begin(), bounded.keys.end(),
                  [](const KeyMetrics& left, const KeyMetrics& right) {
                      if (left.object.tenant_id != right.object.tenant_id) {
                          return left.object.tenant_id < right.object.tenant_id;
                      }
                      return left.object.key < right.object.key;
                  });
        bounded.keys.resize(max_history_keys_);
    }
    if (history_.empty() ||
        history_.back().generated_at_ns != bounded.generated_at_ns) {
        history_key_count_ += bounded.keys.size();
        history_.push_back(std::move(bounded));
    }
    const uint64_t cutoff = snapshot.generated_at_ns > window_ns_
                                ? snapshot.generated_at_ns - window_ns_
                                : 0;
    while (!history_.empty() && history_.front().generated_at_ns < cutoff) {
        history_key_count_ -= history_.front().keys.size();
        history_.pop_front();
    }
    while (max_history_keys_ != 0 && history_key_count_ > max_history_keys_) {
        history_key_count_ -= history_.front().keys.size();
        history_.pop_front();
    }
}

IoPatternSnapshot SlidingWindowAnalyzer::Aggregate(
    const IoPatternSnapshot& current) const {
    Append(current);
    std::lock_guard lock(mutex_);
    IoPatternSnapshot aggregate = current;
    aggregate.keys = LatestKeys(history_);
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
    for (const auto& key : LatestKeys(history_)) {
        tokens.push_back(key.token_count);
        fanouts.push_back(key.prefix_fanout);
        matches.push_back(key.match_length);
        frequencies.push_back(static_cast<uint32_t>(key.access_count_window));
        blocks.push_back(key.block_size);
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
