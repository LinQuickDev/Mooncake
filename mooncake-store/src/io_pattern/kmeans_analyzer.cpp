#include "io_pattern/kmeans_analyzer.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <unordered_map>

namespace mooncake::io_pattern {
namespace {

// Token length, fanout, block size, access frequency and prefix-match length
// are the five documented dimensions.  They are normalized against the active
// sliding window before distance calculation.
using Feature = std::array<float, 5>;

struct SessionFeatures {
    Feature values{};
    size_t samples{0};
};

Feature ToFeature(const KeyMetrics& key) {
    return {static_cast<float>(key.token_count),
            static_cast<float>(key.prefix_fanout),
            static_cast<float>(key.block_size),
            static_cast<float>(key.access_count_window),
            static_cast<float>(key.match_length)};
}

float Distance(const Feature& lhs, const Feature& rhs, const Feature& scale) {
    float distance = 0.0F;
    for (size_t index = 0; index < lhs.size(); ++index) {
        const float normalized = (lhs[index] - rhs[index]) /
                                 std::max(1.0F, scale[index]);
        distance += normalized * normalized;
    }
    return distance;
}

WorkloadType Classify(const Feature& feature,
                      const ThresholdAnalyzerConfig& config) {
    if (feature[0] > config.code_agent_token_count &&
        feature[1] > config.code_agent_prefix_fanout &&
        feature[4] > config.code_agent_match_length) {
        return WorkloadType::kCodeAgent;
    }
    if (feature[2] < config.recommendation_block_size &&
        feature[3] > config.recommendation_frequency) {
        return WorkloadType::kGenerativeRecommendation;
    }
    if (feature[1] > config.conversation_prefix_fanout &&
        feature[4] > config.conversation_match_length) {
        return WorkloadType::kMultiTurnConversation;
    }
    return WorkloadType::kMixed;
}

}  // namespace

PatternResult KMeansWorkloadAnalyzer::Analyze(
    const IoPatternSnapshot& snapshot) const {
    PatternResult result;
    if (snapshot.keys.empty()) {
        result.workload_type = WorkloadType::kMixed;
        return result;
    }

    std::unordered_map<std::string, SessionFeatures> by_session;
    Feature scale{1.0F, 1.0F, 1.0F, 1.0F};
    for (const auto& key : snapshot.keys) {
        const std::string session = key.session_id.empty()
                                        ? key.object.tenant_id.value() + ":" + key.object.key
                                        : key.session_id;
        auto& aggregate = by_session[session];
        const auto feature = ToFeature(key);
        for (size_t index = 0; index < feature.size(); ++index) {
            aggregate.values[index] += feature[index];
            scale[index] = std::max(scale[index], feature[index]);
        }
        ++aggregate.samples;
    }

    std::vector<std::string> session_ids;
    std::vector<Feature> samples;
    session_ids.reserve(by_session.size());
    samples.reserve(by_session.size());
    for (auto& [session, aggregate] : by_session) {
        for (auto& value : aggregate.values) {
            value /= static_cast<float>(aggregate.samples);
        }
        session_ids.push_back(session);
        samples.push_back(aggregate.values);
    }

    const size_t cluster_count = std::min<size_t>(3, samples.size());
    std::vector<Feature> centroids(samples.begin(), samples.begin() + cluster_count);
    std::vector<size_t> assignments(samples.size(), 0);
    for (uint32_t iteration = 0; iteration < config_.iterations; ++iteration) {
        std::vector<Feature> sums(cluster_count);
        std::vector<size_t> counts(cluster_count, 0);
        for (size_t sample_index = 0; sample_index < samples.size(); ++sample_index) {
            size_t best = 0;
            float best_distance = Distance(samples[sample_index], centroids[0], scale);
            for (size_t cluster = 1; cluster < cluster_count; ++cluster) {
                const float distance = Distance(samples[sample_index], centroids[cluster], scale);
                if (distance < best_distance) {
                    best = cluster;
                    best_distance = distance;
                }
            }
            assignments[sample_index] = best;
            ++counts[best];
            for (size_t field = 0; field < samples[sample_index].size(); ++field) {
                sums[best][field] += samples[sample_index][field];
            }
        }
        for (size_t cluster = 0; cluster < cluster_count; ++cluster) {
            if (counts[cluster] == 0) continue;
            for (size_t field = 0; field < centroids[cluster].size(); ++field) {
                centroids[cluster][field] = sums[cluster][field] /
                                            static_cast<float>(counts[cluster]);
            }
        }
    }

    std::vector<WorkloadType> labels;
    labels.reserve(cluster_count);
    for (const auto& centroid : centroids) labels.push_back(Classify(centroid, config_.thresholds));
    WorkloadType global = labels[assignments.front()];
    bool mixed = false;
    for (size_t index = 0; index < samples.size(); ++index) {
        const auto type = labels[assignments[index]];
        result.sessions.push_back(
            SessionPattern{.session_id = session_ids[index], .workload_type = type,
                           .confidence = 1.0F / (1.0F + Distance(
                               samples[index], centroids[assignments[index]], scale))});
        if (type != global) mixed = true;
    }
    result.workload_type = mixed ? WorkloadType::kMixed : global;
    result.workload_confidence = mixed ? 0.5F : result.sessions.front().confidence;

    ThresholdAnalyzer key_analyzer(config_.thresholds);
    result.keys = key_analyzer.Analyze(snapshot).keys;
    return result;
}

WorkloadType KMeansWorkloadAnalyzer::DetectWorkloadType(
    const IoPatternSnapshot& snapshot) const {
    return Analyze(snapshot).workload_type;
}

float KMeansWorkloadAnalyzer::CalculateConfidence(
    const ObjectRef& object, const IoPatternSnapshot& snapshot) const {
    const auto result = Analyze(snapshot);
    const auto it = std::find_if(result.keys.begin(), result.keys.end(),
                                 [&object](const KeyPattern& key) {
                                     return key.object == object;
                                 });
    return it == result.keys.end() ? 0.0F : it->confidence;
}

}  // namespace mooncake::io_pattern
