#include "io_pattern/threshold_analyzer.h"

#include <algorithm>

namespace mooncake::io_pattern {
namespace {

bool IsCodeAgent(const KeyMetrics& key, const ThresholdAnalyzerConfig& config) {
    return key.token_count > config.code_agent_token_count &&
           key.prefix_fanout > config.code_agent_prefix_fanout &&
           key.match_length > config.code_agent_match_length;
}

bool IsRecommendation(const KeyMetrics& key,
                      const ThresholdAnalyzerConfig& config) {
    return key.block_size < config.recommendation_block_size &&
           key.access_count_window > config.recommendation_frequency;
}

bool IsConversation(const KeyMetrics& key,
                    const ThresholdAnalyzerConfig& config) {
    return key.prefix_fanout > config.conversation_prefix_fanout &&
           key.match_length > config.conversation_match_length;
}

float RuleConfidence(const KeyMetrics& key,
                     const ThresholdAnalyzerConfig& config) {
    float score = 0.0F;
    if (IsCodeAgent(key, config)) score = std::max(score, 1.0F);
    if (IsRecommendation(key, config)) score = std::max(score, 1.0F);
    if (IsConversation(key, config)) score = std::max(score, 1.0F);
    // A partial match is useful to policies, but must not look like a
    // definitive workload classification.
    if (score == 0.0F) {
        const float code = std::min(
            {static_cast<float>(key.token_count) /
                 std::max(1.0F, static_cast<float>(config.code_agent_token_count)),
             static_cast<float>(key.prefix_fanout) /
                 std::max(1.0F, static_cast<float>(config.code_agent_prefix_fanout)),
             static_cast<float>(key.match_length) /
                 std::max(1.0F, static_cast<float>(config.code_agent_match_length))});
        const float recommendation = std::min(
            static_cast<float>(config.recommendation_block_size) /
                std::max(1.0F, static_cast<float>(key.block_size)),
             static_cast<float>(key.access_count_window) /
                std::max(1.0F, static_cast<float>(config.recommendation_frequency)));
        const float conversation = std::min(
            static_cast<float>(key.prefix_fanout) /
                std::max(1.0F, static_cast<float>(config.conversation_prefix_fanout)),
            static_cast<float>(key.match_length) /
                std::max(1.0F, static_cast<float>(config.conversation_match_length)));
        score = std::clamp(std::max({code, recommendation, conversation}),
                           0.0F, 1.0F);
    }
    return score;
}

}  // namespace

PatternResult ThresholdAnalyzer::Analyze(
    const IoPatternSnapshot& snapshot) const {
    PatternResult result;
    result.workload_type = DetectWorkloadType(snapshot);
    if (!snapshot.keys.empty()) {
        float total = 0.0F;
        for (const auto& key : snapshot.keys) total += KeyConfidence(key);
        result.workload_confidence =
            std::clamp(total / static_cast<float>(snapshot.keys.size()), 0.0F,
                       1.0F);
    }
    result.keys.reserve(snapshot.keys.size());
    for (const auto& key : snapshot.keys) {
        KeyPattern pattern;
        pattern.object = key.object;
        pattern.confidence = KeyConfidence(key);
        pattern.frequency_score = std::min(
            1.0F, static_cast<float>(key.access_count_window) / 20.0F);
        pattern.idle_score = std::min(
            1.0F, static_cast<float>(key.idle_time_us) / 1'000'000.0F);
        pattern.prefix_score = std::min(
            1.0F, static_cast<float>(key.match_length) / 256.0F);
        pattern.recompute_score = std::min(1.0F, key.recompute_cost);
        pattern.transfer_roi = key.transfer_eta_us == 0
                                   ? 0.0F
                                   : key.recompute_cost /
                                         static_cast<float>(key.transfer_eta_us);
        pattern.migration_safe = !key.pinned;
        result.keys.push_back(pattern);
    }
    return result;
}

WorkloadType ThresholdAnalyzer::DetectWorkloadType(
    const IoPatternSnapshot& snapshot) const {
    if (snapshot.keys.empty()) {
        return WorkloadType::kMixed;
    }
    uint32_t code_agents = 0;
    uint32_t recommendations = 0;
    uint32_t conversations = 0;
    for (const auto& key : snapshot.keys) {
        code_agents += IsCodeAgent(key, config_);
        recommendations += IsRecommendation(key, config_);
        conversations += IsConversation(key, config_);
    }
    const uint32_t matched = static_cast<uint32_t>(code_agents != 0) +
                             static_cast<uint32_t>(recommendations != 0) +
                             static_cast<uint32_t>(conversations != 0);
    if (matched > 1) return WorkloadType::kMixed;
    if (code_agents != 0) return WorkloadType::kCodeAgent;
    if (recommendations != 0) return WorkloadType::kGenerativeRecommendation;
    if (conversations != 0) return WorkloadType::kMultiTurnConversation;
    return WorkloadType::kMixed;
}

float ThresholdAnalyzer::CalculateConfidence(
    const ObjectRef& object, const IoPatternSnapshot& snapshot) const {
    const auto* key = FindKey(object, snapshot);
    return key == nullptr ? 0.0F : KeyConfidence(*key);
}

const KeyMetrics* ThresholdAnalyzer::FindKey(
    const ObjectRef& object, const IoPatternSnapshot& snapshot) const {
    const auto it = std::find_if(
        snapshot.keys.begin(), snapshot.keys.end(),
        [&object](const KeyMetrics& key) { return key.object == object; });
    return it == snapshot.keys.end() ? nullptr : &*it;
}

float ThresholdAnalyzer::KeyConfidence(const KeyMetrics& key) const {
    return RuleConfidence(key, config_);
}

}  // namespace mooncake::io_pattern
