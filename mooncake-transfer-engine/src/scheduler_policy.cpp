// Copyright 2026 Mooncake Authors
// Licensed under the Apache License, Version 2.0.
#include "scheduler/scheduler_policy.h"

#include <algorithm>
#include <iterator>
#include <limits>
#include <tuple>
#include <utility>

namespace mooncake::scheduling {
namespace {

class HierarchicalAdmissionPolicy final : public AdmissionPolicy {
   public:
    AdmissionDecision admit(uint64_t bytes, const RuntimeSnapshot& runtime,
                            const SchedulerConfig& config) const override {
        if (bytes > config.max_outstanding_bytes ||
            bytes > config.max_tenant_outstanding_bytes)
            return AdmissionDecision::REJECT;
        if (runtime.outstanding_tasks >= config.max_outstanding_tasks ||
            bytes > config.max_outstanding_bytes - runtime.outstanding_bytes ||
            runtime.tenant_outstanding_tasks >=
                config.max_tenant_outstanding_tasks ||
            bytes > config.max_tenant_outstanding_bytes -
                        runtime.tenant_outstanding_bytes)
            return AdmissionDecision::DEFER;
        return AdmissionDecision::ACCEPT;
    }
};

class BoundedDispatchBudgetPolicy final : public DispatchBudgetPolicy {
   public:
    uint64_t budget(const ReadyTaskView& task, uint64_t fair_bytes,
                    const RuntimeSnapshot& runtime,
                    const SchedulerConfig& config) const override {
        uint64_t limit = config.max_inflight_bytes;
        if (task.qos.traffic_class != TrafficClass::HIGH)
            limit -= config.reserved_high_bytes;
        if (runtime.inflight_bytes >= limit) return 0;
        return std::min({task.remaining_bytes, config.quantum_bytes, fair_bytes,
                         limit - runtime.inflight_bytes});
    }
};

class IntentQoSPolicy final : public QoSResolutionPolicy {
   public:
    ResolvedQoS resolve(const SchedulingHint& hint,
                        const SchedulerConfig& config) const override {
        ResolvedQoS qos;
        switch (hint.intent) {
            case TaskIntent::CONTROL:
            case TaskIntent::FOREGROUND_GET:
            case TaskIntent::P2D_TRANSFER:
                qos.traffic_class = TrafficClass::HIGH;
                break;
            case TaskIntent::BACKGROUND_PUT:
            case TaskIntent::MIGRATION:
            case TaskIntent::CHECKPOINT:
                qos.traffic_class = TrafficClass::LOW;
                break;
            default:
                break;
        }
        qos.priority_rank = hint.requested_priority.value_or(0);
        auto it = config.tenant_weights.find(hint.tenant_id);
        if (it != config.tenant_weights.end()) qos.weight = it->second;
        return qos;
    }
};

class HierarchicalTaskPolicy final : public TaskSelectionPolicy {
   public:
    explicit HierarchicalTaskPolicy(SchedulerConfig config)
        : config_(std::move(config)) {}

    TaskSelection select(const std::vector<ReadyTaskView>& ready,
                         uint64_t now_ns) override {
        using TenantTasks =
            std::map<std::string, std::vector<const ReadyTaskView*>>;
        std::array<TenantTasks, 3> groups;
        for (const auto& task : ready) {
            groups[static_cast<size_t>(task.qos.traffic_class)]
                  [task.requested->tenant_id]
                      .push_back(&task);
        }
        // Drop inactive domains rather than accumulating credit while idle.
        for (size_t c = 0; c < groups.size(); ++c) {
            if (groups[c].empty()) class_deficit_[c] = 0;
            for (auto it = tenant_deficit_[c].begin();
                 it != tenant_deficit_[c].end();) {
                if (!groups[c].count(it->first))
                    it = tenant_deficit_[c].erase(it);
                else
                    ++it;
            }
        }
        for (size_t attempt = 0; attempt < 3; ++attempt) {
            const size_t c = class_cursor_;
            if (groups[c].empty()) {
                class_cursor_ = (c + 1) % 3;
                continue;
            }
            if (!class_deficit_[c]) {
                class_deficit_[c] =
                    config_.quantum_bytes * config_.class_weights[c];
            }
            auto tenant = groups[c].lower_bound(tenant_cursor_[c]);
            if (tenant == groups[c].end()) tenant = groups[c].begin();
            auto& deficit = tenant_deficit_[c][tenant->first];
            if (!deficit) {
                deficit =
                    config_.quantum_bytes * tenant->second.front()->qos.weight;
            }
            auto key = [&](const ReadyTaskView* task) {
                uint64_t age =
                    now_ns > task->enqueue_ns ? now_ns - task->enqueue_ns : 0;
                uint64_t bonus =
                    config_.aging_interval_ns
                        ? std::min<uint64_t>(age / config_.aging_interval_ns,
                                             config_.max_aging_bonus)
                        : 0;
                int64_t priority =
                    int64_t(task->qos.priority_rank) - int64_t(bonus);
                return std::make_tuple(
                    priority,
                    task->requested->deadline_ns.value_or(
                        std::numeric_limits<uint64_t>::max()),
                    task->sequence);
            };
            auto task = *std::min_element(
                tenant->second.begin(), tenant->second.end(),
                [&](auto* a, auto* b) { return key(a) < key(b); });
            selected_class_ = c;
            selected_tenant_ = tenant->first;
            auto next = std::next(tenant);
            next_tenant_ = next == groups[c].end() ? groups[c].begin()->first
                                                   : next->first;
            return {task->key, std::min(class_deficit_[c], deficit)};
        }
        return {};
    }

    void accepted(const ReadyTaskView&, uint64_t bytes) override {
        auto c = selected_class_;
        auto& tenant = tenant_deficit_[c][selected_tenant_];
        class_deficit_[c] -= bytes;
        tenant -= bytes;
        tenant_cursor_[c] = tenant ? selected_tenant_ : next_tenant_;
        if (!class_deficit_[c]) class_cursor_ = (c + 1) % 3;
    }

   private:
    SchedulerConfig config_;
    std::array<uint64_t, 3> class_deficit_{};
    std::array<std::map<std::string, uint64_t>, 3> tenant_deficit_;
    std::array<std::string, 3> tenant_cursor_;
    size_t class_cursor_{0};
    size_t selected_class_{0};
    std::string selected_tenant_;
    std::string next_tenant_;
};

}  // namespace

std::unique_ptr<QoSResolutionPolicy> makeIntentQoSPolicy() {
    return std::make_unique<IntentQoSPolicy>();
}

std::unique_ptr<AdmissionPolicy> makeHierarchicalAdmissionPolicy() {
    return std::make_unique<HierarchicalAdmissionPolicy>();
}

std::unique_ptr<DispatchBudgetPolicy> makeBoundedDispatchBudgetPolicy() {
    return std::make_unique<BoundedDispatchBudgetPolicy>();
}

std::unique_ptr<TaskSelectionPolicy> makeHierarchicalTaskPolicy(
    const SchedulerConfig& config) {
    return std::make_unique<HierarchicalTaskPolicy>(config);
}

SchedulerPolicySet makeDefaultPolicySet(const SchedulerConfig& config) {
    return {makeIntentQoSPolicy(), makeHierarchicalAdmissionPolicy(),
            makeHierarchicalTaskPolicy(config),
            makeBoundedDispatchBudgetPolicy()};
}

}  // namespace mooncake::scheduling
