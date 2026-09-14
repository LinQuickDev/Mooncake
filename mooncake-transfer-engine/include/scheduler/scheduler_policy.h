// Copyright 2026 Mooncake Authors
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#include "scheduler/scheduling_hint.h"

namespace mooncake::scheduling {

enum class TrafficClass : uint8_t { HIGH, MEDIUM, LOW };

struct SchedulerConfig {
    size_t max_outstanding_tasks{1024};
    uint64_t max_outstanding_bytes{1ULL << 30};
    size_t max_deferred_tasks{256};
    uint64_t max_deferred_bytes{256ULL << 20};
    size_t max_tenant_outstanding_tasks{1024};
    uint64_t max_tenant_outstanding_bytes{1ULL << 30};
    uint64_t max_inflight_bytes{16ULL << 20};
    uint64_t reserved_high_bytes{1ULL << 20};
    uint64_t quantum_bytes{1ULL << 20};
    uint32_t max_slices{32};
    uint64_t aging_interval_ns{10000000};
    uint32_t max_aging_bonus{1024};
    bool deadline_aware{true};
    uint64_t deadline_promotion_window_ns{1000000};
    std::array<uint32_t, 3> class_weights{8, 4, 1};
    std::map<std::string, uint32_t> tenant_weights;
};

struct ResolvedQoS {
    TrafficClass traffic_class{TrafficClass::MEDIUM};
    int32_t priority_rank{0};
    uint32_t weight{1};
};

struct ReadyTaskView {
    uint64_t key;
    const SchedulingHint* requested;
    ResolvedQoS qos;
    uint64_t remaining_bytes;
    uint64_t enqueue_ns;
    uint64_t sequence;
};

struct TaskSelection {
    uint64_t key{0};
    uint64_t available_bytes{0};
};

enum class AdmissionDecision { ACCEPT, DEFER, REJECT };

struct RuntimeSnapshot {
    size_t outstanding_tasks{0};
    uint64_t outstanding_bytes{0};
    size_t tenant_outstanding_tasks{0};
    uint64_t tenant_outstanding_bytes{0};
    uint64_t inflight_bytes{0};
};

class AdmissionPolicy {
   public:
    virtual ~AdmissionPolicy() = default;
    virtual AdmissionDecision admit(uint64_t bytes,
                                    const RuntimeSnapshot& runtime,
                                    const SchedulerConfig& config) const = 0;
};

class DispatchBudgetPolicy {
   public:
    virtual ~DispatchBudgetPolicy() = default;
    virtual uint64_t budget(const ReadyTaskView& task, uint64_t fair_bytes,
                            const RuntimeSnapshot& runtime,
                            const SchedulerConfig& config) const = 0;
};

class QoSResolutionPolicy {
   public:
    virtual ~QoSResolutionPolicy() = default;
    virtual ResolvedQoS resolve(const SchedulingHint& hint,
                                const SchedulerConfig& config) const = 0;
};

// A selection is only charged after transport acceptance. The policy owns
// fairness state, while the scheduler owns task and resource state.
class TaskSelectionPolicy {
   public:
    virtual ~TaskSelectionPolicy() = default;
    virtual TaskSelection select(const std::vector<ReadyTaskView>& ready,
                                 uint64_t now_ns) = 0;
    virtual void accepted(const ReadyTaskView& task, uint64_t bytes) = 0;
};

// Construct a complete policy set before enabling scheduling. No registry
// lookup or configuration parsing occurs in the scheduling loop.
struct SchedulerPolicySet {
    std::unique_ptr<QoSResolutionPolicy> qos;
    std::unique_ptr<AdmissionPolicy> admission;
    std::unique_ptr<TaskSelectionPolicy> task_selection;
    std::unique_ptr<DispatchBudgetPolicy> dispatch_budget;
};

SchedulerPolicySet makeDefaultPolicySet(const SchedulerConfig& config);

std::unique_ptr<QoSResolutionPolicy> makeIntentQoSPolicy();
std::unique_ptr<AdmissionPolicy> makeHierarchicalAdmissionPolicy();
std::unique_ptr<DispatchBudgetPolicy> makeBoundedDispatchBudgetPolicy();
std::unique_ptr<TaskSelectionPolicy> makeHierarchicalTaskPolicy(
    const SchedulerConfig& config);

}  // namespace mooncake::scheduling
