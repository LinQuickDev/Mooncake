// Copyright 2026 Mooncake Authors
// Licensed under the Apache License, Version 2.0.
#include <gtest/gtest.h>

#include <limits>

#include "scheduler/scheduler_policy.h"

namespace mooncake::scheduling {

TEST(SchedulerPolicy, IntentAndExplicitZeroAreIndependent) {
    auto policy = makeIntentQoSPolicy();
    SchedulerConfig config;
    SchedulingHint hint;
    hint.intent = TaskIntent::BACKGROUND_PUT;
    hint.requested_priority = 0;
    auto qos = policy->resolve(hint, config);
    EXPECT_EQ(qos.traffic_class, TrafficClass::LOW);
    EXPECT_EQ(qos.priority_rank, 0);
    hint.requested_priority = std::numeric_limits<int32_t>::min();
    EXPECT_EQ(policy->resolve(hint, config).priority_rank,
              std::numeric_limits<int32_t>::min());
    hint.intent = TaskIntent::FOREGROUND_GET;
    EXPECT_EQ(policy->resolve(hint, config).traffic_class, TrafficClass::HIGH);
}

TEST(SchedulerPolicy, HierarchicalByteShares) {
    SchedulerConfig config;
    config.quantum_bytes = 1024;
    config.class_weights = {4, 2, 1};
    auto policy = makeHierarchicalTaskPolicy(config);
    std::array<SchedulingHint, 4> hints;
    hints[0].tenant_id = "A";
    hints[1].tenant_id = "B";
    hints[2].tenant_id = "C";
    hints[3].tenant_id = "D";
    std::vector<ReadyTaskView> ready{
        {1, &hints[0], {TrafficClass::HIGH, 0, 2}, 1024, 0, 1},
        {2, &hints[1], {TrafficClass::HIGH, 0, 1}, 1024, 0, 2},
        {3, &hints[2], {TrafficClass::MEDIUM, 0, 1}, 1024, 0, 3},
        {4, &hints[3], {TrafficClass::LOW, 0, 1}, 1024, 0, 4}};
    std::array<uint64_t, 4> bytes{};
    for (int grant = 0; grant < 210; ++grant) {
        auto selected = policy->select(ready, 0);
        ASSERT_GE(selected.available_bytes, 1024);
        ASSERT_GE(selected.key, 1);
        ASSERT_LE(selected.key, 4);
        bytes[selected.key - 1] += 1024;
        policy->accepted(ready[selected.key - 1], 1024);
    }
    EXPECT_EQ(bytes[0], 80 * 1024);
    EXPECT_EQ(bytes[1], 40 * 1024);
    EXPECT_EQ(bytes[2], 60 * 1024);
    EXPECT_EQ(bytes[3], 30 * 1024);
}

TEST(SchedulerPolicy, PriorityDeadlineAndStableSequence) {
    SchedulerConfig config;
    auto policy = makeHierarchicalTaskPolicy(config);
    SchedulingHint a, b;
    a.deadline_ns = 100;
    b.deadline_ns = 50;
    std::vector<ReadyTaskView> ready{
        {1, &a, {TrafficClass::MEDIUM, -1, 1}, 16, 0, 2},
        {2, &b, {TrafficClass::MEDIUM, 0, 1}, 16, 0, 1}};
    EXPECT_EQ(policy->select(ready, 0).key, 1);
    ready[1].qos.priority_rank = -1;
    EXPECT_EQ(policy->select(ready, 0).key, 2);
    b.deadline_ns = 100;
    EXPECT_EQ(policy->select(ready, 0).key, 2);
}

TEST(SchedulerPolicy, AgingDoesNotOverflowOrChangeClass) {
    SchedulerConfig config;
    config.aging_interval_ns = 1;
    auto policy = makeHierarchicalTaskPolicy(config);
    SchedulingHint a, b;
    std::vector<ReadyTaskView> ready{
        {1,
         &a,
         {TrafficClass::LOW, std::numeric_limits<int32_t>::min(), 1},
         16,
         0,
         2},
        {2, &b, {TrafficClass::LOW, -10, 1}, 16, 100, 1}};
    EXPECT_EQ(policy->select(ready, 100).key, 1);
}

TEST(SchedulerPolicy, RejectedDispatchDoesNotConsumeCredit) {
    SchedulerConfig config;
    auto policy = makeHierarchicalTaskPolicy(config);
    SchedulingHint hint;
    std::vector<ReadyTaskView> ready{{1, &hint, {}, 128, 0, 1}};
    auto first = policy->select(ready, 0);
    auto retry = policy->select(ready, 0);
    EXPECT_EQ(first.key, retry.key);
    EXPECT_EQ(first.available_bytes, retry.available_bytes);
    policy->accepted(ready[0], 64);
    EXPECT_EQ(policy->select(ready, 0).available_bytes,
              first.available_bytes - 64);
}

TEST(SchedulerPolicy, AdmissionDistinguishesPermanentAndTransientLimits) {
    SchedulerConfig config;
    config.max_outstanding_bytes = 1024;
    config.max_tenant_outstanding_bytes = 512;
    auto policy = makeHierarchicalAdmissionPolicy();
    RuntimeSnapshot runtime;
    EXPECT_EQ(policy->admit(513, runtime, config), AdmissionDecision::REJECT);
    runtime.tenant_outstanding_bytes = 500;
    EXPECT_EQ(policy->admit(16, runtime, config), AdmissionDecision::DEFER);
    runtime.tenant_outstanding_bytes = 0;
    EXPECT_EQ(policy->admit(16, runtime, config), AdmissionDecision::ACCEPT);
}

TEST(SchedulerPolicy, HighReservationCannotBeSpentByLow) {
    SchedulerConfig config;
    config.max_inflight_bytes = 1024;
    config.reserved_high_bytes = 256;
    auto policy = makeBoundedDispatchBudgetPolicy();
    SchedulingHint hint;
    ReadyTaskView task{1, &hint, {TrafficClass::LOW, 0, 1}, 1024, 0, 1};
    RuntimeSnapshot runtime;
    runtime.inflight_bytes = 768;
    EXPECT_EQ(policy->budget(task, 1024, runtime, config), 0);
    task.qos.traffic_class = TrafficClass::HIGH;
    EXPECT_EQ(policy->budget(task, 1024, runtime, config), 256);
}

}  // namespace mooncake::scheduling
