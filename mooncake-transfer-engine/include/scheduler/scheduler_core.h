// Copyright 2026 Mooncake Authors
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "scheduler/scheduler_policy.h"
#include "transport/transport.h"

namespace mooncake {

struct ScheduledTransferRequest {
    Transport::TransferRequest request;
    SchedulingHint hint;
};

namespace scheduling {

// Each grant has a private physical batch. Public batch/task identities never
// enter a transport, so physical completion cannot finish a logical task early.
class SchedulerCore {
   public:
    using BatchID = Transport::BatchID;
    using TransferStatus = Transport::TransferStatus;
    using Route =
        std::function<Status(const Transport::TransferRequest&, Transport*&)>;

    SchedulerCore(SchedulerConfig config, Route route);
    SchedulerCore(SchedulerConfig config, Route route,
                  SchedulerPolicySet policies);
    ~SchedulerCore();
    static Status validate(const SchedulerConfig& config);

    Status submit(BatchID batch,
                  const std::vector<ScheduledTransferRequest>& requests);
    Status status(BatchID batch, size_t task, TransferStatus& result);
    Status batchStatus(BatchID batch, TransferStatus& result);
    Status cancel(BatchID batch, size_t task);
    Status release(BatchID batch);
    bool owns(BatchID batch);

   private:
    struct Task {
        uint64_t key;
        size_t public_id;
        ScheduledTransferRequest input;
        ResolvedQoS qos;
        uint64_t enqueue_ns;
        uint64_t sequence;
        uint64_t completed{0};
        Transport::TransferStatusEnum state{Transport::PENDING};
        bool canceling{false};
        bool dispatch_failed{false};
        bool admitted{false};
        Transport* transport{nullptr};
        Transport::TransferRequest range;
        std::unique_ptr<Transport::BatchDesc> grant;
    };
    struct Batch {
        std::vector<std::unique_ptr<Task>> tasks;
    };

    static uint64_t nowNs();
    static bool terminal(const Task& task);
    ReadyTaskView view(const Task& task) const;
    void run();
    void poll();
    bool dispatch();
    void finish(Task& task, Transport::TransferStatusEnum state);
    void publishBatch(BatchID id, const Batch& batch);
    RuntimeSnapshot snapshot(const std::string& tenant) const;
    void admit(Task& task);

    SchedulerConfig config_;
    Route route_;
    std::unique_ptr<QoSResolutionPolicy> qos_;
    std::unique_ptr<TaskSelectionPolicy> selection_;
    std::unique_ptr<AdmissionPolicy> admission_;
    std::unique_ptr<DispatchBudgetPolicy> budget_;
    std::mutex mutex_;
    std::condition_variable wake_;
    std::unordered_map<BatchID, Batch> batches_;
    uint64_t next_key_{1};
    uint64_t next_sequence_{1};
    size_t outstanding_tasks_{0};
    uint64_t outstanding_bytes_{0};
    size_t admitted_tasks_{0};
    uint64_t admitted_bytes_{0};
    size_t deferred_tasks_{0};
    uint64_t deferred_bytes_{0};
    std::map<std::string, std::pair<size_t, uint64_t>> tenant_usage_;
    uint64_t inflight_bytes_{0};
    bool stopping_{false};
    std::thread worker_;
};

}  // namespace scheduling
}  // namespace mooncake
