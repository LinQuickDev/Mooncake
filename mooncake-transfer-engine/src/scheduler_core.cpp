// Copyright 2026 Mooncake Authors
// Licensed under the Apache License, Version 2.0.
#include "scheduler/scheduler_core.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <stdexcept>
#include <utility>

#include "config.h"

namespace mooncake::scheduling {

uint64_t SchedulerCore::nowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

Status SchedulerCore::validate(const SchedulerConfig& config) {
    if (!config.max_outstanding_tasks || !config.max_outstanding_bytes ||
        !config.max_inflight_bytes || !config.quantum_bytes ||
        !config.max_slices || !config.max_tenant_outstanding_tasks ||
        !config.max_tenant_outstanding_bytes ||
        config.max_deferred_bytes > std::numeric_limits<uint64_t>::max() -
                                        config.max_outstanding_bytes ||
        config.max_deferred_tasks >
            std::numeric_limits<size_t>::max() - config.max_outstanding_tasks ||
        config.reserved_high_bytes >= config.max_inflight_bytes) {
        return Status::InvalidArgument("Invalid scheduler capacity");
    }
    auto valid_weight = [&](uint32_t weight) {
        return weight && config.quantum_bytes <=
                             std::numeric_limits<uint64_t>::max() / weight;
    };
    for (auto weight : config.class_weights) {
        if (!valid_weight(weight))
            return Status::InvalidArgument("Invalid scheduler class weight");
    }
    for (const auto& entry : config.tenant_weights) {
        if (!valid_weight(entry.second))
            return Status::InvalidArgument("Invalid scheduler tenant weight");
    }
    return Status::OK();
}

SchedulerCore::SchedulerCore(SchedulerConfig config, Route route)
    : SchedulerCore(config, std::move(route), makeDefaultPolicySet(config)) {}

SchedulerCore::SchedulerCore(SchedulerConfig config, Route route,
                             SchedulerPolicySet policies)
    : config_(std::move(config)),
      route_(std::move(route)),
      qos_(std::move(policies.qos)),
      selection_(std::move(policies.task_selection)),
      admission_(std::move(policies.admission)),
      budget_(std::move(policies.dispatch_budget)) {
    if (!validate(config_).ok() || !route_ || !qos_ || !selection_ ||
        !admission_ || !budget_)
        throw std::invalid_argument(
            "Invalid scheduler configuration or policies");
    worker_ = std::thread(&SchedulerCore::run, this);
}

SchedulerCore::~SchedulerCore() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
        for (auto& batch : batches_)
            for (auto& task : batch.second.tasks) task->canceling = true;
    }
    wake_.notify_one();
    worker_.join();
}

bool SchedulerCore::terminal(const Task& task) {
    return task.state == Transport::COMPLETED ||
           task.state == Transport::FAILED || task.state == Transport::CANCELED;
}

ReadyTaskView SchedulerCore::view(const Task& task) const {
    auto qos = task.qos;
    auto deadline = task.input.hint.deadline_ns;
    auto now = nowNs();
    if (config_.deadline_aware && deadline &&
        (*deadline <= now ||
         *deadline - now <= config_.deadline_promotion_window_ns)) {
        if (qos.traffic_class == TrafficClass::LOW)
            qos.traffic_class = TrafficClass::MEDIUM;
        else if (qos.traffic_class == TrafficClass::MEDIUM)
            qos.traffic_class = TrafficClass::HIGH;
    }
    return {task.key,
            &task.input.hint,
            qos,
            task.input.request.length - task.completed,
            task.enqueue_ns,
            task.sequence};
}

Status SchedulerCore::submit(
    BatchID id, const std::vector<ScheduledTransferRequest>& requests) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_) return Status::BatchBusy("Scheduler is stopping");
    auto& public_batch = Transport::toBatchDesc(id);
    auto existing = batches_.find(id);
    size_t count =
        existing == batches_.end() ? 0 : existing->second.tasks.size();
    if (public_batch.task_list.size() != count) {
        return Status::InvalidArgument(
            "Cannot mix scheduled and unscheduled tasks in a batch");
    }
    if (count > public_batch.batch_size ||
        requests.size() > public_batch.batch_size - count) {
        return Status::TooManyRequests("Scheduler task capacity exhausted");
    }
    uint64_t bytes = 0;
    auto simulated = snapshot("");
    auto tenants = tenant_usage_;
    size_t deferred_tasks = deferred_tasks_;
    uint64_t deferred_bytes = deferred_bytes_;
    std::vector<std::unique_ptr<Task>> prepared;
    for (const auto& entry : requests) {
        const auto& request = entry.request;
        if (next_key_ == std::numeric_limits<uint64_t>::max() ||
            next_sequence_ == std::numeric_limits<uint64_t>::max())
            return Status::TooManyRequests(
                "Scheduler identity space exhausted");
        if (request.length > config_.max_outstanding_bytes ||
            request.length > config_.max_tenant_outstanding_bytes) {
            return Status::InvalidArgument("Task exceeds scheduler byte limit");
        }
        if (request.length &&
            (!request.source ||
             request.length - 1 >
                 std::numeric_limits<uintptr_t>::max() -
                     reinterpret_cast<uintptr_t>(request.source) ||
             request.length - 1 > std::numeric_limits<uint64_t>::max() -
                                      request.target_offset)) {
            return Status::InvalidArgument("Invalid transfer address range");
        }
        auto task = std::make_unique<Task>();
        task->input = entry;
        if (task->input.hint.tenant_id.empty())
            task->input.hint.tenant_id = "default";
        task->key = next_key_++;
        if (task->input.hint.request_id.empty())
            task->input.hint.request_id = std::to_string(task->key);
        task->qos = qos_->resolve(task->input.hint, config_);
        task->enqueue_ns = nowNs();
        task->sequence = next_sequence_++;
        task->public_id = count + prepared.size();
        auto result = route_(request, task->transport);
        if (!result.ok()) return result;
        if (!task->transport)
            return Status::InvalidArgument(
                "Scheduler route returned no transport");
        auto& tenant = tenants[task->input.hint.tenant_id];
        simulated.tenant_outstanding_tasks = tenant.first;
        simulated.tenant_outstanding_bytes = tenant.second;
        auto decision = admission_->admit(request.length, simulated, config_);
        if (decision == AdmissionDecision::REJECT)
            return Status::InvalidArgument("Scheduler admission rejected task");
        task->admitted = decision == AdmissionDecision::ACCEPT;
        if (task->admitted) {
            ++simulated.outstanding_tasks;
            simulated.outstanding_bytes += request.length;
            ++tenant.first;
            tenant.second += request.length;
        } else {
            if (deferred_tasks >= config_.max_deferred_tasks ||
                request.length > config_.max_deferred_bytes - deferred_bytes)
                return Status::TooManyRequests(
                    "Scheduler deferred queue is full");
            ++deferred_tasks;
            deferred_bytes += request.length;
        }
        if (request.length > std::numeric_limits<uint64_t>::max() - bytes)
            return Status::InvalidArgument("Batch byte count overflows");
        bytes += request.length;
        prepared.push_back(std::move(task));
    }
    if (prepared.empty()) return Status::OK();
    auto& batch = batches_[id];
    public_batch.task_list.resize(count + prepared.size());
    public_batch.is_finished.store(false, std::memory_order_release);
    for (auto& task : prepared) {
        public_batch.task_list[task->public_id].batch_id = id;
        if (task->admitted) {
            admit(*task);
        } else {
            ++deferred_tasks_;
            deferred_bytes_ += task->input.request.length;
        }
        batch.tasks.push_back(std::move(task));
    }
    outstanding_tasks_ += requests.size();
    outstanding_bytes_ += bytes;
    wake_.notify_one();
    return Status::OK();
}

void SchedulerCore::finish(Task& task, Transport::TransferStatusEnum state) {
    task.state = state;
    --outstanding_tasks_;
    outstanding_bytes_ -= task.input.request.length;
    if (task.admitted) {
        --admitted_tasks_;
        admitted_bytes_ -= task.input.request.length;
        auto tenant = tenant_usage_.find(task.input.hint.tenant_id);
        --tenant->second.first;
        tenant->second.second -= task.input.request.length;
        if (!tenant->second.first) tenant_usage_.erase(tenant);
    } else {
        --deferred_tasks_;
        deferred_bytes_ -= task.input.request.length;
    }
}

RuntimeSnapshot SchedulerCore::snapshot(const std::string& tenant) const {
    RuntimeSnapshot result;
    result.outstanding_tasks = admitted_tasks_;
    result.outstanding_bytes = admitted_bytes_;
    result.inflight_bytes = inflight_bytes_;
    auto it = tenant_usage_.find(tenant);
    if (it != tenant_usage_.end()) {
        result.tenant_outstanding_tasks = it->second.first;
        result.tenant_outstanding_bytes = it->second.second;
    }
    return result;
}

void SchedulerCore::admit(Task& task) {
    task.admitted = true;
    ++admitted_tasks_;
    admitted_bytes_ += task.input.request.length;
    auto& tenant = tenant_usage_[task.input.hint.tenant_id];
    ++tenant.first;
    tenant.second += task.input.request.length;
}

void SchedulerCore::publishBatch(BatchID id, const Batch& batch) {
    auto& public_batch = Transport::toBatchDesc(id);
    bool done = true;
    bool failed = false;
    uint64_t bytes = 0;
    for (const auto& task : batch.tasks) {
        auto& public_task = public_batch.task_list[task->public_id];
        public_task.transferred_bytes = task->completed;
        public_task.is_finished = terminal(*task);
        bytes += task->completed;
        done = done && terminal(*task);
        failed = failed || task->state == Transport::FAILED ||
                 task->state == Transport::CANCELED;
    }
    public_batch.finished_transfer_bytes.store(bytes,
                                               std::memory_order_relaxed);
    if (done) {
        public_batch.has_failure.store(failed, std::memory_order_relaxed);
#ifdef USE_EVENT_DRIVEN_COMPLETION
        std::lock_guard<std::mutex> lock(public_batch.completion_mutex);
#endif
        public_batch.is_finished.store(true, std::memory_order_release);
#ifdef USE_EVENT_DRIVEN_COMPLETION
        public_batch.completion_cv.notify_all();
#endif
    }
}

void SchedulerCore::poll() {
    for (auto& batch : batches_) {
        for (auto& ptr : batch.second.tasks) {
            auto& task = *ptr;
            if (terminal(task)) continue;
            if (task.grant) {
                auto& physical = task.grant->task_list[0];
                // Failure/timeout is not proof that DMA has drained. Keep the
                // grant and its credits until every submitted slice is done.
                auto done = __atomic_load_n(
                    &physical.scheduled_completed_slices, __ATOMIC_ACQUIRE);
                auto slice_count =
                    __atomic_load_n(&physical.slice_count, __ATOMIC_ACQUIRE);
                if (done != slice_count) continue;
                auto failed_slices = __atomic_load_n(
                    &physical.failed_slice_count, __ATOMIC_ACQUIRE);
                auto transferred = __atomic_load_n(&physical.transferred_bytes,
                                                   __ATOMIC_ACQUIRE);
                bool failed = task.dispatch_failed || failed_slices ||
                              transferred != task.range.length;
                inflight_bytes_ -= task.range.length;
                task.completed += transferred;
                task.grant.reset();
                if (failed) {
                    finish(task, Transport::FAILED);
                    continue;
                }
                task.state = Transport::PENDING;
                task.enqueue_ns = nowNs();
                task.sequence = next_sequence_++;
            }
            if (task.canceling)
                finish(task, Transport::CANCELED);
            else if (task.completed == task.input.request.length)
                finish(task, Transport::COMPLETED);
            else if (!task.admitted &&
                     admission_->admit(task.input.request.length,
                                       snapshot(task.input.hint.tenant_id),
                                       config_) == AdmissionDecision::ACCEPT) {
                --deferred_tasks_;
                deferred_bytes_ -= task.input.request.length;
                admit(task);
            }
        }
        publishBatch(batch.first, batch.second);
    }
}

bool SchedulerCore::dispatch() {
    std::vector<ReadyTaskView> ready;
    std::unordered_map<uint64_t, Task*> tasks;
    for (auto& batch : batches_) {
        for (auto& ptr : batch.second.tasks) {
            auto& task = *ptr;
            if (task.state != Transport::PENDING || task.canceling ||
                !task.admitted)
                continue;
            if (!budget_->budget(view(task), config_.quantum_bytes,
                                 snapshot(task.input.hint.tenant_id), config_))
                continue;
            ready.push_back(view(task));
            tasks[task.key] = &task;
        }
    }
    auto selected = selection_->select(ready, nowNs());
    if (!selected.key) return false;
    auto found = tasks.find(selected.key);
    if (found == tasks.end() || !selected.available_bytes) return false;
    auto& task = *found->second;
    uint64_t slice_size = globalConfig().slice_size;
    if (!slice_size) {
        finish(task, Transport::FAILED);
        return true;
    }
    uint64_t descriptor_bytes =
        slice_size > std::numeric_limits<uint64_t>::max() / config_.max_slices
            ? std::numeric_limits<uint64_t>::max()
            : slice_size * config_.max_slices;
    uint64_t bytes =
        std::min(budget_->budget(view(task), selected.available_bytes,
                                 snapshot(task.input.hint.tenant_id), config_),
                 descriptor_bytes);
    uint64_t hard_limit = config_.max_inflight_bytes;
    if (task.qos.traffic_class != TrafficClass::HIGH)
        hard_limit -= config_.reserved_high_bytes;
    if (inflight_bytes_ >= hard_limit) return false;
    bytes = std::min({bytes, selected.available_bytes,
                      uint64_t(task.input.request.length - task.completed),
                      hard_limit - inflight_bytes_});
    if (!bytes) return false;
    task.range = task.input.request;
    task.range.source = reinterpret_cast<void*>(
        reinterpret_cast<uintptr_t>(task.range.source) + task.completed);
    task.range.target_offset += task.completed;
    task.range.length = bytes;
    size_t accepted_length = 0;
    auto bounded = task.transport->scheduledTransferLength(
        task.range, config_.max_slices, accepted_length);
    if (!bounded.ok() || !accepted_length || accepted_length > bytes) {
        finish(task, Transport::FAILED);
        return true;
    }
    bytes = accepted_length;
    task.range.length = bytes;
    task.grant = std::make_unique<Transport::BatchDesc>();
    task.grant->id = reinterpret_cast<BatchID>(task.grant.get());
    task.grant->batch_size = 1;
    task.grant->context = nullptr;
    task.grant->task_list.resize(1);
    auto& physical = task.grant->task_list[0];
    physical.batch_id = task.grant->id;
    physical.transport_ = task.transport;
    physical.request = &task.range;
    physical.scheduled = true;
    inflight_bytes_ += bytes;
    task.state = Transport::WAITING;
    auto result = task.transport->submitTransferTask({&physical});
    task.dispatch_failed = !result.ok();
    // A legacy transport may return an error after posting a prefix. Do not
    // replay that range or release its memory/credits before the prefix drains.
    if (result.ok() ||
        __atomic_load_n(&physical.slice_count, __ATOMIC_ACQUIRE) != 0)
        selection_->accepted(view(task), bytes);
    return true;
}

void SchedulerCore::run() {
    std::unique_lock<std::mutex> lock(mutex_);
    for (;;) {
        poll();
        if (stopping_ && !outstanding_tasks_) break;
        if (!stopping_) {
            while (dispatch()) {
            }
        }
        // Completion counters are grant-level aggregation; bounded polling
        // also makes progress when callers never query their batch status.
        wake_.wait_for(lock, std::chrono::microseconds(100));
    }
}

bool SchedulerCore::owns(BatchID id) {
    std::lock_guard<std::mutex> lock(mutex_);
    return batches_.count(id) != 0;
}

Status SchedulerCore::status(BatchID id, size_t index, TransferStatus& result) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = batches_.find(id);
    if (it == batches_.end() || index >= it->second.tasks.size())
        return Status::InvalidArgument("Scheduled task ID out of range");
    const auto& task = *it->second.tasks[index];
    result = {task.state, size_t(task.completed)};
    return Status::OK();
}

Status SchedulerCore::batchStatus(BatchID id, TransferStatus& result) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = batches_.find(id);
    if (it == batches_.end())
        return Status::InvalidArgument("Unknown scheduled batch");
    result = {Transport::COMPLETED, 0};
    bool pending = false, failed = false, canceled = false;
    for (const auto& task : it->second.tasks) {
        result.transferred_bytes += task->completed;
        pending |= !terminal(*task);
        failed |= task->state == Transport::FAILED;
        canceled |= task->state == Transport::CANCELED;
    }
    result.s = pending    ? Transport::WAITING
               : failed   ? Transport::FAILED
               : canceled ? Transport::CANCELED
                          : Transport::COMPLETED;
    return Status::OK();
}

Status SchedulerCore::cancel(BatchID id, size_t index) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = batches_.find(id);
    if (it == batches_.end() || index >= it->second.tasks.size())
        return Status::InvalidArgument("Scheduled task ID out of range");
    it->second.tasks[index]->canceling = true;
    wake_.notify_one();
    return Status::OK();
}

Status SchedulerCore::release(BatchID id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = batches_.find(id);
    if (it == batches_.end()) return Status::OK();
    for (const auto& task : it->second.tasks)
        if (!terminal(*task))
            return Status::BatchBusy("Scheduled batch is busy");
    publishBatch(id, it->second);
    batches_.erase(it);
    return Status::OK();
}

}  // namespace mooncake::scheduling
