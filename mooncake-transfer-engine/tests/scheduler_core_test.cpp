// Copyright 2026 Mooncake Authors
// Licensed under the Apache License, Version 2.0.
#include <gtest/gtest.h>

#include <chrono>
#include <deque>
#include <thread>

#include "scheduler/scheduler_core.h"

namespace mooncake::scheduling {
namespace {

class FakeTransport final : public Transport {
   public:
    bool complete_inline{true};
    bool reject{false};
    std::vector<TransferRequest> ranges;
    std::mutex mutex;
    std::deque<Slice*> pending;

    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::NotImplemented("Use prepared tasks");
    }
    Status scheduledTransferLength(const TransferRequest& request, uint32_t,
                                   size_t& length) override {
        length = request.length;
        return Status::OK();
    }
    Status submitTransferTask(
        const std::vector<TransferTask*>& tasks) override {
        std::lock_guard<std::mutex> lock(mutex);
        for (auto* task : tasks) {
            ranges.push_back(*task->request);
            if (reject) return Status::InvalidArgument("Injected rejection");
            auto* slice = getSliceCache().allocate();
            slice->task = task;
            slice->length = task->request->length;
            task->slice_list.push_back(slice);
            ++task->slice_count;
            if (complete_inline)
                slice->markSuccess();
            else
                pending.push_back(slice);
        }
        return Status::OK();
    }
    void drain() {
        std::lock_guard<std::mutex> lock(mutex);
        complete_inline = true;
        while (!pending.empty()) {
            pending.front()->markSuccess();
            pending.pop_front();
        }
    }
    size_t submitted() {
        std::lock_guard<std::mutex> lock(mutex);
        return ranges.size();
    }
    Status getTransferStatus(BatchID, size_t, TransferStatus&) override {
        return Status::NotImplemented("Scheduler aggregates completion");
    }
    int registerLocalMemory(void*, size_t, const std::string&, bool,
                            bool) override {
        return 0;
    }
    int unregisterLocalMemory(void*, bool) override { return 0; }
    int registerLocalMemoryBatch(const std::vector<BufferEntry>&,
                                 const std::string&) override {
        return 0;
    }
    int unregisterLocalMemoryBatch(const std::vector<void*>&) override {
        return 0;
    }
    const char* getName() const override { return "fake"; }
};

class SchedulerCoreTest : public ::testing::Test {
   protected:
    FakeTransport transport;
    Transport::BatchDesc batch;
    std::unique_ptr<SchedulerCore> scheduler;
    std::array<char, 128> data{};

    void SetUp() override {
        batch.id = reinterpret_cast<Transport::BatchID>(&batch);
        batch.batch_size = 16;
        batch.task_list.reserve(batch.batch_size);
        SchedulerConfig config;
        config.quantum_bytes = 16;
        config.max_inflight_bytes = 16;
        config.reserved_high_bytes = 0;
        config.max_outstanding_tasks = 4;
        config.max_outstanding_bytes = 128;
        config.max_deferred_tasks = 0;
        scheduler = std::make_unique<SchedulerCore>(
            config, [&](const auto&, Transport*& selected) {
                selected = &transport;
                return Status::OK();
            });
    }
    void TearDown() override {
        transport.drain();
        scheduler.reset();
    }
    ScheduledTransferRequest request(size_t bytes) {
        return {
            {Transport::TransferRequest::WRITE, data.data(), 1, 1000, bytes},
            {}};
    }
    template <typename Predicate>
    bool wait(Predicate predicate) {
        auto limit = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        do {
            if (predicate()) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        } while (std::chrono::steady_clock::now() < limit);
        return false;
    }
    bool finished(Transport::TransferStatusEnum expected) {
        return wait([&] {
            Transport::TransferStatus status;
            return scheduler->batchStatus(batch.id, status).ok() &&
                   status.s == expected;
        });
    }
};

TEST_F(SchedulerCoreTest, OwnsRequestsAndAdvancesNonoverlappingRanges) {
    {
        std::vector<ScheduledTransferRequest> temporary{request(70)};
        ASSERT_TRUE(scheduler->submit(batch.id, temporary).ok());
        temporary[0].request.source = nullptr;
    }
    ASSERT_TRUE(finished(Transport::COMPLETED));
    Transport::TransferStatus status;
    ASSERT_TRUE(scheduler->status(batch.id, 0, status).ok());
    EXPECT_EQ(status.transferred_bytes, 70);
    std::lock_guard<std::mutex> lock(transport.mutex);
    ASSERT_EQ(transport.ranges.size(), 5);
    size_t offset = 0;
    for (const auto& range : transport.ranges) {
        EXPECT_EQ(range.source, data.data() + offset);
        EXPECT_EQ(range.target_offset, 1000 + offset);
        EXPECT_LE(range.length, 16);
        offset += range.length;
    }
    EXPECT_EQ(offset, 70);
}

TEST_F(SchedulerCoreTest, RunningCancelWaitsForDrainAndStopsFurtherGrants) {
    transport.complete_inline = false;
    ASSERT_TRUE(scheduler->submit(batch.id, {request(80)}).ok());
    ASSERT_TRUE(wait([&] { return transport.submitted() == 1; }));
    ASSERT_TRUE(scheduler->cancel(batch.id, 0).ok());
    EXPECT_FALSE(scheduler->release(batch.id).ok());
    Transport::TransferStatus status;
    ASSERT_TRUE(scheduler->status(batch.id, 0, status).ok());
    EXPECT_EQ(status.s, Transport::WAITING);
    transport.drain();
    ASSERT_TRUE(finished(Transport::CANCELED));
    EXPECT_EQ(transport.submitted(), 1);
    ASSERT_TRUE(scheduler->status(batch.id, 0, status).ok());
    EXPECT_EQ(status.transferred_bytes, 16);
    EXPECT_TRUE(scheduler->release(batch.id).ok());
    EXPECT_TRUE(batch.task_list[0].is_finished);
    EXPECT_TRUE(batch.has_failure.load());
}

TEST_F(SchedulerCoreTest, AdmissionFailureIsAtomicAndCapacityIsReclaimed) {
    transport.complete_inline = false;
    ASSERT_TRUE(scheduler->submit(batch.id, {request(80)}).ok());
    EXPECT_FALSE(scheduler->submit(batch.id, {request(64), request(16)}).ok());
    EXPECT_EQ(batch.task_list.size(), 1);
    transport.drain();
    ASSERT_TRUE(finished(Transport::COMPLETED));
    ASSERT_TRUE(scheduler->submit(batch.id, {request(64)}).ok());
    ASSERT_TRUE(finished(Transport::COMPLETED));
    EXPECT_EQ(batch.task_list.size(), 2);
}

TEST_F(SchedulerCoreTest, SynchronousFailureDoesNotLeakInflightCredits) {
    transport.reject = true;
    ASSERT_TRUE(scheduler->submit(batch.id, {request(64)}).ok());
    ASSERT_TRUE(finished(Transport::FAILED));
    {
        std::lock_guard<std::mutex> lock(transport.mutex);
        transport.reject = false;
    }
    ASSERT_TRUE(scheduler->submit(batch.id, {request(64)}).ok());
    ASSERT_TRUE(wait([&] {
        Transport::TransferStatus status;
        return scheduler->status(batch.id, 1, status).ok() &&
               status.s == Transport::COMPLETED;
    }));
}

TEST_F(SchedulerCoreTest, ZeroLengthCompletesWithoutPhysicalSubmission) {
    ASSERT_TRUE(scheduler->submit(batch.id, {request(0)}).ok());
    ASSERT_TRUE(finished(Transport::COMPLETED));
    EXPECT_EQ(transport.submitted(), 0);
}

TEST_F(SchedulerCoreTest, HighTaskCompetesAtNextGrantBoundary) {
    transport.complete_inline = false;
    auto low = request(64);
    low.hint.intent = TaskIntent::BACKGROUND_PUT;
    ASSERT_TRUE(scheduler->submit(batch.id, {low}).ok());
    ASSERT_TRUE(wait([&] { return transport.submitted() == 1; }));
    auto high = request(8);
    high.request.target_offset = 2000;
    high.hint.intent = TaskIntent::FOREGROUND_GET;
    ASSERT_TRUE(scheduler->submit(batch.id, {high}).ok());
    Transport::TransferStatus status;
    ASSERT_TRUE(scheduler->status(batch.id, 1, status).ok());
    EXPECT_EQ(status.s, Transport::PENDING);
    transport.drain();
    ASSERT_TRUE(finished(Transport::COMPLETED));
    std::lock_guard<std::mutex> lock(transport.mutex);
    ASSERT_GE(transport.ranges.size(), 2);
    EXPECT_EQ(transport.ranges[1].target_offset, 2000);
}

TEST_F(SchedulerCoreTest, DeferredTaskIsReadmittedAfterQuotaRelease) {
    scheduler.reset();
    SchedulerConfig config;
    config.max_outstanding_tasks = 1;
    config.max_outstanding_bytes = 128;
    config.max_deferred_tasks = 1;
    config.max_deferred_bytes = 128;
    scheduler = std::make_unique<SchedulerCore>(
        config, [&](const auto&, Transport*& selected) {
            selected = &transport;
            return Status::OK();
        });
    transport.complete_inline = false;
    ASSERT_TRUE(scheduler->submit(batch.id, {request(32), request(32)}).ok());
    ASSERT_TRUE(wait([&] { return transport.submitted() == 1; }));
    Transport::TransferStatus status;
    ASSERT_TRUE(scheduler->status(batch.id, 1, status).ok());
    EXPECT_EQ(status.s, Transport::PENDING);
    EXPECT_FALSE(scheduler->submit(batch.id, {request(1)}).ok());
    transport.drain();
    ASSERT_TRUE(finished(Transport::COMPLETED));
    EXPECT_EQ(transport.submitted(), 2);
}

}  // namespace
}  // namespace mooncake::scheduling
