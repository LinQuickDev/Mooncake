// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/runtime/segment.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {
namespace {

class FakeSubBatch : public Transport::SubBatch {
   public:
    size_t size() const override { return task_count; }

    size_t task_count = 0;
    std::vector<Request> requests;
    std::vector<TransferStatus> statuses;
    std::vector<int> poll_counts;
};

class FakeTransport : public Transport {
   public:
    using PollStatusFactory =
        std::function<TransferStatus(const Request&, int)>;

    explicit FakeTransport(TransportType self_type,
                           PollStatusFactory poll_status_factory = {},
                           bool notify_on_submit = false)
        : self_type_(self_type),
          poll_status_factory_(std::move(poll_status_factory)),
          notify_on_submit_(notify_on_submit) {
        caps.dram_to_dram = true;
    }

    std::atomic<int> submit_calls{0};
    std::atomic<int> status_calls{0};
    std::atomic<int> cancel_calls{0};
    std::atomic<int> fail_next_submits{0};
    std::atomic<uint64_t> last_credit_session_high{0};
    std::atomic<uint64_t> last_credit_session_low{0};
    std::atomic<uint64_t> last_credit_epoch{0};
    bool cancellation_supported{true};

    Status install(std::string&, std::shared_ptr<ControlService>,
                   std::shared_ptr<Topology>,
                   std::shared_ptr<Config> = nullptr) override {
        return Status::OK();
    }

    Status allocateSubBatch(SubBatchRef& batch, size_t) override {
        batch = new FakeSubBatch();
        return Status::OK();
    }

    Status freeSubBatch(SubBatchRef& batch) override {
        delete static_cast<FakeSubBatch*>(batch);
        batch = nullptr;
        return Status::OK();
    }

    Status submitTransferTasks(SubBatchRef batch,
                               const std::vector<Request>& requests) override {
        ++submit_calls;
        int failures = fail_next_submits.load(std::memory_order_relaxed);
        while (failures > 0 &&
               !fail_next_submits.compare_exchange_weak(
                   failures, failures - 1, std::memory_order_relaxed)) {
        }
        if (failures > 0) {
            return Status::InternalError(
                "injected zero-handoff submit failure" LOC_MARK);
        }
        last_credit_session_high.store(batch->receiver_credit_session_high,
                                       std::memory_order_relaxed);
        last_credit_session_low.store(batch->receiver_credit_session_low,
                                      std::memory_order_relaxed);
        last_credit_epoch.store(batch->receiver_credit_epoch,
                                std::memory_order_relaxed);
        auto* fake = static_cast<FakeSubBatch*>(batch);
        for (const auto& request : requests) {
            fake->requests.push_back(request);
            fake->statuses.push_back(
                {TransferStatusEnum::COMPLETED, request.length});
            fake->poll_counts.push_back(0);
            ++fake->task_count;
        }
        if (notify_on_submit_) batch->notifyProgress();
        return Status::OK();
    }

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override {
        ++status_calls;
        auto* fake = static_cast<FakeSubBatch*>(batch);
        if (task_id < 0 || task_id >= (int)fake->statuses.size()) {
            return Status::InvalidArgument("bad task_id" LOC_MARK);
        }
        ++fake->poll_counts[task_id];
        if (fake->statuses[task_id].s == TransferStatusEnum::CANCELED) {
            status = fake->statuses[task_id];
        } else if (poll_status_factory_) {
            status = poll_status_factory_(fake->requests[task_id],
                                          fake->poll_counts[task_id]);
        } else {
            status = fake->statuses[task_id];
        }
        return Status::OK();
    }

    bool supportsCancellation() const override {
        return cancellation_supported;
    }

    Status cancelTransferTask(SubBatchRef batch, int task_id) override {
        auto* fake = static_cast<FakeSubBatch*>(batch);
        if (task_id < 0 || task_id >= (int)fake->statuses.size()) {
            return Status::InvalidArgument("bad task_id" LOC_MARK);
        }
        if (!cancellation_supported) {
            return Status::NotImplemented("cancel unsupported" LOC_MARK);
        }
        ++cancel_calls;
        fake->statuses[task_id] = {TransferStatusEnum::CANCELED, 0};
        return Status::OK();
    }

    Status addMemoryBuffer(BufferDesc& desc, const MemoryOptions&) override {
        desc.transports.push_back(self_type_);
        return Status::OK();
    }

    Status addMemoryBuffer(std::vector<BufferDesc>& desc_list,
                           const MemoryOptions& options) override {
        for (auto& desc : desc_list) {
            auto status = addMemoryBuffer(desc, options);
            if (!status.ok()) return status;
        }
        return Status::OK();
    }

    Status removeMemoryBuffer(BufferDesc&) override { return Status::OK(); }

    Status allocateLocalMemory(void** addr, size_t size,
                               MemoryOptions&) override {
        *addr = std::malloc(size);
        return *addr ? Status::OK()
                     : Status::InternalError("malloc failed" LOC_MARK);
    }

    Status freeLocalMemory(void* addr, size_t) override {
        std::free(addr);
        return Status::OK();
    }

    bool warmupMemory(void*, size_t) override { return false; }

    const char* getName() const override { return "<fake-rdma>"; }

   private:
    TransportType self_type_;
    PollStatusFactory poll_status_factory_;
    bool notify_on_submit_;
};

std::shared_ptr<Config> makeRuntimeQueueConfig(size_t max_dispatch_owners,
                                               size_t max_dispatch_bytes,
                                               bool merge_requests = false) {
    auto cfg = std::make_shared<Config>();
    cfg->set("metadata_type", "p2p");
    cfg->set("metadata_servers", "");
    cfg->set("rpc_server_hostname", "127.0.0.1");
    cfg->set("rpc_server_port", "0");
    cfg->set("log_level", "warning");
    cfg->set("merge_requests", merge_requests);
    cfg->set("enable_runtime_queue", true);
    cfg->set("runtime_queue/max_outstanding_owners", 16UL);
    cfg->set("runtime_queue/max_outstanding_bytes", 1UL << 20);
    cfg->set("runtime_queue/max_dispatch_owners", max_dispatch_owners);
    cfg->set("runtime_queue/max_dispatch_bytes", max_dispatch_bytes);
    cfg->set("runtime_queue/staging_owner_reserve", 0UL);
    cfg->set("runtime_queue/staging_byte_reserve", 0UL);
    cfg->set("runtime_queue/progress_fallback_interval_us", 50000UL);

    cfg->set("transports/tcp/enable", false);
    cfg->set("transports/shm/enable", false);
    cfg->set("transports/rdma/enable", false);
    cfg->set("transports/io_uring/enable", false);
    cfg->set("transports/nvlink/enable", false);
    cfg->set("transports/mnnvl/enable", false);
    cfg->set("transports/gds/enable", false);
    cfg->set("transports/ascend_direct/enable", false);
    return cfg;
}

void installFakeRdma(TransferEngineImpl& engine,
                     const std::shared_ptr<FakeTransport>& fake_rdma) {
    std::string seg_name = engine.getSegmentName();
    ASSERT_TRUE(fake_rdma->install(seg_name, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, fake_rdma);
}

void installFakeUb(TransferEngineImpl& engine,
                   const std::shared_ptr<FakeTransport>& fake_ub) {
    std::string seg_name = engine.getSegmentName();
    ASSERT_TRUE(fake_ub->install(seg_name, nullptr, nullptr).ok());
    engine.swapTransportForTest(UB, fake_ub);
}

Request makeLocalWrite(uint8_t* ptr, size_t length) {
    Request request;
    request.opcode = Request::WRITE;
    request.source = ptr;
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(ptr);
    request.length = length;
    request.transport_hint = RDMA;
    return request;
}

Request makeLocalUbWrite(uint8_t* ptr, size_t length) {
    auto request = makeLocalWrite(ptr, length);
    request.transport_hint = UB;
    return request;
}

Request makeRemoteUbWrite(uint8_t* source, SegmentID target_id, uint8_t* target,
                          size_t length) {
    auto request = makeLocalUbWrite(source, length);
    request.target_id = target_id;
    request.target_offset = reinterpret_cast<uint64_t>(target);
    return request;
}

Request makeLocalNetworkWrite(uint8_t* ptr, size_t length) {
    auto request = makeLocalWrite(ptr, length);
    request.transport_hint = UNSPEC;
    return request;
}

TEST(RuntimeQueueDispatch, RejectsOverfullBatchBeforePublishingTasks) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake_rdma);

    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> buffer(kReqLen * 2, 0x11);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());

    BatchID batch = engine.allocateBatch(1);
    ASSERT_NE(batch, (BatchID)0);

    auto status = engine.submitTransfer(
        batch, {makeLocalWrite(buffer.data(), kReqLen),
                makeLocalWrite(buffer.data() + kReqLen, kReqLen)});
    EXPECT_TRUE(status.IsTooManyRequests()) << status.ToString();
    EXPECT_EQ(fake_rdma->submit_calls.load(), 0);

    TransferStatus task_status{};
    EXPECT_TRUE(
        engine.getTransferStatus(batch, 0, task_status).IsInvalidArgument());

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, RejectsOwnerLargerThanDispatchByteWindow) {
    constexpr size_t kReqLen = 4096;
    auto cfg = makeRuntimeQueueConfig(1, kReqLen - 1);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake_rdma);

    std::vector<uint8_t> buffer(kReqLen, 0x77);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());

    BatchID batch = engine.allocateBatch(1);
    ASSERT_NE(batch, (BatchID)0);

    auto status =
        engine.submitTransfer(batch, {makeLocalWrite(buffer.data(), kReqLen)});
    EXPECT_TRUE(status.IsTooManyRequests()) << status.ToString();
    EXPECT_EQ(fake_rdma->submit_calls.load(), 0);

    TransferStatus task_status{};
    EXPECT_TRUE(
        engine.getTransferStatus(batch, 0, task_status).IsInvalidArgument());

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, RejectsEmptyDispatchWindowConfig) {
    auto cfg = makeRuntimeQueueConfig(0, 1UL << 20);
    TransferEngineImpl engine(cfg);

    EXPECT_FALSE(engine.available());

    cfg = makeRuntimeQueueConfig(1, 0);
    TransferEngineImpl byte_window_engine(cfg);

    EXPECT_FALSE(byte_window_engine.available());
}

TEST(RuntimeQueueDispatch, DispatchesOnlyOneWindowOnSubmit) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    std::atomic<bool> complete{false};
    auto fake_rdma = std::make_shared<FakeTransport>(
        RDMA, [&complete](const Request& request, int) {
            return complete.load(std::memory_order_acquire)
                       ? TransferStatus{TransferStatusEnum::COMPLETED,
                                        request.length}
                       : TransferStatus{TransferStatusEnum::PENDING, 0};
        });
    installFakeRdma(engine, fake_rdma);

    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> buffer(kReqLen * 2, 0x22);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());

    BatchID batch = engine.allocateBatch(2);
    ASSERT_NE(batch, (BatchID)0);

    ASSERT_TRUE(
        engine
            .submitTransfer(batch,
                            {makeLocalWrite(buffer.data(), kReqLen),
                             makeLocalWrite(buffer.data() + kReqLen, kReqLen)})
            .ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);

    complete.store(true, std::memory_order_release);
    TransferStatus first{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, first).ok());
    EXPECT_EQ(first.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(fake_rdma->submit_calls.load(), 2);

    TransferStatus second{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 1, second).ok());
    EXPECT_EQ(second.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, KeepsDispatchWindowUntilOwnerIsTerminal) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    std::atomic<bool> complete_first{false};
    auto fake_rdma = std::make_shared<FakeTransport>(
        RDMA, [&complete_first](const Request& request, int) {
            if (!complete_first.load()) {
                return TransferStatus{TransferStatusEnum::PENDING, 0};
            }
            return TransferStatus{TransferStatusEnum::COMPLETED,
                                  request.length};
        });
    installFakeRdma(engine, fake_rdma);

    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> buffer(kReqLen * 2, 0x55);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());

    BatchID batch = engine.allocateBatch(2);
    ASSERT_NE(batch, (BatchID)0);

    ASSERT_TRUE(
        engine
            .submitTransfer(batch,
                            {makeLocalWrite(buffer.data(), kReqLen),
                             makeLocalWrite(buffer.data() + kReqLen, kReqLen)})
            .ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);

    TransferStatus first{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, first).ok());
    EXPECT_EQ(first.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);

    TransferStatus second{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 1, second).ok());
    EXPECT_EQ(second.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);

    complete_first.store(true);
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, first).ok());
    EXPECT_EQ(first.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(fake_rdma->submit_calls.load(), 2);

    ASSERT_TRUE(engine.getTransferStatus(batch, 1, second).ok());
    EXPECT_EQ(second.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, CancelsQueuedOwnerWithoutDispatchingIt) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    std::atomic<bool> complete_first{false};
    auto fake_rdma = std::make_shared<FakeTransport>(
        RDMA, [&complete_first](const Request& request, int) {
            if (!complete_first.load()) {
                return TransferStatus{TransferStatusEnum::PENDING, 0};
            }
            return TransferStatus{TransferStatusEnum::COMPLETED,
                                  request.length};
        });
    installFakeRdma(engine, fake_rdma);

    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> buffer(kReqLen * 2, 0x5a);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(2);
    ASSERT_NE(batch, (BatchID)0);
    ASSERT_TRUE(
        engine
            .submitTransfer(batch,
                            {makeLocalWrite(buffer.data(), kReqLen),
                             makeLocalWrite(buffer.data() + kReqLen, kReqLen)})
            .ok());
    ASSERT_EQ(fake_rdma->submit_calls.load(), 1);

    ASSERT_TRUE(engine.cancelTransfer(batch, 1).ok());
    EXPECT_EQ(fake_rdma->cancel_calls.load(), 0);
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);

    TransferStatus second{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 1, second).ok());
    EXPECT_EQ(second.s, TransferStatusEnum::CANCELED);

    complete_first.store(true);
    TransferStatus first{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, first).ok());
    EXPECT_EQ(first.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, CancelsDispatchedRdmaTaskIdempotently) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma =
        std::make_shared<FakeTransport>(RDMA, [](const Request&, int) {
            return TransferStatus{TransferStatusEnum::PENDING, 0};
        });
    installFakeRdma(engine, fake_rdma);

    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> buffer(kReqLen, 0x6b);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(1);
    ASSERT_NE(batch, (BatchID)0);
    ASSERT_TRUE(
        engine.submitTransfer(batch, {makeLocalWrite(buffer.data(), kReqLen)})
            .ok());

    ASSERT_TRUE(engine.cancelTransfer(batch, 0).ok());
    EXPECT_EQ(fake_rdma->cancel_calls.load(), 1);
    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::CANCELED);
    ASSERT_TRUE(engine.cancelTransfer(batch, 0).ok());
    EXPECT_EQ(fake_rdma->cancel_calls.load(), 1);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, RejectsCancellationForUnsupportedTransport) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    std::atomic<bool> complete{false};
    auto fake_rdma = std::make_shared<FakeTransport>(
        RDMA, [&complete](const Request& request, int) {
            return complete.load(std::memory_order_acquire)
                       ? TransferStatus{TransferStatusEnum::COMPLETED,
                                        request.length}
                       : TransferStatus{TransferStatusEnum::PENDING, 0};
        });
    fake_rdma->cancellation_supported = false;
    installFakeRdma(engine, fake_rdma);

    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> buffer(kReqLen, 0x7c);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(1);
    ASSERT_NE(batch, (BatchID)0);
    ASSERT_TRUE(
        engine.submitTransfer(batch, {makeLocalWrite(buffer.data(), kReqLen)})
            .ok());

    EXPECT_TRUE(engine.cancelTransfer(batch, 0).IsNotImplemented());
    EXPECT_EQ(fake_rdma->cancel_calls.load(), 0);

    complete.store(true, std::memory_order_release);
    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, ProgressWorkerRefillsWindowFromTransportNotify) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    cfg->set("enable_progress_worker", true);
    cfg->set("runtime_queue/progress_fallback_interval_us", 0UL);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(
        RDMA, FakeTransport::PollStatusFactory{}, true);
    installFakeRdma(engine, fake_rdma);

    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> buffer(kReqLen * 2, 0x66);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());

    BatchID batch = engine.allocateBatch(2);
    ASSERT_NE(batch, (BatchID)0);

    ASSERT_TRUE(
        engine
            .submitTransfer(batch,
                            {makeLocalWrite(buffer.data(), kReqLen),
                             makeLocalWrite(buffer.data() + kReqLen, kReqLen)})
            .ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(1000);
    while (std::chrono::steady_clock::now() < deadline &&
           fake_rdma->submit_calls.load() < 2) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    EXPECT_EQ(fake_rdma->submit_calls.load(), 2);

    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, RuntimeQueueDrainsWithoutUserPolling) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    cfg->set("runtime_queue/progress_fallback_interval_us", 1000UL);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(
        RDMA, [](const Request& request, int poll_count) {
            if (poll_count == 1) {
                return TransferStatus{TransferStatusEnum::PENDING, 0};
            }
            return TransferStatus{TransferStatusEnum::COMPLETED,
                                  request.length};
        });
    installFakeRdma(engine, fake_rdma);

    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> buffer(kReqLen * 2, 0x88);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());

    BatchID batch = engine.allocateBatch(2);
    ASSERT_NE(batch, (BatchID)0);

    ASSERT_TRUE(
        engine
            .submitTransfer(batch,
                            {makeLocalWrite(buffer.data(), kReqLen),
                             makeLocalWrite(buffer.data() + kReqLen, kReqLen)})
            .ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(1000);
    while (std::chrono::steady_clock::now() < deadline &&
           (fake_rdma->submit_calls.load() < 2 ||
            fake_rdma->status_calls.load() < 4)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    EXPECT_EQ(fake_rdma->submit_calls.load(), 2);
    EXPECT_GE(fake_rdma->status_calls.load(), 4);

    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, EarlyFreeReclaimsAfterQueuedCompletion) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(
        RDMA, [](const Request& request, int poll_count) {
            if (poll_count == 1) {
                return TransferStatus{TransferStatusEnum::PENDING, 0};
            }
            return TransferStatus{TransferStatusEnum::COMPLETED,
                                  request.length};
        });
    installFakeRdma(engine, fake_rdma);

    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> buffer(kReqLen, 0x33);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());

    BatchID batch = engine.allocateBatch(1);
    ASSERT_NE(batch, (BatchID)0);
    ASSERT_TRUE(
        engine.submitTransfer(batch, {makeLocalWrite(buffer.data(), kReqLen)})
            .ok());

    ASSERT_TRUE(engine.freeBatch(batch).ok());

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(1000);
    while (std::chrono::steady_clock::now() < deadline &&
           fake_rdma->status_calls.load() < 2) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    EXPECT_GE(fake_rdma->status_calls.load(), 2);

    TransferStatus status{};
    EXPECT_TRUE(engine.getTransferStatus(batch, 0, status).IsInvalidArgument());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, PollingDerivedTaskCompletesMergedOwner) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20, true);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake_rdma);

    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> buffer(kReqLen * 2, 0x44);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());

    BatchID batch = engine.allocateBatch(2);
    ASSERT_NE(batch, (BatchID)0);

    ASSERT_TRUE(
        engine
            .submitTransfer(batch,
                            {makeLocalWrite(buffer.data(), kReqLen),
                             makeLocalWrite(buffer.data() + kReqLen, kReqLen)})
            .ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);

    TransferStatus derived{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 1, derived).ok());
    EXPECT_EQ(derived.s, TransferStatusEnum::COMPLETED);

    TransferStatus owner{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, owner).ok());
    EXPECT_EQ(owner.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, ReceiverCreditDefersUbUntilTerminalRelease) {
    constexpr size_t kReqLen = 4096;
    auto cfg = makeRuntimeQueueConfig(2, 1UL << 20, true);
    cfg->set("receiver_credit/enable", true);
    cfg->set("receiver_credit/data_bytes", uint64_t{kReqLen});
    cfg->set("receiver_credit/request_slots", uint64_t{1});
    cfg->set("receiver_credit/freshness_ttl_ms", uint32_t{1000});
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    std::atomic<bool> complete_first{false};
    auto fake_ub = std::make_shared<FakeTransport>(
        UB, [&complete_first](const Request& request, int) {
            if (!complete_first.load(std::memory_order_acquire)) {
                return TransferStatus{TransferStatusEnum::PENDING, 0};
            }
            return TransferStatus{TransferStatusEnum::COMPLETED,
                                  request.length};
        });
    installFakeUb(engine, fake_ub);

    std::vector<uint8_t> buffer(kReqLen * 2, 0x71);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(2);
    ASSERT_NE(batch, (BatchID)0);

    ASSERT_TRUE(
        engine
            .submitTransfer(
                batch, {makeLocalUbWrite(buffer.data(), kReqLen),
                        makeLocalUbWrite(buffer.data() + kReqLen, kReqLen)})
            .ok());
    // Both owners fit the local dispatch window, but receiver credit permits
    // only one physical UB request. Merging must stop at the advertised hard
    // byte capacity, so the second owner remains queued instead of making an
    // inadmissible 8 KiB owner.
    EXPECT_EQ(fake_ub->submit_calls.load(), 1);
    TransferStatus second{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 1, second).ok());
    EXPECT_EQ(second.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(fake_ub->submit_calls.load(), 1);

    complete_first.store(true, std::memory_order_release);
    TransferStatus first{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, first).ok());
    EXPECT_EQ(first.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(fake_ub->submit_calls.load(), 2);
    ASSERT_TRUE(engine.getTransferStatus(batch, 1, second).ok());
    EXPECT_EQ(second.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, ZeroHandoffUbFailureRollsCreditBack) {
    constexpr size_t kReqLen = 4096;
    auto cfg = makeRuntimeQueueConfig(2, 1UL << 20);
    cfg->set("receiver_credit/enable", true);
    cfg->set("receiver_credit/data_bytes", uint64_t{kReqLen});
    cfg->set("receiver_credit/request_slots", uint64_t{1});
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_ub = std::make_shared<FakeTransport>(UB);
    fake_ub->fail_next_submits.store(1);
    installFakeUb(engine, fake_ub);
    std::vector<uint8_t> buffer(kReqLen * 2, 0x72);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(2);
    ASSERT_NE(batch, (BatchID)0);

    ASSERT_TRUE(
        engine
            .submitTransfer(
                batch, {makeLocalUbWrite(buffer.data(), kReqLen),
                        makeLocalUbWrite(buffer.data() + kReqLen, kReqLen)})
            .ok());
    // The first submit failed before handoff. Its cumulative rollback is
    // reported to the receiver before the next owner gets a fresh grant.
    EXPECT_EQ(fake_ub->submit_calls.load(), 2);
    TransferStatus first{}, second{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, first).ok());
    EXPECT_EQ(first.s, TransferStatusEnum::FAILED);
    ASSERT_TRUE(engine.getTransferStatus(batch, 1, second).ok());
    EXPECT_EQ(second.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, RemoteReceiverCreditExchangeGatesUbDispatch) {
    constexpr size_t kReqLen = 4096;
    auto receiver_cfg = makeRuntimeQueueConfig(2, 1UL << 20);
    receiver_cfg->set("receiver_credit/receiver_enable", true);
    receiver_cfg->set("receiver_credit/data_bytes", uint64_t{kReqLen});
    receiver_cfg->set("receiver_credit/request_slots", uint64_t{1});
    auto sender_cfg = makeRuntimeQueueConfig(2, 1UL << 20);
    sender_cfg->set("receiver_credit/sender_enable", true);
    sender_cfg->set("receiver_credit/data_bytes", uint64_t{kReqLen});
    sender_cfg->set("receiver_credit/request_slots", uint64_t{1});

    TransferEngineImpl receiver(receiver_cfg);
    TransferEngineImpl sender(sender_cfg);
    ASSERT_TRUE(receiver.available());
    ASSERT_TRUE(sender.available());

    auto receiver_ub = std::make_shared<FakeTransport>(UB);
    installFakeUb(receiver, receiver_ub);
    std::atomic<bool> complete_first{false};
    auto sender_ub = std::make_shared<FakeTransport>(
        UB, [&complete_first](const Request& request, int) {
            if (!complete_first.load(std::memory_order_acquire)) {
                return TransferStatus{TransferStatusEnum::PENDING, 0};
            }
            return TransferStatus{TransferStatusEnum::COMPLETED,
                                  request.length};
        });
    installFakeUb(sender, sender_ub);

    std::vector<uint8_t> source(kReqLen * 2, 0x81);
    std::vector<uint8_t> target(kReqLen * 2, 0);
    ASSERT_TRUE(sender.registerLocalMemory(source.data(), source.size()).ok());
    ASSERT_TRUE(
        receiver.registerLocalMemory(target.data(), target.size()).ok());
    SegmentID remote = LOCAL_SEGMENT_ID;
    ASSERT_TRUE(sender.openSegment(remote, receiver.getSegmentName()).ok());
    ASSERT_NE(remote, LOCAL_SEGMENT_ID);

    BatchID batch = sender.allocateBatch(2);
    ASSERT_NE(batch, (BatchID)0);
    ASSERT_TRUE(
        sender
            .submitTransfer(
                batch, {makeRemoteUbWrite(source.data(), remote, target.data(),
                                          kReqLen),
                        makeRemoteUbWrite(source.data() + kReqLen, remote,
                                          target.data() + kReqLen, kReqLen)})
            .ok());
    EXPECT_EQ(sender_ub->submit_calls.load(), 1);

    // TCP fallback carries the same receiver-session fence on the wire. The
    // receiver rejects an unfenced or stale request even if the sender's
    // metadata cache still describes the old process incarnation.
    const auto receiver_addr = receiver.getSegmentName();
    const uint64_t session_high = sender_ub->last_credit_session_high.load();
    const uint64_t session_low = sender_ub->last_credit_session_low.load();
    const uint64_t epoch = sender_ub->last_credit_epoch.load();
    ASSERT_NE(session_high | session_low, 0u);
    ASSERT_NE(epoch, 0u);

    auto delegated = makeLocalUbWrite(target.data(), kReqLen);
    delegated.receiver_credit_session_high = session_high;
    delegated.receiver_credit_session_low = session_low;
    delegated.receiver_credit_epoch = epoch + 1;
    EXPECT_TRUE(
        ControlClient::delegate(receiver_addr, delegated).IsRpcServiceError());
    delegated.receiver_credit_epoch = epoch;
    ASSERT_TRUE(ControlClient::delegate(receiver_addr, delegated).ok());
    EXPECT_EQ(receiver_ub->submit_calls.load(), 1);

    delegated.receiver_credit_epoch = 0;
    EXPECT_TRUE(
        ControlClient::delegate(receiver_addr, delegated).IsInvalidArgument());

    EXPECT_TRUE(ControlClient::sendData(
                    receiver_addr, reinterpret_cast<uint64_t>(target.data()),
                    source.data(), kReqLen)
                    .IsRpcServiceError());
    EXPECT_EQ(target[0], 0);
    EXPECT_TRUE(ControlClient::sendData(
                    receiver_addr, reinterpret_cast<uint64_t>(target.data()),
                    source.data(), kReqLen, session_high, session_low,
                    epoch + 1)
                    .IsRpcServiceError());
    EXPECT_EQ(target[0], 0);
    const auto fenced_send = ControlClient::sendData(
        receiver_addr, reinterpret_cast<uint64_t>(target.data()), source.data(),
        kReqLen, session_high, session_low, epoch);
    ASSERT_TRUE(fenced_send.ok()) << fenced_send.ToString();
    EXPECT_EQ(target[0], source[0]);

    std::vector<uint8_t> readback(kReqLen, 0);
    EXPECT_TRUE(ControlClient::recvData(
                    receiver_addr, reinterpret_cast<uint64_t>(target.data()),
                    readback.data(), kReqLen, session_high, session_low,
                    epoch + 1)
                    .IsRpcServiceError());
    const auto fenced_recv = ControlClient::recvData(
        receiver_addr, reinterpret_cast<uint64_t>(target.data()),
        readback.data(), kReqLen, session_high, session_low, epoch);
    ASSERT_TRUE(fenced_recv.ok()) << fenced_recv.ToString();
    EXPECT_EQ(readback, std::vector<uint8_t>(kReqLen, 0x81));

    complete_first.store(true, std::memory_order_release);
    TransferStatus first{}, second{};
    ASSERT_TRUE(sender.getTransferStatus(batch, 0, first).ok());
    EXPECT_EQ(first.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(sender_ub->submit_calls.load(), 2);
    ASSERT_TRUE(sender.getTransferStatus(batch, 1, second).ok());
    EXPECT_EQ(second.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(sender.freeBatch(batch).ok());
    EXPECT_TRUE(sender.closeSegment(remote).ok());
    EXPECT_TRUE(
        sender.unregisterLocalMemory(source.data(), source.size()).ok());
    EXPECT_TRUE(
        receiver.unregisterLocalMemory(target.data(), target.size()).ok());
}

TEST(RuntimeQueueDispatch, RdmaFailureGatesUbFailoverWithReceiverCredit) {
    constexpr size_t kReqLen = 4096;
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    cfg->set("receiver_credit/enable", true);
    cfg->set("receiver_credit/data_bytes", uint64_t{kReqLen});
    cfg->set("receiver_credit/request_slots", uint64_t{1});
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma =
        std::make_shared<FakeTransport>(RDMA, [](const Request&, int) {
            return TransferStatus{TransferStatusEnum::FAILED, 0};
        });
    auto fake_ub = std::make_shared<FakeTransport>(UB);
    installFakeRdma(engine, fake_rdma);
    installFakeUb(engine, fake_ub);

    std::vector<uint8_t> buffer(kReqLen, 0x91);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(1);
    ASSERT_NE(batch, (BatchID)0);
    ASSERT_TRUE(engine
                    .submitTransfer(
                        batch, {makeLocalNetworkWrite(buffer.data(), kReqLen)})
                    .ok());
    ASSERT_EQ(fake_rdma->submit_calls.load(), 1);
    ASSERT_EQ(fake_ub->submit_calls.load(), 0);

    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(fake_ub->submit_calls.load(), 1);
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, ReceiverCreditAlsoGatesInitialRdmaPath) {
    constexpr size_t kReqLen = 4096;
    auto cfg = makeRuntimeQueueConfig(2, 1UL << 20);
    cfg->set("receiver_credit/enable", true);
    cfg->set("receiver_credit/data_bytes", uint64_t{kReqLen});
    cfg->set("receiver_credit/request_slots", uint64_t{1});
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    std::atomic<bool> complete_first{false};
    auto fake_rdma = std::make_shared<FakeTransport>(
        RDMA, [&complete_first](const Request& request, int) {
            if (!complete_first.load(std::memory_order_acquire)) {
                return TransferStatus{TransferStatusEnum::PENDING, 0};
            }
            return TransferStatus{TransferStatusEnum::COMPLETED,
                                  request.length};
        });
    installFakeRdma(engine, fake_rdma);

    std::vector<uint8_t> buffer(kReqLen * 2, 0x92);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(2);
    ASSERT_NE(batch, (BatchID)0);
    ASSERT_TRUE(
        engine
            .submitTransfer(batch,
                            {makeLocalWrite(buffer.data(), kReqLen),
                             makeLocalWrite(buffer.data() + kReqLen, kReqLen)})
            .ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);
    EXPECT_NE(fake_rdma->last_credit_session_high.load() |
                  fake_rdma->last_credit_session_low.load(),
              0u);
    EXPECT_EQ(fake_rdma->last_credit_epoch.load(), 1u);

    complete_first.store(true, std::memory_order_release);
    TransferStatus first{}, second{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, first).ok());
    EXPECT_EQ(first.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(fake_rdma->submit_calls.load(), 2);
    ASSERT_TRUE(engine.getTransferStatus(batch, 1, second).ok());
    EXPECT_EQ(second.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, RejectsRequestLargerThanReceiverCreditCapacity) {
    constexpr size_t kCapacity = 4096;
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    cfg->set("receiver_credit/enable", true);
    cfg->set("receiver_credit/data_bytes", uint64_t{kCapacity});
    cfg->set("receiver_credit/request_slots", uint64_t{1});
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_ub = std::make_shared<FakeTransport>(UB);
    installFakeUb(engine, fake_ub);
    std::vector<uint8_t> buffer(kCapacity * 2, 0x93);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(1);
    ASSERT_NE(batch, (BatchID)0);

    const auto status = engine.submitTransfer(
        batch, {makeLocalUbWrite(buffer.data(), buffer.size())});
    EXPECT_TRUE(status.IsTooManyRequests()) << status.ToString();
    EXPECT_EQ(fake_ub->submit_calls.load(), 0);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, TcpControlDataUsesLegacyFramingWithoutCredit) {
    constexpr size_t kLength = 256;
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());
    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake_rdma);

    std::vector<uint8_t> source(kLength, 0xa5);
    std::vector<uint8_t> target(kLength, 0);
    std::vector<uint8_t> readback(kLength, 0);
    ASSERT_TRUE(engine.registerLocalMemory(target.data(), target.size()).ok());
    const auto server_addr = engine.getSegmentName();

    ASSERT_TRUE(ControlClient::sendData(
                    server_addr, reinterpret_cast<uint64_t>(target.data()),
                    source.data(), source.size())
                    .ok());
    EXPECT_EQ(target, source);
    ASSERT_TRUE(ControlClient::recvData(
                    server_addr, reinterpret_cast<uint64_t>(target.data()),
                    readback.data(), readback.size())
                    .ok());
    EXPECT_EQ(readback, source);

    EXPECT_TRUE(ControlClient::sendData(
                    server_addr, reinterpret_cast<uint64_t>(target.data()),
                    source.data(), source.size(), 1, 0, 0)
                    .IsInvalidArgument());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(target.data(), target.size()).ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
