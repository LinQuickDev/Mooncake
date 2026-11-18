// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "tent/runtime/segment_manager.h"
#include "tent/runtime/segment_registry.h"
#include "tent/transport/ub/buffers.h"
#include "tent/transport/ub/context.h"
#include "tent/transport/ub/endpoint_store.h"
#include "tent/transport/ub/quota.h"
#include "tent/transport/ub/rail_monitor.h"
#include "tent/transport/ub/topology_attrs.h"
#include "tent/transport/ub/ub_transport.h"
#include "tent/transport/ub/workers.h"
#include "ub_fake_topology.h"
#include "ub_fake_urma.h"

namespace mooncake::tent::ub {
namespace {

using namespace fake;
TEST(UbNativeDataPathTest, LocalBuffersRequestProviderLocalOnlyAccess) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, fakeDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());
    UbBufferManager buffers(adapter, {context});

    std::array<char, 64> storage{};
    BufferDesc descriptor{};
    descriptor.addr = reinterpret_cast<uint64_t>(storage.data());
    descriptor.length = storage.size();
    descriptor.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kLocalReadWrite;

    ASSERT_TRUE(buffers.addBuffer(descriptor, options).ok());
    EXPECT_EQ(adapter->lastRegisteredAccess(), SEGMENT_ACCESS_LOCAL_ONLY);

    EXPECT_TRUE(buffers.clear().ok());
    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbNativeDataPathTest,
     MissingCompletionIsFencedBeforeRetryAndEventuallyCompletes) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, fakeDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());
    std::vector<UbContextPtr> contexts{context};
    auto topology = fakeTopology();
    UbBufferManager buffers(adapter, contexts);

    std::array<char, 64> source{};
    std::array<char, 64> target{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>(i + 1);
    }
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    ASSERT_TRUE(buffers.addBuffer(source_desc, options).ok());
    ASSERT_TRUE(buffers.addBuffer(target_desc, options).ok());

    SegmentManager manager(std::make_unique<NullRegistry>());
    ASSERT_TRUE(manager
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = "local";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        auto& memory =
                            std::get<MemorySegmentDesc>(segment.detail);
                        memory.topology = *topology;
                        memory.buffers = {source_desc, target_desc};
                        return Status::OK();
                    })
                    .ok());

    EndpointStore endpoints(adapter, 16, 1);
    RailMonitor rails;
    QuotaManager quota;
    UbParams params;
    params.worker_count = 1;
    params.poller_count = 1;
    params.slice_size = 16;
    params.max_retries = 1;
    params.slice_timeout_ms = 20;

    EndpointResolver resolver = [&](const EndpointResolveRequest& request,
                                    std::shared_ptr<UbEndpoint>& endpoint) {
        UbEndpointKey key{request.local_context->topologyId(),
                          request.remote_segment_id, request.remote_topology_id,
                          "local@ub:fake0:eid0"};
        auto status =
            endpoints.getOrCreate(key, request.local_context, endpoint);
        if (!status.ok() || endpoint->ready()) return status;
        UbBootstrapDesc peer;
        peer.local_eid = fakeDevice().eid;
        peer.endpoint_generation = 100;
        peer.jetty_ids = {777};
        return endpoint->bind(peer);
    };
    UbWorkers workers(adapter, contexts, topology, &manager, &buffers, &rails,
                      &quota, params, std::move(resolver),
                      [&](const std::shared_ptr<UbEndpoint>& endpoint) {
                          (void)endpoints.retire(endpoint);
                      });
    ASSERT_TRUE(workers.start().ok());
    adapter->holdNextCompletion();
    adapter->dropNextQuiescedCompletion();

    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();
    auto task = UbTask::create(request);
    for (size_t offset = 0; offset < request.length; offset += 16) {
        ASSERT_NE(task->addSlice(UbSliceSpec{
                      source.data() + offset,
                      reinterpret_cast<uint64_t>(target.data() + offset), 16,
                      offset, 1}),
                  nullptr);
    }
    ASSERT_TRUE(task->seal());
    ASSERT_TRUE(workers.submit(task).ok());

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (task->transferStatus().s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(task->transferStatus().s, COMPLETED);
    EXPECT_EQ(task->transferStatus().transferred_bytes, source.size());
    EXPECT_EQ(source, target);
    EXPECT_GT(rails.aggregateBandwidth(), 0.0);
    EXPECT_GE(rails.stats(UbPostPath{0, LOCAL_SEGMENT_ID, 0, 1}).timeouts, 1U);
    EXPECT_EQ(adapter->pendingCount(), 0U);
    EXPECT_GE(adapter->quiesceCalls(), 1U);

    EXPECT_TRUE(workers.stop().ok());
    EXPECT_TRUE(endpoints.clear().ok());
    EXPECT_TRUE(buffers.clear().ok());
    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbNativeDataPathTest,
     ProviderLostCompletionIsReclaimedAfterSuccessfulFence) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, fakeDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());
    std::vector<UbContextPtr> contexts{context};
    auto topology = fakeTopology();
    UbBufferManager buffers(adapter, contexts);

    std::array<char, 64> source{};
    std::array<char, 64> target{};
    std::array<char, 64> recovered{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>(i + 1);
    }
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    BufferDesc recovered_desc{};
    recovered_desc.addr = reinterpret_cast<uint64_t>(recovered.data());
    recovered_desc.length = recovered.size();
    recovered_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    ASSERT_TRUE(buffers.addBuffer(source_desc, options).ok());
    ASSERT_TRUE(buffers.addBuffer(target_desc, options).ok());
    ASSERT_TRUE(buffers.addBuffer(recovered_desc, options).ok());

    SegmentManager manager(std::make_unique<NullRegistry>());
    ASSERT_TRUE(
        manager
            .updateLocal([&](SegmentDesc& segment) {
                segment.name = "local";
                segment.type = SegmentType::Memory;
                segment.detail = MemorySegmentDesc{};
                auto& memory = std::get<MemorySegmentDesc>(segment.detail);
                memory.topology = *topology;
                memory.buffers = {source_desc, target_desc, recovered_desc};
                return Status::OK();
            })
            .ok());

    EndpointStore endpoints(adapter, 16, 1);
    RailMonitor rails;
    QuotaManager quota;
    UbParams params;
    params.worker_count = 1;
    params.poller_count = 1;
    params.slice_size = 16;
    params.max_retries = 1;
    params.slice_timeout_ms = 20;

    std::shared_ptr<UbEndpoint> fenced_endpoint;
    EndpointResolver resolver = [&](const EndpointResolveRequest& request,
                                    std::shared_ptr<UbEndpoint>& endpoint) {
        UbEndpointKey key{request.local_context->topologyId(),
                          request.remote_segment_id, request.remote_topology_id,
                          "local@ub:fake0:eid0"};
        auto status =
            endpoints.getOrCreate(key, request.local_context, endpoint);
        if (!status.ok()) return status;
        if (!fenced_endpoint) fenced_endpoint = endpoint;
        if (endpoint->ready()) return status;
        UbBootstrapDesc peer;
        peer.local_eid = fakeDevice().eid;
        peer.endpoint_generation = 100;
        peer.jetty_ids = {777};
        return endpoint->bind(peer);
    };
    UbWorkers workers(adapter, contexts, topology, &manager, &buffers, &rails,
                      &quota, params, std::move(resolver),
                      [&](const std::shared_ptr<UbEndpoint>& endpoint) {
                          (void)endpoints.retire(endpoint);
                      });
    ASSERT_TRUE(workers.start().ok());
    adapter->holdNextCompletion();
    adapter->dropQuiesceCompletions();

    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();
    auto task = UbTask::create(request);
    for (size_t offset = 0; offset < request.length; offset += 16) {
        ASSERT_NE(task->addSlice(UbSliceSpec{
                      source.data() + offset,
                      reinterpret_cast<uint64_t>(target.data() + offset), 16,
                      offset, 1}),
                  nullptr);
    }
    ASSERT_TRUE(task->seal());
    ASSERT_TRUE(workers.submit(task).ok());

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (task->transferStatus().s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(task->transferStatus().s, COMPLETED);
    EXPECT_EQ(task->transferStatus().transferred_bytes, source.size());
    EXPECT_EQ(source, target);
    EXPECT_GE(rails.stats(UbPostPath{0, LOCAL_SEGMENT_ID, 0, 1}).timeouts, 1U);
    EXPECT_GE(adapter->quiesceCalls(), 1U);

    EXPECT_EQ(workers.inflightCount(), 0U);
    EXPECT_EQ(quota.activeReservationCount(), 0U);
    ASSERT_NE(fenced_endpoint, nullptr);
    EXPECT_EQ(fenced_endpoint->outstandingWrs(), 0U);
    EXPECT_EQ(fenced_endpoint->outstandingBytes(), 0U);

    Request followup{};
    followup.opcode = Request::WRITE;
    followup.source = source.data();
    followup.target_id = LOCAL_SEGMENT_ID;
    followup.target_offset = reinterpret_cast<uint64_t>(recovered.data());
    followup.length = source.size();
    auto followup_task = UbTask::create(followup);
    for (size_t offset = 0; offset < followup.length; offset += 16) {
        ASSERT_NE(followup_task->addSlice(UbSliceSpec{
                      source.data() + offset,
                      reinterpret_cast<uint64_t>(recovered.data() + offset), 16,
                      offset, 1}),
                  nullptr);
    }
    ASSERT_TRUE(followup_task->seal());
    ASSERT_TRUE(workers.submit(followup_task).ok());

    const auto followup_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (followup_task->transferStatus().s == PENDING &&
           std::chrono::steady_clock::now() < followup_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(followup_task->transferStatus().s, COMPLETED);
    EXPECT_EQ(followup_task->transferStatus().transferred_bytes, source.size());
    EXPECT_EQ(recovered, source);

    EXPECT_TRUE(workers.stop().ok());
    EXPECT_TRUE(endpoints.clear().ok());
    EXPECT_TRUE(buffers.clear().ok());
    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

// ---------------------------------------------------------------------------
// W2 completion-gate integration cases (design §11.2 I-1..I-3, D-7).

namespace {

// Minimal harness for the gate cases: one local context, one registered
// source/target pair, one resolvable endpoint and a started worker pool. The
// existing cases stay untouched so their baseline behaviour keeps being
// asserted verbatim.
struct GateEnv {
    std::shared_ptr<FakeUrmaAdapter> adapter;
    std::shared_ptr<UbContext> context;
    std::vector<UbContextPtr> contexts;
    std::shared_ptr<Topology> topology;
    std::unique_ptr<UbBufferManager> buffers;
    std::unique_ptr<SegmentManager> manager;
    std::unique_ptr<EndpointStore> endpoints;
    std::unique_ptr<RailMonitor> rails;
    std::unique_ptr<QuotaManager> quota;
    std::unique_ptr<UbWorkers> workers;
    std::shared_ptr<UbEndpoint> endpoint;
    std::array<char, 64> source{};
    std::array<char, 64> target{};
    UbParams params;

    GateEnv() = default;
    GateEnv(const GateEnv&) = delete;
    GateEnv& operator=(const GateEnv&) = delete;

    bool initialize(uint32_t slice_timeout_ms = 20) {
        adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
        if (!adapter->initialize().ok()) return false;
        context = std::make_shared<UbContext>(0, fakeDevice(), adapter);
        if (!context->initialize(1, JfcOptions{}).ok()) return false;
        contexts = {context};
        topology = fakeTopology();
        buffers = std::make_unique<UbBufferManager>(adapter, contexts);
        for (size_t i = 0; i < source.size(); ++i) {
            source[i] = static_cast<char>(i + 1);
        }
        BufferDesc source_desc{};
        source_desc.addr = reinterpret_cast<uint64_t>(source.data());
        source_desc.length = source.size();
        source_desc.location = kWildcardLocation;
        BufferDesc target_desc{};
        target_desc.addr = reinterpret_cast<uint64_t>(target.data());
        target_desc.length = target.size();
        target_desc.location = kWildcardLocation;
        MemoryOptions options;
        options.perm = kGlobalReadWrite;
        if (!buffers->addBuffer(source_desc, options).ok()) return false;
        if (!buffers->addBuffer(target_desc, options).ok()) return false;

        manager =
            std::make_unique<SegmentManager>(std::make_unique<NullRegistry>());
        if (!manager
                 ->updateLocal([&](SegmentDesc& segment) {
                     segment.name = "local";
                     segment.type = SegmentType::Memory;
                     segment.detail = MemorySegmentDesc{};
                     auto& memory = std::get<MemorySegmentDesc>(segment.detail);
                     memory.topology = *topology;
                     memory.buffers = {source_desc, target_desc};
                     return Status::OK();
                 })
                 .ok()) {
            return false;
        }
        endpoints = std::make_unique<EndpointStore>(adapter, 16, 1);
        rails = std::make_unique<RailMonitor>();
        quota = std::make_unique<QuotaManager>();
        params.worker_count = 1;
        params.poller_count = 1;
        params.slice_size = 16;
        params.max_retries = 1;
        params.slice_timeout_ms = slice_timeout_ms;
        EndpointResolver resolver = [&](const EndpointResolveRequest& request,
                                        std::shared_ptr<UbEndpoint>& resolved) {
            UbEndpointKey key{
                request.local_context->topologyId(), request.remote_segment_id,
                request.remote_topology_id, "local@ub:fake0:eid0"};
            auto status =
                endpoints->getOrCreate(key, request.local_context, resolved);
            if (!status.ok()) return status;
            if (!endpoint) endpoint = resolved;
            if (resolved->ready()) return status;
            UbBootstrapDesc peer;
            peer.local_eid = fakeDevice().eid;
            peer.endpoint_generation = 100;
            peer.jetty_ids = {777};
            return resolved->bind(peer);
        };
        workers = std::make_unique<UbWorkers>(
            adapter, contexts, topology, manager.get(), buffers.get(),
            rails.get(), quota.get(), params, std::move(resolver),
            [&](const std::shared_ptr<UbEndpoint>& retired) {
                (void)endpoints->retire(retired);
            });
        return workers->start().ok();
    }

    ~GateEnv() {
        if (workers) {
            EXPECT_TRUE(workers->stop().ok());
        }
        workers.reset();
        if (endpoints) {
            EXPECT_TRUE(endpoints->clear().ok());
        }
        endpoints.reset();
        if (buffers) {
            EXPECT_TRUE(buffers->clear().ok());
        }
        buffers.reset();
        manager.reset();
        if (context) {
            EXPECT_TRUE(context->shutdown().ok());
        }
        context.reset();
        if (adapter) {
            EXPECT_TRUE(adapter->shutdown().ok());
        }
    }

    // Submits one write of `length` bytes with exactly one slice (so one record
    // and one native post), keeping the task alive for the caller.
    UbTask::Ptr submitWrite(size_t length = 16, const char* sink = nullptr) {
        const char* destination = sink ? sink : target.data();
        Request request{};
        request.opcode = Request::WRITE;
        request.source = source.data();
        request.target_id = LOCAL_SEGMENT_ID;
        request.target_offset = reinterpret_cast<uint64_t>(destination);
        request.length = length;
        auto task = UbTask::create(request);
        if (!task->addSlice(UbSliceSpec{source.data(),
                                        reinterpret_cast<uint64_t>(destination),
                                        length, 0, 1})) {
            return nullptr;
        }
        if (!task->seal()) return nullptr;
        if (!workers->submit(task).ok()) return nullptr;
        return task;
    }

    static bool waitFor(
        const std::function<bool()>& predicate,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(3000)) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (predicate()) return true;
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
        return predicate();
    }

    [[nodiscard]] JfcPtr businessJfc() const {
        return context->jfc(0)->handle();
    }
};

}  // namespace

// I-1: a completion that beats the blocking post call must not release the
// endpoint or the quota while the post call stack is still alive, and the
// settlement must happen exactly once afterwards.
TEST(UbNativeDataPathTest, EarlyCompletionWaitsForPostReturn) {
    GateEnv env;
    ASSERT_TRUE(env.initialize(/*slice_timeout_ms=*/20));

    std::atomic<uint64_t> outstanding_inside_gate{0};
    std::atomic<uint64_t> reservations_inside_gate{0};
    std::atomic<uint64_t> records_inside_gate{0};
    std::atomic<uint64_t> returned_inside_gate{0};
    std::atomic<uint64_t> gate_runs{0};
    std::atomic<bool> early_cqe_inside_gate{false};

    env.adapter->setPostGate([&](uint64_t) {
        // Wait until the poller settled the completion while this call stack
        // still holds its handler reference.
        early_cqe_inside_gate.store(
            GateEnv::waitFor(
                [&] { return env.workers->completionStats().early_cqe > 0; }),
            std::memory_order_release);
        outstanding_inside_gate.store(env.endpoint->outstandingWrs(),
                                      std::memory_order_relaxed);
        reservations_inside_gate.store(env.quota->activeReservationCount(),
                                       std::memory_order_relaxed);
        records_inside_gate.store(env.workers->inflightCount(),
                                  std::memory_order_relaxed);
        returned_inside_gate.store(
            env.workers->completionStats().record_returned,
            std::memory_order_relaxed);
        gate_runs.fetch_add(1, std::memory_order_relaxed);
    });

    auto task = env.submitWrite();
    ASSERT_NE(task, nullptr);
    ASSERT_TRUE(
        GateEnv::waitFor([&] { return task->transferStatus().s != PENDING; }));

    EXPECT_TRUE(early_cqe_inside_gate.load(std::memory_order_acquire));
    EXPECT_GE(gate_runs.load(std::memory_order_relaxed), 1U);
    // Nothing was released while the post had not returned (the whole point of
    // W2): the WR still owns its endpoint slot, its quota and its record.
    EXPECT_EQ(outstanding_inside_gate.load(std::memory_order_relaxed), 1U);
    EXPECT_EQ(reservations_inside_gate.load(std::memory_order_relaxed), 1U);
    EXPECT_EQ(records_inside_gate.load(std::memory_order_relaxed), 1U);
    EXPECT_EQ(returned_inside_gate.load(std::memory_order_relaxed), 0U);

    EXPECT_EQ(task->transferStatus().s, COMPLETED);
    EXPECT_EQ(task->transferStatus().transferred_bytes, 16U);
    EXPECT_EQ(env.source[0], env.target[0]);

    const auto stats = env.workers->completionStats();
    EXPECT_GE(stats.early_cqe, 1U);
    EXPECT_EQ(stats.record_published, 1U);
    EXPECT_EQ(stats.record_settled, 1U);
    EXPECT_EQ(stats.record_returned, 1U);
    EXPECT_EQ(stats.record_finalized, 0U);
    EXPECT_EQ(stats.record_rejected, 0U);
    EXPECT_EQ(stats.duplicate_cr, 0U);
    EXPECT_EQ(stats.unknown_token, 0U);
    EXPECT_EQ(stats.one_shot_violations, 0U);
    EXPECT_EQ(stats.handler_ref_leak_on_finalize, 0U);

    EXPECT_EQ(env.workers->inflightCount(), 0U);
    EXPECT_EQ(env.quota->activeReservationCount(), 0U);
    ASSERT_NE(env.endpoint, nullptr);
    EXPECT_EQ(env.endpoint->outstandingWrs(), 0U);
    env.adapter->clearPostGate();
}

// I-2: an unknown token is accounted, reported through the interface-8 seam and
// neither crashes the poller nor disturbs any other responsibility.
TEST(UbNativeDataPathTest, UnknownCompletionTokenIsCountedAndIsolated) {
    GateEnv env;
    ASSERT_TRUE(env.initialize());

    const uint64_t unknown_token = (7ULL << 32) | 0x5A5A;
    env.adapter->injectCompletion(env.businessJfc(), unknown_token);
    ASSERT_TRUE(GateEnv::waitFor(
        [&] { return env.workers->completionStats().unknown_token >= 1; }));
    EXPECT_EQ(env.workers->completionStats().duplicate_cr, 0U);
    EXPECT_GE(env.workers->entityEvents().unknownTokenCount(), 1U);
    EXPECT_EQ(env.adapter->injectedCompletionCount(), 1U);

    // The poller keeps working and an unrelated transfer still completes.
    auto task = env.submitWrite();
    ASSERT_NE(task, nullptr);
    ASSERT_TRUE(
        GateEnv::waitFor([&] { return task->transferStatus().s != PENDING; }));
    EXPECT_EQ(task->transferStatus().s, COMPLETED);
    EXPECT_EQ(env.workers->completionStats().record_returned, 1U);
    EXPECT_EQ(env.workers->inflightCount(), 0U);
}

// I-3: a duplicate completion for a still-registered record is counted, never
// applied twice, and the record is returned exactly once. The second completion
// is injected from the post gate so it is guaranteed to arrive while the record
// is still registered (an unknown token would prove the record was already
// disposed instead).
TEST(UbNativeDataPathTest, DuplicateCompletionIsCountedNotReapplied) {
    GateEnv env;
    ASSERT_TRUE(env.initialize());

    std::atomic<bool> duplicate_seen{false};
    env.adapter->setPostGate([&](uint64_t token) {
        env.adapter->injectCompletion(env.businessJfc(), token);
        duplicate_seen.store(
            GateEnv::waitFor([&] {
                return env.workers->completionStats().duplicate_cr > 0;
            }),
            std::memory_order_release);
    });

    auto task = env.submitWrite();
    ASSERT_NE(task, nullptr);
    ASSERT_TRUE(
        GateEnv::waitFor([&] { return task->transferStatus().s != PENDING; }));

    EXPECT_TRUE(duplicate_seen.load(std::memory_order_acquire));
    EXPECT_EQ(task->transferStatus().s, COMPLETED);
    const auto stats = env.workers->completionStats();
    EXPECT_EQ(stats.duplicate_cr, 1U);
    EXPECT_EQ(stats.record_settled, 1U);
    EXPECT_EQ(stats.record_returned, 1U);
    EXPECT_EQ(stats.record_finalized, 0U);
    EXPECT_EQ(stats.unknown_token, 0U);
    EXPECT_EQ(env.workers->inflightCount(), 0U);
    ASSERT_NE(env.endpoint, nullptr);
    EXPECT_EQ(env.endpoint->outstandingWrs(), 0U);
    EXPECT_EQ(env.quota->activeReservationCount(), 0U);
    env.adapter->clearPostGate();
}

// D-7: a deadline that expires while the native post has not returned must not
// be fenced. The scan defers, nothing is quiesced, and the record completes
// normally once the post returns.
TEST(UbNativeDataPathTest, DeadlineScanDefersWhilePostHasNotReturned) {
    GateEnv env;
    ASSERT_TRUE(env.initialize(/*slice_timeout_ms=*/1));

    std::atomic<uint64_t> quiesce_inside_gate{0};
    std::atomic<bool> deferred{false};
    env.adapter->setPostGate([&](uint64_t) {
        deferred.store(GateEnv::waitFor([&] {
                           return env.workers->completionStats()
                                      .timeout_deferred_by_post_return > 0;
                       }),
                       std::memory_order_release);
        quiesce_inside_gate.store(env.adapter->quiesceCalls(),
                                  std::memory_order_relaxed);
    });

    auto task = env.submitWrite();
    ASSERT_NE(task, nullptr);
    ASSERT_TRUE(
        GateEnv::waitFor([&] { return task->transferStatus().s != PENDING; }));

    EXPECT_TRUE(deferred.load(std::memory_order_acquire));
    // The post had not returned, so the scan must not have fenced or settled.
    EXPECT_EQ(quiesce_inside_gate.load(std::memory_order_relaxed), 0U);
    EXPECT_EQ(task->transferStatus().s, COMPLETED);
    const auto stats = env.workers->completionStats();
    EXPECT_GE(stats.timeout_deferred_by_post_return, 1U);
    EXPECT_EQ(stats.record_returned, 1U);
    EXPECT_EQ(stats.record_finalized, 0U);
    EXPECT_EQ(env.adapter->quiesceCalls(), 0U);
    EXPECT_EQ(env.rails->stats(UbPostPath{0, LOCAL_SEGMENT_ID, 0, 1}).timeouts,
              0U);
    env.adapter->clearPostGate();
}

TEST(UbNativeDataPathTest, EndpointStoreNeverReusesRetiredGeneration) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, fakeDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());
    EndpointStore store(adapter, 2, 1);
    UbEndpointKey key{0, 9, 0, "peer@ub:fake0:eid0"};
    std::array<std::shared_ptr<UbEndpoint>, 8> concurrent;
    std::array<Status, 8> statuses;
    std::vector<std::thread> threads;
    for (size_t i = 0; i < concurrent.size(); ++i) {
        threads.emplace_back([&, i] {
            statuses[i] = store.getOrCreate(key, context, concurrent[i]);
        });
    }
    for (auto& thread : threads) thread.join();
    for (size_t i = 0; i < concurrent.size(); ++i) {
        ASSERT_TRUE(statuses[i].ok());
        EXPECT_EQ(concurrent[i], concurrent[0]);
    }
    auto first = concurrent[0];
    const uint64_t old_generation = first->generation();
    EXPECT_TRUE(store.retire(key, old_generation));
    std::shared_ptr<UbEndpoint> replacement;
    ASSERT_TRUE(store.getOrCreate(key, context, replacement).ok());
    EXPECT_GT(replacement->generation(), old_generation);
    EXPECT_FALSE(store.retire(key, old_generation));
    EXPECT_EQ(store.get(key), replacement);
    EXPECT_TRUE(store.clear().ok());
    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbNativeDataPathTest,
     EndpointStoreDoesNotReplaceEndpointWhenCleanupCannotComplete) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, fakeDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());
    EndpointStore store(adapter, 1, 1);
    UbEndpointKey key{0, 9, 0, "peer@ub:fake0:eid0"};
    std::shared_ptr<UbEndpoint> endpoint;
    ASSERT_TRUE(store.getOrCreate(key, context, endpoint).ok());
    UbBootstrapDesc peer;
    peer.local_eid = fakeDevice().eid;
    peer.endpoint_generation = 100;
    peer.jetty_ids = {777};
    ASSERT_TRUE(endpoint->bind(peer).ok());

    adapter->failResets();
    std::vector<Completion> completions;
    EXPECT_FALSE(endpoint->quiesce(20, completions).ok());

    std::shared_ptr<UbEndpoint> replacement;
    const auto status = store.getOrCreate(key, context, replacement);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(replacement, nullptr);
    EXPECT_EQ(store.size(), 0U);

    adapter->allowResets();
    EXPECT_TRUE(store.clear().ok());
    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbNativeDataPathTest, UbTransportRunsSelfReadWriteOverNativeControlPlane) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    // A serialized discovery snapshot is informational. Current local
    // discovery and runtime path health remain the scheduling authorities.
    auto topology = fakeTopology(false);
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "fake-machine";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        std::get<MemorySegmentDesc>(segment.detail).topology =
                            *topology;
                        return Status::OK();
                    })
                    .ok());

    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/worker_count", 1);
    config->set("transports/ub/poller_count", 1);
    config->set("transports/ub/jfc_per_context", 1);
    config->set("transports/ub/jetty_per_endpoint", 1);
    config->set("transports/ub/max_endpoints", 16);
    config->set("transports/ub/slice_size", 16);
    config->set("transports/ub/max_slices_per_task", 8);
    config->set("transports/ub/max_retries", 1);
    config->set("transports/ub/slice_timeout_ms", 1000);
    config->set("transports/ub/endpoint_cooldown_ms", 100);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    ASSERT_TRUE(
        transport.install(mutable_segment_name, control, topology, config)
            .ok());
    EXPECT_STREQ(transport.getName(), "ub");
    EXPECT_TRUE(transport.supportsCancellation());
    EXPECT_FALSE(transport.supportNotification());
    EXPECT_TRUE(transport.capabilities().dram_to_dram);

    std::array<char, 64> source{};
    std::array<char, 64> target{};
    std::array<char, 144> oversized_source{};
    std::array<char, 144> oversized_target{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>(100 - i);
    }
    const auto expected = source;
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    BufferDesc oversized_source_desc{};
    oversized_source_desc.addr =
        reinterpret_cast<uint64_t>(oversized_source.data());
    oversized_source_desc.length = oversized_source.size();
    oversized_source_desc.location = kWildcardLocation;
    BufferDesc oversized_target_desc{};
    oversized_target_desc.addr =
        reinterpret_cast<uint64_t>(oversized_target.data());
    oversized_target_desc.length = oversized_target.size();
    oversized_target_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    std::vector<BufferDesc> descriptors{
        source_desc, target_desc, oversized_source_desc, oversized_target_desc};
    ASSERT_TRUE(transport.addMemoryBuffer(descriptors, options).ok());
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail).buffers =
                            descriptors;
                        return Status::OK();
                    })
                    .ok());

    Transport::SubBatchRef rejected_batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(rejected_batch, 1).ok());
    Request oversized_request{};
    oversized_request.opcode = Request::WRITE;
    oversized_request.source = oversized_source.data();
    oversized_request.target_id = LOCAL_SEGMENT_ID;
    oversized_request.target_offset =
        reinterpret_cast<uint64_t>(oversized_target.data());
    oversized_request.length = oversized_source.size();
    EXPECT_TRUE(
        transport.submitTransferTasks(rejected_batch, {oversized_request})
            .IsInvalidArgument());
    EXPECT_TRUE(transport.freeSubBatch(rejected_batch).ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 2).ok());
    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());

    TransferStatus transfer{PENDING, 0};
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 0, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(transfer.s, COMPLETED);
    EXPECT_EQ(transfer.transferred_bytes, source.size());
    EXPECT_EQ(source, target);
    EXPECT_GT(transport.getEstimatedBandwidth(), 0.0);

    source.fill(0);
    Request read_request = request;
    read_request.opcode = Request::READ;
    ASSERT_TRUE(transport.submitTransferTasks(batch, {read_request}).ok());
    transfer = TransferStatus{PENDING, 0};
    const auto read_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < read_deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 1, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(transfer.s, COMPLETED);
    EXPECT_EQ(transfer.transferred_bytes, source.size());
    EXPECT_EQ(source, expected);

    std::vector<NicLoadStats> nic_stats;
    ASSERT_TRUE(transport.getNicLoadStats(nic_stats).ok());
    ASSERT_EQ(nic_stats.size(), 1U);
    EXPECT_EQ(nic_stats.front().device_name, "ub:fake0:eid0");
    EXPECT_EQ(nic_stats.front().inflight_bytes, 0U);
    EXPECT_GT(nic_stats.front().ewma_bandwidth_bps, 0.0);

    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_EQ(batch, nullptr);

    // A failed device fence must make uninstall retryable without destroying
    // pollers, registered memory, or the held WR token.
    Transport::SubBatchRef draining_batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(draining_batch, 1).ok());
    adapter->holdNextCompletion();
    ASSERT_TRUE(transport.submitTransferTasks(draining_batch, {request}).ok());
    const auto posted_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (adapter->pendingCount() == 0 &&
           std::chrono::steady_clock::now() < posted_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(adapter->pendingCount(), 1U);
    adapter->failNextQuiesce();
    EXPECT_FALSE(transport.uninstall().ok());
    EXPECT_EQ(adapter->pendingCount(), 1U);
    adapter->failNextShutdown();
    EXPECT_FALSE(transport.uninstall().ok());
    EXPECT_EQ(adapter->pendingCount(), 0U);
    EXPECT_TRUE(transport.freeSubBatch(draining_batch).ok());
    EXPECT_TRUE(transport.uninstall().ok());
}

}  // namespace
}  // namespace mooncake::tent::ub
