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
//
// Test doubles for the TENT UB probe-link W1a unit tests:
//   * fake segment/Jetty/JFC handles (no URMA provider required);
//   * graph builders for multi-port / multi-peer scenarios;
//   * a recording report sink and a virtual clock;
//   * helpers that drive pendingActions/beginPost/completion.

#ifndef TENT_TESTS_UB_PROBE_TEST_SUPPORT_H_
#define TENT_TESTS_UB_PROBE_TEST_SUPPORT_H_

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "tent/transport/ub/probe_link_manager.h"
#include "tent/transport/ub/probe_resources.h"
#include "tent/transport/ub/probe_types.h"
#include "tent/transport/ub/slice.h"

namespace mooncake {
namespace tent {
namespace ub {
namespace probe_test {

class FakeLocalSegment : public LocalSegment {
   public:
    FakeLocalSegment(uint64_t address, uint64_t length)
        : address_(address), length_(length) {}

    [[nodiscard]] bool valid() const noexcept override { return true; }
    [[nodiscard]] uint64_t address() const noexcept override {
        return address_;
    }
    [[nodiscard]] uint64_t length() const noexcept override { return length_; }
    [[nodiscard]] const SegmentDescriptor& descriptor()
        const noexcept override {
        return descriptor_;
    }

   private:
    uint64_t address_;
    uint64_t length_;
    SegmentDescriptor descriptor_;
};

class FakeRemoteSegment : public RemoteSegment {
   public:
    FakeRemoteSegment(uint64_t address, uint64_t length)
        : address_(address), length_(length) {}

    [[nodiscard]] bool valid() const noexcept override { return true; }
    [[nodiscard]] uint64_t address() const noexcept override {
        return address_;
    }
    [[nodiscard]] uint64_t length() const noexcept override { return length_; }
    [[nodiscard]] const SegmentDescriptor& descriptor()
        const noexcept override {
        return descriptor_;
    }

   private:
    uint64_t address_;
    uint64_t length_;
    SegmentDescriptor descriptor_;
};

class FakeJetty : public Jetty {
   public:
    explicit FakeJetty(uint32_t id, uint32_t uasid = 7)
        : id_(id), uasid_(uasid) {}

    [[nodiscard]] bool valid() const noexcept override { return true; }
    [[nodiscard]] uint32_t id() const noexcept override { return id_; }
    [[nodiscard]] uint32_t uasid() const noexcept override { return uasid_; }

   private:
    uint32_t id_;
    uint32_t uasid_;
};

class FakeJfc : public Jfc {
   public:
    [[nodiscard]] bool valid() const noexcept override { return true; }
    [[nodiscard]] int eventFd() const noexcept override { return -1; }
};

// Virtual clock. Starts non-zero because ProbeLinkManager::normalizedNow(0)
// means "read the injected clock".
class TestClock {
   public:
    explicit TestClock(uint64_t start_ns = 1'000'000) : now_ns_(start_ns) {}

    [[nodiscard]] uint64_t now() const { return now_ns_; }
    void set(uint64_t now_ns) { now_ns_ = now_ns; }
    void advance(uint64_t delta_ns) { now_ns_ += delta_ns; }

    [[nodiscard]] ProbeLinkManager::Clock clock() const {
        auto* self = const_cast<TestClock*>(this);
        return [self]() { return self->now_ns_; };
    }

   private:
    uint64_t now_ns_;
};

struct RetireCall {
    LaneKey lane;
    uint64_t jetty_generation{0};
    uint64_t error_epoch{0};
    PathVersion path;
    Status::Code code{Status::Code::kOk};
};

class RecordingReportSink : public ProbeReportSink {
   public:
    void retireProbeLane(const LaneKey& lane, uint64_t jetty_generation,
                         uint64_t error_epoch, const PathVersion& path,
                         const Status& reason) override {
        retire_calls.push_back(RetireCall{lane, jetty_generation, error_epoch,
                                          path, reason.code()});
    }

    void handoffUnproven(const ProbeHandoff& handoff) override {
        handoffs.push_back(handoff);
        ++handoff_calls;
    }

    std::vector<RetireCall> retire_calls;
    // The handoff payload keeps its segment references so tests can check that
    // the PLM transferred ownership instead of releasing early.
    std::vector<ProbeHandoff> handoffs;
    size_t handoff_calls{0};
};

// Deterministic poster double. Tests drive completion settlement themselves;
// this only records the WR/Target it was asked to post and returns a configured
// result.
enum class FakePostResult : uint8_t {
    kAccepted,
    kRejected,
    kUnknown,
};

struct ProbePostCall {
    uint64_t token{0};
    uint64_t jetty_generation{0};
    uint64_t source_address{0};
    uint64_t source_length{0};
    uint64_t area_address{0};
    uint64_t area_length{0};
    uint32_t native_jetty_id{0};
};

class FakeProbePoster : public ProbePoster {
   public:
    explicit FakeProbePoster(FakePostResult result = FakePostResult::kAccepted)
        : result_(result) {}

    Status post(const ProbePostTarget& target, const ProbeWorkRequest& request,
                size_t& posted) override {
        std::lock_guard<std::mutex> lock(mutex_);
        ProbePostCall call;
        call.token = request.token;
        call.jetty_generation = target.jetty_generation;
        call.source_address = request.source_address;
        call.source_length = request.source_length;
        call.area_address = request.area_address;
        call.area_length = request.area_length;
        if (target.probe_jetty) call.native_jetty_id = target.probe_jetty->id();
        calls_.push_back(std::move(call));
        Status status = Status::OK();
        posted = 0;
        switch (result_) {
            case FakePostResult::kAccepted:
                posted = 1;
                break;
            case FakePostResult::kRejected:
                status = Status::InternalError("fake probe post rejected");
                break;
            case FakePostResult::kUnknown:
                // Provider returns OK but proves no acceptance: unknown.
                break;
        }
        cv_.notify_all();
        return status;
    }

    void setResult(FakePostResult result) {
        std::lock_guard<std::mutex> lock(mutex_);
        result_ = result;
    }

    [[nodiscard]] size_t callCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return calls_.size();
    }

    [[nodiscard]] std::vector<ProbePostCall> calls() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return calls_;
    }

    [[nodiscard]] ProbePostCall lastCall() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return calls_.empty() ? ProbePostCall{} : calls_.back();
    }

    bool waitForCalls(size_t count, std::chrono::milliseconds timeout =
                                        std::chrono::milliseconds(2000)) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout,
                            [this, count] { return calls_.size() >= count; });
    }

   private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<ProbePostCall> calls_;
    FakePostResult result_;
};

template <typename Predicate>
bool waitUntil(Predicate predicate, std::chrono::milliseconds timeout =
                                        std::chrono::milliseconds(2000)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return predicate();
}

// Builds and applies interface-7 events with a monotonic event_seq.
class EventDriver {
   public:
    explicit EventDriver(ProbeLinkManager& plm, uint64_t graph_epoch = 1)
        : plm_(plm), graph_epoch_(graph_epoch) {}

    Status send(ProbeEvent event) {
        event.graph_epoch = graph_epoch_;
        event.event_seq = ++seq_;
        return plm_.onPoolAvailabilityChange(event);
    }

    void nextGraphEpoch() {
        ++graph_epoch_;
        seq_ = 0;
    }

    [[nodiscard]] uint64_t seq() const { return seq_; }

   private:
    ProbeLinkManager& plm_;
    uint64_t graph_epoch_;
    uint64_t seq_{0};
};

inline ProbeParams defaultParams() {
    ProbeParams params;
    params.enabled = true;
    params.interval_ms = 1000;
    params.backoff_max_ms = 4000;
    params.timeout_ms = 500;
    params.peer_budget = 4;
    params.failure_threshold = 3;
    params.jfc_mode = ProbeJfcMode::kDedicated;
    return params;
}

inline ProbePortSupply makePortSupply(Topology::NicID local_nic,
                                      uint64_t generation) {
    ProbePortSupply supply;
    supply.local_nic = local_nic;
    supply.probe_jetty = std::make_shared<FakeJetty>(100 + local_nic);
    supply.jetty_generation = generation;
    supply.probe_jfc = std::make_shared<FakeJfc>();
    supply.source_segment = std::make_shared<FakeLocalSegment>(
        0x100000 + generation * 0x1000, 4096);
    supply.source_address = supply.source_segment->address();
    supply.source_length = 1;
    supply.source_generation = generation;
    supply.control_credit = 1;
    supply.peer_budget = 4;
    return supply;
}

inline ProbeEdgeSupply makeEdgeSupply(
    Topology::NicID local_nic, SegmentID peer_id, Topology::NicID remote_nic,
    uint64_t topology_epoch = 1, uint64_t target_epoch = 1,
    uint64_t area_epoch = 1, bool gates_ok = true) {
    ProbeEdgeSupply edge;
    edge.version = PathVersion{ProbePathKey{local_nic, peer_id, remote_nic},
                               topology_epoch, target_epoch};
    edge.peer = ProbePeerKey{peer_id, "peer-" + std::to_string(peer_id)};
    edge.area_segment = std::make_shared<FakeRemoteSegment>(
        0x200000 + remote_nic * 0x1000, 4096);
    edge.area_address = edge.area_segment->address();
    edge.area_length = 1;
    edge.area_epoch = area_epoch;
    edge.remote_jetty =
        RemoteJettyRef{"eid", 200U + remote_nic, 300U + remote_nic, 0xACFE};
    edge.node_gate_ok = gates_ok;
    edge.port_gate_ok = gates_ok;
    edge.profile_gate_ok = gates_ok;
    return edge;
}

inline ProbeGraph makeGraph(std::vector<ProbePortSupply> ports,
                            std::vector<ProbeEdgeSupply> edges,
                            uint64_t graph_epoch = 1) {
    ProbeGraph graph;
    graph.graph_epoch = graph_epoch;
    graph.ports = std::move(ports);
    graph.edges = std::move(edges);
    return graph;
}

inline UbPostPath makeEvidence(Topology::NicID local_nic, SegmentID peer_id,
                               Topology::NicID remote_nic,
                               uint64_t endpoint_generation = 0) {
    return UbPostPath{local_nic, peer_id, remote_nic, endpoint_generation};
}

inline Completion makeCompletion(
    uint64_t token, CompletionCategory category = CompletionCategory::SUCCESS) {
    Completion completion;
    completion.token = token;
    completion.category = category;
    completion.completed_bytes = 1;
    return completion;
}

inline PathHealthView healthOf(const ProbeLinkManager& plm,
                               const PathVersion& version) {
    for (const auto& entry : plm.healthView()) {
        if (entry.version == version) return entry;
    }
    return PathHealthView{};
}

inline bool hasHealth(const ProbeLinkManager& plm, const PathVersion& version) {
    for (const auto& entry : plm.healthView()) {
        if (entry.version == version) return true;
    }
    return false;
}

inline const CampaignEntryView* campaignEntryOf(const CampaignView& campaign,
                                                const PathVersion& version) {
    for (const auto& entry : campaign.entries) {
        if (entry.version == version) return &entry;
    }
    return nullptr;
}

struct PostedProbe {
    uint64_t token{0};
    std::shared_ptr<ProbeRecord> record;
};

// Posts the first schedulable action and returns its token/record.
inline std::optional<PostedProbe> postNext(ProbeLinkManager& plm,
                                           uint64_t& token_seq,
                                           uint64_t now_ns) {
    for (const auto& action : plm.pendingActions(now_ns)) {
        const uint64_t token = makeProbeToken(++token_seq);
        auto record = plm.beginPost(action, token, now_ns);
        if (record) return PostedProbe{token, std::move(record)};
    }
    return std::nullopt;
}

}  // namespace probe_test
}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TESTS_UB_PROBE_TEST_SUPPORT_H_
