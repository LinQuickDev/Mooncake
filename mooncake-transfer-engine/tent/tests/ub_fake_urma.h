// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Shared fake URMA provider used by the UB data-path and probe integration
// tests. It implements the adapter boundary only: segments, Jetties, JFCs and
// completions. No native URMA library is required.

#ifndef TENT_TESTS_UB_FAKE_URMA_H_
#define TENT_TESTS_UB_FAKE_URMA_H_

#include <atomic>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "tent/transport/ub/topology_attrs.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake {
namespace tent {
namespace ub {
namespace fake {

class FakeContext final : public Context {
   public:
    explicit FakeContext(DeviceInfo info) : info_(std::move(info)) {}
    bool valid() const noexcept override { return valid_; }
    const DeviceInfo& deviceInfo() const noexcept override { return info_; }
    int asyncFd() const noexcept override { return -1; }
    void close() { valid_ = false; }

   private:
    DeviceInfo info_;
    bool valid_{true};
};

class FakeJfc final : public Jfc {
   public:
    bool valid() const noexcept override { return valid_; }
    int eventFd() const noexcept override { return -1; }

    void push(Completion completion) {
        std::lock_guard<std::mutex> lock(mutex_);
        completions_.push_back(completion);
    }
    void poll(size_t maximum, std::vector<Completion>& output) {
        std::function<void()> observer;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++poll_count_;
            observer = poll_observer_;
            while (!completions_.empty() && output.size() < maximum) {
                output.push_back(completions_.front());
                completions_.pop_front();
            }
        }
        if (observer) observer();
    }
    void close() { valid_ = false; }

    // Test hooks for the probe polling-order and probe-completion tests.
    void setPollObserver(std::function<void()> observer) {
        std::lock_guard<std::mutex> lock(mutex_);
        poll_observer_ = std::move(observer);
    }
    uint64_t pollCount() {
        std::lock_guard<std::mutex> lock(mutex_);
        return poll_count_;
    }

   private:
    std::mutex mutex_;
    std::deque<Completion> completions_;
    std::function<void()> poll_observer_;
    uint64_t poll_count_{0};
    bool valid_{true};
};

inline SegmentDescriptor makeDescriptor(uint64_t address, uint64_t length) {
    return SegmentDescriptor{
        SegmentDescriptor::kSchemaVersion, 1, 16,
        std::to_string(address) + ":" + std::to_string(length)};
}

inline bool parseDescriptor(const SegmentDescriptor& descriptor,
                            uint64_t& address, uint64_t& length) {
    const auto colon = descriptor.hex.find(':');
    if (descriptor.schema_version != SegmentDescriptor::kSchemaVersion ||
        descriptor.urma_api_version != 1 || descriptor.urma_abi_size != 16 ||
        colon == std::string::npos) {
        return false;
    }
    try {
        address = std::stoull(descriptor.hex.substr(0, colon));
        length = std::stoull(descriptor.hex.substr(colon + 1));
        return length != 0;
    } catch (...) {
        return false;
    }
}

class FakeLocalSegment final : public LocalSegment {
   public:
    FakeLocalSegment(uint64_t address, uint64_t length)
        : address_(address),
          length_(length),
          descriptor_(makeDescriptor(address, length)) {}
    bool valid() const noexcept override { return valid_; }
    uint64_t address() const noexcept override { return address_; }
    uint64_t length() const noexcept override { return length_; }
    const SegmentDescriptor& descriptor() const noexcept override {
        return descriptor_;
    }
    void close() { valid_ = false; }

   private:
    uint64_t address_;
    uint64_t length_;
    SegmentDescriptor descriptor_;
    bool valid_{true};
};

class FakeRemoteSegment final : public RemoteSegment {
   public:
    FakeRemoteSegment(uint64_t address, uint64_t length,
                      SegmentDescriptor descriptor)
        : address_(address),
          length_(length),
          descriptor_(std::move(descriptor)) {}
    bool valid() const noexcept override { return valid_; }
    uint64_t address() const noexcept override { return address_; }
    uint64_t length() const noexcept override { return length_; }
    const SegmentDescriptor& descriptor() const noexcept override {
        return descriptor_;
    }
    void close() { valid_ = false; }

   private:
    uint64_t address_;
    uint64_t length_;
    SegmentDescriptor descriptor_;
    bool valid_{true};
};

class FakeJetty final : public Jetty {
   public:
    FakeJetty(uint32_t id, std::shared_ptr<FakeJfc> jfc)
        : id_(id), jfc_(std::move(jfc)) {}
    bool valid() const noexcept override { return valid_; }
    uint32_t id() const noexcept override { return id_; }
    uint32_t uasid() const noexcept override { return 0; }
    std::shared_ptr<FakeJfc> jfc() const { return jfc_; }
    bool bound() const { return bound_; }
    void bind() { bound_ = true; }
    void unbind() { bound_ = false; }
    void close() {
        valid_ = false;
        bound_ = false;
    }

   private:
    uint32_t id_;
    std::shared_ptr<FakeJfc> jfc_;
    bool valid_{true};
    bool bound_{false};
};

class FakeUrmaAdapter final : public UrmaAdapter {
   public:
    explicit FakeUrmaAdapter(DeviceInfo device)
        : device_(std::move(device)), discovered_devices_{device_} {}

    bool available() const noexcept override { return true; }
    uint32_t nativeApiVersion() const noexcept override { return 1; }
    size_t nativeSegmentDescriptorSize() const noexcept override { return 16; }
    Status initialize() override {
        initialized_ = true;
        return Status::OK();
    }
    Status shutdown() override {
        if (fail_next_shutdown_.exchange(false, std::memory_order_acq_rel)) {
            return Status::InternalError("injected adapter shutdown failure");
        }
        initialized_ = false;
        return Status::OK();
    }
    Status discoverDevices(std::vector<DeviceInfo>& devices) override {
        if (!initialized_) return Status::InvalidArgument("not initialized");
        devices = discovered_devices_;
        return Status::OK();
    }
    void setDiscoveredDevices(std::vector<DeviceInfo> devices) {
        discovered_devices_ = std::move(devices);
    }
    Status openContext(const DeviceInfo& device, ContextPtr& context) override {
        if (!initialized_) return Status::InvalidArgument("not initialized");
        for (const auto& candidate : discovered_devices_) {
            if (candidate.topology_name == device.topology_name) {
                context = std::make_shared<FakeContext>(device);
                return Status::OK();
            }
        }
        return Status::DeviceNotFound("fake device missing");
    }
    Status closeContext(ContextPtr& context) override {
        if (auto fake = std::dynamic_pointer_cast<FakeContext>(context)) {
            fake->close();
        }
        context.reset();
        return Status::OK();
    }
    Status createJfc(const ContextPtr&, const JfcOptions&,
                     JfcPtr& jfc) override {
        jfc = std::make_shared<FakeJfc>();
        live_jfcs_.fetch_add(1, std::memory_order_relaxed);
        return Status::OK();
    }
    Status deleteJfc(JfcPtr& jfc) override {
        if (auto fake = std::dynamic_pointer_cast<FakeJfc>(jfc)) {
            fake->close();
            decrement(live_jfcs_);
        }
        jfc.reset();
        return Status::OK();
    }
    Status registerLocalSegment(const ContextPtr&, uint64_t address,
                                size_t length, const SegmentOptions& options,
                                LocalSegmentPtr& segment) override {
        if (length == 0) return Status::InvalidArgument("empty segment");
        last_registered_access_.store(options.access,
                                      std::memory_order_release);
        segment = std::make_shared<FakeLocalSegment>(address, length);
        live_local_segments_.fetch_add(1, std::memory_order_relaxed);
        return Status::OK();
    }
    Status unregisterLocalSegment(LocalSegmentPtr& segment) override {
        if (auto fake = std::dynamic_pointer_cast<FakeLocalSegment>(segment)) {
            fake->close();
            decrement(live_local_segments_);
        }
        segment.reset();
        return Status::OK();
    }
    Status importRemoteSegment(const ContextPtr&,
                               const SegmentDescriptor& descriptor,
                               const SegmentOptions&,
                               RemoteSegmentPtr& segment) override {
        uint64_t address = 0;
        uint64_t length = 0;
        if (!parseDescriptor(descriptor, address, length)) {
            return Status::InvalidArgument("bad descriptor");
        }
        segment =
            std::make_shared<FakeRemoteSegment>(address, length, descriptor);
        live_remote_segments_.fetch_add(1, std::memory_order_relaxed);
        return Status::OK();
    }
    Status unimportRemoteSegment(RemoteSegmentPtr& segment) override {
        if (auto fake = std::dynamic_pointer_cast<FakeRemoteSegment>(segment)) {
            fake->close();
            decrement(live_remote_segments_);
        }
        segment.reset();
        return Status::OK();
    }
    Status createJetty(const ContextPtr&, const JfcPtr& jfc,
                       const JettyOptions& options, JettyPtr& jetty) override {
        auto fake_jfc = std::dynamic_pointer_cast<FakeJfc>(jfc);
        if (!fake_jfc) return Status::InvalidArgument("bad JFC");
        last_jetty_transport_mode_ = options.transport_mode;
        if (create_jetty_failures_.load(std::memory_order_acquire) != 0 &&
            create_jetty_failures_.fetch_sub(1, std::memory_order_acq_rel) >
                0) {
            return Status::InternalError("injected createJetty failure");
        }
        auto created = std::make_shared<FakeJetty>(next_jetty_id_++, fake_jfc);
        // A pooled send Jetty is never bound by the endpoint logic: the pooling
        // side creates it, leases it and posts through it. Production RM mode
        // carries a per-WR target instead of a binding; an RC-shaped fake has
        // no such thing, so tests may opt into "created means usable" to
        // exercise the pooled success path.
        if (auto_bind_created_jetties_.load(std::memory_order_acquire)) {
            created->bind();
        }
        jetty = std::move(created);
        live_jetties_.fetch_add(1, std::memory_order_relaxed);
        return Status::OK();
    }
    Status deleteJetty(JettyPtr& jetty) override {
        if (delete_jetty_failures_.load(std::memory_order_acquire) != 0 &&
            delete_jetty_failures_.fetch_sub(1, std::memory_order_acq_rel) >
                0) {
            return Status::InternalError("injected deleteJetty failure");
        }
        if (auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty)) {
            fake->close();
            decrement(live_jetties_);
        }
        jetty.reset();
        return Status::OK();
    }
    Status bindJetty(const JettyPtr& jetty, const RemoteJettyInfo&) override {
        auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty);
        if (!fake || !fake->valid())
            return Status::InvalidArgument("bad Jetty");
        fake->bind();
        return Status::OK();
    }
    Status unbindJetty(const JettyPtr& jetty) override {
        if (auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty)) {
            fake->unbind();
        }
        return Status::OK();
    }
    Status resetJetty(const JettyPtr&) override {
        if (fail_resets_.load(std::memory_order_acquire)) {
            return Status::RdmaError("injected Jetty reset failure");
        }
        return Status::OK();
    }
    Status quiesceJetty(const JettyPtr& jetty, uint32_t timeout_ms,
                        std::vector<Completion>& completions) override {
        completions.clear();
        auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty);
        if (!fake || !fake->valid() || timeout_ms == 0) {
            return Status::InvalidArgument("bad Jetty quiesce");
        }
        if (fail_next_quiesce_.exchange(false, std::memory_order_acq_rel)) {
            return Status::RdmaError("injected quiesce failure");
        }
        const bool drop_completion = drop_next_quiesced_completion_.exchange(
            false, std::memory_order_acq_rel);
        std::lock_guard<std::mutex> lock(pending_mutex_);
        if (!quiesce_drops_completions_.load(std::memory_order_acquire)) {
            auto it = pending_.begin();
            while (it != pending_.end()) {
                if (!it->jetty || it->jetty->id() != fake->id()) {
                    ++it;
                    continue;
                }
                if (!drop_completion) {
                    completions.push_back(
                        Completion{CompletionCategory::ENDPOINT_ERROR, 0,
                                   it->request.token, 0, fake->id()});
                }
                it = pending_.erase(it);
            }
        }
        last_quiesced_jetty_id_.store(fake->id(), std::memory_order_release);
        quiesce_calls_.fetch_add(1, std::memory_order_relaxed);
        return Status::OK();
    }
    Status post(const JettyPtr& jetty, const std::vector<WorkRequest>& requests,
                size_t& posted_count) override {
        posted_count = 0;
        auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty);
        if (!fake || !fake->valid() || !fake->bound()) {
            return Status::InvalidArgument("unbound Jetty");
        }
        recordPostJetty(fake->id());
        // A test may force the whole post result (NotAccepted / Unknown), which
        // a real provider only produces on contradictory paths.
        if (auto forced = takeForcedPostOutcome(requests.size())) {
            posted_count = forced->posted_count;
            for (size_t i = 0; i < forced->posted_count && i < requests.size();
                 ++i) {
                if (!requests[i].local_segment || !requests[i].remote_segment ||
                    requests[i].token == 0 || requests[i].length == 0) {
                    return Status::InvalidArgument("bad WR");
                }
                deliverCompletion(fake, requests[i],
                                  CompletionCategory::SUCCESS);
            }
            return forced->status;
        }
        for (const auto& request : requests) {
            if (!request.local_segment || !request.remote_segment ||
                request.token == 0 || request.length == 0) {
                return Status::InvalidArgument("bad WR");
            }
            const auto category = next_completion_.exchange(
                CompletionCategory::SUCCESS, std::memory_order_acq_rel);
            if (hold_next_completion_.exchange(false,
                                               std::memory_order_acq_rel)) {
                std::lock_guard<std::mutex> lock(pending_mutex_);
                pending_.push_back(Pending{fake, request});
                ++posted_count;
                continue;
            }
            deliverCompletion(fake, request, category);
            ++posted_count;
        }
        return Status::OK();
    }
    Status post(const JettyPtr& jetty, const RemoteJettyInfo& target,
                const WorkRequest& request, size_t& posted) override {
        auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty);
        if (!fake || !fake->valid()) {
            posted = 0;
            return Status::InvalidArgument("bad Jetty");
        }
        last_post_ = request;
        last_post_target_ = target;
        last_post_had_target_ = true;
        recordPostJetty(fake->id(), target.id);
        if (auto forced = takeForcedPostOutcome(1)) {
            posted = forced->posted_count == 0 ? 0 : 1;
            if (posted != 0) {
                deliverCompletion(fake, request, CompletionCategory::SUCCESS);
            }
            return forced->status;
        }
        // The probe poster relies on the historical stub contract: accepted,
        // one WR, no completion (probe evidence is injected by its provider).
        // The pooled data path opts into real completion delivery.
        if (!rm_post_delivers_completion_.load(std::memory_order_acquire)) {
            posted = 1;
            return Status::OK();
        }
        if (hold_next_completion_.exchange(false, std::memory_order_acq_rel)) {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            pending_.push_back(Pending{fake, request});
            posted = 1;
            return Status::OK();
        }
        deliverCompletion(fake, request,
                          next_completion_.exchange(CompletionCategory::SUCCESS,
                                                    std::memory_order_acq_rel));
        posted = 1;
        return Status::OK();
    }

    Status poll(const JfcPtr& jfc, size_t maximum,
                std::vector<Completion>& completions) override {
        completions.clear();
        if (fail_next_poll_.exchange(false, std::memory_order_acq_rel)) {
            return Status::InternalError("injected JFC poll failure");
        }
        auto fake = std::dynamic_pointer_cast<FakeJfc>(jfc);
        if (!fake || !fake->valid()) return Status::InvalidArgument("bad JFC");
        fake->poll(maximum, completions);
        delivered_completions_.fetch_add(completions.size(),
                                         std::memory_order_relaxed);
        return Status::OK();
    }

    // Completion-gate hooks (W2 design §11.2). All of them are fake-only: none
    // of them exists on the real adapter.
    //
    // Runs inside post(), after the completion was pushed into the JFC and
    // before post() returns, so a test can hold the post call stack open while
    // the poller settles the early completion. The hook must not throw.
    void setPostGate(std::function<void(uint64_t token)> gate) {
        std::lock_guard<std::mutex> lock(hook_mutex_);
        post_gate_ = std::move(gate);
    }
    void clearPostGate() { setPostGate({}); }
    // Push an arbitrary completion into a JFC (unknown/late token injection).
    void injectCompletion(
        const JfcPtr& jfc, uint64_t token,
        CompletionCategory category = CompletionCategory::SUCCESS,
        uint32_t completed_bytes = 0) {
        auto fake = std::dynamic_pointer_cast<FakeJfc>(jfc);
        if (!fake) return;
        fake->push(Completion{category, 0, token, completed_bytes, 0});
        injected_completions_.fetch_add(1, std::memory_order_relaxed);
    }
    uint64_t injectedCompletionCount() const {
        return injected_completions_.load(std::memory_order_relaxed);
    }
    // Completions handed to the poller by poll() so far.
    uint64_t completionDeliveredCount() const {
        return delivered_completions_.load(std::memory_order_relaxed);
    }

    void failNextCompletion(CompletionCategory category) {
        next_completion_.store(category, std::memory_order_release);
    }
    // Test hook for the pooled data path: make createJetty() return an already
    // usable (bound) Jetty so a pool member can post without the endpoint bind
    // step. Production never does this for RM mode.
    void setAutoBindCreatedJetties(bool auto_bind) {
        auto_bind_created_jetties_.store(auto_bind, std::memory_order_release);
    }
    // W3 review M3: the RM post overload historically only recorded the target.
    // The pooled data path needs it to behave like the RC one (memcpy +
    // completion + hooks), so that behavior is opt-in and the probe poster's
    // contract stays untouched.
    void setRmPostDeliversCompletion(bool deliver) {
        rm_post_delivers_completion_.store(deliver, std::memory_order_release);
    }
    // Forces the result of the next post() (either overload) so a test can
    // construct the two contradictory states the tristate exists for:
    //   NotAccepted: status !ok, posted_count 0
    //   Unknown:     status ok with 0 posted, or !ok with a posted WR
    void setNextPostOutcome(Status status, size_t posted_count) {
        std::lock_guard<std::mutex> lock(hook_mutex_);
        forced_post_outcome_ = PostOutcome{std::move(status), posted_count};
    }
    // Releases every held completion as a SUCCESS (late-CQE injection).
    size_t completePending() {
        std::vector<Pending> pending;
        {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            pending.swap(pending_);
        }
        for (const auto& entry : pending) {
            if (!entry.jetty) continue;
            deliverCompletion(entry.jetty, entry.request,
                              CompletionCategory::SUCCESS);
        }
        return pending.size();
    }
    void failNextCreateJetty() {
        create_jetty_failures_.fetch_add(1, std::memory_order_acq_rel);
    }
    // Fails the next `count` deleteJetty() calls. Two consecutive failures are
    // needed to observe a quarantined member, because shutdown retries once.
    void failJettyDeletes(size_t count) {
        delete_jetty_failures_.fetch_add(count, std::memory_order_acq_rel);
    }
    void failNextDeleteJetty() { failJettyDeletes(1); }
    // The last Jetty passed to quiesceJetty(). Fence-target assertions
    // (D-14 / W3-IN-10) compare it with the Jetty the WR rode.
    uint32_t lastQuiescedJettyId() const {
        return last_quiesced_jetty_id_.load(std::memory_order_acquire);
    }
    // Jetty ids in post() call order: the "assert the call sequence" surface
    // §15.1 asks for.
    std::vector<uint32_t> postJettyIds() const {
        std::lock_guard<std::mutex> lock(sequence_mutex_);
        return post_jetty_ids_;
    }
    // Per-WR RM target ids in post() call order (M6: the slot must follow the
    // posting lane, not the member's JFC rotation).
    std::vector<uint32_t> postTargetIds() const {
        std::lock_guard<std::mutex> lock(sequence_mutex_);
        return post_target_ids_;
    }
    void holdNextCompletion() {
        hold_next_completion_.store(true, std::memory_order_release);
    }
    void pushPendingCompletion(const JettyPtr& jetty, uint64_t token) {
        auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty);
        if (!fake) return;
        std::lock_guard<std::mutex> lock(pending_mutex_);
        WorkRequest request;
        request.token = token;
        pending_.push_back(Pending{fake, request});
    }
    void dropNextQuiescedCompletion() {
        drop_next_quiesced_completion_.store(true, std::memory_order_release);
    }
    void failNextQuiesce() {
        fail_next_quiesce_.store(true, std::memory_order_release);
    }
    void dropQuiesceCompletions() {
        quiesce_drops_completions_.store(true, std::memory_order_release);
    }
    void failResets() { fail_resets_.store(true, std::memory_order_release); }
    void allowResets() { fail_resets_.store(false, std::memory_order_release); }
    void failNextShutdown() {
        fail_next_shutdown_.store(true, std::memory_order_release);
    }
    void failNextPoll() {
        fail_next_poll_.store(true, std::memory_order_release);
    }
    size_t pendingCount() const {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        return pending_.size();
    }
    uint64_t quiesceCalls() const {
        return quiesce_calls_.load(std::memory_order_relaxed);
    }
    uint32_t lastRegisteredAccess() const {
        return last_registered_access_.load(std::memory_order_acquire);
    }
    size_t liveJfcCount() const {
        return live_jfcs_.load(std::memory_order_relaxed);
    }
    size_t liveJettyCount() const {
        return live_jetties_.load(std::memory_order_relaxed);
    }
    size_t liveLocalSegmentCount() const {
        return live_local_segments_.load(std::memory_order_relaxed);
    }
    size_t liveRemoteSegmentCount() const {
        return live_remote_segments_.load(std::memory_order_relaxed);
    }
    WorkRequest lastPost() const { return last_post_; }
    RemoteJettyInfo lastPostTarget() const { return last_post_target_; }
    bool lastPostHadTarget() const { return last_post_had_target_; }
    JettyTransportMode lastJettyTransportMode() const {
        return last_jetty_transport_mode_;
    }

   private:
    static void decrement(std::atomic<size_t>& counter) {
        size_t current = counter.load(std::memory_order_relaxed);
        while (current > 0 &&
               !counter.compare_exchange_weak(current, current - 1,
                                              std::memory_order_relaxed)) {
        }
    }

    struct Pending {
        std::shared_ptr<FakeJetty> jetty;
        WorkRequest request;
    };

    struct PostOutcome {
        Status status = Status::OK();
        size_t posted_count = 0;
    };

    void recordPostJetty(uint32_t id, uint32_t target_id = 0) {
        std::lock_guard<std::mutex> lock(sequence_mutex_);
        post_jetty_ids_.push_back(id);
        post_target_ids_.push_back(target_id);
    }

    // Returns the forced outcome when one was installed, consuming it.
    std::optional<PostOutcome> takeForcedPostOutcome(size_t request_count) {
        (void)request_count;
        std::lock_guard<std::mutex> lock(hook_mutex_);
        if (!forced_post_outcome_.has_value()) return std::nullopt;
        PostOutcome outcome = std::move(*forced_post_outcome_);
        forced_post_outcome_.reset();
        return outcome;
    }

    // The single delivery path: copy the payload for a successful WR, push the
    // completion into the Jetty's JFC and run the post gate hook.
    void deliverCompletion(const std::shared_ptr<FakeJetty>& fake,
                           const WorkRequest& request,
                           CompletionCategory category) {
        if (category == CompletionCategory::SUCCESS) {
            if (request.operation == Operation::WRITE) {
                std::memcpy(
                    reinterpret_cast<void*>(request.remote_address),
                    reinterpret_cast<const void*>(request.local_address),
                    request.length);
            } else {
                std::memcpy(
                    reinterpret_cast<void*>(request.local_address),
                    reinterpret_cast<const void*>(request.remote_address),
                    request.length);
            }
        }
        fake->jfc()->push(Completion{category, 0, request.token,
                                     category == CompletionCategory::SUCCESS
                                         ? static_cast<uint32_t>(request.length)
                                         : 0,
                                     fake->id()});
        std::function<void(uint64_t)> gate;
        {
            std::lock_guard<std::mutex> lock(hook_mutex_);
            gate = post_gate_;
        }
        // The hook runs inside the post call, after the completion is visible
        // to the poller and before post() returns.
        if (gate) gate(request.token);
    }

    DeviceInfo device_;
    std::vector<DeviceInfo> discovered_devices_;
    bool initialized_{false};
    uint32_t next_jetty_id_{1};
    std::atomic<CompletionCategory> next_completion_{
        CompletionCategory::SUCCESS};
    std::atomic<bool> auto_bind_created_jetties_{false};
    std::atomic<bool> rm_post_delivers_completion_{false};
    std::atomic<size_t> create_jetty_failures_{0};
    std::atomic<size_t> delete_jetty_failures_{0};
    std::atomic<uint32_t> last_quiesced_jetty_id_{0};
    std::optional<PostOutcome> forced_post_outcome_;
    mutable std::mutex sequence_mutex_;
    std::vector<uint32_t> post_jetty_ids_;
    std::vector<uint32_t> post_target_ids_;
    std::atomic<bool> hold_next_completion_{false};
    std::atomic<bool> drop_next_quiesced_completion_{false};
    std::atomic<bool> fail_next_quiesce_{false};
    std::atomic<bool> quiesce_drops_completions_{false};
    std::atomic<bool> fail_resets_{false};
    std::atomic<bool> fail_next_shutdown_{false};
    std::atomic<bool> fail_next_poll_{false};
    mutable std::mutex pending_mutex_;
    std::vector<Pending> pending_;
    std::atomic<uint64_t> quiesce_calls_{0};
    std::atomic<uint32_t> last_registered_access_{0};
    // Live native-handle counts, used by the standalone probe provider tests to
    // prove that every resource it creates is released again.
    std::atomic<size_t> live_jfcs_{0};
    std::atomic<size_t> live_jetties_{0};
    std::atomic<size_t> live_local_segments_{0};
    std::atomic<size_t> live_remote_segments_{0};
    mutable std::mutex hook_mutex_;
    std::function<void(uint64_t)> post_gate_;
    std::atomic<uint64_t> injected_completions_{0};
    std::atomic<uint64_t> delivered_completions_{0};
    WorkRequest last_post_;
    RemoteJettyInfo last_post_target_;
    bool last_post_had_target_{false};
    JettyTransportMode last_jetty_transport_mode_{JettyTransportMode::kRc};
};

DeviceInfo fakeDevice() {
    DeviceInfo info;
    info.topology_name = "ub:fake0:eid0";
    info.native_device_name = "fake0";
    info.native_device_path = "/fake/fake0";
    info.eid_index = 0;
    info.eid = "0001:0000:0000:0000:0000:0000:0000:0000";
    info.active = true;
    info.capabilities.max_jfc = 4;
    info.capabilities.max_jetty = 64;
    return info;
}

}  // namespace fake
}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TESTS_UB_FAKE_URMA_H_
