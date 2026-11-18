// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Shared fixture for the W3 pooled tests: a real SendJettyPool on top of the
// fake URMA adapter. Deliberately no workers here - the pool is exercised
// directly so a failure points at the pooling side, not at the scheduler.

#ifndef TENT_TESTS_UB_POOL_TEST_SUPPORT_H_
#define TENT_TESTS_UB_POOL_TEST_SUPPORT_H_

#include <memory>
#include <utility>
#include <vector>

#include "tent/transport/ub/admission.h"
#include "tent/transport/ub/jfc.h"
#include "tent/transport/ub/native_budget.h"
#include "tent/transport/ub/send_jetty_pool.h"
#include "tent/transport/ub/urma_adapter.h"
#include "ub_fake_urma.h"

namespace mooncake::tent::ub {
namespace pool_test {

// One fully assembled, single-JFC pool. Built with the fake adapter so no URMA
// provider is required.
class PoolFixture {
   public:
    explicit PoolFixture(size_t jfc_count = 1, size_t desired_serving = 1,
                         uint64_t credit_capacity = 8) {
        adapter_ = std::make_shared<fake::FakeUrmaAdapter>(fake::fakeDevice());
        (void)adapter_->initialize();
        adapter_->setAutoBindCreatedJetties(true);
        (void)adapter_->openContext(fake::fakeDevice(), context_handle_);
        jfcs_.reserve(jfc_count);
        for (size_t i = 0; i < jfc_count; ++i) {
            JfcPtr handle;
            (void)adapter_->createJfc(context_handle_, JfcOptions{}, handle);
            jfcs_.push_back(std::make_shared<UbJfc>(i, adapter_, handle));
        }
        credits_ = std::make_shared<JfcCreditLedger>();
        for (size_t i = 0; i < jfcs_.size(); ++i) {
            (void)credits_->install(i, credit_capacity);
        }
        budget_ = std::make_shared<NativeBudget>();
        (void)budget_->configure(kDeviceKey, /*verified=*/64,
                                 /*configured=*/8,
                                 /*replacement_reserve=*/2);
        SendJettyPoolOptions options;
        options.desired_serving = desired_serving;
        options.max_creating = 2;
        pool_ = std::make_shared<SendJettyPool>(adapter_, context_handle_,
                                                jfcs_, budget_, kDeviceKey,
                                                credits_, options);
    }

    ~PoolFixture() {
        pool_->beginStop();
        (void)pool_->drainIdle();
        (void)pool_->drainQuarantined();
    }

    [[nodiscard]] SendJettyPool& pool() { return *pool_; }
    [[nodiscard]] std::shared_ptr<SendJettyPool> poolPtr() const {
        return pool_;
    }
    [[nodiscard]] JfcCreditLedger& credits() { return *credits_; }
    [[nodiscard]] std::shared_ptr<fake::FakeUrmaAdapter> adapter() const {
        return adapter_;
    }
    [[nodiscard]] const std::shared_ptr<JfcCreditLedger>& creditsPtr() const {
        return credits_;
    }
    [[nodiscard]] static PoolKey key() {
        PoolKey key;
        key.context_incarnation = 1;
        key.trans_mode = kUrmaTransportModeRc;
        key.priority = 15;
        return key;
    }
    [[nodiscard]] static AttemptKey attempt(uint64_t slice_id = 1) {
        return AttemptKey{1, slice_id, 1};
    }

    static constexpr const char* kDeviceKey = "fake0:/fake/fake0:eid0";

   private:
    std::shared_ptr<fake::FakeUrmaAdapter> adapter_;
    ContextPtr context_handle_;
    std::vector<std::shared_ptr<UbJfc>> jfcs_;
    std::shared_ptr<JfcCreditLedger> credits_;
    std::shared_ptr<NativeBudget> budget_;
    std::shared_ptr<SendJettyPool> pool_;
};

}  // namespace pool_test
}  // namespace mooncake::tent::ub

#endif  // TENT_TESTS_UB_POOL_TEST_SUPPORT_H_
