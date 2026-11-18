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
// ProbeScheduler: the single thread that drives ProbeLinkManager (design §8.1).
// It owns no native resource and does not poll completion queues: it computes
// due probe actions, posts them through ProbePoster, settles the post result,
// scans completion timeouts and sleeps until the next deadline or wake-up.
//
// Threading contract:
//   * start() must be called after ProbeLinkManager::initialize() and after the
//     pollers are running.
//   * stop() joins the thread; it must not be called from the scheduler thread.
//     ProbeLinkManager::drainBeforePollStop() reaches it through the hook that
//     start() installs, which keeps the join ordering required by design §11.4
//     (scheduler joined before the pollers stop).
//   * the injected clock must be compatible with std::chrono::steady_clock,
//     because deadline waits are converted to real time points.

#ifndef TENT_TRANSPORT_UB_PROBE_SCHEDULER_H_
#define TENT_TRANSPORT_UB_PROBE_SCHEDULER_H_

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>

#include "tent/transport/ub/probe_link_manager.h"
#include "tent/transport/ub/probe_resources.h"

namespace mooncake {
namespace tent {
namespace ub {

class ProbeScheduler {
   public:
    using Clock = ProbeLinkManager::Clock;

    ProbeScheduler(ProbeLinkManager& plm, ProbePoster& poster,
                   Clock clock = {});
    ~ProbeScheduler();

    ProbeScheduler(const ProbeScheduler&) = delete;
    ProbeScheduler& operator=(const ProbeScheduler&) = delete;

    // Starts the scheduler thread and installs the wake/stop hooks on the PLM.
    // Idempotent while running.
    void start();

    // W1c close-order hook (design §11.4): no new probe may be posted, but the
    // thread keeps scanning timeouts until it is joined.
    void notePostingStopped();

    // Requests the thread to stop and joins it. Idempotent.
    void stop();

    [[nodiscard]] bool running() const;

   private:
    void run();
    void postOne(const ProbeAction& action, uint64_t now_ns);
    [[nodiscard]] uint64_t normalizedNow() const;

    ProbeLinkManager& plm_;
    ProbePoster& poster_;
    Clock clock_;

    mutable std::mutex mutex_;
    std::condition_variable wake_;
    std::thread thread_;
    std::atomic<bool> stopping_{false};
    // Guarded by mutex_. Incremented by the wake hook; the condition-variable
    // predicate compares it so a wake racing the deadline computation is not
    // lost.
    uint64_t wake_seq_{0};
    // Only the scheduler thread advances this sequence.
    uint64_t token_seq_{0};
};

}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_PROBE_SCHEDULER_H_
