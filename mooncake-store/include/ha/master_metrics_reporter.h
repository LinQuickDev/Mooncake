#pragma once

#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <thread>

#include "types.h"

namespace mooncake {
namespace ha {

// Periodically reports Master storage metrics to the HA backend (etcd/redis).
//
// On Start():
//   1. Grants a dedicated lease (not the leadership lease) on the HA backend.
//   2. Spawns a background thread that collects metrics and PUTs them to a
//      well-known key ({cluster}/masters/{master_id}) at the configured
//      interval.
//
// On Stop() / destruction:
//   1. Signals the reporting thread to exit.
//   2. Revokes the lease so the key is immediately deleted.
//
// If the Master process crashes, the lease expires after lease_ttl_sec
// and the HA backend automatically removes the key — no manual cleanup
// required.
class MasterMetricsReporter {
   public:
    struct Config {
        // Feature toggle.
        bool enabled = false;

        // Interval (seconds) between consecutive metrics reports.
        int report_interval_sec = 5;

        // Dedicated-lease TTL in seconds. Must be > report_interval_sec
        // (ideally ≥ 2×) so the key survives at least one full cycle
        // even if a single PUT is delayed.
        int lease_ttl_sec = 10;

        // Unique identifier of this Master instance (UUID).
        std::string master_id;

        // RPC address of this Master (e.g. "10.0.0.1:50051").
        std::string local_hostname;

        // Cluster namespace used as the etcd key prefix.
        std::string cluster_namespace;

        // Connection string for the HA backend (e.g. "0.0.0.0:2379").
        // Must match the backend already connected via EtcdHelper.
        std::string ha_backend_connstring;
    };

    explicit MasterMetricsReporter(const Config& config);
    ~MasterMetricsReporter();

    // Non-copyable, non-movable.
    MasterMetricsReporter(const MasterMetricsReporter&) = delete;
    MasterMetricsReporter& operator=(const MasterMetricsReporter&) = delete;

    // Start the reporting loop.  Must be called when becoming Primary.
    // Idempotent: calling Start() on an already-running reporter returns OK.
    ErrorCode Start();

    // Stop the reporting loop and revoke the lease.  Should be called
    // when losing leadership.  Blocks until the reporting thread has exited.
    void Stop();

    // Update the role field reported to etcd (e.g. "primary" → "standby").
    void SetRole(const std::string& role);

   private:
    void ReportLoop();
    std::string BuildEtcdKey() const;
    std::string BuildMetricsJson() const;

    Config config_;
    std::string role_;
    mutable std::mutex role_mutex_;

    std::atomic<bool> running_{false};
    std::atomic<bool> shutdown_requested_{false};
    std::thread report_thread_;
    int64_t lease_id_{0};
    std::mutex lease_mutex_;
};

}  // namespace ha
}  // namespace mooncake
