#include "ha/master_metrics_reporter.h"

#include <sstream>
#include <thread>

#include <glog/logging.h>

#include "etcd_helper.h"
#include "master_metric_manager.h"

namespace mooncake {
namespace ha {

MasterMetricsReporter::MasterMetricsReporter(const Config& config)
    : config_(config), role_("unknown") {}

MasterMetricsReporter::~MasterMetricsReporter() { Stop(); }

ErrorCode MasterMetricsReporter::Start() {
    if (!config_.enabled) {
        LOG(INFO) << "MasterMetricsReporter is disabled, skipping start";
        return ErrorCode::OK;
    }

    if (running_.load(std::memory_order_acquire)) {
        LOG(WARNING) << "MasterMetricsReporter is already running";
        return ErrorCode::OK;
    }

    shutdown_requested_.store(false, std::memory_order_release);
    report_thread_ = std::thread([this]() { ReportLoop(); });

    running_.store(true, std::memory_order_release);
    LOG(INFO) << "MasterMetricsReporter started (interval="
              << config_.report_interval_sec
              << "s, lease_ttl=" << config_.lease_ttl_sec
              << "s, cluster=" << config_.cluster_namespace
              << ", master_id=" << config_.master_id << ")";
    return ErrorCode::OK;
}

void MasterMetricsReporter::Stop() {
    if (!running_.load(std::memory_order_acquire)) {
        return;
    }

    shutdown_requested_.store(true, std::memory_order_release);

    if (report_thread_.joinable()) {
        report_thread_.join();
    }

    // Revoke the lease so the key is immediately deleted.
    int64_t lease_to_revoke = 0;
    {
        std::lock_guard<std::mutex> lock(lease_mutex_);
        lease_to_revoke = lease_id_;
        lease_id_ = 0;
    }
    if (lease_to_revoke != 0) {
        auto err = EtcdHelper::RevokeLease(lease_to_revoke);
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "Failed to revoke metrics-report lease "
                         << lease_to_revoke << ": " << toString(err);
        }
    }

    running_.store(false, std::memory_order_release);
    LOG(INFO) << "MasterMetricsReporter stopped";
}

void MasterMetricsReporter::SetRole(const std::string& role) {
    std::lock_guard<std::mutex> lock(role_mutex_);
    role_ = role;
}

void MasterMetricsReporter::ReportLoop() {
    while (!shutdown_requested_.load(std::memory_order_acquire)) {
        // ── 1. Grant a fresh lease (or keep existing) ──
        {
            std::lock_guard<std::mutex> lock(lease_mutex_);
            if (lease_id_ != 0) {
                EtcdHelper::RevokeLease(lease_id_);
                lease_id_ = 0;
            }
        }

        EtcdLeaseId new_lease = 0;
        auto grant_err =
            EtcdHelper::GrantLease(config_.lease_ttl_sec, new_lease);
        if (grant_err != ErrorCode::OK) {
            LOG(WARNING) << "Failed to grant metrics-report lease: "
                         << toString(grant_err) << ", retrying in 1s";
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        {
            std::lock_guard<std::mutex> lock(lease_mutex_);
            lease_id_ = new_lease;
        }

        // ── 2. Collect metrics and PUT ──
        auto key = BuildEtcdKey();
        auto value = BuildMetricsJson();

        auto put_err = EtcdHelper::Put(key.data(), key.size(), value.data(),
                                       value.size());
        if (put_err != ErrorCode::OK) {
            LOG(WARNING) << "Failed to PUT metrics to etcd key=" << key << ": "
                         << toString(put_err);
        }

        // ── 3. Sleep until next cycle (check shutdown every second) ──
        for (int i = 0;
             i < config_.report_interval_sec &&
             !shutdown_requested_.load(std::memory_order_acquire);
             ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    // Final cleanup of the lease (Stop() also does this, but it's safe to
    // double-revoke).
    int64_t lease_to_revoke = 0;
    {
        std::lock_guard<std::mutex> lock(lease_mutex_);
        lease_to_revoke = lease_id_;
        lease_id_ = 0;
    }
    if (lease_to_revoke != 0) {
        EtcdHelper::RevokeLease(lease_to_revoke);
    }
}

std::string MasterMetricsReporter::BuildEtcdKey() const {
    // Pattern: /{cluster_namespace}/masters/{master_id}
    std::ostringstream oss;
    oss << '/' << config_.cluster_namespace << "/masters/"
        << config_.master_id;
    return oss.str();
}

std::string MasterMetricsReporter::BuildMetricsJson() const {
    auto& metrics = MasterMetricManager::instance();

    int64_t mem_total = metrics.get_total_mem_capacity();
    int64_t mem_used = metrics.get_allocated_mem_size();
    int64_t mem_available = mem_total - mem_used;

    int64_t nof_total = metrics.get_total_nof_capacity();
    int64_t nof_used = metrics.get_allocated_nof_size();
    int64_t nof_available = nof_total - nof_used;

    int64_t file_total = metrics.get_total_file_capacity();
    int64_t file_used = metrics.get_allocated_file_size();
    // file_total may be INT64_MAX (unlimited).  Cap available to avoid
    // overflow.
    int64_t file_available =
        (file_total == INT64_MAX) ? INT64_MAX : file_total - file_used;

    int64_t key_count = metrics.get_key_count();
    int64_t active_clients = metrics.get_active_clients();

    // Read role under the lock.
    std::string role;
    {
        std::lock_guard<std::mutex> lock(role_mutex_);
        role = role_;
    }

    // Compact JSON; notable fields are on separate lines for readability
    // but the whole blob is still single-line-friendly for etcd values.
    std::ostringstream json;
    json << "{"
         << "\"id\":\"" << config_.master_id << "\","
         << "\"hostname\":\"" << config_.local_hostname << "\","
         << "\"role\":\"" << role << "\","
         << "\"mem_total_bytes\":" << mem_total << ","
         << "\"mem_used_bytes\":" << mem_used << ","
         << "\"mem_available_bytes\":" << mem_available << ","
         << "\"nof_total_bytes\":" << nof_total << ","
         << "\"nof_used_bytes\":" << nof_used << ","
         << "\"nof_available_bytes\":" << nof_available << ","
         << "\"file_total_bytes\":" << file_total << ","
         << "\"file_used_bytes\":" << file_used << ","
         << "\"file_available_bytes\":" << file_available << ","
         << "\"key_count\":" << key_count << ","
         << "\"active_clients\":" << active_clients << ","
         << "\"updated_at\":" << std::chrono::duration_cast<std::chrono::seconds>(
                                    std::chrono::system_clock::now()
                                        .time_since_epoch())
                                    .count()
         << "}";
    return json.str();
}

}  // namespace ha
}  // namespace mooncake
