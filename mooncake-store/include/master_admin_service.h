#pragma once

#include <csignal>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <semaphore>
#include <string>
#include <string_view>
#include <thread>

#include <ylt/coro_http/coro_http_server.hpp>
#include <ylt/reflection/user_reflect_macro.hpp>
#include <ylt/struct_json/json_writer.h>

#include "ha/ha_types.h"

namespace mooncake {

extern const uint64_t kMetricReportIntervalSeconds;

class WrappedMasterService;

class MasterAdminServer {
   public:
    MasterAdminServer(uint16_t http_port, bool enable_metric_reporting);

    ~MasterAdminServer();

    bool Start();

    void Stop();

    void SetRuntimeState(ha::MasterRuntimeState state);

    void SetObservedLeader(const std::optional<ha::MasterView>& leader_view);

    void SetServiceDelegate(std::shared_ptr<WrappedMasterService> service);

    void SetServiceAvailable(bool available);

   private:
    struct RuntimeSnapshot {
        ha::MasterRuntimeState state = ha::MasterRuntimeState::kStarting;
        std::optional<ha::MasterView> leader_view;
        std::shared_ptr<WrappedMasterService> service;
        bool service_available = false;
    };

    RuntimeSnapshot SnapshotState() const;

    std::string BuildMetricsText() const;

    std::string BuildTenantQuotaMetricsText() const;

    std::string BuildMetricsSummaryText() const;

    std::shared_ptr<WrappedMasterService> GetActiveService() const;

    template <typename Handler>
    void WithActiveService(coro_http::coro_http_response& resp,
                           Handler&& handler) const;

    void HandleMetrics(coro_http::coro_http_request& req,
                       coro_http::coro_http_response& resp);
    void HandleMetricsSummary(coro_http::coro_http_request& req,
                              coro_http::coro_http_response& resp);
    void HandleHealth(coro_http::coro_http_request& req,
                      coro_http::coro_http_response& resp);
    void HandleRole(coro_http::coro_http_request& req,
                    coro_http::coro_http_response& resp);
    void HandleHaStatus(coro_http::coro_http_request& req,
                        coro_http::coro_http_response& resp);
    void HandleLeader(coro_http::coro_http_request& req,
                      coro_http::coro_http_response& resp);
    void HandleQueryKey(coro_http::coro_http_request& req,
                        coro_http::coro_http_response& resp);
    void HandleGetAllKeys(coro_http::coro_http_request& req,
                          coro_http::coro_http_response& resp);
    void HandleGetAllSegments(coro_http::coro_http_request& req,
                              coro_http::coro_http_response& resp);
    void HandleGetSegmentsDetail(coro_http::coro_http_request& req,
                                 coro_http::coro_http_response& resp);
    void HandleQuerySegment(coro_http::coro_http_request& req,
                            coro_http::coro_http_response& resp);
    void HandleCreateDrainJob(coro_http::coro_http_request& req,
                              coro_http::coro_http_response& resp);
    void HandleQueryDrainJob(coro_http::coro_http_request& req,
                             coro_http::coro_http_response& resp);
    void HandleCancelDrainJob(coro_http::coro_http_request& req,
                              coro_http::coro_http_response& resp);
    void HandleSegmentStatus(coro_http::coro_http_request& req,
                             coro_http::coro_http_response& resp);
    void HandleBatchQueryKeys(coro_http::coro_http_request& req,
                              coro_http::coro_http_response& resp);
    void HandleKvEventsStatus(coro_http::coro_http_request& req,
                              coro_http::coro_http_response& resp);
    void HandleGetTenantQuotas(coro_http::coro_http_request& req,
                               coro_http::coro_http_response& resp);
    void HandleUpsertTenantQuota(coro_http::coro_http_request& req,
                                 coro_http::coro_http_response& resp);
    void HandleDeleteTenantQuota(coro_http::coro_http_request& req,
                                 coro_http::coro_http_response& resp);
    void HandleRemoveAll(coro_http::coro_http_request& req,
                         coro_http::coro_http_response& resp);

    void RegisterHandler();

    void InitHttpServer();

    std::string BuildHealthJson() const;

    std::string BuildLeaderJson() const;

    uint16_t http_port_;
    bool enable_metric_reporting_ = false;
    coro_http::coro_http_server http_server_;
    std::thread metric_report_thread_;
    std::atomic<bool> metric_report_running_{false};
    std::binary_semaphore metric_report_stop_sem_{0};
    std::atomic<bool> started_{false};
    mutable std::mutex state_mutex_;
    ha::MasterRuntimeState state_{ha::MasterRuntimeState::kStarting};
    std::optional<ha::MasterView> leader_view_;
    std::shared_ptr<WrappedMasterService> service_;
    bool service_available_ = false;
};

// --- HTTP response types used by InitHttpServer (legacy path in rpc_service.cpp) ---
struct HttpSegmentDetailItem {
    std::string segment_name;
    std::string segment_id;
    std::string client_id;
    std::string base_address;
    uint64_t size_bytes{0};
    std::string size_human;
    std::string te_endpoint;
    std::string protocol;
    std::string status;
    uint64_t allocator_used_bytes{0};
    uint64_t allocator_capacity_bytes{0};
    double allocator_usage_percent{0.0};
};
YLT_REFL(HttpSegmentDetailItem, segment_name, segment_id, client_id,
         base_address, size_bytes, size_human, te_endpoint, protocol, status,
         allocator_used_bytes, allocator_capacity_bytes,
         allocator_usage_percent);

struct HttpSegmentsDetailResponse {
    uint64_t total_segments{0};
    std::vector<HttpSegmentDetailItem> segments;
};
YLT_REFL(HttpSegmentsDetailResponse, total_segments, segments);

struct HttpSegmentStatusResponse {
    bool success{false};
    std::string segment;
    int32_t status{0};
    std::string status_name;
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpSegmentStatusResponse, success, segment, status, status_name,
         error_code, error_message);

// --- Shared helper functions / templates ---
template <typename T>
void WriteJsonResponse(coro_http::coro_http_response& resp,
                       coro_http::status_type status, const T& payload) {
    std::string json;
    struct_json::to_json(payload, json);
    resp.add_header("Content-Type", "application/json; charset=utf-8");
    resp.set_status_and_content(status, std::move(json));
}

template <typename T>
std::string EnumToString(const T& value) {
    std::ostringstream oss;
    oss << value;
    return oss.str();
}

void SetServiceUnavailable(coro_http::coro_http_response& resp,
                           std::string message);

void WriteErrorResponse(coro_http::coro_http_response& resp,
                        coro_http::status_type status, ErrorCode error,
                        std::string message = {});

std::string EscapeJson(std::string_view input);

// --- Drain job HTTP response types ---
struct HttpCreateDrainJobResponse {
    bool success{false};
    std::string job_id;
    std::string status;
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpCreateDrainJobResponse, success, job_id, status, error_code,
         error_message);

struct QueryJobResponse;

struct HttpQueryDrainJobResponse {
    bool success{false};
    std::string job_id;
    int32_t type{0};
    std::string type_name;
    int32_t status{0};
    std::string status_name;
    int64_t created_at_ms_epoch{0};
    int64_t last_updated_at_ms_epoch{0};
    std::vector<std::string> segments;
    uint64_t succeeded_units{0};
    uint64_t failed_units{0};
    uint64_t blocked_units{0};
    uint64_t active_units{0};
    uint64_t migrated_bytes{0};
    std::string message;
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpQueryDrainJobResponse, success, job_id, type, type_name, status,
         status_name, created_at_ms_epoch, last_updated_at_ms_epoch, segments,
         succeeded_units, failed_units, blocked_units, active_units,
         migrated_bytes, message, error_code, error_message);

HttpQueryDrainJobResponse ToHttpQueryDrainJobResponse(
    const QueryJobResponse& job);

struct HttpCancelDrainJobResponse {
    bool success{false};
    std::string job_id;
    std::string status;
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpCancelDrainJobResponse, success, job_id, status, error_code,
         error_message);

coro_http::status_type ErrorCodeToHttpStatus(ErrorCode error);

tl::expected<UUID, ErrorCode> ParseJobId(std::string_view job_id_view);

std::string AppendMetricSections(std::string primary, std::string secondary);

}  // namespace mooncake
