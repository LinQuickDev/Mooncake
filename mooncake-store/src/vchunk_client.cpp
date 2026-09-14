#include "vchunk_client.h"

#include <glog/logging.h>

#include "master_service.h"

namespace mooncake {
namespace {

size_t SafeKeyId(const TenantId& tenant_id, const std::string& key) {
    return std::hash<std::string>{}(tenant_id.MakeScopedKey(key));
}

bool CountsForCircuitBreaker(ErrorCode error) {
    return error == ErrorCode::NO_AVAILABLE_HANDLE ||
           error == ErrorCode::TRANSFER_FAIL || error == ErrorCode::RPC_TIMEOUT ||
           error == ErrorCode::ETCD_OPERATION_ERROR ||
           error == ErrorCode::RPC_FAIL;
}

}  // namespace

VChunkClient::VChunkClient(bool enabled, MasterService& master,
                           VChunkDataPlane& data_plane,
                           VChunkLegacyPath& legacy,
                           std::chrono::milliseconds timeout, NowMs now_ms,
                           uint32_t max_retries,
                           uint32_t circuit_breaker_threshold,
                           std::shared_ptr<VChunkMetrics> metrics)
    : enabled_(enabled),
      owned_control_plane_(std::make_unique<LocalVChunkControlPlane>(master)),
      control_plane_(owned_control_plane_.get()),
      data_plane_(data_plane),
      legacy_(legacy),
      timeout_(timeout),
      now_ms_(std::move(now_ms)),
      max_retries_(max_retries),
      circuit_breaker_threshold_(circuit_breaker_threshold),
      metrics_(metrics ? std::move(metrics)
                       : std::make_shared<VChunkMetrics>()) {}

VChunkClient::VChunkClient(bool enabled, VChunkControlPlane& control_plane,
                           VChunkDataPlane& data_plane,
                           VChunkLegacyPath& legacy,
                           std::chrono::milliseconds timeout, NowMs now_ms,
                           uint32_t max_retries,
                           uint32_t circuit_breaker_threshold,
                           std::shared_ptr<VChunkMetrics> metrics)
    : enabled_(enabled),
      control_plane_(&control_plane),
      data_plane_(data_plane),
      legacy_(legacy),
      timeout_(timeout),
      now_ms_(std::move(now_ms)),
      max_retries_(max_retries),
      circuit_breaker_threshold_(circuit_breaker_threshold),
      metrics_(metrics ? std::move(metrics)
                       : std::make_shared<VChunkMetrics>()) {}

ErrorCode VChunkClient::Put(const TenantId& tenant_id, const std::string& key,
                            const void* source, size_t length) {
    const auto started = Clock::now();
    const auto deadline = started + timeout_;
    if (!enabled_) {
        return legacy_.Put(tenant_id, key, source, length);
    }
    if (circuit_breaker_threshold_ > 0 &&
        consecutive_put_failures_.load() >= circuit_breaker_threshold_) {
        metrics_->Observe(VChunkOperation::PUT, false, 0);
        return ErrorCode::NO_AVAILABLE_HANDLE;
    }
    if (!source || length == 0 || timeout_.count() <= 0 || !now_ms_) {
        metrics_->Observe(VChunkOperation::PUT, false, 0);
        return ErrorCode::INVALID_PARAMS;
    }
    auto created = control_plane_->PutStart(tenant_id, key, length, now_ms_());
    if (!created) {
        if (CountsForCircuitBreaker(created.error())) {
            consecutive_put_failures_.fetch_add(1);
        }
        metrics_->Observe(
            VChunkOperation::PUT, false,
            std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() -
                                                                  started)
                .count());
        return created.error();
    }
    if (Clock::now() >= deadline) {
        (void)control_plane_->PutRevoke(tenant_id, key, created->vchunk_id);
        metrics_->AddRollback();
        metrics_->AddTimeout();
        metrics_->Observe(
            VChunkOperation::PUT, false,
            std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() -
                                                                  started)
                .count());
        return ErrorCode::RPC_TIMEOUT;
    }
    metrics_->AddSlices(created->slice_count);
    metrics_->ObserveLayout(*created);
    ErrorCode transfer = ErrorCode::TRANSFER_FAIL;
    for (uint32_t attempt = 0; attempt <= max_retries_; ++attempt) {
        if (Clock::now() >= deadline) {
            transfer = ErrorCode::RPC_TIMEOUT;
            break;
        }
        transfer = data_plane_.Write(*created, source, length, deadline);
        if (transfer == ErrorCode::OK ||
            (transfer != ErrorCode::TRANSFER_FAIL &&
             transfer != ErrorCode::RPC_TIMEOUT)) {
            break;
        }
        if (attempt < max_retries_) {
            metrics_->AddRetry();
        }
    }
    if (transfer != ErrorCode::OK) {
        if (!created->slices.empty()) {
            const auto& slice = created->slices.front();
            LOG(ERROR) << "vchunk write failed tenant=" << tenant_id.value()
                       << " key_id=" << SafeKeyId(tenant_id, key)
                       << " vchunk_id=" << created->vchunk_id
                       << " status=" << static_cast<int>(created->status)
                       << " slice_index=" << slice.slice_index
                       << " segment=" << slice.target_segment_name
                       << " slice_count=" << created->slice_count
                       << " error=" << static_cast<int>(transfer);
        }
        const auto revoke =
            control_plane_->PutRevoke(tenant_id, key, created->vchunk_id);
        metrics_->AddRollback();
        if (transfer == ErrorCode::RPC_TIMEOUT) {
            metrics_->AddTimeout();
        } else {
            metrics_->AddTransferFailure();
        }
        consecutive_put_failures_.fetch_add(1);
        const auto result = revoke == ErrorCode::OK ? transfer : revoke;
        metrics_->Observe(
            VChunkOperation::PUT, false,
            std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() -
                                                                  started)
                .count());
        return result;
    }
    if (Clock::now() >= deadline) {
        (void)control_plane_->PutRevoke(tenant_id, key, created->vchunk_id);
        metrics_->AddRollback();
        metrics_->AddTimeout();
        metrics_->Observe(
            VChunkOperation::PUT, false,
            std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() -
                                                                  started)
                .count());
        return ErrorCode::RPC_TIMEOUT;
    }
    auto end =
        control_plane_->PutEnd(tenant_id, key, created->vchunk_id, now_ms_());
    // PutEnd may have committed durably while its RPC response was lost. Read
    // back before revoking so an ambiguous timeout cannot leave a successful
    // object reported as failed (or turn it into an orphan).
    if (end != ErrorCode::OK) {
        auto committed = control_plane_->Get(tenant_id, key);
        if (committed &&
            committed->record.vchunk_id == created->vchunk_id &&
            committed->record.status == VChunkStatus::ACTIVE) {
            end = ErrorCode::OK;
        }
    }
    if (end != ErrorCode::OK) {
        control_plane_->PutRevoke(tenant_id, key, created->vchunk_id);
    }
    if (end == ErrorCode::OK) {
        consecutive_put_failures_.store(0);
    } else {
        consecutive_put_failures_.fetch_add(1);
    }
    metrics_->Observe(
        VChunkOperation::PUT, end == ErrorCode::OK,
        std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() -
                                                              started)
            .count());
    return end;
}

ErrorCode VChunkClient::Get(const TenantId& tenant_id, const std::string& key,
                            void* destination, size_t length) {
    const auto started = Clock::now();
    const auto deadline = started + timeout_;
    if (!enabled_) {
        return legacy_.Get(tenant_id, key, destination, length);
    }
    if (!destination || timeout_.count() <= 0) {
        metrics_->Observe(VChunkOperation::GET, false, 0);
        return ErrorCode::INVALID_PARAMS;
    }
    auto read = control_plane_->Get(tenant_id, key);
    if (!read) {
        metrics_->Observe(VChunkOperation::GET, false, 0);
        return read.error();
    }
    if (read->record.total_size != length) {
        metrics_->Observe(VChunkOperation::GET, false, 0);
        return ErrorCode::INVALID_PARAMS;
    }
    if (Clock::now() >= deadline) {
        metrics_->AddTimeout();
        metrics_->Observe(
            VChunkOperation::GET, false,
            std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() -
                                                                  started)
                .count());
        return ErrorCode::RPC_TIMEOUT;
    }
    ErrorCode result = ErrorCode::TRANSFER_FAIL;
    for (uint32_t attempt = 0; attempt <= max_retries_; ++attempt) {
        if (Clock::now() >= deadline) {
            result = ErrorCode::RPC_TIMEOUT;
            break;
        }
        result = data_plane_.Read(read->record, destination, length, deadline);
        if (result == ErrorCode::OK ||
            (result != ErrorCode::TRANSFER_FAIL &&
             result != ErrorCode::RPC_TIMEOUT)) {
            break;
        }
        if (attempt < max_retries_) {
            metrics_->AddRetry();
        }
    }
    if (result == ErrorCode::RPC_TIMEOUT) {
        metrics_->AddTimeout();
    } else if (result == ErrorCode::TRANSFER_FAIL) {
        metrics_->AddTransferFailure();
    }
    if (result != ErrorCode::OK) {
        if (!read->record.slices.empty()) {
            const auto& slice = read->record.slices.front();
            LOG(ERROR) << "vchunk read failed tenant=" << tenant_id.value()
                       << " key_id=" << SafeKeyId(tenant_id, key)
                       << " vchunk_id=" << read->record.vchunk_id
                       << " status=" << static_cast<int>(read->record.status)
                       << " slice_index=" << slice.slice_index
                       << " segment=" << slice.target_segment_name
                       << " slice_count=" << read->record.slice_count
                       << " error=" << static_cast<int>(result);
        }
    }
    metrics_->Observe(
        VChunkOperation::GET, result == ErrorCode::OK,
        std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() -
                                                              started)
            .count());
    return result;
}

ErrorCode VChunkClient::Remove(const TenantId& tenant_id,
                               const std::string& key) {
    const auto started = Clock::now();
    if (!enabled_) {
        return legacy_.Remove(tenant_id, key);
    }
    if (!now_ms_) {
        metrics_->Observe(VChunkOperation::REMOVE, false, 0);
        return ErrorCode::INVALID_PARAMS;
    }
    auto result = control_plane_->Remove(tenant_id, key, now_ms_());
    if (result == ErrorCode::RPC_TIMEOUT || result == ErrorCode::RPC_FAIL) {
        metrics_->AddRetry();
        result = control_plane_->Remove(tenant_id, key, now_ms_());
    }
    metrics_->Observe(
        VChunkOperation::REMOVE, result == ErrorCode::OK,
        std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() -
                                                              started)
            .count());
    return result;
}

std::vector<ErrorCode> VChunkClient::BatchPut(
    const TenantId& tenant_id, const std::vector<PutRequest>& requests) {
    std::vector<ErrorCode> results;
    results.reserve(requests.size());
    for (const auto& request : requests) {
        results.push_back(
            Put(tenant_id, request.key, request.source, request.length));
    }
    return results;
}

std::vector<ErrorCode> VChunkClient::BatchGet(
    const TenantId& tenant_id, const std::vector<GetRequest>& requests) {
    std::vector<ErrorCode> results;
    results.reserve(requests.size());
    for (const auto& request : requests) {
        results.push_back(Get(tenant_id, request.key, request.destination,
                              request.length));
    }
    return results;
}

std::vector<ErrorCode> VChunkClient::BatchRemove(
    const TenantId& tenant_id, const std::vector<std::string>& keys) {
    std::vector<ErrorCode> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.push_back(Remove(tenant_id, key));
    }
    return results;
}

VChunkMetricsSnapshot VChunkClient::MetricsSnapshot() const {
    return metrics_->Snapshot();
}

}  // namespace mooncake
