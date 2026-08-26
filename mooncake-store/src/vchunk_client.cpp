#include "vchunk_client.h"

#include <glog/logging.h>

namespace mooncake {
namespace {

size_t SafeKeyId(const TenantId& tenant_id, const std::string& key) {
    return std::hash<std::string>{}(tenant_id.MakeScopedKey(key));
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
      master_(master),
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
    if (!enabled_) {
        return legacy_.Put(tenant_id, key, source, length);
    }
    if (circuit_breaker_threshold_ > 0 &&
        consecutive_put_failures_.load() >= circuit_breaker_threshold_) {
        metrics_->Observe(VChunkOperation::PUT, false, 0);
        return ErrorCode::NO_AVAILABLE_HANDLE;
    }
    if (!source || length == 0 || timeout_.count() <= 0 || !now_ms_) {
        return ErrorCode::INVALID_PARAMS;
    }
    auto created = master_.VChunkPutStart(tenant_id, key, length, false,
                                           now_ms_());
    if (!created) {
        consecutive_put_failures_.fetch_add(1);
        metrics_->Observe(
            VChunkOperation::PUT, false,
            std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() -
                                                                  started)
                .count());
        return created.error();
    }
    metrics_->AddSlices(created->slice_count);
    metrics_->ObserveLayout(*created);
    const auto deadline = Clock::now() + timeout_;
    ErrorCode transfer = ErrorCode::TRANSFER_FAIL;
    for (uint32_t attempt = 0; attempt <= max_retries_; ++attempt) {
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
        for (const auto& slice : created->slices) {
            LOG(ERROR) << "vchunk write failed tenant=" << tenant_id.value()
                       << " key_id=" << SafeKeyId(tenant_id, key)
                       << " vchunk_id=" << created->vchunk_id
                       << " status=" << static_cast<int>(created->status)
                       << " slice_index=" << slice.slice_index
                       << " segment=" << slice.target_segment_name
                       << " error=" << static_cast<int>(transfer);
        }
        const auto revoke = master_.VChunkPutRevoke(
            tenant_id, key, created->vchunk_id);
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
    const auto end = master_.VChunkPutEnd(tenant_id, key, created->vchunk_id,
                                          now_ms_());
    if (end != ErrorCode::OK) {
        master_.VChunkPutRevoke(tenant_id, key, created->vchunk_id);
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
    if (!enabled_) {
        return legacy_.Get(tenant_id, key, destination, length);
    }
    if (!destination || timeout_.count() <= 0) {
        return ErrorCode::INVALID_PARAMS;
    }
    auto read = master_.AcquireVChunkRead(tenant_id, key);
    if (!read) {
        return read.error();
    }
    if (read->record().total_size != length) {
        return ErrorCode::INVALID_PARAMS;
    }
    const auto deadline = Clock::now() + timeout_;
    ErrorCode result = ErrorCode::TRANSFER_FAIL;
    for (uint32_t attempt = 0; attempt <= max_retries_; ++attempt) {
        result = data_plane_.Read(read->record(), destination, length, deadline);
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
        for (const auto& slice : read->record().slices) {
            LOG(ERROR) << "vchunk read failed tenant=" << tenant_id.value()
                       << " key_id=" << SafeKeyId(tenant_id, key)
                       << " vchunk_id=" << read->record().vchunk_id
                       << " status="
                       << static_cast<int>(read->record().status)
                       << " slice_index=" << slice.slice_index
                       << " segment=" << slice.target_segment_name
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
        return ErrorCode::INVALID_PARAMS;
    }
    const auto result = master_.RemoveVChunk(tenant_id, key, now_ms_());
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
