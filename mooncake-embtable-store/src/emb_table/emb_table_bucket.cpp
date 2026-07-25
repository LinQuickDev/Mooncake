#include "emb_table/emb_table_bucket.h"

#include <glog/logging.h>
#include <mutex>

#include "client_service.h"
#include "embtable_perf.h"
#include "replica.h"

namespace embtable {

namespace {

std::string ExtractHostname(const std::string& endpoint) {
    auto pos = endpoint.rfind(':');
    return pos == std::string::npos ? endpoint : endpoint.substr(0, pos);
}

bool IsSameHost(const std::string& lhs, const std::string& rhs) {
    if (lhs.empty() || rhs.empty()) return false;
    if (lhs == rhs) return true;
    return ExtractHostname(lhs) == ExtractHostname(rhs);
}

bool IsTerminalPublishError(const Status& status) {
    return status.code() == static_cast<int>(ErrorCode::kIndexBuilt) ||
           status.code() == static_cast<int>(ErrorCode::kAlreadyExists) ||
           status.code() == static_cast<int>(ErrorCode::kInvalidArgument) ||
           status.code() == static_cast<int>(ErrorCode::kNotSupported);
}

bool IsNetworkRpcError(const Status& status) { return status.IsNetworkError(); }

}  // namespace

Bucket::Bucket(BucketInfo info, std::shared_ptr<ShareMapStore> shareMapStore,
               std::shared_ptr<mooncake::RealClient> realClient,
               std::shared_ptr<ShareMapStoreClient> shareMapStoreClient,
               const std::string& localHostname, uint16_t shareMapStoreRpcPort)
    : info_(std::move(info)),
      bucketKey_(info_.bucketKey),
      shareMapStore_(std::move(shareMapStore)),
      realClient_(std::move(realClient)),
      shareMapStoreClient_(std::move(shareMapStoreClient)),
      localHostname_(localHostname),
      shareMapStoreRpcPort_(shareMapStoreRpcPort) {}

Status Bucket::ResolveLocality(const Status* requestError) {
    const bool rerouteRequested =
        requestError && IsNetworkRpcError(*requestError);
    if (requestError && !rerouteRequested) return *requestError;

    std::unique_lock<std::mutex> localityLock(localityMutex_);
    const auto now = std::chrono::steady_clock::now();
    if (!rerouteRequested && localityResolved_ &&
        now - localityResolvedAt_ < std::chrono::seconds(1)) {
        const auto endpoint = info_.rpcEndpoint;
        const bool local = isLocal_;
        localityLock.unlock();
        if (local || endpoint.empty() || !shareMapStoreClient_) {
            return Status::OK();
        }
        auto endpointStatus = shareMapStoreClient_->CheckEndpoint(endpoint);
        if (endpointStatus.IsOk()) return Status::OK();
        LOG(WARNING) << "Bucket RPC endpoint is unavailable"
                     << ", bucket_key=" << bucketKey_
                     << ", endpoint=" << endpoint
                     << ", error=" << endpointStatus.msg();
        if (!IsNetworkRpcError(endpointStatus)) return endpointStatus;
        // Fall through to the same endpoint selection path used after a
        // failed request.
    }

    // rpcEndpoint is the only authoritative ShareMap owner route. The
    // Mooncake Store replicas holding bucket metadata may be placed on any
    // node and must not be interpreted as ShareMap owners.
    if (info_.rpcEndpoint.empty()) {
        // Local-only deployments have no RPC routing requirement.
        if (!shareMapStoreClient_ || shareMapStoreRpcPort_ == 0) {
            isLocal_ = true;
            localityResolved_ = true;
            localityResolvedAt_ = now;
            return Status::OK();
        }
        return Status::Error(
            ErrorCode::kInvalidArgument,
            "bucket metadata has no ShareMapStore RPC endpoint: " + bucketKey_);
    }

    isLocal_ = IsSameHost(ExtractHostname(info_.rpcEndpoint), localHostname_);
    localityResolved_ = true;
    localityResolvedAt_ = now;
    const auto endpoint = info_.rpcEndpoint;
    const bool local = isLocal_;
    localityLock.unlock();
    if (!local && !endpoint.empty() && shareMapStoreClient_ &&
        !rerouteRequested) {
        auto endpointStatus = shareMapStoreClient_->CheckEndpoint(endpoint);
        if (endpointStatus.IsOk()) return Status::OK();
        LOG(WARNING) << "Bucket RPC endpoint is unavailable"
                     << ", bucket_key=" << bucketKey_
                     << ", endpoint=" << endpoint
                     << ", error=" << endpointStatus.msg();
        if (!IsNetworkRpcError(endpointStatus)) return endpointStatus;
    }

    if (local) return Status::OK();
    if (!realClient_ || !shareMapStoreClient_ || shareMapStoreRpcPort_ == 0) {
        return Status::Error(
            ErrorCode::kInternal,
            "cannot reroute bucket without RealClient and RPC client: " +
                bucketKey_);
    }

    EmbTableMeta meta(realClient_);
    std::string newEndpoint;
    std::vector<std::string> excludedEndpoints{endpoint};
    while (true) {
        auto routeStatus = meta.SelectRandomRpcEndpoint(
            excludedEndpoints, shareMapStoreRpcPort_, newEndpoint);
        if (!routeStatus.IsOk()) {
            LOG(ERROR) << "Bucket reroute failed to select endpoint"
                       << ", bucket_key=" << bucketKey_
                       << ", failed_endpoint=" << endpoint
                       << ", error=" << routeStatus.msg();
            return routeStatus;
        }
        auto endpointStatus = shareMapStoreClient_->CheckEndpoint(newEndpoint);
        if (endpointStatus.IsOk()) break;
        if (!IsNetworkRpcError(endpointStatus)) return endpointStatus;
        LOG(WARNING) << "Selected reroute endpoint is unavailable"
                     << ", bucket_key=" << bucketKey_
                     << ", endpoint=" << newEndpoint
                     << ", error=" << endpointStatus.msg();
        excludedEndpoints.push_back(newEndpoint);
    }

    BucketInfo updated;
    {
        std::lock_guard<std::mutex> lock(localityMutex_);
        updated = info_;
    }
    updated.rpcEndpoint = newEndpoint;
    LOG(WARNING) << "Rerouting bucket"
                 << ", bucket_key=" << bucketKey_
                 << ", failed_endpoint=" << endpoint
                 << ", new_endpoint=" << newEndpoint;
    auto updateStatus = meta.UpdateBucketMeta(updated);
    if (!updateStatus.IsOk()) {
        LOG(ERROR) << "Bucket reroute failed to update metadata"
                   << ", bucket_key=" << bucketKey_
                   << ", new_endpoint=" << newEndpoint
                   << ", error=" << updateStatus.msg();
        return updateStatus;
    }

    {
        std::lock_guard<std::mutex> lock(localityMutex_);
        info_ = std::move(updated);
        isLocal_ =
            IsSameHost(ExtractHostname(info_.rpcEndpoint), localHostname_);
        localityResolved_ = true;
        localityResolvedAt_ = std::chrono::steady_clock::now();
    }
    LOG(INFO) << "Bucket reroute succeeded"
              << ", bucket_key=" << bucketKey_
              << ", rpc_endpoint=" << newEndpoint << ", is_local=" << IsLocal();
    return Status::OK();
}

void Bucket::InvalidateLocality() {
    std::lock_guard<std::mutex> lock(localityMutex_);
    localityResolved_ = false;
}

Status Bucket::Insert(const std::vector<uint64_t>& keys,
                      const std::vector<StringView>& values, bool& wouldFlush) {
    wouldFlush = false;
    if (keys.size() != values.size()) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "keys/values size mismatch");
    }
    for (const auto& v : values) {
        if (v.size() != info_.valueSize) {
            return Status::Error(ErrorCode::kInvalidArgument,
                                 "value size != bucket valueSize");
        }
    }
    std::lock_guard<std::mutex> lock(bufferMutex_);
    // Inserts always go to the local write buffer; they are flushed later
    // when the buffer is full (design doc 4.1.4).
    localKeys_.reserve(localKeys_.size() + keys.size());
    localValues_.reserve(localValues_.size() + values.size());
    localKeys_.insert(localKeys_.end(), keys.begin(), keys.end());
    for (const auto& v : values) {
        localValues_.emplace_back(v.data(), v.size());
    }
    wouldFlush = localKeys_.size() >= flushThreshold_;
    return Status::OK();
}

Status Bucket::Insert(const std::vector<uint64_t>& keys,
                      const std::vector<StringView>& values) {
    bool unused = false;
    return Insert(keys, values, unused);
}

Status Bucket::Flush() {
    // Serialize flushes for this bucket. A concurrent Find must not observe an
    // empty pending buffer while another thread is still publishing the batch
    // that it already removed from the buffer.
    std::lock_guard<std::mutex> flushLock(flushMutex_);
    std::vector<uint64_t> keys;
    std::vector<std::string> values;
    {
        std::lock_guard<std::mutex> lock(bufferMutex_);
        if (localKeys_.empty()) return Status::OK();
        keys.swap(localKeys_);
        values.swap(localValues_);
    }

    // Resolve locality to decide whether to flush locally or remotely.
    auto s = ResolveLocality();
    if (!s.IsOk()) {
        std::lock_guard<std::mutex> lock(bufferMutex_);
        localKeys_.insert(localKeys_.begin(), keys.begin(), keys.end());
        localValues_.insert(localValues_.begin(), values.begin(), values.end());
        return s;
    }

    std::vector<StringView> views;
    views.reserve(values.size());
    for (const auto& v : values) {
        views.emplace_back(v);
    }

    const bool local = IsLocal();
    const auto endpoint = RpcEndpoint();
    const auto valueSize = Info().valueSize;
    if (local) {
        s = shareMapStore_->Publish(bucketKey_, valueSize, keys, views);
    } else if (!shareMapStoreClient_) {
        s = Status::Error(
            ErrorCode::kInternal,
            "remote bucket requires ShareMapStoreClient: " + bucketKey_);
    } else {
        s = shareMapStoreClient_->Publish(endpoint, bucketKey_, valueSize, keys,
                                          views);
    }
    if (!s.IsOk() && !local && !endpoint.empty() && IsNetworkRpcError(s)) {
        auto rerouteStatus = ResolveLocality(&s);
        if (rerouteStatus.IsOk()) {
            const auto reroutedEndpoint = RpcEndpoint();
            if (IsLocal()) {
                s = shareMapStore_->Publish(bucketKey_, valueSize, keys, views);
            } else if (shareMapStoreClient_) {
                s = shareMapStoreClient_->Publish(reroutedEndpoint, bucketKey_,
                                                  valueSize, keys, views);
            }
        }
    }
    if (!s.IsOk()) {
        InvalidateLocality();
        if (IsTerminalPublishError(s)) {
            LOG(WARNING) << "Discarding permanently rejected pending batch for "
                         << bucketKey_ << ", keys=" << keys.size()
                         << ", error=" << s.msg();
        } else {
            std::lock_guard<std::mutex> lock(bufferMutex_);
            localKeys_.insert(localKeys_.begin(), keys.begin(), keys.end());
            localValues_.insert(localValues_.begin(), values.begin(),
                                values.end());
        }
        return s;
    }
    {
        std::lock_guard<std::mutex> lock(localityMutex_);
        info_.currentSize += keys.size();
    }
    return Status::OK();
}

Status Bucket::Find(
    const std::vector<uint64_t>& keys, std::vector<StringView>& buffers,
    std::vector<std::shared_ptr<mooncake::BufferHandle>>& bufferHandles) {
    UbDiag::PerfPoint point(PerfKey::EMB_RD_BUCKET_FIND_TOTAL,
                            UbDiag::PerfLevel::KEY_MODULE);
    point.Start();
    auto s = ResolveLocality();
    if (!s.IsOk()) {
        point.End(s.code());
        return s;
    }

    const bool local = IsLocal();
    const auto endpoint = RpcEndpoint();
    if (local) {
        // Local query: ShareMap returns StringViews into ShareObject memory.
        s = shareMapStore_->QueryData(bucketKey_, keys, buffers);
        point.End(s.IsOk() ? 0 : s.code());
        return s;
    }
    if (!shareMapStoreClient_) {
        auto status = Status::Error(
            ErrorCode::kInternal,
            "remote bucket requires ShareMapStoreClient: " + bucketKey_);
        point.End(status.code());
        return status;
    }
    // Remote query via RPC; the returned buffer is held by bufferHandles.
    s = shareMapStoreClient_->QueryData(endpoint, bucketKey_, Info().valueSize,
                                        keys, buffers, bufferHandles);
    if (!s.IsOk() && IsNetworkRpcError(s)) {
        auto rerouteStatus = ResolveLocality(&s);
        if (rerouteStatus.IsOk()) {
            if (IsLocal()) {
                s = shareMapStore_->QueryData(bucketKey_, keys, buffers);
            } else {
                s = shareMapStoreClient_->QueryData(RpcEndpoint(), bucketKey_,
                                                    Info().valueSize, keys,
                                                    buffers, bufferHandles);
            }
        }
    }
    if (!s.IsOk()) InvalidateLocality();
    point.End(s.IsOk() ? 0 : s.code());
    return s;
}

Status Bucket::BuildIndex() {
    auto s = Flush();
    if (!s.IsOk()) return s;

    s = ResolveLocality();
    if (!s.IsOk()) return s;

    const bool local = IsLocal();
    const auto endpoint = RpcEndpoint();
    if (local) {
        return shareMapStore_->BuildIndex(bucketKey_);
    }
    if (!shareMapStoreClient_) {
        return Status::Error(
            ErrorCode::kInternal,
            "remote bucket requires ShareMapStoreClient: " + bucketKey_);
    }
    s = shareMapStoreClient_->BuildIndex(endpoint, bucketKey_);
    if (!s.IsOk() && IsNetworkRpcError(s)) {
        auto rerouteStatus = ResolveLocality(&s);
        if (rerouteStatus.IsOk()) {
            if (IsLocal()) {
                s = shareMapStore_->BuildIndex(bucketKey_);
            } else {
                s = shareMapStoreClient_->BuildIndex(RpcEndpoint(), bucketKey_);
            }
        }
    }
    if (!s.IsOk()) InvalidateLocality();
    return s;
}

BucketInfo Bucket::Info() const {
    std::lock_guard<std::mutex> lock(localityMutex_);
    return info_;
}

uint64_t Bucket::PendingCount() const {
    std::lock_guard<std::mutex> lock(bufferMutex_);
    return localKeys_.size();
}

bool Bucket::IsFull() const {
    std::lock_guard<std::mutex> lock(bufferMutex_);
    return localKeys_.size() >= flushThreshold_;
}

uint64_t Bucket::FlushThreshold() const {
    std::lock_guard<std::mutex> lock(bufferMutex_);
    return flushThreshold_;
}

void Bucket::SetFlushThreshold(uint64_t threshold) {
    std::lock_guard<std::mutex> lock(bufferMutex_);
    flushThreshold_ = threshold;
}

bool Bucket::IsLocal() const {
    std::lock_guard<std::mutex> lock(localityMutex_);
    return isLocal_;
}

std::string Bucket::RpcEndpoint() const {
    std::lock_guard<std::mutex> lock(localityMutex_);
    return info_.rpcEndpoint;
}

}  // namespace embtable
