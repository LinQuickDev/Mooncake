#include "emb_table/emb_table_meta.h"

#include <algorithm>
#include <exception>
#include <glog/logging.h>
#include <limits>
#include <random>
#include <span>
#include <unordered_map>
#include <unordered_set>

#include "ylt/struct_json/json_reader.h"
#include "ylt/struct_json/json_writer.h"

namespace embtable {

namespace {
std::string metaKey(const std::string& tableKey) {
    return tableKey + "_tablemeta";
}

std::string bucketMetaKey(const std::string& bucketKey) {
    return bucketKey + "_bucketmeta";
}

Status ValidateTableMeta(const TableMetaInfo& meta) {
    if (meta.tableKey.empty() || meta.tableName.empty() || meta.dimSize == 0 ||
        meta.bucketNum == 0 || meta.bucketCapacity == 0 ||
        meta.tableCapacity == 0) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "invalid table metadata fields");
    }
    uint64_t expectedCapacity = 0;
    if (!CheckedMultiply(meta.bucketNum, meta.bucketCapacity,
                         expectedCapacity) ||
        meta.tableCapacity < expectedCapacity) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "invalid table metadata capacity");
    }
    return Status::OK();
}

Status ValidateBucketMeta(const BucketInfo& info) {
    if (info.bucketKey.empty() || info.tableKey.empty() ||
        info.valueSize == 0 || info.capacity == 0 ||
        info.currentSize > info.capacity) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "invalid bucket metadata fields");
    }
    return Status::OK();
}

constexpr size_t kBucketMetaReplicaNum = 2;

struct SegmentPlacement {
    std::string host;
    std::vector<std::string> segments;
};

std::string ExtractHost(const std::string& segment) {
    const auto pos = segment.rfind(':');
    return pos == std::string::npos ? segment : segment.substr(0, pos);
}

uint16_t ExtractPort(const std::string& endpoint) {
    const auto pos = endpoint.rfind(':');
    if (pos == std::string::npos || pos + 1 >= endpoint.size()) return 0;
    try {
        const auto port = std::stoul(endpoint.substr(pos + 1));
        return port <= std::numeric_limits<uint16_t>::max()
                   ? static_cast<uint16_t>(port)
                   : 0;
    } catch (const std::exception&) {
        return 0;
    }
}

Status QuerySegmentPlacements(
    const std::shared_ptr<mooncake::RealClient>& realClient,
    const std::string& bucketKey, std::vector<SegmentPlacement>& placements) {
    if (!realClient || !realClient->client_) {
        return Status::Error(ErrorCode::kInternal,
                             "Mooncake client is not initialized");
    }
    auto segments = realClient->client_->get_all_segments();
    if (!segments) {
        LOG(ERROR) << "BucketMeta failed to query registered segments"
                   << ", bucket_key=" << bucketKey
                   << ", error=" << mooncake::toString(segments.error());
        return Status::Error(ErrorCode::kIOError,
                             "failed to query registered segments");
    }

    std::unordered_map<std::string, size_t> hostToIndex;
    for (const auto& segment : segments.value()) {
        if (segment.empty()) continue;
        const auto host = ExtractHost(segment);
        if (host.empty()) continue;
        auto [it, inserted] = hostToIndex.emplace(host, placements.size());
        if (inserted) placements.push_back({host, {}});
        auto& hostSegments = placements[it->second].segments;
        if (std::find(hostSegments.begin(), hostSegments.end(), segment) ==
            hostSegments.end()) {
            hostSegments.push_back(segment);
        }
    }

    if (placements.empty()) {
        LOG(ERROR) << "BucketMeta found no registered memory segments"
                   << ", bucket_key=" << bucketKey;
        return Status::Error(ErrorCode::kIOError,
                             "no registered memory segments available");
    }
    return Status::OK();
}

const SegmentPlacement& SelectPlacement(
    const std::vector<SegmentPlacement>& placements,
    const std::string& excludedHost) {
    std::vector<size_t> candidates;
    candidates.reserve(placements.size());
    for (size_t i = 0; i < placements.size(); ++i) {
        if (placements[i].host != excludedHost) candidates.push_back(i);
    }
    if (candidates.empty()) {
        for (size_t i = 0; i < placements.size(); ++i) {
            candidates.push_back(i);
        }
    }

    thread_local std::mt19937 generator(std::random_device{}());
    std::uniform_int_distribution<size_t> hostDistribution(
        0, candidates.size() - 1);
    return placements[candidates[hostDistribution(generator)]];
}

std::string SelectSegment(const SegmentPlacement& placement) {
    thread_local std::mt19937 generator(std::random_device{}());
    std::uniform_int_distribution<size_t> distribution(
        0, placement.segments.size() - 1);
    return placement.segments[distribution(generator)];
}

void ConfigureBucketMetaReplicas(
    const std::vector<SegmentPlacement>& placements,
    const SegmentPlacement& preferredPlacement,
    mooncake::ReplicateConfig& config) {
    config.replica_num = std::min(kBucketMetaReplicaNum, placements.size());
    config.preferred_segments.reserve(config.replica_num);
    config.preferred_segments.push_back(SelectSegment(preferredPlacement));
    for (const auto& placement : placements) {
        if (config.preferred_segments.size() >= config.replica_num) break;
        if (placement.host == preferredPlacement.host) continue;
        config.preferred_segments.push_back(SelectSegment(placement));
    }
}
}  // namespace

EmbTableMeta::EmbTableMeta(std::shared_ptr<mooncake::RealClient> realClient)
    : realClient_(std::move(realClient)) {}

Status EmbTableMeta::CreateTableMeta(const TableMetaInfo& params) {
    auto validation = ValidateTableMeta(params);
    if (!validation.IsOk()) return validation;
    if (!realClient_) {
        return Status::Error(ErrorCode::kInternal,
                             "RealClient is not initialized");
    }
    const int exists = realClient_->isExist(metaKey(params.tableKey));
    if (exists > 0) {
        return Status::Error(ErrorCode::kAlreadyExists,
                             "table already exists: " + params.tableName);
    }
    if (exists < 0) {
        return Status::Error(ErrorCode::kIOError,
                             "failed to check table metadata existence");
    }
    // Serialize to JSON.
    std::string json;
    struct_json::to_json(params, json);

    mooncake::ReplicateConfig config;
    int ret = realClient_->put(metaKey(params.tableKey),
                               std::span<const char>(json.data(), json.size()),
                               config);
    if (ret != 0) {
        return Status::Error(ErrorCode::kIOError,
                             "put failed for table meta: " + params.tableKey);
    }
    metaInfo_ = params;
    return Status::OK();
}

Status EmbTableMeta::QueryTableMeta(const std::string& tableKey,
                                    TableMetaInfo& meta) {
    if (tableKey.empty() || !realClient_) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "invalid table metadata query");
    }
    auto handle = realClient_->get_buffer(metaKey(tableKey));
    if (!handle) {
        return Status::Error(ErrorCode::kNotFound,
                             "table meta not found: " + tableKey);
    }
    std::string buf(static_cast<const char*>(handle->ptr()), handle->size());
    try {
        TableMetaInfo parsed;
        struct_json::from_json(parsed, buf);
        auto validation = ValidateTableMeta(parsed);
        if (!validation.IsOk()) return validation;
        if (parsed.tableKey != tableKey) {
            return Status::Error(ErrorCode::kInvalidArgument,
                                 "table metadata key mismatch");
        }
        meta = std::move(parsed);
    } catch (const std::exception& e) {
        return Status::Error(
            ErrorCode::kInvalidArgument,
            "invalid table metadata JSON: " + std::string(e.what()));
    }
    metaInfo_ = meta;
    return Status::OK();
}

Status EmbTableMeta::UpdateTableMeta(const TableMetaInfo& meta) {
    if (meta.tableKey.empty()) {
        return Status::Error(ErrorCode::kInvalidArgument, "empty tableKey");
    }
    std::string json;
    struct_json::to_json(meta, json);
    mooncake::ReplicateConfig config;
    int ret = realClient_->put(metaKey(meta.tableKey),
                               std::span<const char>(json.data(), json.size()),
                               config);
    if (ret != 0) {
        return Status::Error(
            ErrorCode::kIOError,
            "put (update) failed for table meta: " + meta.tableKey);
    }
    metaInfo_ = meta;
    return Status::OK();
}

Status EmbTableMeta::DeleteTableMeta(const std::string& tableKey) {
    if (tableKey.empty() || !realClient_) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "invalid table metadata delete");
    }
    if (realClient_->remove(metaKey(tableKey)) != 0) {
        return Status::Error(ErrorCode::kIOError,
                             "remove failed for table meta: " + tableKey);
    }
    return Status::OK();
}

Status EmbTableMeta::CreateBucketMeta(BucketInfo& info) {
    auto validation = ValidateBucketMeta(info);
    if (!validation.IsOk()) return validation;
    if (!realClient_) {
        return Status::Error(ErrorCode::kInternal,
                             "RealClient is not initialized");
    }

    LOG(INFO) << "CreateBucketMeta started"
              << ", bucket_key=" << info.bucketKey
              << ", current_rpc_endpoint=" << info.rpcEndpoint;

    std::vector<SegmentPlacement> placements;
    auto placementStatus =
        QuerySegmentPlacements(realClient_, info.bucketKey, placements);
    if (!placementStatus.IsOk()) return placementStatus;

    // Creation must distribute buckets across the complete unique-host set;
    // the failed-host exclusion rule is only used by dynamic rerouting.
    const auto& selectedPlacement = SelectPlacement(placements, "");
    mooncake::ReplicateConfig config;
    ConfigureBucketMetaReplicas(placements, selectedPlacement, config);
    const auto& selectedSegment = config.preferred_segments.front();

    const auto rpcPort = ExtractPort(info.rpcEndpoint);
    if (rpcPort != 0) {
        info.rpcEndpoint =
            selectedPlacement.host + ":" + std::to_string(rpcPort);
    }
    LOG(INFO) << "CreateBucketMeta selected placement"
              << ", bucket_key=" << info.bucketKey
              << ", selected_host=" << selectedPlacement.host
              << ", selected_segment=" << selectedSegment
              << ", unique_host_count=" << placements.size()
              << ", replica_num=" << config.replica_num
              << ", rpc_endpoint=" << info.rpcEndpoint;

    std::string json;
    struct_json::to_json(info, json);
    int ret = realClient_->put(bucketMetaKey(info.bucketKey),
                               std::span<const char>(json.data(), json.size()),
                               config);
    if (ret != 0) {
        LOG(ERROR) << "CreateBucketMeta failed"
                   << ", bucket_key=" << info.bucketKey << ", ret=" << ret;
        return Status::Error(ErrorCode::kIOError,
                             "put failed for bucket meta: " + info.bucketKey);
    }
    LOG(INFO) << "CreateBucketMeta succeeded"
              << ", bucket_key=" << info.bucketKey
              << ", rpc_endpoint=" << info.rpcEndpoint;
    return Status::OK();
}

Status EmbTableMeta::UpdateBucketMeta(const BucketInfo& info) {
    auto validation = ValidateBucketMeta(info);
    if (!validation.IsOk()) return validation;
    if (!realClient_) {
        return Status::Error(ErrorCode::kInternal,
                             "RealClient is not initialized");
    }

    LOG(INFO) << "UpdateBucketMeta started"
              << ", bucket_key=" << info.bucketKey
              << ", rpc_endpoint=" << info.rpcEndpoint;
    std::vector<SegmentPlacement> placements;
    auto placementStatus =
        QuerySegmentPlacements(realClient_, info.bucketKey, placements);
    if (!placementStatus.IsOk()) return placementStatus;

    const auto currentHost = ExtractHost(info.rpcEndpoint);
    const auto& selectedPlacement = SelectPlacement(placements, "");
    const SegmentPlacement* preferredPlacement = &selectedPlacement;
    for (const auto& placement : placements) {
        if (placement.host == currentHost) {
            preferredPlacement = &placement;
            break;
        }
    }
    mooncake::ReplicateConfig config;
    ConfigureBucketMetaReplicas(placements, *preferredPlacement, config);
    const auto& selectedSegment = config.preferred_segments.front();

    LOG(INFO) << "UpdateBucketMeta selected placement"
              << ", bucket_key=" << info.bucketKey
              << ", preferred_host=" << preferredPlacement->host
              << ", preferred_segment=" << selectedSegment
              << ", unique_host_count=" << placements.size()
              << ", replica_num=" << config.replica_num;
    std::string json;
    struct_json::to_json(info, json);
    int ret = realClient_->upsert(
        bucketMetaKey(info.bucketKey),
        std::span<const char>(json.data(), json.size()), config);
    if (ret != 0) {
        LOG(ERROR) << "UpdateBucketMeta failed"
                   << ", bucket_key=" << info.bucketKey << ", ret=" << ret;
        return Status::Error(
            ErrorCode::kIOError,
            "upsert (update) failed for bucket meta: " + info.bucketKey);
    }
    LOG(INFO) << "UpdateBucketMeta succeeded"
              << ", bucket_key=" << info.bucketKey
              << ", rpc_endpoint=" << info.rpcEndpoint;
    return Status::OK();
}

Status EmbTableMeta::SelectRandomRpcEndpoint(const std::string& currentEndpoint,
                                             uint16_t rpcPort,
                                             std::string& rpcEndpoint) const {
    return SelectRandomRpcEndpoint(std::vector<std::string>{currentEndpoint},
                                   rpcPort, rpcEndpoint);
}

Status EmbTableMeta::SelectRandomRpcEndpoint(
    const std::vector<std::string>& excludedEndpoints, uint16_t rpcPort,
    std::string& rpcEndpoint) const {
    if (!realClient_ || rpcPort == 0) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "invalid RPC endpoint selection arguments");
    }
    std::vector<SegmentPlacement> placements;
    auto placementStatus = QuerySegmentPlacements(
        realClient_, "rpc_endpoint_selection", placements);
    if (!placementStatus.IsOk()) return placementStatus;
    std::unordered_set<std::string> excludedHosts;
    for (const auto& endpoint : excludedEndpoints) {
        const auto host = ExtractHost(endpoint);
        if (!host.empty()) excludedHosts.insert(host);
    }
    std::vector<size_t> candidates;
    for (size_t i = 0; i < placements.size(); ++i) {
        if (!excludedHosts.contains(placements[i].host)) {
            candidates.push_back(i);
        }
    }
    if (candidates.empty()) {
        return Status::Error(ErrorCode::kNotFound,
                             "no alternate registered host for bucket reroute");
    }
    thread_local std::mt19937 generator(std::random_device{}());
    std::uniform_int_distribution<size_t> distribution(0,
                                                       candidates.size() - 1);
    const auto& selected = placements[candidates[distribution(generator)]];
    rpcEndpoint = selected.host + ":" + std::to_string(rpcPort);
    LOG(INFO) << "SelectRandomRpcEndpoint selected host"
              << ", excluded_host_count=" << excludedHosts.size()
              << ", selected_host=" << selected.host
              << ", rpc_endpoint=" << rpcEndpoint
              << ", unique_host_count=" << placements.size();
    return Status::OK();
}

Status EmbTableMeta::QueryBucketMeta(const std::string& bucketKey,
                                     BucketInfo& info) {
    if (bucketKey.empty() || !realClient_) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "invalid bucket metadata query");
    }
    auto handle = realClient_->get_buffer(bucketMetaKey(bucketKey));
    if (!handle) {
        return Status::Error(ErrorCode::kNotFound,
                             "bucket meta not found: " + bucketKey);
    }
    std::string buf(static_cast<const char*>(handle->ptr()), handle->size());
    try {
        BucketInfo parsed;
        struct_json::from_json(parsed, buf);
        auto validation = ValidateBucketMeta(parsed);
        if (!validation.IsOk()) return validation;
        if (parsed.bucketKey != bucketKey) {
            return Status::Error(ErrorCode::kInvalidArgument,
                                 "bucket metadata key mismatch");
        }
        info = std::move(parsed);
    } catch (const std::exception& e) {
        return Status::Error(
            ErrorCode::kInvalidArgument,
            "invalid bucket metadata JSON: " + std::string(e.what()));
    }
    return Status::OK();
}

Status EmbTableMeta::DeleteBucketMeta(const std::string& bucketKey) {
    if (bucketKey.empty() || !realClient_) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "invalid bucket metadata delete");
    }
    if (realClient_->remove(bucketMetaKey(bucketKey)) != 0) {
        return Status::Error(ErrorCode::kIOError,
                             "remove failed for bucket meta: " + bucketKey);
    }
    return Status::OK();
}

}  // namespace embtable
