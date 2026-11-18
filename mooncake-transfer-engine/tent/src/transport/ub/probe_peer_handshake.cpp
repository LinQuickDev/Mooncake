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

#include "tent/transport/ub/probe_peer_handshake.h"

#include <utility>

#include <glog/logging.h>

namespace mooncake {
namespace tent {
namespace ub {
namespace {

// Placeholder until the bootstrap probe-area token is frozen. The peer uses the
// token advertised in its descriptor; a zero token means "default".
constexpr uint32_t kDefaultProbeAreaToken = 0xACFE;

}  // namespace

HandshakeProbePeerDirectory::HandshakeProbePeerDirectory(
    std::shared_ptr<UrmaAdapter> adapter,
    std::map<Topology::NicID, ContextPtr> contexts,
    ProbePeerDescriptorSource* source)
    : adapter_(std::move(adapter)),
      contexts_(std::move(contexts)),
      source_(source) {}

HandshakeProbePeerDirectory::~HandshakeProbePeerDirectory() {
    (void)shutdown();
}

Status HandshakeProbePeerDirectory::snapshotEdges(
    std::vector<ProbeEdgeSupply>& edges) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shut_down_) {
        return Status::InvalidArgument("probe peer directory is shut down");
    }
    if (!adapter_ || source_ == nullptr) {
        return Status::InvalidArgument(
            "probe peer directory is not configured");
    }

    std::vector<ProbePeerDescriptor> descriptors;
    auto status = source_->snapshot(descriptors);
    if (!status.ok()) return status;

    // Validate the whole snapshot first: a malformed descriptor must not leave
    // a half-imported cache behind.
    std::vector<const ProbePeerDescriptor*> accepted;
    std::vector<EdgeKey> keys;
    for (const auto& descriptor : descriptors) {
        if (descriptor.schema_version != kProbePeerDescriptorSchemaVersion) {
            // A peer that does not support probing is not an error: skip its
            // edges and surface the count for diagnostics.
            ++unsupported_peer_count_;
            continue;
        }
        if (descriptor.local_nic < 0 ||
            descriptor.peer_segment_id == LOCAL_SEGMENT_ID ||
            descriptor.remote_nic < 0 || descriptor.topology_epoch == 0 ||
            descriptor.target_epoch == 0 || descriptor.area_length == 0 ||
            descriptor.area_epoch == 0 ||
            descriptor.area_descriptor.hex.empty()) {
            return Status::InvalidArgument("malformed probe peer descriptor");
        }
        if (contexts_.find(descriptor.local_nic) == contexts_.end() ||
            !contexts_.at(descriptor.local_nic)) {
            return Status::InvalidArgument(
                "probe peer descriptor references an unknown local port");
        }
        const EdgeKey key{descriptor.local_nic, descriptor.peer_segment_id,
                          descriptor.remote_nic, descriptor.topology_epoch,
                          descriptor.target_epoch};
        for (const auto& seen : keys) {
            if (seen < key || key < seen) continue;
            return Status::InvalidArgument("duplicate probe peer descriptor");
        }
        keys.push_back(key);
        accepted.push_back(&descriptor);
    }

    std::map<EdgeKey, CachedEdge> next;
    std::vector<RemoteSegmentPtr> retired;
    for (const ProbePeerDescriptor* descriptor_ptr : accepted) {
        const auto& descriptor = *descriptor_ptr;
        const auto& context = contexts_.at(descriptor.local_nic);
        const EdgeKey key{descriptor.local_nic, descriptor.peer_segment_id,
                          descriptor.remote_nic, descriptor.topology_epoch,
                          descriptor.target_epoch};
        auto existing = cache_.find(key);
        if (existing != cache_.end() &&
            existing->second.descriptor_hex == descriptor.area_descriptor.hex) {
            // Same probe page: reuse the import and refresh the volatile
            // fields. An area_epoch bump on the same page is exactly the
            // in-place refresh the design expects.
            ProbeEdgeSupply edge = existing->second.edge;
            edge.area_length = descriptor.area_length;
            edge.area_epoch = descriptor.area_epoch;
            edge.remote_jetty = descriptor.remote_jetty;
            edge.node_gate_ok = descriptor.node_gate_ok;
            edge.port_gate_ok = descriptor.port_gate_ok;
            edge.profile_gate_ok = descriptor.profile_gate_ok;
            edge.endpoint_generation = descriptor.endpoint_generation;
            next.emplace(key, CachedEdge{std::move(edge),
                                         descriptor.area_descriptor.hex});
            continue;
        }

        SegmentOptions options;
        // We only write the peer's probe area.
        options.access = SEGMENT_ACCESS_WRITE;
        options.token = descriptor.area_token != 0 ? descriptor.area_token
                                                   : kDefaultProbeAreaToken;
        RemoteSegmentPtr segment;
        auto status = adapter_->importRemoteSegment(
            context, descriptor.area_descriptor, options, segment);
        if (!status.ok() || !segment || segment->address() == 0 ||
            segment->length() == 0) {
            ++import_failure_count_;
            LOG_EVERY_N(WARNING, 100)
                << "UB probe peer area import failed: local_nic="
                << descriptor.local_nic
                << " peer=" << descriptor.peer_segment_id
                << " remote_nic=" << descriptor.remote_nic
                << " status=" << status.ToString();
            // Keep the previous edge (if any) instead of dropping coverage on
            // a transient import failure.
            if (segment) retired.push_back(std::move(segment));
            if (existing != cache_.end()) {
                next.emplace(key, existing->second);
            }
            continue;
        }

        ProbeEdgeSupply edge;
        edge.version = PathVersion{
            ProbePathKey{descriptor.local_nic, descriptor.peer_segment_id,
                         descriptor.remote_nic},
            descriptor.topology_epoch, descriptor.target_epoch};
        edge.peer =
            ProbePeerKey{descriptor.peer_segment_id, descriptor.peer_nic_path};
        edge.area_segment = segment;
        edge.area_address = segment->address();
        edge.area_length = descriptor.area_length;
        edge.area_epoch = descriptor.area_epoch;
        edge.remote_jetty = descriptor.remote_jetty;
        edge.node_gate_ok = descriptor.node_gate_ok;
        edge.port_gate_ok = descriptor.port_gate_ok;
        edge.profile_gate_ok = descriptor.profile_gate_ok;
        edge.endpoint_generation = descriptor.endpoint_generation;

        if (existing != cache_.end() && existing->second.edge.area_segment) {
            retired.push_back(existing->second.edge.area_segment);
        }
        ++import_count_;
        next.emplace(
            key, CachedEdge{std::move(edge), descriptor.area_descriptor.hex});
    }

    // Edges that disappeared from the peer snapshot lose their import.
    for (auto& [key, cached] : cache_) {
        if (next.find(key) == next.end() && cached.edge.area_segment) {
            retired.push_back(cached.edge.area_segment);
        }
    }
    // Drop the old cache references before unimporting their segments: the
    // provider/adapter contract releases the native handle only when the last
    // reference is gone.
    cache_ = std::move(next);
    for (auto& segment : retired) {
        if (segment) (void)adapter_->unimportRemoteSegment(segment);
    }

    edges.clear();
    edges.reserve(cache_.size());
    for (const auto& [key, cached] : cache_) {
        edges.push_back(cached.edge);
    }
    return Status::OK();
}

Status HandshakeProbePeerDirectory::shutdown() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shut_down_) return Status::OK();
    Status first_error = Status::OK();
    std::vector<RemoteSegmentPtr> released;
    released.reserve(cache_.size());
    for (auto& [key, cached] : cache_) {
        if (cached.edge.area_segment) {
            released.push_back(std::move(cached.edge.area_segment));
        }
    }
    cache_.clear();
    for (auto& segment : released) {
        auto status = adapter_->unimportRemoteSegment(segment);
        if (first_error.ok() && !status.ok()) first_error = status;
    }
    shut_down_ = true;
    return first_error;
}

size_t HandshakeProbePeerDirectory::unsupportedPeerCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return unsupported_peer_count_;
}

size_t HandshakeProbePeerDirectory::importFailureCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return import_failure_count_;
}

size_t HandshakeProbePeerDirectory::importCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return import_count_;
}

size_t HandshakeProbePeerDirectory::cachedEdgeCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cache_.size();
}

}  // namespace ub
}  // namespace tent
}  // namespace mooncake
