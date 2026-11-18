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

#include "tent/transport/ub/probe_poster_urma.h"

#include <utility>

namespace mooncake {
namespace tent {
namespace ub {

UrmaProbePoster::UrmaProbePoster(std::shared_ptr<UrmaAdapter> adapter)
    : adapter_(std::move(adapter)) {}

Status UrmaProbePoster::post(const ProbePostTarget& target,
                             const ProbeWorkRequest& request, size_t& posted) {
    posted = 0;
    if (!adapter_) {
        return Status::InvalidArgument("probe poster has no URMA adapter");
    }
    if (!target.probe_jetty) {
        return Status::InvalidArgument("probe poster target has no Jetty");
    }
    if (!target.source_segment) {
        return Status::InvalidArgument(
            "probe poster target has no source segment");
    }
    if (!target.area_segment) {
        return Status::InvalidArgument(
            "probe poster target has no area segment");
    }
    if (request.source_length == 0 || request.area_length == 0) {
        return Status::InvalidArgument("probe work request has zero length");
    }
    if (request.source_length != request.area_length) {
        return Status::InvalidArgument(
            "probe source and area lengths must match");
    }

    WorkRequest write;
    write.operation = Operation::WRITE;
    write.local_address = request.source_address;
    write.remote_address = request.area_address;
    write.length = request.source_length;
    write.token = request.token;
    write.local_segment = target.source_segment;
    write.remote_segment = target.area_segment;
    return adapter_->post(target.probe_jetty, target.remote_jetty, write,
                          posted);
}

}  // namespace ub
}  // namespace tent
}  // namespace mooncake
