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
// P3: probe poster over the RM adapter contract. It turns one ProbeWorkRequest
// into a one-byte WRITE with an explicit per-WR remote target; the adapter
// imports/caches the target and never binds the probe Jetty.

#ifndef TENT_TRANSPORT_UB_PROBE_POSTER_URMA_H_
#define TENT_TRANSPORT_UB_PROBE_POSTER_URMA_H_

#include <memory>

#include "tent/common/status.h"
#include "tent/transport/ub/probe_resources.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake {
namespace tent {
namespace ub {

class UrmaProbePoster final : public ProbePoster {
   public:
    explicit UrmaProbePoster(std::shared_ptr<UrmaAdapter> adapter);

    Status post(const ProbePostTarget& target, const ProbeWorkRequest& request,
                size_t& posted) override;

   private:
    const std::shared_ptr<UrmaAdapter> adapter_;
};

}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_PROBE_POSTER_URMA_H_
