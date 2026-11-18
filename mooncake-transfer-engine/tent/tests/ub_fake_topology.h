// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// Fake topology for tests that need to drive the UB transport or its workers.
// Kept separate from ub_fake_urma.h so adapter-only tests (for example the
// standalone probe provider) do not pull in Topology's out-of-line symbols.

#ifndef TENT_TESTS_UB_FAKE_TOPOLOGY_H_
#define TENT_TESTS_UB_FAKE_TOPOLOGY_H_

#include <memory>

#include "tent/runtime/segment_registry.h"
#include "tent/runtime/topology.h"
#include "tent/transport/ub/topology_attrs.h"
#include "ub_fake_urma.h"

namespace mooncake {
namespace tent {
namespace ub {
namespace fake {

class NullRegistry final : public SegmentRegistry {
   public:
    Status getSegmentDesc(SegmentDescRef&, const std::string&) override {
        return Status::InvalidEntry("not used");
    }
    Status putSegmentDesc(SegmentDescRef&) override { return Status::OK(); }
    Status deleteSegmentDesc(const std::string&) override {
        return Status::OK();
    }
};

std::shared_ptr<Topology> fakeTopology(bool discovery_active = true) {
    auto topology = std::make_shared<Topology>();
    Topology::NicEntry nic{.name = "ub:fake0:eid0",
                           .pci_bus_id = "0000:00:00.0",
                           .type = Topology::NIC_UB,
                           .numa_node = 0};
    auto device = fakeDevice();
    device.active = discovery_active;
    encodeTopologyDeviceAttributes(device, 0, nic.device_attrs);
    topology->nic_list_.push_back(std::move(nic));
    Topology::MemEntry memory;
    memory.name = kWildcardLocation;
    memory.type = Topology::MEM_HOST;
    memory.numa_node = -1;
    memory.device_list[0].push_back(0);
    topology->mem_list_.push_back(std::move(memory));
    return topology;
}

}  // namespace fake
}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TESTS_UB_FAKE_TOPOLOGY_H_
