#pragma once

#include <cstdint>

#include "cvm/cvm_types.h"

namespace mooncake {
namespace cvm {

// Callback interface implemented by MasterService. CvmController calls into
// this delegate to notify the embedding master of slot ownership changes,
// without taking a dependency on the concrete MasterService type.
class CvmServiceDelegate {
   public:
    virtual ~CvmServiceDelegate() = default;

    // Called when this master becomes the stable primary owner of `slot`.
    virtual void OnSlotAcquired(uint16_t slot) = 0;

    // Called when this master releases ownership of `slot`.
    virtual void OnSlotReleased(uint16_t slot) = 0;

    // Called when the CVM membership coordinator decides this master's role
    // should change (e.g. demoted to standby because the submaster quota is
    // full, or promoted back to primary). MasterService reacts by switching
    // its serving/standby state machine accordingly.
    virtual void OnRoleChanged(MasterRole new_role) = 0;

    // Called when the cached slot->primary view changes (slot ownership
    // rebalanced or a primary's lease expired). A standby uses this to re-bind
    // its replay sources even when its own role stays kStandby.
    virtual void OnKvViewChanged() {}
};

}  // namespace cvm
}  // namespace mooncake
