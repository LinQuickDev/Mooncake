#pragma once

#include <optional>

#include "scheduler/scheduling_hint.h"

namespace mooncake {

// Options shared by Store operations that may use Transfer Engine. Local
// memcpy and storage paths ignore scheduling because they use separate pools.
struct OperationOptions {
    std::optional<SchedulingHint> scheduling;
};

}  // namespace mooncake
