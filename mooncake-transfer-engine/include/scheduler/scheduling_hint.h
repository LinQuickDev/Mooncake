// Copyright 2026 Mooncake Authors
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace mooncake {

enum class TaskIntent : uint8_t {
    UNSPEC = 0,
    CONTROL,
    FOREGROUND_GET,
    P2D_TRANSFER,
    BACKGROUND_PUT,
    PREFETCH,
    MIGRATION,
    CHECKPOINT,
    WEIGHT_LOADING,
};

// deadline_ns is an absolute deadline in this TE process's steady clock.
// Smaller numeric priorities are more urgent; zero is an explicit priority.
struct SchedulingHint {
    std::string request_id;
    uint64_t generation{0};
    std::string tenant_id{"default"};
    std::optional<int32_t> requested_priority;
    TaskIntent intent{TaskIntent::UNSPEC};
    std::optional<uint64_t> deadline_ns;
    bool allow_degrade{false};
};

}  // namespace mooncake
