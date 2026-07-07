// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-present Mooncake contributors
//
// Header-only mock for UbDiag. Provides no-op PerfPoint and PerfKey
// definitions so the codebase compiles without the UbDiag library.
// All perf instrumentation becomes a no-op.
//
// Differences from the real extern/ubdiag/include/ubdiag/auto_perf.h:
//   - PerfKey values are all 0 (mock doesn't need unique slot indices).
//   - No auto-init static initializer (mock has no SHM, no PerfManager).
//   - PerfLevel / PerfPoint are no-ops (no SHM writes).

#pragma once

#include <cstdint>
#include <type_traits>

// Process the perf-key definition file to populate the PerfKey enum.
// UBDIAG_PERF_DEF_FILE must be defined by the includer before this header.
enum class PerfKey : int {
// All keys map to 0 — mock PerfPoint ignores the key entirely.
#define PERF_KEY_DEF(name, file, label) name = 0,
#include UBDIAG_PERF_DEF_FILE
#undef PERF_KEY_DEF
};

namespace UbDiag {

enum class PerfLevel : uint8_t {
    SUB_SYSTEM = 1,
    KEY_MODULE = 2,
    MODULE = 3,
    DEBUG = 4,
};

class PerfPoint {
   public:
    explicit PerfPoint(uint32_t slotIndex, PerfLevel level) {
        (void)slotIndex;
        (void)level;
    }

    // Template constructor: accepts any enum type (zero-overhead in real
    // UbDiag). Mock forwards to the no-op core constructor.
    template <typename E, typename = std::enable_if_t<std::is_enum_v<E>>>
    explicit PerfPoint(E key, PerfLevel level = PerfLevel::SUB_SYSTEM)
        : PerfPoint(static_cast<uint32_t>(key), level) {}

    ~PerfPoint() {}

    void Start() {}
    void End(int /*rc*/ = 0) {}
    void Abandon() {}
};

}  // namespace UbDiag
