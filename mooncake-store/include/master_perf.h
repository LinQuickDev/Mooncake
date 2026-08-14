#pragma once
// Spdiag perf-point integration for the mooncake_master process.
// Include this header (at most once per translation unit) in any .cpp that
// needs to construct SpDiag::PerfPoint with a MASTER_* key.
//
// The program name "mooncake_master" keeps master shards separate from the
// client-side "mooncake_store" shards in the shared-memory region.
#define SPDIAG_PERF_DEF_FILE "mooncake_perf_points.def"
#define SPDIAG_PROGRAM_NAME "mooncake_master"
#include "spdiag/auto_perf.h"
