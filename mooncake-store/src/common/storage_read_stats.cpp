#include "storage_backend.h"

namespace mooncake {
namespace {
thread_local StorageReadStats* current_storage_read_stats = nullptr;
}

StorageReadStats* CurrentStorageReadStats() {
    return current_storage_read_stats;
}

ScopedStorageReadStats::ScopedStorageReadStats(StorageReadStats* stats)
    : previous_(current_storage_read_stats) {
    current_storage_read_stats = stats;
}

ScopedStorageReadStats::~ScopedStorageReadStats() {
    current_storage_read_stats = previous_;
}

}  // namespace mooncake
