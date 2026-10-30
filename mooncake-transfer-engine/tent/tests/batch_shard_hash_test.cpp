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

// batchShardIndex decides which of the 64 batch lock shards a BatchID maps
// to. The old `ptr % 64` selection collapsed real allocator patterns onto a
// handful of shards: aligned pointers (low bits zero), a 248-byte stride
// where only every 8th residue occurs, and same-congruence pointers across
// slab arenas (thread-local freelists reuse one block per thread, so the 16
// polling threads in a tebench-style workload sat on 1-2 shards). These
// checks pin the distribution guarantees of the Fibonacci-hash replacement
// for those exact patterns.

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <set>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/transfer_engine_impl.h"

namespace mooncake {
namespace tent {

// Friend of TransferEngineImpl (declared in its header) so the distribution
// checks can call the shard selection function directly.
class BatchShardHashPeer {
   public:
    static size_t shardIndex(BatchID batch_id) {
        return TransferEngineImpl::batchShardIndex(batch_id);
    }
    static constexpr size_t kShards = TransferEngineImpl::kBatchLockShards;
};

}  // namespace tent
}  // namespace mooncake

namespace mooncake {
namespace tent {
namespace {

TEST(BatchShardHash, SpreadsAlignedStridePointers) {
    // Slab-adjacent Batch objects: 248-byte stride, 8-byte aligned (the exact
    // layout the old hash reduced to residues 48 40 32 24 16 8 0 56).
    uintptr_t base = 0x1'0000'0000 + 200;
    std::set<size_t> shards;
    for (size_t i = 0; i < 16; ++i) {
        shards.insert(BatchShardHashPeer::shardIndex(
            reinterpret_cast<BatchID>(base + i * 248)));
    }
    EXPECT_GE(shards.size(), 12u)
        << "16 slab-stride batches hit only " << shards.size() << " shards";
}

TEST(BatchShardHash, SpreadsCrossArenaCongruentPointers) {
    // Two slab arenas whose batch blocks share the same congruence class
    // (e.g. ...200, ...448, ...696 in both arenas). ptr >> 3 would leave
    // these colliding; the multiplication hash must not.
    uintptr_t arena_a = 0x5555'0000'0000 + 0x200;
    uintptr_t arena_b = 0xAAAA'0000'0000 + 0x200;
    std::set<size_t> shards;
    for (size_t i = 0; i < 16; ++i) {
        shards.insert(BatchShardHashPeer::shardIndex(
            reinterpret_cast<BatchID>(arena_a + i * 256)));
        shards.insert(BatchShardHashPeer::shardIndex(
            reinterpret_cast<BatchID>(arena_b + i * 256)));
    }
    EXPECT_GE(shards.size(), 20u)
        << "32 cross-arena batches hit only " << shards.size() << " shards";
}

TEST(BatchShardHash, StableAndInRange) {
    // Deterministic across calls and within the shard array; null never
    // indexes out of bounds.
    for (uintptr_t p :
         {0x0ull, 0x8ull, 0xF1F1'F1F1'F1F1'F1F1ull, 0x7FFF'FFFF'FFFF'FFF8ull}) {
        size_t idx =
            BatchShardHashPeer::shardIndex(reinterpret_cast<BatchID>(p));
        EXPECT_LT(idx, BatchShardHashPeer::kShards);
        EXPECT_EQ(idx,
                  BatchShardHashPeer::shardIndex(reinterpret_cast<BatchID>(p)));
    }
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
