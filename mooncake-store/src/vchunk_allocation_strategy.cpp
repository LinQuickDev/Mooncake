#include "vchunk_allocation_strategy.h"

#include <algorithm>
#include <limits>
#include <unordered_set>

#include "allocation_strategy.h"
#include "random.h"

namespace mooncake {
namespace {

struct Candidate {
    std::string name;
    const std::vector<std::shared_ptr<BufferAllocatorBase>>* allocators;
    uint64_t remaining_slices;
    uint64_t allocated_slices{0};
};

uint64_t AvailableBytes(const BufferAllocatorBase& allocator) {
    const auto capacity = allocator.capacity();
    const auto used = allocator.size();
    if (capacity == kAllocatorUnknownFreeSpace) {
        return allocator.getLargestFreeRegion();
    }
    return capacity > used ? capacity - used : 0;
}

std::unique_ptr<AllocatedBuffer> AllocateFromCandidate(Candidate& candidate,
                                                       size_t size) {
    for (const auto& allocator : *candidate.allocators) {
        if (allocator && allocator->getLargestFreeRegion() >= size) {
            if (auto buffer = allocator->allocate(size)) {
                ++candidate.allocated_slices;
                return buffer;
            }
        }
    }
    return nullptr;
}

}  // namespace

tl::expected<VChunkAllocationResult, ErrorCode> AllocateVChunk(
    const AllocatorManager& allocator_manager, uint64_t total_size,
    VCSliceSizeLevel slice_size_level,
    const std::set<std::string>& excluded_segments) {
    const uint64_t slice_size = SliceSizeLevelToBytes(slice_size_level);
    if (total_size == 0 || slice_size == 0 ||
        total_size > std::numeric_limits<uint64_t>::max() - (slice_size - 1)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const uint64_t slice_count_u64 =
        (total_size + slice_size - 1) / slice_size;
    if (slice_count_u64 > std::numeric_limits<uint32_t>::max()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::vector<Candidate> candidates;
    for (const auto& name : allocator_manager.getNames()) {
        if (excluded_segments.contains(name)) {
            continue;
        }
        const auto* allocators = allocator_manager.getAllocators(name);
        if (allocators == nullptr || allocators->empty()) {
            continue;
        }
        uint64_t available = 0;
        for (const auto& allocator : *allocators) {
            if (!allocator) {
                continue;
            }
            const auto bytes = AvailableBytes(*allocator);
            if (available > std::numeric_limits<uint64_t>::max() - bytes) {
                available = std::numeric_limits<uint64_t>::max();
                break;
            }
            available += bytes;
        }
        const uint64_t weight = available / slice_size;
        if (weight > 0) {
            candidates.push_back({name, allocators, weight, 0});
        }
    }
    if (candidates.empty()) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    const auto slice_count = static_cast<uint32_t>(slice_count_u64);
    VChunkAllocationResult result;
    result.row_size = std::min<size_t>(slice_count, candidates.size());
    result.allocations.reserve(slice_count);
    const size_t start_offset = randomIndex(candidates.size());

    while (result.allocations.size() < slice_count) {
        std::unordered_set<std::string> used_in_row;
        used_in_row.reserve(result.row_size);
        const size_t row = result.allocations.size() / result.row_size;
        const size_t row_width = std::min<size_t>(
            result.row_size, slice_count - result.allocations.size());
        for (size_t column = 0; column < row_width; ++column) {
            bool allocated = false;
            for (size_t attempt = 0; attempt < candidates.size(); ++attempt) {
                const size_t index =
                    (start_offset + row + column + attempt) % candidates.size();
                auto& candidate = candidates[index];
                if (used_in_row.contains(candidate.name) ||
                    candidate.allocated_slices >= candidate.remaining_slices) {
                    continue;
                }
                auto buffer = AllocateFromCandidate(candidate, slice_size);
                if (!buffer) {
                    candidate.remaining_slices = candidate.allocated_slices;
                    continue;
                }

                const auto slice_index =
                    static_cast<uint32_t>(result.allocations.size());
                const uint64_t consumed = slice_index * slice_size;
                const uint32_t logical_length = static_cast<uint32_t>(
                    std::min<uint64_t>(slice_size, total_size - consumed));
                VCSliceAllocation allocation;
                allocation.slice_index = slice_index;
                allocation.segment_name = candidate.name;
                allocation.target_offset =
                    reinterpret_cast<uintptr_t>(buffer->data());
                allocation.logical_length = logical_length;
                allocation.allocated_length =
                    static_cast<uint32_t>(buffer->size());
                allocation.buffer = std::move(buffer);
                result.allocations.push_back(std::move(allocation));
                used_in_row.insert(candidate.name);
                allocated = true;
                break;
            }
            if (!allocated) {
                return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
            }
        }
    }
    return result;
}

}  // namespace mooncake
