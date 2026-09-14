#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "allocator.h"

namespace mooncake::test {

class VChunkTestAllocator
    : public BufferAllocatorBase,
      public std::enable_shared_from_this<VChunkTestAllocator> {
   public:
    VChunkTestAllocator(std::string segment_name, uintptr_t base,
                        size_t capacity)
        : segment_name_(std::move(segment_name)),
          base_(base),
          capacity_(capacity) {}

    std::unique_ptr<AllocatedBuffer> allocate(size_t size) override {
        size_t current = used_.load();
        while (current <= capacity_ && size <= capacity_ - current) {
            if (used_.compare_exchange_weak(current, current + size)) {
                const auto offset = next_offset_.fetch_add(size);
                return std::make_unique<AllocatedBuffer>(
                    shared_from_this(), reinterpret_cast<void*>(base_ + offset),
                    size);
            }
        }
        return nullptr;
    }

    void deallocate(AllocatedBuffer* handle) override {
        used_.fetch_sub(handle->size());
    }

    size_t capacity() const override { return capacity_; }
    size_t size() const override { return used_.load(); }
    std::string getSegmentName() const override { return segment_name_; }
    std::string getTransportEndpoint() const override {
        return segment_name_;
    }
    size_t getLargestFreeRegion() const override {
        const auto used = used_.load();
        return used < capacity_ ? capacity_ - used : 0;
    }

   private:
    std::string segment_name_;
    uintptr_t base_;
    size_t capacity_;
    std::atomic<size_t> used_{0};
    std::atomic<size_t> next_offset_{0};
};

}  // namespace mooncake::test
