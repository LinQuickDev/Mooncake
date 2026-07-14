#pragma once

#include <atomic>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "share_object/share_object.h"
#include "emb_types.h"
#include "real_client.h"

namespace embtable {

// VectorObject is a dynamic vector of fixed-size elements, backed by one or
// more ShareObjects (each of a fixed size, default 64MB). Elements are laid out
// contiguously within a ShareObject at stride `elemSize_`. Append returns the
// global element index; Get reads a single element by index without copying.
//
// Design doc section 8.2: VectorObject owns multiple ShareObjects.
class VectorObject {
   public:
    VectorObject(const std::string& key, uint64_t elemSize,
                 std::shared_ptr<mooncake::RealClient> realClient,
                 uint64_t shareObjectSize = 64ull * 1024 * 1024);

    // Pre-allocate capacity (number of elements) by creating enough
    // ShareObjects up-front.
    Status Reserve(uint64_t capacity);

    // Append `len` bytes of data (must equal elemSize_ for fixed-size records)
    // and return the assigned global element index.
    Status Append(const void* data, size_t len, uint64_t& index);

    // Read element at `index` into the StringView `out` (points into the local
    // buffer of the backing ShareObject; no copy).
    Status Get(uint64_t index, StringView& out) const;

    // Export all 8-byte keys into the provided vector (used by IndexObject
    // before Build). Only meaningful when elemSize_ == sizeof(uint64_t).
    Status ExportToVector(std::vector<uint64_t>& out) const;

    uint64_t Size() const { return size_.load(std::memory_order_acquire); }
    uint64_t Capacity() const { return capacity_; }
    uint64_t ElemSize() const { return elemSize_; }

    // Publish all backing ShareObjects to Mooncake Store.
    Status PublishAll();

   private:
    size_t CalculateShareObjectNum(uint64_t capacity) const;
    std::string GetShareObjectName(size_t idx) const;
    Status ensureCapacity(uint64_t neededElements);

    std::string key_;
    uint64_t elemSize_;          // bytes per element (8-512)
    uint64_t shareObjectSize_;   // bytes per backing ShareObject
    uint64_t capacity_ = 0;      // total element capacity
    std::atomic<uint64_t> size_{0};
    std::vector<std::unique_ptr<ShareObject>> shareObjects_;
    mutable std::shared_mutex rwMutex_;
    std::shared_ptr<mooncake::RealClient> realClient_;
};

}  // namespace embtable
