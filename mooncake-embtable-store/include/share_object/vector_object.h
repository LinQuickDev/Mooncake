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

    // Set the maximum number of elements. This does not allocate backing
    // ShareObjects; capacity == 0 removes the limit.
    Status Reserve(uint64_t capacity);

    // Append `len` bytes of data (must equal elemSize_ for fixed-size records)
    // and return the assigned global element index.
    Status Append(const void* data, size_t len, uint64_t& index);

    // Read element at `index` into the StringView `out` (points into the local
    // buffer of the backing ShareObject; no copy).
    Status Get(uint64_t index, StringView& out) const;

    // Permanently transition this vector to read-only state. Once sealed,
    // mutating operations fail and GetSealed() may read without taking
    // rwMutex_.
    Status Seal();

    // Lock-free read for a sealed vector. The returned StringView remains
    // valid for the lifetime of this VectorObject.
    Status GetSealed(uint64_t index, StringView& out) const;

    // Export all 8-byte keys into the provided vector (used by IndexObject
    // before Build). Only meaningful when elemSize_ == sizeof(uint64_t).
    Status ExportToVector(std::vector<uint64_t>& out) const;

    uint64_t Size() const { return size_.load(std::memory_order_acquire); }
    uint64_t Capacity() const {
        return capacity_.load(std::memory_order_acquire);
    }
    uint64_t ElemSize() const { return elemSize_; }

    // Publish all backing ShareObjects to Mooncake Store.
    Status PublishAll();

    // Import all backing ShareObjects from Mooncake Store into local buffers.
    // Used by cross-node reconstruction: allocates `dataNum` elements worth of
    // ShareObjects (matching the publisher's layout) and pulls each segment
    // via ShareObject::Import(). After this returns, Get() works locally.
    Status ImportAll(uint64_t dataNum);

   private:
    size_t CalculateShareObjectNum(uint64_t capacity) const;
    std::string GetShareObjectName(size_t idx) const;
    Status ensureShareObjects(uint64_t neededElements);
    Status getImpl(uint64_t index, StringView& out) const;

    std::string key_;
    uint64_t elemSize_;                  // bytes per element (8-512)
    uint64_t shareObjectSize_;           // bytes per backing ShareObject
    std::atomic<uint64_t> capacity_{0};  // configured limit; 0 = unlimited
    std::atomic<uint64_t> size_{0};
    std::atomic<bool> sealed_{false};
    std::vector<std::unique_ptr<ShareObject>> shareObjects_;
    mutable std::shared_mutex rwMutex_;
    std::shared_ptr<mooncake::RealClient> realClient_;
};

}  // namespace embtable
