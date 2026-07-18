#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "share_object/share_object.h"
#include "emb_types.h"
#include "real_client.h"

namespace embtable {

// IndexObject holds a perfect hash function (PHF) over a static set of keys.
// Built once via Build(); afterwards Lookup() returns the value-vector index
// in O(1). The PHF is serialized into a backing ShareObject for sharing across
// nodes (design doc section 4.3, 8.3).
//
// The concrete PHF type (boomphf::mphf<...>) is an implementation detail that
// requires the heavy BooPHF.h header; it is held behind an opaque pointer
// (phf_ as std::unique_ptr<void, void(*)(void*)>) so the header stays light.
class IndexObject {
   public:
    IndexObject(const std::string& key,
                std::shared_ptr<mooncake::RealClient> realClient,
                size_t serializedCapacity = 16ull * 1024 * 1024);

    ~IndexObject();

    IndexObject(const IndexObject&) = delete;
    IndexObject& operator=(const IndexObject&) = delete;

    // Build the PHF from the given keys. Overwrites any previous index.
    Status Build(const std::vector<uint64_t>& keys);

    // Lookup a key. On success vecIndex is translated from BooPHF's slot to
    // the original value-vector index. As with any MPHF, callers must still
    // compare the stored key because an unknown key can produce a candidate.
    Status Lookup(uint64_t key, uint64_t& vecIndex) const;

    // Serialize the PHF into the backing ShareObject and Publish it.
    Status Export();

    // Pull the PHF from Mooncake Store and reconstruct its slot translation
    // from the already imported key vector.
    Status Import(const std::vector<uint64_t>& keys);

    bool IsBuilt() const { return built_; }

   private:
    std::string key_;
    // Opaque PHF pointer; concrete type is defined in index_object.cpp.
    // Custom deleter disposes of the boomphf::mphf object correctly.
    std::unique_ptr<void, void (*)(void*)> phf_{nullptr, nullptr};
    // BooPHF assigns its own dense slot to every key. The slot is not the
    // key's insertion position, so translate it back to VectorObject index.
    std::vector<uint64_t> slotToVecIndex_;
    ShareObject shareObject_;
    bool built_ = false;
    size_t serializedCapacity_;
};

}  // namespace embtable
