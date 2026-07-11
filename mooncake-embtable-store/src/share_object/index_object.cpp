#include "embtable/share_object/index_object.h"

#include <climits>
#include <glog/logging.h>
#include <sstream>

#include "BooPHF.h"

namespace embtable {

namespace {
// BBHash uses Hasher_t in two conflicting ways:
//   1. XorshiftHashFunctors::h1/h2 call singleHasher(key, seed) — 2 args
//   2. std::unordered_map<elem_t, uint64_t, Hasher_t> _final_hash — 1 arg
// A functor with a defaulted second argument satisfies both call sites.
struct U64Hasher {
    uint64_t operator()(const uint64_t& key,
                        uint64_t seed = 0xAAAAAAAA55555555ULL) const {
        boomphf::SingleHashFunctor<uint64_t> h;
        return h(key, seed);
    }
};
using BoophfT = boomphf::mphf<uint64_t, U64Hasher>;

// Cast the opaque void* held by IndexObject::phf_ back to the concrete PHF
// pointer type. IndexObject owns the object; the cast is always safe when
// built_ is true.
BoophfT* asPhf(void* p) { return static_cast<BoophfT*>(p); }
const BoophfT* asPhf(const void* p) {
    return static_cast<const BoophfT*>(p);
}

// Custom deleter bridging the opaque-pointer holder to the concrete type.
void deletePhf(void* p) {
    delete static_cast<BoophfT*>(p);
}
}  // namespace

IndexObject::IndexObject(const std::string& key,
                         std::shared_ptr<mooncake::RealClient> realClient,
                         size_t serializedCapacity)
    : key_(key),
      shareObject_(key + "_idx", serializedCapacity, std::move(realClient)),
      serializedCapacity_(serializedCapacity) {}

IndexObject::~IndexObject() {
    // phf_ custom deleter (deletePhf) handles disposal automatically.
}

Status IndexObject::Build(const std::vector<uint64_t>& keys) {
    // Release any previous PHF (reset invokes deletePhf).
    phf_.reset();
    BoophfT* raw = nullptr;
    if (keys.empty()) {
        // Build an empty PHF; lookups will always miss.
        raw = new BoophfT(0, keys, 1, 2.0, false);
    } else {
        raw = new BoophfT(keys.size(), keys, 1, 2.0, false);
    }
    phf_ = {raw, deletePhf};
    built_ = true;
    return Status::OK();
}

Status IndexObject::Lookup(uint64_t key, uint64_t& vecIndex) const {
    if (!built_ || !phf_) {
        return Status::Error(ErrorCode::kIndexNotBuilt,
                             "index not built for key: " + key_);
    }
    uint64_t idx = asPhf(phf_.get())->lookup(key);
    // BBHash returns ULLONG_MAX when the key cannot be located by the PHF.
    if (idx == ULLONG_MAX) {
        return Status::Error(ErrorCode::kNotFound, "key not in index");
    }
    vecIndex = idx;
    return Status::OK();
}

Status IndexObject::Export() {
    if (!built_ || !phf_) {
        return Status::Error(ErrorCode::kIndexNotBuilt,
                             "cannot Export unbuilt index: " + key_);
    }
    auto s = shareObject_.Create();
    if (!s.IsOk()) return s;
    // Serialize PHF into the local buffer via an in-memory stream.
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    asPhf(phf_.get())->save(ss);
    std::string data = ss.str();
    if (data.size() > shareObject_.Size()) {
        return Status::Error(ErrorCode::kBufferFull,
                             "serialized PHF exceeds ShareObject capacity");
    }
    s = shareObject_.Write(0, data.data(), data.size());
    if (!s.IsOk()) return s;
    return shareObject_.Publish();
}

Status IndexObject::Import() {
    auto s = shareObject_.Create();
    if (!s.IsOk()) return s;
    s = shareObject_.Import();
    if (!s.IsOk()) return s;
    std::string raw(static_cast<const char*>(shareObject_.Data()),
                    shareObject_.Size());
    std::stringstream ss(raw, std::ios::in | std::ios::out | std::ios::binary);
    phf_.reset();
    auto* loaded = new BoophfT();
    loaded->load(ss);
    phf_ = {loaded, deletePhf};
    built_ = true;
    return Status::OK();
}

}  // namespace embtable
