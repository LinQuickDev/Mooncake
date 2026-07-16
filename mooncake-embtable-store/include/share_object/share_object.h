#pragma once

#include <cstddef>
#include <cstring>
#include <memory>
#include <string>

#include "emb_types.h"
#include "real_client.h"

namespace embtable {

// ShareObject wraps a contiguous storage unit backed by the Mooncake Store.
//
// Per design doc section 8.1 ("local buffer + whole object upload"):
//   - Read/Write operate on an in-process local buffer.
//   - Publish() uploads the whole local buffer to Mooncake Store via
//     RealClient::put_from().
//   - Import() pulls the whole object back into the local buffer via
//     RealClient::get_into().
//   - Cross-node sharing is achieved by other nodes calling Import() after a
//     Publish(); writes require re-publishing the whole object.
class ShareObject {
   public:
    ShareObject(const std::string& key, size_t size,
                std::shared_ptr<mooncake::RealClient> realClient);

    ~ShareObject();

    ShareObject(const ShareObject&) = delete;
    ShareObject& operator=(const ShareObject&) = delete;
    ShareObject(ShareObject&&) = delete;
    ShareObject& operator=(ShareObject&&) = delete;

    // Allocate the local buffer. Must be called before Read/Write on a newly
    // created object. Idempotent.
    Status Create();

    // Pull the object from Mooncake Store into the local buffer. Returns
    // kNotFound if the object does not exist in the store.
    Status Import();

    // Offset-based read/write on the local buffer.
    Status Read(size_t offset, size_t len, void* data) const;
    Status Write(size_t offset, const void* data, size_t len);

    // Upload the whole local buffer to Mooncake Store (overwrites any
    // existing object with the same key).
    Status Publish();

    // Remove the object from Mooncake Store and free the local buffer.
    Status Del();

    void* Data();
    const void* Data() const;
    size_t Size() const;
    const std::string& Key() const { return key_; }

   private:
    std::string key_;
    size_t size_;
    std::shared_ptr<mooncake::RealClient> realClient_;
    // Local backing buffer. Allocated lazily by Create() or Write().
    std::unique_ptr<char[]> local_buffer_;
    bool owns_local_ = false;  // whether we allocated (and own) local_buffer_
    bool registered_ = false;
};

}  // namespace embtable
