#pragma once

#include <cstddef>
#include <cstring>
#include <string_view>

#include "emb_types.h"

namespace embtable {

// ShareBuffer is a lightweight, non-owning view over a contiguous byte range
// (typically backed by a ShareObject's local buffer). It supports offset-based
// reads/writes without copying (design doc section 8.2).
class ShareBuffer {
   public:
    ShareBuffer() = default;
    ShareBuffer(void* data, size_t size) : data_(data), size_(size) {}

    void* Data() const { return data_; }
    size_t Size() const { return size_; }

    // Read [offset, offset+len) into out. No allocation; caller must size out.
    Status Read(size_t offset, size_t len, StringView& out) const {
        if (offset + len > size_) {
            return Status::Error(ErrorCode::kOutOfRange, "share buffer read oob");
        }
        out = StringView(static_cast<const char*>(data_) + offset, len);
        return Status::OK();
    }

    // Write data at [offset, offset+len).
    Status Write(size_t offset, const void* data, size_t len) const {
        if (offset + len > size_) {
            return Status::Error(ErrorCode::kOutOfRange, "share buffer write oob");
        }
        std::memcpy(static_cast<char*>(data_) + offset, data, len);
        return Status::OK();
    }

   private:
    void* data_ = nullptr;
    size_t size_ = 0;
};

}  // namespace embtable
