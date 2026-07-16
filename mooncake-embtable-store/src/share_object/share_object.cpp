#include "share_object/share_object.h"

#include <glog/logging.h>

namespace embtable {

ShareObject::ShareObject(const std::string& key, size_t size,
                         std::shared_ptr<mooncake::RealClient> realClient)
    : key_(key), size_(size), realClient_(std::move(realClient)) {}

ShareObject::~ShareObject() {
    if (registered_ && local_buffer_ && realClient_) {
        realClient_->unregister_buffer(local_buffer_.get());
    }
}

Status ShareObject::Create() {
    if (local_buffer_) {
        return Status::OK();
    }
    if (size_ == 0) {
        return Status::Error(ErrorCode::kInvalidArgument, "size is 0");
    }
    // Over-allocate to a multiple of 64 to keep alignment friendly for
    // downstream consumers (e.g. value records up to 512B).
    local_buffer_ = std::unique_ptr<char[]>(new char[size_]);
    std::memset(local_buffer_.get(), 0, size_);
    if (!realClient_ ||
        realClient_->register_buffer(local_buffer_.get(), size_) != 0) {
        local_buffer_.reset();
        return Status::Error(ErrorCode::kIOError,
                             "register_buffer failed for key: " + key_);
    }
    owns_local_ = true;
    registered_ = true;
    return Status::OK();
}

Status ShareObject::Import() {
    if (!local_buffer_) {
        auto s = Create();
        if (!s.IsOk()) return s;
    }
    int64_t ret = realClient_->get_into(key_, local_buffer_.get(), size_);
    if (ret < 0) {
        return Status::Error(ErrorCode::kNotFound,
                             "get_into failed for key: " + key_);
    }
    return Status::OK();
}

Status ShareObject::Read(size_t offset, size_t len, void* data) const {
    if (!local_buffer_) {
        return Status::Error(ErrorCode::kInternal,
                             "local buffer not initialized: " + key_);
    }
    if (offset + len > size_) {
        return Status::Error(ErrorCode::kOutOfRange,
                             "read out of range for key: " + key_);
    }
    std::memcpy(data, local_buffer_.get() + offset, len);
    return Status::OK();
}

Status ShareObject::Write(size_t offset, const void* data, size_t len) {
    if (!local_buffer_) {
        auto s = Create();
        if (!s.IsOk()) return s;
    }
    if (offset + len > size_) {
        return Status::Error(ErrorCode::kOutOfRange,
                             "write out of range for key: " + key_);
    }
    std::memcpy(local_buffer_.get() + offset, data, len);
    return Status::OK();
}

Status ShareObject::Publish() {
    if (!local_buffer_) {
        return Status::Error(ErrorCode::kInternal,
                             "cannot publish empty local buffer: " + key_);
    }
    int ret = realClient_->put_from(key_, local_buffer_.get(), size_);
    if (ret != 0) {
        return Status::Error(ErrorCode::kIOError,
                             "put_from failed for key: " + key_);
    }
    return Status::OK();
}

Status ShareObject::Del() {
    int ret = realClient_->remove(key_);
    if (ret != 0) {
        // Treat remove failure as non-fatal (object may already be gone).
        LOG(WARNING) << "remove failed for key: " << key_ << " ret=" << ret;
    }
    if (registered_ && local_buffer_) {
        int unregisterRet = realClient_->unregister_buffer(local_buffer_.get());
        if (unregisterRet != 0) {
            LOG(WARNING) << "unregister_buffer failed for key: " << key_
                         << " ret=" << unregisterRet;
        }
    }
    local_buffer_.reset();
    owns_local_ = false;
    registered_ = false;
    return Status::OK();
}

void* ShareObject::Data() { return local_buffer_.get(); }

const void* ShareObject::Data() const { return local_buffer_.get(); }

size_t ShareObject::Size() const { return size_; }

}  // namespace embtable
