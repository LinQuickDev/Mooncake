#include "share_object/share_object.h"

#include <new>

#include <glog/logging.h>

namespace embtable {

ShareObject::ShareObject(const std::string& key, size_t size,
                         std::shared_ptr<mooncake::RealClient> realClient)
    : key_(key), size_(size), realClient_(std::move(realClient)) {}

ShareObject::~ShareObject() {
    if (registered_ && local_buffer_ && realClient_) {
        realClient_->unregister_buffer(local_buffer_->ptr());
    }
}

Status ShareObject::Create() {
    if (local_buffer_) {
        return Status::OK();
    }
    if (size_ == 0) {
        return Status::Error(ErrorCode::kInvalidArgument, "size is 0");
    }
    if (!realClient_) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "RealClient is null for key: " + key_);
    }
    try {
        local_allocator_ = mooncake::ClientBufferAllocator::create(
            size_, realClient_->protocol);
        auto allocation = local_allocator_->allocate(size_);
        if (!allocation) {
            local_allocator_.reset();
            return Status::Error(ErrorCode::kBufferFull,
                                 "buffer allocation failed for key: " + key_);
        }
        local_buffer_ = std::make_shared<mooncake::BufferHandle>(
            std::move(allocation.value()));
    } catch (const std::bad_alloc&) {
        local_buffer_.reset();
        local_allocator_.reset();
        return Status::Error(ErrorCode::kInternal,
                             "buffer allocation failed for key: " + key_);
    }
    std::memset(local_buffer_->ptr(), 0, size_);
    if (realClient_->register_buffer(local_buffer_->ptr(), size_) != 0) {
        local_buffer_.reset();
        local_allocator_.reset();
        return Status::Error(ErrorCode::kIOError,
                             "register_buffer failed for key: " + key_);
    }
    registered_ = true;
    return Status::OK();
}

Status ShareObject::Import() {
    if (!local_buffer_) {
        auto s = Create();
        if (!s.IsOk()) return s;
    }
    int64_t ret = realClient_->get_into(key_, local_buffer_->ptr(), size_);
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
    if (offset > size_ || len > size_ - offset) {
        return Status::Error(ErrorCode::kOutOfRange,
                             "read out of range for key: " + key_);
    }
    std::memcpy(data, static_cast<const char*>(local_buffer_->ptr()) + offset,
                len);
    return Status::OK();
}

Status ShareObject::Write(size_t offset, const void* data, size_t len) {
    if (!local_buffer_) {
        auto s = Create();
        if (!s.IsOk()) return s;
    }
    if (offset > size_ || len > size_ - offset) {
        return Status::Error(ErrorCode::kOutOfRange,
                             "write out of range for key: " + key_);
    }
    std::memcpy(static_cast<char*>(local_buffer_->ptr()) + offset, data, len);
    return Status::OK();
}

Status ShareObject::Publish() {
    if (!local_buffer_) {
        return Status::Error(ErrorCode::kInternal,
                             "cannot publish empty local buffer: " + key_);
    }
    int ret = realClient_->put_from(key_, local_buffer_->ptr(), size_);
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
        int unregisterRet =
            realClient_->unregister_buffer(local_buffer_->ptr());
        if (unregisterRet != 0) {
            LOG(WARNING) << "unregister_buffer failed for key: " << key_
                         << " ret=" << unregisterRet;
        }
    }
    local_buffer_.reset();
    local_allocator_.reset();
    registered_ = false;
    return Status::OK();
}

void* ShareObject::Data() {
    return local_buffer_ ? local_buffer_->ptr() : nullptr;
}

const void* ShareObject::Data() const {
    return local_buffer_ ? local_buffer_->ptr() : nullptr;
}

size_t ShareObject::Size() const { return size_; }

}  // namespace embtable
