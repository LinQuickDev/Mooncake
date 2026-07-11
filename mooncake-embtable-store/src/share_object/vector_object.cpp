#include "embtable/share_object/vector_object.h"

#include <glog/logging.h>

namespace embtable {

VectorObject::VectorObject(const std::string& key, uint64_t elemSize,
                           std::shared_ptr<mooncake::RealClient> realClient,
                           uint64_t shareObjectSize)
    : key_(key),
      elemSize_(elemSize),
      shareObjectSize_(shareObjectSize),
      realClient_(std::move(realClient)) {
    if (elemSize_ == 0) {
        elemSize_ = 1;  // avoid divide-by-zero; caller should validate
    }
}

size_t VectorObject::CalculateShareObjectNum(uint64_t capacity) const {
    if (capacity == 0) return 0;
    uint64_t elemsPerObj = shareObjectSize_ / elemSize_;
    if (elemsPerObj == 0) elemsPerObj = 1;
    return (capacity + elemsPerObj - 1) / elemsPerObj;
}

std::string VectorObject::GetShareObjectName(size_t idx) const {
    return key_ + "_seg" + std::to_string(idx);
}

Status VectorObject::ensureCapacity(uint64_t neededElements) {
    if (neededElements <= capacity_) return Status::OK();
    size_t neededObjs = CalculateShareObjectNum(neededElements);
    while (shareObjects_.size() < neededObjs) {
        size_t idx = shareObjects_.size();
        auto obj = std::make_unique<ShareObject>(GetShareObjectName(idx),
                                                 shareObjectSize_, realClient_);
        auto s = obj->Create();
        if (!s.IsOk()) {
            return Status::Error(ErrorCode::kInternal,
                                 "Create ShareObject failed: " + s.msg());
        }
        shareObjects_.push_back(std::move(obj));
    }
    uint64_t elemsPerObj = shareObjectSize_ / elemSize_;
    if (elemsPerObj == 0) elemsPerObj = 1;
    capacity_ = shareObjects_.size() * elemsPerObj;
    return Status::OK();
}

Status VectorObject::Reserve(uint64_t capacity) {
    std::unique_lock<std::shared_mutex> lock(rwMutex_);
    return ensureCapacity(capacity);
}

Status VectorObject::Append(const void* data, size_t len, uint64_t& index) {
    if (len != elemSize_) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "Append len != elemSize");
    }
    std::unique_lock<std::shared_mutex> lock(rwMutex_);
    uint64_t next = size_.load(std::memory_order_relaxed);
    auto s = ensureCapacity(next + 1);
    if (!s.IsOk()) return s;
    uint64_t elemsPerObj = shareObjectSize_ / elemSize_;
    if (elemsPerObj == 0) elemsPerObj = 1;
    size_t objIdx = next / elemsPerObj;
    size_t offInObj = (next % elemsPerObj) * elemSize_;
    if (objIdx >= shareObjects_.size()) {
        return Status::Error(ErrorCode::kInternal, "object index out of range");
    }
    s = shareObjects_[objIdx]->Write(offInObj, data, len);
    if (!s.IsOk()) return s;
    size_.store(next + 1, std::memory_order_release);
    index = next;
    return Status::OK();
}

Status VectorObject::Get(uint64_t index, StringView& out) const {
    std::shared_lock<std::shared_mutex> lock(rwMutex_);
    if (index >= size_.load(std::memory_order_acquire)) {
        return Status::Error(ErrorCode::kOutOfRange, "Get index out of range");
    }
    uint64_t elemsPerObj = shareObjectSize_ / elemSize_;
    if (elemsPerObj == 0) elemsPerObj = 1;
    size_t objIdx = index / elemsPerObj;
    size_t offInObj = (index % elemsPerObj) * elemSize_;
    if (objIdx >= shareObjects_.size()) {
        return Status::Error(ErrorCode::kInternal, "backing object missing");
    }
    const void* base = shareObjects_[objIdx]->Data();
    if (base == nullptr) {
        return Status::Error(ErrorCode::kInternal, "local buffer null");
    }
    out = StringView(static_cast<const char*>(base) + offInObj, elemSize_);
    return Status::OK();
}

Status VectorObject::ExportToVector(std::vector<uint64_t>& out) const {
    std::shared_lock<std::shared_mutex> lock(rwMutex_);
    if (elemSize_ != sizeof(uint64_t)) {
        return Status::Error(ErrorCode::kInvalidArgument,
                             "ExportToVector requires elemSize==8");
    }
    uint64_t total = size_.load(std::memory_order_acquire);
    out.clear();
    out.reserve(total);
    uint64_t elemsPerObj = shareObjectSize_ / elemSize_;
    if (elemsPerObj == 0) elemsPerObj = 1;
    for (uint64_t i = 0; i < total; ++i) {
        size_t objIdx = i / elemsPerObj;
        size_t offInObj = (i % elemsPerObj) * elemSize_;
        if (objIdx >= shareObjects_.size()) {
            return Status::Error(ErrorCode::kInternal, "object missing");
        }
        const char* base =
            static_cast<const char*>(shareObjects_[objIdx]->Data());
        if (base == nullptr) {
            return Status::Error(ErrorCode::kInternal, "local buffer null");
        }
        uint64_t key;
        std::memcpy(&key, base + offInObj, sizeof(uint64_t));
        out.push_back(key);
    }
    return Status::OK();
}

Status VectorObject::PublishAll() {
    std::shared_lock<std::shared_mutex> lock(rwMutex_);
    for (auto& obj : shareObjects_) {
        auto s = obj->Publish();
        if (!s.IsOk()) {
            LOG(ERROR) << "PublishAll failed for " << obj->Key() << ": "
                       << s.msg();
            return s;
        }
    }
    return Status::OK();
}

}  // namespace embtable
