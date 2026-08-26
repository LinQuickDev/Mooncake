#include "vchunk_transfer_engine.h"

#include <thread>
#include <unordered_map>
#include <vector>

namespace mooncake {
namespace {

class BatchGuard {
   public:
    BatchGuard(TransferEngine& engine, size_t size)
        : engine_(engine), id_(engine.allocateBatchID(size)) {}
    ~BatchGuard() {
        if (id_ != INVALID_BATCH_ID) {
            engine_.freeBatchID(id_);
        }
    }
    BatchID id() const { return id_; }

   private:
    TransferEngine& engine_;
    BatchID id_;
};

}  // namespace

tl::expected<std::vector<TransferRequest>, ErrorCode>
BuildVChunkTransferRequests(const VChunkMetadataRecord& record, void* buffer,
                            size_t length, TransferRequest::OpCode opcode,
                            const VChunkSegmentResolver& resolve_segment) {
    VChunkConfig validation_config;
    validation_config.enabled = true;
    if (!buffer || !resolve_segment || record.total_size != length ||
        record.slices.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto validation = ValidateVChunkMetadata(record, validation_config);
    if (validation != ErrorCode::OK) {
        return tl::make_unexpected(validation);
    }
    std::unordered_map<std::string, SegmentHandle> handles;
    std::vector<TransferRequest> requests;
    requests.reserve(record.slices.size());
    size_t logical_offset = 0;
    for (const auto& slice : record.slices) {
        auto it = handles.find(slice.target_segment_name);
        if (it == handles.end()) {
            auto handle = resolve_segment(slice.target_segment_name);
            if (!handle) {
                return tl::make_unexpected(handle.error());
            }
            it = handles.emplace(slice.target_segment_name, *handle).first;
        }
        if (slice.logical_length > length - logical_offset) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        requests.push_back(TransferRequest{
            opcode, static_cast<char*>(buffer) + logical_offset, it->second,
            slice.target_offset, slice.logical_length});
        logical_offset += slice.logical_length;
    }
    if (logical_offset != length) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return requests;
}

ErrorCode TransferEngineVChunkDataPlane::Write(
    const VChunkMetadataRecord& record, const void* source, size_t length,
    std::chrono::steady_clock::time_point deadline) {
    return Transfer(record, const_cast<void*>(source), length,
                    TransferRequest::WRITE, deadline);
}

ErrorCode TransferEngineVChunkDataPlane::Read(
    const VChunkMetadataRecord& record, void* destination, size_t length,
    std::chrono::steady_clock::time_point deadline) {
    return Transfer(record, destination, length, TransferRequest::READ,
                    deadline);
}

ErrorCode TransferEngineVChunkDataPlane::Transfer(
    const VChunkMetadataRecord& record, void* buffer, size_t length,
    TransferRequest::OpCode opcode,
    std::chrono::steady_clock::time_point deadline) {
    auto requests = BuildVChunkTransferRequests(
        record, buffer, length, opcode, [this](const std::string& segment) {
            const auto handle = engine_.openSegment(segment);
            if (handle == static_cast<SegmentHandle>(ERR_INVALID_ARGUMENT)) {
                return tl::expected<SegmentHandle, ErrorCode>(
                    tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND));
            }
            return tl::expected<SegmentHandle, ErrorCode>(handle);
        });
    if (!requests) {
        return requests.error();
    }

    BatchGuard batch(engine_, requests->size());
    if (batch.id() == INVALID_BATCH_ID) {
        return ErrorCode::TRANSFER_FAIL;
    }
    if (!engine_.submitTransfer(batch.id(), *requests).ok()) {
        return ErrorCode::TRANSFER_FAIL;
    }
    while (std::chrono::steady_clock::now() < deadline) {
        TransferStatus status{};
        if (!engine_.getBatchTransferStatus(batch.id(), status).ok()) {
            return ErrorCode::TRANSFER_FAIL;
        }
        if (status.s == TransferStatusEnum::COMPLETED) {
            return status.transferred_bytes == length ? ErrorCode::OK
                                                      : ErrorCode::TRANSFER_FAIL;
        }
        if (status.s == TransferStatusEnum::FAILED ||
            status.s == TransferStatusEnum::TIMEOUT ||
            status.s == TransferStatusEnum::CANCELED ||
            status.s == TransferStatusEnum::INVALID) {
            return ErrorCode::TRANSFER_FAIL;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return ErrorCode::RPC_TIMEOUT;
}

}  // namespace mooncake
