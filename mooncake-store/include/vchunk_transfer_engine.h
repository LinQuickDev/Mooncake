#pragma once

#include <chrono>
#include <functional>
#include <vector>

#include "transfer_engine.h"
#include "vchunk_client.h"

namespace mooncake {

using VChunkSegmentResolver =
    std::function<tl::expected<SegmentHandle, ErrorCode>(const std::string&)>;

tl::expected<std::vector<TransferRequest>, ErrorCode>
BuildVChunkTransferRequests(const VChunkMetadataRecord& record, void* buffer,
                            size_t length, TransferRequest::OpCode opcode,
                            const VChunkSegmentResolver& resolve_segment);

class TransferEngineVChunkDataPlane final : public VChunkDataPlane {
   public:
    explicit TransferEngineVChunkDataPlane(TransferEngine& engine)
        : engine_(engine) {}

    ErrorCode Write(const VChunkMetadataRecord& record, const void* source,
                    size_t length,
                    std::chrono::steady_clock::time_point deadline) override;
    ErrorCode Read(const VChunkMetadataRecord& record, void* destination,
                   size_t length,
                   std::chrono::steady_clock::time_point deadline) override;

   private:
    ErrorCode Transfer(const VChunkMetadataRecord& record, void* buffer,
                       size_t length, TransferRequest::OpCode opcode,
                       std::chrono::steady_clock::time_point deadline);

    TransferEngine& engine_;
};

}  // namespace mooncake
