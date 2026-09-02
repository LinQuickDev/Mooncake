#pragma once

#include <memory>
#include <string_view>

#include "cfm_protocol.h"
#include "runtime.h"

namespace mooncake::io_pattern {

// Server-side counterpart of CfmRpcChannel. Bind Handle() as an
// InProcessCfmRpcTransport::SendHandler or adapt it to a network RPC server.
class CfmIngress final {
   public:
    explicit CfmIngress(std::shared_ptr<IoPatternRuntime> runtime,
                        std::shared_ptr<CfmBinaryCodec> codec =
                            std::make_shared<CfmBinaryCodec>())
        : runtime_(std::move(runtime)), codec_(std::move(codec)) {}

    bool Handle(std::string_view method, std::string_view payload);

   private:
    std::shared_ptr<IoPatternRuntime> runtime_;
    std::shared_ptr<CfmBinaryCodec> codec_;
};

}  // namespace mooncake::io_pattern
