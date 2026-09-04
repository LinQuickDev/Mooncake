#pragma once

#include <optional>
#include <utility>

#include "types.h"
#include "../types.h"

namespace mooncake::io_pattern {

struct CfmPollResult {
    enum class Status { kCommand, kEmpty, kError };

    static CfmPollResult Command(PolicyCommand command,
                                 uint64_t delivery_id = 0) {
        return {.status = Status::kCommand,
                .command = std::move(command),
                .delivery_id = delivery_id};
    }
    static CfmPollResult Empty() { return {.status = Status::kEmpty}; }
    static CfmPollResult Error() { return {.status = Status::kError}; }

    Status status{Status::kEmpty};
    std::optional<PolicyCommand> command;
    uint64_t delivery_id{0};
};

// Transport-neutral CFM RPC channel. Implementations own serialization,
// retries and connection lifecycle.
class CfmChannel {
   public:
    virtual ~CfmChannel() = default;
    virtual bool SendSnapshot(const IoPatternSnapshot& snapshot) = 0;
    virtual CfmPollResult PollPolicyResult() = 0;
    std::optional<PolicyCommand> PollPolicy() {
        auto result = PollPolicyResult();
        if (result.status != CfmPollResult::Status::kCommand ||
            !result.command) {
            return std::nullopt;
        }
        if (!AcknowledgePolicy(result.delivery_id, true)) return std::nullopt;
        return std::move(result.command);
    }
    virtual bool AcknowledgePolicy(uint64_t, bool) { return true; }
    virtual ErrorCode ExecutePrefetch(const PrefetchPlan& plan) = 0;
};

}  // namespace mooncake::io_pattern
