#pragma once

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

namespace embtable {

// Unified string view type (see design doc section 8.4)
using StringView = std::string_view;

// Error codes used by Status (non-exhaustive; modules may extend)
enum class ErrorCode : int {
    kOk = 0,
    kInvalidArgument = 1,
    kNotFound = 2,
    kAlreadyExists = 3,
    kIndexNotBuilt = 4,
    kIndexBuilt = 5,  // ShareMap is read-only after BuildIndex
    kInternal = 6,
    kIOError = 7,
    kBufferFull = 8,
    kOutOfRange = 9,
    kNotSupported = 10,
};

// Checked helpers for sizes crossing RPC, allocation, and pointer boundaries.
inline bool CheckedAdd(uint64_t lhs, uint64_t rhs, uint64_t& result) {
    if (rhs > std::numeric_limits<uint64_t>::max() - lhs) return false;
    result = lhs + rhs;
    return true;
}

inline bool CheckedMultiply(uint64_t lhs, uint64_t rhs, uint64_t& result) {
    if (lhs != 0 && rhs > std::numeric_limits<uint64_t>::max() / lhs) {
        return false;
    }
    result = lhs * rhs;
    return true;
}

inline bool IsRangeValid(uint64_t offset, uint64_t length, uint64_t capacity) {
    return offset <= capacity && length <= capacity - offset;
}

// A simple Status type holding a code and an optional message.
// (design doc section 8.4)
class Status {
   public:
    Status() = default;
    Status(ErrorCode code, std::string msg)
        : code_(static_cast<int>(code)), msg_(std::move(msg)) {}

    static Status OK() { return Status(); }

    static Status Error(ErrorCode code, std::string msg = {}) {
        return Status(code, std::move(msg));
    }

    // Transport-level error returned by an RPC client. This remains encoded
    // as kIOError for compatibility with the existing RPC status wire format,
    // while callers can distinguish it from storage and application errors.
    static Status NetworkError(std::string msg) {
        Status status(ErrorCode::kIOError, std::move(msg));
        status.networkError_ = true;
        return status;
    }

    bool IsOk() const { return code_ == 0; }
    bool IsError() const { return code_ != 0; }
    int code() const { return code_; }
    const std::string& msg() const { return msg_; }
    bool IsNetworkError() const { return networkError_; }

    // Allow `if (status)` idiom.
    explicit operator bool() const { return IsOk(); }

   private:
    int code_ = 0;
    std::string msg_;
    bool networkError_ = false;
};

// Hash function selector (design doc section 8.4)
enum class HashFunctionType {
    kCity,
    kMurmur3,
    kXxHash,
};

}  // namespace embtable
