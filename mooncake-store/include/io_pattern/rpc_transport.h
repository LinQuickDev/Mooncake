#pragma once

#include <chrono>
#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <string_view>
#include <functional>
#include <vector>

#include "cfm_channel.h"
#include "reporter.h"

namespace mooncake::io_pattern {

class CfmRpcCodec {
   public:
    virtual ~CfmRpcCodec() = default;
    virtual std::string EncodeSnapshot(const IoPatternSnapshot&) const = 0;
    virtual std::string EncodePrefetch(const PrefetchPlan&) const = 0;
    virtual std::string EncodeMetricBatch(const MetricBatch&) const = 0;
    virtual std::optional<PolicyCommand> DecodePolicy(
        const std::string&) const = 0;
};

class CfmRpcTransport {
   public:
    virtual ~CfmRpcTransport() = default;
    // Implementations that communicate with a remote CFM should override this
    // to bind the connection to the configured service credential. Keeping a
    // default preserves compatibility with trusted in-process transports.
    virtual bool Authenticate(std::string_view token) { return token.empty(); }
    virtual bool Send(std::string_view method, std::string_view payload,
                      std::chrono::milliseconds timeout) = 0;
    virtual std::optional<std::string> Receive(
        std::string_view method, std::chrono::milliseconds timeout) = 0;
};

struct CfmRpcConfig {
    std::chrono::milliseconds timeout{500};
    std::string auth_token;
};

// A concrete authenticated endpoint for embedded deployments and integration
// tests. It is intentionally transport-agnostic at the codec boundary: a
// socket/HTTP implementation can expose the same method names and wire bytes.
class InProcessCfmRpcTransport final : public CfmRpcTransport {
   public:
    using SendHandler = std::function<bool(std::string_view, std::string_view)>;

    explicit InProcessCfmRpcTransport(std::string auth_token,
                                      SendHandler send_handler = {})
        : auth_token_(std::move(auth_token)), send_handler_(std::move(send_handler)) {}

    bool Authenticate(std::string_view token) override;
    bool Send(std::string_view method, std::string_view payload,
              std::chrono::milliseconds timeout) override;
    std::optional<std::string> Receive(
        std::string_view method, std::chrono::milliseconds timeout) override;

    void EnqueuePolicy(std::string payload);
    void SetSendHandler(SendHandler handler);

   private:
    mutable std::mutex mutex_;
    const std::string auth_token_;
    bool authenticated_{false};
    SendHandler send_handler_;
    std::queue<std::string> policies_;
};

class CfmRpcChannel final : public CfmChannel {
   public:
    CfmRpcChannel(std::shared_ptr<CfmRpcTransport> transport,
                  std::shared_ptr<CfmRpcCodec> codec,
                  CfmRpcConfig config = {})
        : transport_(std::move(transport)),
          codec_(std::move(codec)),
          config_(config) {}

    bool SendSnapshot(const IoPatternSnapshot& snapshot) override;
    std::optional<PolicyCommand> PollPolicy() override;
    ErrorCode ExecutePrefetch(const PrefetchPlan& plan) override;
    bool SendMetricBatch(const MetricBatch& batch);

   private:
    bool EnsureAuthenticated();

    std::shared_ptr<CfmRpcTransport> transport_;
    std::shared_ptr<CfmRpcCodec> codec_;
    CfmRpcConfig config_;
    std::mutex authentication_mutex_;
    bool authenticated_{false};
};

// Reuses a bounded set of authenticated CFM channels. Requests are selected
// round-robin; an unavailable member is skipped so one failed connection does
// not stall policy reporting.
class CfmChannelPool final : public CfmChannel {
   public:
    explicit CfmChannelPool(std::vector<std::shared_ptr<CfmChannel>> channels)
        : channels_(std::move(channels)) {}

    bool SendSnapshot(const IoPatternSnapshot& snapshot) override;
    std::optional<PolicyCommand> PollPolicy() override;
    ErrorCode ExecutePrefetch(const PrefetchPlan& plan) override;

   private:
    std::shared_ptr<CfmChannel> Next() const;

    std::vector<std::shared_ptr<CfmChannel>> channels_;
    mutable std::atomic<size_t> next_{0};
};

// Adapts the RPC channel to the reporter's asynchronous batch sink.
inline MetricBatchSink MakeCfmMetricBatchSink(
    std::shared_ptr<CfmRpcChannel> channel) {
    return [channel = std::move(channel)](const MetricBatch& batch) {
        return channel && channel->SendMetricBatch(batch);
    };
}

}  // namespace mooncake::io_pattern
