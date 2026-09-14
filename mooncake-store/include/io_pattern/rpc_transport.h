#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <functional>
#include <utility>
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

// Transport for CFM reports addressed to the SubMaster that owns the reported
// keys. There is no authentication: Mooncake RPCs run inside the trusted
// deployment, and CFM is an embedded component of every SubMaster reached over
// its regular coro_rpc endpoint (the same endpoint all other Mooncake APIs
// use). Only method/payload delivery is needed because the receiver merges
// reports into its local runtime instead of maintaining per-node policy
// queues.
class CfmRpcTransport {
   public:
    virtual ~CfmRpcTransport() = default;
    virtual bool Send(std::string_view method, std::string_view payload,
                      std::chrono::milliseconds timeout) = 0;
};

// Production CFM transport over Mooncake's existing coro_rpc connection pool.
// It targets the CFM handler registered on the Master RPC service of the
// SubMaster that owns the reported keys.
class CoroRpcCfmTransport final : public CfmRpcTransport {
   public:
    CoroRpcCfmTransport(std::string endpoint,
                        std::chrono::milliseconds default_timeout =
                            std::chrono::milliseconds(500));
    ~CoroRpcCfmTransport() override;

    bool Send(std::string_view method, std::string_view payload,
              std::chrono::milliseconds timeout) override;

   private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

struct CfmRpcConfig {
    std::chrono::milliseconds timeout{500};
};

// An in-process CFM endpoint for embedded deployments and integration tests.
// It is intentionally transport-agnostic at the codec boundary: a
// socket/HTTP implementation can expose the same method names and wire bytes.
class InProcessCfmRpcTransport final : public CfmRpcTransport {
   public:
    using SendHandler = std::function<bool(std::string_view, std::string_view)>;

    explicit InProcessCfmRpcTransport(SendHandler send_handler = {})
        : send_handler_(std::move(send_handler)) {}

    bool Send(std::string_view method, std::string_view payload,
              std::chrono::milliseconds) override;

    void SetSendHandler(SendHandler handler);

   private:
    mutable std::mutex mutex_;
    SendHandler send_handler_;
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
    bool SendMetricBatch(const MetricBatch& batch) override;
    ErrorCode ExecutePrefetch(const PrefetchPlan& plan) override;

   private:
    std::shared_ptr<CfmRpcTransport> transport_;
    std::shared_ptr<CfmRpcCodec> codec_;
    CfmRpcConfig config_;
};

// Reuses a bounded set of CFM channels. Requests are selected round-robin; an
// unavailable member is skipped so one failed connection does not stall
// metric reporting.
class CfmChannelPool final : public CfmChannel {
   public:
    explicit CfmChannelPool(std::vector<std::shared_ptr<CfmChannel>> channels)
        : channels_(std::move(channels)) {}

    bool SendSnapshot(const IoPatternSnapshot& snapshot) override;
    bool SendMetricBatch(const MetricBatch& batch) override;
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
