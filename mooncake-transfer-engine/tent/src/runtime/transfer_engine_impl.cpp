// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/control_plane.h"

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <map>
#include <optional>
#include <random>
#include <stdexcept>

#include "tent/common/config.h"
#include "tent/common/status.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/segment.h"
#include "tent/runtime/segment_tracker.h"
#include "tent/runtime/progress_worker.h"
#include "tent/runtime/proxy_manager.h"
#include "tent/runtime/transport.h"
#include "tent/runtime/topology.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/slab.h"
#include "tent/common/utils/ip.h"
#include "tent/common/utils/random.h"
#include "tent/metrics/tent_metrics.h"
#include "tent/metrics/config_loader.h"

namespace mooncake {
namespace tent {

namespace {
constexpr uint8_t kRedisMaxDbIndex = 255;
constexpr uint8_t kRedisDefaultDbIndex = 0;

uint64_t randomIdentityWord() {
    try {
        static thread_local std::random_device random;
        return (static_cast<uint64_t>(random()) << 32) ^ random() ^
               getCurrentTimeInNano();
    } catch (const std::exception&) {
        // random_device is allowed to be unavailable on unusual libstdc++
        // targets. Preserve startup while still mixing a process-local nonce
        // into the clock-seeded fallback generator.
        static std::atomic<uint64_t> nonce{1};
        auto& random = SimpleRandom::Get();
        return (static_cast<uint64_t>(random.next()) << 32) ^ random.next() ^
               nonce.fetch_add(1, std::memory_order_relaxed);
    }
}

SenderInstanceId makeSenderInstanceId() {
    SenderInstanceId id{randomIdentityWord(), randomIdentityWord()};
    if (id.empty()) id.low = 1;
    return id;
}

ReceiverSessionId makeReceiverSessionId() {
    ReceiverSessionId id{randomIdentityWord(), randomIdentityWord()};
    if (id.empty()) id.low = 1;
    return id;
}
}  // namespace

struct Batch {
    Batch() : max_size(0) { sub_batch.fill(nullptr); }

    ~Batch() {}

    std::array<Transport::SubBatchRef, kSupportedTransportTypes> sub_batch;
    std::vector<TaskInfo> task_list;
    size_t max_size;
    size_t runtime_refs{0};
    bool free_requested{false};
    uint64_t queue_token{0};

    struct SubmitHook {
        size_t start_task_id{0};
        size_t end_task_id{0};  // [start, end)
        Notification notifi;
        bool fired{false};
        std::unordered_set<SegmentID> targets;
    };
    std::vector<SubmitHook> submit_hooks;
};

struct PreservedTentConfigOverrides {
    std::optional<std::string> metadata_type;
    std::optional<std::string> metadata_servers;
    std::optional<std::string> local_segment_name;
    std::optional<std::string> rpc_server_hostname;
    std::optional<json> rpc_server_port;
};

template <typename T>
std::optional<T> captureExplicitConfigValue(const Config& config,
                                            const std::string& key,
                                            const T& default_value) {
    if (!config.contains(key)) {
        return std::nullopt;
    }
    return config.get<T>(key, default_value);
}

std::optional<long long> tryParseConfigIntString(const std::string& value) {
    try {
        size_t parsed_chars = 0;
        long long parsed_value = std::stoll(value, &parsed_chars);
        if (parsed_chars == value.size()) {
            return parsed_value;
        }
    } catch (...) {
    }
    return std::nullopt;
}

Status validateRpcServerPortValue(long long value, const std::string& source,
                                  uint16_t& port) {
    constexpr long long kMinPort = 0;
    constexpr long long kMaxPort = std::numeric_limits<uint16_t>::max();
    if (value < kMinPort || value > kMaxPort) {
        return Status::InvalidArgument("Invalid rpc_server_port '" + source +
                                       "', expected value in range [0, " +
                                       std::to_string(kMaxPort) + "]" +
                                       LOC_MARK);
    }

    port = static_cast<uint16_t>(value);
    return Status::OK();
}

Status getRpcServerPortFromConfig(const Config& config, uint16_t default_value,
                                  uint16_t& port) {
    constexpr const char* kKey = "rpc_server_port";
    if (!config.contains(kKey)) {
        port = default_value;
        return Status::OK();
    }

    json raw_value = config.get<json>(kKey, json());
    if (raw_value.is_number_integer() || raw_value.is_number_unsigned()) {
        long long numeric_value = raw_value.get<long long>();
        return validateRpcServerPortValue(numeric_value,
                                          std::to_string(numeric_value), port);
    }

    if (raw_value.is_string()) {
        auto string_value = raw_value.get<std::string>();
        auto parsed_value = tryParseConfigIntString(string_value);
        if (!parsed_value.has_value()) {
            return Status::InvalidArgument(
                "Invalid rpc_server_port '" + string_value +
                "', expected integer in range [0, 65535]" LOC_MARK);
        }
        return validateRpcServerPortValue(*parsed_value, string_value, port);
    }

    return Status::InvalidArgument(
        "rpc_server_port must be an integer or integer string" LOC_MARK);
}

PreservedTentConfigOverrides captureExplicitTransferEngineConfig(
    const Config& config) {
    PreservedTentConfigOverrides preserved;
    preserved.metadata_type =
        captureExplicitConfigValue(config, "metadata_type", std::string());
    preserved.metadata_servers =
        captureExplicitConfigValue(config, "metadata_servers", std::string());
    preserved.local_segment_name =
        captureExplicitConfigValue(config, "local_segment_name", std::string());
    preserved.rpc_server_hostname = captureExplicitConfigValue(
        config, "rpc_server_hostname", std::string());
    preserved.rpc_server_port =
        captureExplicitConfigValue(config, "rpc_server_port", json());
    return preserved;
}

template <typename T>
void restoreExplicitConfigValue(Config& config, const std::string& key,
                                const std::optional<T>& value) {
    if (value.has_value()) {
        config.set(key, *value);
    }
}

void restoreExplicitTransferEngineConfig(
    Config& config, const PreservedTentConfigOverrides& preserved) {
    restoreExplicitConfigValue(config, "metadata_type",
                               preserved.metadata_type);
    restoreExplicitConfigValue(config, "metadata_servers",
                               preserved.metadata_servers);
    restoreExplicitConfigValue(config, "local_segment_name",
                               preserved.local_segment_name);
    restoreExplicitConfigValue(config, "rpc_server_hostname",
                               preserved.rpc_server_hostname);
    restoreExplicitConfigValue(config, "rpc_server_port",
                               preserved.rpc_server_port);
}

TransferEngineImpl::TransferEngineImpl()
    : conf_(std::make_shared<Config>()),
      available_(false),
      port_(0),
      ipv6_(false),
      merge_requests_(true) {
    ConfigHelper().loadFromEnv(*conf_);
    auto status = construct();
    if (!status.ok()) {
        LOG(ERROR) << "Failed to construct Transfer Engine instance: "
                   << status.ToString();
    } else {
        available_ = true;
    }
}

TransferEngineImpl::TransferEngineImpl(std::shared_ptr<Config> conf)
    : conf_(conf),
      available_(false),
      port_(0),
      ipv6_(false),
      merge_requests_(true) {
    auto preserved = captureExplicitTransferEngineConfig(*conf_);
    // Allow MC_TENT_CONF to supply shared defaults while keeping the caller's
    // explicit metadata identity intact.
    ConfigHelper().loadFromEnv(*conf_);
    restoreExplicitTransferEngineConfig(*conf_, preserved);
    auto status = construct();
    if (!status.ok()) {
        LOG(ERROR) << "Failed to construct Transfer Engine instance: "
                   << status.ToString();
    } else {
        available_ = true;
    }
}

TransferEngineImpl::~TransferEngineImpl() { deconstruct(); }

std::string randomSegmentName() {
    std::string name = "segment_noname_";
    for (int i = 0; i < 8; ++i) name += 'a' + SimpleRandom::Get().next(26);
    return name;
}

void setLogLevel(const std::string level) {
    if (level == "info")
        FLAGS_minloglevel = google::INFO;
    else if (level == "warning")
        FLAGS_minloglevel = google::WARNING;
    else if (level == "error")
        FLAGS_minloglevel = google::ERROR;
}

static std::string readIdentityFile(const char* path) {
    std::ifstream file(path);
    if (!file) return "";
    std::string content((std::istreambuf_iterator<char>(file)),
                        std::istreambuf_iterator<char>());
    if (!content.empty() && content.back() == '\n') content.pop_back();
    return content;
}

std::string getMachineID() {
    const std::string boot_id =
        readIdentityFile("/proc/sys/kernel/random/boot_id");
    const std::string machine_id = readIdentityFile("/etc/machine-id");

    if (!boot_id.empty() && !machine_id.empty()) {
        return boot_id + ":" + machine_id;
    }

    if (!boot_id.empty()) return boot_id;
    if (!machine_id.empty()) return machine_id;

    std::string content = "undefined_machine_";
    for (int i = 0; i < 16; ++i) content += 'a' + SimpleRandom::Get().next(26);
    LOG(WARNING) << "TENT getMachineID source=fallback value=" << content;
    return content;
}

Status TransferEngineImpl::setupLocalSegment() {
    auto& manager = metadata_->segmentManager();
    CHECK_STATUS(manager.updateLocal([&](SegmentDesc& segment) -> Status {
        segment.name = local_segment_name_;
        segment.type = SegmentType::Memory;
        segment.machine_id = getMachineID();
        segment.rpc_server_addr = buildIpAddrWithPort(hostname_, port_, ipv6_);
        auto& detail = std::get<MemorySegmentDesc>(segment.detail);
        detail.topology = *(topology_.get());
        if (receiver_credit_authority_) {
            detail.receiver_credit = receiver_credit_authority_->advert();
        } else {
            detail.receiver_credit.reset();
        }
        return Status::OK();
    }));
    local_segment_tracker_ = std::make_unique<SegmentTracker>(manager);
    return manager.synchronizeLocal();
}

Status TransferEngineImpl::construct() {
    auto metadata_type = conf_->get("metadata_type", "p2p");
    auto metadata_servers = conf_->get("metadata_servers", "");

    setLogLevel(conf_->get("log_level", "info"));
    hostname_ = conf_->get("rpc_server_hostname", "");
    local_segment_name_ = conf_->get("local_segment_name", "");
    CHECK_STATUS(getRpcServerPortFromConfig(*conf_, 0, port_));
    merge_requests_ = conf_->get("merge_requests", true);
    max_failover_attempts_ = conf_->get("max_failover_attempts", 3);
    enable_auto_failover_on_poll_ =
        conf_->get("enable_auto_failover_on_poll", true);
    enable_progress_worker_ = conf_->get("enable_progress_worker", false);
    runtime_queue_config_.enabled = conf_->get("enable_runtime_queue", false);
    const bool receiver_credit_legacy_enable =
        conf_->get("receiver_credit/enable", false);
    receiver_credit_config_.sender_enabled = conf_->get(
        "receiver_credit/sender_enable", receiver_credit_legacy_enable);
    receiver_credit_config_.receiver_enabled = conf_->get(
        "receiver_credit/receiver_enable", receiver_credit_legacy_enable);
    if (receiver_credit_config_.sender_enabled) {
        runtime_queue_config_.enabled = true;
    }
    if (runtime_queue_config_.enabled) enable_progress_worker_ = true;
    runtime_queue_config_.limits.max_outstanding_owners =
        conf_->get("runtime_queue/max_outstanding_owners", 1024UL);
    runtime_queue_config_.limits.max_outstanding_bytes =
        conf_->get("runtime_queue/max_outstanding_bytes", 1UL << 30);
    runtime_queue_config_.limits.staging_owner_reserve =
        conf_->get("runtime_queue/staging_owner_reserve", 0UL);
    runtime_queue_config_.limits.staging_byte_reserve =
        conf_->get("runtime_queue/staging_byte_reserve", 0UL);
    runtime_queue_config_.limits.deadline_aware =
        conf_->get("runtime_queue/deadline_aware", false);
    runtime_queue_config_.limits.mlu_local_threshold =
        conf_->get("runtime_queue/mlu_local_threshold", 0.0);
    runtime_queue_config_.limits.promotion_slack_ns =
        conf_->get("runtime_queue/promotion_slack_ns", 0UL);
    runtime_queue_config_.max_dispatch_owners =
        conf_->get("runtime_queue/max_dispatch_owners", 64UL);
    runtime_queue_config_.max_dispatch_bytes =
        conf_->get("runtime_queue/max_dispatch_bytes", 64UL << 20);
    runtime_queue_config_.progress_fallback_interval =
        std::chrono::microseconds(
            conf_->get("runtime_queue/progress_fallback_interval_us", 50000UL));

    receiver_credit_config_.limits.capacity[0] =
        conf_->get("receiver_credit/data_bytes",
                   static_cast<uint64_t>(
                       runtime_queue_config_.limits.max_outstanding_bytes));
    receiver_credit_config_.limits.capacity[1] =
        conf_->get("receiver_credit/request_slots",
                   static_cast<uint64_t>(
                       runtime_queue_config_.limits.max_outstanding_owners));
    receiver_credit_config_.limits.capacity[2] = 0;
    receiver_credit_config_.limits.capacity[3] = 0;
    receiver_credit_config_.limits.freshness_ttl_ms =
        conf_->get("receiver_credit/freshness_ttl_ms", uint32_t{1000});
    receiver_credit_config_.limits.max_senders =
        conf_->get("receiver_credit/max_senders", size_t{1024});
    receiver_credit_config_.limits.max_entries =
        conf_->get("receiver_credit/max_authority_entries", size_t{4096});
    receiver_credit_config_.max_sender_entries =
        conf_->get("receiver_credit/max_sender_entries", size_t{4096});
    receiver_credit_config_.max_freshness_ttl_ms =
        conf_->get("receiver_credit/max_freshness_ttl_ms", uint32_t{60000});
    if (conf_->get("receiver_credit/staging_slots", uint64_t{0}) != 0 ||
        conf_->get("receiver_credit/consumer_slots", uint64_t{0}) != 0) {
        return Status::InvalidArgument(
            "staging/consumer receiver-credit resources are not supported "
            "by the PR6 runtime gate" LOC_MARK);
    }
    if (receiver_credit_config_.sender_enabled) {
        if (receiver_credit_config_.max_sender_entries == 0 ||
            receiver_credit_config_.max_freshness_ttl_ms == 0 ||
            runtime_queue_config_.progress_fallback_interval.count() <= 0) {
            return Status::InvalidArgument(
                "receiver-credit sender requires non-zero entries, TTL "
                "limit, and progress fallback interval" LOC_MARK);
        }
    }
    if (receiver_credit_config_.receiver_enabled) {
        if (receiver_credit_config_.limits.capacity[0] == 0 ||
            receiver_credit_config_.limits.capacity[1] == 0 ||
            receiver_credit_config_.limits.max_senders == 0 ||
            receiver_credit_config_.limits.max_entries <
                receiver_credit_config_.limits.max_senders) {
            return Status::InvalidArgument(
                "receiver credit requires non-zero bytes, request slots, "
                "and a replay-history limit no smaller than the sender "
                "limit" LOC_MARK);
        }
        if (receiver_credit_config_.limits.freshness_ttl_ms >
            receiver_credit_config_.max_freshness_ttl_ms) {
            return Status::InvalidArgument(
                "receiver credit TTL exceeds sender safety limit" LOC_MARK);
        }
    }
    if (runtime_queue_config_.enabled &&
        (runtime_queue_config_.max_dispatch_owners == 0 ||
         runtime_queue_config_.max_dispatch_bytes == 0)) {
        return Status::InvalidArgument(
            "runtime queue dispatch window must be non-zero" LOC_MARK);
    }
    runtime_queue_ = std::make_unique<LocalTransferAdmissionQueue>(
        runtime_queue_config_.limits);
    sender_instance_id_ = makeSenderInstanceId();
    sender_credit_ledger_ = std::make_unique<SenderCreditLedger>(
        std::max<size_t>(receiver_credit_config_.max_sender_entries, 1),
        std::max<uint32_t>(receiver_credit_config_.max_freshness_ttl_ms, 1));
    if (receiver_credit_config_.receiver_enabled) {
        receiver_credit_authority_ = std::make_unique<ReceiverCreditAuthority>(
            makeReceiverSessionId(), 1, receiver_credit_config_.limits);
        CHECK_STATUS(receiver_credit_authority_->status());
    }
    if (!hostname_.empty())
        CHECK_STATUS(checkLocalIpAddress(hostname_, ipv6_));
    else
        CHECK_STATUS(discoverLocalIpAddress(hostname_, ipv6_));

    topology_ = std::make_shared<Topology>();
    auto loader = &Platform::getLoader(conf_);
    CHECK_STATUS(topology_->discover(
        {loader}, conf_->get("transports/ub/enable", false)));

    metadata_ =
        std::make_shared<ControlService>(metadata_type, metadata_servers, this);

    if (receiver_credit_authority_) {
        metadata_->setReceiverCreditCallback(
            [this](const ReceiverCreditExchangeRequestV1& request,
                   ReceiverCreditExchangeReplyV1& reply) {
                return receiver_credit_authority_->exchange(request, reply);
            });
    }

    CHECK_STATUS(metadata_->start(port_, ipv6_));

    if (metadata_type == "p2p")
        local_segment_name_ = buildIpAddrWithPort(hostname_, port_, ipv6_);
    else if (local_segment_name_.empty())
        local_segment_name_ = randomSegmentName();

    CHECK_STATUS(setupLocalSegment());

    // Initialize transport selector
    transport_selector_ = std::make_unique<TransportSelector>(conf_);
    transport_selector_->setTopology(topology_);

    // Check if legacy mode is enabled (use original getTransportType logic)
    bool legacy_mode = conf_->get("use_legacy_transport_selection", false);
    transport_selector_->setLegacyMode(legacy_mode);
    if (legacy_mode) {
        LOG(INFO) << "Using legacy transport selection (original logic)";
    }

    CHECK_STATUS(loadTransports());

    std::string transport_string;
    for (auto& transport : transport_list_) {
        if (transport) {
            auto status = transport->install(local_segment_name_, metadata_,
                                             topology_, conf_);
            if (!status.ok()) {
                LOG(WARNING) << "Transport " << transport->getName()
                             << " skipped: " << status.ToString();
                transport = nullptr;
                continue;
            }
            transport_string += transport->getName();
            transport_string += " ";
        }
    }

    staging_proxy_ = std::make_unique<ProxyManager>(this);

    if (runtime_queue_config_.limits.deadline_aware &&
        runtime_queue_config_.limits.mlu_local_threshold > 0.0) {
        std::array<std::weak_ptr<Transport>, kSupportedTransportTypes>
            bandwidth_providers;
        bool has_bandwidth_provider = false;
        for (auto type : {TransportType::RDMA, TransportType::UB}) {
            auto& transport = transport_list_[static_cast<int>(type)];
            if (transport) {
                bandwidth_providers[static_cast<int>(type)] = transport;
                has_bandwidth_provider = true;
            }
        }
        if (has_bandwidth_provider) {
            runtime_queue_->setTransportDegradationPolicy(
                [bandwidth_providers](TransportType type) -> double {
                    if (type < 0 || type >= kSupportedTransportTypes)
                        return -1.0;
                    if (auto transport =
                            bandwidth_providers[static_cast<int>(type)]
                                .lock()) {
                        return transport->getEstimatedBandwidth();
                    }
                    return -1.0;
                },
                DegradationHooks{}, nullptr);
            LOG(INFO) << "Admission queue degradation: live network bw"
                      << ", theta_local="
                      << runtime_queue_config_.limits.mlu_local_threshold;
        } else {
            LOG(WARNING)
                << "Admission queue degradation requested but RDMA and UB "
                   "transports are unavailable";
        }
    }

    if (enable_progress_worker_) {
        progress_worker_ = std::make_unique<ProgressWorker>(
            this, runtime_queue_config_.enabled
                      ? runtime_queue_config_.progress_fallback_interval
                      : std::chrono::microseconds(0));
        progress_worker_->start();
    }

    // Initialize and start Metrics system
    auto metrics_config = MetricsConfigLoader::loadWithDefaults(conf_.get());
    if (metrics_config.enabled) {
        std::string validation_error;
        if (!MetricsConfigLoader::validateConfig(metrics_config,
                                                 &validation_error)) {
            LOG(WARNING) << "Invalid metrics configuration: "
                         << validation_error << ", Metrics system disabled";
        } else {
            // Initialize metrics
            auto status = TentMetrics::instance().initialize(metrics_config);
            if (!status.ok()) {
                LOG(WARNING) << "Failed to initialize TENT metrics: "
                             << status.ToString();
            } else {
                LOG(INFO) << "TENT Metrics system initialized";
            }
        }
    } else {
        LOG(INFO) << "Metrics system disabled by configuration";
    }

    if (conf_->get("verbose", false)) {
        LOG(INFO) << "========== Transfer Engine Parameters ==========";
        LOG(INFO) << " - Segment Name:       " << local_segment_name_;
        LOG(INFO) << " - RPC Server Address: "
                  << buildIpAddrWithPort(hostname_, port_, ipv6_);
        LOG(INFO) << " - Metadata Type:      " << metadata_type;
        LOG(INFO) << " - Metadata Servers:   " << metadata_servers;
        LOG(INFO) << " - Loaded Transports:  " << transport_string;
        LOG(INFO) << "================================================";
    } else {
        LOG(INFO) << "Transfer Engine " << local_segment_name_
                  << " started successfully";
    }

    return Status::OK();
}

Status TransferEngineImpl::deconstruct() {
    // Metrics cleanup is handled automatically by TentMetrics destructor

    // Stop the progress worker first so it cannot race with batch teardown
    // below (it dereferences BatchID into Batch* via progressBatch). Keep the
    // object alive until transports are destroyed: completion paths may still
    // issue a final no-op wake while their workers are joining.
    if (progress_worker_) {
        progress_worker_->stop();
    }

    // Only a reservation that never reached a transport can be returned here.
    // A remaining committed owner is not terminal, and the UB DMA fence is
    // established later while its transport is destroyed. Conservatively
    // abandon that authorization in the old receiver session instead of
    // making it available while DMA may still be active.
    size_t abandoned_receiver_credits = 0;
    for (auto& [_, queued] : queued_owners_) {
        if (!queued.credit.active) continue;
        if (!queued.credit.committed) {
            rollbackReceiverCredit(queued.credit);
        } else {
            queued.credit = ReceiverCreditReservation{};
            ++abandoned_receiver_credits;
        }
    }
    if (abandoned_receiver_credits != 0) {
        LOG(WARNING) << "Conservatively abandoned "
                     << abandoned_receiver_credits
                     << " in-flight receiver-credit reservations during "
                        "shutdown";
    }
    progressReceiverCreditReleases();

    // Destroy staging_proxy_ first: its destructor calls back into
    // unregisterLocalMemory/freeLocalMemory, which require
    // local_segment_tracker_ and metadata_ to be alive.
    staging_proxy_.reset();

    if (local_segment_tracker_) {
        local_segment_tracker_->forEach([&](const BufferDesc& desc) -> Status {
            // Snapshot entries are immutable; transports may scrub fields of
            // their deregistration argument, so hand them a copy.
            BufferDesc copy = desc;
            for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
                if (transport_list_[type])
                    transport_list_[type]->removeMemoryBuffer(copy);
            }
            return Status::OK();
        });
    }

    // Free all batches BEFORE destroying transports, so that
    // freeSubBatch() can properly return SubBatch/Slice objects
    // to the global Slab/allocator instances used by the transports.
    //
    // Safety note: freeSubBatch() only performs Slab deallocation and
    // does not access transport-internal state (workers, connections).
    // Callers must ensure no transfers are in-flight before calling
    // deconstruct().
    {
        std::lock_guard<std::recursive_mutex> lk(progress_mutex_);
        std::unordered_set<Batch*> released_batches;
        auto release_batch = [&](Batch* batch) {
            if (!released_batches.insert(batch).second) return;
            for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
                auto& transport = transport_list_[type];
                auto& sub_batch = batch->sub_batch[type];
                if (!transport || !sub_batch) continue;
                transport->freeSubBatch(sub_batch);
            }
            Slab<Batch>::Get().deallocate(batch);
        };
        for (auto& batch : batch_set_.active) release_batch(batch);
        for (auto& batch : batch_set_.freelist) release_batch(batch);
        batch_set_.active.clear();
        batch_set_.freelist.clear();
        alive_batches_.clear();
    }

    // Now safe to destroy transports (workers join here)
    for (auto& transport : transport_list_) transport.reset();
    progress_worker_.reset();
    local_segment_tracker_.reset();
    if (metadata_) {
        metadata_->segmentManager().deleteLocal();
        metadata_.reset();
    }
    return Status::OK();
}

const std::string TransferEngineImpl::getSegmentName() const {
    return local_segment_name_;
}

const std::string TransferEngineImpl::getRpcServerAddress() const {
    return hostname_;
}

uint16_t TransferEngineImpl::getRpcServerPort() const { return port_; }

Status TransferEngineImpl::exportLocalSegment(std::string& shared_handle) {
    return Status::NotImplemented(
        "exportLocalSegment not implemented" LOC_MARK);
}

Status TransferEngineImpl::importRemoteSegment(
    SegmentID& handle, const std::string& shared_handle) {
    return Status::NotImplemented(
        "importRemoteSegment not implemented" LOC_MARK);
}

Status TransferEngineImpl::openSegment(SegmentID& handle,
                                       const std::string& segment_name) {
    if (segment_name.empty() || segment_name == local_segment_name_) {
        handle = LOCAL_SEGMENT_ID;
        return Status::OK();
    }
    return metadata_->segmentManager().openRemote(handle, segment_name);
}

Status TransferEngineImpl::closeSegment(SegmentID handle) {
    if (handle == LOCAL_SEGMENT_ID) return Status::OK();
    return metadata_->segmentManager().closeRemote(handle);
}

Status TransferEngineImpl::getSegmentInfo(SegmentID handle, SegmentInfo& info) {
    // Owning reference: keeps the snapshot alive while we read through it.
    SegmentDescRef desc;
    if (handle == LOCAL_SEGMENT_ID) {
        desc = metadata_->segmentManager().getLocal();
    } else {
        CHECK_STATUS(metadata_->segmentManager().getRemoteCached(desc, handle));
    }
    if (desc->type == SegmentType::File) {
        info.type = SegmentInfo::File;
        auto& detail = std::get<FileSegmentDesc>(desc->detail);
        for (auto& entry : detail.buffers) {
            info.buffers.emplace_back(
                SegmentInfo::Buffer{.base = entry.offset,
                                    .length = entry.length,
                                    .location = kWildcardLocation});
        }
    } else {
        info.type = SegmentInfo::Memory;
        auto& detail = std::get<MemorySegmentDesc>(desc->detail);
        for (auto& entry : detail.buffers) {
            if (entry.internal) continue;
            info.buffers.emplace_back(
                SegmentInfo::Buffer{.base = (uint64_t)entry.addr,
                                    .length = entry.length,
                                    .location = entry.location});
        }
    }
    return Status::OK();
}

Status TransferEngineImpl::allocateLocalMemory(void** addr, size_t size,
                                               Location location) {
    return allocateLocalMemory(addr, size, location, false);
}

Status TransferEngineImpl::allocateLocalMemory(void** addr, size_t size,
                                               Location location,
                                               bool internal) {
    // Decide transport type based on location
    MemoryOptions options;
    options.location = location;
    options.internal = internal;
    if (location == kWildcardLocation ||
        LocationParser(location).type() == "cpu") {
        if (transport_list_[SHM])
            options.type = SHM;
        else if (transport_list_[RDMA])
            options.type = RDMA;
        else if (transport_list_[UB])
            options.type = UB;
        else
            options.type = TCP;
    } else {
        if (transport_list_[MNNVL])
            options.type = MNNVL;
        else if (transport_list_[RDMA])
            options.type = RDMA;
        else if (transport_list_[UB])
            options.type = UB;
        else
            options.type = TCP;
    }
    return allocateLocalMemory(addr, size, options);
}

Status TransferEngineImpl::allocateLocalMemory(void** addr, size_t size,
                                               MemoryOptions& options) {
    if (options.type == UNSPEC) {
        if (transport_list_[RDMA])
            options.type = RDMA;
        else if (transport_list_[UB])
            options.type = UB;
        else if (transport_list_[TCP])
            options.type = TCP;
        else
            return Status::InvalidArgument(
                "Not supported type in memory options" LOC_MARK);
    }
    auto& transport = transport_list_[options.type];
    if (!transport)
        return Status::InvalidArgument(
            "Not supported type in memory options" LOC_MARK);
    CHECK_STATUS(transport->allocateLocalMemory(addr, size, options));
    std::lock_guard<std::mutex> lock(mutex_);
    AllocatedMemory entry{.addr = *addr,
                          .size = size,
                          .transport = transport.get(),
                          .options = options};
    allocated_memory_.push_back(entry);
    return Status::OK();
}

Status TransferEngineImpl::freeLocalMemory(void* addr) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = allocated_memory_.begin(); it != allocated_memory_.end();
         ++it) {
        if (it->addr == addr) {
            auto status = it->transport->freeLocalMemory(addr, it->size);
            allocated_memory_.erase(it);
            return status;
        }
    }
    return Status::InvalidArgument("Address region not registered" LOC_MARK);
}

Status TransferEngineImpl::registerLocalMemory(void* addr, size_t size,
                                               Permission permission) {
    MemoryOptions options;
    options.perm = permission;
    return registerLocalMemory({addr}, {size}, options);
}

Status TransferEngineImpl::registerLocalMemory(std::vector<void*> addr_list,
                                               std::vector<size_t> size_list,
                                               Permission permission) {
    MemoryOptions options;
    options.perm = permission;
    return registerLocalMemory(addr_list, size_list, options);
}

std::vector<TransportType> TransferEngineImpl::getSupportedTransports(
    TransportType request_type) {
    std::vector<TransportType> result;
    if (request_type != UNSPEC) {
        if (request_type >= 0 && request_type < kSupportedTransportTypes &&
            transport_list_[request_type]) {
            result.push_back(request_type);
        }
        return result;
    }
    if (transport_list_[MNNVL]) result.push_back(MNNVL);
    if (transport_list_[NVLINK]) result.push_back(NVLINK);
    if (transport_list_[RDMA]) result.push_back(RDMA);
    if (transport_list_[UB]) result.push_back(UB);
    if (transport_list_[SUNRISE_LINK]) result.push_back(SUNRISE_LINK);
    if (transport_list_[AscendDirect]) result.push_back(AscendDirect);
    if (transport_list_[SHM]) result.push_back(SHM);
    if (transport_list_[TCP]) result.push_back(TCP);
    if (transport_list_[GDS]) result.push_back(GDS);
    if (transport_list_[TPU]) result.push_back(TPU);
    return result;
}

Status TransferEngineImpl::registerLocalMemory(std::vector<void*> addr_list,
                                               std::vector<size_t> size_list,
                                               MemoryOptions& options) {
    if (addr_list.size() != size_list.size()) {
        return Status::InvalidArgument(
            "Mismatched addresses and sizes in registerLocalMemory" LOC_MARK);
    }
    auto transports = getSupportedTransports(options.type);
    if (transports.empty()) {
        return Status::InvalidArgument(
            "No available transport for registerLocalMemory" LOC_MARK);
    }

    // Build BufferDescs: warm-up → NUMA probe → fill location
    std::vector<BufferDesc> desc_list;
    desc_list.reserve(addr_list.size());
    for (size_t i = 0; i < addr_list.size(); ++i) {
        BufferDesc desc;
        desc.addr = (uint64_t)addr_list[i];
        desc.length = size_list[i];

        // MR warm-up: pin pages via temp ibv_reg_mr, benefits both
        // subsequent RDMA registration and NUMA probing
        bool pages_pinned = false;
        for (auto type : transports) {
            if (transport_list_[type]->warmupMemory(addr_list[i],
                                                    size_list[i])) {
                pages_pinned = true;
                break;
            }
        }

        // NUMA probe: skip prefault if warm-up already pinned pages
        auto entries = Platform::getLoader().getLocation(
            addr_list[i], size_list[i], pages_pinned);
        if (entries.size() == 1) {
            desc.location = entries[0].location;
        } else {
            desc.location = entries[0].location;
            desc.regions = coalesceRegions(entries);
        }
        desc.ref_count = 1;
        if (options.location != kWildcardLocation)
            desc.location = options.location;
        if (options.internal) desc.internal = options.internal;
        desc_list.push_back(std::move(desc));
    }

    auto status = local_segment_tracker_->addInBatch(
        desc_list, [&](std::vector<BufferDesc>& descs) -> Status {
            std::vector<TransportType> registered_transports;
            Status first_error = Status::OK();
            for (auto type : transports) {
                auto s = transport_list_[type]->addMemoryBuffer(descs, options);
                if (!s.ok()) {
                    LOG(WARNING) << "Failed to register memory with "
                                 << transport_list_[type]->getName() << ": "
                                 << s.ToString();
                    // addMemoryBuffer implementations are required to be
                    // transactional, but invoke remove on the failing
                    // transport as defensive cleanup for a partially
                    // constructed backend.
                    for (auto& desc : descs) {
                        auto cleanup =
                            transport_list_[type]->removeMemoryBuffer(desc);
                        if (!cleanup.ok()) LOG(WARNING) << cleanup.ToString();
                    }
                    if (first_error.ok()) first_error = s;
                    // Explicit transport registration is strict. UNSPEC keeps
                    // the long-standing best-effort behavior across several
                    // installed transports, but may not publish a buffer for
                    // which every backend failed.
                    if (options.type != UNSPEC) return s;
                    continue;
                }
                registered_transports.push_back(type);
            }
            if (registered_transports.empty() && !first_error.ok()) {
                return first_error;
            }
            return Status::OK();
        });
    if (!status.ok()) return status;
    // Synchronize local segment to metadata server so remote peers can see the
    // new buffers
    return metadata_->segmentManager().synchronizeLocal();
}

// WARNING: before exiting TE, make sure that all local memory are
// unregistered, otherwise the CUDA may halt!
Status TransferEngineImpl::unregisterLocalMemory(void* addr, size_t size) {
    bool removed = false;
    auto status = local_segment_tracker_->remove(
        (uint64_t)addr, size, [&](BufferDesc& desc) -> Status {
            removed = true;
            // Backends may scrub their own metadata from desc, including the
            // transports vector. Iterate a snapshot so every backend that
            // registered the buffer is still called exactly once.
            const auto transports = desc.transports;
            for (auto type : transports) {
                auto status = transport_list_[type]->removeMemoryBuffer(desc);
                if (!status.ok()) LOG(WARNING) << status.ToString();
            }
            return Status::OK();
        });
    if (!status.ok()) return status;
    if (!removed) return Status::OK();
    return metadata_->segmentManager().synchronizeLocal();
}

Status TransferEngineImpl::unregisterLocalMemory(
    std::vector<void*> addr_list, std::vector<size_t> size_list) {
    if (!size_list.empty() && addr_list.size() != size_list.size()) {
        return Status::InvalidArgument(
            "Mismatched addresses and sizes in unregisterLocalMemory" LOC_MARK);
    }
    bool removed_any = false;
    for (size_t i = 0; i < addr_list.size(); ++i) {
        bool removed = false;
        auto status = local_segment_tracker_->remove(
            (uint64_t)addr_list[i], size_list.empty() ? 0 : size_list[i],
            [&](BufferDesc& desc) -> Status {
                removed = true;
                const auto transports = desc.transports;
                for (auto type : transports) {
                    auto s = transport_list_[type]->removeMemoryBuffer(desc);
                    if (!s.ok()) LOG(WARNING) << s.ToString();
                }
                return Status::OK();
            });
        if (!status.ok()) return status;
        if (removed) removed_any = true;
    }
    if (!removed_any) return Status::OK();
    return metadata_->segmentManager().synchronizeLocal();
}

BatchID TransferEngineImpl::allocateBatch(size_t batch_size) {
    Batch* batch = Slab<Batch>::Get().allocate();
    if (!batch) return (BatchID)0;
    batch->max_size = batch_size;
    BatchID batch_id = (BatchID)batch;
    std::lock_guard<std::recursive_mutex> lk(progress_mutex_);
    batch_set_.active.insert(batch);
    alive_batches_.insert(batch_id);
    return batch_id;
}

Status TransferEngineImpl::freeBatch(BatchID batch_id) {
    if (!batch_id) return Status::InvalidArgument("Invalid batch ID" LOC_MARK);
    Batch* batch = (Batch*)(batch_id);
    std::lock_guard<std::recursive_mutex> lk(progress_mutex_);
    if (!alive_batches_.count(batch_id))
        return Status::InvalidArgument("Batch is not alive" LOC_MARK);
    if (runtime_queue_config_.enabled && batch->queue_token != 0) {
        auto retire_status = retireQueueForBatch(batch);
        if (!retire_status.ok() && !retire_status.IsInvalidEntry()) {
            return retire_status;
        }
    }
    if (batch->free_requested) {
        CHECK_STATUS(lazyFreeBatch());
        return Status::OK();
    }
    batch->free_requested = true;
    batch_set_.freelist.push_back(batch);
    lazyFreeBatch();
    return Status::OK();
}

Status TransferEngineImpl::lazyFreeBatch() {
    std::lock_guard<std::recursive_mutex> lk(progress_mutex_);
    for (auto it = batch_set_.freelist.begin();
         it != batch_set_.freelist.end();) {
        auto& batch = *it;
        if (batch->runtime_refs > 0) {
            it++;
            continue;
        }
        TransferStatus overall_status;
        CHECK_STATUS(getTransferStatus((BatchID)batch, overall_status));
        if (overall_status.s == PENDING) {
            it++;
            continue;
        }
        if (runtime_queue_config_.enabled && batch->queue_token != 0) {
            CHECK_STATUS(retireQueueForBatch(batch));
        }
        for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
            auto& transport = transport_list_[type];
            auto& sub_batch = batch->sub_batch[type];
            if (transport && sub_batch) transport->freeSubBatch(sub_batch);
        }
        batch_set_.active.erase(batch);
        alive_batches_.erase((BatchID)batch);
        Slab<Batch>::Get().deallocate(batch);
        it = batch_set_.freelist.erase(it);
    }
    return Status::OK();
}

Status TransferEngineImpl::retainBatch(BatchID batch_id, Batch*& batch) {
    if (!batch_id) return Status::InvalidArgument("Invalid batch ID" LOC_MARK);
    std::lock_guard<std::recursive_mutex> lk(progress_mutex_);
    if (!alive_batches_.count(batch_id)) {
        return Status::InvalidArgument("Batch is not alive" LOC_MARK);
    }
    batch = (Batch*)batch_id;
    if (batch->free_requested) {
        return Status::InvalidArgument("Batch is being freed" LOC_MARK);
    }
    ++batch->runtime_refs;
    return Status::OK();
}

Status TransferEngineImpl::releaseBatch(Batch* batch) {
    if (!batch) return Status::InvalidArgument("Invalid batch" LOC_MARK);
    std::lock_guard<std::recursive_mutex> lk(progress_mutex_);
    if (batch->runtime_refs == 0) {
        return Status::InternalError("Batch runtime ref underflow" LOC_MARK);
    }
    --batch->runtime_refs;
    if (batch->runtime_refs == 0 && batch->free_requested) {
        CHECK_STATUS(lazyFreeBatch());
    }
    return Status::OK();
}

class TransferEngineImpl::BatchRef {
   public:
    BatchRef(TransferEngineImpl& engine, Batch* batch)
        : engine_(engine), batch_(batch) {}

    ~BatchRef() {
        if (!batch_) return;
        auto status = engine_.releaseBatch(batch_);
        if (!status.ok()) {
            LOG(WARNING) << "failed to release batch ref: "
                         << status.ToString();
        }
    }

    BatchRef(const BatchRef&) = delete;
    BatchRef& operator=(const BatchRef&) = delete;

    Batch* get() const { return batch_; }

    Status release() {
        if (!batch_) return Status::OK();
        auto status = engine_.releaseBatch(batch_);
        batch_ = nullptr;
        return status;
    }

   private:
    TransferEngineImpl& engine_;
    Batch* batch_{nullptr};
};

static bool isGpuType(MemoryType t) {
    // TPU HBM behaves like a GPU that lacks NIC access: it is a device-side
    // memory that can only reach the network by staging through host DRAM.
    // Treating it as a "gpu type" makes the capability checks route its
    // device<->host hop to TpuTransport (gpu_to_dram / dram_to_gpu) while
    // leaving gpu_to_gpu unsatisfiable, which forces host-DRAM staging.
    return t == MTYPE_CUDA || t == MTYPE_ROCM || t == MTYPE_TPU;
}

static bool checkAvailability(const std::shared_ptr<Transport>& xport,
                              MemoryType local) {
    if (local == MTYPE_CPU) return xport && xport->capabilities().dram_to_file;
    if (isGpuType(local)) return xport && xport->capabilities().gpu_to_file;
    return false;
}

static bool checkAvailability(const std::shared_ptr<Transport>& xport,
                              MemoryType local, MemoryType remote) {
    if (local == MTYPE_CPU && remote == MTYPE_CPU)
        return xport && xport->capabilities().dram_to_dram;
    if (isGpuType(local) && isGpuType(remote))
        return xport && xport->capabilities().gpu_to_gpu;
    if (local == MTYPE_CPU && isGpuType(remote))
        return xport && xport->capabilities().dram_to_gpu;
    if (isGpuType(local) && remote == MTYPE_CPU)
        return xport && xport->capabilities().gpu_to_dram;
    return false;
}

static MemoryType getTypeEnum(const std::string& type) {
    if (type == "cpu" || type == "*") return MTYPE_CPU;
    if (type == "cuda") return MTYPE_CUDA;
    if (type == "npu") return MTYPE_CUDA;
    if (type == "rocm") return MTYPE_ROCM;
    if (type == "tpu") return MTYPE_TPU;
    return MTYPE_UNKNOWN;
}

Status TransferEngineImpl::validateTransportHint(const Request& req,
                                                 size_t request_index) {
    if (req.transport_hint == UNSPEC) return Status::OK();
    if ((int)req.transport_hint < 0 ||
        (int)req.transport_hint >= kSupportedTransportTypes) {
        return Status::InvalidArgument(
            "transport_hint out of range for request[" +
            std::to_string(request_index) + "]" LOC_MARK);
    }
    if (!transport_list_[req.transport_hint]) {
        return Status::InvalidArgument(
            "transport_hint=" +
            std::string(transportTypeName(req.transport_hint)) +
            " is not enabled in config (request[" +
            std::to_string(request_index) + "])" LOC_MARK);
    }
    return Status::OK();
}

SelectionResult TransferEngineImpl::getTransportType(const Request& request,
                                                     int transport_index) {
    // Owning reference: keeps the snapshot alive while we read through it.
    SegmentDescRef desc;
    if (request.target_id == LOCAL_SEGMENT_ID) {
        desc = metadata_->segmentManager().getLocal();
    } else {
        auto status = metadata_->segmentManager().getRemoteCached(
            desc, request.target_id);
        if (!status.ok()) return SelectionResult{};
    }
    auto local_mtype = Platform::getLoader().getMemoryType(request.source);

    const TransportType hint = request.transport_hint;

    // Legacy mode: use original logic (before TransportSelector)
    if (transport_selector_ && transport_selector_->isLegacyMode()) {
        SelectionResult result;
        std::vector<TransportType> raw;
        if (desc->type == SegmentType::File) {
            if (checkAvailability(transport_list_[GDS], local_mtype))
                raw.push_back(GDS);
            if (checkAvailability(transport_list_[IOURING], local_mtype))
                raw.push_back(IOURING);
        } else {
            auto entry =
                desc->findBuffer(request.target_offset, request.length);
            if (entry) {
                bool same_machine = (request.target_id == LOCAL_SEGMENT_ID);
                if (!same_machine) {
                    auto local_desc = metadata_->segmentManager().getLocal();
                    same_machine = local_desc && !desc->machine_id.empty() &&
                                   !local_desc->machine_id.empty() &&
                                   desc->machine_id == local_desc->machine_id;
                }
                auto remote_mtype =
                    getTypeEnum(LocationParser(entry->location).type());
                for (auto type : entry->transports) {
                    // NVLINK/SHM are same-machine only; TPU is a
                    // local-stage-only executor and must never carry a remote
                    // hop.
                    if ((type == NVLINK || type == SHM || type == TPU) &&
                        !same_machine)
                        continue;
                    if (checkAvailability(transport_list_[type], local_mtype,
                                          remote_mtype)) {
                        raw.push_back(type);
                    }
                }
            }
        }

        auto candidates = TransportSelector::reorderWithHint(raw, hint);
        if (!candidates) {
            return result;  // UNSPEC: hint not authorized for this req
        }
        if (transport_index >= 0 &&
            (size_t)transport_index < candidates->size()) {
            result.transport = (*candidates)[transport_index];
        }
        return result;
    }

    // Selector mode: build ctx, then defer everything to
    // TransportSelector::select().
    SelectionContext ctx;
    ctx.transfer_size = request.length;
    ctx.priority_level =
        request.priority;  // Use request priority for selection
    ctx.policy_name = request.policy_name;  // Optional: bind to specific policy
    ctx.intent_type = request.intent_type;  // Business intent policy filter

    if (desc->type == SegmentType::File) {
        // File segment: use selector with empty buffer_transports
        ctx.segment_type = SegmentType::File;
        ctx.same_machine = true;  // File is always local
        ctx.local_memory_type = local_mtype;
        ctx.remote_memory_type = MTYPE_CPU;
        ctx.buffer_transports = nullptr;  // Empty - use policy priority
    } else {
        // Memory segment
        auto entry = desc->findBuffer(request.target_offset, request.length);
        if (!entry) return SelectionResult{};
        bool same_machine =
            (desc->machine_id ==
             metadata_->segmentManager().getLocal()->machine_id);
        auto remote_mtype = getTypeEnum(LocationParser(entry->location).type());

        ctx.segment_type = SegmentType::Memory;
        ctx.same_machine = same_machine;
        ctx.local_memory_type = local_mtype;
        ctx.remote_memory_type = remote_mtype;
        ctx.buffer_transports = &entry->transports;
    }

    return transport_selector_->select(ctx, transport_list_, transport_index,
                                       hint);
}

std::string printRequest(const Request& request) {
    std::stringstream ss;
    ss << "opcode " << request.opcode << " source " << request.source
       << " target_id " << request.target_id << " target_offset "
       << (void*)request.target_offset << " length " << request.length
       << " transport_hint " << transportTypeName(request.transport_hint);
    return ss.str();
}

struct BufferKey {
    uint64_t addr{0};
    uint64_t length{0};

    bool operator==(const BufferKey&) const = default;
};

struct RequestBoundaryInfo {
    std::optional<BufferKey> source_key;
    std::optional<BufferKey> target_key;
    // A required receiver-credit advert limits the size of one physical
    // owner.  Request merging must not turn individually admissible requests
    // into an owner that can never acquire credit.
    std::optional<uint64_t> receiver_credit_data_capacity;
};

struct MergeResult {
    std::vector<Request> request_list;
    std::map<size_t, size_t> task_lookup;
};

struct TransferEngineImpl::PreparedSubmit {
    struct Task {
        size_t merged_task_index{0};
        size_t task_id{0};
    };

    struct Owner {
        size_t owner_task_id{0};
        bool has_owner_task_id{false};
        std::vector<size_t> derived_task_ids;
        Request request{};
        SelectionResult route{};
        bool receiver_credit_required{false};
        bool receiver_credit_exempt{false};
        bool staging{false};
        std::vector<std::string> staging_params;
    };

    std::chrono::steady_clock::time_point submit_time{};
    std::vector<Task> tasks;
    std::vector<Owner> owners;
};

namespace {

bool tryAddUint64(uint64_t lhs, uint64_t rhs, uint64_t& out) {
    if (rhs > std::numeric_limits<uint64_t>::max() - lhs) return false;
    out = lhs + rhs;
    return true;
}

MergeResult makePassThroughMergeResult(const std::vector<Request>& requests) {
    MergeResult result;
    result.request_list.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        result.request_list.push_back(requests[i]);
        result.task_lookup[i] = i;
    }
    return result;
}

uint64_t requestSourceAddr(const Request& request) {
    return static_cast<uint64_t>(reinterpret_cast<uintptr_t>(request.source));
}

}  // namespace

MergeResult mergeRequests(const std::vector<Request>& requests,
                          const std::vector<RequestBoundaryInfo>& boundaries,
                          bool do_merge) {
    if (requests.empty()) return {};
    if (!do_merge || boundaries.size() != requests.size()) {
        return makePassThroughMergeResult(requests);
    }

    struct Item {
        Request req;
        RequestBoundaryInfo boundary;
        size_t orig_idx;
    };

    std::vector<Item> items;
    items.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        items.push_back({requests[i], boundaries[i], i});
    }

    std::sort(items.begin(), items.end(), [](const Item& a, const Item& b) {
        if (a.req.opcode != b.req.opcode) return a.req.opcode < b.req.opcode;
        if (a.req.target_id != b.req.target_id)
            return a.req.target_id < b.req.target_id;
        if (a.req.transport_hint != b.req.transport_hint)
            return a.req.transport_hint < b.req.transport_hint;
        if (a.req.target_offset != b.req.target_offset)
            return a.req.target_offset < b.req.target_offset;
        return requestSourceAddr(a.req) < requestSourceAddr(b.req);
    });

    auto can_merge = [](const Item& last, const Item& curr) {
        if (last.req.opcode != curr.req.opcode ||
            last.req.target_id != curr.req.target_id) {
            return false;
        }
        // Mixed transport_hint inside one batch must not be merged.
        if (last.req.transport_hint != curr.req.transport_hint) {
            return false;
        }
        if (!last.boundary.source_key || !curr.boundary.source_key ||
            !last.boundary.target_key || !curr.boundary.target_key) {
            return false;
        }
        if (last.boundary.source_key != curr.boundary.source_key ||
            last.boundary.target_key != curr.boundary.target_key) {
            return false;
        }
        if (curr.req.length >
            std::numeric_limits<size_t>::max() - last.req.length) {
            return false;
        }
        const size_t merged_length = last.req.length + curr.req.length;
        if (last.boundary.receiver_credit_data_capacity !=
            curr.boundary.receiver_credit_data_capacity) {
            return false;
        }
        if (last.boundary.receiver_credit_data_capacity &&
            merged_length > *last.boundary.receiver_credit_data_capacity) {
            return false;
        }

        uint64_t last_source_end = 0;
        uint64_t last_target_end = 0;
        if (!tryAddUint64(requestSourceAddr(last.req), last.req.length,
                          last_source_end) ||
            !tryAddUint64(last.req.target_offset, last.req.length,
                          last_target_end)) {
            return false;
        }
        return last_source_end == requestSourceAddr(curr.req) &&
               last_target_end == curr.req.target_offset;
    };

    std::vector<Item> merged_items;
    merged_items.reserve(items.size());

    MergeResult result;
    for (const auto& item : items) {
        if (merged_items.empty() || !can_merge(merged_items.back(), item)) {
            merged_items.push_back(item);
        } else {
            merged_items.back().req.length += item.req.length;
        }
        result.task_lookup[item.orig_idx] = merged_items.size() - 1;
    }

    result.request_list.reserve(merged_items.size());
    for (const auto& item : merged_items) {
        result.request_list.push_back(item.req);
    }
    return result;
}

std::optional<BufferKey> toBufferKey(BufferDesc* buffer) {
    if (!buffer) return std::nullopt;
    return BufferKey{buffer->addr, buffer->length};
}

std::vector<RequestBoundaryInfo> resolveRequestBoundaries(
    ControlService* metadata, const std::vector<Request>& requests) {
    // Group requests by target_id so withCachedSegment fires at most once per
    // peer.
    std::vector<RequestBoundaryInfo> boundaries(requests.size());
    // Owning reference: keeps the snapshot alive while we read through it.
    auto local_desc = metadata->segmentManager().getLocal();

    if (local_desc) {
        for (size_t i = 0; i < requests.size(); ++i) {
            auto source_addr = static_cast<uint64_t>(
                reinterpret_cast<uintptr_t>(requests[i].source));
            boundaries[i].source_key = toBufferKey(
                local_desc->findBuffer(source_addr, requests[i].length));
        }
    }

    std::unordered_map<SegmentID, std::vector<size_t>> by_target;
    for (size_t i = 0; i < requests.size(); ++i) {
        by_target[requests[i].target_id].push_back(i);
    }

    for (auto& [target_id, idxs] : by_target) {
        metadata->segmentManager().withCachedSegment(
            target_id, [&](SegmentDesc* target_desc) {
                std::optional<uint64_t> receiver_credit_data_capacity;
                if (target_desc->type == SegmentType::Memory &&
                    target_desc->getMemory().receiver_credit) {
                    // Zero deliberately disables merging for malformed
                    // adverts; prepareSubmit validates and rejects them with
                    // the protocol-specific error below.
                    receiver_credit_data_capacity = 0;
                    for (const auto& capacity :
                         target_desc->getMemory().receiver_credit->capacities) {
                        if (capacity.resource == CreditResource::DataBytes) {
                            receiver_credit_data_capacity = capacity.total;
                            break;
                        }
                    }
                }
                bool any_missing = false;
                for (size_t i : idxs) {
                    const auto& r = requests[i];
                    boundaries[i].receiver_credit_data_capacity =
                        receiver_credit_data_capacity;
                    auto* buffer =
                        target_desc->findBuffer(r.target_offset, r.length);
                    if (!buffer) {
                        any_missing = true;
                        boundaries[i].target_key = std::nullopt;
                    } else {
                        boundaries[i].target_key = toBufferKey(buffer);
                    }
                }
                // Invariant: when this lambda returns NeedsRefreshCache, all
                // writes it made in this pass are wiped before it returns.
                // Reason: withCachedSegment will invalidate the cache and try
                // ONE refetch; if that refetch fails (e.g. peer RPC down) the
                // retry pass never runs, and any tentative writes from this
                // (stale) pass would leak downstream into mergeRequests. By
                // clearing here we leave a clean nullopt state for the group,
                // and the retry pass (if it does run) repopulates from the
                // fresh desc so the wipe is harmless.
                if (any_missing) {
                    for (size_t i : idxs) {
                        boundaries[i].target_key = std::nullopt;
                    }
                    return Status::NeedsRefreshCache(
                        "Requested address is not in registered "
                        "buffer" LOC_MARK);
                }
                return Status::OK();
            });
    }
    return boundaries;
}

void TransferEngineImpl::findStagingPolicy(const Request& request,
                                           std::vector<std::string>& policy) {
    if (request.target_id == LOCAL_SEGMENT_ID) return;

    SegmentDesc* desc = nullptr;
    BufferDesc* entry = nullptr;
    // Owning reference: `entry` is used after the lambda returns.
    SegmentDescRef pin;
    auto status = metadata_->segmentManager().withCachedSegment(
        request.target_id, pin, [&](SegmentDesc* segment) {
            desc = segment;
            entry = desc->findBuffer(request.target_offset, request.length);
            if (!entry)
                return Status::NeedsRefreshCache(
                    "Requested address is not in registered buffer" LOC_MARK);
            return Status::OK();
        });

    if (!status.ok()) return;
    auto local =
        Platform::getLoader().getLocation(request.source, 1)[0].location;
    auto remote = entry->location;
    auto local_mtype = getTypeEnum(LocationParser(local).type());
    auto remote_mtype = getTypeEnum(LocationParser(remote).type());
    auto server_addr = desc->rpc_server_addr;
    policy.clear();
    // case 1: rdma without gpu direct
    if (transport_list_[RDMA] && transport_list_[NVLINK]) {
        auto& xport = transport_list_[RDMA];
        auto& caps = xport->capabilities();
        if (local_mtype == MTYPE_CUDA && remote_mtype == MTYPE_CUDA &&
            !caps.gpu_to_gpu) {
            policy.push_back(server_addr);
            policy.push_back(topology_->findNearMem(local));
            policy.push_back(desc->getMemory().topology.findNearMem(remote));
        } else if (local_mtype == MTYPE_CUDA && remote_mtype == MTYPE_CPU &&
                   !caps.gpu_to_dram) {
            policy.push_back(server_addr);
            policy.push_back(topology_->findNearMem(local));
            policy.push_back("");  // no remote stage
        } else if (local_mtype == MTYPE_CPU && remote_mtype == MTYPE_CUDA &&
                   !caps.dram_to_gpu) {
            policy.push_back(server_addr);
            policy.push_back("");  // no local stage
            policy.push_back(desc->getMemory().topology.findNearMem(remote));
        }
    }
    // case 2: pure mnnvl
    if (transport_list_[MNNVL] && transport_list_[NVLINK]) {
        auto& xport = transport_list_[MNNVL];
        auto& caps = xport->capabilities();
        if (local_mtype == MTYPE_CPU && remote_mtype == MTYPE_CPU &&
            !caps.dram_to_dram) {
            policy.push_back(server_addr);
            policy.push_back(topology_->findNearMem(local, Topology::MEM_CUDA));
            policy.push_back("");  // remote stage
        } else if (local_mtype == MTYPE_CUDA && remote_mtype == MTYPE_CPU &&
                   !caps.gpu_to_dram) {
            policy.push_back(server_addr);
            policy.push_back("");  // no local stage
            policy.push_back(desc->getMemory().topology.findNearMem(
                remote, Topology::MEM_CUDA));
        }
    }
    // case 3: TPU. HBM is not NIC-addressable, so any hop touching TPU memory
    // is staged through host DRAM: TpuTransport performs the local HBM<->host
    // copy (via the PJRT adapter) and the host<->host hop is carried by
    // whatever host-DRAM network transport is present. TPU deployments (e.g.
    // cloud TPU VMs) are typically TCP/multi-NIC rather than RDMA, so we gate
    // on either; the cross stage itself is routed by capability (dram_to_dram),
    // so TCP is selected when RDMA is absent. We also require TpuTransport (the
    // local HBM<->host executor), mirroring how the CUDA cases gate on NVLINK.
    // An empty stage location means "no staging needed on that side".
    if (transport_list_[TPU] && (transport_list_[RDMA] || transport_list_[UB] ||
                                 transport_list_[TCP])) {
        if (local_mtype == MTYPE_TPU && remote_mtype == MTYPE_TPU) {
            policy.clear();
            policy.push_back(server_addr);
            policy.push_back(topology_->findNearMem(local));
            policy.push_back(desc->getMemory().topology.findNearMem(remote));
        } else if (local_mtype == MTYPE_TPU && remote_mtype == MTYPE_CPU) {
            policy.clear();
            policy.push_back(server_addr);
            policy.push_back(topology_->findNearMem(local));
            policy.push_back("");  // remote already host DRAM
        } else if (local_mtype == MTYPE_CPU && remote_mtype == MTYPE_TPU) {
            policy.clear();
            policy.push_back(server_addr);
            policy.push_back("");  // local already host DRAM
            policy.push_back(desc->getMemory().topology.findNearMem(remote));
        }
    }
}

SelectionResult TransferEngineImpl::resolveTransport(const Request& req,
                                                     int transport_index,
                                                     bool invalidate_on_fail) {
    auto result = getTransportType(req, transport_index);
    if (result.transport == UNSPEC && invalidate_on_fail) {
        metadata_->segmentManager().invalidateRemote(req.target_id);
        result = getTransportType(req, transport_index);
    }
    return result;
}

Status TransferEngineImpl::prepareSubmit(
    Batch* batch, const std::vector<Request>& request_list,
    QueueOwnerKind owner_kind, PreparedSubmit& prepared) {
    if (!batch) return Status::InvalidArgument("Invalid batch" LOC_MARK);
    for (size_t i = 0; i < request_list.size(); ++i) {
        auto st = validateTransportHint(request_list[i], i);
        if (!st.ok()) return st;
    }

    prepared = PreparedSubmit{};
    const size_t start_task_id = batch->task_list.size();
    prepared.submit_time = std::chrono::steady_clock::now();
    auto merge_boundaries =
        merge_requests_
            ? resolveRequestBoundaries(metadata_.get(), request_list)
            : std::vector<RequestBoundaryInfo>{};
    auto merged =
        mergeRequests(request_list, merge_boundaries, merge_requests_);

    prepared.owners.reserve(merged.request_list.size());
    for (const auto& request : merged.request_list) {
        PreparedSubmit::Owner owner;
        owner.request = request;
        owner.route = resolveTransport(owner.request, 0);
        owner.receiver_credit_exempt =
            owner_kind == QueueOwnerKind::StagingInternal;
        if (owner_kind != QueueOwnerKind::StagingInternal) {
            ReceiverCreditTarget credit_target;
            CHECK_STATUS(
                resolveReceiverCreditTarget(owner.request, credit_target));
            owner.receiver_credit_required = credit_target.required;
            if (credit_target.required &&
                !receiver_credit_config_.sender_enabled) {
                return Status::InvalidArgument(
                    "peer requires receiver credit but the sender gate is "
                    "disabled" LOC_MARK);
            }
            CHECK_STATUS(
                validateReceiverCreditCapacity(owner.request, credit_target));
        } else {
            const bool has_credit_fence =
                owner.request.receiver_credit_session_high != 0 ||
                owner.request.receiver_credit_session_low != 0 ||
                owner.request.receiver_credit_epoch != 0;
            if (has_credit_fence) {
                ReceiverCreditTarget credit_target;
                CHECK_STATUS(
                    resolveReceiverCreditTarget(owner.request, credit_target));
                if (!credit_target.required ||
                    credit_target.advert.receiver_session_id.high !=
                        owner.request.receiver_credit_session_high ||
                    credit_target.advert.receiver_session_id.low !=
                        owner.request.receiver_credit_session_low ||
                    credit_target.advert.epoch !=
                        owner.request.receiver_credit_epoch) {
                    return Status::InvalidEntry(
                        "staging receiver-credit session changed" LOC_MARK);
                }
            }
        }
        if (owner.route.transport == TCP) {
            findStagingPolicy(owner.request, owner.staging_params);
            owner.staging = !owner.staging_params.empty() && staging_proxy_;
        }
        prepared.owners.push_back(std::move(owner));
    }

    prepared.tasks.reserve(merged.task_lookup.size());
    for (const auto& kv : merged.task_lookup) {
        const size_t public_task_index = kv.first;
        const size_t merged_task_index = kv.second;
        const size_t task_id = start_task_id + public_task_index;
        auto& owner = prepared.owners[merged_task_index];
        if (!owner.has_owner_task_id) {
            owner.owner_task_id = task_id;
            owner.has_owner_task_id = true;
        } else {
            owner.derived_task_ids.push_back(task_id);
        }
        prepared.tasks.push_back({merged_task_index, task_id});
    }
    return Status::OK();
}

uint64_t TransferEngineImpl::nextBatchToken() { return next_batch_token_++; }

void TransferEngineImpl::attachProgressNotifier(
    Batch* batch, Transport::SubBatchRef sub_batch) {
    if (!batch || !sub_batch) return;
    sub_batch->progress_batch_id = (BatchID)batch;
    sub_batch->notify_progress = [this](BatchID batch_id) {
        notifyBatchMaybeReady(batch_id);
    };
}

Status TransferEngineImpl::resolveReceiverCreditTarget(
    const Request& request, ReceiverCreditTarget& target) const {
    target = ReceiverCreditTarget{};
    SegmentDescRef segment;
    if (request.target_id == LOCAL_SEGMENT_ID) {
        segment = metadata_->segmentManager().getLocal();
    } else {
        CHECK_STATUS(metadata_->segmentManager().getRemoteCached(
            segment, request.target_id));
    }
    if (!segment || segment->type != SegmentType::Memory) return Status::OK();
    const auto& memory = segment->getMemory();
    if (!memory.receiver_credit) return Status::OK();

    const auto& advert = *memory.receiver_credit;
    if (advert.schema_version != kReceiverCreditProtocolVersion ||
        advert.flags != kReceiverCreditRequired ||
        advert.receiver_session_id.empty() || advert.epoch == 0 ||
        advert.freshness_ttl_ms == 0 ||
        advert.freshness_ttl_ms >
            receiver_credit_config_.max_freshness_ttl_ms ||
        advert.resources.empty() ||
        advert.resources.size() > kCreditResourceCount ||
        advert.capacities.size() != advert.resources.size()) {
        return Status::InvalidArgument(
            "invalid receiver-credit advertisement" LOC_MARK);
    }

    std::array<bool, kCreditResourceCount> resources{};
    for (const auto resource : advert.resources) {
        const auto raw = static_cast<uint16_t>(resource);
        if (raw < 1 || raw > kCreditResourceCount || resources[raw - 1]) {
            return Status::InvalidArgument(
                "invalid receiver-credit resource advertisement" LOC_MARK);
        }
        resources[raw - 1] = true;
    }
    std::array<bool, kCreditResourceCount> capacities{};
    for (const auto& capacity : advert.capacities) {
        const auto raw = static_cast<uint16_t>(capacity.resource);
        if (raw < 1 || raw > kCreditResourceCount || capacity.total == 0 ||
            capacities[raw - 1] || !resources[raw - 1]) {
            return Status::InvalidArgument(
                "invalid receiver-credit capacity advertisement" LOC_MARK);
        }
        capacities[raw - 1] = true;
    }
    if (capacities != resources) {
        return Status::InvalidArgument(
            "receiver-credit resources/capacities mismatch" LOC_MARK);
    }
    if (!resources[static_cast<size_t>(CreditResource::DataBytes) - 1] ||
        !resources[static_cast<size_t>(CreditResource::RequestSlots) - 1]) {
        return Status::InvalidArgument(
            "receiver credit lacks bytes/request resources" LOC_MARK);
    }
    if (request.target_id != LOCAL_SEGMENT_ID &&
        segment->rpc_server_addr.empty()) {
        return Status::InvalidArgument(
            "receiver credit peer has no RPC address" LOC_MARK);
    }
    target.required = true;
    target.advert = advert;
    target.rpc_server_addr = segment->rpc_server_addr;
    return Status::OK();
}

Status TransferEngineImpl::validateReceiverCreditCapacity(
    const Request& request, const ReceiverCreditTarget& target) const {
    if (!target.required) return Status::OK();
    uint64_t data_bytes = 0;
    uint64_t request_slots = 0;
    for (const auto& capacity : target.advert.capacities) {
        if (capacity.resource == CreditResource::DataBytes) {
            data_bytes = capacity.total;
        } else if (capacity.resource == CreditResource::RequestSlots) {
            request_slots = capacity.total;
        }
    }
    if (request_slots == 0 || request.length > data_bytes) {
        return Status::TooManyRequests(
            "request exceeds receiver-credit capacity" LOC_MARK);
    }
    return Status::OK();
}

Status TransferEngineImpl::exchangeReceiverCredit(
    SegmentID target_id, const std::string& rpc_server_addr,
    const ReceiverCreditExchangeRequestV1& request,
    ReceiverCreditExchangeReplyV1& reply) {
    if (target_id == LOCAL_SEGMENT_ID) {
        if (!receiver_credit_authority_) {
            return Status::InvalidEntry(
                "local receiver-credit authority is unavailable" LOC_MARK);
        }
        return receiver_credit_authority_->exchange(request, reply);
    }
    return ControlClient::exchangeReceiverCredit(rpc_server_addr, request,
                                                 reply);
}

Status TransferEngineImpl::acquireReceiverCredit(
    const Request& request, ReceiverCreditReservation& reservation,
    bool& acquired) {
    acquired = false;
    if (reservation.active) {
        return Status::InternalError(
            "receiver credit attempt already has a reservation" LOC_MARK);
    }

    ReceiverCreditTarget target;
    CHECK_STATUS(resolveReceiverCreditTarget(request, target));
    if (!target.required) {
        acquired = true;
        return Status::OK();
    }
    CHECK_STATUS(validateReceiverCreditCapacity(request, target));
    if (!receiver_credit_config_.sender_enabled || !sender_credit_ledger_) {
        return Status::InvalidEntry(
            "receiver-credit sender gate is disabled" LOC_MARK);
    }

    const uint32_t qos_class = static_cast<uint32_t>(
        std::clamp(request.priority, static_cast<int>(PRIO_HIGH),
                   static_cast<int>(PRIO_LOW)));
    CreditKey key{target.advert.receiver_session_id, sender_instance_id_,
                  qos_class};
    CreditCharge charge{{{CreditResource::DataBytes, request.length},
                         {CreditResource::RequestSlots, 1}}};
    observeReceiverCreditSession(request.target_id,
                                 target.advert.receiver_session_id);
    CHECK_STATUS(sender_credit_ledger_->activate(key, target.advert.epoch));

    auto reserve_status = sender_credit_ledger_->tryReserve(key, charge);
    if (!reserve_status.ok()) {
        if (!reserve_status.IsTooManyRequests() &&
            !reserve_status.IsInvalidEntry()) {
            return reserve_status;
        }

        ReceiverCreditExchangeRequestV1 exchange_request;
        CHECK_STATUS(sender_credit_ledger_->prepareExchange(key, charge,
                                                            exchange_request));
        ReceiverCreditExchangeReplyV1 exchange_reply;
        auto exchange_status =
            exchangeReceiverCredit(request.target_id, target.rpc_server_addr,
                                   exchange_request, exchange_reply);
        if (!exchange_status.ok()) {
            if (request.target_id != LOCAL_SEGMENT_ID) {
                (void)metadata_->segmentManager().invalidateRemote(
                    request.target_id);
            }
            LOG(WARNING) << "Receiver-credit exchange deferred: "
                         << exchange_status.ToString();
            return Status::OK();
        }
        if (!(exchange_reply.sender_instance_id == sender_instance_id_)) {
            return Status::InvalidArgument(
                "receiver-credit reply sender mismatch" LOC_MARK);
        }
        if (exchange_reply.flags != 0) {
            return Status::InvalidArgument(
                "receiver-credit demand reply flags" LOC_MARK);
        }
        CreditUpdateDisposition disposition;
        CHECK_STATUS(sender_credit_ledger_->applyUpdate(
            key, exchange_reply.update, disposition));
        pending_credit_releases_.erase(key);
        reserve_status = sender_credit_ledger_->tryReserve(key, charge);
        if (reserve_status.IsTooManyRequests() ||
            reserve_status.IsInvalidEntry()) {
            return Status::OK();
        }
        CHECK_STATUS(reserve_status);
    }

    reservation.active = true;
    reservation.committed = false;
    reservation.key = key;
    reservation.charge = std::move(charge);
    reservation.target_id = request.target_id;
    reservation.rpc_server_addr = std::move(target.rpc_server_addr);
    reservation.epoch = target.advert.epoch;
    acquired = true;
    return Status::OK();
}

void TransferEngineImpl::rollbackReceiverCredit(
    ReceiverCreditReservation& reservation) {
    if (!reservation.active) return;
    if (!reservation.committed) {
        const auto key = reservation.key;
        const auto target_id = reservation.target_id;
        const auto rpc_server_addr = reservation.rpc_server_addr;
        const auto epoch = reservation.epoch;
        auto status = sender_credit_ledger_->rollbackReservation(
            reservation.key, reservation.charge);
        reservation = ReceiverCreditReservation{};
        if (!status.ok()) {
            LOG(ERROR) << "Receiver-credit rollback failed: "
                       << status.ToString();
            return;
        }
        scheduleReceiverCreditRelease(key, target_id, rpc_server_addr, epoch);
        (void)tryReportReceiverCreditRelease(key);
        return;
    }
    reservation = ReceiverCreditReservation{};
}

void TransferEngineImpl::releaseReceiverCredit(
    ReceiverCreditReservation& reservation) {
    if (!reservation.active) return;
    if (!reservation.committed) {
        rollbackReceiverCredit(reservation);
        return;
    }

    const auto key = reservation.key;
    const auto target_id = reservation.target_id;
    const auto rpc_server_addr = reservation.rpc_server_addr;
    const auto epoch = reservation.epoch;
    auto status = sender_credit_ledger_->releaseCommitted(reservation.key,
                                                          reservation.charge);
    reservation = ReceiverCreditReservation{};
    if (!status.ok()) {
        LOG(ERROR) << "Receiver-credit terminal release failed: "
                   << status.ToString();
        return;
    }

    scheduleReceiverCreditRelease(key, target_id, rpc_server_addr, epoch);
    (void)tryReportReceiverCreditRelease(key);
}

void TransferEngineImpl::scheduleReceiverCreditRelease(
    const CreditKey& key, SegmentID target_id,
    const std::string& rpc_server_addr, uint64_t epoch) {
    auto& pending = pending_credit_releases_[key];
    pending.target_id = target_id;
    pending.rpc_server_addr = rpc_server_addr;
    pending.epoch = epoch;
    pending.retry_at = std::chrono::steady_clock::now();
}

bool TransferEngineImpl::tryReportReceiverCreditRelease(const CreditKey& key) {
    auto pending_it = pending_credit_releases_.find(key);
    if (pending_it == pending_credit_releases_.end()) return true;
    auto& pending = pending_it->second;
    if (std::chrono::steady_clock::now() < pending.retry_at) return false;

    auto defer_retry = [&](const Status& status) {
        ++pending.attempts;
        const uint32_t shift = std::min<uint32_t>(pending.attempts - 1, 4);
        const auto delay = std::chrono::milliseconds(50U << shift);
        pending.retry_at = std::chrono::steady_clock::now() + delay;
        LOG(WARNING) << "Receiver-credit release exchange deferred (attempt "
                     << pending.attempts << "): " << status.ToString();
        return false;
    };

    ReceiverCreditExchangeRequestV1 request;
    auto status = sender_credit_ledger_->prepareReleaseExchange(key, request);
    if (!status.ok()) {
        return defer_retry(status);
    }
    ReceiverCreditExchangeReplyV1 reply;
    status = exchangeReceiverCredit(pending.target_id, pending.rpc_server_addr,
                                    request, reply);
    if (!status.ok()) {
        return defer_retry(status);
    }
    if (!(reply.sender_instance_id == sender_instance_id_)) {
        return defer_retry(Status::InvalidArgument(
            "receiver-credit release reply sender mismatch" LOC_MARK));
    }
    if (reply.flags != 0) {
        return defer_retry(Status::InvalidArgument(
            "receiver-credit release reply flags" LOC_MARK));
    }
    CreditUpdateDisposition disposition;
    status = sender_credit_ledger_->applyUpdate(key, reply.update, disposition);
    if (!status.ok()) {
        return defer_retry(status);
    }
    pending_credit_releases_.erase(pending_it);
    notifyRuntimeQueueReady();
    return true;
}

void TransferEngineImpl::progressReceiverCreditReleases() {
    const auto now = std::chrono::steady_clock::now();
    std::vector<CreditKey> due;
    due.reserve(pending_credit_releases_.size());
    for (const auto& [key, pending] : pending_credit_releases_) {
        if (pending.retry_at <= now) due.push_back(key);
    }
    for (const auto& key : due) (void)tryReportReceiverCreditRelease(key);
}

void TransferEngineImpl::observeReceiverCreditSession(
    SegmentID target_id, const ReceiverSessionId& session) {
    auto [it, inserted] =
        receiver_credit_sessions_.try_emplace(target_id, session);
    if (inserted || it->second == session) return;

    const auto stale_session = it->second;
    auto status = sender_credit_ledger_->deactivateSession(stale_session);
    if (!status.ok()) {
        LOG(ERROR) << "Receiver-credit stale session cleanup failed: "
                   << status.ToString();
    }
    for (auto pending = pending_credit_releases_.begin();
         pending != pending_credit_releases_.end();) {
        if (pending->first.receiver_session == stale_session) {
            pending = pending_credit_releases_.erase(pending);
        } else {
            ++pending;
        }
    }
    it->second = session;
}

void TransferEngineImpl::bindReceiverCreditFence(
    Transport::SubBatchRef sub_batch,
    const ReceiverCreditReservation& reservation) const {
    if (!sub_batch) return;
    sub_batch->receiver_credit_session_high = 0;
    sub_batch->receiver_credit_session_low = 0;
    sub_batch->receiver_credit_epoch = 0;
    if (!reservation.active) return;
    sub_batch->receiver_credit_session_high =
        reservation.key.receiver_session.high;
    sub_batch->receiver_credit_session_low =
        reservation.key.receiver_session.low;
    sub_batch->receiver_credit_epoch = reservation.epoch;
}

void TransferEngineImpl::bindReceiverCreditFence(
    Transport::SubBatchRef sub_batch, const Request& request) const {
    if (!sub_batch) return;
    sub_batch->receiver_credit_session_high =
        request.receiver_credit_session_high;
    sub_batch->receiver_credit_session_low =
        request.receiver_credit_session_low;
    sub_batch->receiver_credit_epoch = request.receiver_credit_epoch;
}

Status TransferEngineImpl::commitPreparedSubmit(
    Batch* batch, const PreparedSubmit& prepared) {
    if (!batch) return Status::InvalidArgument("Invalid batch" LOC_MARK);

    std::vector<Request> classified_request_list[kSupportedTransportTypes];
    std::vector<size_t> task_id_list[kSupportedTransportTypes];
    std::unordered_map<size_t, TaskInfo> merged_task_id_map;

    batch->task_list.insert(batch->task_list.end(), prepared.tasks.size(),
                            TaskInfo{});

    std::unordered_map<TransportType, size_t> next_sub_task_id;
    for (const auto& task_plan : prepared.tasks) {
        size_t task_id = task_plan.task_id;
        size_t merged_task_id = task_plan.merged_task_index;
        auto& task = batch->task_list[task_id];
        const auto& owner = prepared.owners[merged_task_id];
        auto& merged_request = owner.request;
        if (merged_task_id_map.count(merged_task_id)) {
            task = merged_task_id_map[merged_task_id];
            task.derived = true;
            if (task.type != UNSPEC) task_id_list[task.type].push_back(task_id);
            continue;
        }

        task.failover_count = 0;
        task.xport_priority = 0;
        task.status = PENDING;
        task.request = merged_request;
        task.staging = false;
        task.start_time =
            prepared.submit_time;  // Record start time for latency tracking
        task.dispatch_time = prepared.submit_time;  // No queue wait on direct
        task.type = owner.route.transport;
        task.device_mask = owner.route.device_mask;
        if (owner.route.qp_pool) task.qp_pool = *owner.route.qp_pool;
        if (task.type == UNSPEC) {
            LOG(WARNING) << "Unable to find registered buffer for request: "
                         << printRequest(merged_request);
            merged_task_id_map[merged_task_id] = task;
            continue;
        }

        if (owner.staging) {
            task.staging = true;
            // Staging is an orchestration step, not a concrete transport
            // attempt. ProxyManager chunks the transfer and issues the real
            // Transport::submitTransferTasks() calls, which are counted where
            // they recurse through the non-staging path below. Only stamp the
            // logical request's first post time for stage decomposition here.
            auto status = staging_proxy_->submit(&task, (BatchID)batch,
                                                 owner.staging_params);
            if (!status.ok()) {
                task.staging = false;
                task.type = UNSPEC;
            } else {
                task.post_time = std::chrono::steady_clock::now();
            }
            continue;
        }

        if (!batch->sub_batch[task.type]) {
            auto& transport = transport_list_[task.type];
            auto status = transport->allocateSubBatch(
                batch->sub_batch[task.type], batch->max_size);
            if (!status.ok()) {
                LOG(WARNING) << "Failed to allocate SubBatch " << task.type
                             << ":" << status.ToString();
                merged_task_id_map[merged_task_id] = task;
                continue;
            }
            attachProgressNotifier(batch, batch->sub_batch[task.type]);
        }

        if (!next_sub_task_id.count(task.type))
            next_sub_task_id[task.type] = batch->sub_batch[task.type]->size();
        size_t sub_task_id = next_sub_task_id[task.type];
        next_sub_task_id[task.type]++;

        classified_request_list[task.type].push_back(merged_request);
        task.sub_task_id = sub_task_id;
        task.derived = false;
        task_id_list[task.type].push_back(task_id);
        merged_task_id_map[merged_task_id] = task;
    }

    for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
        if (classified_request_list[type].empty()) continue;
        auto& transport = transport_list_[type];
        auto& sub_batch = batch->sub_batch[type];

        // RDMA and UB both spray slices across topology-selected NICs.
        if ((type == RDMA || type == UB) && !task_id_list[type].empty()) {
            // Use the device_mask from the first task (we assume all tasks in
            // this batch should have the same policy)
            sub_batch->device_mask =
                batch->task_list[task_id_list[type][0]].device_mask;
            if (type == RDMA) {
                sub_batch->qp_pool =
                    batch->task_list[task_id_list[type][0]].qp_pool;
            }
        }

        auto attempt_start = std::chrono::steady_clock::now();
        for (auto& task_id : task_id_list[type]) {
            startTransportAttempt(batch->task_list[task_id],
                                  static_cast<TransportType>(type),
                                  attempt_start);
        }
        auto status = transport->submitTransferTasks(
            sub_batch, classified_request_list[type]);
        if (!status.ok()) {
            auto attempt_end = std::chrono::steady_clock::now();
            for (auto& task_id : task_id_list[type]) {
                finishTransportAttempt(batch->task_list[task_id], FAILED,
                                       attempt_end);
                batch->task_list[task_id].type = UNSPEC;
            }
        }
    }

    return Status::OK();
}

Status TransferEngineImpl::enqueuePreparedSubmit(Batch* batch,
                                                 const PreparedSubmit& prepared,
                                                 QueueOwnerKind owner_kind) {
    std::lock_guard<std::recursive_mutex> lk(progress_mutex_);
    if (prepared.tasks.empty()) return Status::OK();
    if (prepared.tasks.size() > batch->max_size - batch->task_list.size()) {
        return Status::TooManyRequests(
            "batch public task capacity exceeded" LOC_MARK);
    }

    const uint64_t batch_token =
        batch->queue_token != 0 ? batch->queue_token : nextBatchToken();
    QueueSubmit submit;
    submit.batch_token = batch_token;
    submit.batch_slots_left = batch->max_size - batch->task_list.size();
    submit.owners.reserve(prepared.owners.size());
    for (const auto& owner : prepared.owners) {
        if (owner.request.length > runtime_queue_config_.max_dispatch_bytes) {
            return Status::TooManyRequests(
                "request exceeds runtime queue dispatch byte window" LOC_MARK);
        }
        QueueOwnerInput input;
        input.owner_task_id = owner.owner_task_id;
        input.derived_task_ids = owner.derived_task_ids;
        input.request = owner.request;
        input.kind = owner_kind;
        input.transport = owner.route.transport;
        input.degradation_eligible =
            (owner.route.transport == RDMA || owner.route.transport == UB) &&
            !owner.staging;
        submit.owners.push_back(std::move(input));
    }

    std::vector<QueueOwnerId> admitted_owner_ids;
    CHECK_STATUS(runtime_queue_->tryAdmit(submit, admitted_owner_ids));
    batch->queue_token = batch_token;

    batch->task_list.insert(batch->task_list.end(), prepared.tasks.size(),
                            TaskInfo{});
    for (const auto& task_plan : prepared.tasks) {
        auto& task = batch->task_list[task_plan.task_id];
        const auto& owner = prepared.owners[task_plan.merged_task_index];
        task.failover_count = 0;
        task.xport_priority = 0;
        task.status = PENDING;
        task.request = owner.request;
        task.staging = false;
        task.start_time = prepared.submit_time;
        task.type = UNSPEC;
        task.sub_task_id = -1;
        task.device_mask = owner.route.device_mask;
        if (owner.route.qp_pool) task.qp_pool = *owner.route.qp_pool;
        task.derived = task_plan.task_id != owner.owner_task_id;
    }

    for (size_t i = 0; i < admitted_owner_ids.size(); ++i) {
        QueuedOwnerState queued;
        queued.batch = batch;
        queued.owner_task_id = prepared.owners[i].owner_task_id;
        queued.byte_charge = prepared.owners[i].request.length;
        queued.receiver_credit_exempt =
            prepared.owners[i].receiver_credit_exempt;
        queued.public_task_ids.push_back(prepared.owners[i].owner_task_id);
        queued.public_task_ids.insert(
            queued.public_task_ids.end(),
            prepared.owners[i].derived_task_ids.begin(),
            prepared.owners[i].derived_task_ids.end());
        queued_owners_.emplace(admitted_owner_ids[i], queued);
    }
    return Status::OK();
}

Status TransferEngineImpl::finishQueuedOwner(
    QueueOwnerId owner_id, TransferStatusEnum terminal_status) {
    auto queued_it = queued_owners_.find(owner_id);
    if (queued_it == queued_owners_.end()) {
        return Status::InvalidEntry("queued owner not found" LOC_MARK);
    }
    auto& queued = queued_it->second;
    if (queued.in_dispatch_window) {
        if (dispatch_inflight_owners_ == 0 ||
            dispatch_inflight_bytes_ < queued.byte_charge) {
            return Status::InternalError(
                "runtime dispatch window accounting underflow" LOC_MARK);
        }
    }
    releaseReceiverCredit(queued.credit);
    CHECK_STATUS(runtime_queue_->complete(owner_id, terminal_status));
    if (queued.in_dispatch_window) {
        --dispatch_inflight_owners_;
        dispatch_inflight_bytes_ -= queued.byte_charge;
        queued.in_dispatch_window = false;
    }
    for (const auto task_id : queued.public_task_ids) {
        queued.batch->task_list[task_id].status = terminal_status;
    }
    queued_owners_.erase(queued_it);
    return Status::OK();
}

Status TransferEngineImpl::cancelQueuedOwner(QueueOwnerId owner_id) {
    auto queued_it = queued_owners_.find(owner_id);
    if (queued_it == queued_owners_.end()) {
        return Status::InvalidEntry("queued owner not found" LOC_MARK);
    }
    if (queued_it->second.in_dispatch_window) {
        return Status::InvalidEntry(
            "queued owner is already dispatching" LOC_MARK);
    }
    CHECK_STATUS(runtime_queue_->cancel(owner_id));
    for (const auto task_id : queued_it->second.public_task_ids) {
        auto& task = queued_it->second.batch->task_list[task_id];
        task.cancel_requested = true;
        task.status = CANCELED;
    }
    queued_owners_.erase(queued_it);
    return Status::OK();
}

Status TransferEngineImpl::retireQueueForBatch(Batch* batch) {
    if (!batch || batch->queue_token == 0) return Status::OK();
    auto status = runtime_queue_->retireBatch(batch->queue_token);
    if (!status.ok()) return status;
    batch->queue_token = 0;
    return Status::OK();
}

Status TransferEngineImpl::markQueuedOwnerSubmitted(QueueOwnerId owner_id) {
    auto queued_it = queued_owners_.find(owner_id);
    if (queued_it == queued_owners_.end()) {
        return Status::InternalError("queued owner metadata missing" LOC_MARK);
    }
    auto& queued = queued_it->second;
    if (!queued.in_dispatch_window) {
        queued.in_dispatch_window = true;
        ++dispatch_inflight_owners_;
        dispatch_inflight_bytes_ += queued.byte_charge;
    }
    return Status::OK();
}

Status TransferEngineImpl::dispatchQueuedOwner(QueueOwnerId owner_id) {
    auto queued_it = queued_owners_.find(owner_id);
    if (queued_it == queued_owners_.end()) {
        return Status::InternalError("queued owner metadata missing" LOC_MARK);
    }
    auto& queued = queued_it->second;
    auto* batch = queued.batch;
    auto& task = batch->task_list[queued.owner_task_id];
    task.dispatch_time = std::chrono::steady_clock::now();
    auto route = resolveTransport(task.request, 0);
    task.type = route.transport;
    task.device_mask = route.device_mask;
    if (route.qp_pool) task.qp_pool = *route.qp_pool;
    if (task.type == UNSPEC) {
        return finishQueuedOwner(owner_id, FAILED);
    }

    if (task.type == TCP) {
        std::vector<std::string> staging_params;
        findStagingPolicy(task.request, staging_params);
        if (!staging_params.empty() && staging_proxy_) {
            bool acquired = true;
            auto credit_status = Status::OK();
            if (!queued.receiver_credit_exempt) {
                acquired = false;
                credit_status = acquireReceiverCredit(task.request,
                                                      queued.credit, acquired);
            }
            if (!credit_status.ok()) {
                LOG(WARNING) << "Receiver-credit gate rejected staging "
                                "dispatch: "
                             << credit_status.ToString();
                return finishQueuedOwner(owner_id, FAILED);
            }
            if (!acquired) {
                task.type = UNSPEC;
                task.sub_task_id = -1;
                CHECK_STATUS(runtime_queue_->defer(owner_id));
                return Status::OK();
            }
            if (queued.credit.active) {
                task.request.receiver_credit_session_high =
                    queued.credit.key.receiver_session.high;
                task.request.receiver_credit_session_low =
                    queued.credit.key.receiver_session.low;
                task.request.receiver_credit_epoch = queued.credit.epoch;
            }
            task.staging = true;
            // Orchestration only; the real transport submissions issued by
            // ProxyManager are counted where they recurse through the
            // non-staging path below.
            auto status =
                staging_proxy_->submit(&task, (BatchID)batch, staging_params);
            if (!status.ok()) {
                rollbackReceiverCredit(queued.credit);
                return finishQueuedOwner(owner_id, FAILED);
            }
            if (queued.credit.active) queued.credit.committed = true;
            task.post_time = std::chrono::steady_clock::now();
            return markQueuedOwnerSubmitted(owner_id);
        }
    }

    if (!batch->sub_batch[task.type]) {
        auto& transport = transport_list_[task.type];
        if (!transport) return finishQueuedOwner(owner_id, FAILED);
        auto status = transport->allocateSubBatch(batch->sub_batch[task.type],
                                                  batch->max_size);
        if (!status.ok()) return finishQueuedOwner(owner_id, FAILED);
        attachProgressNotifier(batch, batch->sub_batch[task.type]);
    }

    auto& transport = transport_list_[task.type];
    if (!transport) return finishQueuedOwner(owner_id, FAILED);
    auto& sub_batch = batch->sub_batch[task.type];
    if (task.type == RDMA || task.type == UB) {
        sub_batch->device_mask = task.device_mask;
        if (task.type == RDMA) sub_batch->qp_pool = task.qp_pool;
    }

    bool acquired = true;
    auto credit_status = Status::OK();
    if (!queued.receiver_credit_exempt) {
        acquired = false;
        credit_status =
            acquireReceiverCredit(task.request, queued.credit, acquired);
    }
    if (!credit_status.ok()) {
        LOG(WARNING) << "Receiver-credit gate rejected dispatch: "
                     << credit_status.ToString();
        return finishQueuedOwner(owner_id, FAILED);
    }
    if (!acquired) {
        task.type = UNSPEC;
        task.sub_task_id = -1;
        CHECK_STATUS(runtime_queue_->defer(owner_id));
        return Status::OK();
    }
    if (queued.receiver_credit_exempt) {
        bindReceiverCreditFence(sub_batch, task.request);
    } else {
        bindReceiverCreditFence(sub_batch, queued.credit);
    }
    task.sub_task_id = sub_batch->size();
    startTransportAttempt(task, task.type, std::chrono::steady_clock::now());
    auto status = transport->submitTransferTasks(sub_batch, {task.request});
    if (!status.ok()) {
        finishTransportAttempt(task, FAILED, std::chrono::steady_clock::now());
        rollbackReceiverCredit(queued.credit);
        task.type = UNSPEC;
        return finishQueuedOwner(owner_id, FAILED);
    }
    if (queued.credit.active) queued.credit.committed = true;
    task.post_time = std::chrono::steady_clock::now();
    return markQueuedOwnerSubmitted(owner_id);
}

Status TransferEngineImpl::refillDispatchWindow() {
    std::lock_guard<std::recursive_mutex> lk(progress_mutex_);
    if (!runtime_queue_config_.enabled) return Status::OK();
    if (dispatch_inflight_owners_ >=
            runtime_queue_config_.max_dispatch_owners ||
        dispatch_inflight_bytes_ >= runtime_queue_config_.max_dispatch_bytes) {
        return Status::OK();
    }

    const size_t owner_budget =
        runtime_queue_config_.max_dispatch_owners - dispatch_inflight_owners_;
    const size_t byte_budget =
        runtime_queue_config_.max_dispatch_bytes - dispatch_inflight_bytes_;
    std::vector<QueueOwnerId> dropped;
    auto picked =
        runtime_queue_->pickForDispatch(owner_budget, byte_budget, &dropped);
    for (const auto owner_id : dropped) {
        auto queued_it = queued_owners_.find(owner_id);
        if (queued_it == queued_owners_.end()) continue;
        for (const auto task_id : queued_it->second.public_task_ids) {
            queued_it->second.batch->task_list[task_id].status = CANCELED;
        }
        queued_owners_.erase(queued_it);
    }
    for (const auto owner_id : picked) {
        CHECK_STATUS(dispatchQueuedOwner(owner_id));
    }
    return Status::OK();
}

Status TransferEngineImpl::progressRuntimeQueue() {
    std::lock_guard<std::recursive_mutex> lk(progress_mutex_);
    if (!runtime_queue_config_.enabled) return Status::OK();

    progressReceiverCreditReleases();
    CHECK_STATUS(refillDispatchWindow());

    std::vector<QueueOwnerId> owner_ids;
    owner_ids.reserve(queued_owners_.size());
    for (const auto& entry : queued_owners_) {
        if (entry.second.in_dispatch_window) owner_ids.push_back(entry.first);
    }

    bool released_window = false;
    for (const auto owner_id : owner_ids) {
        auto queued_it = queued_owners_.find(owner_id);
        if (queued_it == queued_owners_.end()) continue;

        auto& queued = queued_it->second;
        if (!queued.in_dispatch_window) continue;
        auto* batch = queued.batch;
        if (!batch || !alive_batches_.count((BatchID)batch)) continue;
        if (queued.owner_task_id >= batch->task_list.size()) {
            return Status::InternalError(
                "queued owner task id out of range" LOC_MARK);
        }

        auto& task = batch->task_list[queued.owner_task_id];
        auto prev_status = task.status;
        TransferStatus task_status;
        CHECK_STATUS(pollTaskStatus(batch, queued.owner_task_id, task_status));
        updateTaskStatusAfterPoll(batch, queued.owner_task_id, task_status,
                                  true);
        recordTaskCompletionMetrics(task, prev_status, task_status.s);

        if (task_status.s == PENDING) continue;

        CHECK_STATUS(finishQueuedOwner(owner_id, task_status.s));
        if (task_status.s == COMPLETED)
            CHECK_STATUS(maybeFireSubmitHooks(batch));
        released_window = true;
    }

    if (released_window) CHECK_STATUS(refillDispatchWindow());
    return Status::OK();
}

bool TransferEngineImpl::hasActiveRuntimeQueue() {
    std::lock_guard<std::recursive_mutex> lk(progress_mutex_);
    return runtime_queue_config_.enabled &&
           (!queued_owners_.empty() || !pending_credit_releases_.empty());
}

bool TransferEngineImpl::shouldQueueSubmit(const PreparedSubmit& prepared,
                                           QueueOwnerKind owner_kind) const {
    if (!runtime_queue_config_.enabled) return false;
    if (owner_kind == QueueOwnerKind::StagingInternal) return true;
    if (std::any_of(prepared.owners.begin(), prepared.owners.end(),
                    [](const PreparedSubmit::Owner& owner) {
                        return owner.receiver_credit_required;
                    })) {
        return true;
    }
    return std::none_of(
        prepared.owners.begin(), prepared.owners.end(),
        [](const PreparedSubmit::Owner& owner) { return owner.staging; });
}

Status TransferEngineImpl::submitTransfer(
    BatchID batch_id, const std::vector<Request>& request_list,
    const Notification* notifi, QueueOwnerKind owner_kind) {
    Batch* batch = nullptr;
    CHECK_STATUS(retainBatch(batch_id, batch));
    BatchRef batch_ref(*this, batch);
    const size_t start_task_id = batch_ref.get()->task_list.size();
    PreparedSubmit prepared;
    CHECK_STATUS(
        prepareSubmit(batch_ref.get(), request_list, owner_kind, prepared));

    if (shouldQueueSubmit(prepared, owner_kind)) {
        CHECK_STATUS(
            enqueuePreparedSubmit(batch_ref.get(), prepared, owner_kind));
        auto dispatch_status = refillDispatchWindow();
        if (!dispatch_status.ok()) {
            LOG(WARNING) << "runtime queue dispatch failed after admission: "
                         << dispatch_status.ToString();
        }
        notifyRuntimeQueueReady();
    } else {
        CHECK_STATUS(commitPreparedSubmit(batch_ref.get(), prepared));
    }

    if (notifi) {
        addSubmitHook(batch_ref.get(), start_task_id, request_list, *notifi);
    }
    return batch_ref.release();
}

Status TransferEngineImpl::submitTransfer(
    BatchID batch_id, const std::vector<Request>& request_list) {
    return submitTransfer(batch_id, request_list, nullptr,
                          QueueOwnerKind::User);
}

Status TransferEngineImpl::submitStagingTransfer(
    BatchID batch_id, const std::vector<Request>& request_list) {
    return submitTransfer(batch_id, request_list, nullptr,
                          QueueOwnerKind::StagingInternal);
}

void TransferEngineImpl::addSubmitHook(Batch* batch, size_t start_task_id,
                                       const std::vector<Request>& request_list,
                                       const Notification& notifi) {
    Batch::SubmitHook hook;
    hook.start_task_id = start_task_id;
    hook.end_task_id = start_task_id + request_list.size();
    hook.notifi = notifi;
    hook.fired = false;
    for (const auto& request : request_list)
        hook.targets.insert(request.target_id);
    batch->submit_hooks.emplace_back(std::move(hook));
}

Status TransferEngineImpl::maybeFireSubmitHooks(Batch* batch, bool check) {
    for (auto& hook : batch->submit_hooks) {
        if (hook.fired) continue;
        bool all_completed = true;
        if (check) {
            for (size_t tid = hook.start_task_id; tid < hook.end_task_id;
                 ++tid) {
                auto& t = batch->task_list[tid];
                if (t.status == PENDING) {
                    all_completed = false;
                    break;
                }
                if (t.status != COMPLETED) {
                    all_completed = false;
                    break;
                }
            }
        }
        if (!all_completed) continue;
        Status last = Status::OK();
        for (auto target_id : hook.targets) {
            last = sendNotification(target_id, hook.notifi);
            if (!last.ok()) {
                LOG(WARNING) << "sendNotification failed: " << last.ToString();
                break;
            }
        }
        if (last.ok()) hook.fired = true;
    }
    return Status::OK();
}

Status TransferEngineImpl::submitTransfer(
    BatchID batch_id, const std::vector<Request>& request_list,
    const Notification& notifi) {
    return submitTransfer(batch_id, request_list, &notifi,
                          QueueOwnerKind::User);
}

Status TransferEngineImpl::cancelTransfer(BatchID batch_id, size_t task_id) {
    if (!batch_id) return Status::InvalidArgument("Invalid batch ID" LOC_MARK);
    std::lock_guard<std::recursive_mutex> lk(progress_mutex_);
    if (!alive_batches_.count(batch_id)) {
        return Status::InvalidArgument("Batch is not alive" LOC_MARK);
    }
    auto* batch = reinterpret_cast<Batch*>(batch_id);
    if (task_id >= batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task ID" LOC_MARK);
    }

    size_t owner_task_id = task_id;
    if (runtime_queue_config_.enabled && batch->queue_token != 0) {
        QueueOwnerId owner_id = 0;
        auto resolve_status =
            runtime_queue_->resolveOwner(batch->queue_token, task_id, owner_id);
        if (resolve_status.ok()) {
            auto queued_it = queued_owners_.find(owner_id);
            if (queued_it == queued_owners_.end()) {
                TransferStatusEnum public_status = PENDING;
                CHECK_STATUS(runtime_queue_->getPublicStatus(
                    batch->queue_token, task_id, public_status));
                return public_status != PENDING
                           ? Status::OK()
                           : Status::InvalidEntry(
                                 "queued owner metadata missing" LOC_MARK);
            }
            owner_task_id = queued_it->second.owner_task_id;
            if (!queued_it->second.in_dispatch_window) {
                CHECK_STATUS(cancelQueuedOwner(owner_id));
                CHECK_STATUS(refillDispatchWindow());
                notifyRuntimeQueueReady();
                return Status::OK();
            }
        }
    }

    auto& owner = batch->task_list[owner_task_id];
    if (owner.status != PENDING) return Status::OK();
    if (owner.staging) {
        return Status::NotImplemented(
            "staging transfer cancellation is not implemented" LOC_MARK);
    }
    if (owner.type == UNSPEC) {
        owner.cancel_requested = true;
        owner.status = CANCELED;
        return Status::OK();
    }
    auto& transport = transport_list_[owner.type];
    auto& sub_batch = batch->sub_batch[owner.type];
    if (!transport || !sub_batch) {
        return Status::InvalidArgument("Transport not available" LOC_MARK);
    }
    if (!transport->supportsCancellation()) {
        return Status::NotImplemented(
            "selected transport does not support cancellation" LOC_MARK);
    }

    CHECK_STATUS(transport->cancelTransferTask(sub_batch, owner.sub_task_id));
    // Merged public tasks share one physical transport task. Mark every alias
    // so polling any of them cannot trigger failover after cancellation.
    for (auto& task : batch->task_list) {
        if (task.type == owner.type && task.sub_task_id == owner.sub_task_id) {
            task.cancel_requested = true;
        }
    }
    return Status::OK();
}

Status TransferEngineImpl::resubmitTransferTask(Batch* batch, size_t task_id) {
    auto& task = batch->task_list[task_id];
    auto prev_type = task.type;

    QueueOwnerId queue_owner_id = 0;
    QueuedOwnerState* queued_owner = nullptr;
    if (runtime_queue_config_.enabled && batch->queue_token != 0 &&
        runtime_queue_
            ->resolveOwner(batch->queue_token, task_id, queue_owner_id)
            .ok()) {
        auto queued_it = queued_owners_.find(queue_owner_id);
        if (queued_it != queued_owners_.end()) {
            queued_owner = &queued_it->second;
        }
    }

    if (++task.failover_count > max_failover_attempts_) {
        LOG(WARNING) << "Task failover limit reached ("
                     << max_failover_attempts_
                     << "), last transport=" << transportTypeName(prev_type);
        return Status::InvalidEntry(
            "Failover limit exceeded, all transports exhausted");
    }

    if (task.staging)
        task.staging = false;
    else
        task.xport_priority = task.failover_count;

    auto result = resolveTransport(task.request, task.xport_priority);
    auto type = result.transport;
    if (type == UNSPEC) {
        LOG(WARNING) << "No more transports available after "
                     << transportTypeName(prev_type) << " failed";
        return Status::InvalidEntry("All available transports are failed");
    }

    LOG(INFO) << "Transport failover: " << transportTypeName(prev_type)
              << " -> " << transportTypeName(type) << " (attempt "
              << task.failover_count << "/" << max_failover_attempts_ << ")";
    TENT_RECORD_TRANSPORT_FAILOVER(prev_type, type);

    auto& transport = transport_list_[type];
    if (!batch->sub_batch[type]) {
        CHECK_STATUS(transport->allocateSubBatch(batch->sub_batch[type],
                                                 batch->max_size));
        attachProgressNotifier(batch, batch->sub_batch[type]);
    }
    auto& sub_batch = batch->sub_batch[type];
    task.sub_task_id = sub_batch->size();
    task.type = type;
    ReceiverCreditTarget credit_target;
    const bool credit_exempt =
        queued_owner && queued_owner->receiver_credit_exempt;
    if (!credit_exempt) {
        CHECK_STATUS(resolveReceiverCreditTarget(task.request, credit_target));
        if (credit_target.required && !queued_owner) {
            return Status::InvalidEntry(
                "credit-required failover needs runtime queue "
                "ownership" LOC_MARK);
        }
    }
    if (queued_owner && queued_owner->credit.active &&
        (!credit_target.required ||
         queued_owner->credit.key.receiver_session !=
             credit_target.advert.receiver_session_id ||
         queued_owner->credit.epoch != credit_target.advert.epoch)) {
        // A reservation from another receiver incarnation cannot authorize
        // this attempt. Do not report it to a possibly replaced authority;
        // abandon it and acquire against the current session below.
        queued_owner->credit = ReceiverCreditReservation{};
    }
    bool acquired_this_attempt = false;
    if (!credit_exempt && queued_owner && !queued_owner->credit.active) {
        bool acquired = false;
        CHECK_STATUS(acquireReceiverCredit(task.request, queued_owner->credit,
                                           acquired));
        if (!acquired) {
            if (!queued_owner->in_dispatch_window ||
                dispatch_inflight_owners_ == 0 ||
                dispatch_inflight_bytes_ < queued_owner->byte_charge) {
                return Status::InternalError(
                    "runtime dispatch window accounting underflow" LOC_MARK);
            }
            --dispatch_inflight_owners_;
            dispatch_inflight_bytes_ -= queued_owner->byte_charge;
            queued_owner->in_dispatch_window = false;
            CHECK_STATUS(runtime_queue_->defer(queue_owner_id));
            task.type = UNSPEC;
            task.sub_task_id = -1;
            return Status::OK();
        }
        acquired_this_attempt = queued_owner->credit.active;
    }
    if (queued_owner) {
        if (queued_owner->receiver_credit_exempt) {
            bindReceiverCreditFence(sub_batch, task.request);
        } else {
            bindReceiverCreditFence(sub_batch, queued_owner->credit);
        }
    }
    startTransportAttempt(task, type, std::chrono::steady_clock::now());
    auto submit_status =
        transport->submitTransferTasks(sub_batch, {task.request});
    if (!submit_status.ok()) {
        finishTransportAttempt(task, FAILED, std::chrono::steady_clock::now());
        if (queued_owner && acquired_this_attempt) {
            rollbackReceiverCredit(queued_owner->credit);
        }
        return submit_status;
    }
    if (queued_owner && queued_owner->credit.active) {
        queued_owner->credit.committed = true;
    }
    task.post_time = std::chrono::steady_clock::now();
    return Status::OK();
}

Status TransferEngineImpl::pollTaskStatus(Batch* batch, size_t task_id,
                                          TransferStatus& task_status) {
    auto& task = batch->task_list[task_id];
    if (task.staging) {
        return staging_proxy_->getStatus(&task, task_status);
    }

    if (task.type == UNSPEC) {
        task_status.s = FAILED;
        task_status.transferred_bytes = 0;
        return Status::OK();
    }

    auto& transport = transport_list_[task.type];
    auto& sub_batch = batch->sub_batch[task.type];
    if (!transport || !sub_batch) {
        return Status::InvalidArgument("Transport not available" LOC_MARK);
    }
    return transport->getTransferStatus(sub_batch, task.sub_task_id,
                                        task_status);
}

void TransferEngineImpl::updateTaskStatusAfterPoll(Batch* batch, size_t task_id,
                                                   TransferStatus& task_status,
                                                   bool allow_failover) {
    auto& task = batch->task_list[task_id];
    task.status = task_status.s;
    if (!allow_failover || task.cancel_requested || task_status.s != FAILED ||
        task.type == UNSPEC)
        return;

    // The current physical transport attempt has failed even if the logical
    // request will recover through failover. Close it before task.type is
    // overwritten by resubmitTransferTask().
    finishTransportAttempt(task, FAILED, std::chrono::steady_clock::now());
    if (resubmitTransferTask(batch, task_id).ok()) {
        task_status.s = PENDING;
        task.status = PENDING;
    }
}

Status TransferEngineImpl::sendNotification(SegmentID target_id,
                                            const Notification& notifi) {
    for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
        auto& transport = transport_list_[type];
        if (!transport || !transport->supportNotification()) continue;
        return transport->sendNotification(target_id, notifi);
    }
    return Status::InvalidArgument("Notification not supported" LOC_MARK);
}

Status TransferEngineImpl::probePeerAliveByID(SegmentID target_id) {
    return metadata_->segmentManager().withCachedSegment(
        target_id, [&](SegmentDesc* segment) {
            auto rpc_server_addr = segment->rpc_server_addr;
            if (rpc_server_addr.empty()) {
                return Status::NeedsRefreshCache(
                    "Empty RPC server addr" LOC_MARK);
            }
            auto status = ControlClient::probe(rpc_server_addr);
            if (status.IsRpcServiceError()) {
                // Perhaps rpc_server_addr can be updated in the future
                return Status::NeedsRefreshCache(
                    "RPC service error: " + std::string{status.message()} +
                    LOC_MARK);
            }
            return status;
        });
}

Status TransferEngineImpl::receiveNotification(
    std::vector<Notification>& notifi_list) {
    for (size_t type = 0; type < kSupportedTransportTypes; ++type) {
        auto& transport = transport_list_[type];
        if (!transport || !transport->supportNotification()) continue;
        return transport->receiveNotification(notifi_list);
    }
    return Status::InvalidArgument("Notification not supported" LOC_MARK);
}

Status TransferEngineImpl::getTransferStatus(BatchID batch_id, size_t task_id,
                                             TransferStatus& task_status) {
    if (!batch_id) return Status::InvalidArgument("Invalid batch ID" LOC_MARK);
    std::lock_guard<std::recursive_mutex> lk(progress_mutex_);
    if (!alive_batches_.count(batch_id))
        return Status::InvalidArgument("Batch is not alive" LOC_MARK);
    Batch* batch = (Batch*)(batch_id);
    if (task_id >= batch->task_list.size())
        return Status::InvalidArgument("Invalid task ID" LOC_MARK);
    const size_t public_task_id = task_id;
    size_t poll_task_id = task_id;
    CHECK_STATUS(refillDispatchWindow());
    if (runtime_queue_config_.enabled && batch->queue_token != 0) {
        QueueOwnerId owner_id = 0;
        auto resolve_status = runtime_queue_->resolveOwner(
            batch->queue_token, public_task_id, owner_id);
        if (resolve_status.ok()) {
            TransferStatusEnum public_status = PENDING;
            CHECK_STATUS(runtime_queue_->getPublicStatus(
                batch->queue_token, public_task_id, public_status));
            auto queued_it = queued_owners_.find(owner_id);
            if (public_status != PENDING ||
                (queued_it != queued_owners_.end() &&
                 !queued_it->second.in_dispatch_window)) {
                task_status.s = public_status;
                task_status.transferred_bytes =
                    public_status == COMPLETED
                        ? batch->task_list[public_task_id].request.length
                        : 0;
                return Status::OK();
            }
            if (batch->task_list[public_task_id].derived &&
                queued_it != queued_owners_.end()) {
                poll_task_id = queued_it->second.owner_task_id;
            }
        }
    }
    auto& task = batch->task_list[poll_task_id];
    auto prev_status = task.status;
    CHECK_STATUS(pollTaskStatus(batch, poll_task_id, task_status));
    updateTaskStatusAfterPoll(batch, poll_task_id, task_status,
                              enable_auto_failover_on_poll_);
    if (runtime_queue_config_.enabled && batch->queue_token != 0 &&
        task_status.s != PENDING) {
        QueueOwnerId owner_id = 0;
        auto resolve_status = runtime_queue_->resolveOwner(
            batch->queue_token, public_task_id, owner_id);
        if (resolve_status.ok()) {
            CHECK_STATUS(finishQueuedOwner(owner_id, task_status.s));
            CHECK_STATUS(refillDispatchWindow());
        }
    }

    // Record metrics when task transitions to terminal state
    recordTaskCompletionMetrics(batch->task_list[poll_task_id], prev_status,
                                task_status.s);

    if (task_status.s == COMPLETED) CHECK_STATUS(maybeFireSubmitHooks(batch));
    return Status::OK();
}

Status TransferEngineImpl::getTransferStatus(
    BatchID batch_id, std::vector<TransferStatus>& status_list) {
    if (!batch_id) return Status::InvalidArgument("Invalid batch ID" LOC_MARK);
    std::lock_guard<std::recursive_mutex> lk(progress_mutex_);
    if (!alive_batches_.count(batch_id))
        return Status::InvalidArgument("Batch is not alive" LOC_MARK);
    Batch* batch = (Batch*)(batch_id);
    status_list.clear();
    for (size_t task_id = 0; task_id < batch->task_list.size(); ++task_id) {
        TransferStatus task_status;
        CHECK_STATUS(getTransferStatus(batch_id, task_id, task_status));
        status_list.push_back(task_status);
    }
    return Status::OK();
}

Status TransferEngineImpl::getBatchStatus(BatchID batch_id,
                                          TransferStatus& overall_status,
                                          bool allow_failover) {
    if (!batch_id) return Status::InvalidArgument("Invalid batch ID" LOC_MARK);
    std::lock_guard<std::recursive_mutex> lk(progress_mutex_);
    if (!alive_batches_.count(batch_id))
        return Status::InvalidArgument("Batch is not alive" LOC_MARK);
    CHECK_STATUS(refillDispatchWindow());
    Batch* batch = (Batch*)(batch_id);
    overall_status.s = PENDING;
    overall_status.transferred_bytes = 0;
    size_t success_tasks = 0;
    size_t failed_tasks = 0;
    size_t total_tasks = 0;
    TransferStatusEnum worst_failure = PENDING;
    auto isWorse = [](TransferStatusEnum cur, TransferStatusEnum best) {
        static const std::unordered_map<TransferStatusEnum, int> severity = {
            {INITIAL, 0},  {PENDING, 0}, {COMPLETED, 0}, {INVALID, 1},
            {CANCELED, 2}, {TIMEOUT, 3}, {FAILED, 4},
        };
        return severity.at(cur) > severity.at(best);
    };
    for (size_t task_id = 0; task_id < batch->task_list.size(); ++task_id) {
        auto& task = batch->task_list[task_id];
        if (task.derived) continue;  // This task is performed by other tasks
        total_tasks++;
        if (runtime_queue_config_.enabled && batch->queue_token != 0) {
            QueueOwnerId owner_id = 0;
            auto resolve_status = runtime_queue_->resolveOwner(
                batch->queue_token, task_id, owner_id);
            if (resolve_status.ok()) {
                TransferStatusEnum public_status = PENDING;
                CHECK_STATUS(runtime_queue_->getPublicStatus(
                    batch->queue_token, task_id, public_status));
                auto queued_it = queued_owners_.find(owner_id);
                if (public_status == PENDING) {
                    if (queued_it != queued_owners_.end() &&
                        !queued_it->second.in_dispatch_window) {
                        continue;
                    }
                }
                if (public_status == COMPLETED) {
                    success_tasks++;
                    overall_status.transferred_bytes += task.request.length;
                    continue;
                }
                if (public_status != PENDING) {
                    failed_tasks++;
                    if (isWorse(public_status, worst_failure))
                        worst_failure = public_status;
                    continue;
                }
            }
        }
        TransferStatus task_status;
        if (task.status != PENDING) {
            if (task.status == COMPLETED) {
                success_tasks++;
                overall_status.transferred_bytes += task.request.length;
            } else {
                failed_tasks++;
                if (isWorse(task.status, worst_failure))
                    worst_failure = task.status;
            }
            continue;
        }
        auto prev_status = task.status;
        CHECK_STATUS(pollTaskStatus(batch, task_id, task_status));
        updateTaskStatusAfterPoll(batch, task_id, task_status, allow_failover);
        if (runtime_queue_config_.enabled && batch->queue_token != 0 &&
            task_status.s != PENDING) {
            QueueOwnerId owner_id = 0;
            auto resolve_status = runtime_queue_->resolveOwner(
                batch->queue_token, task_id, owner_id);
            if (resolve_status.ok()) {
                CHECK_STATUS(finishQueuedOwner(owner_id, task_status.s));
                CHECK_STATUS(refillDispatchWindow());
            }
        }

        if (task_status.s == COMPLETED) {
            success_tasks++;
            overall_status.transferred_bytes += task_status.transferred_bytes;
        } else if (task_status.s != PENDING) {
            failed_tasks++;
            if (isWorse(task_status.s, worst_failure))
                worst_failure = task_status.s;
        }

        // Record metrics when task transitions to terminal state
        recordTaskCompletionMetrics(batch->task_list[task_id], prev_status,
                                    task_status.s);
    }
    // Determine overall status: COMPLETED only when all succeed; FAILED only
    // when all tasks are terminal (no in-flight work) and at least one failed;
    // otherwise PENDING (some tasks still running).
    if (success_tasks == total_tasks) {
        overall_status.s = COMPLETED;
    } else if (success_tasks + failed_tasks == total_tasks) {
        overall_status.s = worst_failure;
    }
    // else: some tasks still PENDING → overall_status.s stays PENDING
    CHECK_STATUS(maybeFireSubmitHooks(batch, overall_status.s == COMPLETED));
    return Status::OK();
}

Status TransferEngineImpl::getTransferStatus(BatchID batch_id,
                                             TransferStatus& overall_status) {
    return getBatchStatus(batch_id, overall_status,
                          enable_auto_failover_on_poll_);
}

Status TransferEngineImpl::progressBatch(BatchID batch_id,
                                         TransferStatus& overall_status) {
    return getBatchStatus(batch_id, overall_status, true);
}

Status TransferEngineImpl::getNicLoadStats(
    std::vector<NicLoadStats>& stats) const {
    stats.clear();
    for (const auto& transport : transport_list_) {
        if (transport) {
            CHECK_STATUS(transport->getNicLoadStats(stats));
        }
    }
    return Status::OK();
}

void TransferEngineImpl::notifyBatchMaybeReady(BatchID batch_id) {
    if (progress_worker_) progress_worker_->notifyBatchMaybeReady(batch_id);
}

void TransferEngineImpl::notifyRuntimeQueueReady() {
    if (progress_worker_) progress_worker_->notifyRuntimeQueueReady();
}

Status TransferEngineImpl::waitTransferCompletion(BatchID batch_id) {
    TransferStatus xfer_status;
    while (true) {
        CHECK_STATUS(progressBatch(batch_id, xfer_status));
        if (xfer_status.s != PENDING) {
            freeBatch(batch_id);
            return xfer_status.s == COMPLETED
                       ? Status::OK()
                       : Status::InternalError(
                             "Transfer failed: " +
                             std::to_string((int)xfer_status.s));
        }
    }
}

Status TransferEngineImpl::transferSync(
    const std::vector<Request>& request_list) {
    auto has_any_fence = [](const Request& request) {
        return request.receiver_credit_session_high != 0 ||
               request.receiver_credit_session_low != 0 ||
               request.receiver_credit_epoch != 0;
    };
    auto has_complete_fence = [](const Request& request) {
        return (request.receiver_credit_session_high != 0 ||
                request.receiver_credit_session_low != 0) &&
               request.receiver_credit_epoch != 0;
    };
    const bool delegated_credit_fence =
        !request_list.empty() &&
        std::all_of(request_list.begin(), request_list.end(),
                    has_complete_fence);
    if (!delegated_credit_fence &&
        std::any_of(request_list.begin(), request_list.end(), has_any_fence)) {
        return Status::InvalidArgument(
            "mixed or partial delegated receiver-credit fence" LOC_MARK);
    }

    auto batch_id = allocateBatch(request_list.size());
    auto submit_status =
        submitTransfer(batch_id, request_list, nullptr,
                       delegated_credit_fence ? QueueOwnerKind::StagingInternal
                                              : QueueOwnerKind::User);
    if (!submit_status.ok()) {
        (void)freeBatch(batch_id);
        return submit_status;
    }
    while (true) {
        TransferStatus xfer_status;
        CHECK_STATUS(progressBatch(batch_id, xfer_status));
        if (xfer_status.s == COMPLETED) break;
        if (xfer_status.s != PENDING) {
            CHECK_STATUS(freeBatch(batch_id));
            return Status::InternalError(
                "Transfer via stage buffer failed" LOC_MARK);
        }
    }
    CHECK_STATUS(freeBatch(batch_id));
    return Status::OK();
}

uint64_t TransferEngineImpl::lockStageBuffer(const std::string& location) {
    uint64_t addr = 0;
    auto status = staging_proxy_->pinStageBuffer(location, addr);
    if (!status.ok()) LOG(ERROR) << status.ToString();
    return addr;
}

Status TransferEngineImpl::unlockStageBuffer(uint64_t addr) {
    return staging_proxy_->unpinStageBuffer(addr);
}

void TransferEngineImpl::recordTaskCompletionMetrics(
    TaskInfo& task, TransferStatusEnum prev_status,
    TransferStatusEnum new_status) {
#if TENT_METRICS_ENABLED
    if (prev_status == PENDING && new_status != PENDING && !task.derived) {
        auto end_time = std::chrono::steady_clock::now();
        finishTransportAttempt(task, new_status, end_time);
        auto start_time = task.start_time;
        if (start_time.time_since_epoch().count() > 0) {
            double latency_seconds =
                std::chrono::duration<double>(end_time - start_time).count();
            if (new_status == COMPLETED) {
                if (task.request.opcode == Request::READ) {
                    TentMetrics::instance().recordReadCompleted(
                        task.type, task.request.length, latency_seconds);
                } else {
                    TentMetrics::instance().recordWriteCompleted(
                        task.type, task.request.length, latency_seconds);
                }
                // Causal chain stage decomposition. These stage metrics stay
                // attributed to the final (task.type) transport and measure the
                // full request span for backward compatibility; per-attempt and
                // initial-transport breakdowns live in the additive
                // tent_transport_attempt_* metrics instead.
                if (task.dispatch_time.time_since_epoch().count() > 0) {
                    double queue_wait_us =
                        std::chrono::duration<double, std::micro>(
                            task.dispatch_time - start_time)
                            .count();
                    TENT_RECORD_STAGE_LATENCY(TentMetrics::Stage::QueueWait,
                                              task.type, queue_wait_us);
                    if (task.post_time.time_since_epoch().count() > 0) {
                        double dispatch_us =
                            std::chrono::duration<double, std::micro>(
                                task.post_time - task.dispatch_time)
                                .count();
                        double transport_us =
                            std::chrono::duration<double, std::micro>(
                                end_time - task.post_time)
                                .count();
                        TENT_RECORD_STAGE_LATENCY(TentMetrics::Stage::Dispatch,
                                                  task.type, dispatch_us);
                        TENT_RECORD_STAGE_LATENCY(TentMetrics::Stage::Transport,
                                                  task.type, transport_us);
                    }
                }
            } else if (new_status == FAILED) {
                if (task.request.opcode == Request::READ) {
                    TentMetrics::instance().recordReadFailed(task.type);
                } else {
                    TentMetrics::instance().recordWriteFailed(task.type);
                }
            }
            // Observability only (RFC #2519): deadline feasibility. The
            // infeasible-at-submit case (deadline already in the past when
            // the transfer was submitted) is independent of whether the
            // transfer ultimately completed or failed, so it is recorded for
            // both outcomes. The feasible MLU ratio requires the actual
            // transfer latency, so it is only recorded on COMPLETED.
            if (task.request.deadline_ns != 0) {
                uint64_t start_ns = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        start_time.time_since_epoch())
                        .count());
                if (task.request.deadline_ns > start_ns) {
                    if (new_status == COMPLETED) {
                        double window_seconds =
                            (task.request.deadline_ns - start_ns) / 1e9;
                        TentMetrics::instance().recordDeadlineMLU(
                            task.type, latency_seconds / window_seconds);
                    }
                } else {
                    // Deadline already in the past at submit: infeasible.
                    // Recorded into a dedicated counter so it does not
                    // pollute the MLU histogram with a sentinel value.
                    TentMetrics::instance().recordDeadlineInfeasible(task.type);
                }
            }
            // Reset start_time to prevent duplicate recording
            task.start_time = std::chrono::steady_clock::time_point{};
        }
    }
#endif  // TENT_METRICS_ENABLED
}

void TransferEngineImpl::startTransportAttempt(
    TaskInfo& task, TransportType type,
    std::chrono::steady_clock::time_point post_time) {
    if (task.derived) return;
    if (task.post_time.time_since_epoch().count() == 0) {
        task.post_time = post_time;
    }
    task.attempt_post_time = post_time;
    // Capture the transport now so the attempt is attributed correctly even if
    // task.type is overwritten by failover before finishTransportAttempt().
    task.attempt_type = type;
    task.attempt_active = true;
#if TENT_METRICS_ENABLED
    TentMetrics::instance().recordTransportAttemptStarted(type,
                                                          task.request.opcode);
#else
    (void)type;
#endif
}

void TransferEngineImpl::finishTransportAttempt(
    TaskInfo& task, TransferStatusEnum status,
    std::chrono::steady_clock::time_point end_time) {
    if (!task.attempt_active) return;
    task.attempt_active = false;
#if TENT_METRICS_ENABLED
    auto post_time = task.attempt_post_time;
    if (post_time.time_since_epoch().count() == 0) return;
    double latency_us =
        std::chrono::duration<double, std::micro>(end_time - post_time).count();
    TentMetrics::instance().recordTransportAttemptFinished(
        task.attempt_type, task.request.opcode, status, latency_us);
#else
    (void)status;
    (void)end_time;
#endif
}

}  // namespace tent
}  // namespace mooncake
