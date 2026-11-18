// Copyright 2026 KVCache.AI
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
//
// Two-node probe-only selftest. It bypasses TransferEngine/bootstrap:
//
//   node A: discover probe devices -> create local resources -> write a.json
//           -> wait for b.json -> build remote edges -> run PLM + scheduler
//   node B: same, mirrored.
//
// Run both nodes against a shared exchange directory (NFS/sshfs) or copy the
// JSON files manually. The probe test mode is enabled by default; use
// --apply-health=false for a pure link check.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <arpa/inet.h>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstdlib>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "tent/thirdparty/nlohmann/json.h"
#include "tent/transport/ub/device_selection.h"
#include "tent/transport/ub/probe_link_manager.h"
#include "tent/transport/ub/probe_peer_handshake.h"
#include "tent/transport/ub/probe_poster_urma.h"
#include "tent/transport/ub/probe_provider_standalone.h"
#include "tent/transport/ub/probe_scheduler.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake {
namespace tent {
namespace ub {
namespace {

std::atomic<bool> g_stop{false};

void onSignal(int) { g_stop.store(true, std::memory_order_release); }

uint64_t selftestNowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

struct Options {
    std::string node;
    std::string peer;
    enum class Mode { kFile, kTarget, kInitiator };
    Mode mode{Mode::kFile};
    std::string exchange_dir{"/tmp/tent_probe_selftest"};
    std::string bind_address{"0.0.0.0"};
    uint16_t listen_port{19090};
    std::string target_host{"127.0.0.1"};
    uint16_t target_port{19090};
    uint64_t duration_s{60};
    uint64_t wait_timeout_s{120};
    uint64_t test_interval_ms{5000};
    uint64_t interval_ms{1000};
    uint64_t timeout_ms{1000};
    uint32_t jfc_depth{64};
    uint32_t peer_budget{4};
    uint64_t source_page_size{0};
    bool apply_health{false};
    bool list_devices{false};
    bool verbose_paths{false};
    std::vector<std::string> device_filter;
    struct PortPair {
        int32_t local_nic{0};
        int32_t peer_nic{0};
    };
    std::vector<PortPair> pairs;
};

void usage(const char* argv0) {
    std::cout
        << "Usage: " << argv0 << " --node <name> --peer <name> [options]\n"
        << "  --mode <file|target|initiator>  exchange mode (default file)\n"
        << "  --exchange-dir <dir>      file mode: shared directory for "
           "<node>.json\n"
        << "  --listen-port <port>      target mode: TCP listen port (default "
           "19090)\n"
        << "  --bind-address <addr>     target mode: bind address (default "
           "0.0.0.0)\n"
        << "  --target-host <host>      initiator mode: target host\n"
        << "  --target-port <port>      initiator mode: target port\n"
        << "  --duration <sec>          run time (default 60)\n"
        << "  --wait-timeout <sec>      wait for peer JSON (default 120)\n"
        << "  --test-interval-ms <ms>  probe test cadence (default 5000)\n"
        << "  --apply-health <bool>     false = pure link check (default "
           "false)\n"
        << "  --interval-ms <ms>        normal retry interval (default 1000)\n"
        << "  --timeout-ms <ms>         probe completion timeout (default "
           "1000)\n"
        << "  --jfc-depth <n>           probe JFC depth (default 64)\n"
        << "  --peer-budget <n>         per-peer in-flight budget (default "
           "4)\n"
        << "  --device-filter <a,b>     native/topology names (default all "
           "non-bonding)\n"
        << "  --source-page-size <n>    0 = system page (default 0)\n"
        << "  --list-devices            list selectable non-bonding ports "
           "(with index) and exit\n"
        << "  --pairs <l>:<p>,...       explicit local/peer port pairs, e.g. "
           "1:3,2:4\n"
        << "  --verbose-paths           print every path state each report\n"
        << "\nRemove stale <node>.json files from the exchange directory "
           "before "
           "a new run.\n";
}

bool parseBool(const std::string& value, bool& out) {
    if (value == "1" || value == "true" || value == "yes") {
        out = true;
        return true;
    }
    if (value == "0" || value == "false" || value == "no") {
        out = false;
        return true;
    }
    return false;
}

bool parseOptions(int argc, char** argv, Options& options) {
    for (int i = 1; i < argc; ++i) {
        const std::string key = argv[i];
        auto value = [&](std::string& out) {
            if (i + 1 >= argc) return false;
            out = argv[++i];
            return true;
        };
        if (key == "--help" || key == "-h") {
            return false;
        } else if (key == "--node") {
            if (!value(options.node)) return false;
        } else if (key == "--peer") {
            if (!value(options.peer)) return false;
        } else if (key == "--mode") {
            std::string text;
            if (!value(text)) return false;
            if (text == "file") {
                options.mode = Options::Mode::kFile;
            } else if (text == "target") {
                options.mode = Options::Mode::kTarget;
            } else if (text == "initiator") {
                options.mode = Options::Mode::kInitiator;
            } else {
                return false;
            }
        } else if (key == "--exchange-dir") {
            if (!value(options.exchange_dir)) return false;
        } else if (key == "--listen-port") {
            std::string text;
            if (!value(text)) return false;
            options.listen_port = static_cast<uint16_t>(std::stoul(text));
        } else if (key == "--bind-address") {
            if (!value(options.bind_address)) return false;
        } else if (key == "--target-host") {
            if (!value(options.target_host)) return false;
        } else if (key == "--target-port") {
            std::string text;
            if (!value(text)) return false;
            options.target_port = static_cast<uint16_t>(std::stoul(text));
        } else if (key == "--duration") {
            std::string text;
            if (!value(text)) return false;
            options.duration_s = std::stoull(text);
        } else if (key == "--wait-timeout") {
            std::string text;
            if (!value(text)) return false;
            options.wait_timeout_s = std::stoull(text);
        } else if (key == "--test-interval-ms") {
            std::string text;
            if (!value(text)) return false;
            options.test_interval_ms = std::stoull(text);
        } else if (key == "--interval-ms") {
            std::string text;
            if (!value(text)) return false;
            options.interval_ms = std::stoull(text);
        } else if (key == "--timeout-ms") {
            std::string text;
            if (!value(text)) return false;
            options.timeout_ms = std::stoull(text);
        } else if (key == "--jfc-depth") {
            std::string text;
            if (!value(text)) return false;
            options.jfc_depth = static_cast<uint32_t>(std::stoul(text));
        } else if (key == "--peer-budget") {
            std::string text;
            if (!value(text)) return false;
            options.peer_budget = static_cast<uint32_t>(std::stoul(text));
        } else if (key == "--source-page-size") {
            std::string text;
            if (!value(text)) return false;
            options.source_page_size = std::stoull(text);
        } else if (key == "--apply-health") {
            std::string text;
            if (!value(text) || !parseBool(text, options.apply_health)) {
                return false;
            }
        } else if (key == "--list-devices") {
            options.list_devices = true;
        } else if (key == "--verbose-paths") {
            options.verbose_paths = true;
        } else if (key == "--pairs") {
            std::string text;
            if (!value(text)) return false;
            size_t start = 0;
            while (start <= text.size()) {
                const size_t comma = text.find(',', start);
                const std::string item = text.substr(
                    start, comma == std::string::npos ? std::string::npos
                                                      : comma - start);
                const size_t colon = item.find(':');
                if (colon == std::string::npos || colon == 0 ||
                    colon + 1 == item.size()) {
                    return false;
                }
                options.pairs.push_back(
                    Options::PortPair{std::stoi(item.substr(0, colon)),
                                      std::stoi(item.substr(colon + 1))});
                if (comma == std::string::npos) break;
                start = comma + 1;
            }
        } else if (key == "--device-filter") {
            std::string text;
            if (!value(text)) return false;
            size_t start = 0;
            while (start <= text.size()) {
                const size_t comma = text.find(',', start);
                const std::string item = text.substr(
                    start, comma == std::string::npos ? std::string::npos
                                                      : comma - start);
                if (!item.empty()) options.device_filter.push_back(item);
                if (comma == std::string::npos) break;
                start = comma + 1;
            }
        } else {
            std::cerr << "unknown option: " << key << "\n";
            return false;
        }
    }
    if (options.list_devices) return true;
    if (options.node.empty() || options.peer.empty()) return false;
    if (options.node == options.peer) {
        std::cerr << "--node and --peer must differ\n";
        return false;
    }
    return true;
}

nlohmann::json descriptorToJson(const SegmentDescriptor& descriptor) {
    return nlohmann::json{{"schema_version", descriptor.schema_version},
                          {"urma_api_version", descriptor.urma_api_version},
                          {"urma_abi_size", descriptor.urma_abi_size},
                          {"hex", descriptor.hex}};
}

bool descriptorFromJson(const nlohmann::json& value,
                        SegmentDescriptor& descriptor) {
    if (!value.is_object()) return false;
    descriptor.schema_version = value.value("schema_version", 0U);
    descriptor.urma_api_version = value.value("urma_api_version", 0U);
    descriptor.urma_abi_size = value.value("urma_abi_size", 0U);
    descriptor.hex = value.value("hex", std::string());
    return descriptor.schema_version != 0 && !descriptor.hex.empty();
}

struct PeerPort {
    int32_t local_nic{-1};
    std::string native_device_name;
    std::string topology_name;
    std::string device_eid;
    SegmentDescriptor area_descriptor;
    uint64_t area_epoch{0};
    uint32_t area_token{0};
    uint32_t jetty_id{0};
    uint32_t jetty_uasid{0};
    uint32_t jetty_token{0};
};

class StaticPeerSource final : public ProbePeerDescriptorSource {
   public:
    explicit StaticPeerSource(std::vector<ProbePeerDescriptor> descriptors)
        : descriptors_(std::move(descriptors)) {}

    Status snapshot(std::vector<ProbePeerDescriptor>& descriptors) override {
        descriptors = descriptors_;
        return Status::OK();
    }

   private:
    std::vector<ProbePeerDescriptor> descriptors_;
};

nlohmann::json buildLocalJson(const std::string& node,
                              const StandaloneProbeResourceProvider& provider) {
    const auto ports = provider.portViews();
    const auto areas = provider.localAreaViews();
    std::map<Topology::NicID,
             const StandaloneProbeResourceProvider::LocalAreaView*>
        area_by_nic;
    for (const auto& area : areas) area_by_nic[area.local_nic] = &area;

    nlohmann::json root;
    root["schema"] = 1;
    root["node"] = node;
    root["ports"] = nlohmann::json::array();
    for (const auto& port : ports) {
        auto area = area_by_nic.find(port.local_nic);
        if (area == area_by_nic.end()) continue;
        const auto& local_area = *area->second;
        root["ports"].push_back(nlohmann::json{
            {"local_nic", port.local_nic},
            {"native_device_name", port.native_device_name},
            {"topology_name", port.topology_name},
            {"device_eid", port.device_eid},
            {"area_descriptor", descriptorToJson(local_area.descriptor)},
            {"area_length", 1},
            {"area_page_size", local_area.length},
            {"area_epoch", local_area.epoch},
            {"area_token", local_area.token},
            {"probe_jetty",
             {{"eid", port.device_eid},
              {"id", port.native_jetty_id},
              {"uasid", port.native_jetty_uasid},
              {"token", local_area.token}}}});
    }
    return root;
}

void writeLocalJson(const std::string& path, const std::string& node,
                    const StandaloneProbeResourceProvider& provider) {
    const auto root = buildLocalJson(node, provider);
    std::filesystem::create_directories(
        std::filesystem::path(path).parent_path());
    const std::string tmp = path + ".tmp";
    {
        std::ofstream out(tmp, std::ios::trunc);
        if (!out) throw std::runtime_error("cannot write " + tmp);
        out << root.dump(2) << "\n";
    }
    std::filesystem::rename(tmp, path);
}

void setSocketTimeouts(int fd, uint64_t timeout_s) {
    timeval timeout{};
    timeout.tv_sec = static_cast<time_t>(timeout_s);
    timeout.tv_usec = 0;
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
}

bool sendAll(int fd, const void* data, size_t length) {
    const char* cursor = static_cast<const char*>(data);
    size_t sent = 0;
    while (sent < length) {
        const ssize_t rc = ::send(fd, cursor + sent, length - sent, 0);
        if (rc > 0) {
            sent += static_cast<size_t>(rc);
            continue;
        }
        if (rc < 0 && errno == EINTR) continue;
        return false;
    }
    return true;
}

bool recvAll(int fd, void* data, size_t length) {
    char* cursor = static_cast<char*>(data);
    size_t received = 0;
    while (received < length) {
        const ssize_t rc = ::recv(fd, cursor + received, length - received, 0);
        if (rc > 0) {
            received += static_cast<size_t>(rc);
            continue;
        }
        if (rc < 0 && errno == EINTR) continue;
        return false;
    }
    return true;
}

bool sendFrame(int fd, const nlohmann::json& payload) {
    const std::string text = payload.dump();
    if (text.size() > 4 * 1024 * 1024) return false;
    const uint32_t length = htonl(static_cast<uint32_t>(text.size()));
    return sendAll(fd, &length, sizeof(length)) &&
           sendAll(fd, text.data(), text.size());
}

bool recvFrame(int fd, nlohmann::json& payload) {
    uint32_t length = 0;
    if (!recvAll(fd, &length, sizeof(length))) return false;
    length = ntohl(length);
    if (length == 0 || length > 4 * 1024 * 1024) return false;
    std::string text(length, '\0');
    if (!recvAll(fd, text.data(), text.size())) return false;
    try {
        payload = nlohmann::json::parse(text);
    } catch (const std::exception&) {
        return false;
    }
    return true;
}

int connectWithTimeout(const std::string& host, uint16_t port,
                       uint64_t timeout_s) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* addresses = nullptr;
    const std::string port_text = std::to_string(port);
    if (getaddrinfo(host.c_str(), port_text.c_str(), &hints, &addresses) != 0) {
        return -1;
    }
    int connected = -1;
    for (addrinfo* address = addresses; address != nullptr;
         address = address->ai_next) {
        const int fd = ::socket(address->ai_family, address->ai_socktype,
                                address->ai_protocol);
        if (fd < 0) continue;
        const int flags = fcntl(fd, F_GETFL, 0);
        (void)fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        int rc = ::connect(fd, address->ai_addr, address->ai_addrlen);
        if (rc < 0 && errno == EINPROGRESS) {
            pollfd descriptor{fd, POLLOUT, 0};
            rc = ::poll(&descriptor, 1, static_cast<int>(timeout_s * 1000));
            if (rc > 0) {
                int error = 0;
                socklen_t error_length = sizeof(error);
                (void)getsockopt(fd, SOL_SOCKET, SO_ERROR, &error,
                                 &error_length);
                rc = error == 0 ? 0 : -1;
            } else {
                rc = -1;
            }
        } else {
            rc = rc < 0 ? -1 : 0;
        }
        if (rc == 0) {
            (void)fcntl(fd, F_SETFL, flags);
            setSocketTimeouts(fd, timeout_s);
            connected = fd;
            break;
        }
        ::close(fd);
    }
    freeaddrinfo(addresses);
    return connected;
}

nlohmann::json exchangeOverTcp(const nlohmann::json& local,
                               const Options& options) {
    nlohmann::json peer;
    if (options.mode == Options::Mode::kTarget) {
        int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd < 0) {
            throw std::runtime_error("cannot create listen socket");
        }
        int reuse = 1;
        (void)setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse,
                         sizeof(reuse));
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(options.listen_port);
        if (inet_pton(AF_INET, options.bind_address.c_str(),
                      &address.sin_addr) != 1) {
            address.sin_addr.s_addr = htonl(INADDR_ANY);
        }
        if (::bind(listen_fd, reinterpret_cast<sockaddr*>(&address),
                   sizeof(address)) != 0 ||
            ::listen(listen_fd, 1) != 0) {
            ::close(listen_fd);
            throw std::runtime_error("cannot bind/listen on " +
                                     options.bind_address + ":" +
                                     std::to_string(options.listen_port));
        }
        std::cout << "target listening on " << options.bind_address << ":"
                  << options.listen_port << std::endl;
        pollfd descriptor{listen_fd, POLLIN, 0};
        const int ready = ::poll(
            &descriptor, 1, static_cast<int>(options.wait_timeout_s * 1000));
        if (ready <= 0) {
            ::close(listen_fd);
            throw std::runtime_error("timed out waiting for initiator");
        }
        const int fd = ::accept(listen_fd, nullptr, nullptr);
        ::close(listen_fd);
        if (fd < 0) throw std::runtime_error("accept failed");
        setSocketTimeouts(fd, options.wait_timeout_s);
        const bool ok = recvFrame(fd, peer) && sendFrame(fd, local);
        ::close(fd);
        if (!ok) throw std::runtime_error("target exchange failed");
    } else {
        const int fd = connectWithTimeout(
            options.target_host, options.target_port, options.wait_timeout_s);
        if (fd < 0) {
            throw std::runtime_error("cannot connect to " +
                                     options.target_host + ":" +
                                     std::to_string(options.target_port));
        }
        const bool ok = sendFrame(fd, local) && recvFrame(fd, peer);
        ::close(fd);
        if (!ok) throw std::runtime_error("initiator exchange failed");
    }
    return peer;
}

bool waitForPeerJson(const std::string& path, uint64_t timeout_s,
                     nlohmann::json& root) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);
    while (!g_stop.load(std::memory_order_acquire)) {
        if (std::filesystem::exists(path)) {
            try {
                std::ifstream in(path);
                in >> root;
                if (root.value("schema", 0) == 1) return true;
            } catch (const std::exception& error) {
                std::cerr << "peer JSON is not ready: " << error.what() << "\n";
            }
        }
        if (std::chrono::steady_clock::now() >= deadline) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}

std::vector<PeerPort> parsePeerPorts(const nlohmann::json& root,
                                     const std::string& peer_node) {
    std::vector<PeerPort> ports;
    if (!root.contains("node") ||
        root["node"].get<std::string>() != peer_node) {
        throw std::runtime_error("peer JSON node does not match --peer");
    }
    if (!root.contains("ports") || !root["ports"].is_array()) {
        throw std::runtime_error("peer JSON has no ports array");
    }
    for (const auto& item : root["ports"]) {
        PeerPort port;
        port.local_nic = item.value("local_nic", -1);
        port.native_device_name =
            item.value("native_device_name", std::string());
        port.topology_name = item.value("topology_name", std::string());
        port.device_eid = item.value("device_eid", std::string());
        port.area_epoch = item.value("area_epoch", 0ULL);
        port.area_token = item.value("area_token", 0U);
        if (!item.contains("area_descriptor") ||
            !descriptorFromJson(item["area_descriptor"],
                                port.area_descriptor) ||
            port.local_nic < 0 || port.area_epoch == 0 ||
            port.device_eid.empty()) {
            throw std::runtime_error("peer JSON has an invalid port");
        }
        if (item.contains("probe_jetty")) {
            const auto& jetty = item["probe_jetty"];
            port.jetty_id = jetty.value("id", 0U);
            port.jetty_uasid = jetty.value("uasid", 0U);
            port.jetty_token = jetty.value("token", port.area_token);
        }
        if (port.jetty_id == 0) {
            throw std::runtime_error("peer JSON has no probe Jetty id");
        }
        ports.push_back(std::move(port));
    }
    if (ports.empty()) throw std::runtime_error("peer JSON has no ports");
    return ports;
}

const char* taskName(TaskState state) {
    switch (state) {
        case TaskState::kIdle:
            return "idle";
        case TaskState::kQueued:
            return "queued";
        case TaskState::kInflight:
            return "inflight";
    }
    return "?";
}

const char* pendingName(PendingReason reason) {
    switch (reason) {
        case PendingReason::kNone:
            return "none";
        case PendingReason::kLaneRetired:
            return "lane_retired";
        case PendingReason::kPortRevoked:
            return "port_revoked";
        case PendingReason::kContextFailure:
            return "context_failure";
    }
    return "?";
}

void printStats(const ProbeLinkManager& plm, uint64_t elapsed_s,
                bool verbose_paths) {
    const auto stats = plm.stats();
    const auto view = plm.healthView();
    size_t eligible = 0;
    size_t healthy = 0;
    size_t isolated = 0;
    size_t recovering = 0;
    size_t disabled = 0;
    size_t pending = 0;
    for (const auto& entry : view) {
        switch (entry.health) {
            case LinkHealth::kEligibleUnverified:
                ++eligible;
                break;
            case LinkHealth::kHealthy:
                ++healthy;
                break;
            case LinkHealth::kIsolated:
                ++isolated;
                break;
            case LinkHealth::kRecovering:
                ++recovering;
                break;
            case LinkHealth::kDisabled:
                ++disabled;
                break;
        }
        if (entry.pending) ++pending;
    }
    std::cout << "[probe] t=" << elapsed_s << "s paths=" << view.size()
              << " eligible=" << eligible << " healthy=" << healthy
              << " isolated=" << isolated << " recovering=" << recovering
              << " disabled=" << disabled << " pending=" << pending
              << " inflight=" << plm.inflightCount()
              << " test_post=" << stats.probe_test_post
              << " ok=" << stats.probe_test_ok
              << " fail=" << stats.probe_test_fail
              << " missed=" << stats.probe_test_missed
              << " timeout=" << stats.probe_timeout_unknown << std::endl;
    if (verbose_paths) {
        for (const auto& entry : view) {
            std::cout << "[path] local=" << entry.version.path.local_nic
                      << " peer=" << entry.peer.segment_id
                      << " remote=" << entry.version.path.remote_nic
                      << " state=" << linkHealthName(entry.health)
                      << " task=" << taskName(entry.task) << " pending="
                      << (entry.pending ? pendingName(entry.pending_reason)
                                        : "none")
                      << " fault_epoch=" << entry.fault_epoch
                      << " failures=" << entry.consecutive_failures
                      << " seq=" << entry.probe_seq << std::endl;
        }
    }
}

}  // namespace

int runSelftest(const Options& options) {
    auto adapter = createDefaultUrmaAdapter();
    if (!adapter || !adapter->available()) {
        std::cerr
            << "TENT UB adapter is unavailable: rebuild with USE_UB=ON and a "
               "real URMA library (URMA_LIBRARY)\n";
        return 1;
    }
    auto status = adapter->initialize();
    if (!status.ok()) {
        std::cerr << "adapter initialize failed: " << status.ToString() << "\n";
        return 1;
    }
    if (options.list_devices) {
        std::vector<DeviceInfo> devices;
        status = adapter->discoverDevices(devices);
        if (!status.ok()) {
            std::cerr << "device discovery failed: " << status.ToString()
                      << "\n";
            adapter->shutdown();
            return 1;
        }
        size_t index = 0;
        for (const auto& device : devices) {
            if (!device.active || isBondingDevice(device)) continue;
            std::cout << "index=" << index++
                      << " native=" << device.native_device_name
                      << " topology=" << device.topology_name
                      << " eid=" << device.eid
                      << " eid_index=" << device.eid_index
                      << " transport_modes="
                      << device.capabilities.transport_modes << std::endl;
        }
        adapter->shutdown();
        return 0;
    }

    ProbeParams params;
    params.enabled = true;
    params.provider = ProbeProviderKind::kStandalone;
    params.jfc_mode = ProbeJfcMode::kDedicated;
    params.jfc_depth = options.jfc_depth;
    params.interval_ms = options.interval_ms;
    params.timeout_ms = options.timeout_ms;
    params.peer_budget = options.peer_budget;
    params.device_filter = options.device_filter;
    params.source_page_size = options.source_page_size;
    params.test_periodic_all = true;
    params.test_interval_ms = options.test_interval_ms;
    params.test_apply_health = options.apply_health;
    if (!params.valid()) {
        std::cerr << "invalid probe parameters\n";
        return 1;
    }

    auto provider =
        std::make_unique<StandaloneProbeResourceProvider>(adapter, params);
    status = provider->discover();
    if (!status.ok()) {
        std::cerr << "probe discovery failed: " << status.ToString() << "\n";
        return 1;
    }
    status = provider->prepareLocal();
    if (!status.ok()) {
        std::cerr << "probe local resource creation failed: "
                  << status.ToString() << "\n";
        return 1;
    }

    const nlohmann::json local_json = buildLocalJson(options.node, *provider);
    nlohmann::json peer_json;
    try {
        if (options.mode == Options::Mode::kFile) {
            const std::filesystem::path exchange(options.exchange_dir);
            const auto local_file = exchange / (options.node + ".json");
            const auto peer_file = exchange / (options.peer + ".json");
            writeLocalJson(local_file.string(), options.node, *provider);
            std::cout << "local descriptors written to " << local_file
                      << "; waiting for " << peer_file << std::endl;
            if (!waitForPeerJson(peer_file.string(), options.wait_timeout_s,
                                 peer_json)) {
                throw std::runtime_error("timed out waiting for peer JSON");
            }
        } else {
            peer_json = exchangeOverTcp(local_json, options);
        }
    } catch (const std::exception& error) {
        std::cerr << "descriptor exchange failed: " << error.what() << "\n";
        provider->shutdown();
        adapter->shutdown();
        return 1;
    }
    std::vector<PeerPort> peer_ports;
    try {
        peer_ports = parsePeerPorts(peer_json, options.peer);
    } catch (const std::exception& error) {
        std::cerr << "invalid peer JSON: " << error.what() << "\n";
        provider->shutdown();
        adapter->shutdown();
        return 1;
    }

    auto local_ports = provider->portViews();
    if (local_ports.empty()) {
        std::cerr << "no local probe port\n";
        provider->shutdown();
        adapter->shutdown();
        return 1;
    }
    std::sort(local_ports.begin(), local_ports.end(),
              [](const auto& left, const auto& right) {
                  return left.local_nic < right.local_nic;
              });
    std::sort(peer_ports.begin(), peer_ports.end(),
              [](const PeerPort& left, const PeerPort& right) {
                  return left.local_nic < right.local_nic;
              });
    std::map<int32_t, const StandaloneProbeResourceProvider::PortView*>
        local_by_nic;
    std::map<int32_t, const PeerPort*> peer_by_nic;
    for (const auto& port : local_ports) local_by_nic[port.local_nic] = &port;
    for (const auto& port : peer_ports) peer_by_nic[port.local_nic] = &port;

    std::vector<std::pair<const StandaloneProbeResourceProvider::PortView*,
                          const PeerPort*>>
        pairs;
    if (!options.pairs.empty()) {
        for (const auto& requested : options.pairs) {
            auto local = local_by_nic.find(requested.local_nic);
            auto peer = peer_by_nic.find(requested.peer_nic);
            if (local == local_by_nic.end() || peer == peer_by_nic.end()) {
                std::cerr << "warning: pair " << requested.local_nic << ":"
                          << requested.peer_nic << " is not available\n";
                continue;
            }
            pairs.emplace_back(local->second, peer->second);
        }
    } else {
        // Same-name physical ports are the intended adjacency when both sides
        // export native_device_name; discovery order is only a fallback.
        // topology_name contains the EID, so it must win over the native name
        // when one device exposes multiple EIDs.
        std::map<std::string, const PeerPort*> peer_by_topology;
        std::map<std::string, const PeerPort*> peer_by_native;
        for (const auto& port : peer_ports) {
            if (!port.topology_name.empty()) {
                peer_by_topology.emplace(port.topology_name, &port);
            }
            if (!port.native_device_name.empty()) {
                peer_by_native.emplace(port.native_device_name, &port);
            }
        }
        std::set<const PeerPort*> used;
        std::vector<const StandaloneProbeResourceProvider::PortView*> unmatched;
        for (const auto& local : local_ports) {
            const PeerPort* matched = nullptr;
            if (!local.topology_name.empty()) {
                auto peer = peer_by_topology.find(local.topology_name);
                if (peer != peer_by_topology.end() &&
                    used.find(peer->second) == used.end()) {
                    matched = peer->second;
                }
            }
            if (matched == nullptr && !local.native_device_name.empty()) {
                auto peer = peer_by_native.find(local.native_device_name);
                if (peer != peer_by_native.end() &&
                    used.find(peer->second) == used.end()) {
                    matched = peer->second;
                }
            }
            if (matched != nullptr) {
                used.insert(matched);
                pairs.emplace_back(&local, matched);
                continue;
            }
            unmatched.push_back(&local);
        }
        std::vector<const PeerPort*> remaining;
        for (const auto& port : peer_ports) {
            if (used.find(&port) == used.end()) remaining.push_back(&port);
        }
        const size_t fallback = std::min(unmatched.size(), remaining.size());
        for (size_t i = 0; i < fallback; ++i) {
            pairs.emplace_back(unmatched[i], remaining[i]);
        }
        if (pairs.size() != local_ports.size()) {
            std::cerr << "warning: paired " << pairs.size() << " of "
                      << local_ports.size() << " local port(s)\n";
        }
    }

    std::vector<ProbePeerDescriptor> descriptors;
    constexpr SegmentID kPeerSegmentId = 1;
    for (const auto& pair : pairs) {
        const auto& local = *pair.first;
        const auto& peer = *pair.second;
        std::cout << "probe pair: local_nic=" << local.local_nic
                  << " topology=" << local.topology_name
                  << " -> peer_nic=" << peer.local_nic
                  << " topology=" << peer.topology_name << std::endl;
        ProbePeerDescriptor descriptor;
        descriptor.local_nic = local.local_nic;
        descriptor.peer_segment_id = kPeerSegmentId;
        descriptor.remote_nic = peer.local_nic;
        descriptor.peer_nic_path = "peer@" + options.peer;
        descriptor.topology_epoch = 1;
        descriptor.target_epoch = 1;
        descriptor.area_descriptor = peer.area_descriptor;
        descriptor.area_length = 1;
        descriptor.area_epoch = peer.area_epoch;
        descriptor.area_token = peer.area_token;
        descriptor.remote_jetty = RemoteJettyInfo{
            peer.device_eid, peer.jetty_id, peer.jetty_uasid, peer.jetty_token};
        descriptor.node_gate_ok = true;
        descriptor.port_gate_ok = true;
        descriptor.profile_gate_ok = true;
        descriptors.push_back(std::move(descriptor));
    }
    if (descriptors.empty()) {
        std::cerr << "no matching local/peer probe port\n";
        provider->shutdown();
        adapter->shutdown();
        return 1;
    }

    StaticPeerSource source(std::move(descriptors));
    HandshakeProbePeerDirectory directory(adapter, provider->contexts(),
                                          &source);
    provider->setRemoteDirectory(&directory);

    ProbeGraph graph;
    status = provider->snapshot(graph);
    if (!status.ok()) {
        std::cerr << "probe graph build failed: " << status.ToString() << "\n";
        directory.shutdown();
        provider->shutdown();
        adapter->shutdown();
        return 1;
    }

    ProbeLinkManager plm(params);
    plm.setReportSink(provider.get());
    status = plm.initialize(graph);
    if (!status.ok()) {
        std::cerr << "probe manager initialize failed: " << status.ToString()
                  << "\n";
        directory.shutdown();
        provider->shutdown();
        adapter->shutdown();
        return 1;
    }
    // The PLM now owns its copy of the supply; drop the local graph references
    // so a later lane rebuild can delete the old Jetty handle.
    std::vector<JfcPtr> poll_jfcs;
    for (const auto& port : graph.ports) {
        if (port.probe_jfc) poll_jfcs.push_back(port.probe_jfc);
    }
    graph.ports.clear();
    graph.edges.clear();

    provider->setAvailabilitySink(plm.availabilitySink());
    provider->setCompletionSink([&plm](const Completion& completion) {
        if (!isProbeToken(completion.token)) {
            return Status::InvalidArgument("non-probe completion");
        }
        if (!plm.onProbeCompletion(completion)) {
            return Status::InternalError("probe completion was not accepted");
        }
        return Status::OK();
    });

    std::atomic<bool> poll_stop{false};
    std::vector<std::thread> pollers;
    for (const auto& jfc : poll_jfcs) {
        pollers.emplace_back([&, jfc] {
            while (!poll_stop.load(std::memory_order_acquire)) {
                std::vector<Completion> completions;
                auto poll_status = adapter->poll(jfc, 64, completions);
                if (!poll_status.ok()) {
                    std::cerr
                        << "probe JFC poll failed: " << poll_status.ToString()
                        << "\n";
                    break;
                }
                for (const auto& completion : completions) {
                    (void)plm.onProbeCompletion(completion);
                }
                if (completions.empty()) {
                    std::this_thread::sleep_for(std::chrono::microseconds(200));
                }
            }
        });
    }

    UrmaProbePoster poster(adapter);
    ProbeScheduler scheduler(plm, poster);
    scheduler.start();

    const uint64_t start_ns = selftestNowNs();
    const uint64_t duration_ns = options.duration_s * 1'000'000'000ULL;
    // One report per test round instead of a fixed one-second tick.
    const uint64_t report_interval_ns =
        std::max<uint64_t>(options.test_interval_ms, 1) * 1'000'000ULL;
    uint64_t next_report_ns = start_ns + report_interval_ns;
    std::cout << "probe selftest started: node=" << options.node
              << " peer=" << options.peer
              << " test_interval_ms=" << options.test_interval_ms
              << " apply_health=" << (options.apply_health ? "true" : "false")
              << std::endl;
    printStats(plm, 0, options.verbose_paths);

    while (!g_stop.load(std::memory_order_acquire)) {
        const uint64_t now = selftestNowNs();
        if (now - start_ns >= duration_ns) break;
        if (now >= next_report_ns) {
            printStats(plm, (now - start_ns) / 1'000'000'000ULL,
                       options.verbose_paths);
            next_report_ns += report_interval_ns;
            if (next_report_ns <= now) {
                next_report_ns = now + report_interval_ns;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::cout << "stopping probe selftest..." << std::endl;
    const auto drain =
        plm.drainBeforePollStop(selftestNowNs() + params.timeoutNs());
    poll_stop.store(true, std::memory_order_release);
    for (auto& poller : pollers) {
        if (poller.joinable()) poller.join();
    }
    if (drain.unproven != 0) {
        // The design keeps unresolved probe resources alive on the process
        // exit path; do not unimport/close memory that may still be in DMA.
        std::cout << "unproven probe records: " << drain.unproven
                  << "; leaving resources to process teardown" << std::endl;
        std::cout.flush();
        std::cerr.flush();
        std::_Exit(2);
    }
    provider->setAvailabilitySink(nullptr);
    provider->setCompletionSink(nullptr);
    // Unimport remote probe areas before the probe contexts close.
    directory.shutdown();
    provider->shutdown();
    adapter->shutdown();
    std::cout << "probe selftest stopped: settled=" << drain.settled
              << " unproven=" << drain.unproven
              << " drained=" << (drain.drained ? "true" : "false") << std::endl;
    return drain.drained ? 0 : 2;
}

}  // namespace ub
}  // namespace tent
}  // namespace mooncake

int main(int argc, char** argv) {
    using namespace mooncake::tent::ub;
    try {
        std::signal(SIGINT, onSignal);
        std::signal(SIGTERM, onSignal);
        Options options;
        if (!parseOptions(argc, argv, options)) {
            usage(argv[0]);
            return 1;
        }
        return runSelftest(options);
    } catch (const std::exception& error) {
        std::cerr << "probe selftest failed: " << error.what() << "\n";
        return 1;
    }
}
