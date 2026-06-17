#pragma once

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <sstream>
#include <string>
#include <string_view>

#ifdef YLT_ENABLE_URMA
#include <ylt/coro_io/urma/urma_socket.hpp>
#endif

namespace mooncake {

inline std::string GetRpcProtocolFromEnv() {
    const char* value = std::getenv("MC_RPC_PROTOCOL");
    if (value && std::string_view(value) == "rdma") {
        return "rdma";
    }
#ifdef YLT_ENABLE_URMA
    if (value && std::string_view(value) == "urma") {
        return "urma";
    }
#endif
    return "tcp";
}

inline bool IsUrmaRpcProtocol() {
    const char* value = std::getenv("MC_RPC_PROTOCOL");
    return value && std::string_view(value) == "urma";
}

#ifdef YLT_ENABLE_URMA

inline std::string GetRpcUrmaDeviceFromEnv() {
    const char* value = std::getenv("MC_RPC_URMA_DEVICE");
    return value ? std::string(value) : std::string("bonding_dev_0");
}

inline uint64_t GetRpcUrmaUintFromEnv(const char* name,
                                      uint64_t default_value) {
    const char* value = std::getenv(name);
    if (!value || *value == '\0' || *value == '-') {
        return default_value;
    }

    char* end = nullptr;
    auto parsed = std::strtoull(value, &end, 10);
    if (end == value || *end != '\0') {
        return default_value;
    }
    return parsed;
}

inline coro_io::urma_socket_t::config_t MakeUrmaRpcConfigFromEnv() {
    auto queue_depth = GetRpcUrmaUintFromEnv("MC_RPC_URMA_QUEUE_DEPTH", 64);
    if (queue_depth == 0) {
        queue_depth = 1;
    }
    queue_depth = std::min<uint64_t>(
        queue_depth, std::numeric_limits<uint16_t>::max());

    auto buffer_size = GetRpcUrmaUintFromEnv("MC_RPC_URMA_BUFFER_SIZE", 4096);
    buffer_size = std::min<uint64_t>(
        buffer_size, std::numeric_limits<uint32_t>::max());

    auto max_memory_mib =
        GetRpcUrmaUintFromEnv("MC_RPC_URMA_MAX_MEMORY_MIB", 256);
    max_memory_mib = std::min<uint64_t>(
        max_memory_mib, std::numeric_limits<uint64_t>::max() / 1024 / 1024);

    auto eid_index = GetRpcUrmaUintFromEnv("MC_RPC_URMA_EID_INDEX", 0);
    eid_index = std::min<uint64_t>(
        eid_index, static_cast<uint64_t>(std::numeric_limits<int>::max()));

    coro_io::urma_socket_t::config_t config{};
    config.cq_size = static_cast<uint32_t>(queue_depth * 2 + 8);
    config.recv_buffer_cnt = static_cast<uint16_t>(queue_depth);
    config.send_buffer_cnt = static_cast<uint16_t>(queue_depth);
    config.buffer_size = static_cast<uint32_t>(buffer_size);
    config.max_memory_usage = max_memory_mib * 1024ull * 1024ull;
    config.device_name = GetRpcUrmaDeviceFromEnv();
    config.eid_index = static_cast<int>(eid_index);
    config.tp_type = URMA_CTP;
    return config;
}

inline std::string FormatUrmaRpcConfig(
    const coro_io::urma_socket_t::config_t& config) {
    std::ostringstream os;
    os << "device=" << config.device_name << ", eid_index=" << config.eid_index
       << ", queue_depth=" << config.recv_buffer_cnt
       << ", cq_size=" << config.cq_size
       << ", buffer_size=" << config.buffer_size
       << ", max_memory_mib=" << config.max_memory_usage / 1024 / 1024;
    return os.str();
}

#endif

}  // namespace mooncake
