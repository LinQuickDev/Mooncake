#include <gflags/gflags.h>
#include <algorithm>
#include <array>
#include <csignal>
#include <string_view>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "client_service.h"
#include "common.h"
#include "config.h"
#include "mooncake_logging.h"
#include "real_client.h"
#include "scheduler/scheduler_policy.h"

using namespace mooncake;

DEFINE_string(host, "0.0.0.0", "Local hostname");
DEFINE_string(metadata_server, "http://127.0.0.1:8080/metadata",
              "Metadata server connection string");
DEFINE_string(device_names, "", "Device names");
DEFINE_string(master_server_address, "127.0.0.1:50051",
              "Master server address");
DEFINE_string(protocol, "tcp", "Protocol");
DEFINE_int32(port, 50052, "Real Client service port");
DEFINE_string(global_segment_size, "4 GB", "Size of global segment");
DEFINE_string(local_buffer_size, "0", "Size of local buffer (e.g., 16MB, 1GB)");
DEFINE_int32(threads, 1, "Number of threads for client service");
DEFINE_string(tenant_id, "default", "Tenant identifier");
DEFINE_bool(scheduling, false, "Enable Transfer Engine scheduling");
DEFINE_uint64(scheduler_quantum_bytes, 1ULL << 20, "Scheduler byte quantum");
DEFINE_uint64(scheduler_max_inflight_bytes, 16ULL << 20,
              "Scheduler maximum in-flight bytes");
DEFINE_uint64(scheduler_reserved_high_bytes, 1ULL << 20,
              "Scheduler bytes reserved for HIGH traffic");
DEFINE_uint32(scheduler_max_slices, 32,
              "Scheduler maximum transport slices per grant");
DEFINE_string(scheduler_class_weights, "8:4:1",
              "Scheduler class weights in HIGH:MEDIUM:LOW order");
DEFINE_bool(enable_offload, false, "Enable offload availability");
DEFINE_bool(start_offload_rpc_server, true,
            "Expose TCP RPC for disk-tier reads "
            "(batch_get_offload_object / release_offload_buffer). "
            "Effective only when --enable_offload is true. "
            "Disable for a write-only owner.");
DEFINE_int32(offload_rpc_thread_num, 8,
             "Number of threads for the offload RPC server. "
             "Effective only when --enable_offload and "
             "--start_offload_rpc_server are true.");
DECLARE_bool(enable_http_server);
DECLARE_int32(http_port);

namespace {

std::array<uint32_t, 3> parseClassWeights() {
    const std::string_view text = FLAGS_scheduler_class_weights;
    const size_t first_separator = text.find(':');
    const size_t second_separator =
        first_separator == std::string_view::npos
            ? std::string_view::npos
            : text.find(':', first_separator + 1);
    LOG_ASSERT(first_separator != std::string_view::npos &&
               second_separator != std::string_view::npos &&
               text.find(':', second_separator + 1) == std::string_view::npos)
        << "--scheduler_class_weights must be HIGH:MEDIUM:LOW, for example "
           "8:4:1";

    const std::array<std::string_view, 3> tokens = {
        text.substr(0, first_separator),
        text.substr(first_separator + 1,
                    second_separator - first_separator - 1),
        text.substr(second_separator + 1),
    };
    std::array<uint32_t, 3> weights{};
    for (size_t i = 0; i < tokens.size(); ++i) {
        const auto value = mooncake::parseFromString<uint32_t>(tokens[i]);
        LOG_ASSERT(value.has_value() && *value > 0)
            << "--scheduler_class_weights entries must be positive uint32 "
               "values";
        weights[i] = *value;
    }
    return weights;
}

}  // namespace

namespace mooncake {
void RegisterClientRpcService(coro_rpc::coro_rpc_server &server,
                              RealClient &real_client) {
    server.register_handler<&RealClient::put_dummy_helper>(&real_client);
    server.register_handler<&RealClient::put_batch_dummy_helper>(&real_client);
    server.register_handler<&RealClient::put_parts_dummy_helper>(&real_client);
    server.register_handler<&RealClient::remove_internal>(&real_client);
    server.register_handler<&RealClient::removeByRegex_internal>(&real_client);
    server.register_handler<&RealClient::removeAll_internal>(&real_client);
    server.register_handler<&RealClient::batchRemove_internal>(&real_client);
    server.register_handler<&RealClient::isExist_internal>(&real_client);
    server.register_handler<&RealClient::batchIsExist_internal>(&real_client);
    server.register_handler<&RealClient::getSize_internal>(&real_client);
    server.register_handler<&RealClient::batch_put_from_dummy_helper>(
        &real_client);
    server.register_handler<
        &RealClient::batch_put_from_multi_buffers_dummy_helper>(&real_client);
    server.register_handler<&RealClient::upsert_dummy_helper>(&real_client);
    server.register_handler<&RealClient::upsert_from_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::upsert_parts_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::batch_upsert_from_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::upsert_batch_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::batch_get_into_dummy_helper>(
        &real_client);
    server.register_handler<
        &RealClient::batch_get_into_multi_buffers_dummy_helper>(&real_client);
    server.register_handler<&RealClient::get_into_range_shm_helper>(
        &real_client);
    server.register_handler<&RealClient::get_into_ranges_shm_helper>(
        &real_client);
    server.register_handler<&RealClient::map_shm_internal>(&real_client);
    server.register_handler<&RealClient::ascend_shm_internal>(&real_client);
    server.register_handler<&RealClient::ascend_ipc_shm_internal>(&real_client);
    server.register_handler<&RealClient::ascend_unmap_shm_internal>(
        &real_client);
    server.register_handler<&RealClient::is_shm_mapped_internal>(&real_client);
    server.register_handler<&RealClient::unmap_shm_internal>(&real_client);
    server.register_handler<&RealClient::unregister_shm_buffer_internal>(
        &real_client);
    server.register_handler<&RealClient::service_ready_internal>(&real_client);
    server.register_handler<&RealClient::ping>(&real_client);
    server.register_handler<&RealClient::acquire_hot_cache>(&real_client);
    server.register_handler<&RealClient::release_hot_cache>(&real_client);
    server.register_handler<&RealClient::batch_acquire_hot_cache>(&real_client);
    server.register_handler<&RealClient::batch_release_hot_cache>(&real_client);
    server.register_handler<&RealClient::acquire_buffer_dummy>(&real_client);
    server.register_handler<&RealClient::release_buffer_dummy>(&real_client);
    server.register_handler<&RealClient::batch_acquire_buffer_dummy>(
        &real_client);
    server.register_handler<&RealClient::allocate_buffer_dummy>(&real_client);
    server.register_handler<&RealClient::create_copy_task>(&real_client);
    server.register_handler<&RealClient::create_move_task>(&real_client);
    server.register_handler<&RealClient::query_task>(&real_client);
    server.register_handler<&RealClient::batch_get_offload_object>(
        &real_client);
    server.register_handler<&RealClient::batch_get_offload_object_push>(
        &real_client);
    server.register_handler<&RealClient::release_offload_buffer>(&real_client);
}
}  // namespace mooncake

int main(int argc, char *argv[]) {
    // Attention !!!
    // Initialization of ResourceTracker must be the most earliest.
    // Otherwise, the main thread will not apply signal mask before other
    // spawning threads, leading to missing signal processing.
    mooncake::ResourceTracker::getInstance();

    gflags::ParseCommandLineFlags(&argc, &argv, true);
    // Guard against double init: globalConfig() (transfer engine) may already
    // have called InitGoogleLogging and populated FLAGS_log_dir from
    // MC_LOG_DIR.
    if (!FLAGS_log_dir.empty() && !google::IsGoogleLoggingInitialized()) {
        google::InitGoogleLogging(argv[0]);
    }
    mooncake::logging::ApplyMooncakeLogEnableToGlog();

    size_t global_segment_size = string_to_byte_size(FLAGS_global_segment_size);
    size_t local_buffer_size = string_to_byte_size(FLAGS_local_buffer_size);
#ifdef USE_ASCEND_DIRECT
    // just set to true, does not affect GPU process.
    globalConfig().ascend_agent_mode = true;
#endif

    auto client_inst = RealClient::create();
    const size_t offload_rpc_thread_num =
        static_cast<size_t>(std::max(1, FLAGS_offload_rpc_thread_num));
    if (FLAGS_offload_rpc_thread_num <= 0) {
        LOG(WARNING) << "Invalid --offload_rpc_thread_num="
                     << FLAGS_offload_rpc_thread_num << ", using 1";
    }
    auto res = client_inst->setup_internal(
        FLAGS_host, FLAGS_metadata_server, global_segment_size,
        local_buffer_size, FLAGS_protocol, FLAGS_device_names,
        FLAGS_master_server_address, nullptr,
        "@mooncake_client_" + std::to_string(FLAGS_port) + ".sock", FLAGS_port,
        FLAGS_enable_offload, FLAGS_start_offload_rpc_server, "",
        FLAGS_tenant_id, FLAGS_enable_http_server, FLAGS_http_port, offload_rpc_thread_num);
    if (!res) {
        LOG(FATAL) << "Failed to setup client: " << toString(res.error());
        return -1;
    }

    if (FLAGS_scheduling) {
        mooncake::scheduling::SchedulerConfig scheduler_config;
        scheduler_config.quantum_bytes = FLAGS_scheduler_quantum_bytes;
        scheduler_config.max_inflight_bytes =
            FLAGS_scheduler_max_inflight_bytes;
        scheduler_config.reserved_high_bytes =
            FLAGS_scheduler_reserved_high_bytes;
        scheduler_config.max_slices = FLAGS_scheduler_max_slices;
        scheduler_config.class_weights = parseClassWeights();
        const auto status =
            client_inst->configureScheduling(scheduler_config);
        if (!status.ok()) {
            LOG(ERROR) << "Failed to configure Transfer Engine scheduling: "
                       << status.ToString();
            return -1;
        }
        LOG(INFO) << "Transfer Engine scheduling enabled with class weights "
                  << scheduler_config.class_weights[0] << ':'
                  << scheduler_config.class_weights[1] << ':'
                  << scheduler_config.class_weights[2]
                  << ", quantum_bytes=" << scheduler_config.quantum_bytes
                  << ", max_inflight_bytes="
                  << scheduler_config.max_inflight_bytes
                  << ", reserved_high_bytes="
                  << scheduler_config.reserved_high_bytes
                  << ", max_slices=" << scheduler_config.max_slices;
    }

    if (client_inst->start_dummy_client_monitor()) {
        LOG(FATAL) << "Failed to start dummy client monitor thread";
        return -1;
    }

    auto rpc_bind_host = getHostNameWithoutPort(FLAGS_host);
    coro_rpc::coro_rpc_server server(FLAGS_threads, FLAGS_port, rpc_bind_host);
    RegisterClientRpcService(server, *client_inst);

    LOG(INFO) << "Starting real client service on " << rpc_bind_host << ":"
              << FLAGS_port;

    return server.start();
}
