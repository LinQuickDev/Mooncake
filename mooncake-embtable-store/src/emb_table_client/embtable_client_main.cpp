#include <algorithm>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "emb_table_client/emb_table_client.h"
#include "real_client.h"
#include "utils.h"

DEFINE_string(embtable_local_hostname, "",
              "Local hostname used for bucket locality detection");
DEFINE_string(embtable_metadata_server, "http://127.0.0.1:8080/metadata",
              "Transfer Engine metadata server");
DEFINE_string(embtable_master_address, "127.0.0.1:50051",
              "Mooncake Store master address");
DEFINE_string(embtable_protocol, "tcp", "Transfer protocol");
DEFINE_string(embtable_device_names, "", "Transfer device names");
DEFINE_string(embtable_global_segment_size, "4 GB",
              "Mooncake Store global segment size");
DEFINE_string(embtable_local_buffer_size, "16 MB",
              "Mooncake Store local buffer size");
DEFINE_uint32(embtable_rpc_port, 50055,
              "EmbTable and ShareMapStore RPC service port");
DEFINE_uint32(embtable_rpc_threads, 4, "EmbTable RPC worker threads");
DEFINE_string(embtable_transfer_buffer_size, "64 MB",
              "Registered RPC data-plane transfer buffer size");
DEFINE_string(embtable_share_object_size, "64 MB", "Default ShareObject size");

int main(int argc, char* argv[]) {
    mooncake::ResourceTracker::getInstance();
    gflags::SetUsageMessage(
        "Run a multi-table EmbTable storage-node RPC service");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    if (FLAGS_embtable_rpc_port == 0 || FLAGS_embtable_rpc_port > UINT16_MAX) {
        LOG(ERROR) << "--embtable_rpc_port must be in [1, 65535]";
        return 1;
    }

    embtable::EmbTableClient::Options options;
    options.createNew = false;
    options.localHostname = FLAGS_embtable_local_hostname;
    options.rpcThreads = FLAGS_embtable_rpc_threads;
    options.deployment.masterAddress = FLAGS_embtable_master_address;
    options.deployment.metadataServer = FLAGS_embtable_metadata_server;
    options.deployment.protocol = FLAGS_embtable_protocol;
    options.deployment.deviceNames = FLAGS_embtable_device_names;
    options.deployment.globalSegmentSize =
        mooncake::string_to_byte_size(FLAGS_embtable_global_segment_size);
    options.deployment.localBufferSize =
        mooncake::string_to_byte_size(FLAGS_embtable_local_buffer_size);
    options.deployment.rpcPort = static_cast<uint16_t>(FLAGS_embtable_rpc_port);
    options.deployment.enableEmbTableRpc = true;
    options.deployment.transferBufferSize =
        mooncake::string_to_byte_size(FLAGS_embtable_transfer_buffer_size);
    options.deployment.shareObjectSize =
        mooncake::string_to_byte_size(FLAGS_embtable_share_object_size);

    // Every ShareObject is published as one Mooncake Store object. Reject an
    // impossible configuration at startup instead of failing later during
    // BuildIndex with an opaque put_from/insufficient-space error.
    constexpr uint64_t kIndexObjectSize = 16ull * 1024 * 1024;
    const uint64_t largestObjectSize =
        std::max(options.deployment.shareObjectSize, kIndexObjectSize);
    if (options.deployment.globalSegmentSize < largestObjectSize) {
        LOG(ERROR) << "--embtable_global_segment_size ("
                   << options.deployment.globalSegmentSize
                   << " bytes) must be at least the largest EmbTable object ("
                   << largestObjectSize
                   << " bytes); increase global segment size or reduce "
                      "--embtable_share_object_size";
        return 1;
    }

    embtable::EmbTableClient client(std::move(options));
    auto status = client.Init();
    if (!status.IsOk()) {
        LOG(ERROR) << "EmbTableClient initialization failed: " << status.msg();
        return 1;
    }

    LOG(INFO) << "EmbTable storage node is ready for multi-table DDL/data RPC";
    std::mutex mutex;
    std::condition_variable condition;
    std::unique_lock<std::mutex> lock(mutex);
    condition.wait(lock);
    return 0;
}
