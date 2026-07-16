#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "emb_table_client/emb_table_client.h"
#include "real_client.h"
#include "utils.h"

DEFINE_string(embtable_table_name, "", "Embedding table name");
DEFINE_uint32(embtable_num_buckets, 16, "Number of embedding buckets");
DEFINE_uint64(embtable_value_size, 0, "Embedding value size in bytes");
DEFINE_bool(embtable_create_new, true, "Create table metadata on startup");
DEFINE_string(embtable_local_hostname, "",
              "Local hostname used for bucket locality detection");
DEFINE_string(embtable_metadata_server,
              "http://127.0.0.1:8080/metadata",
              "Transfer Engine metadata server");
DEFINE_string(embtable_master_address, "127.0.0.1:50051",
              "Mooncake Store master address");
DEFINE_string(embtable_protocol, "tcp", "Transfer protocol");
DEFINE_string(embtable_device_names, "", "Transfer device names");
DEFINE_uint32(embtable_rpc_port, 50055, "ShareMapStore RPC service port");
DEFINE_uint32(embtable_rpc_threads, 4, "ShareMapStore RPC worker threads");
DEFINE_string(embtable_transfer_buffer_size, "64 MB",
              "Registered RPC data-plane transfer buffer size");
DEFINE_string(embtable_share_object_size, "64 MB",
              "Default ShareObject size");

int main(int argc, char* argv[]) {
    mooncake::ResourceTracker::getInstance();
    gflags::SetUsageMessage(
        "Run a co-process EmbTable and ShareMapStore RPC service");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    if (FLAGS_embtable_table_name.empty() ||
        FLAGS_embtable_value_size == 0) {
        LOG(ERROR) << "--embtable_table_name and --embtable_value_size are "
                      "required";
        return 1;
    }
    if (FLAGS_embtable_rpc_port > UINT16_MAX) {
        LOG(ERROR) << "--embtable_rpc_port exceeds uint16 range";
        return 1;
    }

    embtable::EmbTableClient::Options options;
    options.tableName = FLAGS_embtable_table_name;
    options.numBuckets = FLAGS_embtable_num_buckets;
    options.valueSize = FLAGS_embtable_value_size;
    options.createNew = FLAGS_embtable_create_new;
    options.localHostname = FLAGS_embtable_local_hostname;
    options.rpcThreads = FLAGS_embtable_rpc_threads;
    options.deployment.masterAddress = FLAGS_embtable_master_address;
    options.deployment.metadataServer = FLAGS_embtable_metadata_server;
    options.deployment.protocol = FLAGS_embtable_protocol;
    options.deployment.deviceNames = FLAGS_embtable_device_names;
    options.deployment.rpcPort =
        static_cast<uint16_t>(FLAGS_embtable_rpc_port);
    options.deployment.transferBufferSize =
        mooncake::string_to_byte_size(FLAGS_embtable_transfer_buffer_size);
    options.deployment.shareObjectSize =
        mooncake::string_to_byte_size(FLAGS_embtable_share_object_size);

    embtable::EmbTableClient client(std::move(options));
    auto status = client.Init();
    if (!status.IsOk()) {
        LOG(ERROR) << "EmbTableClient initialization failed: " << status.msg();
        return 1;
    }

    LOG(INFO) << "EmbTableClient is ready";
    std::mutex mutex;
    std::condition_variable condition;
    std::unique_lock<std::mutex> lock(mutex);
    condition.wait(lock);
    return 0;
}
