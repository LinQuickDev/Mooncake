#include "master_config.h"

#include <gtest/gtest.h>

namespace mooncake::test {

TEST(MasterServiceConfigTest, OplogBatchMaxEntriesDefaultsTo1024) {
    MasterConfig master_config;
    EXPECT_EQ(1024u, master_config.oplog_batch_max_entries);

    MasterServiceConfig service_config;
    EXPECT_EQ(1024u, service_config.oplog_batch_max_entries);
}

TEST(MasterServiceConfigTest, OplogIsDisabledByDefault) {
    MasterConfig master_config;
    EXPECT_FALSE(master_config.enable_oplog);

    MasterServiceConfig service_config;
    EXPECT_FALSE(service_config.enable_oplog);
}

TEST(MasterServiceConfigTest, OplogBuilderOverrideIsRespected) {
    auto config = MasterServiceConfig::builder().set_enable_oplog(true).build();

    EXPECT_TRUE(config.enable_oplog);
}

TEST(MasterServiceConfigTest, OplogEnablementPropagatesToServingConfig) {
    MasterConfig master_config{};
    master_config.enable_oplog = true;
    MasterServiceSupervisorConfig supervisor_config(master_config);

    WrappedMasterServiceConfig wrapped_config(supervisor_config, 1);
    MasterServiceConfig service_config(wrapped_config);

    EXPECT_TRUE(supervisor_config.enable_oplog);
    EXPECT_TRUE(wrapped_config.enable_oplog);
    EXPECT_TRUE(service_config.enable_oplog);
}

TEST(MasterServiceConfigTest, OplogBatchMaxEntriesBuilderOverrideRespected) {
    auto config =
        MasterServiceConfig::builder().set_oplog_batch_max_entries(17).build();

    EXPECT_EQ(17u, config.oplog_batch_max_entries);
}

TEST(MasterServiceConfigTest, IoPatternCfmPropagatesToServingConfig) {
    MasterConfig master_config{};
    master_config.io_pattern_cfm = {.endpoint = "cfm.example:50051",
                                    .node_id = "master-a",
                                    .auth_token = "secret",
                                    .producer_auth_token = "producer-secret",
                                    .timeout_ms = 750,
                                    .policy_queue_capacity = 32};

    MasterServiceSupervisorConfig supervisor_config(master_config);
    WrappedMasterServiceConfig wrapped_config(supervisor_config, 1);
    MasterServiceConfig service_config(wrapped_config);

    EXPECT_EQ(service_config.io_pattern_cfm.endpoint, "cfm.example:50051");
    EXPECT_EQ(service_config.io_pattern_cfm.node_id, "master-a");
    EXPECT_EQ(service_config.io_pattern_cfm.auth_token, "secret");
    EXPECT_EQ(service_config.io_pattern_cfm.producer_auth_token,
              "producer-secret");
    EXPECT_EQ(service_config.io_pattern_cfm.timeout_ms, 750);
    EXPECT_EQ(service_config.io_pattern_cfm.policy_queue_capacity, 32);
}

}  // namespace mooncake::test
