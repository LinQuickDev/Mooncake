#include "multi_transport.h"
#include "transfer_engine.h"

#include <gtest/gtest.h>

#include <string>

namespace mooncake {
namespace {

TEST(VChunkTransferEngineProbeTest, MultiSliceBatchWaitsThenCompletes) {
    std::string local_server_name = "vchunk-probe";
    MultiTransport transport(nullptr, local_server_name);
    const auto batch_id = transport.allocateBatchID(3);
    ASSERT_NE(batch_id, INVALID_BATCH_ID);

    auto& batch = Transport::toBatchDesc(batch_id);
    for (size_t i = 0; i < 3; ++i) {
        batch.task_list.emplace_back();
        auto& task = batch.task_list.back();
        task.batch_id = batch_id;
        task.slice_count = 1;
        task.total_bytes = 4096;
    }

    TransferStatus status;
    ASSERT_TRUE(transport.getBatchTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::WAITING);
    EXPECT_FALSE(transport.freeBatchID(batch_id).ok());

    for (auto& task : batch.task_list) {
        task.success_slice_count = 1;
        task.transferred_bytes = task.total_bytes;
    }

    ASSERT_TRUE(transport.getBatchTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 3U * 4096U);
    EXPECT_TRUE(transport.freeBatchID(batch_id).ok());
}

TEST(VChunkTransferEngineProbeTest, EmptyBatchIsImmediatelyComplete) {
    std::string local_server_name = "vchunk-probe";
    MultiTransport transport(nullptr, local_server_name);
    const auto batch_id = transport.allocateBatchID(1);
    ASSERT_NE(batch_id, INVALID_BATCH_ID);

    TransferStatus status;
    ASSERT_TRUE(transport.getBatchTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_TRUE(transport.freeBatchID(batch_id).ok());
}

}  // namespace
}  // namespace mooncake
