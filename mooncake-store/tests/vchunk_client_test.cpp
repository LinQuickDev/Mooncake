#include "vchunk_client.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace mooncake {
namespace {

Segment MakeSegment(const std::string& name, uintptr_t base) {
    Segment segment;
    segment.name = name;
    segment.base = reinterpret_cast<void*>(base);
    segment.size = 32U * 1024U * 1024U;
    segment.protocol = "tcp";
    segment.te_endpoint = name;
    return segment;
}

class MemoryDataPlane final : public VChunkDataPlane {
   public:
    ErrorCode Write(const VChunkMetadataRecord& record, const void* source,
                    size_t length,
                    std::chrono::steady_clock::time_point deadline) override {
        if (fail_write || std::chrono::steady_clock::now() >= deadline) {
            return fail_write ? ErrorCode::TRANSFER_FAIL
                              : ErrorCode::RPC_TIMEOUT;
        }
        if (!source || record.total_size != length) {
            return ErrorCode::INVALID_PARAMS;
        }
        std::vector<std::vector<uint8_t>> slices;
        size_t offset = 0;
        for (const auto& slice : record.slices) {
            if (slice.slice_index != slices.size() ||
                slice.logical_length > length - offset) {
                return ErrorCode::INVALID_PARAMS;
            }
            const auto* begin = static_cast<const uint8_t*>(source) + offset;
            slices.emplace_back(begin, begin + slice.logical_length);
            offset += slice.logical_length;
        }
        if (offset != length) {
            return ErrorCode::INVALID_PARAMS;
        }
        std::lock_guard<std::mutex> guard(mutex);
        objects[record.vchunk_id] = std::move(slices);
        return ErrorCode::OK;
    }

    ErrorCode Read(const VChunkMetadataRecord& record, void* destination,
                   size_t length,
                   std::chrono::steady_clock::time_point deadline) override {
        if (fail_read || std::chrono::steady_clock::now() >= deadline) {
            return fail_read ? ErrorCode::TRANSFER_FAIL
                             : ErrorCode::RPC_TIMEOUT;
        }
        if (block_reads) {
            std::unique_lock<std::mutex> guard(sync_mutex);
            read_entered = true;
            sync_cv.notify_all();
            sync_cv.wait(guard, [&] { return release_read; });
        }
        std::lock_guard<std::mutex> guard(mutex);
        const auto it = objects.find(record.vchunk_id);
        if (it == objects.end()) {
            return ErrorCode::OBJECT_NOT_FOUND;
        }
        size_t offset = 0;
        for (size_t i = 0; i < it->second.size(); ++i) {
            if (record.slices[i].slice_index != i ||
                it->second[i].size() > length - offset) {
                return ErrorCode::TRANSFER_FAIL;
            }
            std::memcpy(static_cast<uint8_t*>(destination) + offset,
                        it->second[i].data(), it->second[i].size());
            offset += it->second[i].size();
        }
        return offset == length ? ErrorCode::OK : ErrorCode::TRANSFER_FAIL;
    }

    bool fail_write{false};
    bool fail_read{false};
    bool block_reads{false};
    bool read_entered{false};
    bool release_read{false};
    std::mutex sync_mutex;
    std::condition_variable sync_cv;
    std::mutex mutex;
    std::unordered_map<std::string, std::vector<std::vector<uint8_t>>> objects;
};

class LegacySpy final : public VChunkLegacyPath {
   public:
    ErrorCode Put(const TenantId&, const std::string&, const void*,
                  size_t) override {
        ++puts;
        return ErrorCode::OK;
    }
    ErrorCode Get(const TenantId&, const std::string&, void*, size_t) override {
        ++gets;
        return ErrorCode::OK;
    }
    ErrorCode Remove(const TenantId&, const std::string&) override {
        ++removes;
        return ErrorCode::OK;
    }
    int puts{0};
    int gets{0};
    int removes{0};
};

struct ClientFixture : testing::Test {
    ClientFixture() : service(MakeConfig()) {
        const auto client_id = generate_uuid();
        EXPECT_TRUE(service.MountSegment(
                               MakeSegment("vchunk-a", 0xB00000000ULL),
                               client_id)
                        .has_value());
        EXPECT_TRUE(service.MountSegment(
                               MakeSegment("vchunk-b", 0xC00000000ULL),
                               client_id)
                        .has_value());
    }

    static MasterServiceConfig MakeConfig() {
        MasterServiceConfig config;
        config.memory_allocator = BufferAllocatorType::OFFSET;
        config.vchunk_config.enabled = true;
        return config;
    }

    int64_t now{100};
    MasterService service;
    MemoryDataPlane data;
    LegacySpy legacy;
};

TEST_F(ClientFixture, PutGetRemoveRoundTripForPiercingSizes) {
    const std::array<size_t, 6> sizes{4096, 64U * 1024U, 256U * 1024U,
                                      1024U * 1024U, 4U * 1024U * 1024U,
                                      1024U * 1024U + 17U};
    VChunkClient client(true, service, data, legacy,
                        std::chrono::seconds(1), [this] { return ++now; });
    std::mt19937 random(7);
    for (const auto size : sizes) {
        std::vector<uint8_t> source(size);
        std::generate(source.begin(), source.end(), [&] { return random(); });
        std::vector<uint8_t> destination(size, 0);
        const auto key = "key-" + std::to_string(size);
        ASSERT_EQ(client.Put(TenantId("tenant"), key, source.data(), size),
                  ErrorCode::OK);
        ASSERT_EQ(client.Get(TenantId("tenant"), key, destination.data(), size),
                  ErrorCode::OK);
        EXPECT_EQ(destination, source);
        EXPECT_EQ(client.Remove(TenantId("tenant"), key), ErrorCode::OK);
        EXPECT_EQ(client.Remove(TenantId("tenant"), key), ErrorCode::OK);
        EXPECT_EQ(client.Get(TenantId("tenant"), key, destination.data(), size),
                  ErrorCode::OBJECT_NOT_FOUND);
    }
}

TEST_F(ClientFixture, FailedWriteRevokesCreatingObject) {
    VChunkClient client(true, service, data, legacy,
                        std::chrono::seconds(1), [this] { return ++now; });
    std::vector<uint8_t> source(8192, 1);
    data.fail_write = true;
    EXPECT_EQ(client.Put(TenantId("tenant"), "key", source.data(),
                         source.size()),
              ErrorCode::TRANSFER_FAIL);
    EXPECT_EQ(service.GetVChunk(TenantId("tenant"), "key").error(),
              ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(ClientFixture, FailedReadDoesNotReturnPartialSuccess) {
    VChunkClient client(true, service, data, legacy,
                        std::chrono::seconds(1), [this] { return ++now; });
    std::vector<uint8_t> source(8192, 3);
    ASSERT_EQ(client.Put(TenantId("tenant"), "key", source.data(),
                         source.size()),
              ErrorCode::OK);
    std::vector<uint8_t> destination(source.size(), 0);
    data.fail_read = true;
    EXPECT_EQ(client.Get(TenantId("tenant"), "key", destination.data(),
                         destination.size()),
              ErrorCode::TRANSFER_FAIL);
}

TEST_F(ClientFixture, DisabledVChunkUsesOnlyLegacyPath) {
    VChunkClient client(false, service, data, legacy,
                        std::chrono::seconds(1), [this] { return ++now; });
    uint8_t value = 1;
    EXPECT_EQ(client.Put(TenantId("tenant"), "key", &value, 1), ErrorCode::OK);
    EXPECT_EQ(client.Get(TenantId("tenant"), "key", &value, 1), ErrorCode::OK);
    EXPECT_EQ(client.Remove(TenantId("tenant"), "key"), ErrorCode::OK);
    EXPECT_EQ(legacy.puts, 1);
    EXPECT_EQ(legacy.gets, 1);
    EXPECT_EQ(legacy.removes, 1);
    EXPECT_EQ(service.GetVChunk(TenantId("tenant"), "key").error(),
              ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(ClientFixture, InflightGetCompletesWhileRemoveBlocksNewReads) {
    VChunkClient client(true, service, data, legacy,
                        std::chrono::seconds(1), [this] { return ++now; });
    std::vector<uint8_t> source(8192, 9);
    ASSERT_EQ(client.Put(TenantId("tenant"), "key", source.data(),
                         source.size()),
              ErrorCode::OK);
    data.block_reads = true;
    std::vector<uint8_t> destination(source.size(), 0);
    ErrorCode read_result = ErrorCode::INTERNAL_ERROR;
    std::thread reader([&] {
        read_result = client.Get(TenantId("tenant"), "key", destination.data(),
                                 destination.size());
    });
    {
        std::unique_lock<std::mutex> guard(data.sync_mutex);
        data.sync_cv.wait(guard, [&] { return data.read_entered; });
    }

    EXPECT_EQ(client.Remove(TenantId("tenant"), "key"), ErrorCode::OK);
    std::vector<uint8_t> second(source.size(), 0);
    EXPECT_EQ(client.Get(TenantId("tenant"), "key", second.data(),
                         second.size()),
              ErrorCode::OBJECT_NOT_FOUND);
    {
        std::lock_guard<std::mutex> guard(data.sync_mutex);
        data.release_read = true;
    }
    data.sync_cv.notify_all();
    reader.join();
    EXPECT_EQ(read_result, ErrorCode::OK);
    EXPECT_EQ(destination, source);
}

}  // namespace
}  // namespace mooncake
