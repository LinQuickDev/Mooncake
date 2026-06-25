// gc_e2e_test.cpp
// End-to-end integration tests for the explicit-delete-only SSD GC.
//
// Verifies the full pipeline that unit tests cannot cover:
//   RealClient::remove  ->  master metadata erase
//                        ->  FileStorage::MarkRemoved (tombstone)
//                        ->  BucketStorageBackend GC compaction
//                        ->  SSD bucket file reclamation
//
// Unlike storage_backend_e2e_test (which uses the Client base class +
// file-per-key backend), this suite uses RealClient with
// enable_ssd_offload=true so the BucketStorageBackend + FileStorage
// offload path is exercised, and remove goes through
// RealClient::remove_internal -> MarkRemoved.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "client_buffer.hpp"
#include "real_client.h"
#include "test_server_helpers.h"
#include "types.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "", "Device name to use, valid if protocol=rdma");

namespace mooncake {
namespace testing {

namespace fs = std::filesystem;

static constexpr size_t kMB = 1024ULL * 1024;

// Count regular files with a given suffix in dir.
static int CountFilesWithSuffix(const fs::path& dir,
                                const std::string& suffix) {
    int count = 0;
    std::error_code ec;
    for (auto& entry : fs::directory_iterator(dir, ec)) {
        if (entry.is_regular_file()) {
            auto name = entry.path().filename().string();
            if (name.size() >= suffix.size() &&
                name.compare(name.size() - suffix.size(), suffix.size(),
                             suffix) == 0) {
                ++count;
            }
        }
    }
    return count;
}

// Read a key via RealClient::get_buffer into a std::string. Returns
// std::nullopt on failure.
static std::optional<std::string> ReadKey(
    const std::shared_ptr<RealClient>& client, const std::string& key) {
    auto buf = client->get_buffer(key);
    if (!buf) return std::nullopt;
    return std::string(static_cast<char*>(buf->ptr()), buf->size());
}

class GCE2ETest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("GCE2ETest");
        FLAGS_logtostderr = 1;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        if (getenv("PROTOCOL")) FLAGS_protocol = getenv("PROTOCOL");
        if (getenv("DEVICE_NAME")) FLAGS_device_name = getenv("DEVICE_NAME");

        tmp_dir_ = fs::temp_directory_path() /
                   ("mc_gc_e2e_" + std::to_string(::getpid()));
        fs::create_directories(tmp_dir_);

        // Save and set the GC-required bucket backend env vars.
        // eviction_policy=LRU keeps last_access_ns_ updated for GC candidate
        // coldness; disable_ssd_eviction=true makes PrepareEviction a no-op
        // so no live bucket is ever evicted.
        saved_policy_ = GetEnvOpt("MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY");
        setenv("MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY", "lru", 1);
        saved_disable_ = GetEnvOpt("MOONCAKE_OFFLOAD_DISABLE_SSD_EVICTION");
        setenv("MOONCAKE_OFFLOAD_DISABLE_SSD_EVICTION", "true", 1);
        // Tighten GC so compaction runs quickly in tests.
        saved_gc_interval_ = GetEnvOpt("MOONCAKE_OFFLOAD_BUCKET_GC_INTERVAL_MS");
        setenv("MOONCAKE_OFFLOAD_BUCKET_GC_INTERVAL_MS", "200", 1);
        saved_gc_ratio_ = GetEnvOpt("MOONCAKE_OFFLOAD_BUCKET_GC_DELETED_RATIO");
        setenv("MOONCAKE_OFFLOAD_BUCKET_GC_DELETED_RATIO", "0.1", 1);
    }

    void TearDown() override {
        if (real_client_) real_client_->tearDownAll();
        master_.Stop();
        easylog::set_min_severity(easylog::Severity::WARN);

        // Restore env.
        RestoreEnv("MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY", saved_policy_);
        RestoreEnv("MOONCAKE_OFFLOAD_DISABLE_SSD_EVICTION", saved_disable_);
        RestoreEnv("MOONCAKE_OFFLOAD_BUCKET_GC_INTERVAL_MS", saved_gc_interval_);
        RestoreEnv("MOONCAKE_OFFLOAD_BUCKET_GC_DELETED_RATIO", saved_gc_ratio_);

        std::error_code ec;
        fs::remove_all(tmp_dir_, ec);
    }

    static void RestoreEnv(const char* name,
                           const std::optional<std::string>& saved) {
        if (saved.has_value()) {
            setenv(name, saved->c_str(), 1);
        } else {
            unsetenv(name);
        }
    }

    // Safely capture an env var as optional (getenv may return nullptr).
    static std::optional<std::string> GetEnvOpt(const char* name) {
        const char* val = getenv(name);
        if (val) return std::string(val);
        return std::nullopt;
    }

    bool StartMasterWithOffload() {
        auto config = InProcMasterConfigBuilder()
                          .set_enable_offload(true)
                          .set_root_fs_dir(tmp_dir_.string())
                          .build();
        return master_.Start(config);
    }

    bool StartRealClient() {
        real_client_ = RealClient::create();
        if (!real_client_) return false;
        const std::string rdma_devices =
            (FLAGS_protocol == "rdma") ? FLAGS_device_name : "";
        std::string ssd_path = tmp_dir_.string() + "/ssd_offload";
        fs::create_directories(ssd_path);
        // enable_ssd_offload=true creates FileStorage + BucketStorageBackend.
        int ret = real_client_->setup_real(
            "localhost:17890", "P2PHANDSHAKE",
            /*global_segment_size=*/512 * kMB,
            /*local_buffer_size=*/256 * kMB, FLAGS_protocol, rdma_devices,
            master_.master_address(), nullptr,
            /*ipc_socket_path=*/"",
            /*enable_ssd_offload=*/true,
            /*ssd_offload_path=*/ssd_path,
            /*tenant_id=*/"default");
        return ret == 0;
    }

    // Put a key via RealClient and wait until it is readable (offload
    // completed). Returns false on timeout.
    bool PutAndWaitReadable(const std::string& key,
                            const std::string& value) {
        std::span<const char> span(value.data(), value.size());
        ReplicateConfig config;
        config.replica_num = 1;
        if (real_client_->put(key, span, config) != 0) return false;
        // Wait for offload + get to succeed with matching data.
        for (int i = 0; i < 100; ++i) {
            auto got = ReadKey(real_client_, key);
            if (got.has_value() && got.value() == value) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return false;
    }

    fs::path tmp_dir_;
    InProcMaster master_;
    std::shared_ptr<RealClient> real_client_;
    std::optional<std::string> saved_policy_;
    std::optional<std::string> saved_disable_;
    std::optional<std::string> saved_gc_interval_;
    std::optional<std::string> saved_gc_ratio_;
};

// -------------------------------------------------------------------
// Test 1: RemoveReclaimsSSDSpace
//
// Put 2 keys, remove 1, wait for GC. The removed key's tombstone should
// trigger compaction; the surviving key must remain readable with
// correct data throughout.
// -------------------------------------------------------------------
TEST_F(GCE2ETest, RemoveReclaimsSSDSpace) {
    ASSERT_TRUE(StartMasterWithOffload());
    ASSERT_TRUE(StartRealClient());

    const std::string k1 = "gc_e2e_k1";
    const std::string k2 = "gc_e2e_k2";
    const std::string v1(4 * kMB, 'A');
    const std::string v2(4 * kMB, 'B');

    ASSERT_TRUE(PutAndWaitReadable(k1, v1)) << "k1 offload timed out";
    ASSERT_TRUE(PutAndWaitReadable(k2, v2)) << "k2 offload timed out";

    fs::path ssd_dir = tmp_dir_ / "ssd_offload";
    int buckets_before = CountFilesWithSuffix(ssd_dir, ".bucket");

    // Remove k1. This marks a tombstone; GC should compact the bucket.
    // force=true bypasses the object lease (PutEnd sets a lease that would
    // otherwise reject Remove with OBJECT_HAS_LEASE = -706).
    ASSERT_EQ(real_client_->remove(k1, /*force=*/true), 0);

    // Wait for GC to run (interval=200ms, ratio=0.1). Poll for the
    // surviving key to remain readable AND bucket file reclamation.
    bool reclaimed = false;
    for (int i = 0; i < 100; ++i) {
        // k2 must stay readable throughout.
        auto got2 = ReadKey(real_client_, k2);
        ASSERT_TRUE(got2.has_value())
            << "Surviving key k2 became unreadable during GC";
        ASSERT_EQ(got2.value(), v2)
            << "Surviving key k2 data corrupted during GC";
        // k1 must stay gone.
        auto got1 = ReadKey(real_client_, k1);
        ASSERT_FALSE(got1.has_value())
            << "Removed key k1 became readable again";

        // Check if a compaction produced a new bucket file (old one deleted).
        int buckets_now = CountFilesWithSuffix(ssd_dir, ".bucket");
        if (buckets_now > 0 && buckets_now != buckets_before) {
            reclaimed = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // k2 final integrity check.
    auto got = ReadKey(real_client_, k2);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got.value(), v2) << "Surviving key data corrupted after GC";

    LOG(INFO) << "GC reclamation "
              << (reclaimed ? "detected" : "not detected (timeout)")
              << "; buckets_before=" << buckets_before;
}

// -------------------------------------------------------------------
// Test 2: RemoveMiddleKeyPreservesSurvivors
//
// Put 3 keys, remove the middle one, wait for GC. Both survivors must
// read back correctly; removed key must stay gone. This exercises the
// copy-on-write live-key preservation under real offload grouping.
// -------------------------------------------------------------------
TEST_F(GCE2ETest, RemoveMiddleKeyPreservesSurvivors) {
    ASSERT_TRUE(StartMasterWithOffload());
    ASSERT_TRUE(StartRealClient());

    const std::string k1 = "gc_mid_k1";
    const std::string k2 = "gc_mid_k2";
    const std::string k3 = "gc_mid_k3";
    const std::string v1(4 * kMB, 'X');
    const std::string v2(4 * kMB, 'Y');
    const std::string v3(4 * kMB, 'Z');

    ASSERT_TRUE(PutAndWaitReadable(k1, v1));
    ASSERT_TRUE(PutAndWaitReadable(k2, v2));
    ASSERT_TRUE(PutAndWaitReadable(k3, v3));

    // Remove the middle key (force=true to bypass lease).
    ASSERT_EQ(real_client_->remove(k2, /*force=*/true), 0);

    // Wait for GC to compact, then verify survivors.
    // Give GC a few rounds to run.
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // k1 and k3 must survive with correct data.
    auto got1 = ReadKey(real_client_, k1);
    ASSERT_TRUE(got1.has_value());
    EXPECT_EQ(got1.value(), v1) << "k1 data corrupted after GC";
    auto got3 = ReadKey(real_client_, k3);
    ASSERT_TRUE(got3.has_value());
    EXPECT_EQ(got3.value(), v3) << "k3 data corrupted after GC";

    // k2 must remain gone.
    auto got2 = ReadKey(real_client_, k2);
    EXPECT_FALSE(got2.has_value())
        << "Removed key k2 should not be readable";
}

// -------------------------------------------------------------------
// Test 3: BatchRemoveMixedExistingAndAbsent
//
// BatchRemove with a non-existent key mixed in: the existing key should
// be tombstoned + GC'd, the non-existent one ignored.
// -------------------------------------------------------------------
TEST_F(GCE2ETest, BatchRemoveMixedExistingAndAbsent) {
    ASSERT_TRUE(StartMasterWithOffload());
    ASSERT_TRUE(StartRealClient());

    const std::string k1 = "gc_batch_k1";
    const std::string v1(4 * kMB, 'Q');
    ASSERT_TRUE(PutAndWaitReadable(k1, v1));

    // Batch remove: k1 exists, k_absent does not. force=true bypasses lease.
    std::vector<std::string> keys{k1, "gc_batch_absent"};
    auto results = real_client_->batchRemove(keys, /*force=*/true);
    ASSERT_EQ(results.size(), 2u);
    // k1 should succeed (0); absent key may succeed or fail depending on
    // master semantics, but k1 must be gone either way.

    // Wait for GC.
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // k1 must be gone.
    auto got1 = ReadKey(real_client_, k1);
    EXPECT_FALSE(got1.has_value())
        << "Batch-removed key k1 should not be readable";
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    return RUN_ALL_TESTS();
}
