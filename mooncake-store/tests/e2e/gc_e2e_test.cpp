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

// List all .bucket file names (without directory) in dir.
static std::vector<std::string> ListBucketFiles(const fs::path& dir) {
    std::vector<std::string> names;
    std::error_code ec;
    for (auto& entry : fs::directory_iterator(dir, ec)) {
        if (entry.is_regular_file()) {
            auto name = entry.path().filename().string();
            if (name.size() >= 6 &&
                name.compare(name.size() - 6, 6, ".bucket") == 0) {
                names.push_back(name);
            }
        }
    }
    return names;
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
        // Tighten GC so compaction runs after remove. Set interval long
        // enough that GC doesn't fire during offload settlement (which
        // could compact a bucket before remove creates a tombstone).
        saved_gc_interval_ = GetEnvOpt("MOONCAKE_OFFLOAD_BUCKET_GC_INTERVAL_MS");
        setenv("MOONCAKE_OFFLOAD_BUCKET_GC_INTERVAL_MS", "15000", 1);
        saved_gc_ratio_ = GetEnvOpt("MOONCAKE_OFFLOAD_BUCKET_GC_DELETED_RATIO");
        setenv("MOONCAKE_OFFLOAD_BUCKET_GC_DELETED_RATIO", "0.01", 1);
        // Set bucket_keys_limit=1 so each offloaded key fills a bucket
        // immediately. This ensures .bucket files are written on the first
        // heartbeat after put. With limit=2, keys may sit in the ungrouped
        // pool and no .bucket file is written.
        saved_bucket_keys_limit_ =
            GetEnvOpt("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT");
        setenv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "1", 1);
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
        RestoreEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT",
                   saved_bucket_keys_limit_);

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
        // Match production config: enable_offload=true, no root_fs_dir
        // (master doesn't do disk caching; offload tasks are pushed to the
        // client's FileStorage via heartbeat). Set a long lease TTL so
        // objects aren't evicted before offload completes.
        auto config = InProcMasterConfigBuilder()
                          .set_enable_offload(true)
                          .set_default_kv_lease_ttl(300000)
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
        // Set MOONCAKE_OFFLOAD_FILE_STORAGE_PATH env var (same as production)
        // so FileStorageConfig::FromEnvironment picks it up. This matches the
        // production deployment pattern where the env var is set before
        // launching the client.
        setenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH", ssd_path.c_str(), 1);
        setenv("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR",
               "bucket_storage_backend", 1);
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

    // Put a key via RealClient and wait until it has been offloaded to the
    // BucketStorageBackend (a .bucket file appears on SSD). Returns false on
    // timeout. Waiting on memory reads is insufficient — offload is async
    // (PutEnd queues, heartbeat drains) and MarkRemoved is a no-op until the
    // key lands in object_bucket_map_.
    bool PutAndWaitOffloaded(const std::string& key,
                             const std::string& value,
                             const fs::path& ssd_dir) {
        std::span<const char> span(value.data(), value.size());
        ReplicateConfig config;
        config.replica_num = 1;
        if (real_client_->put(key, span, config) != 0) {
            return false;
        }
        // Wait for offload: a .bucket file must appear in ssd_dir, AND the
        // key must be readable via get_buffer (confirms data integrity).
        // Heartbeat interval is 10s, so wait up to 40s.
        for (int i = 0; i < 400; ++i) {  // up to 40s
            int buckets = CountFilesWithSuffix(ssd_dir, ".bucket");
            if (buckets > 0) {
                auto got = ReadKey(real_client_, key);
                if (got.has_value() && got.value() == value) return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return false;
    }

    // After all keys are put and individually confirmed offloaded, wait an
    // extra heartbeat cycle to ensure ALL keys have been drained from the
    // offloading queue into object_bucket_map_. Without this, a key put
    // after the first heartbeat may not yet be in object_bucket_map_ when
    // MarkRemoved is called, making the tombstone a no-op.
    void WaitForAllOffloadsSettled() {
        // Wait less than gc_interval_ms (15s) so GC doesn't fire during
        // settlement. Two heartbeat cycles (10s each) would be ideal, but
        // 12s is enough for the 2nd heartbeat to drain remaining tasks
        // while staying under the GC interval.
        std::this_thread::sleep_for(std::chrono::seconds(12));
    }

    // Put multiple keys, then wait until ALL are offloaded (a .bucket file
    // appears and each key is readable). Keys put before the next heartbeat
    // are grouped into the same bucket (up to bucket_keys_limit).
    bool PutBatchAndWaitOffloaded(
        const std::vector<std::pair<std::string, std::string>>& kvs,
        const fs::path& ssd_dir) {
        ReplicateConfig config;
        config.replica_num = 1;
        for (const auto& [key, value] : kvs) {
            std::span<const char> span(value.data(), value.size());
            if (real_client_->put(key, span, config) != 0) return false;
        }
        // Wait for offload of all keys: a .bucket file MUST appear (offload
        // completed) AND each key must be readable via get_buffer.
        // Heartbeat interval is 10s; with bucket_keys_limit=2, 2 keys fill
        // a bucket on the first heartbeat after put. Wait up to 40s.
        for (int i = 0; i < 400; ++i) {  // up to 40s
            if (CountFilesWithSuffix(ssd_dir, ".bucket") > 0) {
                bool all_ok = true;
                for (const auto& [key, value] : kvs) {
                    auto got = ReadKey(real_client_, key);
                    if (!got.has_value() || got.value() != value) {
                        all_ok = false;
                        break;
                    }
                }
                if (all_ok) return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return false;
    }

    // Wait for GC compaction: detect by old bucket file(s) disappearing and
    // new one(s) appearing. Returns the set of bucket file names before and
    // after for caller verification. Polls up to 30s.
    bool WaitForCompaction(const fs::path& ssd_dir,
                           const std::vector<std::string>& buckets_before,
                           std::vector<std::string>& buckets_after) {
        for (int i = 0; i < 150; ++i) {  // up to 30s
            buckets_after = ListBucketFiles(ssd_dir);
            // Compaction: at least one old bucket file gone, or set changed.
            if (buckets_after != buckets_before) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        buckets_after = ListBucketFiles(ssd_dir);
        return false;
    }

    fs::path tmp_dir_;
    InProcMaster master_;
    std::shared_ptr<RealClient> real_client_;
    std::optional<std::string> saved_policy_;
    std::optional<std::string> saved_disable_;
    std::optional<std::string> saved_gc_interval_;
    std::optional<std::string> saved_gc_ratio_;
    std::optional<std::string> saved_bucket_keys_limit_;
};

// -------------------------------------------------------------------
// Test 1: RemoveReclaimsSSDSpace
//
// Put 2 keys (each in its own bucket, bucket_keys_limit=1), remove k1.
// GC compaction must delete k1's (now-empty) bucket file. Validates:
//   - k1's .bucket file is eventually deleted (space reclaimed)
//   - k2 remains readable with correct data throughout
//   - k1 stays gone
// -------------------------------------------------------------------
TEST_F(GCE2ETest, RemoveReclaimsSSDSpace) {
    ASSERT_TRUE(StartMasterWithOffload());
    ASSERT_TRUE(StartRealClient());

    const std::string k1 = "gc_e2e_k1";
    const std::string k2 = "gc_e2e_k2";
    const std::string v1(4 * kMB, 'A');
    const std::string v2(4 * kMB, 'B');

    fs::path ssd_dir = tmp_dir_ / "ssd_offload";
    ASSERT_TRUE(PutAndWaitOffloaded(k1, v1, ssd_dir))
        << "k1 offload timed out";
    ASSERT_TRUE(PutAndWaitOffloaded(k2, v2, ssd_dir))
        << "k2 offload timed out";

    int buckets_before = CountFilesWithSuffix(ssd_dir, ".bucket");
    ASSERT_GT(buckets_before, 0) << "No bucket files after offload";

    // Wait for all offload tasks to settle into object_bucket_map_.
    WaitForAllOffloadsSettled();

    // Snapshot bucket file names AFTER settle (all buckets written) and
    // BEFORE remove. This is the baseline for detecting GC compaction.
    auto bucket_files_before = ListBucketFiles(ssd_dir);
    int buckets_after_settle = CountFilesWithSuffix(ssd_dir, ".bucket");

    // Remove k1. GC should compact (delete k1's empty bucket file).
    ASSERT_EQ(real_client_->remove(k1, /*force=*/true), 0);

    // Wait for GC: bucket file set should change (old file deleted, and/or
    // new file written if compaction rewrote surviving keys).
    bool reclaimed = false;
    for (int i = 0; i < 150; ++i) {  // up to 30s
        // k2 must stay readable with correct data throughout GC.
        auto got2 = ReadKey(real_client_, k2);
        ASSERT_TRUE(got2.has_value())
            << "Surviving key k2 became unreadable during GC";
        ASSERT_EQ(got2.value(), v2)
            << "Surviving key k2 data corrupted during GC";
        // k1 must stay gone.
        auto got1 = ReadKey(real_client_, k1);
        ASSERT_FALSE(got1.has_value())
            << "Removed key k1 became readable again";

        // Detect: any file in before-set gone, OR count decreased.
        auto bucket_files_now = ListBucketFiles(ssd_dir);
        for (const auto& old_name : bucket_files_before) {
            if (std::find(bucket_files_now.begin(),
                          bucket_files_now.end(),
                          old_name) == bucket_files_now.end()) {
                reclaimed = true;
                break;
            }
        }
        int buckets_now = CountFilesWithSuffix(ssd_dir, ".bucket");
        if (!reclaimed && buckets_now < buckets_after_settle) {
            reclaimed = true;
        }
        if (reclaimed) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // Final checks.
    auto got = ReadKey(real_client_, k2);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got.value(), v2) << "Surviving key data corrupted after GC";

    EXPECT_TRUE(reclaimed) << "GC did not reduce bucket file count within 30s";
}

// -------------------------------------------------------------------
// Test 2: RemoveMiddleKeyPreservesSurvivors
//
// Put 2 keys, remove 1, wait for GC. The surviving key must remain
// readable with correct data. This is the core "don't lose un-removed
// keys" invariant.
// -------------------------------------------------------------------
TEST_F(GCE2ETest, RemoveMiddleKeyPreservesSurvivors) {
    ASSERT_TRUE(StartMasterWithOffload());
    ASSERT_TRUE(StartRealClient());

    const std::string k1 = "gc_mid_k1";
    const std::string k2 = "gc_mid_k2";
    const std::string v1(4 * kMB, 'X');
    const std::string v2(4 * kMB, 'Y');

    fs::path ssd_dir = tmp_dir_ / "ssd_offload";
    ASSERT_TRUE(PutAndWaitOffloaded(k1, v1, ssd_dir))
        << "k1 offload timed out";
    ASSERT_TRUE(PutAndWaitOffloaded(k2, v2, ssd_dir))
        << "k2 offload timed out";

    int buckets_before = CountFilesWithSuffix(ssd_dir, ".bucket");
    ASSERT_GT(buckets_before, 0);

    // Wait for all offload tasks to settle into object_bucket_map_.
    WaitForAllOffloadsSettled();

    // Snapshot bucket file names AFTER settle (all buckets written) and
    // BEFORE remove. This is the baseline for detecting GC compaction.
    auto bucket_files_before = ListBucketFiles(ssd_dir);
    // Re-count after settle — more buckets may have appeared.
    int buckets_after_settle = CountFilesWithSuffix(ssd_dir, ".bucket");

    // Remove k2 (force=true to bypass lease).
    ASSERT_EQ(real_client_->remove(k2, /*force=*/true), 0);

    // Wait for GC: bucket file set should change.
    bool compacted = false;
    for (int i = 0; i < 150; ++i) {
        auto got1 = ReadKey(real_client_, k1);
        if (got1.has_value() && got1.value() == v1) {
            auto bucket_files_now = ListBucketFiles(ssd_dir);
            // Detect: any file in before-set gone, OR count decreased.
            for (const auto& old_name : bucket_files_before) {
                if (std::find(bucket_files_now.begin(),
                              bucket_files_now.end(),
                              old_name) == bucket_files_now.end()) {
                    compacted = true;
                    break;
                }
            }
            int buckets_now = CountFilesWithSuffix(ssd_dir, ".bucket");
            if (!compacted && buckets_now < buckets_after_settle) {
                compacted = true;
            }
            if (compacted) break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // k1 must survive with correct data.
    auto got1 = ReadKey(real_client_, k1);
    ASSERT_TRUE(got1.has_value());
    EXPECT_EQ(got1.value(), v1) << "k1 data corrupted after GC";

    // k2 must remain gone.
    auto got2 = ReadKey(real_client_, k2);
    EXPECT_FALSE(got2.has_value())
        << "Removed key k2 should not be readable";

    EXPECT_TRUE(compacted) << "GC compaction not detected within 30s";
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
    const std::string k2 = "gc_batch_k2";
    const std::string v1(4 * kMB, 'Q');
    const std::string v2(4 * kMB, 'R');

    fs::path ssd_dir = tmp_dir_ / "ssd_offload";
    ASSERT_TRUE(PutAndWaitOffloaded(k1, v1, ssd_dir))
        << "k1 offload timed out";
    ASSERT_TRUE(PutAndWaitOffloaded(k2, v2, ssd_dir))
        << "k2 offload timed out";

    int buckets_before = CountFilesWithSuffix(ssd_dir, ".bucket");
    ASSERT_GT(buckets_before, 0);

    // Wait for all offload tasks to settle into object_bucket_map_.
    WaitForAllOffloadsSettled();

    // Snapshot bucket file names AFTER settle (all buckets written) and
    // BEFORE remove.
    auto bucket_files_before = ListBucketFiles(ssd_dir);
    int buckets_after_settle = CountFilesWithSuffix(ssd_dir, ".bucket");

    // Batch remove: k1 exists, k_absent does not. force=true bypasses lease.
    std::vector<std::string> keys{k1, "gc_batch_absent"};
    auto results = real_client_->batchRemove(keys, /*force=*/true);
    ASSERT_EQ(results.size(), 2u);

    // Wait for GC: bucket file set should change.
    bool compacted = false;
    for (int i = 0; i < 150; ++i) {
        auto got2 = ReadKey(real_client_, k2);
        if (got2.has_value() && got2.value() == v2) {
            auto bucket_files_now = ListBucketFiles(ssd_dir);
            for (const auto& old_name : bucket_files_before) {
                if (std::find(bucket_files_now.begin(),
                              bucket_files_now.end(),
                              old_name) == bucket_files_now.end()) {
                    compacted = true;
                    break;
                }
            }
            int buckets_now = CountFilesWithSuffix(ssd_dir, ".bucket");
            if (!compacted && buckets_now < buckets_after_settle) {
                compacted = true;
            }
            if (compacted) break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // k1 must be gone.
    auto got1 = ReadKey(real_client_, k1);
    EXPECT_FALSE(got1.has_value())
        << "Batch-removed key k1 should not be readable";

    // k2 must survive.
    auto got2 = ReadKey(real_client_, k2);
    ASSERT_TRUE(got2.has_value());
    EXPECT_EQ(got2.value(), v2) << "k2 data corrupted after GC";

    EXPECT_TRUE(compacted) << "GC compaction not detected within 30s";
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    return RUN_ALL_TESTS();
}
