// Copyright 2024 KVCache.AI
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

#ifndef UB_TRANSPORT_H
#define UB_TRANSPORT_H
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "topology.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {
class UbContext;
class UbEndPoint;
class UrmaContext;
class TransferMetadata;
class UbWorkerpool;

enum UB_ENDPOINT_TYPE { URMA_ENDPOINT = 0, OBMM_ENDPOINT = 1 };

// ub transport supports integration of endpoints that use UB protocol software.
// Currently, two types of endpoints are supported: urma and obmm.
// urma link : https://atomgit.com/openeuler/umdk
// obmm link : https://atomgit.com/openeuler/obmm
class UbTransport : public Transport {
    friend class UbContext;
    friend class UbEndPoint;
    friend class UrmaContext;
    friend class UbWorkerPool;

   public:
    using BufferDesc = TransferMetadata::BufferDesc;
    using SegmentDesc = TransferMetadata::SegmentDesc;
    using HandShakeDesc = TransferMetadata::HandShakeDesc;

   public:
    UbTransport(UB_ENDPOINT_TYPE endpoint_type = URMA_ENDPOINT);

    ~UbTransport();

    int install(std::string& local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo) override;

    int registerLocalMemory(void* addr, size_t length,
                            const std::string& location, bool remote_accessible,
                            bool update_metadata = true) override;

    int unregisterLocalMemory(void* addr, bool update_metadata = true) override;

    int registerLocalMemoryBatch(const std::vector<BufferEntry>& buffer_list,
                                 const std::string& location) override;

    int unregisterLocalMemoryBatch(
        const std::vector<void*>& addr_list) override;

    const char* getName() const override { return "ub"; }

    // TRANSFER

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest>& entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask*>& task_list) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status) override;

    SegmentID getSegmentID(const std::string& segment_name);

   private:
    int allocateLocalSegmentID();

    struct StagingLease {
        void* host_ptr = nullptr;
        size_t size = 0;
    };

    struct DeviceRegion {
        uint64_t addr = 0;
        size_t length = 0;
        std::string location;
        bool remote_accessible = true;
    };

    struct StagingState {
        void* original_device_ptr = nullptr;
        void* staging_ptr = nullptr;
        size_t size = 0;
        size_t lease_size = 0;
        TransferRequest::OpCode opcode = TransferRequest::WRITE;
        std::atomic<uint64_t> completed_slices{0};
        uint64_t total_slices = 0;
        std::atomic<bool> failed{false};
        std::mutex deferred_mutex;
        std::vector<Slice*> deferred_success_slices;
    };

    bool stagingEnabled() const;
    bool isDevicePointer(const void* ptr) const;
    bool isLogicalDeviceRange(const void* ptr, size_t length) const;
    int registerLogicalDeviceRegion(void* addr, size_t length,
                                    const std::string& location,
                                    bool remote_accessible);
    int unregisterLogicalDeviceRegion(void* addr);
    bool copyDeviceToHost(void* dst, const void* src, size_t size) const;
    bool copyHostToDevice(void* dst, const void* src, size_t size) const;
    Status acquireStaging(size_t size, StagingLease& lease);
    void releaseStaging(const StagingLease& lease);
    bool isStagedSlice(Slice* slice);
    bool shouldDeferSuccess(Slice* slice);
    void attachStaging(TransferTask* task,
                       std::shared_ptr<StagingState> state);
    void attachStagingSlice(Slice* slice,
                            const std::shared_ptr<StagingState>& state);
    void detachStagingSlice(Slice* slice);
    std::shared_ptr<StagingState> stagingStateForSlice(Slice* slice);
    void cleanupStagingForTask(TransferTask* task,
                               bool detach_all_slices = false);
    void onStagedSliceSuccess(Slice* slice);
    void onStagedSliceFinalFailure(Slice* slice);

   public:
    int onSetupConnections(const HandShakeDesc& peer_desc,
                           HandShakeDesc& local_desc);

    int sendHandshake(const std::string& peer_server_name,
                      const HandShakeDesc& local_desc,
                      HandShakeDesc& peer_desc) {
        return metadata_->sendHandshake(peer_server_name, local_desc,
                                        peer_desc);
    }

   private:
    static int init(UbTransport* transport);

    static void uninit(UbTransport* transport);

    static int initializeUbResources(UbTransport* transport);

    static std::shared_ptr<UbContext> buildContext(
        UbTransport* transport, const std::string& device_name,
        int max_endpoints);

    int startHandshakeDaemon(std::string& local_server_name);

   public:
    static int selectDevice(SegmentDesc* desc, uint64_t offset, size_t length,
                            int& buffer_id, int& device_id, int retry_cnt = 0);
    static int selectDevice(SegmentDesc* desc, uint64_t offset, size_t length,
                            std::string_view hint, int& buffer_id,
                            int& device_id, int retry_cnt = 0);

   private:
    std::vector<std::shared_ptr<UbContext>> context_list_;
    std::shared_ptr<Topology> local_topology_;
    UB_ENDPOINT_TYPE endpoint_type_;
    bool runtime_initialized_ = false;

    mutable std::once_flag staging_config_once_;
    mutable bool staging_enabled_ = false;
    mutable std::mutex device_region_mutex_;
    std::vector<DeviceRegion> device_regions_;
    std::mutex staging_pool_mutex_;
    void* staging_pool_base_ = nullptr;
    size_t staging_pool_size_ = 0;
    size_t staging_pool_offset_ = 0;
    std::deque<std::pair<void*, size_t>> staging_free_list_;
    std::mutex staging_state_mutex_;
    std::unordered_map<TransferTask*, std::shared_ptr<StagingState>>
        task_staging_map_;
    std::unordered_map<Slice*, std::shared_ptr<StagingState>>
        slice_staging_map_;
};
}  // namespace mooncake

#endif  // UB_TRANSPORT_H
