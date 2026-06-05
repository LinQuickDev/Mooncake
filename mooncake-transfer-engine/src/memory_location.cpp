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

#include <dirent.h>
#include <cstring>
#include <cctype>

#include "memory_location.h"

#include "cuda_alike.h"

namespace mooncake {

uintptr_t alignPage(uintptr_t address) { return address & ~(pagesize - 1); }

int parseCpuNumaNode(const std::string& location) {
    const std::string prefix = "cpu:";
    if (location.rfind(prefix, 0) != 0) return -1;
    try { return std::stoi(location.substr(prefix.size())); }
    catch (const std::exception&) { return -1; }
}

int resolveBufferNumaNode(const std::string& name, uint64_t length, uint64_t offset) {
    SegmentsLocationInfo info;
    if (parseSegmentsLocation(name, info))                 // 分段 buffer
        return parseCpuNumaNode(resolveSegmentsLocation(info, length, offset));
    return parseCpuNumaNode(name);                         // 非分段 "cpu:N"
}

// 数 /sys/devices/system/node 下的 NUMA 节点数
static size_t getNumaNodeCount() {
    int count = 0;
    DIR* dir = opendir("/sys/devices/system/node");
    if (!dir) return 0;
    for (dirent* e = readdir(dir); e; e = readdir(dir))
        if (strncmp(e->d_name, "node", 4) == 0 && isdigit((unsigned char)e->d_name[4])) count++;
    closedir(dir);
    return (size_t)count;
}

// 前半 -> chip1，后半 -> chip2（直接对齐用户给的 NumaIdToChipId 参考实现）
uint8_t numaNodeToChipId(int numa_node, size_t numa_count) {
    constexpr uint8_t chipId1 = 1, chipId2 = 2;
    if (numa_node < 0) return INVALID_CHIP_ID;          // 对应 INVALID_NUMA_ID
    if (numa_count == 0) {
        numa_count = getNumaNodeCount();
        if (numa_count == 0) return INVALID_CHIP_ID;
    }
    if ((size_t)numa_node >= numa_count) return INVALID_CHIP_ID;
    const size_t firstHalfCount = (numa_count + 1) / 2;
    return ((size_t)numa_node < firstHalfCount) ? chipId1 : chipId2;
}

std::string genCpuNodeName(int node) {
    if (node >= 0) return "cpu:" + std::to_string(node);
    return kWildcardLocation;
}

std::string genGpuNodeName(int node) {
    if (node >= 0) return GPU_PREFIX + std::to_string(node);
    return kWildcardLocation;
}

const std::vector<MemoryLocationEntry> getMemoryLocation(void *start,
                                                         size_t len,
                                                         bool only_first_page) {
    std::vector<MemoryLocationEntry> entries;

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
    cudaPointerAttributes attributes;
    cudaError_t result = cudaPointerGetAttributes(&attributes, start);
    if (result != cudaSuccess) {
        LOG(ERROR) << "cudaPointerGetAttributes failed (Error code: " << result
                   << " - " << cudaGetErrorString(result) << ")" << std::endl;
        entries.push_back({(uint64_t)start, len, kWildcardLocation});
        return entries;
    }

    if (attributes.type == cudaMemoryTypeDevice) {
        entries.push_back(
            {(uint64_t)start, len, genGpuNodeName(attributes.device)});
        return entries;
    }
#endif

    // start and end address may not be page aligned.
    uintptr_t aligned_start = alignPage((uintptr_t)start);
    long long n =
        only_first_page
            ? 1
            : (uintptr_t(start) - aligned_start + len + pagesize - 1) /
                  pagesize;
    void **pages = (void **)malloc(sizeof(void *) * n);
    int *status = (int *)malloc(sizeof(int) * n);

    for (long long i = 0; i < n; i++) {
        pages[i] = (void *)((char *)aligned_start + i * pagesize);
    }

    int rc = numa_move_pages(0, n, pages, nullptr, status, 0);
    if (rc != 0) {
        PLOG(WARNING) << "Failed to get NUMA node, addr: " << start
                      << ", len: " << len;
        entries.push_back({(uint64_t)start, len, kWildcardLocation});
        free(pages);
        free(status);
        return entries;
    }

    int node = status[0];
    uint64_t start_addr = (uint64_t)start;
    uint64_t new_start_addr;
    for (long long i = 1; i < n; i++) {
        if (status[i] != node) {
            new_start_addr = alignPage((uint64_t)start) + i * pagesize;
            entries.push_back({start_addr, size_t(new_start_addr - start_addr),
                               genCpuNodeName(node)});
            start_addr = new_start_addr;
            node = status[i];
        }
    }
    entries.push_back(
        {start_addr, (uint64_t)start + len - start_addr, genCpuNodeName(node)});
    free(pages);
    free(status);
    return entries;
}

/* ------------------------------------------------------------------ */
/* Segments location helpers                                          */
/* ------------------------------------------------------------------ */

std::string buildSegmentsLocation(size_t page_size,
                                  const std::vector<int> &numa_nodes) {
    std::string result =
        kSegmentsLocationPrefix + std::to_string(page_size) + ":";
    for (size_t i = 0; i < numa_nodes.size(); ++i) {
        if (i > 0) result += ",";
        result += std::to_string(numa_nodes[i]);
    }
    return result;
}

bool parseSegmentsLocation(const std::string &name,
                           SegmentsLocationInfo &info) {
    if (name.rfind(kSegmentsLocationPrefix, 0) != 0) return false;

    try {
        // "segments:<page_size>:<n0>,<n1>,..."
        std::string body = name.substr(kSegmentsLocationPrefix.size());
        auto colon = body.find(':');
        if (colon == std::string::npos) return false;

        info.page_size = std::stoull(body.substr(0, colon));
        info.numa_nodes.clear();

        std::string nodes_str = body.substr(colon + 1);
        size_t pos = 0;
        while (pos < nodes_str.size()) {
            auto comma = nodes_str.find(',', pos);
            std::string tok = (comma == std::string::npos)
                                  ? nodes_str.substr(pos)
                                  : nodes_str.substr(pos, comma - pos);
            if (!tok.empty()) {
                info.numa_nodes.push_back(std::stoi(tok));
            }
            pos = (comma == std::string::npos) ? nodes_str.size() : comma + 1;
        }
    } catch (const std::exception &) {
        return false;
    }
    return !info.numa_nodes.empty();
}

std::string resolveSegmentsLocation(const SegmentsLocationInfo &info,
                                    uint64_t buffer_length, uint64_t offset) {
    size_t n = info.numa_nodes.size();
    if (n == 0) return kWildcardLocation;
    size_t region_size = buffer_length / n;
    if (region_size == 0) return kWildcardLocation;
    size_t idx = offset / region_size;
    if (idx >= n) idx = n - 1;  // clamp for tail bytes
    return "cpu:" + std::to_string(info.numa_nodes[idx]);
}

}  // namespace mooncake
