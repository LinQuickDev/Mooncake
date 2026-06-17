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
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <map>
#include <set>
#include <string>
#include <unordered_map>

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

namespace {

// 读取 NUMA 节点 nodeN 下第一个 CPU 的物理 package(chip) 编号。
// 路径：/sys/devices/system/node/nodeN/cpulist -> 取首个 cpu ->
//       /sys/devices/system/cpu/cpuX/topology/physical_package_id。失败返回 -1。
int readNodePackageId(int node) {
    char path[256];
    snprintf(path, sizeof(path),
             "/sys/devices/system/node/node%d/cpulist", node);
    std::ifstream cpulist(path);
    if (!cpulist) return -1;
    std::string s;
    std::getline(cpulist, s);  // 形如 "0-23" 或 "0-7,16-23"
    int cpu = -1;
    try {
        cpu = std::stoi(s);  // 取列表首个 CPU
    } catch (const std::exception&) {
        return -1;
    }
    if (cpu < 0) return -1;

    snprintf(path, sizeof(path),
             "/sys/devices/system/cpu/cpu%d/topology/physical_package_id", cpu);
    std::ifstream pkg(path);
    if (!pkg) return -1;
    int package_id = -1;
    pkg >> package_id;
    return package_id;
}

// 进程内只构建一次：从服务器拓扑(sysfs)读取 NUMA节点 -> chip(物理package) 映射。
// chip id 采用「package 排序后 1-based」编号，兼容旧约定（双 chip 即 1/2），
// 同时正确处理节点与 package 非顺序对应的拓扑。
class NumaChipMap {
   public:
    static const NumaChipMap& Instance() {
        static const NumaChipMap inst;
        return inst;
    }

    // 返回该 NUMA 节点所属 chip id；未知返回 INVALID_CHIP_ID。
    uint8_t ChipId(int numa_node) const {
        auto it = node_to_chip_.find(numa_node);
        return it == node_to_chip_.end() ? INVALID_CHIP_ID : it->second;
    }

    bool Empty() const { return node_to_chip_.empty(); }

   private:
    NumaChipMap() { Build(); }

    void Build() {
        std::map<int, int> node_to_pkg;  // numa node -> physical package id
        std::set<int> packages;

        DIR* dir = opendir("/sys/devices/system/node");
        if (!dir) return;
        for (dirent* e = readdir(dir); e; e = readdir(dir)) {
            if (strncmp(e->d_name, "node", 4) != 0 ||
                !isdigit((unsigned char)e->d_name[4]))
                continue;
            int node = atoi(e->d_name + 4);
            int pkg = readNodePackageId(node);
            if (pkg < 0) continue;
            node_to_pkg[node] = pkg;
            packages.insert(pkg);
        }
        closedir(dir);

        // package id 排序去重 -> 1-based chip id
        std::map<int, uint8_t> pkg_to_chip;
        uint8_t chip = 1;
        for (int p : packages) pkg_to_chip[p] = chip++;

        for (const auto& [node, pkg] : node_to_pkg)
            node_to_chip_[node] = pkg_to_chip[pkg];

        std::string mapping;
        for (const auto& [node, pkg] : node_to_pkg) {
            if (!mapping.empty()) mapping += ", ";
            mapping += "numa:" + std::to_string(node) +
                       " package:" + std::to_string(pkg) +
                       " chip:" + std::to_string(pkg_to_chip[pkg]);
        }
        LOG(INFO) << "[numa_affinity] numa_chip_map nodes="
                  << node_to_chip_.size() << " chips=" << packages.size()
                  << " mapping={" << mapping << "}";
    }

    std::unordered_map<int, uint8_t> node_to_chip_;
};

}  // namespace

// NUMA 节点 -> chip id：优先按 sysfs 真实拓扑(physical_package_id)映射；
// 若 sysfs 不可读则回退到旧的「前半 chip1 / 后半 chip2」启发式。
uint8_t numaNodeToChipId(int numa_node, size_t numa_count) {
    if (numa_node < 0) return INVALID_CHIP_ID;  // 对应 INVALID_NUMA_ID

    const auto& chip_map = NumaChipMap::Instance();
    if (!chip_map.Empty()) {
        return chip_map.ChipId(numa_node);
    }

    // Fallback：sysfs 读取失败（如受限容器），退回原启发式，保证不破坏既有行为。
    constexpr uint8_t chipId1 = 1, chipId2 = 2;
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
    defined(USE_COREX) || defined(USE_SUNRISE)
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
