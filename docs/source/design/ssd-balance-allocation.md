# SSD负载均衡分配策略设计文档

## 1. 概述

### 1.1 问题背景

现有 `FreeRatioFirstAllocationStrategy` 在选择segment时只考虑DDR空闲比例，忽略了SSD水位。这导致以下问题：

- 一个segment的DDR空闲但SSD已满时，数据仍被分配到该segment
- 后续eviction时无法offload到SSD（因为SSD已满），DDR产生backpressure
- 最终DDR被填满，整个节点无法接受新写入

### 1.2 解决方案

新增 `SsdBalanceAllocationStrategy`，按SSD空闲比例做负载均衡：

- 默认只看SSD水位（alpha=0），优先选择SSD空闲的节点
- SSD达到高水位时禁止向该节点写入，但不驱逐SSD数据
- DDR达到驱逐水位时临时禁止写入，水位下降后自动恢复

### 1.3 适用场景

多节点集群中每个节点有DDR+本地SSD的分层存储环境。

## 2. 设计目标

| 目标 | 说明 |
|------|------|
| SSD比例均衡 | 按SSD空闲比例选择segment，优先写入SSD空闲的节点 |
| SSD驱逐保护 | SSD达到高水位时禁止写入，绝不驱逐SSD数据（避免数据丢失） |
| DDR准入控制 | DDR达到驱逐水位时临时禁止写入，水位下降后自动恢复 |
| 全满暂停 | 所有节点DDR都满时暂停所有put，等待节点释放空间 |

## 3. 核心算法

### 3.1 SSD比例计算

```
ssd_free_ratio = (ssd_total_capacity - ssd_used_bytes) / ssd_total_capacity
```

- 无SSD信息的segment：`ssd_free_ratio = 1.0`（不约束）
- `ssd_used_bytes` 通过 `std::atomic<int64_t>` 跟踪，在offload成功时递增，磁盘驱逐时递减

### 3.2 候选采样与排序

```
1. 采样 min(6 * replica_num, total_segments) 个候选segment
2. 排除SSD使用率 >= ssd_high_watermark_ratio 的segment
3. 按ssd_free_ratio降序排序
4. 从top-N候选中尝试分配
5. 如果replica_num未满足，fallback到随机分配
```

### 3.3 SSD高水位保护

当segment的SSD使用率 >= `ssd_high_watermark_ratio`（默认0.90）时：

- **禁止**向该segment分配新数据
- **绝不驱逐**SSD上的已有数据（驱逐意味着数据不可恢复丢失）
- SSD数据只能通过以下方式释放：
  - 正常promotion（访问命中后提升回DDR）
  - TTL过期（软pin到期后自动清理）
- SSD水位下降后，节点自动恢复可写状态

### 3.4 DDR写入准入控制

当全局DDR使用率 > `eviction_high_watermark_ratio`（默认0.95）时：

- 设置 `need_mem_eviction_ = true`，加速eviction
- 返回 `NO_AVAILABLE_HANDLE`，客户端带退避重试
- DDR水位下降（eviction释放空间）后，分配自动恢复

如果所有节点DDR都超过水位，所有put暂停，直到至少一个节点eviction完成释放空间。

## 4. 决策流程

### 4.1 AllocateAndInsertMetadata流程

```
AllocateAndInsertMetadata()
│
├── 检查全局DDR使用率
│   └── DDR > eviction_high_watermark_ratio?
│       ├── YES → need_mem_eviction_ = true
│       │         返回 NO_AVAILABLE_HANDLE（临时禁止）
│       └── NO  → 继续
│
├── 获取AllocatorManager和SsdMetricsProvider
│
├── 处理preferred segments（同现有逻辑）
│   └── 检查SSD水位，跳过高水位segment
│
├── 候选采样 + SSD比例排序
│   ├── 排除excluded/used segments
│   ├── 排除SSD高水位segments
│   └── 按ssd_free_ratio降序排序，取top-N
│
├── 尝试分配
│
└── Fallback随机分配
    └── 同样排除SSD高水位segments
```

### 4.2 SSD水位检查

```
isSsdHighWatermark(segment_name)
│
├── 查询SsdMetricsProvider
│   ├── total = getSsdTotalCapacity(segment_name)
│   └── used = getSsdUsedBytes(segment_name)
│
├── total <= 0?
│   └── 返回false（无SSD信息，不阻塞）
│
└── used/total >= ssd_high_watermark_ratio?
    ├── YES → 排除该segment
    └── NO  → 允许分配
```

## 5. SSD使用量追踪

### 5.1 数据结构

`LocalDiskSegment` 新增字段：

```cpp
std::atomic<int64_t> ssd_used_bytes{0};
```

### 5.2 更新时机

| 事件 | 操作 | 触发位置 |
|------|------|----------|
| offload成功 | `ssd_used_bytes += data_size` | `NotifyOffloadSuccess` |
| 磁盘replica被驱逐 | `ssd_used_bytes -= object_size` | `EvictDiskReplica` |

### 5.3 暴露接口

通过 `SsdMetricsProvider` 接口：

```cpp
class SsdMetricsProvider {
    virtual int64_t getSsdTotalCapacity(const std::string& segment_name) const = 0;
    virtual int64_t getSsdUsedBytes(const std::string& segment_name) const = 0;
};
```

`ScopedLocalDiskSegmentAccess` 实现该接口，通过 segment_name → client_id → LocalDiskSegment 查找。

## 6. 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--allocation_strategy` | `random` | 设为 `ssd_balance` 启用本策略 |
| `--ssd_high_watermark_ratio` | `0.90` | SSD使用率上限，超过则禁止向该节点写入 |

启用方式：

```bash
./mooncake_master --allocation_strategy=ssd_balance --ssd_high_watermark_ratio=0.90
```

## 7. 代码结构

### 7.1 新增/修改文件

| 文件 | 变更类型 | 说明 |
|------|----------|------|
| `include/allocation_strategy.h` | 修改 | 新增 `SsdMetricsProvider` 接口、`SsdBalanceAllocationStrategy` 类、更新工厂函数 |
| `include/types.h` | 修改 | `AllocationStrategyType` 枚举新增 `SSD_BALANCE` |
| `include/segment.h` | 修改 | `LocalDiskSegment` 新增 `ssd_used_bytes`；`ScopedLocalDiskSegmentAccess` 实现 `SsdMetricsProvider` |
| `src/segment.cpp` | 修改 | 实现 `getSsdTotalCapacity` 和 `getSsdUsedBytes` |
| `include/master_config.h` | 修改 | 新增 `ssd_high_watermark_ratio` 配置字段 |
| `src/master.cpp` | 修改 | 新增 `--ssd_high_watermark_ratio` gflag |
| `include/master_service.h` | 修改 | 新增 `ssd_high_watermark_ratio_` 成员 |
| `src/master_service.cpp` | 修改 | DDR准入控制、传SSD provider、SSD使用量追踪 |

### 7.2 类继承关系

```
AllocationStrategy (抽象基类)
├── RandomAllocationStrategy
│   └── FreeRatioFirstAllocationStrategy
│       └── SsdBalanceAllocationStrategy  ← 新增
└── CxlAllocationStrategy

SsdMetricsProvider (抽象接口)
└── ScopedLocalDiskSegmentAccess  ← 新增实现
```

## 8. 验证方案

详见 `tests/ssd_balance_verify.py`，包含以下测试用例：

1. **SSD负载均衡验证**：不同SSD使用率的节点，验证数据按SSD空闲比例分布
2. **SSD驱逐保护验证**：填满SSD到高水位，验证写入被禁止但已有数据不被驱逐
3. **DDR准入控制验证**：填满DDR到水位，验证写入临时被禁止后恢复
4. **全节点SSD满验证**：所有节点SSD满后验证写入暂停，释放后恢复
