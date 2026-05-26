# Mooncake SSD Offload 机制

## 1. 核心概念

Offload 是 Mooncake 将数据从 **DRAM（MEMORY副本）** 迁移到 **本地 SSD（LOCAL_DISK副本）** 的过程。与 Eviction（直接丢弃）不同，Offload 将数据持久化到磁盘，后续可通过 Load 路径读回。

```
MEMORY副本 ──Offload──→ LOCAL_DISK副本 ──Promotion──→ MEMORY副本
    │                       │
    └──Eviction（丢弃）      └──Disk Eviction（丢弃）
```

## 2. 核心数据流

### 2.1 Offload（内存 → SSD）

```mermaid
sequenceDiagram
    participant FS as FileStorage (Client)
    participant M as MasterService

    loop 每隔 heartbeat_interval (默认10s)
        FS->>M: OffloadObjectHeartbeat(client_id, enable_offloading)
        M-->>FS: 返回 offloading_objects {key→size}
    end

    Note over FS: 执行 OffloadObjects()
    FS->>FS: BatchQuerySegmentSlices() 从内存读数据
    FS->>FS: StorageBackend::BatchOffload() 写入SSD
    FS->>M: NotifyOffloadSuccess(keys, metadatas)
    Note over M: 释放MEMORY副本refcnt<br/>添加LOCAL_DISK副本(COMPLETE)
```

### 2.2 Load（SSD → 请求方）

```mermaid
sequenceDiagram
    participant RC as 请求方Client
    participant M as MasterService
    participant TC as 目标Client (FileStorage)

    RC->>M: Get/BatchGet(keys)
    M-->>RC: 返回 LOCAL_DISK 副本位置
    RC->>TC: batch_get_offload_object(keys)
    TC->>TC: 从SSD读取到ClientBuffer
    TC-->>RC: 返回 batch_id + RDMA地址
    RC->>RC: TransferEngine RDMA零拷贝拉取
    RC->>TC: release_offload_buffer(batch_id)
```

## 3. 触发时机与 Key 选取

系统有两种 offload 触发模式，由 `offload_on_evict` 开关控制：

### 模式 A：PutEnd 即入队（默认，`offload_on_evict=false`）

```mermaid
flowchart TD
    A[Client 调用 PutEnd] --> B{enable_offload?<br/>!offload_on_evict?}
    B -->|Yes| C[将该对象的所有已完成<br/>MEMORY副本加入 offloading_queue]
    B -->|No| D[不做任何offload操作]
    C --> E[副本 refcnt++ 防止被evict]
    E --> F[等心跳线程取出执行]
```

- **选取标准**：所有 PutEnd 完成的对象**无差别入队**
- **无筛选逻辑**：不区分冷热，全部 offload

### 模式 B：Eviction 时入队（`offload_on_evict=true`）

```mermaid
flowchart TD
    A[内存使用率 > eviction_high_watermark] --> B[BatchEvict 开始淘汰]
    B --> C{遍历候选对象}
    C --> D{已有 LOCAL_DISK 副本?}
    D -->|Yes| E[安全,直接evict MEMORY副本]
    D -->|No| F{offload队列达到上限?}
    F -->|No| G[PushOffloadingQueue<br/>refcnt++ 保护]
    F -->|Yes| H{offload_force_evict?}
    H -->|Yes| I[强制evict,数据丢失]
    H -->|No| J[跳过,保留数据]
    G --> K[等心跳线程取出执行]
```

- **选取标准**：由 `BatchEvict` 决定候选对象，基于 **lease_timeout 时间排序**（近似 LRU）
- **两轮扫描**：第一轮淘汰无 soft pin 的对象，第二轮淘汰有 soft pin 的对象（需 `allow_evict_soft_pinned_objects=true`）
- **保护机制**：入队时 `refcnt++` 防止 offload 期间被 evict

### Master 端 Eviction 流程

```mermaid
flowchart TD
    A[EvictionThreadFunc 后台线程] --> B{内存使用率 ><br/>eviction_high_watermark?}
    B -->|No| C[休眠,继续监测]
    B -->|Yes| D[计算本次evict目标量]
    D --> E[BatchEvict<br/>按lease_timeout排序选候选]
    E --> F{offload_on_evict模式?}
    F -->|No| G[直接evict MEMORY副本]
    F -->|Yes| H[尝试先offload再evict]
```

## 4. Offload 与 Eviction 的关系

| 维度 | Offload | Eviction |
|------|---------|----------|
| 目的 | 将数据持久化到 SSD | 释放内存空间 |
| 数据去向 | 本地 SSD 文件 | 丢弃 |
| 数据可恢复 | 是（通过 Load/Promotion） | 否 |
| 触发者 | 心跳线程（定时） | Eviction 后台线程（水位触发） |
| 副本变化 | MEMORY → LOCAL_DISK | MEMORY → 删除 |

**协同关系**：
- Offload 是 Eviction 的**前置安全网**——先持久化再释放，避免数据丢失
- `offload_on_evict=true` 时二者紧密耦合：eviction 候选先尝试 offload，成功后才释放内存
- `offload_on_evict=false` 时二者独立：PutEnd 时入 offload 队列，eviction 按自己逻辑运行

## 5. 四种配置组合

| 组合 | enable_offload | offload_on_evict | offload_force_evict | 行为 |
|------|:-:|:-:|:-:|------|
| A（默认） | true | false | false | PutEnd 立即入 offload 队列，eviction 独立运行 |
| B | true | true | false | eviction 时才尝试 offload，失败则跳过（保留数据） |
| C | true | true | true | eviction 时先 offload，队列满则强制 evict（数据丢失） |
| D | true | false | true | 等同 A（force_evict 无效） |

## 6. Promotion（SSD → 内存热提升）

当 `promotion_on_hit=true` 时，频繁访问的 LOCAL_DISK 数据自动提升回内存：

```mermaid
flowchart TD
    A[Get 命中 LOCAL_DISK 副本] --> B[TryPushPromotionQueue]
    B --> C{准入检查}
    C -->|频率 >= threshold| D{内存水位 < 高水位?}
    C -->|频率不足| Z[跳过]
    D -->|Yes| E{去重:无MEMORY副本且无进行中任务?}
    D -->|No| Z
    E -->|Yes| F{队列 < promotion_queue_limit?}
    E -->|No| Z
    F -->|Yes| G[加入promotion队列]
    F -->|No| Z
    G --> H[心跳线程取出<br/>分配MEMORY副本→SSD读取→RDMA写入]
```

- **频率统计**：Count-Min Sketch，阈值 `promotion_admission_threshold`（默认 2）
- **每次心跳限 1 个** promotion 任务（`kMaxPerHeartbeat=1`）

## 7. 所有控制开关与环境变量

### Master 端开关（master.yaml 或命令行参数）

| 开关 | 默认值 | 说明 |
|------|--------|------|
| `enable_offload` | false | 总开关：是否启用 SSD offload |
| `offload_on_evict` | false | true=推迟到 eviction 时才 offload；false=PutEnd 立即入队 |
| `offload_force_evict` | false | true=offload 队列满时强制 evict（数据丢失） |
| `promotion_on_hit` | false | true=自动将热 SSD 数据提升回内存 |
| `promotion_admission_threshold` | 2 | 提升的最小访问频率 |
| `promotion_queue_limit` | 50000 | 待提升队列最大长度 |
| `eviction_high_watermark_ratio` | 0.85 | 内存使用率触发 eviction 的阈值 |
| `eviction_ratio` | 0.05 | 每轮 eviction 目标回收比例 |

### Client 端环境变量

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` | `/data/file_storage` | SSD 存储目录 |
| `MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR` | `bucket_storage_backend` | 存储后端：`bucket_storage_backend` / `file_per_key_storage_backend` / `offset_allocator_storage_backend` |
| `MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES` | 1280MB | Load 用的 staging buffer 大小 |
| `MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES` | 2TB | SSD 磁盘使用上限 |
| `MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS` | 10 | 心跳间隔（秒） |
| `MOONCAKE_OFFLOAD_USE_URING` | false | 是否启用 io_uring 异步 I/O |

### 内部硬编码常量

| 常量 | 值 | 说明 |
|------|----|------|
| `offloading_queue_limit_` | 50000 | 单客户端 offload 队列最大长度 |
| `kOffloadCapRatio` | 0.5 | force_evict 触发阈值 = 队列上限 × 50% |
