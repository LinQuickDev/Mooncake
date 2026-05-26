# Offload + Promotion 冷热交换机制验证指南

## 验证目标

验证 Mooncake 的完整冷热数据交换循环：

```
MEMORY ──Offload──→ LOCAL_DISK ──Promotion──→ MEMORY
  │                      │                      │
  └── Eviction           └── Load (SSD→Client)  └── 热数据回到内存
```

四个测试场景：

| test_name | 验证内容 |
|-----------|---------|
| `offload` | **基础 Offload**：写入数据，验证数据从 MEMORY 下沉到 LOCAL_DISK（SSD） |
| `load` | **SSD Load 路径**：数据被 eviction 淘汰后，从 SSD 读取仍成功（走 offload RPC 路径） |
| `promotion` | **Promotion on Hit**：频繁读取 LOCAL_DISK key，验证其被提升回 MEMORY |
| `exchange` | **冷热交换闭环**：写入混合数据，热 key 回到 MEMORY，冷 key 留在 SSD |

## 前置条件

- 编译完成 mooncake_store（含 `mooncake_master` 可执行文件）
- 编译完成 mooncake-wheel（含 Python `mooncake.store` 模块）
- 安装 Python 3

## 验证脚本

```bash
python tests/verify_offload_promotion.py --test <test_name>
```

## 默认规模

DDR = 32MB（`SEGMENT_SIZE_BYTES`），Value = 1MB，NumKeys = 80（≈80MB，远超 32MB DDR，必然触发 eviction）

## 注意事项

- **每次测试前清空 SSD 目录**：`rm -rf <SSD_PATH> && mkdir -p <SSD_PATH>`
- **MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS 必须设为 1**：默认 10s 会导致 offload/promotion 延迟过长
- **MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES 设小（如 10MB）**：默认 256MB 太大，测试数据量不够一个桶，不会落盘
- **put() 不抛异常**：`store.put()` 返回整数状态码（0=成功，非0=失败）
- **promotion_on_hit 必须在 Master 端启用**：不是 Client 端参数

---

## 验证 1：基础 Offload（MEMORY → SSD）

写入数据到 DDR，等待 offload 心跳将数据持久化到 SSD，验证 LOCAL_DISK 副本出现。

### 机制说明

在默认模式（`offload_on_evict=false`）下，每个 `PutEnd` 完成的对象被立即加入 offload 队列。心跳线程每隔 `MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS` 秒（设为 1）取出队列中的对象，从 MEMORY 读取数据写入 SSD，然后通过 `NotifyOffloadSuccess` 通知 Master 添加 LOCAL_DISK 副本。

**Terminal 1** — 启动 Master：

```bash
mooncake_master \
    --port=50053 \
    --http_metadata_server_port=8880 \
    --enable_http_metadata_server=true \
    --metrics_port=9104 \
    --enable_offload=true \
    --offload_on_evict=false \
    --default_kv_lease_ttl=2000
```

**Terminal 2** — 运行验证脚本：

```bash
MC_METADATA_SERVER=http://127.0.0.1:8880/metadata \
MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=1 \
MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/tmp/mooncake_offload_promotion_1 \
MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES=10485760 \
SEGMENT_SIZE_BYTES=33554432 \
python tests/verify_offload_promotion.py --test offload
```

### 预期观察

- 写入 30 个 1MB 对象（全部成功，DDR 32MB 足够初始写入）
- 等待 15s（心跳间隔 1s，足够多轮 offload 完成）
- `batch_get_replica_desc()` 返回至少部分 key 拥有 `LOCAL_DISK` 副本
- 输出示例：
  ```
  Replica distribution: memory_only=10, local_disk_only=5, both=15, none=0
  ```

### 判断标准

- `local_disk_only + both > 0`（至少有一些 key 已 offload 到 SSD）
- 脚本输出 `[PASS] Basic Offload (MEMORY -> SSD)`

---

## 验证 2：SSD Load 路径

写入大量数据溢出 DDR → eviction 淘汰 MEMORY 副本 → 通过 offload RPC 从 SSD 读取 → 验证数据完整。

### 机制说明

当对象仅有 LOCAL_DISK 副本时，`Get` 请求走 Load 路径：Master 返回 LOCAL_DISK 副本位置 → 请求方 Client 向目标 Client 发起 `batch_get_offload_object` RPC → 目标端从 SSD 读取到 ClientBuffer → 通过 Transfer Engine RDMA 零拷贝传输。读取计数由 `get_offload_rpc_read_count()` 反映。

**Terminal 1** — 启动 Master（启用 offload_on_evict 使 eviction 触发 offload）：

```bash
mooncake_master \
    --port=50053 \
    --http_metadata_server_port=8880 \
    --enable_http_metadata_server=true \
    --metrics_port=9104 \
    --enable_offload=true \
    --offload_on_evict=true \
    --default_kv_lease_ttl=2000 \
    --eviction_high_watermark_ratio=0.70
```

**Terminal 2** — 运行验证脚本：

```bash
MC_METADATA_SERVER=http://127.0.0.1:8880/metadata \
MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=1 \
MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/tmp/mooncake_offload_promotion_2 \
MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES=10485760 \
SEGMENT_SIZE_BYTES=33554432 \
python tests/verify_offload_promotion.py --test load
```

### 预期观察

- 写入 80 个 1MB 对象（≈80MB，远超 32MB DDR）
- eviction 淘汰部分 MEMORY 副本，offload 将其持久化到 SSD
- 从 LOCAL_DISK-only key 读取：`store.get(key)` 返回正确字节
- `get_offload_rpc_read_count()` 计数增加（说明走的是 SSD→Client Load 路径）

### 判断标准

- 至少存在一些 LOCAL_DISK-only key
- 从这些 key 读取数据成功（与写入的字节完全一致）
- 脚本输出 `[PASS] SSD Load Path (SSD -> Client)`

---

## 验证 3：Promotion on Hit（SSD → MEMORY 热提升）

整体流程：DDR 溢出 → offload 到 SSD → 反复读取特定 key → Master 的 `TryPushPromotionQueue` 将其加入 promotion 队列 → Client 心跳线程执行 promotion → key 重新获得 MEMORY 副本 → 后续读走 RDMA 零拷贝路径，offload RPC 计数不再增加。

### 机制说明

Promotion 准入检查（`TryPushPromotionQueue`）包含四级门控：

1. **频率门控**：Count-Min Sketch 统计访问频率 ≥ `promotion_admission_threshold`（默认 2）
2. **水位门控**：DRAM 使用率 < `eviction_high_watermark_ratio`
3. **去重门控**：无已有 MEMORY 副本，无进行中 promotion 任务
4. **容量门控**：`promotion_in_flight_` < `promotion_queue_limit`

通过后加入 promotion 队列。Client 心跳线程（每次取 1 个任务）执行：`PromotionAllocStart` 分配 MEMORY 副本 → SSD 读取 → RDMA 写入 → `NotifyPromotionSuccess`。

**Terminal 1** — 启动 Master（关键：必须开启 promotion_on_hit）：

```bash
mooncake_master \
    --port=50053 \
    --http_metadata_server_port=8880 \
    --enable_http_metadata_server=true \
    --metrics_port=9104 \
    --enable_offload=true \
    --offload_on_evict=true \
    --promotion_on_hit=true \
    --promotion_admission_threshold=2 \
    --default_kv_lease_ttl=2000 \
    --eviction_high_watermark_ratio=0.70
```

**Terminal 2** — 运行验证脚本：

```bash
MC_METADATA_SERVER=http://127.0.0.1:8880/metadata \
MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=1 \
MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/tmp/mooncake_offload_promotion_3 \
MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES=10485760 \
SEGMENT_SIZE_BYTES=33554432 \
python tests/verify_offload_promotion.py --test promotion
```

### 预期观察

- 写入 80 个 1MB 对象（溢出 32MB DDR）→ offload + eviction 后出现 LOCAL_DISK-only key
- 对 hot key 执行 4 次 `store.get()`（超过 `promotion_admission_threshold=2`）
- 等待 25s（覆盖 Master 和 Client 各至少一次心跳）
- hot key 重新出现 MEMORY 副本：`replica_types = ['MEMORY', 'LOCAL_DISK']`
- 后续 `store.get()` 的 offload RPC 计数不再增加（读从 MEMORY 服务）

### 判断标准

- hot key 在 promotion 等待后拥有 MEMORY 副本
- 脚本输出 `[PASS] Promotion on Hit (SSD -> MEMORY)`

---

## 验证 4：冷热交换闭环

混合写入热 key 和冷 key → overflow DDR → 热 key 被反复访问触发 promotion 回到 MEMORY → 冷 key 留在 SSD。

### 机制说明

这是 offload + promotion 的端到端验证：

1. 写入 80 个 key（20% 标记为 hot，80% 为 cold）
2. Offload + eviction：所有 key 进入 LOCAL_DISK
3. 热 key 反复读取 4 次（触发 promotion）
4. 冷 key 偶尔读取 1 次（不触发 promotion）
5. 等待 promotion 心跳完成后：
   - 热 key：应恢复 MEMORY 副本（从 SSD 提升）
   - 冷 key：应保持在 LOCAL_DISK-only 状态

**Terminal 1** — 启动 Master（同验证 3）：

```bash
mooncake_master \
    --port=50053 \
    --http_metadata_server_port=8880 \
    --enable_http_metadata_server=true \
    --metrics_port=9104 \
    --enable_offload=true \
    --offload_on_evict=true \
    --promotion_on_hit=true \
    --promotion_admission_threshold=2 \
    --default_kv_lease_ttl=2000 \
    --eviction_high_watermark_ratio=0.70
```

**Terminal 2** — 运行验证脚本：

```bash
MC_METADATA_SERVER=http://127.0.0.1:8880/metadata \
MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=1 \
MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/tmp/mooncake_offload_promotion_4 \
MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES=10485760 \
SEGMENT_SIZE_BYTES=33554432 \
python tests/verify_offload_promotion.py --test exchange
```

### 预期观察

```
After promotion cycle:
  Hot  keys: 5 with MEMORY, 3 LOCAL_DISK-only
  Cold keys: 2 with MEMORY, 58 LOCAL_DISK-only
```

- 热 key 中至少有一部分获得了 MEMORY 副本（promoted）
- 冷 key 绝大多数保持在 LOCAL_DISK-only（未触发 promotion）

### 判断标准

- 至少有一些 hot key 被提升到 MEMORY
- 冷 key 大部分留在 LOCAL_DISK-only
- 脚本输出 `[PASS] Cold-Hot Exchange`

---

## 运行全部测试

```bash
MC_METADATA_SERVER=http://127.0.0.1:8880/metadata \
MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=1 \
MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/tmp/mooncake_offload_promotion_all \
MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES=10485760 \
SEGMENT_SIZE_BYTES=33554432 \
python tests/verify_offload_promotion.py --test all
```

## 关键环境变量

| 变量 | 推荐值 | 说明 |
|------|--------|------|
| `MC_METADATA_SERVER` | `http://127.0.0.1:8880/metadata` | HTTP 元数据服务器地址 |
| `MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS` | `1` | **必须设为 1**，默认 10s 太慢 |
| `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` | 测试专用目录 | 每次测试前清空 |
| `MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES` | `10485760`（10MB） | 必须设小，默认 256MB 导致不足一桶不落盘 |
| `SEGMENT_SIZE_BYTES` | `33554432`（32MB） | DDR 段大小，设小以快速触发 eviction |
| `DEFAULT_KV_LEASE_TTL` | `2000` | 缩短 lease 使 eviction 更快生效 |

## 关键 Master 启动参数

| 参数 | 推荐值 | 说明 |
|------|--------|------|
| `--enable_offload` | `true` | 总开关 |
| `--offload_on_evict` | `true`（验证 2/3/4） | eviction 时触发 offload |
| `--promotion_on_hit` | `true`（验证 3/4） | 启用热数据提升 |
| `--promotion_admission_threshold` | `2` | 访问频率阈值 |
| `--eviction_high_watermark_ratio` | `0.70` | 降低水位提前触发 eviction |
| `--default_kv_lease_ttl` | `2000` | 缩短 lease 加速淘汰 |

## 日志观察方法

**Client 侧 offload 日志**（需设置环境变量 `GLOG_v=1`）：

```
V... file_storage.cpp:...] Group objects with total object count: ...
V... file_storage.cpp:...] OffloadObjects completed: keys=..., time=...us
```

**Client 侧 promotion 日志**（需设置 `GLOG_v=1`）：

```
V... file_storage.cpp:...] ProcessPromotionTasks: got N promotion tasks
V... file_storage.cpp:...] Promotion completed for key=...
```

**Master 侧 promotion 日志**（需启动参数 `--v=1`）：

```
V... master_service.cpp:...] Promotion task enqueued for key=...
V... master_service.cpp:...] PromotionAllocStart: key=..., size=...
V... master_service.cpp:...] NotifyPromotionSuccess: key=... promoted to MEMORY
```

**观察 offload 落盘进度**：

```bash
watch -n 1 'find /tmp/mooncake_offload_promotion_all -name "*.bucket" -exec du -sh {} \;'
```

## 故障排查

| 现象 | 可能原因 | 解决方案 |
|------|----------|----------|
| No LOCAL_DISK replicas after offload wait | 数据量不足一个 bucket（256MB 默认）/ 心跳间隔太长 | 设 `MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES=10485760` 和 `MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=1` |
| No LOCAL_DISK-only keys（全部 memory_only） | DDR 太大未触发 eviction | 设 `SEGMENT_SIZE_BYTES=33554432`（32MB），增加 `--num-keys` |
| No promotion even after repeated reads | Master 未启动 `promotion_on_hit` / promotion 等待不够 | 确认 `--promotion_on_hit=true`，增加 `PROMOTION_WAIT_SECONDS` |
| `store.get()` 返回错误 | Key 的 lease 已过期被 eviction 且未成功 offload | 降低 `eviction_high_watermark_ratio`，确保 offload 先于 eviction 完成 |
| BucketStorageBackend 不落盘 | ungrouped_offloading_objects 留存，数据不够一桶 | 确保 `bucket_keys_limit` 和 `bucket_size_limit` 都满足（设小） |
