# SSD Explicit-Delete-Only GC for Bucket Storage Backend

- Status: Draft
- Date: 2026-06-25
- Branch: `supercache_dev`
- Owner: TBD

## 1. Problem

Mooncake Store 的 `Remove` / `BatchRemove` 当前只删除 master 元数据和
hot cache，不删除 offload 到 SSD 的 bucket 文件。被删除 key 的数据仍留在
`.bucket` 文件里成为孤儿，SSD 空间无法回收。

现网诉求：

- 一个 bucket 多个 key，多数 4MB，少数几 KB（约 100:1）。
- 删除接口调用后，需要通过阈值/定时触发回收 SSD 文件。
- 一个 bucket 中有的 key 删了有的没删，未删的 key 不能丢。
- 要保证并发一致性。

## 2. Constraints

1. **不启用 Mooncake 现有 bucket LRU/FIFO eviction。** SSD 空间回收只能
   由显式调用 `Remove` / `BatchRemove` 触发；未删除 key 即使空间紧张也
   不能被删。
2. **只给 `Remove` / `BatchRemove` 实现 SSD 回收。** `RemoveByRegex` /
   `RemoveAll` 保持现有逻辑不动，不进入 bucket GC。
3. **不改变现有接口签名与逻辑。** `Remove` / `BatchRemove` /
   `RemoveByRegex` / `RemoveAll` 对外语义不变；master 协议、`LOCAL_DISK`
   replica 描述、默认 256MB bucket 均不变。
4. **尽量少动原代码，性能优先。**
5. **未删除 key 不能丢。**
6. **要保证并发一致性。**

## 3. Background: current mechanism

### 3.1 架构分层

- `Client::storage_backend_`（`StorageBackend` 基类，file-per-key 那套，
  带 `file_write_queue_`）与 `FileStorage::storage_backend_`
  （`BucketStorageBackend`）是**两个不同对象**。
  - `mooncake-store/src/client_service.cpp:828`（`Client::storage_backend_`）
  - `mooncake-store/include/file_storage.h:133`
    （`FileStorage::storage_backend_`）
  - `mooncake-store/src/file_storage.cpp:198`（由 `CreateStorageBackend`
    创建，默认 bucket 类型）
- `Client::RemoveAll()` 调的 `storage_backend_->RemoveAll()` 只会动
  `file_write_queue_`，**不会删 bucket 文件**。
  - `mooncake-store/src/client_service.cpp:2803`
  - `mooncake-store/src/storage_backend.cpp:617`（`StorageBackend::RemoveAll`
    基类实现，遍历 root_dir 删文件 + 操作 `file_write_queue_`）
- `Client::Remove` / `Client::RemoveByRegex` 中的本地 SSD 删除代码是
  注释掉的：
  - `mooncake-store/src/client_service.cpp:2762`
  - `mooncake-store/src/client_service.cpp:2783`
- 因此新机制只在 `FileStorage` / `BucketStorageBackend` 这一层实现，
  与 `Client::RemoveAll/RemoveByRegex` 走的基类路径天然隔离。

### 3.2 调用链与接入点

- `RealClient::remove_internal` → `client_->Remove(key, force)`
  - `mooncake-store/src/real_client.cpp:2066`
- `RealClient::batchRemove_internal` → `client_->BatchRemove(keys, force)`
  - `mooncake-store/src/real_client.cpp:2108`
- `Client::Remove` / `Client::BatchRemove` 基类逻辑：
  - bump hot cache generation
  - `master_client_.Remove` / `master_client_.BatchRemove`
  - remove hot cache entry
  - `mooncake-store/src/client_service.cpp:2756`（`Client::Remove`）
  - `mooncake-store/src/client_service.cpp:2811`（`Client::BatchRemove`）
- `file_storage_` 在 `RealClient` 上，`enable_ssd_offload` 时创建：
  - `mooncake-store/src/real_client.cpp:947`
- `Client` 基类**不持有** `file_storage_`。

**接入点结论：** 为满足"不改变现有接口逻辑"，不在 `Client::Remove` /
`Client::BatchRemove` 里改。改在 `RealClient::remove_internal` /
`RealClient::batchRemove_internal`：`client_->Remove/BatchRemove` 成功后，
调用 `file_storage_->MarkRemoved` / `file_storage_->BatchMarkRemoved`。
`Client::Remove` / `Client::BatchRemove` 本身完全不动。

### 3.3 bucket 数据结构

- `BucketMetadata` 持久化 `data_size`、`keys`、`metadatas`
  （per-key `{offset, key_size, data_size}`）。
  - `mooncake-store/include/storage_backend.h:33`
  - 持久化字段集合：`mooncake-store/include/storage_backend.h:91`
- 运行时字段（不持久化）：`meta_size`、`inflight_reads_`、
  `last_access_ns_`。
- `object_bucket_map_`：key → `StorageObjectMetadata{bucket_id, offset,
  key_size, data_size}`。
- `buckets_`：bucket_id → `shared_ptr<BucketMetadata>`，按 bucket_id
  单调递增。
- `lru_index_`：`set<{last_access_ns_, bucket_id}>`，LRU 候选用。
  - `mooncake-store/include/storage_backend.h:956`（mutex_）
  - `mooncake-store/include/storage_backend.h:973`（lru_index_）

### 3.4 现有 LRU/FIFO eviction（新设计不使用其删除行为）

- `PrepareEviction(required_size)` 在空间超 `max_total_size` 时按
  FIFO/LRU **整 bucket** 删除所有 key。
  - `mooncake-store/src/storage_backend.cpp:2189`
- `SelectEvictionCandidate()`：FIFO 取 `buckets_.begin()`；LRU 取
  `lru_index_` 最小。
  - `mooncake-store/src/storage_backend.cpp:2142`
- `FinalizeEviction()` 等 `inflight_reads_==0` 后删 `.bucket`/`.meta`。
  - `mooncake-store/src/storage_backend.cpp:2245`
- 这套机制会删未删除 key，**不满足约束 1**，新设计不使用它来做空间
  回收。

**关键约束**：`BatchOffload` 现有无条件调用
`PrepareEviction(required_size)`（`storage_backend.cpp:1297`）。新设计
通过 **`eviction_policy=LRU` + `disable_ssd_eviction=true`** 的组合让
`PrepareEviction` 成为 no-op（`storage_backend.cpp:2193`，
`disable_ssd_eviction` 短路返回空 `result`），从而不会删任何 bucket
（含未删除 key），满足约束 1。

这个组合的关键好处是 **复用现有 LRU 排序逻辑**：

- `last_access_ns_` 在 `BatchLoad` 里门控条件是
  `eviction_policy == LRU`（`storage_backend.cpp:1430`），**不看**
  `disable_ssd_eviction`。所以 `LRU + disable_ssd_eviction=true` 下
  `last_access_ns_` 照常更新，冷热信号有效。
- `lru_index_` 在 `BatchOffload` 里无条件 `emplace(0, bucket_id)`
  （`storage_backend.cpp:1354`），照常维护。
- `SelectEvictionCandidate()` 的 LRU 分支（`storage_backend.cpp:2151`）
  可被 GC 复用来选最冷 bucket。

因此 GC **不需要新增** `gc_last_read_ns_` 字段，直接复用 `last_access_ns_`
和 `lru_index_`，减少改动。

空间不足时的行为也由现有代码正确处理：`disable_ssd_eviction=true` 下
`IsEnableOffloading()`（`storage_backend.cpp:1748`）不走"eviction 兜底"
分支，而是走 keys/size 限额检查，空间不足时返回 false，`BatchOffload`
入口（`storage_backend.cpp:998`）直接返回错误，**不写盘、不删 key**。
GC 是独立于 `PrepareEviction` 的新机制，不通过它回收空间。

> 不使用 `eviction_policy=NONE`：那样 `last_access_ns_` 永不更新（门控
> 失效），GC 没有冷热信号；且失去了复用 `lru_index_`/`SelectEvictionCandidate`
> 的机会。
> 不使用 `eviction_policy=LRU` 但不设 `disable_ssd_eviction=true`：
> `PrepareEviction` 会在空间不足时删整 bucket（含未删除 key），违反
> 约束 1。

### 3.5 读路径并发保护

- `BatchLoad` 在共享锁内复制 read plan 并建 `BucketReadGuard`（递增
  `inflight_reads_`），锁外做 IO。
  - `mooncake-store/src/storage_backend.cpp:1377`
  - `mooncake-store/include/storage_backend.h:93`（`BucketReadGuard`）
- `BatchOffload` 写新 bucket 前先 `PrepareEviction` 腾空间，
  duplicate key pre-check。
  - `mooncake-store/src/storage_backend.cpp:1259`
  - `mooncake-store/src/storage_backend.cpp:1329`（duplicate pre-check）

## 4. Design overview

> 关闭现有 LRU/FIFO SSD eviction 作为空间回收手段；`Remove` /
> `BatchRemove` 成功后只在 `BucketStorageBackend` 中标记 key tombstone；
> 后台 GC 只 compact 含 tombstone 的 bucket，通过 copy-on-write 重写
> 未删除 key，最终删除旧 bucket 文件；空间不足时只能回收 tombstone，
> 不能删除其它 key。

三个新概念：

1. **tombstone**：`MarkRemoved` 后，key 从 `object_bucket_map_` 删除
   （本地立即不可见），但旧 bucket 文件仍保留其数据，等待 GC 回收。
2. **GC candidate**：`deleted_bytes_ > 0` 且未在 compact 的 bucket。
3. **compaction**：copy-on-write 重写 live key 到新 bucket，删旧 bucket。

## 5. Detailed design

### 5.1 新接口

`StorageBackendInterface` 增加默认空实现（只有 bucket backend 真正实现）：

```cpp
// 只标记 tombstone，不删文件，不重写 bucket
virtual void MarkRemoved(const std::string& key) {}
virtual void BatchMarkRemoved(const std::vector<std::string>& keys) {}
```

`FileStorage` 转发：

```cpp
void FileStorage::MarkRemoved(const std::string& key) {
    storage_backend_->MarkRemoved(key);  // -> BucketStorageBackend
}
void FileStorage::BatchMarkRemoved(const std::vector<std::string>& keys) {
    storage_backend_->BatchMarkRemoved(keys);
}
```

接入点（`RealClient`）：

```cpp
tl::expected<void, ErrorCode> RealClient::remove_internal(
    const std::string &key, bool force) {
    if (!client_) { ... }
    auto remove_result = client_->Remove(key, force);   // 不动
    if (!remove_result) return tl::unexpected(remove_result.error());
    if (file_storage_) file_storage_->MarkRemoved(key); // 新增
    return {};
}

std::vector<tl::expected<void, ErrorCode>>
RealClient::batchRemove_internal(
    const std::vector<std::string> &keys, bool force) {
    if (!client_) { ... }
    auto results = client_->BatchRemove(keys, force);   // 不动
    if (file_storage_) {
        std::vector<std::string> removed;
        for (size_t i = 0; i < keys.size(); ++i) {
            if (i < results.size() && results[i].has_value()) {
                removed.push_back(keys[i]);
            }
        }
        if (!removed.empty()) file_storage_->BatchMarkRemoved(removed);
    }
    return results;
}
```

`Client::Remove` / `Client::BatchRemove` 不动。`RemoveByRegex` /
`RemoveAll` 不动（它们不调 `MarkRemoved`，且走基类路径，不与 bucket GC
共享数据结构）。

> 边界：若 `Remove` 在 master 成功但 `MarkRemoved` 前 key 已不在本地
> `object_bucket_map_`（例如已被 GC compact 搬走、或本就不在本地），
> `MarkRemoved` 内部按"不存在即返回"处理，幂等安全。

### 5.2 bucket GC 状态

`BucketMetadata` 增加 **runtime-only** 字段（不进 `.meta` 持久化）：

```cpp
std::atomic<int64_t> deleted_bytes_{0};   // 已 MarkRemoved 的字节数
std::atomic<bool>    compacting_{false};  // 是否正在 compact，防重入
```

候选条件：`deleted_bytes_ > 0 && !compacting_`。

**冷热信号复用现有 `last_access_ns_` / `lru_index_`，不新增字段。**
前提是配置 `eviction_policy=LRU + disable_ssd_eviction=true`（见 3.4）：
`disable_ssd_eviction` 让 `PrepareEviction` 不删 bucket（满足约束 1），
`eviction_policy=LRU` 让 `BatchLoad` 照常更新 `last_access_ns_`、
`BatchOffload` 照常维护 `lru_index_`，GC 候选排序直接复用。详见 3.4。

> 不做 key 级冷热分流：GC 是 copy-on-write 整 bucket 重写，一个 bucket
> 的 live key 要么全搬要么不搬；key 级分流需引入"热/冷 bucket 分类"和
> key 级访问频率统计（当前没有），改动大且超出本次范围。对象级热度已由
> master 侧 Count-Min Sketch promotion admission（`master_service.cpp:3421`）
> 处理，与 bucket SSD GC 是两层，不耦合。

不持久化 tombstone 的理由（取舍点 A，第一版）：
- `MarkRemoved` 已从 `object_bucket_map_` 删除，重启后这些 key 在本地
  本就不可见。
- 重启恢复 `ScanMeta` 重建 `object_bucket_map_` 时，master 已删的 key
  若仍出现在 `.meta`，会变成"本地可见、master 不可见"孤儿。
- 第一版接受重启后有少量孤儿，靠后续 offload 复写或人工 `RemoveAll`
  兜底；孤儿不影响正确性，只影响空间利用率。
- 第二版可在 `FileStorage::Init` 的 `ScanMeta` 阶段对每个 key 反查
  master `ExistKey`，master 不存在的就跳过。

### 5.3 `MarkRemoved` 语义（bucket backend）

持 `mutex_` 独占锁：

1. 查 `object_bucket_map_[key]`：
   - 不存在 → 直接返回（幂等，key 不在本地 SSD 或已被删）。
   - 存在 → 记录 `bucket_id`、`data_size + key_size`。
2. 从 `object_bucket_map_` 删除该 key → 本地 `BatchLoad/IsExist` 立即
   不可见。
3. `buckets_[bucket_id]->deleted_bytes_ += (data_size + key_size)`。
4. 返回。**不做任何磁盘 IO。**

`BatchMarkRemoved` 在同一把 `mutex_` 独占锁内批量处理全部 keys，减少
锁竞争；逻辑等价于循环 `MarkRemoved`，但只加一次锁。

### 5.4 GC 触发

后台 GC 线程在 `BucketStorageBackend::Init` 启动，`~BucketStorageBackend`
停止。

触发条件（满足其一）：

1. **定时**：每 `GC_INTERVAL_MS`（默认 1000ms）扫一次候选。
2. **空间阈值**：`total_size_ / max_total_size >=
   GC_HIGH_WATERMARK_RATIO`（默认 0.90）。
3. **删除比例**：某 bucket `deleted_bytes / bucket_data_size >=
   GC_DELETED_RATIO`（默认 0.25）。

每轮只 compact `GC_MAX_BUCKETS_PER_ROUND`（默认 1）个 bucket，限速，
不阻塞前台。

**关键约束**：即使空间阈值触发，也只选 `deleted_bytes_ > 0` 的 bucket。
没有 tombstone 可回收时，GC 不做任何事；offload 路径若仍空间不足，
返回错误（`KEYS_ULTRA_LIMIT` / `INTERNAL_ERROR`），**绝不删未删除 key**。

候选排序（bucket 间冷热分流，复用现有 LRU，第一版简单策略）：

1. `deleted_ratio` 高的优先（主信号：回收收益大）；
2. 相同 ratio 时 `last_access_ns_` 小的优先（冷 bucket 先 compact，
   减少与前台读冲突）。冷 bucket 选择复用
   `SelectEvictionCandidate()`（`storage_backend.cpp:2151`）的 LRU 分支
   逻辑，但**语义不同**：不是选来删，而是选来 compact，且必须叠加
   `deleted_bytes_ > 0` 过滤——`deleted_bytes_==0` 的 bucket 即使最冷
   也不碰。

新增配置（环境变量，默认值保守）：

```
MOONCAKE_OFFLOAD_BUCKET_GC_ENABLE=true
MOONCAKE_OFFLOAD_BUCKET_GC_INTERVAL_MS=1000
MOONCAKE_OFFLOAD_BUCKET_GC_DELETED_RATIO=0.25
MOONCAKE_OFFLOAD_BUCKET_GC_HIGH_WATERMARK_RATIO=0.90
MOONCAKE_OFFLOAD_BUCKET_GC_MAX_BUCKETS_PER_ROUND=1
```

### 5.5 Compaction 流程（copy-on-write）

对一个候选 bucket：

**Step 1 — 锁内快照 live keys：**

- 持 `mutex_` 独占。
- 若 `compacting_` 已为 true，跳过该 bucket。
- 置 `compacting_ = true`。
- 遍历 `bucket->keys`，对每个 key 查 `object_bucket_map_`：
  - 仍在 map 且 `bucket_id` 指向当前 bucket → live。
  - 否则 → 已删除（不进新 bucket）。
- 复制 live keys 的 `{key, offset, key_size, data_size}` 到 read plan。
- 创建 `BucketReadGuard`（保护旧 bucket 文件在读取期间不被删）。
- 释放锁。

**Step 2 — 锁外读旧 bucket live 数据：**

- 用 read plan 从旧 `.bucket` 读 live keys 的数据。
- 不持锁。

**Step 3 — 锁外写新 bucket：**

- 申请新 `bucket_id`。
- `BuildBucket` + `WriteBucket`（复用现有逻辑）写新 `.bucket`/`.meta`。

**Step 4 — 锁内原子切换（二次校验）：**

- 持 `mutex_` 独占。
- 对每个 live key 二次校验
  `object_bucket_map_[key].bucket_id == 旧 bucket`：
  - 仍成立 → 把 mapping 切到新 bucket，更新 metadata。
  - 不成立（compaction 期间该 key 被 `MarkRemoved` 或被新 offload 覆盖）
    → 不切，跳过该 key。
- 新 bucket 加入 `buckets_` 和 `lru_index_`。新 bucket 的
  `last_access_ns_` 继承旧 bucket 的值（冷热度延续，避免 compact 后
  立刻被再次选中）；`deleted_bytes_` 置 0，`compacting_` 置 false。
  `lru_index_` 在 `BatchOffload` 无条件 `emplace(0, id)`
  （`storage_backend.cpp:1354`）时已维护，此处保持兼容；GC 复用它做
  候选排序，不再用于 eviction 删除。
- 旧 bucket 从 `buckets_` 删除。
- 更新 `total_size_`（新 bucket 减去 live key 后的净大小）。
- 释放锁。

**Step 5 — 锁外删旧 bucket 文件：**

- 等旧 bucket `inflight_reads_ == 0`（复用 `FinalizeEviction` 等待逻辑）。
- 删旧 `.bucket`/`.meta`。
- 旧 bucket 的 `BucketReadGuard` 在此期间一直保护文件，函数返回时
  析构。

**边界：**

- live keys 为空 → 跳过 Step 3，直接走 Step 4 切换（删 mapping）+ Step 5
  删旧文件。等价于"整 bucket 都是 tombstone，直接删"。
- compaction 期间又有 key 被 `MarkRemoved` → Step 4 二次校验自然排除。
- compaction 期间有新 offload 写入同名 key → 当前 `BatchOffload` 的
  duplicate 检查（`storage_backend.cpp:1329`）保证不会覆盖已存在 key；
  二次校验比对的是"旧 mapping 指向旧 bucket"，所以该 key 仍被正确搬到
  新 bucket，与新 offload bucket 不会冲突。

### 5.6 失败处理

- Step 2 读旧 bucket 失败 → 放弃本次 compaction，`compacting_=false`，
  保留旧 bucket 不动，下轮重试。不丢数据。
- Step 3 写新 bucket 失败 → 删除已写的临时新文件，`compacting_=false`，
  保留旧 bucket，下轮重试。不丢数据。
- Step 4 二次校验后 live 集合为空 → 删除已写的新 bucket 临时文件，
  直接删旧 bucket（走 Step 5）。不丢数据。
- Step 5 删旧文件失败 → 旧文件成为孤儿磁盘文件，日志告警；下次 GC
  或重启扫描清理。不影响正确性，因为旧 bucket 已从 `buckets_`/
  `object_bucket_map_` 移除，不会被读到。

## 6. Concurrency consistency

| 场景 | 保证 |
|---|---|
| `Remove` vs `BatchLoad` | 读先拿 read plan + guard → 旧文件保留到读完；remove 先删 mapping → 读找不到 key。最终一致。 |
| GC vs `BatchLoad` | 切换前旧 bucket 可读；切换后新读走新 bucket；旧文件等 in-flight 归零再删。 |
| GC vs `Remove` | `MarkRemoved` 持独占锁删 mapping；GC Step 4 二次校验发现 mapping 已不在旧 bucket → 不搬该 key。 |
| GC vs `BatchOffload` | 新 offload 是新 bucket，不冲突；duplicate 检查保护同名 key；二次校验比对旧 mapping 指向旧 bucket。 |
| GC vs GC | `compacting_` 标志防同一 bucket 重入；每轮只处理一个 bucket。 |
| GC vs `RemoveAll`/`RemoveByRegex` | 不同对象、不同数据结构（基类 `file_write_queue_` vs bucket maps），天然隔离。 |

锁使用约定：

- 所有对 `object_bucket_map_`、`buckets_`、`lru_index_` 的读写，沿用
  现有 `mutex_`（SharedMutex）。
- `MarkRemoved`、GC Step 1/Step 4 用独占锁；`BatchLoad` 用共享锁。
- 文件 IO 一律在锁外完成，复用 `BucketReadGuard`/`inflight_reads_`
  保护文件生命周期。
- 冷热信号无需新增更新点：`last_access_ns_` 由现有 `BatchLoad` 在
  `eviction_policy=LRU` 下更新（`storage_backend.cpp:1430`），共享锁下
  relaxed store，无额外锁开销。新设计不动该更新点。

## 7. Scope

### 7.1 第一版做

1. `StorageBackendInterface` 加 `MarkRemoved` / `BatchMarkRemoved` 默认
   空实现。
2. `BucketStorageBackend` 实现 `MarkRemoved` / `BatchMarkRemoved`。
3. `BucketMetadata` 加 runtime-only `deleted_bytes_` / `compacting_`。
4. `BucketStorageBackend` 后台 GC 线程 + compaction；候选排序复用现有
   `last_access_ns_` / `lru_index_`（`SelectEvictionCandidate` LRU 逻辑
   + `deleted_bytes_>0` 过滤），不新增冷热字段。
5. `FileStorage` 转发 `MarkRemoved` / `BatchMarkRemoved`。
6. `RealClient::remove_internal` / `batchRemove_internal` 接入。
7. 新增 GC 配置环境变量；部署文档明确要求
   `MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY=lru` +
   `MOONCAKE_OFFLOAD_DISABLE_SSD_EVICTION=true`。
8. 单元测试：见第 8 节。

### 7.2 第一版不做

- key-level LRU。
- 大小分级 bucket。
- 同步 delete compaction。
- 修改 master 协议 / `LOCAL_DISK` replica 描述。
- 改 `Remove` / `BatchRemove` / `RemoveByRegex` / `RemoveAll` 对外接口。
- 持久化 tombstone / 重启 master 对账清理孤儿（取舍点 A）。
- 让 `RemoveByRegex` / `RemoveAll` 进入 bucket GC。

### 7.3 非目标（明确排除）

- 不删除任何未经 `Remove`/`BatchRemove` 且 master 未确认删除的 key。
- 不用现有 LRU/FIFO `PrepareEviction` 作为空间回收手段（`disable_ssd_eviction=true`
  使其 no-op；LRU 仅复用于 GC 候选排序，不用于 eviction 删除）。

## 8. Testing

### 8.1 单元测试（bucket backend 层）

1. `MarkRemoved` 后 `IsExist` 返回 false，`BatchLoad` 找不到该 key。
2. `MarkRemoved` 不影响同一 bucket 内其它 key 的 `BatchLoad`。
3. `MarkRemoved` 不存在的 key → 幂等成功。
4. GC 触发后：旧 bucket 文件被删，新 bucket 文件存在，live key 仍可
   `BatchLoad`，deleted key 不可。
5. 全部 tombstone 的 bucket → 不写新 bucket，直接删旧文件。
6. compaction 期间并发 `MarkRemoved` live key → 该 key 不进新 bucket，
   新 bucket 正确。
7. compaction 期间并发 `BatchLoad` live key → 读到正确数据，不阻塞。
8. compaction 期间并发 `BatchOffload` 同名 key → 不冲突，duplicate
   检查生效。
9. GC 线程不删 `deleted_bytes_==0` 的 bucket（验证约束 1）。
10. 空间不足且无 tombstone → offload 返回错误，不删任何 live bucket。
11. `eviction_policy=LRU + disable_ssd_eviction=true` 下，`BatchLoad` 仍
    更新 `last_access_ns_`（回归点：验证冷热信号有效，`PrepareEviction`
    是 no-op）。
12. 两个 bucket 相同 `deleted_ratio`，未读的（`last_access_ns_` 小）优先
    被选中 compact（验证冷热分流复用 LRU）。
13. compaction 后新 bucket 的 `last_access_ns_` 继承旧 bucket，且
    `deleted_bytes_==0`、`compacting_==false`。
14. `PrepareEviction` 在 `disable_ssd_eviction=true` 下不删任何 bucket，
    即使空间超 `max_total_size`（验证约束 1 的前提）。

### 8.2 集成测试

- `RealClient::remove` 后，SSD 文件空间最终被回收（poll 磁盘占用
  下降）。
- `BatchRemove` 部分成功（部分 key master 拒绝）→ 只有成功的 key 进
  GC。
- 重启后孤儿 key 不影响正确读写（取舍点 A 验证）。
- 长时间混合 workload：put / get / remove / batch_remove 交替，无数据
  丢失、无死锁。

## 9. Risks & trade-offs

- **写放大**：256MB bucket 只删少量 key 时，compaction 要重写整个 live
  部分。缓解：`GC_DELETED_RATIO=0.25` 阈值，只在收益足够时 compact；
  冷 bucket 优先。
- **重启孤儿**：取舍点 A 接受重启后有少量孤儿。缓解：后续第二版加
  `ScanMeta` + master `ExistKey` 对账。
- **空间不足降级**：无 tombstone 可回收时 offload 失败。这是约束 1 的
  必然结果，非 bug；需在文档/日志中明确。
- **`lru_index_` 复用语义变化**：`lru_index_` 仍由 `BatchOffload` 维护，
  GC 复用它选冷 bucket 做 compaction 候选；但 `PrepareEviction` 因
  `disable_ssd_eviction=true` 成为 no-op，不再用 `lru_index_` 做 eviction
  删除。需在代码注释中说明，避免误用 `SelectEvictionCandidate` 做删除。

## 10. Open questions

- 无（取舍点 A 已定，第一版范围已定）。
