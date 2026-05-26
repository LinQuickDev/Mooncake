# Put/Get 底层逻辑详解

---

## 第一部分：Put/PutBatch

### Put 的完整底层逻辑（单 key）

#### 第1层：Python 绑定（store_py.cpp L2531）

```
store.put(key, value, config)
```
1. 将 Python buffer 转为 C++ `std::span<const char>`
2. **释放 GIL**（`py::gil_scoped_release`）
3. 调用 `store_->put(key, span, config)`

#### 第2层：RealClient（real_client.cpp L1584 → L1535）

`put()` 调用 `put_internal()`，逻辑如下：

1. **参数校验**：检查 client 是否初始化、allocator 是否存在
2. **分配缓冲区**：`client_buffer_allocator_->allocate(value.size_bytes())`
   - 为什么需要分配？因为用户的数据可能在任意内存位置，而 RDMA 传输要求内存必须是"注册过的"（MR，Memory Region）。`client_buffer_allocator_` 分配的就是已注册的 RDMA 内存
3. **拷贝数据**：`memcpy(buffer_handle.ptr(), value.data(), value.size_bytes())`
   - 把用户数据从普通内存拷贝到 RDMA 注册内存
4. **切分 Slice**：`split_into_slices(buffer_handle)`
   - 如果数据量大于 `kMaxSliceSize`（默认 256KB），需要切成多个 Slice。每个 Slice 是一段连续内存的描述符（指针+长度），对应一次 RDMA 写操作
5. 调用 `client_->Put(key, slices, config)` 进入第3层

#### 第3层：Client 服务层（client_service.cpp L1237）

`Client::Put()` 逻辑如下：

1. **PutStart**：`master_client_.PutStart(key, slice_lengths, config)`
   - 向 Master 发送 RPC，申请为这个 key 分配 replica
   - Master 选择目标 segment，返回 replica 描述符列表（每个描述符包含目标内存地址、大小、传输端点等）
   - 如果 key 已存在，返回 `OBJECT_ALREADY_EXISTS`，直接返回成功
   - 如果没有可用空间，返回 `NO_AVAILABLE_HANDLE`

2. **处理磁盘副本**（如果有的话）：
   - **逆序遍历** replica 列表（`rbegin/rend`），找磁盘类型的副本
   - 调用 `PutToLocalFile(key, slices, disk_descriptor)` 写本地磁盘
   - **只处理一个磁盘副本就 break**

   > **为什么只处理一个磁盘副本？** 因为磁盘副本是写入本地存储后端的，一个 Client 只有一个 `storage_backend_`，即使 Master 分配了多个磁盘副本描述符，当前 Client 只能写入自己本地的磁盘，无需重复写入。注释也明确说明：`// Only one disk replica is needed`。
   >
   > **那其他磁盘副本怎么办？** Master 分配的多个磁盘副本描述符指向不同节点的磁盘。当前 Client 只负责写入自己本地的那个磁盘副本（通过 `storage_backend_`），其他节点的磁盘副本由 Master 协调其他节点来写入，或者由 Master 在后续的 rebalance 流程中补齐。Client 的职责就是：本地有 `storage_backend_` 就写一个本地磁盘副本，其余的不管。
   >
   > **为什么必须先处理磁盘副本？** 因为 `PutToLocalFile` 是异步的——数据拼接在调用线程同步完成，但实际磁盘 I/O 和 `PutEnd(DISK)`/`PutRevoke(DISK)` 在 `write_thread_pool_` 中异步执行。先启动磁盘写入，可以尽早触发异步的 `PutEnd(DISK)`，避免与后续内存副本的 `PutEnd(MEMORY)` 产生竞态。

3. **处理内存副本**：
   - 遍历 replica 列表，找内存类型的副本
   - 对**每个**内存副本调用 `TransferWrite(replica, slices)`

4. **TransferWrite → TransferData**：
   - `transfer_submitter_->submit(replica, slices, WRITE)` — 提交异步 RDMA/TCP 写传输
   - 返回 `TransferFuture`
   - `future->get()` — 阻塞等待传输完成
   - 传输层根据协议选择策略：RDMA 直接写远端内存，TCP 通过 socket 传输

5. **传输结果处理**：
   - 成功：`master_client_.PutEnd(key, MEMORY)` — 通知 Master 本次 Put 完成，replica 正式生效
   - 失败：`master_client_.PutRevoke(key, MEMORY)` — 通知 Master 撤销本次 Put，释放已分配的 replica

---

### PutBatch 的完整底层逻辑（批量 key）

#### 第1层：Python 绑定（store_py.cpp L2574）

```
store.put_batch(keys, values, config)
```
1. 将所有 Python buffer 转为 `std::vector<std::span<const char>>`
2. **释放 GIL**
3. 调用 `store_->put_batch(keys, spans, config)`

#### 第2层：RealClient（real_client.cpp L1682 → L1599）

`put_batch()` 调用 `put_batch_internal()`，逻辑如下：

1. **参数校验**：检查 keys 和 values 大小是否匹配
2. **循环逐 key 处理**（串行）：
   - 对每个 key-value 对：
     - `allocator->allocate(value.size_bytes())` — 分配 RDMA 注册内存
     - `memcpy(buffer_handle.ptr(), value.data(), value.size_bytes())` — 拷贝数据
     - `split_into_slices(buffer_handle)` — 切分 Slice
   - 将所有 key 的 slices 收集到 `batched_slices` map 中
3. 调用 `client_->BatchPut(keys, ordered_batched_slices, config)` 进入第3层

#### 第3层：Client 服务层（client_service.cpp L2055）

`Client::BatchPut()` 逻辑如下，分为 **6 个阶段**：

1. **CreatePutOperations**：为每个 key 创建 `PutOperation` 对象，包含 key 和对应的 slices

2. **StartBatchPut**：
   - 调用 `master_client_.BatchPutStart(keys, slice_lengths, config)`
   - 一次 RPC 批量为所有 key 分配 replica
   - 每个返回结果可能是成功（带 replica 列表）或失败
   - 失败的 op 标记错误，后续阶段跳过

3. **SubmitTransfers**（提交阶段）：
   - 对每个未失败的 op：
     - 如果有磁盘副本：**逆序遍历找磁盘副本 → `PutToLocalFile()` 写磁盘 → 只处理一个就 break**（与 Put 相同逻辑）
     - 对每个内存副本：`transfer_submitter_->submit(replica, slices, WRITE)` 提交异步传输
     - 返回的 `TransferFuture` 存入 `op.pending_transfers`
     - 如果任一 replica 提交失败，标记 op 错误，清空 pending_transfers

4. **WaitForTransfers**（等待阶段）：
   - 对每个有 pending_transfers 的 op：
     - 遍历所有 `TransferFuture`，调用 `future.get()` 阻塞等待
     - 如果任一传输失败，记录首个错误，标记 op 失败
     - 注意：即使有失败，也会等待所有 future 完成，避免资源泄漏

5. **FinalizeBatchPut**（收尾阶段）：
   - 将 op 分为三类：
     - **传输成功的 op**：调用 `master_client_.BatchPutEnd(successful_keys)` — 批量确认，replica 正式生效
     - **传输失败但已分配 replica 的 op**：调用 `master_client_.BatchPutRevoke(failed_keys)` — 批量撤销，释放 replica
     - **早期失败的 op**（未分配 replica）：无需清理

6. **CollectResults**：
   - 从每个 PutOperation 收集结果
   - `OBJECT_ALREADY_EXISTS` 视为成功
   - 返回 `vector<expected<void, ErrorCode>>`

---

### Put 关键区别总结

| 维度 | Put（单key） | PutBatch（批量） |
|------|-------------|----------------|
| Master RPC | 1次 PutStart + 1次 PutEnd/Revoke | 1次 BatchPutStart + 1次 BatchPutEnd + 1次 BatchPutRevoke |
| 传输方式 | 同步：submit → 立即 get() 等待 | 异步批量：先 submit 所有 → 再统一 wait 所有 |
| 失败处理 | 传输失败立即 PutRevoke | 传输失败后统一在 FinalizeBatchPut 中 BatchPutRevoke |
| 磁盘副本 | 只处理1个本地磁盘副本 | 每个op只处理1个本地磁盘副本 |
| 数据拷贝 | 1次 memcpy + split | N次 memcpy + split（逐key串行） |

---

### 拷贝路径 vs 零拷贝路径

| API | 是否 memcpy | buffer 来源 | 底层调用 |
|-----|-----------|------------|---------|
| `put` / `put_parts` / `put_batch` | 是 | `client_buffer_allocator_->allocate()` 分配 | `client_->Put()` / `client_->BatchPut()` |
| `put_from` / `batch_put_from` / `batch_put_from_multi_buffers` | 否（零拷贝） | 用户提供的外部 buffer 指针 | `client_->Put()` / `client_->BatchPut()` |

拷贝路径中，`client_buffer_allocator_` 分配的 buffer 是注册过 RDMA 的内存区域，`memcpy` 将用户数据拷贝进去后才能进行零拷贝 RDMA 传输。而 `put_from`/`batch_put_from` 系列要求用户提前通过 `register_buffer()` 注册内存，从而跳过 memcpy 步骤。

---

### 零拷贝路径详解：put_from / batch_put_from / batch_put_from_multi_buffers

#### 三者的差异

| API | 每个 key 对应的 buffer | Slice 构造方式 | 底层调用 |
|-----|----------------------|--------------|---------|
| `put_from` | 1个连续 buffer | 按 `kMaxSliceSize` 手动切片 | `client_->Put()` |
| `batch_put_from` | 1个连续 buffer | 按 `kMaxSliceSize` 手动切片 | `client_->BatchPut()` |
| `batch_put_from_multi_buffers` | 多个不连续 buffer | 每个 buffer 直接作为一个 Slice | `client_->BatchPut()` |

**除了 buffer 来源和 Slice 构造方式不同，三者进入 `client_->Put()` / `client_->BatchPut()` 之后的流程完全相同**——都要经历 PutStart → 磁盘副本处理 → 内存副本传输 → PutEnd/Revoke 的完整流程。

`batch_put_from_multi_buffers` 的典型场景：一个对象的数据分散在多个不连续的 GPU 内存区域中（例如 vLLM 中 KV cache 的多个 layer tensor），每个区域单独注册，然后直接作为 Slice 传入，无需先拼接成连续内存。

#### register_buffer 的实现与收益

`register_buffer_internal` 的调用链：

```
RealClient::register_buffer_internal(buffer, size)
  → Client::RegisterLocalMemory(buffer, size, location, ...)
    → TransferEngine::registerLocalMemory(buffer, size, ...)
      → RdmaTransport::registerLocalMemoryInternal(buffer, size, ...)
        → RdmaContext::registerMemoryRegion(addr, length, access)
          → ibv_reg_mr(pd_, addr, length, access)    // CPU 内存
          → ibv_reg_dmabuf_mr(pd_, ..., dmabuf_fd, access)  // GPU 内存
```

**RDMA 内存注册做了什么？**

1. **CPU 内存**：调用 `ibv_reg_mr()` 将内存页锁定（pin），并注册到 RDMA 保护域（Protection Domain），获取 `lkey`/`rkey`。锁定后，RDMA NIC 可以直接通过 DMA 访问这些内存，无需 CPU 介入。
2. **GPU 内存**：通过 CUDA 的 DMA-BUF 机制获取文件描述符，再调用 `ibv_reg_dmabuf_mr()` 注册，RDMA NIC 可以直接从 GPU 显存读取数据并发送到远端，无需先拷贝到 CPU。

**register_buffer 的核心收益：**

1. **消除数据拷贝**：`put_internal` 需要将数据 memcpy 到共享内存池的内部缓冲区，而 `put_from_internal` 直接从已注册的用户缓冲区创建 Slice，省去了这次拷贝。对于大块数据（如 GPU 上的 KV cache tensor），可以显著降低延迟和 CPU 开销。

2. **RDMA 零拷贝传输**：`ibv_reg_mr()` 将内存页锁定（pin），RDMA NIC 可以直接通过 DMA 访问这些内存，无需 CPU 介入。未注册的内存无法被 RDMA NIC 直接访问。

3. **GPU 内存直传**：通过 `ibv_reg_dmabuf_mr()` 注册 GPU 内存，RDMA NIC 可以直接从 GPU 显存读取数据并发送到远端，无需先拷贝到 CPU 再发送。

4. **一次注册，多次使用**：`register_buffer` 通常在初始化阶段调用一次（例如 vLLM 启动时注册整个 KV cache 内存池），之后所有 `put_from`/`get_into` 操作都可以零拷贝地复用该注册。`ibv_reg_mr` 本身是一个昂贵的操作（涉及页表锁定和 NIC 映射），避免每次操作都重新注册是关键优化。

5. **子区域支持**：通过 `resolve_registered_buffer`，注册一个大区域后，可以对其中的任意子区域进行零拷贝操作，灵活支持 tensor 切片等场景。

---

### PutToLocalFile：磁盘副本写入机制

`PutToLocalFile`（client_service.cpp L2593）的完整流程：

**阶段1：同步数据暂存（调用线程）**

1. 遍历所有 Slice，计算总大小
2. 对每个 Slice：
   - 如果是 GPU 指针（`IsDevicePointer` 检测）→ 通过 Pinned Buffer Pool 做 D2H（Device-to-Host）拷贝，暂存到 `std::string`
   - 如果是普通主机内存指针 → 直接 append 到 `std::string`
3. 这一步必须在调用线程同步完成，因为 BatchPut 还未返回给 Python，GPU buffer 不会被复用

**阶段2：异步磁盘写入（write_thread_pool_）**

1. `storage_backend_->StoreObject(path, value, key)` 写入磁盘文件
2. 写入成功 → `master_client_.PutEnd(key, DISK)` + 处理驱逐通知
3. 写入失败 → `master_client_.PutRevoke(key, DISK)`

**磁盘写入没有使用 RDMA 或高速网络通道**。内存副本通过 `TransferWrite` → `transfer_submitter_` 走传输引擎（可能使用 RDMA/TCP），但磁盘副本走的是纯本地文件 I/O 路径。

底层文件 I/O 有三种实现（根据编译选项和配置选择）：

| 后端 | 条件 | 特点 |
|------|------|------|
| **PosixFile** | 默认 | POSIX `preadv`/`pwritev`，普通本地文件 I/O |
| **UringFile** | 编译时 `USE_URING` + 运行时配置 | Linux `io_uring` 异步 I/O，读操作启用 `O_DIRECT` 绕过页缓存 |
| **ThreeFSFile** | 编译时 `USE_3FS` | 3FS 分布式文件系统（高性能用户态文件系统），通过 `hf3fs_reg_fd` 注册 |

此外，存储后端有三种架构模式：

| 模式 | 类名 | 特点 |
|------|------|------|
| `kFilePerKey` | `StorageBackendAdaptor` | 每个 key 一个文件，序列化为 protobuf |
| `kBucket` | `BucketStorageBackend` | 多个 key 聚合到一个 bucket 文件，支持 FIFO/LRU 驱逐 |
| `kOffsetAllocator` | `OffsetAllocatorStorageBackend` | 单一预分配数据文件 + offset allocator |

**总结**：磁盘副本的写入就是普通的本地文件 I/O，没有 RDMA。可选的 `io_uring` 和 `3FS` 是本地 I/O 路径上的优化，不是网络传输加速。GPU 数据需要先做 D2H 拷贝到主机内存，再写入磁盘。

---

## 第二部分：Get/GetBatch

### Get 的完整底层逻辑（单 key）

#### 第1层：Python 绑定（store_py.cpp L398）

```
store.get(key)  →  py::bytes
```

1. **性能打点**：`UbDiag::PerfPoint pt(PerfKey::GET_STORE_PY_GET)`
2. **初始化检查**：`is_client_initialized()` — 若未初始化，返回 `py::bytes("\\0", 0)`
3. **释放 GIL**：`py::gil_scoped_release release_gil` — 避免阻塞 Python 其他线程
4. **调用底层**：`store_->get_buffer(key)` — 返回 `shared_ptr<BufferHandle>`
5. **空指针检查**：若 `buffer_handle` 为空，返回 `kNullString`
6. **重新获取 GIL**：`py::gil_scoped_acquire acquire_gil`
7. **数据转换**：将 `buffer_handle->ptr()` 和 `buffer_handle->size()` 转为 `pybind11::bytes` 返回

#### 第2层：RealClient（real_client.cpp L2497 → L2374）

`get_buffer()` 调用 `get_buffer_internal()`，逻辑如下：

1. **参数校验**：检查 client 是否初始化、allocator 是否存在

2. **查询元数据**：`client_->Query(key)`
   - 成功 → 获取 replica 列表
   - 失败且为 `OBJECT_NOT_FOUND` 或 `REPLICA_IS_NOT_READY` → 静默返回 nullptr
   - 其他错误 → LOG(ERROR) + 返回 nullptr

3. **选择最优副本**：`SelectBestReplica(replica_list, local_endpoints)`
   - 优先级：**本地 MEMORY > 远端 MEMORY > LOCAL_DISK > DISK**
   - 只考虑 `status == COMPLETE` 的副本
   - 无可用副本 → 返回 nullptr

4. **计算总大小**：`calculate_total_size(replica)`
   - MEMORY → `buffer_descriptor.size_`
   - DISK → `disk_descriptor.object_size`
   - LOCAL_DISK → `local_disk_descriptor.object_size`
   - total_length == 0 → 返回 nullptr

5. **分配缓冲区**：`client_buffer_allocator->allocate(total_length)`
   - 失败 → 返回 nullptr

6. **分支处理（根据副本类型）**：

   **[分支A] LOCAL_DISK 副本**：
   - 调用 `batch_get_into_offload_object_internal(endpoint, objects)`
   - 通过 RPC 从远端节点的 SSD 读取数据
   - 数据直接写入分配的 buffer

   **[分支B] MEMORY / DISK 副本**：
   - `allocateSlices(slices, replica, buffer_handle->ptr())`：构造 Slice 描述符
     - MEMORY：单个 `Slice{buffer_ptr, handle.size_}`
     - DISK：按 `kMaxSliceSize` 分片
   - `FilterQueryResult(query_result, replica)`：构造仅包含选定副本的 QueryResult，防止 Client::Get 内部选错副本
   - `client_->Get(key, filtered_qr, slices)` → 进入第3层

#### 第3层：Client 服务层（client_service.cpp L787）

`Client::Get(key, query_result, slices)` 逻辑如下：

1. **查找完整副本**：`FindFirstCompleteReplica(query_result.replicas, replica)`
   - 遍历副本列表，找第一个 `status == COMPLETE` 的副本
   - 失败 → `INVALID_REPLICA`

2. **Hot Cache 检查**（仅 MEMORY 副本）：
   - `RedirectToHotCache(object_key, replica)`
   - 如果本地 Hot Cache 有该 key 的数据：
     - 获取本地缓存块引用
     - 大小匹配检查
     - 修改 replica 的 `buffer_address` 指向本地缓存地址
     - `transport_endpoint` 设为 `local_hostname_`（变为本地传输）
   - 缓存未命中 → 不修改 replica，走正常远程传输

3. **TransferRead**：执行实际数据传输
   - `TransferRead(replica, slices)` → `TransferData(replica, slices, READ)`
   - `transfer_submitter_->submit(replica, slices, READ)` → 返回 `TransferFuture`
   - `future->get()` → 阻塞等待传输完成
   - 传输策略选择：
     - **本地传输**（源和目标在同一节点）→ `MemcpyWorkerPool` 异步 memcpy
     - **远程传输**（跨节点）→ `TransferEngine` 提交 RDMA/TCP/CXL 传输请求
     - **文件读取**（DISK 副本）→ `FilereadWorkerPool` 异步文件读取

4. **释放 Hot Cache**：`hot_cache_->ReleaseHotKey(object_key)`（如果使用了缓存）

5. **Hot Cache 频率准入**：`ShouldAdmitToHotCache(key, cache_used)`
   - 使用 CountMinSketch 统计访问频率
   - 超过阈值 → `ProcessSlicesAsync(key, slices, replica)` 异步将数据写入本地 Hot Cache
   - `cache_used=true` 时跳过（已从缓存服务，无需再提升）

6. **Lease 过期检查**：`query_result.IsLeaseExpired()` → `LEASE_EXPIRED` 错误

---

### GetBatch 的完整底层逻辑（批量 key）

#### 第1层：Python 绑定（store_py.cpp L425）

```
store.get_batch(keys)  →  list[py::bytes]
```

1. **性能打点**：`PerfKey::GET_STORE_PY_GET_BATCH`
2. **初始化检查**：未初始化返回 `{kNullString}`
3. **释放 GIL**：`py::gil_scoped_release release_gil`
4. **调用底层**：`store_->batch_get_buffer(keys)` — 返回 `vector<shared_ptr<BufferHandle>>`
5. **空结果检查**：若 `batch_data.empty()`，返回 `{kNullString}`
6. **重新获取 GIL**
7. **逐项转换**：遍历 `batch_data`，对每个非空项转为 `pybind11::bytes`，空项转为 `kNullString`

#### 第2层：RealClient（real_client.cpp L2908 → L2682）

`batch_get_buffer()` 调用 `batch_get_buffer_internal()`，逻辑如下：

1. **参数校验**：检查 client 是否初始化、keys 是否为空

2. **批量查询元数据**：`client_->BatchQuery(keys)`

3. **逐 key 准备操作**：
   - 遍历每个 key 的 query_result
   - 查询失败（`OBJECT_NOT_FOUND` / `REPLICA_IS_NOT_READY`）→ 静默跳过
   - `SelectBestReplica` 选择最优副本
   - `calculate_total_size` 计算大小
   - `client_buffer_allocator->allocate` 分配缓冲区
   - `allocateSlices` 构造 slices
   - **分类为两类操作**：
     - `valid_ops`（MEMORY / DISK 副本）→ 走 `client_->BatchGet`
     - `disk_ops`（LOCAL_DISK 副本）→ 走 SSD RPC

4. **执行 MEMORY/DISK 批量传输**：
   - 收集所有 valid_ops 的 keys、query_results、slices
   - `client_->BatchGet(batch_keys, batch_query_results, batch_slices)` → 进入第3层
   - 成功的 key → 将 buffer_handle 放入 `final_results`
   - 失败的 key → LOG(ERROR)，对应位置保持 nullptr

5. **执行 LOCAL_DISK 批量传输**：
   - 按 `transport_endpoint` 分组
   - 对每个 endpoint 调用 `batch_get_into_offload_object_internal(endpoint, objects)`
   - 成功 → 放入 `final_results`
   - 失败 → LOG(ERROR)

#### 第3层：Client 服务层（client_service.cpp L1034）

`Client::BatchGet(keys, query_results, slices)` 逻辑如下：

1. **前置检查**：`transfer_submitter_` 是否初始化、query_results 大小是否匹配

2. **分支判断**：
   - `prefer_alloc_in_same_node=true` → 走 `BatchGetWhenPreferSameNode`（按 endpoint 分组批量提交）
   - 默认路径 → 走下面的并行提交+等待流程

3. **阶段A：并行提交所有传输**：
   - 对每个 key：
     - `FindFirstCompleteReplica` → 找 COMPLETE 副本
     - `RedirectToHotCache` → 检查/重定向到本地缓存
     - `transfer_submitter_->submit(replica, slices, READ)` → 异步返回 `TransferFuture`
     - 存入 `pending_transfers: (index, key, future, replica, cache_used)`
   - 提交失败 → 释放 Hot Cache + 记录错误

4. **阶段B：等待所有传输完成**：
   - 对每个 pending_transfer：
     - `future.get()` → 等待结果
     - 释放 Hot Cache（如果使用了）
     - 成功 → 检查是否应提升到 Hot Cache（`ShouldAdmitToHotCache` → `ProcessSlicesAsync`）
     - 失败 → `results[index] = error`

5. **阶段C：批量 Lease 过期检查**：
   - 统一用当前时间检查所有 query_results 的 lease
   - 过期 → `LEASE_EXPIRED`

---

### Get 关键区别总结

| 维度 | Get（单key） | GetBatch（批量） |
|------|-------------|----------------|
| Master RPC | 1次 Query | 1次 BatchQuery |
| 传输方式 | 同步：submit → 立即 get() 等待 | 异步批量：先 submit 所有 → 再统一 wait 所有 |
| 副本选择 | SelectBestReplica 选1个 | 每个 key 各自 SelectBestReplica |
| Hot Cache | 单 key 检查/准入/释放 | 批量检查/准入/释放，统计缓存命中率 |
| LOCAL_DISK | 单 key RPC | 按 endpoint 分组批量 RPC |
| 错误处理 | 单 key 失败直接返回错误 | 每个 key 独立返回结果，互不影响 |

---

### Get 的副本类型与传输路径

| 副本类型 | 数据位置 | 传输方式 | 代码路径 |
|---------|---------|---------|---------|
| **MEMORY** | 远端节点内存 | RDMA/TCP/CXL（远程）或 memcpy（本地） | `TransferRead` → `transfer_submitter_->submit(READ)` |
| **DISK** | 本地磁盘文件 | 本地文件 I/O | `TransferRead` → `FilereadWorkerPool` |
| **LOCAL_DISK** | 远端节点 SSD | RPC 到远端节点读 SSD | `batch_get_into_offload_object_internal` |

**注意**：DISK 和 LOCAL_DISK 的区别——DISK 是本地磁盘文件，LOCAL_DISK 是远端节点的 SSD（名称容易混淆）。LOCAL_DISK 通过 offload RPC 让远端节点从其 SSD 读取数据后通过网络返回。

---

### 深度解析1：MEMORY 本地和远端都走 submitMemoryReadOperation 吗？

**是的，本地和远端 MEMORY 副本都走 `submitMemoryReadOperation()`**，内部通过 `selectStrategy()` 自动区分：

```
submit(replica, slices, READ)
│
├─ replica.is_memory_replica() == true
│   └─ submitMemoryReadOperation(handle, slices, 0)
│       └─ selectStrategy(handle, slices)
│           │
│           ├─ isLocalTransfer(handle) == true
│           │   → LOCAL_MEMCPY → submitMemcpyOperation()
│           │     直接 std::memcpy 或 gpu_staging::CopyAuto
│           │     提交到 MemcpyWorkerPool（1个线程，受内存带宽限制）
│           │
│           └─ isLocalTransfer(handle) == false
│               → TRANSFER_ENGINE → submitTransferEngineOperation()
│                 engine_.openSegment(endpoint) + engine_.submitTransfer()
│                 RDMA/TCP/CXL 远程传输
│
└─ replica.is_memory_replica() == false（DISK）
    └─ submitFileReadOperation()
        → FilereadWorkerPool（本地文件 I/O，10个线程）
```

**`selectStrategy` 的判断逻辑**（transfer_task.cpp L798）：

1. `memcpy_enabled_ == false` → 强制 `TRANSFER_ENGINE`（环境变量 `MC_STORE_MEMCPY` 控制，有 RDMA 时禁用 memcpy）
2. `isLocalTransfer(handle) == true` → `LOCAL_MEMCPY`（比较 `handle.transport_endpoint_` 与本机端点）
3. 默认 → `TRANSFER_ENGINE`

**`isLocalTransfer` 的判断**：将副本的 `transport_endpoint_` 与 `engine_.getLocalIpAndPort()` 比较，相同则说明数据在本机，走 memcpy；否则走 Transfer Engine 远程传输。

**注意**：`submit()` 的 `is_memory_replica() == false` 分支（即 `submitFileReadOperation`）只处理 DISK 副本，**不处理 LOCAL_DISK**。LOCAL_DISK 在 RealClient 层就被提前拦截，走独立的 RPC 路径。

---

### 深度解析2：LOCAL_DISK 为什么不能走策略模式？

LOCAL_DISK 不能走 `TransferSubmitter::submit()` 的策略模式，根本原因是**数据不在本机，也不在共享文件系统上，而是在远端 Worker 节点的本地 SSD 上**。

三种副本的数据位置和访问方式完全不同：

| 副本类型 | 数据位置 | 访问方式 |
|---------|---------|---------|
| **MEMORY** | 某个节点的内存中（有 `buffer_address` + `transport_endpoint`） | 直接通过传输引擎 RDMA/TCP 读取，或本地 memcpy |
| **DISK** | 本机的共享文件系统路径（有 `file_path`） | 本地文件 I/O（`FilereadWorkerPool`） |
| **LOCAL_DISK** | 远端 Worker 节点的本地 SSD（有 `client_id` + `transport_endpoint`） | **两阶段**：先 RPC 让远端从 SSD 读到内存，再通过传输引擎拉取 |

LOCAL_DISK 无法走策略模式的原因：

**1. 需要先触发远端 SSD 读取**

MEMORY 副本的数据已经在内存中，`buffer_address` 直接可用；DISK 副本的 `file_path` 在本机，可以直接文件 I/O。但 LOCAL_DISK 的数据在远端 SSD 上，**远端必须先执行一次 SSD → 内存的数据搬运**，客户端才能通过传输引擎读取。这个"远端 SSD 读取"步骤无法在 `submit()` 内部完成，因为 `submit()` 只负责本机侧的传输调度。

**2. 两阶段协议需要状态协调**

LOCAL_DISK 的完整读取流程：

```
客户端                                    远端 Worker 节点
  |                                          |
  |--- RPC: batch_get_offload_object ------->| 1. FileStorage::BatchGet()
  |    (keys, sizes)                         |    AllocateBatch() → 分配对齐内存缓冲区
  |                                          |    BatchLoad() → 从 SSD 读取到缓冲区
  |<--- RPC Response -----------------------|    返回 {pointers, transfer_engine_addr, gc_ttl_ms}
  |                                          |
  |=== RDMA/传输引擎 READ ===================| 2. 传输引擎将远端缓冲区数据拉到本地
  |    source=本地slice地址                  |    (远端内存 → 本地内存/GPU显存)
  |    target=远端segment+pointer偏移        |
  |                                          |
  |--- RPC: release_offload_buffer --------->| 3. 释放远端缓冲区（fire-and-forget）
  |                                          |
```

这个两阶段协议涉及：RPC 调用 → 等待远端 SSD I/O → 获取远端内存地址 → 传输引擎拉取 → 释放远端缓冲区。中间有多个状态需要协调（远端缓冲区的分配、GC 租约、主动释放），无法用 `submit()` 的单次提交+等待模式表达。

**3. 远端缓冲区有生命周期管理**

远端 Worker 为 LOCAL_DISK 读取分配了临时内存缓冲区，有 `gc_ttl_ms`（默认 5000ms）租约。如果客户端超时未完成传输，远端 GC 线程会回收缓冲区，数据丢失。客户端必须在传输完成后主动调用 `release_offload_buffer` 释放，加速缓冲区回收。这种跨节点的缓冲区生命周期管理超出了 `TransferSubmitter` 的职责范围。

---

### 深度解析3：LOCAL_DISK 的读取速度比 DISK 快吗？

**通常 LOCAL_DISK 更快**，原因如下：

**1. 传输路径对比**

| 阶段 | DISK | LOCAL_DISK |
|------|------|------------|
| 数据位置 | 本机共享文件系统 | 远端 Worker SSD |
| 读取方式 | 本地文件 I/O（preadv/io_uring） | 远端 SSD → 远端内存 → RDMA → 本地 |
| 目标内存 | 只能写 CPU 内存 | 可直接写 GPU 内存 |
| 网络开销 | 无 | 有（RPC + RDMA） |

**2. 为什么 LOCAL_DISK 通常更快？**

- **SSD 性能优势**：LOCAL_DISK 存储在 Worker 节点的本地 NVMe SSD 上，随机读写性能远高于 DISK 使用的共享文件系统（可能是 NFS、CephFS 等网络文件系统，或普通 HDD）
- **并行读取**：LOCAL_DISK 的远端 Worker 使用专用线程池并行从 SSD 读取（`coro_io::post`），然后通过 RDMA 高速传输回来；DISK 的本地文件 I/O 虽然也用 `FilereadWorkerPool`（10线程），但受限于共享文件系统的 I/O 能力
- **GPU 直写**：LOCAL_DISK 通过 RDMA 传输可以直接写入 GPU 内存（用户通过 `register_buffer` 预注册），而 DISK 的本地文件 I/O 只能写 CPU 内存，如果目标是 GPU 内存还需要额外的 D2H 中转
- **io_uring 优化**：LOCAL_DISK 的远端 Worker 读取 SSD 时可以使用 io_uring + O_DIRECT 零拷贝读取，避免内核态拷贝

**3. 什么情况下 DISK 可能更快？**

- 数据量很小（网络开销占比大）
- 本地共享文件系统使用 NVMe SSD 且网络带宽有限
- RDMA 网络不可用，回退到 TCP 传输

**4. 速度排序总结**

```
本地 MEMORY（memcpy）> 远端 MEMORY（RDMA）> LOCAL_DISK（远端SSD+RDMA）> DISK（本地文件I/O）
```

这个排序与 `SelectBestReplica` 的优先级一致：系统自动选择最快的可用副本。

---

### 深度解析4：为什么 LOCAL_DISK 优先级高于 DISK？

`SelectBestReplica` 的优先级：**本地 MEMORY > 远端 MEMORY > LOCAL_DISK > DISK**

LOCAL_DISK 优先于 DISK 的原因：

**1. 数据来源的可靠性不同**

| 维度 | DISK | LOCAL_DISK |
|------|------|------------|
| 数据位置 | Master 管理的共享文件系统路径（`file_path`） | Worker 节点的本地 SSD |
| 创建者 | Master 在 PutStart 时自动创建 | Worker 完成数据卸载后通知 Master 创建 |
| 归属关系 | 无 client_id，全局共享 | 绑定到特定 client_id + transport_endpoint |
| 生命周期 | 随对象存在 | 绑定到客户端，客户端失活时被清理 |

LOCAL_DISK 的数据由活跃 Worker 写入并管理，数据新鲜且确定可用；而 DISK 是 Master 侧共享文件系统上的数据，可能存在路径解析、文件系统可用性等额外风险。

**2. 传输路径的灵活性不同**

- **LOCAL_DISK**：RPC + RDMA 传输路径，数据可以直接写入 GPU 内存（用户通过 `register_buffer` 预注册）
- **DISK**：本地文件 I/O（`FilereadWorkerPool`），只能写 CPU 可寻址的内存，如果分配器返回了 GPU 内存则读取会失败，需要额外的临时 CPU 缓冲区中转

**3. 架构定位不同**

LOCAL_DISK 是较新的架构设计——当内存不足时，Worker 将 MEMORY 副本卸载（offload）到本地 SSD，形成 MEMORY → LOCAL_DISK 的降级路径。这是数据生命周期中的自然降级，数据仍然由活跃 Worker 管理。而 DISK 是更早期的、由 Master 直接管理的共享存储机制，属于完全不同的存储层。

**4. transport_endpoint 的含义差异**

| 副本类型 | transport_endpoint 含义 |
|---------|----------------------|
| MEMORY | 内存段所在节点的传输引擎端点（RDMA NIC 地址） |
| DISK | 无 transport_endpoint（DiskDescriptor 中没有此字段） |
| LOCAL_DISK | 拥有该 SSD 数据的 Worker 节点的 **RPC 服务地址** |

LOCAL_DISK 有明确的 `transport_endpoint`（Worker 的 RPC 地址），客户端可以直接发起远程读取；而 DISK 没有远程端点，只能本地文件 I/O。

---

### 深度解析5：为什么使用了 Hot Cache 就要释放？

在 `Client::Get` 和 `Client::BatchGet` 中，如果使用了 Hot Cache（`cache_used=true`），在数据传输完成后必须调用 `ReleaseHotKey(key)`。这不是"释放缓存数据"，而是**释放对缓存块的引用计数**。

**Hot Cache 的引用计数机制：**

```
GetHotKey(key)     →  ref_count++  （获取引用，保护缓存块不被驱逐）
  ↓
数据传输/memcpy     →  从 blk->addr 读取数据到用户 buffer
  ↓
ReleaseHotKey(key) →  ref_count--  （释放引用，允许缓存块重新可被驱逐）
```

**`GetHotKey` 做了什么？**（local_hot_cache.cpp L115）

1. 在 `key_to_lru_it_` 中查找 key
2. 找到后 `ref_count++`（原子操作）——这相当于对缓存块加了一把"读锁"
3. `accessed = true`（延迟 LRU touch 标志）
4. 返回 `HotMemBlock*` 指针

**`RedirectToHotCache` 做了什么？**（client_service.cpp L1212）

1. 调用 `GetHotKey(key)` 获取缓存块（`ref_count++`）
2. 将 replica 的 `buffer_address_` 改为缓存块的内存地址 `blk->addr`
3. 将 `transport_endpoint_` 改为本地地址（变为本地 memcpy 传输）

**`ReleaseHotKey` 做了什么？**（local_hot_cache.cpp L136）

1. 在 `key_to_lru_it_` 中查找 key
2. `ref_count--`（原子操作）

**如果不释放会怎样？**

`ref_count` 永远不为 0，该缓存块在 `GetFreeBlock()` 中会被跳过，永远无法被驱逐和重用。随着时间推移，越来越多的块被"锁死"，可用缓存容量持续减少，最终导致 Hot Cache 完全失效——`GetFreeBlock()` 返回 `nullptr`，新数据无法进入缓存。

**为什么不在传输前就释放？**

因为 `RedirectToHotCache` 将传输目标重定向到了缓存块的内存地址。如果提前释放引用，缓存块可能被其他线程驱逐（`GetFreeBlock` 会驱逐 `ref_count == 0` 的块），其内存可能被新数据覆盖，导致当前传输读到脏数据。必须在 `future.get()` 确认传输完成后才能释放——此时 memcpy 已经完成，数据已经安全拷贝到用户 buffer 中。

---

## 第三部分：Put vs Get 对比

| 维度 | Put | Get |
|------|-----|-----|
| Master 交互 | PutStart → PutEnd/PutRevoke | Query → 无需再通知 Master |
| 副本处理 | 写入所有内存副本 + 1个磁盘副本 | 只读1个最优副本 |
| 传输方向 | WRITE（本地→远端） | READ（远端→本地） |
| 数据拷贝 | 需要 memcpy 到 RDMA 注册内存 | 需要 allocate 缓冲区接收数据 |
| 零拷贝路径 | `put_from`（用户 buffer 已注册） | `get_into`（用户 buffer 已注册） |
| Hot Cache | 不涉及 | 有频率准入机制（CountMinSketch） |
| 失败恢复 | PutRevoke 撤销 replica | 无需撤销，换副本重试或返回错误 |
| 磁盘写入 | `PutToLocalFile`（异步线程池写磁盘） | 不涉及（Get 只读磁盘） |
| Lease | 不涉及 | 有 Lease 过期检查 |
