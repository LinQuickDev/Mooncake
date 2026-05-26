# Mooncake Store 日志参考手册

本文档描述 `get` / `get_batch` / `put` / `put_batch` 四个操作的全链路日志输出。

日志来源三个层次：
- **Python 绑定层** — `mooncake-integration/store/store_py.cpp`
- **核心逻辑层** — `mooncake-store/src/real_client.cpp`
- **传输服务层** — `mooncake-store/src/client_service.cpp`

---

## 1. `get` 日志链路

正常路径日志按调用顺序：

```
store_py::get
  ├ get start
  ├ real_client::get_buffer_internal
  │   ├ query_success
  │   ├ replica_selected
  │   ├ [SSD 路径] ssd_read_detail
  │   └ get_breakdown
  ├ client_service::Get
  │   └ transfer_read_completed
  ├ client_service::TransferData
  │   └ transfer_data op[READ]
  └ get complete
```

### 1.1 Python 绑定层 — `store_py.cpp::get`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `get start` | INFO | `get start key[{key}]` | 操作开始 |
| `get complete` | INFO | `get complete key[{key}] rc[0] size[{size}] elapsed_us[{us}]` | 操作成功完成 |
| `get complete` | INFO | `get complete key[{key}] rc[-1] elapsed_us[{us}]` | 操作失败 |
| `get_slow` | WARNING | `get_slow key[{key}] size[{size}] elapsed_us[{us}]` | 耗时超过 3ms 触发慢操作告警 |

### 1.2 核心逻辑层 — `real_client.cpp::get_buffer_internal`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `query_success` | INFO | `query_success key[{key}] replicas[{n}]` | Master 查询成功，返回 n 个副本 |
| `replica_selected` | INFO | `replica_selected key[{key}] type[{type}] endpoint[{ip:port}] size[{bytes}]` | Memory/LocalDisk 副本选中，含 endpoint |
| `replica_selected` | INFO | `replica_selected key[{key}] type[disk] file_path[{path}] size[{bytes}]` | Disk 副本选中，含文件路径 |
| `get_breakdown` | INFO | `get_breakdown key[{key}] query_us[{t1}] select_us[{t2}] alloc_us[{t3}] read_us[{t4}] total_us[{total}] type[{type}] status[{status}]` | 分阶段耗时汇总 |

**`get_breakdown` 字段说明：**

| 字段 | 含义 |
|------|------|
| `query_us` | Master 查询耗时（微秒） |
| `select_us` | 副本选择耗时 |
| `alloc_us` | 缓冲区分配耗时 |
| `read_us` | 数据读取耗时（RDMA/文件IO/SSD RPC） |
| `total_us` | 总耗时 |
| `type` | 副本类型：`memory` / `local_disk` / `disk` |
| `status` | 结果：`read_ok` / `read_fail` / `ssd_ok` / `ssd_fail` |

### 1.3 传输服务层 — `client_service.cpp::Get`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `transfer_read_completed` | INFO | `transfer_read_completed key[{key}] elapsed_us[{us}] data_size[{bytes}] cache_hit[{0/1}]` | RDMA/文件传输完成 |
| `transfer_read_failed` | ERROR | `transfer_read_failed key={key}` | 传输失败 |
| `lease_expired_before_data_transfer_completed` | WARNING | `lease_expired_before_data_transfer_completed key={key}` | 租约过期 |

### 1.4 传输引擎层 — `client_service.cpp::TransferData`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `transfer_data` | INFO | `transfer_data op[READ] submit_us[{t1}] wait_us[{t2}] result[{code}]` | 传输耗时拆分 |

**字段说明：**

| 字段 | 含义 |
|------|------|
| `submit_us` | 提交传输请求耗时 |
| `wait_us` | 等待传输完成耗时 |
| `result` | 传输结果，`OK` 表示成功 |

### 1.5 SSD Offload 路径 — `real_client.cpp::batch_get_into_offload_object_internal`

仅当副本类型为 `local_disk`（远端 SSD）时触发。

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `ssd_read_detail` | INFO | `ssd_read_detail endpoint[{ip:port}] num_keys[{n}] total_size[{bytes}] elapsed_ms[{ms}] batch_id[{id}]` | SSD RPC 读取详情 |

---

## 2. `get_batch` 日志链路

```
store_py::get_batch
  ├ get_batch start
  ├ real_client::batch_get_buffer_internal
  │   ├ batch_query_result
  │   ├ [逐 key] replica_selected (无此日志，batch 不逐 key 输出)
  │   ├ [SSD 路径] ssd_read_detail
  │   └ batch_get_breakdown
  ├ client_service::BatchGet
  │   └ batch_get_transfer_complete
  ├ client_service::TransferData (多次)
  │   └ transfer_data op[READ]
  └ get_batch complete
```

### 2.1 Python 绑定层 — `store_py.cpp::get_batch`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `get_batch start` | INFO | `get_batch start num_keys[{n}]` | 操作开始 |
| `get_batch complete` | INFO | `get_batch complete num_keys[{n}] success[{s}] rc[0] elapsed_us[{us}]` | 操作成功完成 |
| `get_batch complete` | INFO | `get_batch complete num_keys[{n}] rc[-1] elapsed_us[{us}]` | 操作失败 |
| `get_batch_slow` | WARNING | `get_batch_slow num_keys[{n}] elapsed_us[{us}]` | 耗时超过 10ms 触发慢操作告警 |

### 2.2 核心逻辑层 — `real_client.cpp::batch_get_buffer_internal`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `batch_query_result` | INFO | `batch_query_result num_keys[{n}] num_found[{f}]` | 批量查询结果，f 为找到的 key 数 |
| `batch_get_breakdown` | INFO | `batch_get_breakdown num_keys[{n}] query_us[{t1}] prep_us[{t2}] read_us[{t3}] total_us[{total}] batch_get_ops[{m}] ssd_offload_ops[{s}] success[{ok}]` | 分阶段耗时汇总 |

**`batch_get_breakdown` 字段说明：**

| 字段 | 含义 |
|------|------|
| `query_us` | 批量 Master 查询耗时 |
| `prep_us` | 准备阶段耗时（副本选择 + 缓冲区分配，逐 key 循环） |
| `read_us` | 数据读取耗时（BatchGet + SSD RPC） |
| `total_us` | 总耗时 |
| `batch_get_ops` | 走 BatchGet 的 key 数（MEMORY + DISK 副本） |
| `ssd_offload_ops` | 走 SSD RPC 的 key 数（LOCAL_DISK 副本） |
| `success` | 成功读取的 key 数 |

### 2.3 传输服务层 — `client_service.cpp::BatchGet`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `batch_get_transfer_complete` | INFO | `batch_get_transfer_complete num_keys[{n}] success[{s}] elapsed_us[{us}] pending_count[{c}]` | 批量传输完成 |

**字段说明：**

| 字段 | 含义 |
|------|------|
| `pending_count` | 总传输任务数（提交的 TransferFuture 数量） |

### 2.4 传输引擎层 — 同 `get` 的 `transfer_data`

### 2.5 SSD Offload 路径 — 同 `get` 的 `ssd_read_detail`

---

## 3. `put` 日志链路

```
store_py::put
  ├ put start
  ├ real_client::put_internal
  │   └ put_result
  ├ client_service::Put
  │   ├ put_start_success (或 OBJECT_ALREADY_EXISTS)
  │   └ put_end_success
  ├ client_service::TransferData
  │   └ transfer_data op[WRITE]
  └ put complete
```

### 3.1 Python 绑定层 — `store_py.cpp::put`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `put start` | INFO | `put start key[{key}] size[{bytes}]` | 操作开始 |
| `put complete` | INFO | `put complete key[{key}] rc[{ret}] elapsed_us[{us}]` | 操作完成，rc=0 成功 |
| `put_slow` | WARNING | `put_slow key[{key}] size[{bytes}] elapsed_us[{us}]` | 耗时超过 3ms 触发慢操作告警 |

### 3.2 核心逻辑层 — `real_client.cpp::put_internal`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `put_result` | INFO | `put_result key[{key}] rc[0] size[{bytes}]` | Put 成功 |
| `put_result` | INFO | `put_result key[{key}] rc[{code}] size[{bytes}]` | Put 失败，code 为错误码 |

### 3.3 传输服务层 — `client_service.cpp::Put`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `put_start` | INFO | `put_start key[{key}] rc[OBJECT_ALREADY_EXISTS]` | 对象已存在，直接返回成功 |
| `put_start_success` | INFO | `put_start_success key[{key}] replicas[{n}]` | Master 分配 replica 成功 |
| `put_end_success` | INFO | `put_end_success key[{key}] transfer_us[{us}] data_size[{bytes}]` | Put 完成，数据写入成功 |

**`put_end_success` 字段说明：**

| 字段 | 含义 |
|------|------|
| `transfer_us` | 传输阶段总耗时（含磁盘写入 + RDMA 传输） |
| `data_size` | 写入数据大小 |

### 3.4 传输引擎层 — `client_service.cpp::TransferData`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `transfer_data` | INFO | `transfer_data op[WRITE] submit_us[{t1}] wait_us[{t2}] result[{code}]` | 传输耗时拆分 |

字段含义同 GET 路径的 `transfer_data`。

---

## 4. `put_batch` 日志链路

```
store_py::put_batch
  ├ put_batch start
  ├ real_client::put_batch_internal
  │   └ batch_put_result
  ├ client_service::BatchPut
  │   ├ batch_put start
  │   └ batch_put complete
  ├ client_service::TransferData (多次)
  │   └ transfer_data op[WRITE]
  └ put_batch complete
```

### 4.1 Python 绑定层 — `store_py.cpp::put_batch`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `put_batch start` | INFO | `put_batch start num_keys[{n}] total_size[{bytes}]` | 操作开始 |
| `put_batch complete` | INFO | `put_batch complete num_keys[{n}] rc[{ret}] elapsed_us[{us}]` | 操作完成，rc=0 成功 |
| `put_batch_slow` | WARNING | `put_batch_slow num_keys[{n}] elapsed_us[{us}]` | 耗时超过 10ms 触发慢操作告警 |

### 4.2 核心逻辑层 — `real_client.cpp::put_batch_internal`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `batch_put_result` | INFO | `batch_put_result num_keys[{n}] num_failed[{f}]` | 批量 Put 结果 |

### 4.3 传输服务层 — `client_service.cpp::BatchPut`

| 关键字 | 级别 | 格式 | 说明 |
|--------|------|------|------|
| `batch_put start` | INFO | `batch_put start num_keys[{n}]` | 批量 Put 传输开始 |
| `batch_put complete` | INFO | `batch_put complete num_keys[{n}] num_failed[{f}] transfer_us[{us}] total_size[{bytes}]` | 批量 Put 完成（正常路径） |
| `batch_put complete` | INFO | `batch_put complete num_keys[{n}] num_failed[{f}] total_size[{bytes}]` | 批量 Put 完成（prefer_same_node 路径，无 transfer_us） |

### 4.4 传输引擎层 — 同 `put` 的 `transfer_data`

---

## 5. 附录：PerfPoint 打点与日志对照表

PerfPoint 定义在 `mooncake-integration/store/mooncake_perf_points.def`。
使用 `ubdiag show` 可查看实时性能数据，配合日志进行交叉分析。

### GET 侧

| PerfPoint 名称 | 定义位置 | 标签 | 对应日志关键字 |
|----------------|---------|------|---------------|
| `GET_STORE_PY_GET` | store_py.cpp::get | Get | `get start` / `get complete` |
| `GET_BUFFER_INTERNAL` | store_py.cpp::get | GetBuffer | `get_breakdown` |
| `GET_INTERNAL_QUERY` | real_client.cpp::get_buffer_internal | Query | `query_success` |
| `GET_INTERNAL_SELECT_REPLICA` | real_client.cpp::get_buffer_internal | SelectReplica | `replica_selected` |
| `GET_INTERNAL_ALLOC_BUFFER` | real_client.cpp::get_buffer_internal | AllocBuffer | `get_breakdown` alloc_us |
| `GET_INTERNAL_SSD_READ` | real_client.cpp::get_buffer_internal | SSDRead | `ssd_read_detail` |
| `GET_INTERNAL_MEM_READ` | real_client.cpp::get_buffer_internal | MemRead | `transfer_read_completed` |
| `GET_INTERNAL_DISK_READ` | real_client.cpp::get_buffer_internal | DiskRead | `transfer_read_completed` |
| `GET_SSD_OFFLOAD_RPC` | real_client.cpp::batch_get_into_offload_object_internal | OffloadRpc | `ssd_read_detail` |
| `GET_SSD_TRANSFER_DATA` | real_client.cpp::batch_get_into_offload_object_internal | TransferData | `ssd_read_detail` |
| `GET_SSD_RELEASE_BUFFER` | real_client.cpp::batch_get_into_offload_object_internal | ReleaseBuffer | — |
| `GET_SINGLE_FIND_REPLICA` | client_service.cpp::Get | FindReplica | `transfer_read_completed` |
| `GET_SINGLE_HOT_CACHE` | client_service.cpp::Get | HotCache | `transfer_read_completed` cache_hit |
| `GET_SINGLE_TRANSFER_READ` | client_service.cpp::Get | TransferRead | `transfer_read_completed` |
| `GET_SINGLE_RELEASE_CACHE` | client_service.cpp::Get | ReleaseCache | — |
| `GET_SINGLE_ASYNC_CACHE` | client_service.cpp::Get | AsyncCache | — |
| `GET_SINGLE_TRANSFER_FULL` | client_service.cpp::TransferData | TransferData | `transfer_data op[READ]` |
| `GET_SINGLE_TRANSFER_SUBMIT` | client_service.cpp::TransferData | Submit | `transfer_data` submit_us |
| `GET_SINGLE_TRANSFER_WAIT` | client_service.cpp::TransferData | Wait | `transfer_data` wait_us |

### GET BATCH 侧

| PerfPoint 名称 | 定义位置 | 标签 | 对应日志关键字 |
|----------------|---------|------|---------------|
| `GET_STORE_PY_GET_BATCH` | store_py.cpp::get_batch | GetBatch | `get_batch start` / `get_batch complete` |
| `GET_BATCH_BUFFER_INTERNAL` | store_py.cpp::get_batch | BatchGetBuffer | `batch_get_breakdown` |
| `GET_BATCH_INTERNAL_QUERY` | real_client.cpp::batch_get_buffer_internal | BatchQuery | `batch_query_result` |
| `GET_BATCH_INTERNAL_PREPARATION` | real_client.cpp::batch_get_buffer_internal | Preparation | `batch_get_breakdown` prep_us |
| `GET_BATCH_INTERNAL_SELECT_REPLICA` | real_client.cpp::batch_get_buffer_internal | SelectReplica | — |
| `GET_BATCH_INTERNAL_ALLOC_BUFFER` | real_client.cpp::batch_get_buffer_internal | AllocBuffer | — |
| `GET_BATCH_INTERNAL_SSD_READ` | real_client.cpp::batch_get_buffer_internal | SSDRead | `ssd_read_detail` |
| `GET_BATCH_INTERNAL_MEMDISH_READ` | real_client.cpp::batch_get_buffer_internal | MemDiskRead | `batch_get_transfer_complete` |
| `GET_BATCH_FIND_REPLICA` | client_service.cpp::BatchGet | FindReplica | — |
| `GET_BATCH_HOT_CACHE` | client_service.cpp::BatchGet | HotCache | — |
| `GET_BATCH_SUBMIT` | client_service.cpp::BatchGet | Submit | — |
| `GET_BATCH_WAIT` | client_service.cpp::BatchGet | Wait | — |
| `GET_BATCH_RELEASE_CACHE` | client_service.cpp::BatchGet | ReleaseCache | — |
| `GET_BATCH_ASYNC_CACHE` | client_service.cpp::BatchGet | AsyncCache | — |

### PUT 侧

| PerfPoint 名称 | 定义位置 | 标签 | 对应日志关键字 |
|----------------|---------|------|---------------|
| `PUT_STORE_PY_PUT` | store_py.cpp::put | Put | `put start` / `put complete` |
| `PUT_INTERNAL_FULL` | store_py.cpp::put | PutBuffer | `put_result` |
| `PUT_INTERNAL_ALLOC_BUFFER` | real_client.cpp::put_internal | AllocBuffer | — |
| `PUT_INTERNAL_MEM_COPY` | real_client.cpp::put_internal | MemCopy | — |
| `PUT_INTERNAL_SPLIT_SLICES` | real_client.cpp::put_internal | SplitSlices | — |
| `PUT_SINGLE_FULL` | client_service.cpp::Put | TransferPut | `put_end_success` |
| `PUT_SINGLE_PUT_START` | client_service.cpp::Put | PutStart | `put_start_success` |
| `PUT_SINGLE_DISK_WRITE` | client_service.cpp::Put | DiskWrite | `put_end_success` |
| `PUT_SINGLE_TRANSFER_WRITE` | client_service.cpp::Put | TransferWrite | `put_end_success` |
| `PUT_SINGLE_PUT_END` | client_service.cpp::Put | PutEnd | `put_end_success` |
| `PUT_SINGLE_PUT_REVOKE` | client_service.cpp::Put | PutRevoke | — |
| `PUT_SINGLE_TRANSFER_FULL` | client_service.cpp::TransferData | TransferData | `transfer_data op[WRITE]` |
| `PUT_SINGLE_TRANSFER_SUBMIT` | client_service.cpp::TransferData | Submit | `transfer_data` submit_us |
| `PUT_SINGLE_TRANSFER_WAIT` | client_service.cpp::TransferData | Wait | `transfer_data` wait_us |

### PUT BATCH 侧

| PerfPoint 名称 | 定义位置 | 标签 | 对应日志关键字 |
|----------------|---------|------|---------------|
| `PUT_STORE_PY_PUT_BATCH` | store_py.cpp::put_batch | PutBatch | `put_batch start` / `put_batch complete` |
| `PUT_BATCH_INTERNAL_FULL` | store_py.cpp::put_batch | BatchPutBuffer | `batch_put_result` |
| `PUT_BATCH_INTERNAL_ALLOC_BUFFER` | real_client.cpp::put_batch_internal | AllocBuffer | — |
| `PUT_BATCH_INTERNAL_MEM_COPY` | real_client.cpp::put_batch_internal | MemCopy | — |
| `PUT_BATCH_INTERNAL_SPLIT_SLICES` | real_client.cpp::put_batch_internal | SplitSlices | — |
| `PUT_BATCH_FULL` | client_service.cpp::BatchPut | TransferBatchPut | `batch_put complete` |
| `PUT_BATCH_CREATE_OPS` | client_service.cpp::BatchPut | CreateOps | — |
| `PUT_BATCH_PUT_START` | client_service.cpp::StartBatchPut | PutStart | — |
| `PUT_BATCH_SUBMIT` | client_service.cpp::SubmitTransfers | Submit | — |
| `PUT_BATCH_DISK_WRITE` | client_service.cpp::SubmitTransfers | DiskWrite | — |
| `PUT_BATCH_WAIT` | client_service.cpp::WaitForTransfers | Wait | — |
| `PUT_BATCH_PUT_END` | client_service.cpp::FinalizeBatchPut | PutEnd | — |
| `PUT_BATCH_PUT_REVOKE` | client_service.cpp::FinalizeBatchPut | PutRevoke | — |
| `PUT_BATCH_COLLECT_RESULTS` | client_service.cpp::BatchPut | CollectResults | — |
