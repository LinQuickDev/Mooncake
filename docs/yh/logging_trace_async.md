# Mooncake Store 日志 Trace 与异步输出改造说明

## 修改范围

本次改造围绕 Mooncake Store 的 get/put 读写链路：

- 新增 `mooncake-common/include/mooncake_logging.h` 和 `mooncake-common/src/mooncake_logging.cpp`。
- `real_client.cpp`、`client_service.cpp`、`transfer_task.cpp` 的关键日志从 `LOG/VLOG` 切换为 `MC_LOG/MC_VLOG`。
- `mooncake_common` 新增日志实现源文件，并让 `mooncake_store` 链接 `mooncake_common`。
- `MC_LOG_ENABLE=off` 会同步影响 transfer engine 的 `FLAGS_minloglevel`，用于关闭未迁移到 `MC_LOG` 的 glog 普通日志。

## TraceId

每个 RealClient 入口自动创建一个进程内唯一的 `trace_id`，不改变公开 API：

- get 路径：`get_buffer`、`batch_get_buffer`、`get_into`、`batch_get_into`。
- put 路径：`put`、`put_batch`、`put_parts`、`put_from`、`batch_put_from`、`batch_put_from_multi_buffers`。

`trace_id` 由进程 ID、启动后的 steady clock 时间片和原子递增计数组合生成。同步调用链通过 thread-local 传递；异步路径在提交任务时捕获当前 trace，并在 worker 线程中用 `ScopedTraceId` 恢复。

日志格式统一带：

```text
trace_id[123456789] get_into_breakdown key[k1] query_us[10] select_us[2] read_us[80] total_us[95] type[MEMORY] mode[full] status[read_ok]
```

如果日志不在请求上下文中，trace 字段为：

```text
trace_id[none] ...
```

## 日志开关与级别

新增环境变量：

```bash
MC_LOG_ENABLE=on   # 默认，输出日志
MC_LOG_ENABLE=off  # 关闭 INFO/WARNING/ERROR 普通日志
```

可接受的关闭值包括 `off`、`0`、`false`、`no`。`FATAL` 不受关闭影响。

`MC_LOG` 同时尊重 glog 当前的 `FLAGS_minloglevel`，因此 `MC_LOG_LEVEL` 仍然可以控制新日志的最低输出级别：

```bash
MC_LOG_LEVEL=INFO     # 输出 INFO/WARNING/ERROR
MC_LOG_LEVEL=WARNING  # 输出 WARNING/ERROR
MC_LOG_LEVEL=ERROR    # 只输出 ERROR
```

`MC_LOG_ENABLE=off` 的优先级更高，会关闭普通 `MC_LOG` 日志。

## 异步 glog 输出

`MC_LOG` 默认异步输出，不额外增加 `MC_LOG_ASYNC` 开关。未迁移到 `MC_LOG` 的原生 `LOG(...)` 仍然走 glog 自己的同步输出路径，不会进入本队列。

1. 业务线程构造日志消息。
2. `AsyncLogMessage` 析构时把 `severity/file/line/trace_id/message` 放入有界队列。
3. 单后台线程消费队列，并调用 glog 落盘或输出。
4. 队列容量固定为 8192；队列满时阻塞，优先保证日志不丢。
5. 进程退出时通过 `atexit` flush 队列。

## TransferData Wait 首次耗时定位

没有在 `store_py.cpp::PYBIND11_MODULE(store, m)` 加 `MC_IMPORT_TRACE` 或 import 阶段计时。

为了定位第一次 `TransferData::Wait` 很长的问题，本次在 transfer 首次执行路径增加拆解日志：

- `client_create_breakdown`：拆 `ConnectToMaster`、`GetStorageConfig`、`InitTransferEngine`、`InitTransferSubmitter`。
- `open_segment_breakdown`：记录 `openSegment` endpoint、耗时和状态。
- `submit_transfer_breakdown`：记录 `allocateBatchID`、`submitTransfer`、batch id、request 数和是否首次 transfer。
- `transfer_future_wait`：记录 `future->get()` 等待耗时、策略、是否首次 wait。
- `transfer_data`：记录 submit/wait 总拆分、策略、读写方向和结果。
- UB / URMA 首次建连路径额外增加：
  - `urma_create_jetty_breakdown`
  - `urma_endpoint_construct_breakdown`
  - `urma_active_setup_breakdown`
  - `urma_passive_setup_breakdown`
  - `urma_do_setup_all_breakdown`
  - `urma_import_jetty_breakdown`
  - `urma_bind_jetty_breakdown`

使用方法：连续执行两次相同 get/put，对比第一次和第二次的上述日志。如果第一次主要长在 `open_segment_breakdown` 或 `submit_transfer_breakdown`，说明更接近 transfer/连接 lazy init；如果长在 `transfer_future_wait`，继续看底层 transport 完成路径。

对应 UbDiag PerfPoint：

- `UB_HANDSHAKE_ENCODE` / `UB_HANDSHAKE_DECODE`
- `UB_ENDPOINT_CONSTRUCT` / `UB_ENDPOINT_CREATE_JETTY`
- `UB_ENDPOINT_ACTIVE_SETUP` / `UB_ENDPOINT_ACTIVE_HANDSHAKE`
- `UB_ENDPOINT_PASSIVE_SETUP`
- `UB_ENDPOINT_DO_SETUP_ALL`
- `UB_ENDPOINT_IMPORT_JETTY` / `UB_ENDPOINT_BIND_JETTY`

`store_py.cpp` 中原先新增的 `get start/get complete/get_slow` 和 `put start/put complete/put_slow` 已移除，避免 Python binding 层同步 glog 与 store/transfer 层异步 `MC_LOG` 混排造成顺序误读。

## RealClient 慢操作告警

RealClient 公开入口统一输出慢操作 WARNING，默认阈值为 `3000us`：

- 单 key：`get_buffer_slow`、`get_into_slow`、`put_slow`、`put_from_slow`、`put_parts_slow`
- 批量：`batch_get_buffer_slow`、`batch_get_into_slow`、`put_batch_slow`、`batch_put_from_slow`、`batch_put_from_multi_buffers_slow`

示例：

```text
trace_id[...] get_buffer_slow key[k1] size[4194304] elapsed_us[18143] rc[0]
trace_id[...] batch_get_into_slow num_keys[128] size[536870912] elapsed_us[12000] success[127]
```

慢日志位于 `execute_timed_operation` 的 latency callback 中，复用入口级耗时，并受 `MC_LOG_ENABLE` / `MC_LOG_LEVEL` 控制。
