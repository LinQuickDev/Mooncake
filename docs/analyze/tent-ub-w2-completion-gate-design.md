# TENT UB W2 骨架详细设计：CompletionRecord、完成门与 dispatcher 骨架

> 日期：2026-11-16。定位：**W2 子里程碑详设**（总设计 §13 实施切分的第二步）。
> 上游文档（冲突时以上游为准）：本文的父文档是《tent-ub-post-poll-threadpool-design》（下称**总设计**）；探测侧共用契约见《tent-ub-probe-link-detail-design》（下称**探测详设**）；探测资源供给见《tent-ub-probe-standalone-supply》。
> 代码基线：`49d7757b`（探测链路：修复拓扑映射、退避与异步重建）。本文行号均对该基线；父文档行号对应 `956af65e`，两者在 `workers.cpp` 上同源（W1 只做接缝，未动完成路径）。
> 状态：**已实现（2026-11-16，W2a–W2d 全部落地）**，落地偏差见 §12.1。W1（探测）已实现且默认关闭；W2 是业务 post/poll 路径的第一个里程碑。
> 阅读顺序建议：§0（决策）→ §2（心智模型）→ §6（完成门）→ §8（改动清单）。

## 0. 已确认决策与锁定默认

W2 的争议点集中在"记录该长什么样"和"谁有权释放资源"，先把结论列死，正文只给论证。

| 编号 | 决策 | 锁定默认 | 依据 |
| --- | --- | --- | --- |
| D-1 | W2 只做**记录 + 完成门 + 分派骨架**，不碰选路、准入、K、队列边界、defer 分桶、超时语义 | 是；W2 对生产行为**只有两处**可观察变化（§2.3） | 总设计 §13 表 W2 行只列这三项 |
| D-2 | 记录采用**共享内核 + 侧挂 owner**：`CompletionRecord` 只放身份与一次性事实位，不含任一侧私有类型；业务 owner 放 `BusinessRecord`，探测 owner 放 `ProbeRecord` 其余字段 | 是；内核零依赖 `slice.h` / `probe_types.h` | 总设计 §7.3 要求"两侧共用同一结构"，但两侧身份与资源句柄确实不同 |
| D-3 | **registry 的删除权归"完成认领方"**（CQE、fence 证据或超时证据）；post 侧只写事实位，永不删表。**唯一例外：`NotAccepted`（确定零接受）可由 post 侧删除**，因为 WR 未发出、不可能再有完成 | 是 | 否则 post 先返回时会删表，导致随后到达的合法 CQE 被判未知 token |
| D-4 | `handler_refs` 是**完成门事实位**，不是生命周期手段（`shared_ptr` 已保证存活）；post 调用栈与每次结算作用域各持一个引用 | 是 | 总设计 §1.3.4"旧 handler 不再访问可复用状态"；fixed by early-CQE 用例 |
| D-5 | `acceptance` 三态字段现在就定；W2 由 legacy 适配函数从 `posted_count` 推导。`kUnknown` **只由 `posted_count > 1`（自相矛盾的适配器）产生**：不猜、不回滚，阻塞到超时 fence 为止（§12.1 #3） | 是；W3 换真三态只改 `legacyAcceptance()` 一个函数 | 总设计 §6；P0-B 未就绪 |
| D-6 | 正常归还谓词是**必要条件**，最终裁决仍在池化侧；W2 无 lease，`lease_return_claimed` 这个 CAS 保护的是"outstanding + quota"释放 | 是；P0-D 后同一 CAS 位置换成 `release(lease)` | 总设计 §1.3.4 / §7.4 |
| D-7 | 超时入口 W2 **保留同步 quiesce**，但只对 `handler_refs == 0` 的记录执行 fence；refs>0 的记录本轮跳过、下一轮再来 | 是；W5 只删掉 quiesce 段，门与 finalize 不动 | 总设计 §8 要求 W5 才重构超时，但 W2 不能引入"post 未返回就释放 owner" |
| D-8 | 逻辑结果拆成"决定"与"投递"：**决定**在 settle 认领时写入记录，**投递**（`slice->resolveAttempt`）在 owner 释放之后、`handler_refs == 0` 之后；W4 再迁往 callback 执行器 | 是 | 否则 early-CQE 时可能在 `adapter->post()` 尚未返回时就 `enqueueRetry` 同一 slice |
| D-9 | `ProbeRecord` 改为**嵌入** `CompletionRecord core`，PLM 内 `settled/timed_out/token` 机械替换；**语义零变化** | 是；单列一个 commit 便于回退 | 接口 5 要求 probe record 复用同一结构；core 已含通用 drop reason，PLM 的 `OrphanReason` 保留，W6 统一枚举 |
| D-10 | dispatcher 只实现 **namespace → registry → unknown** 三步；AE 与 flag/status 分类等 P0-B 的 typed CR | 是 | 当前 `Completion` 无 flag/类型字段（探测详设 §11.2 已确认） |
| D-11 | 新增两个窄缝：`gate::Sink`（完成门副作用出口）与 `EntityEventSink`（接口 8 的 W2 空实现） | 是 | 完成门必须能脱离 `UbWorkers` 单测 |
| D-12 | 观测用 `CompletionStats`（原子计数 + `snapshot()`），风格对齐 `ProbeStats` | 是 | `ProbeStats` 是仓库内既有范式 |

## 1. 范围与不做什么

### 1.1 W2 做什么

| 项 | 内容 |
| --- | --- |
| 记录 | `CompletionRecord`（共享内核）+ `BusinessRecord`（业务 owner），替换 `UbWorkers::Inflight` |
| 完成门 | 双入口 `onPostReturn` / `onRealCr`，`handler_refs` 纪律；逻辑结果一次认领 + owner 处置三条互斥出口（归还 / finalize / 回滚） |
| 分派 | `dispatchCompletion` 成骨架：probe namespace 先行、业务 registry 查表、未知 token 记账 |
| 接缝 | `gate::Sink`、`EntityEventSink`、`CompletionStats`；三处完成消费点同源（主循环 / `stop()` quiesce / `scanTimeouts`） |
| 探测 | `ProbeRecord` 内核统一（纯结构迁移，不改 PLM 判定） |
| 测试 | 门单测（无线程 / 有线程）、fake adapter 集成、既有回归全绿 |

### 1.2 W2 不做什么（明确拒绝，避免评审预期错位）

| 不做 | 归属 |
| --- | --- |
| 三态 `PostResult`、单 WR 接口、`tryClaimPost` | P0-B / P0-D（池化侧）→ W3 |
| 选路改造、`endpoint->jetty(worker_index)` 移除 | P0-D → W3 |
| K 准入（`task_max_inflight_attempts`） | W3 |
| 有界队列、defer 分桶、availability epoch | W3/W4 |
| 十类分类、typed marker、AE、flush 提取服务 | W4 |
| 超时结构重构（有序集合、按 poller 分区）、去掉同步 quiesce | W5 |
| callback 执行器、关闭顺序重排、接口 2/8/9/10 真实调用 | W4/W5/W6 |
| 探测语义修改（退避、campaign、门禁） | 已冻结，W2 不碰 |

### 1.3 与前后里程碑的接缝

```text
W1 探测   ──(ProbeRecord 已有)──┐
                                ├── W2 记录/门/分派 ── W3 post（真三态+K+lease）
W3 依赖 P0-B/P0-D 契约冻结 ─────┘                      └── W4 poll（分类/执行器）
                                                              └── W5 超时（删 quiesce）
```

W2 的存在价值是让 W3–W6 **只改策略、不改记账**：三态 acceptance、K、lease、十类分类、回调执行器都挂在这条记录上，先把它做对，后面的每一步都只是替换一个函数体。

## 2. 心智模型

### 2.1 一句话

> 每条在途 WR 有一张"票据"，记四件事——**post 有没有返回、WR 有没有终结（或被 fence 证明已停）、有没有故障、还有谁在碰它**。"还资源"全进程只能发生一次，而且必须先拿到"责任已终结"的证据。

### 2.2 一次业务 WR 的四个时刻

```text
t0 publish      token 入 registry；post_claimed=true；handler_refs=1（post 栈持有）
t1 native post  adapter->post(单 WR)
t2a CQE 先到    onRealCr：写终态结果 → release-store wr_terminal；refs+1 → 作用域 → refs-1
t2b post 返回   onPostReturn：写 acceptance → release-store post_returned；refs-1 → 0
t3 advance      refs==0 → 一次性认领逻辑结果（onSettle）→ 有责任证据后一次性认领 owner 处置
```

**两个时刻可以任意先后**，t3 由"最后一个把 refs 减到 0 的人"触发；t3 可被多个线程重复调用，但每个认领位只有一次能赢。

### 2.3 W2 唯一的两处行为变化

两处变化同源（都出在"CQE 早于 post 返回"），因此可以合并成一次评审关注点：

| 变化 | 现状（基线） | W2 | 为什么必须现在改 |
| --- | --- | --- | --- |
| early CQE 的释放时点（连带切片投递时点） | CQE 抢先时 `handleCompletion` 立即 `releaseInflight`：`releaseOutstanding` + `quota->release` 发生在 `adapter->post()` 尚未返回时 | 等 `handler_refs==0` 才释放；切片结算也一并后移 | 提前归还 quota 会让其他 worker 超额准入；P0-D 之后就是提前归还 lease，属于数据正确性问题 |
| 重复 CQE | 第二次查表失败静默丢弃（`return`） | 计入 `unknown_token`/`duplicate_cr` 观测并经 sink 上报 | 门要求"只结算一次"可验证；静默丢弃无法证明没双释放 |
| post 未返回时的超时 | 直接标记并 fence（可能在 `adapter->post()` 还在用该记录时回收内存） | 该轮跳过，`post_returned` 之后才 fence（D-7） | 先保证安全，再谈时效；见下方"代价" |

前两处同源（early CQE），第三处是 D-7 的直接后果，必须一起评审。

**D-7 的代价**：`adapter->post()` 若在 provider 内永久阻塞，该记录**永远不会被超时扫描 fence**（只会每轮累加 `timeout_deferred_by_post_return`），slice 只能等 `stop()`；而 `stop()` 又要先 join posting 线程，因此同样卡住。这是"宁可慢也不在未知 DMA 上回收内存"的取舍，**已登记为 §13 风险**，出口留给 W5（超时结构重构）——届时 record 归属 poller 分区、post 阻塞由池化侧接口 9/10 上报，不再由本侧扫描兜底。

其余路径（成功、错误、超时、stop）**结算结果与释放次数完全不变**，只是把释放动作收进各出口的一次性认领里（§6.3/§6.4）。

### 2.4 为什么把"完成门"从"结算"里拆出来

现状把三件事挤在 `handleCompletion` 一个函数里：查表认领、释放资源、推进 slice。三者的正确条件其实不同：

| 动作 | 正确条件 | 现状风险 |
| --- | --- | --- |
| 认领 CQE | token 在表里且未被认领 | 重复 CQE 静默丢弃，不可观测 |
| 释放 owner | 有责任证据（`wr_terminal` / `fence_proven` / 确定零接受）**且** 无 handler 在跑 **且** 成功情形额外要求 post 已返回 | early CQE 提前释放 |
| 推进 slice | 前两者都完成 | early CQE 时可能在 post 未返回时 `enqueueRetry` |

拆开后每一行都有独立谓词、独立测试、独立计数，W3 换策略时不需要重新推理释放正确性。

## 3. 现状基线与差距

| # | 现状（基线 `49d7757b`） | 位置 | W2 目标 |
| --- | --- | --- | --- |
| 1 | `Inflight` 只有 token/引用/超时位，`resources_released` 是唯一的幂等位 | `workers.cpp:54-68` | 共享内核（状态位 + 认领位，§5.1）；业务 owner 侧挂 |
| 2 | 完成路径"查表即删表即释放"，无事实门 | `workers.cpp:1052-1058`、`1218-1233` | 逻辑结算 + owner 处置三条互斥出口（归还 / finalize / 回滚） |
| 3 | CQE 与 post 返回无协同，`handler_refs` 不存在 | `workers.cpp:979`、`1052` | 双入口 + refs 纪律 |
| 4 | 重复/未知 token 静默返回 | `workers.cpp:1056-1058` | 记账 + `EntityEventSink` stub |
| 5 | `dispatchCompletion` 只有两分支，token==0 直接丢弃 | `workers.cpp:472-484` | 骨架：namespace/registry/unknown，留 W4 桩 |
| 6 | 超时路径在锁内全表扫 + 同步 quiesce + 立即释放，不检查 post 是否返回 | `workers.cpp:1148-1216` | 保留 quiesce，但加 `refs==0` 前置与 finalize 统一释放 |
| 7 | `ProbeRecord` 自带 `settled/timed_out`，与业务记录各写一套 | `probe_link_manager.h:82-107` | 嵌入共享内核，事实位同源 |
| 8 | 无完成路径统计 | `RailStats` 只覆盖路径级 | `CompletionStats` |

## 4. 模块与文件

### 4.1 分层

```text
        ┌───────────────────────── UbWorkers（业务） ─────────────────────────┐
        │ processPending         handleCompletion           scanTimeouts       │
        │      │                       │                         │              │
        │      │                  dispatchCompletion ◄────────────┘              │
        │      │                       │                                        │
        │      │              ┌────────┴─────────┐                              │
        │      │              ▼                  ▼                              │
        │      │      gate::onRealCr     probe_manager_->onProbeCompletion        │
        │      ▼              │                                                 │
        │ gate::onPostReturn  │                                                 │
        │      │              │                                                 │
        │      └──────► gate::advance ◄──────────── gate::onTimeout / onFence*   │
        │                     │                                                 │
        │                     ▼                                                 │
        │        gate::Sink（UbWorkers 实现）                                    │
        │        onSettle / onFault / onOwners / onBlocked / onDuplicate         │
        │                     │                                                 │
        │                     ▼                                                 │
        │        CompletionRecord（纯事实位，无业务类型）                         │
        └─────────────────────────────────────────────────────────────────────┘
```

`gate::*` 与 `CompletionRecord` **不 include** `slice.h`、`workers.h`、`probe_link_manager.h`：这是它们能被独立单测的前提（D-11）。

### 4.2 文件清单

| 文件 | 动作 | 内容 | 估算 |
| --- | --- | --- | --- |
| `include/tent/transport/ub/completion_record.h` | 新增 | `RecordKind`、`PostAcceptance`、`RecordPath`、`CompletionRecord`、`gate::{Sink, PostReturn, WrFacts, BlockReason, FaultKind}`、`CompletionStats` | ~260 行 |
| `src/transport/ub/completion_record.cpp` | 新增 | `gate::onPostReturn/onRealCr/onTimeout/onFenceProven/onFenceFailed/onUnposted/advance`、`CompletionStats::snapshot` | ~240 行 |
| `include/tent/transport/ub/entity_event.h` | 新增 | 接口 8 的最小形状：`EntityEventSink` + `EntityEvent{kind,...}`（W2 只用 `kUnknownToken` 与 `kUnclassified`） | ~120 行 |
| `include/tent/transport/ub/workers.h` | 改 | 删 `Inflight`；`records_` 取代 `inflight_`；新增 sink 实现声明、stats 取值器 | ~40 行改动 |
| `src/transport/ub/workers.cpp` | 改 | `BusinessRecord`、publish/claim/sink、`handleCompletion` 拆分、`scanTimeouts` 加前置、`stop()` 尾部改 finalize | ~180 行改动 |
| `include/tent/transport/ub/probe_link_manager.h` | 改 | `ProbeRecord` 嵌入 `CompletionRecord core` | ~10 行改动 |
| `src/transport/ub/probe_link_manager.cpp` | 改 | `settled/timed_out/token` → `core.*`（20 处机械替换） | ~30 行改动 |
| `tests/ub_completion_gate_test.cpp` | 新增 | 门单测（无线程 + 交错 + 并发） | ~450 行 |
| `tests/ub_completion_record_test.cpp` | 新增 | 记录/统计/legacy acceptance 映射/refs 泄漏 | ~220 行 |
| `tests/ub_fake_urma.h` | 改 | 新增 post 进入/返回钩子、单条完成注入 | ~40 行 |
| `tests/ub_native_data_path_test.cpp` | 改 | 补 early-CQE、未知 token、重复 CQE 三例 | ~150 行 |
| `tests/CMakeLists.txt` | 改 | 两个新 gtest 目标（只链 `completion_record.cpp`） | ~20 行 |

**不新增常驻线程**，不新增配置键（W2 无可调参数）。

## 5. 数据模型

### 5.1 `CompletionRecord` 字段总表

§7.3 的字段名保持不变，落地形态按 D-2 分两层。

| §7.3 字段 | 落地 | 类型 | 业务取值 | 探测取值 | 说明 |
| --- | --- | --- | --- | --- | --- |
| `token` | 内核 | `uint64_t` | `nextCompletionToken()` | `makeProbeToken(seq)` | registry 主键，发布后不可变 |
| `attempt_key` | 内核（标量） | `uint64_t id` + `uint32_t serial` | slice_id / attempt | probe_seq / 0 | **只用于认领与观测**，业务判定仍读 `BusinessRecord` 里的 `UbAttemptToken` |
| `path_version` | 内核 | `RecordPath{local_nic, segment_id, remote_nic}` | `UbPostPath` 三元组 | `PathVersion::path` | 两侧同构，转换见 §5.4 |
| `jetty_generation` | 内核 | `uint64_t generation` | `path.endpoint_generation` | `key.jetty_generation` | 观测与 stale 判定用 |
| `topology_epoch` / `target_epoch` / `fault_epoch` | 内核 | `uint64_t` ×3 | 0 / 0 / W3 起填 | 原值 | 业务在 W2 恒 0（真正代次校验在 W3） |
| `jetty_lease` | 业务 owner | `uint64_t lease_id` | 恒 0（W2 无池） | 空置 | W3 起为 `JettyLease` |
| `target_ref` | 业务 owner | `RemoteSegmentPtr` | import 后的远端段 | 空置 | P0-B 后拆出 `RemoteJettyInfo` 引用 |
| `segment_refs` | 业务 owner | `LocalSegmentPtr` + `RemoteSegmentPtr` | 已持有 | 探测：`source` / `area` | 探测的两页就是 segment refs |
| `quota_reservation` | 业务 owner | `QuotaReservation` | 已预留 | 探测：控制 credit（`uint32_t`） | 释放都必须一次性 |
| `buffer_owner` | 业务 owner | `UbTask::Ptr` + slice 指针 | `PendingSlice` | 探测：源页 pin（同 `source`） | 钉住内存到结算之后 |
| `orphan_reason` | 内核 | 通用版 `RecordDropReason` | W2 恒 `kNone`（W6 用） | 由 `OrphanReason` 映射 | 接口 5 注 3 要求补字段 |

内核状态位与认领位分两组，全部 `std::atomic`。**分清"状态"和"认领"是本文最要紧的一条**：状态回答"发生了什么"，认领回答"谁负责做某件事、做过没有"。

状态位（写者唯一，多读者）：

| 位 | 语义 | 置位者 | 备注 |
| --- | --- | --- | --- |
| `post_claimed` | 本侧已取得该 attempt 的唯一 post 权 | publish 时 | 只防本侧双发；"能否 post"的权威判定在池化侧 claim（W3） |
| `post_returned` | native post 已返回（含失败） | `onPostReturn` | release 发布，保护 `acceptance`/`posted_bytes` |
| `acceptance` | `NotAccepted / Accepted / Unknown` | `onPostReturn`（一次写） | 派生态，不是认领位 |
| `logical_outcome` | `COMPLETED / FAILED / TIMEOUT / CANCELED` | `advance` 的 settle 认领 | 只决定一次，投递在 owner 处置之后 |
| `wr_terminal` | 第一条真实 CQE 已到 | `onRealCr` | **责任证据之一**（硬件责任终结） |
| `fence_proven` | 同步 quiesce 成功，旧 DMA 已停 | `scanTimeouts` | **责任证据之二** |
| `timed_out` | 本记录的逻辑结果为超时 | `scanTimeouts` | 可撤销（fence 失败时，§6.7） |
| `fault_seen` | 首次真实错误已记（逻辑） | 错误 CQE / 超时 | **不等于责任终结**，单独不可作为释放依据 |
| `handler_refs` | 正在访问可复用状态的执行体数量 | post 栈与结算作用域 | 门事实位，非生命周期 |

认领位（一次性动作，簇内互斥）：

| 位 | 认领的动作 | 认领者 |
| --- | --- | --- |
| `cr_claimed` | `terminal` / `wr_terminal` 的**写入权**（"这条完成由我落账"） | `onRealCr` |
| `settle_claimed` | 选择并提交逻辑结果（COMPLETED/FAILED/TIMEOUT/CANCELED） | `advance` |
| `reject_claimed` | 拒绝回滚（`NotAccepted`） | `advance` |
| `finalize_claimed` | 故障/超时/停止路径的 owner 清理 | `advance` |
| `lease_return_claimed` | 正常归还 owner | `advance` |
| `fault_reported` | 故障副作用（rail_monitor/PLM/接口 2）的一次性上报 | `advance` 或 `scanTimeouts` |
| `conflict_claimed` | "确定零接受却收到完成"这条矛盾记录只上报一次 | `advance`（refs 归零后的唯一汇合点） |

两条硬规矩：

1. **释放 owner 的前提是"责任证据"**——`wr_terminal || fence_proven || 确定零接受（NotAccepted）`。`fault_seen` 只是逻辑标记，没有责任证据时释放资源就是在未知 DMA 上回收内存（探测详设 §9.4 的 L-4 同构）。
2. **`timed_out` 与 `finalize_claimed` 是两个独立位**；超时不终结责任，只标记逻辑结果。
3. **重复完成必须靠认领位而不是事实位判定**：`wr_terminal` 是给读方的发布事实，`cr_claimed` 才是"谁写它"的认领。poller 上的真实 CQE 与 `scanTimeouts()`/`stop()` 里 fence 回吐的完成**会由两个线程同时进入 `onRealCr`**（一个 JFC 只有一个 poller，但 fence 在触发它的线程上执行），因此 `onRealCr` 必须先 `cr_claimed.exchange`、失败即走 `onDuplicate`，**绝不能先 load `wr_terminal` 再写 `terminal`**（那是数据竞争，且可能混入后到者的 category/bytes）。

### 5.2 内存序表

只有"发布—读取"一对，其余全是 CAS。**顺序错误的表现是"事实看齐了但数据是旧的"**，所以逐个写死：

| 写入 | 内存序 | 配套读取 | 规则 |
| --- | --- | --- | --- |
| `acceptance` | `relaxed` store | 先 acquire 读 `post_returned`，再 relaxed 读 | acceptance 必须先写，`post_returned` 后写 |
| `post_returned` | `release` store | `acquire` load | 与上一条成对 |
| 终结结果（category/bytes/status） | 普通写，**在 `wr_terminal` 置位之前** | 读方 acquire 后读 | 只有一个写入者，等价于 single-writer 发布 |
| `wr_terminal` | `release` store | `acquire` load | 置位必须在结果字段之后 |
| `handler_refs` | `acq_rel` fetch_add / fetch_sub | — | 减法返回值 0 时由该线程调用 `advance` |
| `fault_seen` / `timed_out` / `finalize_claimed` / `lease_return_claimed` / `post_claimed` | `acq_rel` CAS（`exchange` 用于纯一次性） | — | CAS 失败即"别人已认领"，调用方必须直接返回 |

`wr_terminal` 的一条硬规矩：**结果字段先写、`wr_terminal` 后置**，否则读方可能看到置位却读到未初始化 category。这条在代码里靠一个 `storeWrTerminal(WrFacts)` 小函数收口，禁止散写。

### 5.3 `BusinessRecord`（业务 owner）

```cpp
// workers.cpp 内私有类型；不放头文件，避免业务句柄泄漏到共享内核
struct UbWorkers::BusinessRecord {
    ub::CompletionRecord core;      // 共享内核（事实位 + 身份）
    PendingSlice pending;           // task/slice/opcode/target_id/priority
    UbAttemptToken attempt;         // 完整业务身份（切片结算要用）
    UbPostPath path;                // 选路结果（评价与探测证据要用）
    std::shared_ptr<UbEndpoint> endpoint;
    LocalSegmentPtr local_segment;
    RemoteSegmentPtr remote_segment;
    QuotaReservation quota;
    size_t posted_bytes{0};
    uint64_t posted_ns{0};
};
```

`inflight_` 改名为 `records_`，类型 `unordered_map<uint64_t, std::shared_ptr<BusinessRecord>>`；`inflight_mutex_` 改名 `records_mutex_`。**改名不是洁癖**：W5 会新增一个"按 deadline 有序"的第二索引，两个集合必须一眼分得清。

### 5.4 身份映射（两侧共用内核的关键）

```text
业务： RecordPath{ local_nic = path.local_topology_id,
                   segment_id = path.remote_segment_id,
                   remote_nic = path.remote_device_id }
       generation       = path.endpoint_generation
       topology_epoch   = 0          (业务无此概念)
       target_epoch     = 0          (W3 起填 target 代次)
       fault_epoch      = 0          (W3 起从 PLM 快照填)

探测： RecordPath{ local_nic = key.version.path.local_nic,
                   segment_id = key.version.path.segment_id,
                   remote_nic = key.version.path.remote_nic }
       generation       = key.jetty_generation
       topology_epoch   = key.version.topology_epoch
       target_epoch     = key.version.target_epoch
       fault_epoch      = key.fault_epoch
```

转换函数各写在自己的 owner 侧（`makeBusinessRecordPath()` / `makeProbeRecordPath()`），**不进内核**：内核只认标量，不认任何一方的结构体。这样 P0-D 改 `UbPostPath` 时不会牵动探测，反之亦然。

### 5.5 `ProbeRecord` 合成（D-9）

```cpp
struct ProbeRecord {
    ub::CompletionRecord core;   // 新增：token / attempt / path / 责任位 / timed_out
    ProbeKey key;                // 完整探测身份（保持原样）
    ProbePeerKey peer;           // 预算释放用（保持原样）
    uint64_t posted_ns{0};       // W2 暂留，W6 合并进 core
    uint64_t deadline_ns{0};     // 同上
    RecordState state{RecordState::kPosting};
    OrphanReason orphan_reason{OrphanReason::kNone};
    LocalSegmentPtr source;
    RemoteSegmentPtr area;
    uint32_t credit{0};
    bool test_probe{false};
};
```

替换规则（唯一允许的改动形态）：

| 现状 | W2 |
| --- | --- |
| `record->settled.exchange(true)`（= "该记录已被结算"） | `record->core.finalize_claimed.exchange(true)`（同一语义：一次性清理认领） |
| `record->timed_out.exchange(true)` | `record->core.timed_out.exchange(true)` |
| `record->token` | `record->core.token` |
| `record->key` / `peer` / `state` / `orphan_reason` / `source` / `area` / `credit` | **不动** |

判定条件、锁边界、副作用顺序、日志文本**一律不动**；W2d 的出口是"探测详设 §14.2 的 12 个用例全部原样通过"。

### 5.6 token 命名空间与 registry

- token 段划分沿用探测详设 §5.3：高 16 位为 kind，业务 `0`、探测 `1`；`token == 0` 保留给原生 marker。
- 业务 token 仍是 `next_token_` 递增（当前实现从 1 起，天然落在 kind 0）。
- registry 三条规则：
  1. **发布**：`tryCommitPost` 成功后、`adapter->post()` 之前插入（现状已如此，保持）；
  2. **查找**：只在 `records_mutex_` 内取 `shared_ptr`，出锁后用；
  3. **删除**：只有完成认领方可以删除——实现上是"owner 处置完成时删"（`onOwners` 的 `eraseRecord`），因为处置只由认领方执行；超时 fence 成功方在处置**之前**删表，且必须在同一临界区内复核"该记录仍属于我"（CQE 可能已抢先认领并处置）。删除点不放在 `wr_terminal` 置位时，是为了让"记录还在表里 = 重复完成（`duplicate_cr`）"与"记录已不在表里 = 未知 token（`unknown_token`）"可区分（§7.1）。**`onPostReturn` 永不删表**（D-3）。

## 6. 完成门

### 6.1 双入口

```cpp
// handler_refs 由调用作用域的 RAII 守卫持有（§6.5），门入口本身不加减引用：
// 由"最后一个释放者"调用 advance，任何提前返回都不会漏减。

// gate::onPostReturn —— processPending 在 adapter->post() 返回后调用，唯一一次
void onPostReturn(CompletionRecord& rec, const PostReturn& r, GateContext& ctx) {
    if (rec.post_returned.load(std::memory_order_acquire)) {     // 防御：本入口只该被调用一次
        ctx.stats.one_shot_violations++;                         // 恒 0；非 0 即缺陷
        return;
    }
    rec.acceptance.store(r.acceptance, std::memory_order_relaxed);  // ① 数据先写
    rec.posted_bytes = r.posted_bytes;                              //    数据先写
    rec.post_retryable = r.retryable;                               //    数据先写
    rec.post_returned.store(true, std::memory_order_release);       // ② release 发布
}

// gate::onRealCr —— dispatcher（或 fence 回吐路径）在 registry 命中后调用
void onRealCr(CompletionRecord& rec, const WrFacts& f, Sink& sink, GateContext& ctx) {
    if (rec.cr_claimed.exchange(true, std::memory_order_acq_rel)) {  // 重复完成：认领失败
        ctx.stats.duplicate_cr++;
        sink.onDuplicate(rec);                                   //   只记账，不碰 record
        return;
    }
    if (!rec.post_returned.load(std::memory_order_acquire)) ctx.stats.early_cqe++;
    rec.terminal = f;                                            // ① 结果先写
    if (!f.success) rec.fault_seen.store(true, std::memory_order_release);  // 同属结果
    rec.wr_terminal.store(true, std::memory_order_release);      // ② release 发布
}
```

> **为什么重复判定用 `cr_claimed` 而不是 `wr_terminal`**：一个 JFC 只有一个 poller，但 `scanTimeouts()`/`stop()` 的 fence 在**触发它的那个线程**上回吐完成，所以两条真实完成可能由两个线程同时进入 `onRealCr`。先 `load` 再写 `terminal` 是数据竞争，且两条结果可能被混写；认领位保证只有一条落账（IN-1）。

两个入口都遵循同一条发布纪律，不写成 `exchange` 后赋数据：

| 步骤 | 内容 | 违反后果 |
| --- | --- | --- |
| ① | 普通写结果字段（`acceptance`/`posted_bytes`/`terminal`） | — |
| ② | `release` store 一次性位（`post_returned`/`wr_terminal`） | 置位早于数据 → 读方看到"事实已发生、数据是旧值" |
| ③ | `handler_refs` 减一，返回 0 者调 `advance` | 每人都调 → 无谓 CAS 竞争，语义不显式 |

`post_returned` 用 load 而非 exchange 做一次性检查，是为了让"数据先写、置位后写"的顺序不受 exchange 语义干扰；真正的一次性保证来自"该入口在调用约定上只被调用一次"，以及 `one_shot_violations` 的持续为 0（§9）。伪码里的 `stats` 由 `GateContext`（统计 + 时钟）注入，gate 内不使用任何全局变量，否则 G-11 无法在单线程下复现内存序问题。

`gate` 对外的全部入口就这六个，调用者固定；**除这些入口外，任何代码不得直接改 record 的事实位**：

| 入口 | 调用者 | 一次性语义 | 返回 |
| --- | --- | --- | --- |
| `onPostReturn(rec, r, sink)` | `processPending`（post 返回后） | `acceptance` + `post_returned` 一次 | void |
| `onRealCr(rec, f, sink)` | `dispatchCompletion` | `wr_terminal` 一次；重复走 `onDuplicate` | void |
| `onTimeout(rec, sink)` | `scanTimeouts` | `timed_out` + `fault_reported` 一次；**不调 `advance`** | `false` = 已被别的出口认领 |
| `onFenceProven(rec, sink)` | `scanTimeouts`（quiesce 成功后） | `fence_proven` 一次，随后 `advance` | void |
| `onFenceFailed(rec)` | `scanTimeouts`（quiesce 失败后） | 撤销 `timed_out`（可重复调用） | void |
| `onUnposted(rec, reason, sink)` | `stop()`（posting/poller 均已 join） | 兜底 `settle` + `onOwners`，幂等；有 handler 引用时**拒绝释放**并计数 | void |

**`onTimeout` 为什么不调 `advance`**：此时 `timed_out` 是**暂定**的（fence 还没证据）。若此刻就认领逻辑结果，一旦 fence 失败、`timed_out` 被撤销，逻辑结果却已定格为 TIMEOUT，就与基线"fence 失败后自然 SUCCESS CQE 仍算 COMPLETED"的行为不一致。因此超时的逻辑结果只在 `onFenceProven` 或真实 CQE 到达时认领。

### 6.2 正常归还谓词（必要条件）

```text
readyForNormalReturn(rec) =
     rec.post_returned            // post 已返回
  && rec.acceptance == kAccepted  // 且确认被接受
  && rec.wr_terminal              // 且有真实 CQE 认领
  && rec.terminal.success         // 且是 SUCCESS
  && !rec.fault_seen              // 且未记故障（超时、错误 CQE、本地错误都置位）
  && !rec.timed_out               // 且未被超时标记（迟到成功不解除）
  && !rec.finalize_claimed        // 且未进入 finalize
  && rec.handler_refs == 0        // 且无 handler 仍在访问可复用状态
```

对应总设计 §1.3.4 的六项，另外两项**不由本侧判定**：

| §1.3.4 条件 | W2 落点 |
| --- | --- |
| 未见 fault/retire | 本侧 `fault_seen` 近似；`retire` 是池化侧状态，W3 交给 `release()` 复核 |
| pool OPEN | 池化侧 `release()` 内裁决，W2 无池 |
| `lease_return_claimed` | 本侧 CAS，作为"归还已发生"的唯一凭据（D-6） |

### 6.3 `advance`：先结算逻辑结果，再处置 owner

`advance` 做两件**互不替代**的事，各有自己的认领位。把它们混在一起是 W2 最容易写错的地方：`fault_seen` 只该决定"记不记故障、报不报"，不该决定"能不能释放资源"。

```cpp
void advance(CompletionRecord& rec, Sink& sink, GateContext& ctx) {
    if (rec.handler_refs.load(std::memory_order_acquire) != 0) {          // 还有人在跑
        ctx.stats.gate_blocked_by_handler_refs++;
        return;
    }

    // 责任证据三选一：真实 CQE / fence 成功 / 确定零接受（NotAccepted 本身即证据）
    const bool responsibility_ended =
        rec.wr_terminal.load(std::memory_order_acquire) ||
        rec.fence_proven.load(std::memory_order_acquire) ||
        (rec.post_returned.load(std::memory_order_acquire) &&
         rec.acceptance.load(std::memory_order_relaxed) == PostAcceptance::kNotAccepted);

    // "确定零接受却收到完成"的自相矛盾记录：两个事实由两条入口各自发布，可能互相
    // 看不见对方刚写的位（先看后写），所以在入口里计数会漏计。两个事实都在释放自己
    // 那份 handler 引用**之前**发布，而引用计数是 acquire/release 链，因此"最后一个
    // 释放者"——也就是执行本函数的线程——必然同时看见两者，计数在这里精确一次。
    if (rec.notAccepted() && rec.wr_terminal.load(acquire) &&
        !rec.conflict_claimed.exchange(true, acquire))
        ctx.stats.not_accepted_with_completion++;

    // 责任没终结就只能上报，绝不能释放（IN-3）；此时也**不认领结算位**，
    // 否则真实 CQE 到达后逻辑结果再也写不进去。
    if (!responsibility_ended) {
        ctx.stats.gate_blocked_by_responsibility++;
        sink.onBlocked(rec, BlockReason::kResponsibility);
        return;
    }

    // ---- 第一段：逻辑结果（谁先认领谁定，一次性）----
    if (!rec.settle_claimed.exchange(true, std::memory_order_acq_rel)) {
        sink.onSettle(rec, logicalOutcomeOf(rec));
    }

    // ---- 第二段：owner 处置（三条互斥出口，各一个一次性位）----
    if (rec.post_returned.load(std::memory_order_acquire) &&
        rec.acceptance.load(std::memory_order_relaxed) == PostAcceptance::kNotAccepted) {
        if (rec.reject_claimed.exchange(true, std::memory_order_acq_rel)) return;
        sink.onOwners(rec, OwnerDisposition::kReject);      // 逆序回滚（W3 完整化）
        return;
    }

    if (rec.fault_seen.load(std::memory_order_acquire) ||
        rec.timed_out.load(std::memory_order_acquire)) {
        if (!rec.fault_reported.exchange(true, std::memory_order_acq_rel)) {
            sink.onFault(rec, faultKindOf(rec));            // rail_monitor / PLM / 接口 2，一次性
        }
        if (rec.finalize_claimed.exchange(true, std::memory_order_acq_rel)) return;
        sink.onOwners(rec, OwnerDisposition::kFinalize);    // 故障/超时清理
        return;
    }

    if (!readyForNormalReturn(rec)) {
        sink.onBlocked(rec, blockReasonOf(rec));            // 只观测，不改 record
        return;
    }
    if (rec.lease_return_claimed.exchange(true, std::memory_order_acq_rel)) return;
    sink.onOwners(rec, OwnerDisposition::kReturn);          // 正常归还，顺序见 §6.8
}
```

**逻辑结果的判定**（与基线逐条对齐，`logicalOutcomeOf`）：

| 情形 | 结果 | 基线对应 |
| --- | --- | --- |
| `acceptance == kNotAccepted` | CANCELED（未执行；`retryable` 按现状 `retryableStatus(status)`） | 现有 NotAccepted 回滚后 `resolveAttempt(FAILED, retryable)` |
| `timed_out == true` | TIMEOUT（Unknown） | `handleCompletion` 里 `timed_out` 分支优先于 category |
| 否则按 CQE | SUCCESS → COMPLETED；其他 → FAILED | 现有 `switch (completion.category)` |
| `fence_proven` 且 `wr_terminal == false` | TIMEOUT（fence 后仍无完成） | 现有 `scanTimeouts` 的 probe-lost 分支 |

> `timed_out` 的"可撤销"语义（fence 失败时撤销）与基线一致：撤销后若真实 SUCCESS CQE 才到，逻辑结果仍是 COMPLETED，与基线相同；`fault_seen` 保留只影响"走 finalize 而不是正常归还"，不影响逻辑结果。

**为什么顺序是"拒绝 → 故障 → 正常"，以及为什么责任证据要单独判**：

1. **责任证据优先于一切处置**：没有 `wr_terminal`/`fence_proven` 就释放 owner，等于在可能仍在 DMA 的页上回收内存。基线用"自然 CQE 或 quiesce 成功"两条路保证这一点，W2 把它显式化为一个事实位。
2. **拒绝优先于故障**：`NotAccepted` 表示这条 WR 从未发出；此时若并发扫描已置 `timed_out`，它是伪超时。先判拒绝可避免一条未存在的 WR 白报一次故障。
3. **故障优先于正常**：`fault_seen`/`timed_out` 置位后正常归还永不成立（§6.2），先判故障保证 `onFault` 与 `onOwners(kFinalize)` 在同一作用域、顺序固定。
4. **`onBlocked` 不是动作**：只累计"门为什么没过"，可以高频调用。

### 6.4 出口总表

| 出口 | 触发条件 | 认领位 | sink 回调 | 基线对应 |
| --- | --- | --- | --- | --- |
| 逻辑结算 | 每次 `advance` | `settle_claimed` | `onSettle`（只记录结果，不投递） | `resolveInflight`（一次；W2 把"决定"与"投递"拆开） |
| 正常归还 | 谓词全真 | `lease_return_claimed` | `onOwners(kReturn)` | `releaseInflight` + `resolveInflight(COMPLETED)` |
| 拒绝回滚 | `NotAccepted` | `reject_claimed` | `onOwners(kReject)` | 现有回滚块（`workers.cpp:978-1009`） |
| 故障清理 | `fault_seen`/`timed_out` | `finalize_claimed` | `onFault` + `onOwners(kFinalize)` | 错误分支的 `releaseInflight` |
| 停止兜底 | `stop()` 显式调用 | 同上 | `onOwners(kFinalize)` | `stop()` 尾部释放 |

### 6.5 `handler_refs` 纪律（D-4）

| 时刻 | 操作 | 理由 |
| --- | --- | --- |
| 入表**之前**、`adapter->post()` 前 | 守卫构造 → `fetch_add(1)` | post 栈要用 segment/target；先取引用再入表，避免超时扫描看到"已发布但 `refs==0`"的记录去 fence 一个还没开始的 post |
| post 作用域结束（`onPostReturn` 之后） | 守卫析构 → `fetch_sub(1)` | 栈即将退出 |
| dispatcher 找到 record 后 | 守卫构造 → `fetch_add(1)` | 结算作用域开始 |
| 结算作用域结束（`onRealCr` 返回后） | 守卫析构 → `fetch_sub(1)` | 作用域结束 |
| `stop()`/超时 fence 的兜底结算 | 不持引用（调用方已 join 执行体；`onUnposted` 发现 refs>0 时拒绝释放，§6.9） | 与 CQE 侧同构 |

三条硬规矩：

1. **只有 `fetch_sub` 返回 0 的线程调用 `advance`**（不是"每个进入的线程都调用"）——避免无谓 CAS 竞争，也让"最后一个离开的人关门"语义显式；
2. `handler_refs` 不参与生命周期（`shared_ptr` 已经保证），只参与门判定；
3. 每个作用域用 RAII 守卫（`HandlerRef`）：**守卫同时负责加与减**，门入口不自己加减，因此异常/提前返回路径都不漏减。

### 6.6 交错矩阵（W2a 验收依据）

| # | post 返回 | CQE / 证据 | refs 结束时 | 期望 |
| --- | --- | --- | --- | --- |
| 1 | Accepted 先 | SUCCESS 后到（`wr_terminal`） | 0 | `onSettle(COMPLETED)` 一次；`onOwners(kReturn)` 一次 |
| 2 | 未返回 | SUCCESS 先到 | CQE 侧 0，post 侧仍 1 | **不归还**；post 返回后 `onOwners(kReturn)` 一次 |
| 3 | Accepted 先 | SUCCESS 同时到（两线程同时减到 0） | 两方都见 0 | `settle`/`return` 的 CAS 各只一胜；`onBlocked` 允许重复 |
| 4 | Accepted 先 | 第二/第三条重复 CQE | — | `onDuplicate` 一次；record 不被二次触碰 |
| 5 | NotAccepted | 永不到达 | 0 | `onSettle(CANCELED)` + `onOwners(kReject)` 各一次；不归还、不报故障 |
| 6 | Accepted | 错误 CQE（`wr_terminal`） | 0 | `fault_seen`；`onFault` 一次；`onOwners(kFinalize)` 一次 |
| 7 | Accepted | 超时 fence 成功（`fence_proven`）后迟到 SUCCESS | 0 | `onSettle(TIMEOUT)` 优先；`onOwners(kFinalize)` 一次；迟到 CQE 只走 `onDuplicate`/未知 token |
| 8 | 未返回 | 无 CQE，超时先到 | refs>0 | 本轮跳过（D-7）；post 返回后才 fence |
| 9 | 未返回 | fence 失败 | — | 撤销 `timed_out`、**不释放**、下轮重试 fence（§6.7） |
| 10 | 均未发生，`stop()` | — | posting 已 join | 兜底 `onSettle(FAILED)` + `onOwners(kFinalize)` 各一次 |

### 6.7 超时入口（D-7，W2 形态）

```text
scanTimeouts(now):
  for rec in records_ where deadline <= now && !timed_out:
      if rec.handler_refs != 0:                       // post 尚未返回：不动任何标记
          stats.timeout_deferred_by_post_return++
          continue
      if !gate::onTimeout(rec, sink):                 // 一次性：置 timed_out + 副作用（= 现状 recordTimeoutOnce）
          continue                                    // 已被别的出口认领
      status = fence(rec.endpoint)                    // 同步 quiesce（W5 删除）
      dispatch 每个 drained completion                // 经同一 dispatcher（可能正好是真实 CQE）
      lock(records_):
          if rec 已不在表内: continue                  // 上面的完成已认领并删表，交给它结算
          if status.ok(): erase rec
      if !status.ok():                                // fence 未成立
          rememberEndpointDrain(rec.endpoint)
          gate::onFenceFailed(rec)                    // 撤销 timed_out（尚未认领逻辑结果）
          continue                                    // 不释放、不结算，下轮重试 fence
      gate::onFenceProven(rec, sink)                  // 置 fence_proven 并 advance：
                                                      //   onSettle(TIMEOUT) + onOwners(kFinalize)
```

四个必须与现状逐字对齐的点：

1. **fence 失败时撤销 `timed_out`**（现状 `workers.cpp:1188-1190`），且此时**逻辑结果尚未认领**：fence 不成立就没有"旧 DMA 已停"的证明，此时既不能释放 owner，也不能阻止后续自然完成。撤销标记让该记录回到"在途"，下一轮重新尝试 fence——**不是重发 WR**。
   `onTimeout` 只置 `timed_out`，**不认领故障副作用**：`fault_reported` 在随后 `advance` 的 `onFault` 里才认领。否则"fence 失败撤销超时 → 之后到达真实错误 CQE"会因侧效应已被认领而丢掉 rail 记账与成员退役（基线 `recordTimeoutOnce` 也只在有证据后执行），与 IN-11 冲突。
2. **先 dispatch drained 再判处置**：quiesce 回吐的完成可能正是该 token 的真实 CQE，必须先经 dispatcher 记账（它自己会认领 `wr_terminal`），再决定 owner 处置，否则会漏掉"硬件责任已终结"这一事实。
3. **erase 与"是否仍属于我"在同一临界区判定**：若 CQE 侧已认领并删表，扫描方必须放弃处置（否则两个出口都去释放）。这是 D-3 在超时路径上的落点。
4. **`retryable` 的现状语义保留**：`onSettle` 里按现状给 TIMEOUT 结果（fence 成功后允许重试），W5 才改重试策略（§1.4.1）。

### 6.8 正常归还顺序（§7.4 的 W2 落地）

| 序 | 动作 | W2 实现 | W3+ |
| --- | --- | --- | --- |
| 1 | `release(lease)` | `endpoint->releaseOutstanding(bytes)` | `pool->release(lease)`（可拒还转退役） |
| 2 | 释放 K / quota | `quota_->release(rec.quota)`；K 不存在 | `releaseAttemptSlot()` + quota |
| 3 | 释放 target/segment 引用 | 清空 `remote_segment`/`local_segment` | 同 + `unimportRemote` 协调 |
| 4 | 解除 buffer pin | 无显式 pin；slice 引用在下一步后自然释放 | 显式 pin 计数 |
| 5 | 投递逻辑结果 | `slice->resolveAttempt()`（仍在本线程，W4 才迁执行器） | 入 callback 执行器 |

**顺序不可换**：第 1、2 步先于第 5 步，否则新 attempt 可能在旧 lease 未归还时被准入（这正是 §2.3 要修的问题）；第 3 步后于第 2 步，因为 quota 记账可能仍读 segment 长度。**逻辑结果的"决定"已在 `onSettle` 完成，这里只做投递**，因此重试不可能早于资源归还。

### 6.9 `stop()` 与未决

- `posting_threads_` join 之后所有 post 栈已退出，`handler_refs` 只可能来自 poller；`stop()` 的兜底与 CQE/超时侧共用同一组认领位（`settle_claimed` / `lease_return_claimed` / `finalize_claimed` / `reject_claimed`），任一已被认领则兜底只补结算、不再释放，**双释放不可能**。
- `stop()` 现有"quiesce 全部 endpoint → dispatch drained → 等 100ms → 停 poller → 清空剩余"的顺序**不动**；只把尾部 `releaseInflight + resolveAttempt` 换成 `gate::onUnposted(rec, kShutdown, sink)`（内部就是 `settle` + `onOwners(kFinalize)`）。
- `onUnposted` 的调用约定是"posting 与 poller 均已 join"，因此 `handler_refs` 必为 0。防御性地：若发现 `refs > 0`，只补 `settle` 并**拒绝释放**（`handler_ref_leak_on_finalize++`），把缺陷暴露成计数而不是"post 未返回就回收内存"——宁可泄漏一条记录，也不换一次 use-after-free。
- 返回值语义不动（W6 才引入 Draining/Unproven 报告）。

## 7. dispatcher 骨架

### 7.1 判定顺序（与探测详设 §11.2 一致）

```text
dispatchCompletion(c) -> bool:          // 返回值语义沿用基线：本完成是否被某个责任消费
  1. c.token == 0
       -> entity_events_.onMarkerOrUnknown(...)             // W2：只计数；W4 起按接口 8 区分 marker/AE
          return false                                     // 基线也返回 false，行为不变
  2. isProbeToken(c.token) && probe_manager_
       -> probe_manager_->onProbeCompletion(c)              // namespace 先行，不查业务表
          return true
  3. records_.find(c.token) 命中
       -> HandlerRef 守卫；gate::onRealCr(...)              // 业务结算（含重复判定）
          return true
  4. 未命中（未知/迟到/重复里"已被认领并删表"的那部分）
       -> stats.unknown_token++
          entity_events_.onUnknownToken(c.token, jfc, evidence)
          return false                                     // 不影响其他责任
```

第 1 步在 W2 只计数；第 4 步的 `unknown_token` 同时涵盖"从未见过"与"重复完成"，两者的区分靠 `gate::onRealCr` 里的 `wr_terminal` 判定（重复会先命中第 3 步的 record，由 `onDuplicate` 记账）。**`duplicate_cr` 与 `unknown_token` 不是同一个数**：前者说明"record 还在、只是已完成"，后者说明"record 已不在表内"。

### 7.2 四类输入的处置

| 输入 | 判定 | W2 处置 | W4 补齐 |
| --- | --- | --- | --- |
| 业务 token 命中 | registry 查表 | 双入口推进完成门 | 十类分类、flag/status 拆分 |
| 探测 token | 高 16 位 | 原样转发 PLM（含带 flag 的真实 CQE） | 不变（namespace 先行已保证） |
| token == 0 | — | 只计数，返回 false（与基线一致） | marker/AE 按实体与 kind 分流后经接口 8 上报 |
| 未知/迟到/重复 | 查表失败 | 记账 + sink 上报 | 与 marker 上报共用接口 8 形状 |

### 7.3 unknown token 语义

- 计数分两个：`unknown_token`（从来没见过）与 `duplicate_cr`（见过且已被认领，即重复完成）。两者的区分靠"record 已从 registry 删除"这一事实，因此**删除权归认领方**（D-3）同时解决了可观测性。
- `EntityEventSink` 在 W2 只有一个空实现（计数 + `LOG_EVERY_N`），签名按接口 8 预留 `(jfc_key, entity_type, local_id, kind, evidence)`；W4 填内容时不改调用点。
- **不因未知 token 中断轮询**、不影响其他 token（对应总设计 §14 异常矩阵"未知 token"行）。

### 7.4 三处完成消费点必须同源

| 位置 | 现状 | W2 |
| --- | --- | --- |
| poll 主循环（`workers.cpp:446`、`:467`） | `dispatchCompletion` | 不变（函数体升级） |
| `stop()` 的 quiesce 回吐（`workers.cpp:~235`） | `dispatchCompletion` | 不变 |
| `scanTimeouts()` 的 quiesce 回吐（`workers.cpp:1173`） | `dispatchCompletion` | 不变 |

这三处是探测详设 §9.1 明确点名的"必须替换全部三处"，W2 保持单一入口，禁止任何一处绕过 dispatcher 直接调用 `handleCompletion`。

## 8. 接缝与改造清单

| 文件 / 位置 | 动作 | 要点 |
| --- | --- | --- |
| `workers.h:182` `struct Inflight;` | 删 | 前置声明换成 `struct BusinessRecord;` |
| `workers.h:201` `dispatchCompletion` | 保留签名 | 内部改走 §7.1 |
| `workers.h:208-215` `releaseInflight/resolveInflight` | 拆 | `releaseOwners(rec, disposition)` + `settleSlice(rec)` 两个小函数，由 `gate::Sink` 调用 |
| `workers.h:246-253` `inflight_` | 改名 | `records_` + `records_mutex_`；`inflight_cv_` 保留（`stop()` 等它） |
| `workers.cpp:54-68` `Inflight` | 替换 | `BusinessRecord`（§5.3） |
| `workers.cpp:962` `inflight_.emplace` | 改 | 发布 record：`post_claimed` 置位、`handler_refs=1` |
| `workers.cpp:979` `posted_count == 1` | 改 | 收口为 `legacyAcceptance(status, posted_count)`（§0 D-5），随后 `gate::onPostReturn` |
| `workers.cpp:978-1009` NotAccepted 分支 | 改 | 走 `settle` + `onOwners(kReject)`；回滚内容 W2 不变 |
| `workers.cpp:1052-1146` `handleCompletion` | 拆 | 查表 → `HandlerRef` → `gate::onRealCr`；分支逻辑移入 sink |
| `workers.cpp:1148-1216` `scanTimeouts` | 改 | §6.7：refs 前置 + finalize 收口 |
| `workers.cpp:1218-1233` `releaseInflight` | 改 | 变 `releaseOwners(rec, disposition)`（一次性 CAS 用对应认领位） |
| `workers.cpp:163-296` `stop()` | 改 | 尾部兜底改 `onOwners(kFinalize)`（含 `kShutdown` 原因）；其余顺序不动 |
| `probe_link_manager.h:82-107` | 改 | `ProbeRecord` 嵌入 core（§5.5） |
| 新增 `completion_record.*`、`entity_event.h` | 新增 | §4.2 |

**施工顺序（每步可编译、可跑既有回归）**：

1. 新增 `completion_record.h/.cpp` + 门单测（此时无人调用，纯新增）；
2. `BusinessRecord` 替换 `Inflight`，`inflight_` 改名，**行为不变**（`releaseInflight` 原样搬进 `releaseOwners`）——此步只验证回归；
3. 接入 `onPostReturn` / `onRealCr` 与 `handler_refs`（行为变化点，§2.3）；
4. `scanTimeouts` 加 refs 前置与 finalize 收口；
5. dispatcher 骨架 + `unknown_token`/`duplicate_cr` 观测 + `EntityEventSink` stub；
6. `ProbeRecord` 内核统一（独立 commit，可整条回退）；
7. 测试补齐（early-CQE/重复/未知/cancel 交错）与文档回写。

## 9. 观测

`CompletionStats`（原子计数，`snapshot()` 返回普通值结构）：

| 指标 | 含义 | 期望 |
| --- | --- | --- |
| `record_published` / `record_settled` / `record_returned` / `record_finalized` / `record_rejected` | 发布与四条出口的流量 | `published == settled + 未决`；`settled == returned + finalized + rejected` |
| `early_cqe` (= `cqe_before_post_return`) | CQE 早于 post 返回的次数 | 常态 > 0（fake 下必现）；不是异常 |
| `duplicate_cr` | 重复 CQE | 每次重复 +1，且 record 不被二次触碰 |
| `unknown_token` | 查表失败 | 只在真的迟到/未知时增长 |
| `gate_blocked_by_{post_return,cqe,responsibility,handler_refs,fault,not_accepted,acceptance}` | 门阻塞原因分布（`BlockReason` 逐项） | 用于解释"为什么还没归还/没结算"；`acceptance` 只在不接受状态无法证明时增长 |
| `not_accepted_with_completion` | "确定零接受"却收到真实完成（自相矛盾的适配器） | **每条冲突记录 +1**（由 `conflict_claimed` 在 refs 归零的 `advance` 汇合点判定，两个到达顺序都精确计入）；由 reject 出口隔离该端点。记录若已被 reject 出口删表，迟到完成改按 `unknown_token` 记账——仍然可见，只是换了桶 |
| `gate_claim_race` | 两线程同时争到 refs==0 | 正常竞争，非缺陷 |
| `one_shot_violations` | 任一次性位被写两次 | **恒 0**，非 0 即缺陷 |
| `handler_ref_leak_on_finalize` | finalize 时 refs>0 | **恒 0** |
| `timeout_deferred_by_post_return` | 超时因 post 未返回而延后 | 常态接近 0；持续增长说明 adapter post 阻塞 |

日志：记录相关日志统一带 `token / local_nic / segment / remote_nic / generation / terminal_category`，热路径用 `LOG_EVERY_N`（每 1000 次一条）。**不在归还路径打 INFO**。

## 10. 不变量与异常矩阵

### 10.1 不变量（写进代码注释，并由测试守住）

| 编号 | 不变量 |
| --- | --- |
| IN-1 | 每个 token 的 `terminal` 写入 / `settle` / `reject` / `finalize` / `return` 五个动作各最多发生一次（五个动作认领位），另有两个辅助认领位保证副作用只做一次：`cr_claimed` 保 `terminal` 写入、`conflict_claimed` 保冲突上报（`fault_reported` 保故障副作用）；共 7 个一次性位 |
| IN-2 | 正常归还与 finalize 互斥：`fault_seen`/`timed_out` 任一置位后永不走正常归还 |
| IN-3 | **没有责任证据（`wr_terminal` / `fence_proven` / `NotAccepted`）时不释放任何 owner**；`fault_seen` 单独不足以释放 |
| IN-4 | registry 的删除只发生在完成认领方；`onPostReturn` 仅在 `NotAccepted`（零接受、不可能有 CQE）时删表；超时路径删表前必须复核"仍属于我" |
| IN-5 | `wr_terminal`/`post_returned` 置位后，其读方看到的结果字段必然已写入（§5.2 顺序） |
| IN-6 | `handler_refs` 在任何 sink 回调返回后不为负；finalize/return 时必为 0 |
| IN-7 | 逻辑结果的**决定**只发生一次（`settle_claimed`）；其**投递**（`resolveAttempt`）在 owner 释放之后、`handler_refs==0` 之后 |
| IN-8 | 未知 token 不改变任何其他 record 的状态，不中断轮询 |
| IN-9 | `token == 0` 永不进入业务 registry 查找 |
| IN-10 | 探测记录的事实位与业务同源（同一 `CompletionRecord` 定义），探测语义零变化 |
| IN-11 | 超时路径在 fence 失败后必须撤销 `timed_out`，且不重复上报副作用（`fault_reported` 一次性） |

### 10.2 异常矩阵

| 场景 | 必须证明 | 用例 |
| --- | --- | --- |
| early CQE | owner 不提前释放；归还一次；post 返回后才结算 | G-2、I-1 |
| post 先返回 | 归还一次 | G-1 |
| 重复 CQE | 只结算一次 + 计入 `duplicate_cr` | G-4、I-3、G-16（并发首次完成） |
| NotAccepted | 不归还、不记故障、回滚完整（W2 现状回滚） | G-5 |
| 错误 CQE | `fault_seen` + finalize 一次 + 不归还 | G-6 |
| 超时 + 迟到成功 | 不归还；finalize 一次；逻辑 TIMEOUT | G-7、I-4 |
| 超时 + post 未返回 | 本轮跳过；post 返回后才 fence | G-8 |
| 超时 fence 失败 | 撤销 `timed_out`、不释放、不重复上报，下轮重试 | G-14、I-4 |
| cancel 在途 | `tryCommitPost` 失败路径不变（未 publish 即不涉及门） | 既有用例 |
| cancel 在登记但未 post | 不占 lease/quota；不产生 record | 既有用例 |
| `stop()` 兜底 | settle 一次、owner 清理一次 | G-10、I-5 |
| 未知 token | 记账不崩、不影响其他责任 | I-2 |
| probe 完成 | 仍转发 PLM；不影响业务表 | 既有 12 个 probe 用例 |

## 11. 测试矩阵

### 11.1 门单测（`tent_ub_completion_gate_test`，不链接 `workers.cpp`）

用 fake `gate::Sink` 记录回调顺序与次数；用注释里的"交错脚本"按 §6.6 的 10 行逐条驱动。并发用例用 `std::barrier`（或 atomic spin 栅栏）让两个线程在同一点同时进入 `advance`，循环 10k 轮断言"归还计数 == 1"。

| 用例 | 内容 |
| --- | --- |
| G-1..G-10 | §6.6 交错矩阵逐行 |
| G-11 | 内存序：post 线程写 `acceptance`，CQE 线程读——用 TSan 目标可选；无常驻 TSan 目标时用"延时注入"制造反序 |
| G-12 | `handler_refs` 泄漏检测：故意少减一次，断言 `handler_ref_leak_on_finalize==1` 且不崩溃 |
| G-13 | 一次性位被写两次 → `one_shot_violations==1` |
| G-14 | fence 失败撤销路径：`timed_out` 回 false、无 owner 释放、`fault_reported` 只置一次 |
| G-15 | `legacyAcceptance()` 映射表：`posted_count ∈ {0,1}` × `status ok/fail` 四种组合，另加 `posted_count>1 → Unknown` |
| G-16 | 并发首次完成：4 线程同时进入 `onRealCr`，仅一条落账、`terminal` 不混写（TSan 下同样为零告警；旧实现会报数据竞争） |

### 11.2 集成（fake adapter）

| 用例 | 位置 | 依赖新增 fake 钩子 |
| --- | --- | --- |
| I-1 early CQE：post 内部把 CQE 推入 JFC 并阻塞到 poller 排空，断言未提前 release | `ub_native_data_path_test.cpp` | `setPostGate(std::function<void()>)`（在 push 之后、return 之前调用） |
| I-2 注入未知 token CQE | 同上 | `injectCompletion(JfcPtr, token)` |
| I-3 注入重复 token CQE | 同上 | 同上 |
| I-4 超时 + 迟到成功 | 同上 | 既有 `holdNextCompletion` + `pushPendingCompletion` |
| I-5 `stop()` 有在途记录 | 同上 | 既有 |
| I-6 probe 转发不受影响 | 既有 `ub_probe_integration_test.cpp` | 无 |

`ub_fake_urma.h` 需要新增的钩子（约 40 行）：`setPostGate`、`injectCompletion`、`completionDeliveredCount`。全部只在 fake 内生效，不影响真实 adapter。

### 11.3 回归门槛

```bash
cmake --build build --target tent_ub_completion_gate_test tent_ub_completion_record_test \
      tent_ub_native_data_path_test tent_ub_teardown_test tent_ub_probe_integration_test -j
ctest --test-dir build -R "tent_ub_" --output-on-failure
```

必须全绿，尤其是：`MissingCompletionIsFencedBeforeRetryAndEventuallyCompletes`（超时语义未变，仍断言 `quiesceCalls>=1` 与 `timeouts>=1`）、`ProviderLostCompletionIsReclaimedAfterSuccessfulFence`（fence 后回收语义未变）。

## 12. 实施切分

| 片 | 内容 | 出口 | 依赖 | 量级 |
| --- | --- | --- | --- | --- |
| **W2a** | `completion_record.*` + 门测试 G-1..G-15 | 交错矩阵 10 行 + 认领位严格一次 | 无 | ~250 行 + ~450 行测试 |
| **W2b** | `BusinessRecord` 迁移 + 双入口接入 + refs 纪律 | early CQE 不提前释放；既有回归全绿 | W2a | ~120 行改动 |
| **W2c** | dispatcher 骨架 + `EntityEventSink` + `CompletionStats` + `scanTimeouts` 前置 | 未知/重复 token 可观测；超时语义不变 | W2b | ~90 行改动 + ~200 行测试 |
| **W2d** | `ProbeRecord` 内核统一 + 文档回写 | probe 12 用例原样通过；两侧共用同一结构 | W2b | ~40 行改动 |

W2a 可与池化侧 P0-B 契约冻结并行；W2b 之后若 P0-B/P0-D 就绪，W3 直接替换 `legacyAcceptance()` 与 `release(lease)` 两处即可。单人约 4–5 天，W2a 占一半。

### 12.1 实现状态与落地偏差（2026-11-16）

W2a–W2d 全部落地，文件与 §4.2 清单一致（`completion_record.h/.cpp`、`entity_event.h`、`workers.h/.cpp`、`probe_link_manager.h/.cpp`、两个新测试目标、fake adapter 钩子与四个集成用例）。回归口径：

```bash
cmake --build build --target tent_ub_completion_gate_test tent_ub_completion_record_test \
      tent_ub_native_data_path_test tent_ub_teardown_test tent_ub_probe_integration_test -j
ctest --test-dir build -R "tent_ub_" --output-on-failure
```

§5.1（认领位）、§5.6（删除点）、§6.1/§6.3（伪码与入口表）、§6.5（引用纪律）、§6.7（超时侧效应归属）、§9（指标）、§10.1（IN-1）**已按落地形状改写**，正文与代码一致。下表只列仍与正文有实质差别、需要评审记住的点：

| # | 偏差 | 理由 |
| --- | --- | --- |
| 1 | 故障副作用合并成两类 `FaultKind`（截止期超时 / 真实错误 CQE），后者再按 `terminal.outcome` 分类；**成员退役保持在基线的两处**（错误分类分支内、fence 成功之后） | 基线 CQE 错误分支是"先释放后退役"、fence 成功路径是"先退役后释放"；集中到一处会改变 `EndpointStore` 的 quarantine 时序 |
| 2 | `NotAccepted` 的**逻辑结果**由门判为 `CANCELED`，业务 sink 仍**投递 `FAILED`** + 基线可重试性 | 上游 §8 冻结"post 确定未接受 = FAILED/NotExecuted"，§2.3 要求结算结果不变；W3 引入真三态时再统一（`deliverSettlement` 一个 switch） |
| 3 | `legacyAcceptance()` 对 `posted_count > 1` 返回 `Unknown`（不猜、不回滚），该记录一直阻塞到超时 fence；阻塞原因单列为 `BlockReason::kAcceptance` | 适配器多报时无法证明哪条 WR 在飞；回滚可能回收仍在 DMA 的页（IN-3）。代价是占用到 `slice_timeout_ms`，已由 `gate_blocked_by_acceptance` 观测 |
| 4 | `GateContext` 自持计数器，不注入时钟 | W2 的判定不依赖时间（完成时刻随 `WrFacts` 传入）；无全局变量，单线程即可复现 |
| 5 | `entity_event.h` 增加 `kUnclassified`；token 0 只计数、type 记 `kJfc` | W2 无法区分 marker/AE，只计数不误报类型；W4 填内容时不改调用点 |
| 6 | `dispatchCompletion` 增加两个**默认参数** `local_nic`/`jfc_index` | 接口 8 需要 JFC 键；默认值保持既有调用点（fence/stop 回吐）可用 |
| 7 | `post_claimed` 只写不读 | §5.1 的状态位（防本侧双发、留观测）；"能否 post"的权威判定在 W3 的 `tryClaimPost` |
| 8 | `ProbeRecord`：`settled → core.finalize_claimed`、`timed_out/token → core.*`，身份在 `beginPost` 写入，`core.orphan_reason` 由 `OrphanReason` 映射 | D-9 的机械替换；判定、锁边界、副作用顺序、日志文本未动，探测 12 用例原样通过 |

## 13. 风险与缓解

| 风险 | 影响 | 缓解 |
| --- | --- | --- |
| 把 `handler_refs` 当成生命周期手段，出现"记录提前析构" | 崩溃 | 共享内核只由 `shared_ptr` 持有；refs 与生命周期解耦（§6.5） |
| 双入口中"结果字段写在置位之后" | 读到未初始化 category | §6.1 的"①数据 ②release 发布"两步纪律 + G-11 |
| `onPostReturn` 顺手删表 | 合法 CQE 被判未知 token | D-3 写进注释；IN-4 + I-3 |
| W2 悄悄改了错误分支的重试语义 | 回归失败 / 线上行为漂移 | §6.3 明确"错误分支逻辑保持现状，只换释放收口" |
| 超时路径与 CQE 竞争导致双 finalize | 双释放 | 全部走 `finalize_claimed` CAS；IN-1 + IN-3 |
| probe 内核迁移引入语义漂移 | 探测恢复链退化 | 只允许 §5.5 的机械替换；独立 commit；12 个 probe 用例为门槛 |
| `adapter->post()` 在 provider 内阻塞不返回（D-7 的 liveness 代价） | 该记录不会被超时扫描 fence，只能等 `stop()`，而 `stop()` 同样要先 join posting 线程 | 不赌时效：宁可不释放也不在未知 DMA 上回收内存；用 `timeout_deferred_by_post_return` 持续观测，W5 重构超时归属（record 按 poller 分区、post 阻塞经接口 9/10 上报）后再消除 |
| W2 期间 P0-B/P0-D 契约变化 | 返工 | 记录层只依赖 `PostAcceptance` 与 `RecordPath` 两个自有类型，acceptor 映射集中在 `legacyAcceptance()` |
| 没有真实硬件，门只被 fake 证明 | 实机才暴露 | 与 W1 同样口径：fake 证明记账，实机门槛在 P0 之后 |

## 14. 待确认问题

1. **接口 2 的到达时间**：W2 的错误分支是否要在 `fault_seen` 置位处补一次 `beginErrorOnce` 调用（当前走 `endpoint_retirer_` 与 `probe_manager_->onBusinessPathError`）。若池化侧接口 2 在 W3 才可用，W2 维持现状调用点。
2. **typed CR 的字段形状**（P0-B）：`Completion` 是否新增 `flag`/`cr_type`。影响 W4 挂载点，不影响 W2 结构。
3. **`CompletionStats` 的对外暴露方式**：仓库内 `ProbeStats` 通过 `snapshot()` 供测试与日志，无 metrics 上报通道；W2 是否需要接入 `getNicLoadStats` 之类的既有出口，还是先只做日志。
4. **`resolveAttempt` 留在 poll 线程的代价**：W2 保持现状（D-8），若实测 poller p99 受影响，可提前把 callback 执行器从 W4 拉到 W2 后。
5. **TSan 是否可作为 CI 门槛**：并发用例（G-11）的强度取决于是否常开 TSan。
6. **`stop()` 尾部兜底与 `EntityEventSink` 的顺序**：W6 会重排关闭顺序，W2 需确认不留下"上报先于结算"的隐患。
7. **`UbPostPath` 是否纳入 topology_epoch**（P0-D 可能调整结构）：W2 的 `RecordPath` 转换函数是唯一耦合点，改动量 O(1)。

## 15. 对上游文档的回写清单

**回写状态（2026-11-17）：下列各项全部已回写。**

| 上游位置 | 回写内容 | 状态 |
| --- | --- | --- |
| 总设计 §3 表第 4 行 | 标注 W2 已替换为「共享内核 + 侧挂 owner」 | 已回写 |
| 总设计 §1.3.4 / §7.4 | 归还判据拆三段；责任证据三选一；registry 删除权归完成认领方、post 侧不删表（`NotAccepted` 除外）；`handler_refs` 由 RAII 守卫持有、最后释放者推进门 | 已回写 |
| 总设计 §6 | Unknown 的 W2 过渡态（接口 2 未接入 → 阻塞到超时 fence）；`legacyAcceptance()` 的 `Unknown` 只来自 `posted_count > 1` | 已回写 |
| 总设计 §7.3 | 字段块按落地形态重写：身份 / 状态位 / 认领位 / 侧挂 owner 四组，新增 `cr_claimed`/`settle_claimed`/`conflict_claimed`/`timed_out`/`orphan_reason`；补"共享内核 + 侧挂 owner""决定与投递分离" | 已回写 |
| 总设计 §8 / §13.1 | W2 过渡态（保留 quiesce + `handler_refs==0` 前置、`fault_reported` 延迟到 `onFault`）与"post 永久阻塞则不超时"的 liveness 风险（出口留 W5） | 已回写 |
| 总设计 §9.4 / §11 接口 5 | `ProbeRecord.core` 已落地；接口 5 的"W2 起…"改为已落地（`PostResult` 三态仍待 P0-B） | 已回写 |
| 总设计 §13 | W2 实现状态段（W2a–W2d、三处可观察变化） | 已回写 |
| 总设计 §14 | 新增"超时到期但 native post 未返回"一行 | 已回写 |
| 总设计 §15 | 新指标组（`record_*`/`duplicate_cr`/`gate_blocked_by_*`/`handler_ref_leak_on_finalize`/`not_accepted_with_completion`/`timeout_deferred_by_post_return`）；源码索引与改造点清单补 `completion_record.*`、`entity_event.h` 与两个测试目标 | 已回写 |
| 探测详设 §5.2 / §9.2 | `ProbeRecord` 字段块按 `core` 嵌入重写；结算伪码补旧名（`settled`/`timed_out`/`token`）↔ `core.*` 的映射说明 | 已回写 |
| 探测详设 §3.1 | "CompletionRecord / 完成门统一（W2）"标注已完成 | 已回写 |

注 1：`tent-ub-probe-standalone-supply.md` 无需改动——W2 未触碰探测资源供给（其 `eid` 相关改动属另一项工作）。

注 2：`docs/source/`（Sphinx 站点）不收录 `docs/analyze/`，因此这些回写不需要 `make html` 验证。

## 16. 术语表

| 术语 | 含义 |
| --- | --- |
| 记录（record） | 一条在途 WR 的票据；W2 起为"共享内核 + 侧挂 owner" |
| 内核（core） | `CompletionRecord`：身份标量与一次性事实位，不含任一侧私有类型 |
| 事实位 | 回答"某件事发生过没有"的原子布尔位；用 CAS 保证只发生一次 |
| 认领（claim） | 用 CAS 赢得某一次性动作的执行权；输者必须直接返回 |
| 完成门（gate） | 依事实位判定"能否正常归还"的谓词与出口集合 |
| 正常归还 | 谓词全真后归还 owner；本侧判定是必要条件，最终裁决在池化侧 |
| 责任证据 | 证明"这条 WR 不可能再碰内存"的事实：真实 CQE、fence 成功、或确定零接受（NotAccepted） |
| finalize | 错误/超时/停止路径的 owner 清理段；与正常归还互斥 |
| 回滚（reject） | `NotAccepted` 时逆序撤销已持有资源；不是归还 |
| handler_refs | 正在访问可复用状态的执行体计数；门事实位，不是生命周期 |
| 早到 CQE | CQE 先于 `adapter->post()` 返回到达；W2 的核心修复场景 |
| 重复 CQE | 同一 token 的第二条完成；只记账，不二次结算 |
| 逻辑结果 | `COMPLETED / FAILED / TIMEOUT / CANCELED`；**决定**在 settle 认领时一次写成，**投递**在 owner 释放之后 |
| legacy acceptance | W2 由 `posted_count` 推导的 acceptance；W3 换真三态 |
