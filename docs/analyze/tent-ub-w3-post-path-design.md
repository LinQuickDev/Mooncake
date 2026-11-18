# TENT UB W3 详细设计：选路、准入与三态 post（对齐 rm_pool 池化侧接口）

> 日期：2026-11-17。定位：**W3 子里程碑详设**（总设计 §13 实施切分的第三步）。
> 上游文档（冲突时以上游为准）：父文档《tent-ub-post-poll-threadpool-design》（下称**总设计**）§5/§6/§12；记录与完成门见《tent-ub-w2-completion-gate-design》（下称**W2 详设**）；健康门禁与探测证据见《tent-ub-probe-link-detail-design》（下称**探测详设**）；探测资源供给见《tent-ub-probe-standalone-supply》。
> 本侧代码基线：`e5932b7a`（探测链路：指定 eid），W1、W2 均已实现。
> 池化侧接口基线：`930/Mooncake`（commit `a9edee30`，即同事的 rm_pool 工作区）。本文所有 `send_jetty_pool.h` / `admission.h` / `attempt_gate.h` / `native_budget.h` / `urma_adapter.h` / `params.h` 引用均指该工作区，前缀 `930/Mooncake/mooncake-transfer-engine/tent/`。
> 状态：**设计文档，未实现**。本文只定 W3（post 路径）的形状；W4–W6 只列接缝，不展开。
> 阅读顺序建议：§0（决策）→ §2（心智模型）→ §8（准入与认领）→ §10（归还映射）→ §3（合并清单）。

## 0. 已确认决策与锁定默认

W3 的争议点集中在三件事：**"谁来认领一次 post"**、**"池化侧给的接口哪些直接用、哪些要补"**、**"W2 的门与池化侧的 `AttemptGate` 谁说话"**。先把结论列死。

| 编号 | 决策 | 锁定默认 | 依据 |
| --- | --- | --- | --- |
| D-1 | W3 = 总设计的 W3 片**全部五块**：有界队列、defer 分桶、K 准入、选路健康门禁、原子准入＋三态 acceptance | 是；W4/W5/W6 不在本文范围 | 总设计 §13 W3 行；W2 详设 §1.2 把前四项明确推给 W3 |
| D-2 | **模式门**：仅当 `context->pooledSendPool() != nullptr`（即 `runtime_profile=rm_pool` 且池装配成功）时走 W3 路径；否则逐字节保持 W2 行为 | 是；同一二进制内两套行为并存，回归靠此隔离 | 池化侧 [workers.cpp](../../930/Mooncake/mooncake-transfer-engine/tent/src/transport/ub/workers.cpp#L934-L990) 已是"池存在即走 pooled 分支"的写法；本侧 RC legacy / RM legacy 不可被 W3 波及 |
| D-3 | **账本唯一**：继续用 W2 的 `CompletionRecord` + `BusinessRecord`；**不接入**池化侧 [attempt_gate.h](../../930/Mooncake/mooncake-transfer-engine/tent/include/tent/transport/ub/attempt_gate.h#L86) 的 `AttemptGate`。池化侧的 `pooled_settled` 位由 W2 的 `lease_return_claimed` / `reject_claimed` / `finalize_claimed` 取代 | 是；两个门同时存在就是两套账，必须删一个 | W2 详设 §6（门是超集：认领位、`handler_refs`、超时 fence、矛盾计数）；探测详设 §17 要求"两侧共用同一结构" |
| D-4 | **认领位置**：`SendJettyPool::tryLease()` 成功即"工位已认领"（成员转 `kInUse`，独占、LIFO 不回退）。**池化侧尚无 `tryClaimPost`**（§3.3 M-7），W3 以"本侧 `fault_epoch` 快照复核＋`stopping()` 复核"过渡，并把 `tryClaimPost(lease, fault_epoch_snapshot)` 作为对池化侧的**增补请求 R-1**（待确认 #1） | 过渡形态：窗口压到"复核 → `tryCommitPost` → 发布 → post"，残漏上界＝posting 线程数（§8.2）；R-1 落地后整体替换为一个分支 | 池化详设 §5.2 规则 3 与总设计 §5.5 要求 PostGate ticket/close 线性化认领；930 只有池级 `stopping_`，成员级由独占 lease 覆盖，**路径级必须靠 claim 原子化** |
| D-5 | **`TooManyRequests` 复用**：池化侧对"停止中"和"容量不足"返回同一状态码（[send_jetty_pool.h](../../930/Mooncake/mooncake-transfer-engine/tent/include/tent/transport/ub/send_jetty_pool.h#L226-L233)）。W3 在失败后**二次查 `stopping()`** 分流：true → `requestCancellation`；false → defer。**不新增错误码** | 是；池化侧已有 `stop_rejections` 计数，本侧补一个 `pool_stop_rejections` 便于对账 | 停止中的 defer 等于把 slice 挂死到永久；defer 又不能消费 retry |
| D-6 | **K 归 `UbTask`**（`slice.h` 每 Task 原子计数 `inflight_attempts`）；池化侧 `TaskPermitLedger` 是**进程级**上限（[admission.h](../../930/Mooncake/mooncake-transfer-engine/tent/include/tent/transport/ub/admission.h#L106)），语义与总设计 §5.3"单网络 UbTask 跨 Context 上限"不一致，W3 **不接入**；作为 R-3 请池化侧改为按 task 键控或降级为可选全局风控（默认关闭） | 是；进程级 K=4 会把全进程并发 attempt 锁死到 4，属正确性缺陷而非调参问题 | 总设计 §5.3、§1.2.5；930 该接口的注释自身也写"process-wide" |
| D-7 | **credit 随 lease**：用 `LeaseWithCredit{lease, jfc_index, jfc_credit_token}`；credit 的释放权归结算路径（与 lease 归还同一认领块），池永不释放 caller credit | 是；照抄 [admission.h](../../930/Mooncake/mooncake-transfer-engine/tent/include/tent/transport/ub/admission.h#L23) 的契约 | 池化侧注释与 [send_jetty_pool.h](../../930/Mooncake/mooncake-transfer-engine/tent/include/tent/transport/ub/send_jetty_pool.h#L263-L266) 的 `creditLedger()` 语义 |
| D-8 | **有界队列**：`business_queue_budget` 计入 queued＋parked；submit 按整 Task all-or-nothing，**容量判定严格先于任何 `markQueued`** | 是；默认 `65536`（允许 64 KiB×65536＝4 GiB 整 Task），且启动校验 `>= max_slices_per_task` | 总设计 §5.1 及其注（kQueued 无 unmark 路径，先标记后拒绝会永久卡死该 Task） |
| D-9 | **defer 五桶**：`kTaskPermit / kQuota / kPoolIdle / kCredit / kPathUnavailable`；每桶单调 epoch；"先读 epoch、再判条件、登记后复核"；stopping 全量回流；删除现 [workers.cpp](../../mooncake-transfer-engine/tent/src/transport/ub/workers.cpp#L560) 的 50µs 盲等 | 是；credit 与 pool idle 在池化侧 R-2 未给 miss reason 前合并为 `kPoolIdle`，细分为待确认 #3 | 总设计 §5.4；池化侧 `tryLease` 的 miss 只有一句 `TooManyRequests` |
| D-10 | **健康门禁**：候选必须 `PLM.businessPathUsable(path)`；PLM 为空（探测关闭）→ 全部通过；**图上没有的路径视为可用**（不因探测图不全饿死业务） | 是；查询是本侧新增的 PLM 公共 API（§9.2 的 `resolveVersionLocked` 复用） | 总设计 §5.2；探测详设 §9.2 的可用态是 `HEALTHY ∪ ELIGIBLE_UNVERIFIED` |
| D-11 | **评分口径**：新增本侧 `PathLoadLedger`（path 键的在途 WR/字节），替换 `UbPathSelectionScore::endpoint_outstanding_{wrs,bytes}` 的取值来源；EWMA 保留；`RailMonitor::paused` **不再参与选路**（ISOLATED/退避已等价，且暂停语义归 PLM） | 是；字段改名以免"旧字段装新值"的静默漂移 | 总设计 §5.2"口径修正"段；探测详设 §3.2"W1 期间 rail pause 保持原样，W3 才切健康门禁" |
| D-12 | **三态归一化点**：统一改用 adapter 的四参 `post(jetty, {wr}, posted_count, acceptance)`（[urma_adapter.h](../../930/Mooncake/mooncake-transfer-engine/tent/include/tent/transport/ub/urma_adapter.h#L499)），删除本侧 `legacyAcceptance()`；`kUnknown` 不重放不释放，等 CQE 或 fence | 是；W2 详设 D-5 已承诺"W3 换真三态只改这一个函数" | 总设计 §6；930 `classifyPostAcceptance` 与本侧旧映射对单 WR 等价，可整段替换 |
| D-13 | **归还映射**：`kReturn` → `returnOnce(lease, true)`；`kReject`（确定零接受，未跨边界）→ `returnOnce(lease, true)`；`kFinalize`（超时/首错/关闭）→ `retireFaulted(lease, reason)`。池 `stopping()` 时由池内终裁为退役 | 是；`returnOnce(..., false)` 只作不可判定兜底 | 总设计 §7.4"release 是终裁点"；930 [send_jetty_pool.cpp](../../930/Mooncake/mooncake-transfer-engine/tent/src/transport/ub/send_jetty_pool.cpp#L228-L262) 的 stopping 分支 |
| D-14 | **超时（W5 之前）**沿用 W2 的同步 quiesce，但 fence 目标从 `endpoint->jetty(worker_index)` 改为 **lease 成员的 Jetty**；fence 成功后走 `kFinalize` → 退役 | 是；W5 只删 quiesce 段，不动这里 | W2 详设 D-7；总设计 §1.4.2"lease 交池化侧退役处置" |
| D-15 | **长度校验不纳入 W3**（池化侧 `AttemptGate` 的正常归还谓词含 `length_valid`）。W3 只**记账** `completion_length_mismatch`，不改结算与归还谓词 | 是；改谓词等于改 W2 已冻结的语义，须单独立项 | 总设计 §6.2 谓词无长度项；列为待确认 #6 |

## 1. 范围与不做什么

### 1.1 W3 做什么

| 项 | 内容 | 池化侧对应物 |
| --- | --- | --- |
| 队列 | 有界业务队列（queued＋parked 计预算）、整 Task all-or-nothing submit、启动校验 | 无（W3 新增） |
| defer | 五桶 availability epoch、快照复核、stopping 全量回流、parked 回流保持优先级 | 无（W3 新增） |
| K | `UbTask::tryAcquireAttemptSlot/releaseAttemptSlot`；`deferAttempt` 让 slice 回 `kRetryPending` 且不消费 retry | [slice.h](../../930/Mooncake/mooncake-transfer-engine/tent/include/tent/transport/ub/slice.h#L364) 的 `deferAttempt` 可直接采用 |
| 选路 | PLM 健康过滤、`PathLoadLedger` 评分、pool 有无空闲的候选预筛 | `pool->servingCount()`（需补 `idleCount`，R-2） |
| 准入 | T→K→quota→lease(+credit)→认领复核→commitPost→发布→post；每步失败逆序回滚 | `tryLease / LeaseWithCredit / JfcCreditLedger` |
| 三态 post | `NotAccepted` 回滚、`Accepted` 正常门、`Unknown` 排空 | `PostAcceptance` + 四参 `post()` |
| 归还 | 门出口 → `returnOnce / retireFaulted`，credit 一次性释放，permit 随记录 | `returnOnce / retireFaulted / creditLedger()` |

### 1.2 W3 不做什么（明确拒绝，避免评审预期错位）

| 不做 | 归属 |
| --- | --- |
| 十类事件分类、typed marker、AE 上报、flush 提取服务、callback 执行器 | W4 |
| 超时结构重构（有序集合、按 poller 分区）、删除同步 quiesce | W5 |
| 关闭顺序重排、Draining/Unproven 跨侧移交、接口 8/9/10 真实调用 | W6（接口 10 的 JFC 排他与池 `drainIdle` 的时序见 §11.4） |
| 探测语义（退避、campaign、门禁、重建） | 已冻结，W3 只**消费**健康与 `fault_epoch` |
| 业务 Jetty 的创建/退役/删除机械、`modify ERROR`、`DELETE_READY`、补池 | 池化侧（930 的 `SendJettyPool` + `NativeBudget`） |
| RC 绑定式路径 | 不在本设计范围（探测详设 §3.4 同口径） |

### 1.3 与前后里程碑的接缝

```text
W1 探测（已实现）── 健康状态机 + fault_epoch + 路径桶 bump ─┐
                                                          ├─ W3 选路门禁 / 认领复核 / 三态 / 归还
W2 记录与门（已实现）── CompletionRecord + 双入口 + 三出口 ─┘            │
                                                                        ├─ W4 十类分类 + 执行器（只改 dispatcher 后半段）
池化侧 rm_pool（930 a9edee30）── lease/credit/permit/return ─────────────┘        └─ W5 超时；W6 关闭
```

W3 之后 `workers.cpp` 的"策略"只剩两处可替换点：`chooseAndResolveEndpoint()`（选路＋准入）与 `onOwners()`（归还映射）；记账、门、分派均已冻结。

## 2. 心智模型

### 2.1 一句话

> 一次 attempt 的**一次** native post，必须同时拿到四张票：**K 票**（本 Task 的并发配额）、**quota 票**（路径容量）、**lease 票**（独占一个池成员及其 JFC credit）、**认领票**（路径此刻没被隔离、池没关闭）。四张票都在非阻塞路径上拿，任何一张拿不到就**原路退回并 defer**，绝不在持票状态下等待。

### 2.2 一次 attempt 的八个时刻

```text
t0 route       buildRoute：target / Region / 远端 buffer 就绪（不变）
t1 K           task->tryAcquireAttemptSlot() → 失败 defer(kTaskPermit)
t2 quota       quota_->tryAcquireFirst(paths) → 失败 defer(kQuota)
t3 lease       pool->tryLease(pool_key, attempt_key, LeaseWithCredit)
                 → 失败：stopping() ? cancel : defer(kPoolIdle)
t4 attempt     slice->beginAttempt(path)（本 Task 的第 N 次 attempt）
t5 claim       fault_epoch 快照 + pool->stopping() 复核 → 失败：逆序回滚 + defer(kPathUnavailable)
t6 commit      slice->tryCommitPost(attempt)（取消的线性化点；失败即全量回滚，无 WR 越界）
t7 publish     record 入 registry（token 先发布，CQE 可早于 post 返回）
t8 post        adapter->post(lease.member()->jetty, {wr}, posted_count, acceptance)
                 → gate::onPostReturn：acceptance 一次性写入
```

`t3` 的 `tryLease` 内部已经做了 **LIFO + JFC credit 预留 + 预算**（池化侧实现），所以本侧不再单独"预留 credit"，也不需要 `QuotaManager` 之外的第二个 credit 账本。

### 2.3 W3 相对 W2 的可观察变化

| 变化 | W2 | W3 | 为什么必须现在改 |
| --- | --- | --- | --- |
| 选路 | 不看探测健康；rail pause 生效 | 健康过滤；rail pause 退场 | 否则隔离路径仍被选中，探测恢复等于白做 |
| 队列 | 无界 deque；容量自查 | 有界；整 Task 背压 | 无界队列在大 Task 风暴下 OOM |
| 容量等待 | 50µs 盲等后重排 | 分桶 epoch 唤醒 | 盲等既费 CPU 又丢唤醒（无法区分"K 释放"与"池归还"） |
| 并发上限 | 无 K | 每 Task K | 一个 Task 可以瞬间打满全进程工位 |
| 工位归属 | endpoint 持有 Jetty | 池成员 + lease | 为"每 attempt 一行 native 资源责任" |
| post 结果 | `posted_count` 推断 | 真三态 | `Unknown` 不再被猜成 Accepted |

W3 **不改变**：完成门的判定、逻辑结果的三层语义、错误分支的 retry 计费、探测结算——这是评审时必须守住的不变量。

## 3. 两侧现状基线与差距

### 3.1 本侧（`e5932b7a`）

| # | 现状 | 位置 | W3 目标 |
| --- | --- | --- | --- |
| 1 | 队列是 `array<deque>`，无容量 | [workers.h:271](../../mooncake-transfer-engine/tent/include/tent/transport/ub/workers.h#L271) | 有界＋parked 计预算 |
| 2 | submit 先 `markQueued` | [workers.cpp:333](../../mooncake-transfer-engine/tent/src/transport/ub/workers.cpp#L333) | 先算容量、后改状态 |
| 3 | 容量不足等 50µs | [workers.cpp:560](../../mooncake-transfer-engine/tent/src/transport/ub/workers.cpp#L560) | 五桶 defer＋epoch 唤醒 |
| 4 | 无 K | `slice.h` 无 `inflight_attempts` | 每 Task K＋`deferAttempt` |
| 5 | 选路看 rail pause、`endpoint->outstandingWrs()` | [workers.cpp:801](../../mooncake-transfer-engine/tent/src/transport/ub/workers.cpp#L801)、[workers.cpp:851-861](../../mooncake-transfer-engine/tent/src/transport/ub/workers.cpp#L851-L861) | 健康过滤＋`PathLoadLedger` |
| 6 | 工位 `endpoint->jetty(worker_index)` | [workers.cpp:1051](../../mooncake-transfer-engine/tent/src/transport/ub/workers.cpp#L1049) | `lease.member()->jetty` |
| 7 | `legacyAcceptance()` 从 `posted_count` 推断 | [completion_record.cpp:267](../../mooncake-transfer-engine/tent/src/transport/ub/completion_record.cpp#L262) | 删除，用 adapter 三态 |
| 8 | `releaseOwnerResources()` 只退 outstanding＋quota | [workers.cpp:1377](../../mooncake-transfer-engine/tent/src/transport/ub/workers.cpp#L1377) | 加 `returnOnce / retireFaulted`＋credit |
| 9 | fence 目标为 `endpoint->jetty(worker_index)` | [workers.cpp:1301](../../mooncake-transfer-engine/tent/src/transport/ub/workers.cpp#L1301) 起的 `scanTimeouts()` | lease 成员 Jetty |
| 10 | PLM 只喂证据，无"业务可用"查询 | [probe_link_manager.h](../../mooncake-transfer-engine/tent/include/tent/transport/ub/probe_link_manager.h#L250-L256) | 新增 `businessPathUsable()` / `businessFaultEpoch()` |

### 3.2 池化侧已提供（`930/Mooncake@a9edee30`）

| 能力 | 接口 | 语义要点 |
| --- | --- | --- |
| 每 attempt 独占工位 | [`tryLease(PoolKey, AttemptKey, LeaseWithCredit&)`](../../930/Mooncake/mooncake-transfer-engine/tent/include/tent/transport/ub/send_jetty_pool.h#L231) | 非阻塞；LIFO；JFC credit 在扫描中预留；`TooManyRequests`＝defer 语义；`kInUse` 独占 |
| 归还 | [`returnOnce(lease, completion_gate_passed)`](../../930/Mooncake/mooncake-transfer-engine/tent/include/tent/transport/ub/send_jetty_pool.h#L238) | 一次性；`true` 且池未停 → `push_front` 回 LIFO；`false` 或停止中 → 退役删除；双归还返回 `InvalidArgument` |
| 退役 | [`retireFaulted(lease, reason)`](../../930/Mooncake/mooncake-transfer-engine/tent/include/tent/transport/ub/send_jetty_pool.h#L242) | 首错永久退役（INV07）；删除失败转 `quarantined_` 并保留预算 |
| JFC credit | [`JfcCreditLedger::tryReserve/release`](../../930/Mooncake/mooncake-transfer-engine/tent/include/tent/transport/ub/admission.h#L23) | 令牌制；重复释放无害；`capacity = jfc_depth - jfc_credit_reserve`（install 时算好） |
| 三态 post | [`PostAcceptance` + 四参 `post()`](../../930/Mooncake/mooncake-transfer-engine/tent/include/tent/transport/ub/urma_adapter.h#L323-L356) | 归一化函数 `classifyPostAcceptance(status, posted_count, request_count)` |
| 池生命周期 | `beginStop / drainIdle / drainQuarantined / stats / servingCount` | 停止中 lease 与归还都转退役；`drainIdle` 在无在途时删除空闲成员 |
| 预算 | `NativeBudget::{tryReserve, transition, releaseAfterProvenDestruction}` | 按物理设备计 Jetty 数；删除未证伪不释放 |
| 装配 | [`rm_pool` install](../../930/Mooncake/mooncake-transfer-engine/tent/src/transport/ub/ub_transport.cpp#L485-L554) | 每 Context 一个 `SendJettyPool` + 一个 `JfcCreditLedger`；`Probe.enabled` 目前被要求为 true（见 M-3） |
| 参考数据路径 | [`workers.cpp` pooled 分支](../../930/Mooncake/mooncake-transfer-engine/tent/src/transport/ub/workers.cpp#L934-L1125) | 与本设计 §8 顺序基本一致，可直接对照评审 |

### 3.3 差距与合并清单

930 是从 `1dd24ec1` 附近拉出的分支，**没有** W1/W2 的 `completion_record.*`、`probe_*`、`entity_event.h`。W3 开工的前提是把池化侧文件合入本侧，并做下列取舍：

| 编号 | 项 | 处理 | 备注 |
| --- | --- | --- | --- |
| M-1 | `send_jetty_pool.*`、`admission.*`、`native_budget.*`、`attempt_gate.*`、`params.h`（`runtime_profile`/`send_pool`/`native_budget`）、`urma_adapter.h`（`PostAcceptance`＋四参 post）、`slice.h`（`deferAttempt`）、`bootstrap*`/`identity_import.*`（如 W3 需要 `rmTarget`） | 合入 | 合入即"池化侧 P0-B/P0-D 冻结"的事实形态；`workers.cpp` 的参考实现只作对照，不整文件覆盖 |
| M-2 | `attempt_gate.*` | **不合入**（或只留作池化侧自测） | 与 W2 门重复（D-3）；两份账必然漂移 |
| M-3 | 配置键冲突：930 要求 `runtime_profile=rm_pool` ⇒ `probe.enabled=true`，而本侧 v1.7 探测独立、`probe/enabled` 默认 false | 放宽：`rm_pool` 只要求 `trans_mode=rm`、`physical_only=true`、`jetty_pool.enable=true`；探测键以 v1.7 为准（`probe/enabled` 默认 false，`probe/provider` 等保留） | 930 的 `probe/interval`、`probe/timeout`、`probe/backoff_max` 等旧键与 v1.7 的 `_ms` 形式重名冲突，迁移时以 v1.7 表为准并保留旧键别名一个版本 |
| M-4 | `bootstrap` / `identity_import` / `device_identities` | 归池化侧轨道；W3 只消费 `endpoint->rmTarget(worker_index)` 与 `endpoint->generation()` | 本侧不重复实现 |
| M-5 | 探测 Jetty（v1.7 自建）不计入 `NativeBudget` | 保留现状，作为**风险 RK5**：`rm_pool` 与探测同机启用时，`max_jetty` 需为探测口数预留；记为待确认 #7 | 否则设备上限被业务池吃满后探测建 Jetty 失败 |
| M-6 | 930 `workers.cpp` 无 W1 的 probe 槽位/分派/证据喂给 | 合并时**保留本侧 W1 的接缝**（probe 先分派、三处证据喂给、两个启停钩子） | 探测详设 §11.1–§11.4 是冻结契约 |
| M-7 | 池化详设 §5.2 规则 3 的"PostGate ticket 与 close 线性化后认领唯一 post"在 930 代码里**未实现**（`PostGate` 0 处引用，只有池级 `stopping_`；`active_post` 计数也没有） | 视为**待补**：成员级那一半已由"独占 lease＋退役只发生在结算路径"覆盖；路径级那一半由本侧 `fault_epoch` 快照复核过渡，并作为 **R-1** 请池化侧补 `tryClaimPost`（§8.2、待确认 #1） | 缺口事实必须回写池化详设（§19）；R-1 的时间点决定 §8.2 的过渡代码能活多久 |

## 4. 模块与文件

### 4.1 分层

```text
UbTransport (装配)
  ├─ UbContext ── pooledSendPool() ── SendJettyPool（池化侧）
  ├─ JfcCreditLedger（池化侧，随 pool 附出）
  ├─ ProbeLinkManager（W1，本侧）── 健康 / fault_epoch / 路径桶
  └─ UbWorkers（本侧）
       ├─ PostScheduler：submit 队列、PendingSlice、defer 桶        ← W3 重写
       ├─ PathSelector：候选条件、评分、PathLoadLedger              ← W3 重写
       ├─ Admission：K/quota/lease/claim/commit 与回滚               ← W3 新增
       ├─ CompletionGate：gate::*（W2，冻结）                        ← 不改判定
       └─ OwnerExit：onOwners → pool 调用（W3 改函数体）
```

### 4.2 文件清单

| 文件 | 动作 | 要点 |
| --- | --- | --- |
| `include/tent/transport/ub/post_scheduler.h` + `.cpp` | 新增（可选拆分） | 有界队列、五桶 DeferRegistry、epoch 唤醒；也可先留在 `workers.*` 内，按 §16 分片决定 |
| `include/tent/transport/ub/path_load_ledger.h` + `.cpp` | 新增 | path 键在途 WR/字节；无锁读、原子更新 |
| `include/tent/transport/ub/workers.h` / `workers.cpp` | 改 | 准入顺序、认领复核、pool 归还、fence 目标、K 与 defer 接线 |
| `include/tent/transport/ub/slice.h` | 改 | `inflight_attempts`、`tryAcquireAttemptSlot`、`releaseAttemptSlot`、`deferAttempt`（取自 930） |
| `include/tent/transport/ub/completion_record.h/.cpp` | 改 | 删 `legacyAcceptance()`；加 `completion_length_mismatch` 计数（D-15） |
| `include/tent/transport/ub/probe_link_manager.h/.cpp` | 改 | 新增 `businessPathUsable` / `businessFaultEpoch`（只加只读查询） |
| `include/tent/transport/ub/params.h`、`src/common/config.cpp` | 改 | `business_queue_budget`；合入 `send_pool/*`；交叉校验 |
| `include/tent/transport/ub/send_jetty_pool.h` | 改（小） | R-2：`idleCount(pool_key)`、`creatingCount(pool_key)`、`LeaseMissReason` |
| `src/transport/ub/ub_transport.cpp` | 改 | 合入 rm_pool 装配；把 pool 句柄挂到 Context；`beginStop` 接入 stop 序列 |
| `tests/ub_admission_test.cpp`、`ub_path_selection_test.cpp`、`ub_post_claim_test.cpp`、`ub_pool_integration_test.cpp` | 新增 | §15 |

### 4.3 类型归属

| 类型 | 归属 | 谁写 | 谁读 |
| --- | --- | --- | --- |
| `PoolKey` / `AttemptKey` / `LaneKey` / `PoolMember` / `SliceJettyLease` / `LeaseWithCredit` | 池化侧 | pool | workers（构造 key、持 lease） |
| `PoolMemberState` / `SendJettyPoolStats` | 池化侧 | pool | workers（观测、候选预筛） |
| `JfcCreditLedger` / `TaskPermitTicket` | 池化侧 | 结算路径释放 credit | workers |
| `CompletionRecord` / `BusinessRecord` / `PostAcceptance` | 本侧 | gate/workers | gate/workers |
| `PathLoadLedger` / `DeferRegistry` | 本侧 | workers | workers |

`CompletionRecord` 与 `PostAcceptance` **两侧同名同义**（接口 5）；`AttemptKey` 由本侧构造，映射见 §5.2。

## 5. 数据模型

### 5.1 `BusinessRecord` 的 W3 增量

```cpp
struct UbWorkers::BusinessRecord {
    CompletionRecord core;             // W2，不动
    PendingSlice pending;              // W2
    UbAttemptToken attempt;            // W2
    UbPostPath path;                   // W2
    std::shared_ptr<UbEndpoint> endpoint;
    LocalSegmentPtr local_segment;
    RemoteSegmentPtr remote_segment;
    QuotaReservation quota;            // W2

    // ---- W3 ----
    std::shared_ptr<SendJettyPool> pool;   // nullptr = 非池化模式（W2 路径）
    SliceJettyLease lease;                 // 一次性移交给 returnOnce/retireFaulted
    size_t credit_jfc_index{0};
    uint64_t credit_token{0};
    TaskPermitTicket permit;               // RAII：随 record 析构释放（= WR 责任终结）
    uint64_t fault_epoch_snapshot{0};      // 认领锚点；同时写进 core.fault_epoch
    uint64_t posted_ns{0};                 // W2 已有，保留
};
```

- `lease` 的归还**只允许**发生在 `onOwners`（门已认领出口位之后）；记录析构时若 `lease.valid()`，池的 `onLeaseLeaked` 会把成员转 `kQuarantined` 并计 `leaked_leases`——这是兜底而不是正常路径。
- `permit` 是 `TaskPermitTicket`（RAII、无 native 责任），因此"record 析构即还 K"成立；而 record 的析构点固定在 `onOwners` 的 `eraseRecord` 之后，等价于"WR 责任终结后才还 K"（总设计 §5.3）。
- `fault_epoch_snapshot` 同时写入 `core.fault_epoch`，用于消除探测详设 §15.2 的 `probe_evidence_unmapped`（业务证据的代次可映射）。

### 5.2 身份映射

| 本侧 | 池化侧 | 生成点 | 说明 |
| --- | --- | --- | --- |
| `CompletionRecord.token` | —（池化侧不感知 token） | `nextCompletionToken()` | 高 16 位命名空间，探测/业务分段 |
| `AttemptKey{task_id, slice_id, attempt_generation}` | 同名结构 | `processPending`：`{reinterpret_cast<uint64_t>(task.get()), attempt.slice_id, attempt.attempt}` | `attempt.attempt` 必须非 0（`AttemptKey::valid()` 要求） |
| `LaneKey{jetty_id, lane_generation}` | 同名结构 | 池内创建成员时分配，`lease.lane()` 取出 | 成员代次永不复用 |
| `PoolKey{context_incarnation, trans_mode, priority, bind_tp_type_mask}` | 同名结构 | 由 `endpoint->jettyOptions()` 与 `context->incarnation()` 组合 | 两 attempt 不同 profile 不会共享成员 |
| `core.path`（`RecordPath`） | — | 由 `UbPostPath` 转换 | 日志与证据喂给共用 |

### 5.3 `PathLoadLedger`

```cpp
// 只用于评分，不参与准入（准入归 quota 与 lease）。
class PathLoadLedger {
   public:
    void onPublish(const UbPostPath& path, size_t bytes);   // 发布 record 时 +1/+bytes
    void onFinalize(const UbPostPath& path, size_t bytes);  // 归还/回收时 -1/-bytes（与 quota 释放同点）
    struct Load { uint64_t wrs{0}; uint64_t bytes{0}; };
    [[nodiscard]] Load load(const UbPostPath& path) const;  // 无锁读（分段原子）
};
```

- 键 = `UbPostPath`（local_nic, segment, remote_nic, endpoint_generation）——注意 **generation 参与键**，跨代负载不累加；`onFinalize` 用 record 里的同一 path，保证正负配平。
- 为什么不用 `endpoint->outstandingWrs()`：它随 endpoint 换代清零，会把"换了一代 endpoint"误读成"这条路径空了"（总设计 §5.2 明确要替换）。

### 5.4 `DeferRegistry`

```cpp
enum class DeferReason : uint8_t {
    kTaskPermit = 0,      // K 满
    kQuota,               // 路径容量满
    kPoolIdle,            // 池无空闲成员 / lease miss / credit 不足（R-2 前合并）
    kPathUnavailable,     // 健康不可用、代次失效、认领复核失败、池停止竞争的失败者
    kCount
};

struct DeferredSlice {
    PendingSlice pending;
    DeferReason reason;
    uint64_t wait_epoch;
};

class DeferRegistry {
    std::array<std::deque<DeferredSlice>, (size_t)DeferReason::kCount> parked_;
    std::array<uint64_t, (size_t)DeferReason::kCount> bucket_epoch_{};
    std::array<bool, (size_t)DeferReason::kCount> dirty_{};
    size_t parked_total_{0};   // 计入 queue budget
   public:
    uint64_t epoch(DeferReason r) const;
    bool park(DeferredSlice item);            // 复核 wait_epoch：桶已前进则返回 false（不入表）
    void bump(DeferReason r);                 // epoch++ 且置脏位，notify 一次
    size_t drainDirty(std::vector<PendingSlice>& out);  // 只回流 wait_epoch < 当前 epoch 的项
    void drainAll(std::vector<PendingSlice>& out);      // stopping：全桶回流
    size_t parkedTotal() const;
};
```

- **分桶的意义**：一次 K 释放只 bump `kTaskPermit`，不惊动 pool-miss 桶；一次健康恢复只 bump `kPathUnavailable`。这直接消除 50µs 盲等的 CPU 成本，也避免"任一资源变化就全量重排"的惊群。
- 回流由**任一被唤醒的 post worker 在取队列前**执行（单次扫描消化全部脏桶），回流后按原优先级重新入队；回流失败者以**新快照**重新 park（`defer_reflow_repark` 观测）。

### 5.5 与 W2 门的字段对齐

| W2 字段 | W3 用途 |
| --- | --- |
| `post_claimed` | 仍是"本侧已发"的本地防重位；**权威在 lease 与认领复核** |
| `post_returned` / `acceptance` | 由 `gate::onPostReturn` 写，来源改为 adapter 三态 |
| `wr_terminal` / `fence_proven` / `notAccepted()` | 责任证据三选一（IN-3），W3 不变 |
| `finalize_claimed` / `reject_claimed` / `lease_return_claimed` | **就是**池归还/退役的三条互斥出口（取代 930 的 `pooled_settled`） |
| `handler_refs` | post 栈与结算作用域各持一个（W2），W3 不新增用途 |

## 6. 提交与队列

### 6.1 submit（整 Task all-or-nothing）

```text
submit(task, device_mask):
  if !accepting_ || task == nullptr || !task->sealed(): return错误
  need = task 中处于 {kInitial, kRetryPending} 且未取消的 slice 数   // 只统计，不改状态
  {
    lock(queue_mutex_)
    if (queued_ + defer_.parkedTotal() + need > params_.business_queue_budget)
        return TooManyRequests("UB business queue budget exhausted")    // 整 Task 背压，slice 未被触碰
    for (slice : task->slices()):
        if (!slice->markQueued()) continue;        // 已在终态/已取消：跳过，不占容量
        push(PendingSlice{task, slice, device_mask, priority, ...})
  }
  queue_cv_.notify_all()
  return OK
```

两条纪律（写进代码注释）：

1. **容量判定严格先于 `markQueued`**：`kQueued` 没有 unmark 路径，先标后拒会让被拒 Task 的 slice 永久滞留 `kQueued`，重试 submit 全部失败（总设计 §5.1 注）。
2. `need` 只统计"可入队"slice；`markQueued` 失败者视为已终结，不占预算、不计入 need 的差额补偿（否则取消风暴下会误背压）。

### 6.2 启动校验

```text
if params_.business_queue_budget < params_.max_slices_per_task:
    return InvalidArgument("business_queue_budget must be >= max_slices_per_task")
```

理由：64 KiB×65536 片的整 Task 在 all-or-nothing 语义下必须一次性准入，否则大请求稳定背压（总设计 §5.1 尾段）。`max_slices_per_task` 由现有配置给出，无需新增键。

### 6.3 队列/优先级

- 保持现有三档优先级 `PRIO_HIGH/NORMAL/LOW`（[workers.h:271](../../mooncake-transfer-engine/tent/include/tent/transport/ub/workers.h#L271)），`popPending` 仍按优先级取；回流项按 `pending.priority` 回原档，**不提升优先级**（防回流抖动打乱公平性）。
- `queued_` 计数在 push/pop 处维护，供 §6.1 与 §13 观测共用；停止后 `submit` 直接拒绝。

## 7. 选路与准入

### 7.1 候选资格（四层，顺序固定）

```text
eligible(candidate, pool, plm):
  ① 本侧健康    plm == nullptr ? true : plm->businessPathUsable(candidate.path)
  ② 池化侧资源  pool == nullptr ? true : (!pool->stopping() && pool->servingCount(pool_key) > 0)
  ③ 数据面      endpoint->ready() && endpoint->generation() == candidate.path.endpoint_generation
                 && quota->availability(path, len).can_acquire
  ④ 责任        target/target_epoch 有效、Region/注册代次有效（沿用 buildRoute＋buffers）
```

- ① 的"PLM 为空 → 通过"覆盖探测关闭；"图上没有的路径 → 通过"（PLM 查询返回"未收录"时视为可用）是 D-10 的硬要求：**不能因为探测图的邻接配置不全把业务饿死**，这正是 W1 只做旁路的延伸。
- ② 的 `servingCount()` 是 `idle + in_use`（[send_jetty_pool.h](../../930/Mooncake/mooncake-transfer-engine/tent/include/tent/transport/ub/send_jetty_pool.h#L260)），只用于**预筛**，权威仍是 `tryLease`；所以即使它说"有"，lease 仍可能 miss → `kPoolIdle` 桶。
- ③ 的 `endpoint->outstanding` 账仍保留（quota 与 endpoint 生命周期需要），但**不进评分**（D-11）。

### 7.2 评分

`UbPathSelectionScore` 字段改名与取值：

| 字段 | W2 来源 | W3 来源 |
| --- | --- | --- |
| `quota_available` | `quota_->availability` | 不变 |
| `topology_rank` | 拓扑 | 不变 |
| `normalized_inflight` | quota | 不变 |
| `normalized_quota_wrs` | quota | 不变 |
| `endpoint_outstanding_wrs/bytes` → `path_inflight_wrs/bytes` | `endpoint->outstandingWrs/Bytes` | `PathLoadLedger::load(path)` |
| `has_bandwidth_sample` / `ewma_bandwidth_bytes_per_second` | `RailMonitor` EWMA | 不变（仍采样） |
| `retry_order` / `local_topology_id` / `remote_device_id` | 稳定尾 | 不变 |

比较顺序不变（`betterUbPathScore` 一字不改，只改字段来源）；`rail_stats.paused` 的两处 `continue`（[workers.cpp:801](../../mooncake-transfer-engine/tent/src/transport/ub/workers.cpp#L801)、[workers.cpp:851](../../mooncake-transfer-engine/tent/src/transport/ub/workers.cpp#L851)）删除，暂停语义由 ① 承担。

### 7.3 K 准入

```text
// slice.h（照 930 采用，命名对齐本侧）
UbTask:
  std::atomic<uint32_t> inflight_attempts_{0};
  bool tryAcquireAttemptSlot():  CAS(inflight < task_max_inflight_attempts) ? +1 : false
  void releaseAttemptSlot():     fetch_sub(1)（不小于 0）
slice.h 新增:
  bool deferAttempt(token, now_ns): 校验 token 与 kPosting 状态；
      未取消 → 回 kRetryPending（不 ++retry_count_，不 markFailed）；
      已取消 → 终态 CANCELED；返回是否成功 defer
```

- K 的槽位在 `t1` 拿、在 `onOwners`（record 析构）还：与"WR 责任终结"同点，满足总设计 §5.3"WR 安全结束即可还 K，不必等工位删除"。
- K 满 → `defer(kTaskPermit)`；`deferAttempt` 把 slice 退回 `kRetryPending`，**不消耗 retry**——这是"容量压力 ≠ 失败"的唯一实现方式。
- 池化侧的 `TaskPermitLedger` 不接入（D-6、R-3）。

### 7.4 defer 与唤醒

```text
准入评估（每条件先读自己的桶 epoch，再判）：
  ep = defer_.epoch(kTaskPermit);   if (!acquireSlot())  -> rollback(); park(K, ep)
  ep = defer_.epoch(kQuota);        if (!quota)          -> rollback(); park(Quota, ep)
  ep = defer_.epoch(kPoolIdle);     if (lease miss)      -> rollback(); park(PoolIdle, ep)
  ep = defer_.epoch(kPathUnavailable); if (健康/代次/复核失败) -> rollback(); park(Path, ep)

park(reason, ep):
  if (stopping_) { slice->requestCancellation(); return }     // 停止中不 park
  if (!defer_.park({pending, reason, ep})) {                  // 复核发现桶已前进
      reflowOne(pending);                                     // 立即回流，不入表（防丢唤醒）
  } else {
      ++queued_?  // 否：parked 单独计数，但参与预算
  }

bump(reason):  epoch[reason]++；dirty[reason]=true；queue_cv_.notify_all()
  触发点: K 释放；quota 释放；lease 归还/退役；池 `kProbeHandleReplaced` 等事件；PLM 健康变化（路径桶）
```

回流：

```text
postingLoop:
  while posting_:
    if (!popPending(pending)) {
        if (defer_.hasDirty()) reflow();      // 单次扫描消化所有脏桶，回流项回队列
        else 等待 queue_cv_（超时用 PLM nextDeadline/1ms 上限，**不睡眠 50µs**）
        continue
    }
    processPending(pending, worker_index)
```

> 唤醒源不是"猜时间"，而是**资源变化的同一线程**：`releaseOwnerResources()` 释放 quota 后 bump `kQuota`；`onOwners` 归还 lease 后 bump `kPoolIdle`；PLM 健康迁移后 bump `kPathUnavailable`（探测详设 §9.2 已要求 PLM 内部在可用性变化时 bump 路径桶，W1 已实现，只是 W3 之前无人消费）。

## 8. 原子准入与认领

### 8.1 顺序（固定，逆序回滚）

```text
processPending(pending, worker_index):
  ① route      = buildRoute(pending)                          // 目标/Region/buffer
  ② endpoint,path,quota = chooseAndResolveEndpoint(...)       // 选路＋quota 预留（W2 现有）
  ③ attempt    = slice->beginAttempt(path)                    // 本 Task 第 N 次 attempt
  ④ K          if (!task->tryAcquireAttemptSlot())            // 每 Task K（D-6）
                  -> 回滚(quota/outstanding) + deferAttempt + park(kTaskPermit); return
  ⑤ snapshot   fault_epoch = plm ? plm->businessFaultEpoch(path) : 0   // 认领锚点
  ⑥ lease      pool->tryLease(pool_key, attempt_key, lw)      // 失败见 §8.3/§8.4
  ⑦ claim      if (plm && plm->businessFaultEpoch(path) != fault_epoch) -> 回滚+park(Path)
                if (pool && pool->stopping())                          -> 回滚+park(Path)
  ⑧ commit     if (!slice->tryCommitPost(attempt))             -> 回滚全部（无 WR 越界）
  ⑨ publish    record 入 registry；core.post_claimed=true；core.fault_epoch=snapshot
  ⑩ post       adapter->post(lease.member()->jetty, {wr}, posted_count, acceptance)
                gate::onPostReturn(core, {acceptance, posted_bytes, retryable})
```

注 1：④ 的 K 用本侧 `UbTask` 计数（D-6）；池化侧 `TaskPermitLedger` 改按 task 键控（R-3）落地前不接入，`BusinessRecord::permit` 字段保留但为空。

注 2：⑤ 的快照在 ⑥ lease **之前**、⑦ 的比对在 lease **之后**，覆盖"选路 → lease"这段最长的窗口（§8.2）。

### 8.2 认领复核（D-4 的过渡形态；R-1 待池化侧确认）

```cpp
// 紧贴 native post 之前的复核；无锁长临界区，只读两个标量。
bool UbWorkers::claimPost(BusinessRecord& r) {
    if (r.pool && r.pool->stopping()) return false;                 // 池关闭：快路拦截
    if (probe_manager_ &&
        probe_manager_->businessFaultEpoch(r.path) != r.fault_epoch_snapshot)
        return false;                                               // 路径在评分后被隔离
    return true;
}
```

- **为什么要"先取快照、lease 后再比对"**：`fault_epoch` 由 PLM 在业务首错时 `++`（[probe_link_manager.cpp:1226](../../mooncake-transfer-engine/tent/src/transport/ub/probe_link_manager.cpp#L1226)）；如果在 lease 之后、post 之前该路径已被隔离，这一次 attempt 就应放弃而不是向坏路径 post。快照点落在 lease 之前，比对点落在 lease 之后，覆盖了"选路 → lease"这段最长的窗口。
- **池 `stopping()` 那一维本来就是线性的**：`tryLease` 在池 `mutex_` 内查 `stopping_`（[send_jetty_pool.cpp:72-78](../../930/Mooncake/mooncake-transfer-engine/tent/src/transport/ub/send_jetty_pool.cpp#L72-L78)），所以"lease 成功之后才 `beginStop`"的漏网只会发生在 lease 与 post 之间的一小段；`claimPost` 里的 `stopping()` 复核把它也拦掉，且**即使漏过也无害**——`returnOnce` 在 `stopping_` 时一律转退役（D-13）。
- **成员级"已被判故障"这一维不需要认领**：成员被 lease 期间是独占的，930 的 `PoolMemberState` 只有在归还/退役/关闭 drain/泄漏路径才会离开 `kInUse`（[send_jetty_pool.cpp](../../930/Mooncake/mooncake-transfer-engine/tent/src/transport/ub/send_jetty_pool.cpp) 的状态赋值只在 `returnOnce`/`retireFaulted`/`drainIdle`/`onLeaseLeaked` 里），也就是说**没有第三方能在持有者 post 之前把成员标坏**。池化详设 §5.2 规则 3 的成员级那一半，已被"独占 lease＋退役只发生在结算路径"覆盖。
- **真正非原子的只有路径健康一维**：它由 PLM 在另一把锁里变更，本侧无法与其线性化。这一维正是 R-1 要解决的**唯一**问题。

#### R-1：请池化侧补 `tryClaimPost`（待确认）

池化详设 §5.2 规则 3 原文要求"发布 token 和全部 owner；**PostGate ticket 与 close 线性化，复核 stopping、PathVersion、fault_epoch、target/注册代次后认领唯一 post**"，总设计 §5.5 把这条落成接口 `tryClaimPost(lease, fault_epoch_snapshot)`。930 的工作区**两样都没实现**（`PostGate`/`active_post` 零引用，只有池级 `stopping_`）。请求的形状：

```cpp
// 池化侧：与 beginStop()/成员退役共用同一把 mutex_，票一旦发出，close 不再作废它。
//   OK            -> out 持票；此后路径隔离/池关闭都不再阻止这一次 post
//   GateClosed    -> kStopping | kPathIsolated；本侧回滚（lease/credit/quota/K）并 defer/cancel，不消耗 retry
Status SendJettyPool::tryClaimPost(const SliceJettyLease& lease,
                                   uint64_t fault_epoch_snapshot,
                                   ClaimTicket& out);
```

| 维度 | 说明 |
| --- | --- |
| 为什么必须由池侧实现 | 票的生命周期、成员退役、池关闭都在池侧；只有和这些状态变更共用一把锁，才能"close 不作废已认领、也不放行未认领" |
| 池侧需要什么 | 一条极窄的**故障代次视图**（本侧在隔离时推 `onPathFaultEpoch(path, epoch)`，或池侧按 `(local_nic, segment, remote_nic)` 存一个原子槽）；不需要 token、记录结构或 completion 语义 |
| 本侧需要什么 | 只是把 §8.2 的复核三行换成一次 `tryClaimPost` 调用；`claimPost()` 其余逻辑（回滚、分桶）不变 |
| 补的收益 | 彻底消除"隔离窗口内最多 `worker_count` 次多发 WR"的残漏，`claim_after_isolation` 退化为纯观测，不再承担安全职责 |
| 何时必须补 | 若实机/压测显示残漏已放大成员 churn（判据见下方"过渡期观测"）→ 必须补；否则可排队，但**接口缺口要在池化详设里登记为已知偏离**（§19） |

R-1 落地前的过渡形态就是本节开头的 `claimPost()`；落地后它整体替换为一个分支，其余代码不动。

#### 过渡期的窗口与代价

- **窗口**：`claimPost()` 与 `adapter->post()` 之间只隔 `tryCommitPost`、registry 插入与 `gate::HandlerRef` 构造，全部非阻塞，属微秒级。
- **残漏上界**：每条 posting 线程在窗口内最多泄漏一次 post，因此**单次隔离事件最多泄漏 `worker_count` 次**（默认 6），不会出现风暴式放大；泄漏的这次 WR 要么正常完成，要么再记一次路径错误——而 PLM 对已隔离路径的重复错误幂等、不抬 `fault_epoch`（[probe_link_manager.cpp:1213-1219](../../mooncake-transfer-engine/tent/src/transport/ub/probe_link_manager.cpp#L1213-L1219)）。
- **代价**：一次注定失败的 DMA＋完成；该池成员被 `retireFaulted` 退役（池补一个成员，吃 `NativeBudget`）；campaign 多一轮探测。**没有内存/数据正确性影响**：成员与 buffer 都是本次 attempt 自己的，结算路径与正常失败一致。
- **不采用"发出去再撤销"**：撤销只有 flush/fence 一条路（慢、需完成消费方配合、且"撤销成功"本身要证据，W5 才重构），在未知 DMA 上操作远贵于"少发一次"。

#### 过渡期观测（也是 R-1 的紧迫度依据）

| 指标 | 含义 | 判据 |
| --- | --- | --- |
| `claim_after_isolation` | 复核**拦下**的次数（救回来的） | 常态极小；增长说明隔离频繁 |
| `post_on_isolated_path` | post 返回后比对发现 `fault_epoch` 已变（**残漏的上界估计**，含"post 之后才隔离"） | 与 `claim_after_isolation` 成对看 |
| `pooled_retire{reason=path_fault}` | 因路径故障退役的成员数 | 与 `post_on_isolated_path` 同向增长 → 残漏已在放大 churn |

**R-1 的紧迫度判据**：`post_on_isolated_path / posted_total > 1e-4`（或按天累计超过约定阈值）且 `pooled_retire{reason=path_fault}` 同向上升时，R-1 从"可排队"转为"必须先于 W4 做"。代码注释必须写明这段窗口，避免后来者把它当成漏检。

### 8.3 `GateClosed` 与池停止

| 场景 | 判定 | 处置 |
| --- | --- | --- |
| 池 `stopping()`（`tryLease` 返回 `TooManyRequests`） | 失败后二次查 `pool->stopping()` | true → `requestCancellation`（不是 defer）；false → `park(kPoolIdle)` |
| 池在 lease 成功后才 `beginStop()` | `claimPost()` 看到 stopping | 回滚并 `park(kPathUnavailable)`，下一轮回流时 submit 已关、直接取消 |
| 路径在快照后被隔离 | `claimPost()` epoch 不等 | 回滚并 `park(kPathUnavailable)`（健康恢复会 bump 该桶） |

`GateClosed` 不消耗 retry、不 `markFailed`、不记路径故障（总设计 §5.5 注）。

### 8.4 逆序回滚表

| 失败步 | 已持资源 | 回滚动作（严格逆序） | slice 结算 |
| --- | --- | --- | --- |
| ② 选路失败 | 无 | — | 走 W2 现有 `resolveBeforePost` |
| ③ `beginAttempt` 失败 | quota | `quota_->release`；`endpoint->releaseOutstanding` | 已取消/终态，不处理 |
| ④ K 满 | quota | 同上 | `deferAttempt` + park(K) |
| ⑥ lease miss（`TooManyRequests`） | quota | 同上 | 池停止 → 取消；否则 `deferAttempt` + park(PoolIdle) |
| ⑥ lease 硬错误（非 `TooManyRequests`） | quota | 同上 | `resolveAttempt(FAILED, retryable)`（真实失败，计 retry） |
| ⑦ 认领失败 | quota＋lease＋credit | `credits->release` → `returnOnce(lease, true)` → quota → outstanding | `deferAttempt` + park(Path) |
| ⑧ `tryCommitPost` 失败（取消赢了） | quota＋lease＋credit | 同上 | 取消已由 slice 内部终态化；不得再 defer |

**"未跨 native 边界"是回滚可以 `returnOnce(lease, true)` 的唯一依据**：WR 没发出，成员干净，回池不违反 INV07（首错退役）。这也是池化侧参考实现把回滚归还在 `tryCommitPost` 失败分支里写成 `returnOnce(..., true)` 的原因（[workers.cpp](../../930/Mooncake/mooncake-transfer-engine/tent/src/transport/ub/workers.cpp#L1002-L1021)）。

## 9. 三态 acceptance

### 9.1 归一化点唯一

- 统一调用四参 `adapter->post(jetty, {wr}, posted_count, acceptance)`；默认实现由 `classifyPostAcceptance(status, posted_count, 1)` 给出（[urma_adapter.h](../../930/Mooncake/mooncake-transfer-engine/tent/include/tent/transport/ub/urma_adapter.h#L346-L356)）。
- **删除**本侧 `legacyAcceptance()`；W2 的 `gate::onPostReturn` 保持不变，只是 `acceptance` 从参数来而不是从 `posted_count` 推。
- 单 WR 断言保留：`posted_count > 1` 在池化模式下视为 adapter 缺陷（`InternalError`），但**不得**据此回滚（D-12：`Unknown` 交排空）。

| acceptance | 门的事实 | owner 出口 | 回滚/重试 | 说明 |
| --- | --- | --- | --- | --- |
| `kAccepted` | `post_returned`＋`acceptance` | 正常门（等 CQE） | 不回滚 | 即使 `status` 非 OK，只要 `posted_count==1` 仍是 Accepted（跨了边界） |
| `kNotAccepted` | 同上 | `kReject` | 逆序回滚；slice `FAILED`，**正常消耗 retry** | 确定零接受才允许回滚（IN-3） |
| `kUnknown` | 同上 | 等 CQE 或 fence → `kFinalize` | **不回滚、不重放、不释放 lease** | 自相矛盾的 adapter；`gate_blocked_by_acceptance` 持续观测 |

### 9.2 `Unknown` 的排空路径

`Unknown` 记录在拿到 CQE 前不满足正常归还谓词（`acceptance != Accepted`），也不满足 reject（不是 `NotAccepted`），因此只会：

1. CQE 到达 → `wr_terminal` → 若成功，仍因 `acceptance != Accepted` 被门阻塞（`gate_blocked_by_acceptance`）→ 直到 `slice_timeout_ms` 超时 fence → `kFinalize` → 退役成员；
2. 超时先到 → fence 成功 → 同上；
3. 关闭 → `onUnposted` 兜底 → `kFinalize`。

这三条都不释放 lease 给下一个 attempt，符合"不赌 DMA 已停"。

## 10. 完成与归还

### 10.1 门出口 → 池动作

| 门出口 | 触发 | 池动作 | credit | permit | 备注 |
| --- | --- | --- | --- | --- | --- |
| `kReturn` | 正常谓词全真 | `returnOnce(lease, true)`（池内若 stopping → 退役） | `credits->release` | 随记录析构 | 池是终裁者（总设计 §7.4） |
| `kReject` | `NotAccepted` | `returnOnce(lease, true)`（未跨边界） | 同上 | 同上 | 双保护：门的 `reject_claimed`＋池的 `double_returns_` |
| `kFinalize` | 超时 fence／首错 CQE／关闭未发 | `retireFaulted(lease, reason)` | 同上 | 同上 | 首错必须永久退役（INV07） |

`releaseOwnerResources()` 改造后的骨架：

```cpp
void UbWorkers::releaseOwnerResources(BusinessRecord& r, OwnerDisposition d) {
    if (r.endpoint) r.endpoint->releaseOutstanding(len);        // W2 保留
    if (r.quota.valid()) quota_->release(r.quota);              // W2 保留
    if (r.pool && r.lease.valid()) {                            // W3
        if (d == OwnerDisposition::kFinalize)
            (void)r.pool->retireFaulted(
                std::move(r.lease),
                recordDropReasonName(r.core.orphan_reason.load()));
        else
            (void)r.pool->returnOnce(std::move(r.lease), /*gate_passed=*/true);
        if (auto credits = r.pool->creditLedger())
            credits->release(r.credit_jfc_index, r.credit_token);  // 恰好一次
    }
    defer_.bump(DeferReason::kQuota);        // 唤醒 quota 桶
    defer_.bump(DeferReason::kPoolIdle);     // 唤醒池桶（归还/退役都用）
    queue_cv_.notify_all();
}
```

- **credit 的释放顺序在池调用之后**：池调用内部会移动成员句柄，credit 与成员 JFC 绑定；先释放 credit 可能在"成员尚未归还"时把该 JFC 的额度借给新 WR，虽然语义上仍安全，但会让"额度释放"早于"工位归还"，难以用单一观测口径对账。
- **`path_load_ledger.onFinalize`** 与此同点调用，保证 `path_inflight_*` 与 quota 正负配平。

### 10.2 首错与故障

首次真实错误（`fault_seen`）在 W2 已落成 `kFinalize`＋`onFault`（`beginErrorOnce`／`probe.onBusinessPathError`）；W3 只把"lease 不归还"从注释变成真实的 `retireFaulted`，并保证：

1. `retireFaulted` 的 reason 用错误类别名（`completionOutcomeName`），便于池侧日志归因；
2. 成员退役与 `endpoint_retirer_` 的既有退役是**两件事**：前者是"这个池成员不复用"，后者是"这条 endpoint 代次作废"；W3 两者都保留（W1 的旁路期三罚现象在 W3 后收敛为"路径隔离＋成员退役＋endpoint 退役"中的后两者，rail pause 已退场）。

### 10.3 超时（W5 之前）

```text
scanTimeouts():                                   // W2 结构，W3 只换 fence 目标
  for record in records_ where now >= deadline:
     if (record.core.handler_refs != 0) { timeout_deferred_by_post_return++; continue; }
     gate::onTimeout(...)                          // 暂定标记，不认领逻辑结果
     jetty = record.pool ? record.lease.member()->jetty : record.endpoint->jetty(worker_index)
     status = adapter_->quiesceJetty(jetty, ...)   // 同步 fence（W5 删除）
     status.ok() ? gate::onFenceProven(...) : gate::onFenceFailed(...)
```

fence 之后 `advance` → `kFinalize` → `retireFaulted`。`quiesceJetty` 在池化模式下**必须**用 lease 的 Jetty：`endpoint->jetty(worker_index)` 可能指向另一个（未参与本次 attempt 的）Jetty，fence 错对象等于没 fence。

### 10.4 与探测的咬合

- 证据喂给三处（[workers.cpp](../../mooncake-transfer-engine/tent/src/transport/ub/workers.cpp) 的 success 分支／首错分支／JFC 失败）不变；W3 增加第四处：**认领失败**（`claimPost` 返回 false）时计 `claim_after_isolation`，不触发新的路径故障、不建 campaign。
- `core.fault_epoch` 与 `fault_epoch_snapshot` 写入后，`probe_evidence_unmapped` 应归零（探测详设 §15.2 的收尾项）。
- 探测 Jetty 与业务池**物理隔离**：池的 `beginStop/drainIdle` 不影响 `StandaloneProbeResourceProvider`；关闭顺序见 §11.3。

## 11. 启停

### 11.1 启动

顺序在 W2 基础上只加两处：

```text
install: 池装配（池化侧）→ 失败 fail-closed；成功则 context->attachPooledSendPool
workers.start(): poller 先跑 → probe_scheduler.start → post 池 → submit 开放
校验: business_queue_budget >= max_slices_per_task（否则 install 失败）
```

### 11.2 关闭

```text
stop():
  停 submit / accepting_=false
  队列与 defer 全部 requestCancellation；stopping 触发 defer_.drainAll()（回流后命中谓词者取消）
  posting_=false；join post 池
  for each context with pool: pool->beginStop()        // ← W3 在此点之后不再有新 lease
  probe.notePostingStopped()；probe.drainBeforePollStop(deadline)
  业务 endpoint quiesce（poller 仍在跑）→ 未完成记录 onUnposted → kFinalize → retireFaulted
  polling_=false；join poller
  for each pool: drainIdle()                            // 无在途后才删空闲成员
  for each pool: drainQuarantined()                     // 删除失败者留在 quarantine
  provider 释放探测资源（W1 现有顺序）
```

- `beginStop()` 放在 join post 池**之后**、quiesce 之前：join 之后不会再有新 lease；而 quiesce 阶段仍要靠 `returnOnce/retireFaulted` 结算在途记录，池必须在"停止但可归还"状态（这正是 `returnOnce` 在 stopping 时转退役的语义）。
- `drainIdle()` 必须在 poller join 之后：空闲成员的 JFC 上可能仍有 marker/flush 完成要消费；JFC 级排他属 W6/接口 10（总设计 §7.6），W3 只保证"不提前删"。
- 重复 `stop()` 幂等：`beginStop/drainIdle/drainQuarantined` 均可重入（池化侧实现已保证）。

### 11.3 与探测关闭顺序的关系

探测详设 §9.6/§11.4 要求"调度线程 join 早于停 poller"，W3 不改；W3 只把业务侧的 `beginStop` 插在 posting join 之后。两者共同的硬约束：**任何"删除 Jetty/JFC"的动作不得早于它自己的完成消费方停止**。

### 11.4 与池化侧 drain 的时序

`drainIdle()` 会 `deleteJetty` 空闲成员——它们的 JFC 已完成（无在途 WR 才能回到 idle），所以不需要接口 10；但**退役成员**的删除（`retireFaulted` 内联的 `destroyRetired`）发生在 poll 线程上，它删除的是"已有完成证据或已退役"的成员，仍可能在 JFC 上留下 marker 记录，W4 之后由 flush 提取服务消费。W3 不解决，记入 §17 风险 RK6。

## 12. 配置项

沿用池化侧（`transports/ub/` 前缀，已实现）：

| 键 | 默认 | 语义 |
| --- | --- | --- |
| `runtime_profile` | `rc_legacy` | `rc_legacy` / `rm_legacy` / `rm_pool`；`rm_pool` 走 W3 路径 |
| `jetty_pool/enable` | false | `rm_pool` 必须 true |
| `jetty_pool/force_disable` | false | 运行时刹车，优先级最高 |
| `send_pool/desired_per_context` | 6 | 每 `PoolKey` 的静态 Serving 上限（idle＋in_use），不随负载扩容 |
| `send_pool/task_max_inflight_attempts` | 4 | 池化侧 ledger 的上限（W3 不接入，见 D-6/R-3） |
| `send_pool/max_creating_per_key` | 2 | 并发创建上限（防补池风暴） |
| `send_pool/jfc_credit_reserve` | 16 | JFC 控制额度预留；`capacity = depth - reserve` |
| `native_budget/max_jetty_per_device` | 0（按 verified 推算） | 每物理设备原生 Jetty 预算 |
| `native_budget/replacement_reserve` | 0 | 替换预留 |

W3 本侧新增：

| 键 | 默认 | 语义 | 校验 |
| --- | --- | --- | --- |
| `business_queue_budget` | 65536 | queued＋parked 总容量（片） | 必填正数；`>= max_slices_per_task`，否则 install 失败 |
| `task_max_inflight_attempts` | 4 | **每 Task** K（与 `send_pool/...` 同名不同义，见待确认 #4） | 正数；`rm_pool` 下才是硬约束 |
| `defer_reflow_batch` | 256 | 单次回流最多处理的项数（防长临界区） | 正数 |

交叉校验（install 时一次性全查，失败 fail-closed）：

```text
business_queue_budget >= max_slices_per_task
task_max_inflight_attempts >= 1
rm_pool ⇒ trans_mode == rm && jetty_pool/enable && physical_only
rm_pool ⇒ desired_per_context >= 1
```

**与 930 的键冲突**（M-3）：`probe/enabled` 不再由 `rm_pool` 强制为 true；探测键一律以探测详设 §13.1 的 v1.7 表为准（`probe/interval_ms`、`probe/timeout_ms`、`probe/backoff_max_ms`、`probe/jfc_mode`、`probe/provider`、`probe/device_filter`、`probe/test/*`），930 的旧键 `probe/interval` / `probe/timeout` / `probe/backoff_max` 保留一版别名并打 deprecation 日志。

## 13. 观测

复用 W2 的 `CompletionStats`（不变），新增：

| 指标 | 含义 | 期望 |
| --- | --- | --- |
| `queue_depth` / `parked_depth` | 队列与 defer 深度 | 稳态 < budget；持续打满说明 K/池过小 |
| `submit_backpressure` | 整 Task 被拒次数 | 与 `business_queue_budget` 调参相关，不是错误 |
| `defer_pending{reason}` / `defer_epoch_wakeup{reason}` | 各桶 park 与唤醒次数 | park≈wakeup；长期只增说明该桶无释放源 |
| `defer_reflow_repark` | 回流后立即再次 park 的比例 | 高说明唤醒源与条件不一致（丢唤醒反向证据） |
| `defer_bucket_snapshot_retry` | 复核发现桶已前进、不入表直接重试 | 正常为小量；异常高说明判定与登记窗口太长 |
| `task_K_wait` | K 满 defer 次数 | 与 `task_max_inflight_attempts` 呼应 |
| `pool_lease_attempts` / `pool_lease_misses` / `pool_stop_rejections` | lease 结果分布 | miss 高 → `desired_per_context` 或 credit 偏小 |
| `credit_starved` | `tryReserve` 失败次数（R-2 有 miss reason 后可用） | 高 → `jfc_credit_reserve` 偏大或 depth 偏小 |
| `claim_after_isolation` | 认领复核失败次数（拦下的多发） | 常态极小；与 `post_on_isolated_path` 成对看 |
| `post_on_isolated_path` | post 返回后发现路径已隔离（残漏上界） | 与上一项成对看；触发 §8.2 的 R-1 紧迫度判据 |
| `path_inflight_peak{path}` | `PathLoadLedger` 峰值 | 评分是否真的把负载摊开 |
| `completion_length_mismatch` | 完成字节 ≠ 提交字节 | 恒 0；非 0 即缺陷（D-15 只记账） |
| `pooled_return` / `pooled_retire{reason}` | 归还/退役次数 | `return+retire+未决 == 发布` |
| `pool_return_double` | 池报 `double_returns_` 增量 | 恒 0；非 0 说明门与池双账漂移 |

日志：`record` 一行带 `token / local_nic / segment / remote_nic / lane / generation / acceptance / disposition`；热路径 `LOG_EVERY_N`（1000）。**不在归还路径打 INFO**（沿 W2 纪律）。

## 14. 不变量与异常矩阵

### 14.1 不变量（写进代码注释，由测试守住）

| 编号 | 不变量 |
| --- | --- |
| W3-IN-1 | 一次 attempt 最多一个 lease、最多一次 `returnOnce/retireFaulted`、最多一次 credit 释放；三者由门的三个互斥认领位保证 |
| W3-IN-2 | 未跨 native 边界的 attempt 才能 `returnOnce(..., true)`；跨过的 attempt 只有正常门或退役两条路 |
| W3-IN-3 | defer 不消耗 retry、不记路径故障、不 markFailed；回流失败以新快照重 park |
| W3-IN-4 | submit 的容量判定先于任何 slice 状态变更；被拒 Task 的 slice 状态零改动 |
| W3-IN-5 | K 槽位与 record 生命周期同点释放；任何出口都不得先于 `onOwners` 释放 |
| W3-IN-6 | 选路不得因为探测图缺边而排除业务路径；健康只做**减法**（已隔离才排除） |
| W3-IN-7 | `PathLoadLedger` 正负配平：`onPublish` 与 `onFinalize` 成对，跨代不同键 |
| W3-IN-8 | 认领复核与 native post 之间不得有阻塞调用（无锁、无 RPC、无 sleep） |
| W3-IN-9 | 池 `stopping()` 之后不发起新 lease；`tryLease` 的 `TooManyRequests` 必须二次区分停止与容量 |
| W3-IN-10 | 超时 fence 的目标必须是本次 attempt 的 lease 成员（池化模式下） |

### 14.2 异常矩阵

| 场景 | 必须证明 |
| --- | --- |
| 队列满 | 整 Task 背压；slice 状态零改动；重试 submit 可行；parked 计入预算 |
| K 满 / quota 满 / credit 不足 / 池无空闲 | defer；retry 不变；对应桶 bump 后恢复；不惊动其他桶 |
| 池 `stopping()` 与容量不足同码 | 失败后二次判定；停止中取消而非 park，不产生永久 parked 项 |
| lease 成功后池停止 | 认领复核失败 → 回滚 → 取消；lease 归还且成员退役、预算最终释放 |
| 评分后被隔离、post 前复核 | 回滚完整（lease/credit/quota/K/outstanding）；slice 回 `kRetryPending`；`claim_after_isolation` +1 |
| `NotAccepted` | 逆序回滚；成员回池（未跨边界）；retry 正常消耗；`quota`/`outstanding` 归零 |
| `NotAccepted` × 迟到 CQE（矛盾） | 门记 `not_accepted_with_completion`；池只归还一次；不双删 |
| `Unknown` | 不回滚不重放；lease 保留；超时 fence 后退役；`gate_blocked_by_acceptance` 增长 |
| early CQE（CQE 先于 post 返回） | 门等 `handler_refs==0`；lease 不提前归还；credit 不提前释放 |
| 首错 CQE | 结算＋一次性上报＋路径隔离；`retireFaulted` 恰一次；成员不复用 |
| 超时且 post 未返回 | 跳过本轮（W2 D-7）；`timeout_deferred_by_post_return` 增长；绝不 fence 在用记录 |
| 超时 fence 成功 | 用 lease 成员 Jetty；`kFinalize` → 退役；迟到 CQE 不二次结算 |
| 超时 fence 失败 | `timed_out` 撤销；记录保留；成员不归还也不删除（quarantine 兜底） |
| 关闭时在途 lease | `onUnposted` → `kFinalize` → 退役；credit 释放；permit 随记录；池 drain 后无泄漏 |
| 双归还 / 泄漏 lease | 池侧 `double_returns_` / `leaked_leases` 计数；门的 `one_shot_violations` 恒 0 |
| 池删除失败 | 成员进 `quarantined_` 并保留预算；`drainQuarantined` 重试；不丢句柄 |
| 非池化模式（RC/RM legacy） | 逐字节保持 W2 行为；`pool==nullptr` 分支不触碰 lease/credit/permit |

## 15. 测试矩阵

### 15.1 单测（不链接真实 adapter）

| 目标 | 用例 |
| --- | --- |
| `tent_ub_admission_test` | 队列容量边界（含 parked）；先判容量后改状态（被拒 Task 的 slice 仍是 `kInitial`）；K 满 defer 不消耗 retry；`deferAttempt` 回 `kRetryPending`；五桶 epoch 唤醒只惊醒对应桶；快照复核防丢唤醒（bump 发生在判定与登记之间）；stopping 全量回流与取消 |
| `tent_ub_path_selection_test` | 健康过滤（HEALTHY/ELIGIBLE 通过，ISOLATED/RECOVERING/DISABLED 排除）；PLM 为空全通过；图上无此路径通过；`PathLoadLedger` 排序翻转（同一拓扑下负载高者排后）；ledger 正负配平；rail pause 不再影响选路 |
| `tent_ub_post_claim_test` | 准入顺序与回滚表逐行（用 fake pool/adapter 断言调用序列）；认领复核失败的回滚完整性；`tryCommitPost` 失败的回滚；`TooManyRequests` 二次分流（stopping/容量）；非池化模式零行为变化 |
| `tent_ub_completion_gate_test`（W2 既有，追加） | 三态：`NotAccepted`→`kReject`、`Unknown`→阻塞、`Accepted`→正常；`completion_length_mismatch` 只记账不改判定 |

### 15.2 集成（fake adapter + 真 `SendJettyPool`）

| 目标 | 用例 |
| --- | --- |
| `tent_ub_pool_integration_test` | 正常成功 → `returnOnce(true)` 回 LIFO 头；首错 → `retireFaulted` 且成员不复用；early CQE → credit 在 `handler_refs==0` 后才释放；`Unknown` + fence → 退役；关闭交错（lease 在途 + `beginStop`）→ 归还即退役、无泄漏；`drainIdle/drainQuarantined` 后 `stats()` 归零；预算：每成员一 Jetty，退役删除成功后预算释放 |
| 回归 | `ctest --test-dir build -R "tent_ub_"` 全绿；重点 `tent_ub_native_data_path_test`、`tent_ub_teardown_test`、`tent_ub_probe_integration_test`、`tent_ub_probe_standalone_provider_test` |

回归口径（与 W2 一致）：

```bash
cmake --build build --target tent_ub_admission_test tent_ub_path_selection_test \
      tent_ub_post_claim_test tent_ub_pool_integration_test \
      tent_ub_completion_gate_test tent_ub_native_data_path_test \
      tent_ub_teardown_test tent_ub_probe_integration_test -j
ctest --test-dir build -R "tent_ub_" --output-on-failure
```

### 15.3 硬门槛

- `tent_ub_pool_integration_test` 必须覆盖"early CQE + lease"与"超时 fence + lease"两条交错，否则 D-4/D-14 只有纸面论证。
- 非池化模式的回归必须**逐字节**不动：以 `git diff` 审查 `pool == nullptr` 分支不得出现行为差异。
- 实机门槛：需要真实 URMA（`USE_UB=ON` + 有效 `URMA_LIBRARY`）验证 `tryLease` 的 credit 与 `post` 三态；本机默认 `TENT_HAS_REAL_URMA=OFF`，只能对象级编译。

## 16. 实施切分

| 片 | 内容 | 出口 | 依赖 | 量级 |
| --- | --- | --- | --- | --- |
| **W3a** | 有界队列＋all-or-nothing submit＋启动校验 | 队列满整 Task 背压、slice 不被触碰 | 无 | ~150 行＋~200 行测试 |
| **W3b** | 五桶 defer＋epoch 唤醒＋删 50µs 盲等 | 各桶独立唤醒；丢唤醒用例通过 | 无 | ~200 行＋~250 行测试 |
| **W3c** | `UbTask` K＋`deferAttempt`＋接 `onOwners` 释放 | K 满 defer 不消耗 retry；出口配平 | 无 | ~120 行＋~200 行测试 |
| **W3d** | PLM 健康查询＋`PathLoadLedger`＋评分改源＋rail pause 退场 | 隔离路径不再被选中；负载摊开 | W1（已实现） | ~150 行＋~250 行测试 |
| **W3e** | 合并池化文件；准入顺序＋认领复核＋三态＋归还映射＋fence 目标 | 回滚表逐行；三态用例；关闭交错无泄漏 | **合入 930 池化文件**；R-1（可后补）/R-2/R-3（可不阻塞） | ~250 行＋~350 行测试 |

顺序建议：**W3a→W3b→W3c 可立即开工**（纯本侧，不依赖池化侧）；W3d 紧随（只依赖 W1 已实现的 PLM）；W3e 在 M-1 合入后做。单人约 5–6 天，W3e 占一半以上。

每片独立 commit、独立可回退；W3e 的"合入池化文件"与"接线"拆成两个 commit，前者不改行为（池未接），后者切模式门（D-2）。

## 17. 风险与缓解

| 编号 | 风险 | 影响 | 缓解 |
| --- | --- | --- | --- |
| RK1 | 认领窗口非原子（R-1 未落地，见 §8.2） | 单次隔离事件最多多发 `worker_count` 次 WR：浪费一次 DMA、退役一个池成员、campaign 多一轮 | 窗口压到几条指令；`claim_after_isolation`＋`post_on_isolated_path` 成对实测；`leaked/posted > 1e-4` 时把 R-1 提到 W4 之前；R-1 落地后一行替换 |
| RK2 | 池化侧 `TooManyRequests` 语义复用（停止/容量） | 停止中被误 park，slice 挂到进程退出 | D-5 的二次判定＋`pool_stop_rejections`；测试覆盖 lease 后停止 |
| RK3 | 进程级 `TaskPermitLedger` 被误接入 | 全进程并发 attempt 锁死到 `K` | D-6 不接入；R-3；接线处编译期断言（`static_assert(!kUseProcessPermitLedger)` 或直接不引头文件） |
| RK4 | 两套门并存 | 双账、双归还 | D-3；合入时删除 `attempt_gate.*`；`pool_return_double` 恒 0 |
| RK5 | 探测 Jetty 不占 `NativeBudget`（M-5） | `rm_pool` + 探测同机时 `max_jetty` 超额，探测建 Jetty 失败 | 待确认 #7：`native_budget/max_jetty_per_device` 需计入探测口数，或把探测注册进 `BudgetRole::kProbe` |
| RK6 | `drainIdle` 与 flush/marker 交错 | 删 Jetty 后仍有完成记录 | W3 只在 poller join 后 drain；flush 提取服务属 W4/W6；RK6 记入观测 `drain_before_flush_count` |
| RK7 | fence 用错 Jetty | 超时记录被"证明"已停但实际没停 | D-14：fence 目标取 lease 成员；单测断言调用参数 |
| RK8 | 健康过滤引入新饥饿 | 图上无此路径的业务被永久 defer | `businessPathUsable` 对未收录路径返回可用（IN-6）；`path_not_in_graph` 计数 |
| RK9 | `PathLoadLedger` 泄漏（发布后无 finalize） | 评分永久偏斜 | 与 quota 同点释放（IN-7）；`path_load_imbalance` 观测（发布与终结差值） |
| RK10 | 非池化模式被波及 | RC 生产路径回归 | D-2 模式门＋`pool==nullptr` 分支审查；回归全绿 |
| RK11 | 无真实 URMA 硬件 | credit/三态只在 fake 下证明 | 与 W1/W2 同口径：fake 证明记账，实机门槛单独立项 |

## 18. 待确认问题

1. **R-1 `tryClaimPost`**：池化侧是否补 `tryClaimPost(lease, fault_epoch_snapshot)`（与其 close/退役同一同步域）？形状见 §8.2；池侧只需一条按路径的故障代次视图（本侧隔离时推送），不需要 token/记录结构。**若不补，本设计长期停在 §8.2 的过渡形态，并以 `post_on_isolated_path` 的实测值决定何时重新提出。** 这是 W3e 唯一的架构级待决项。
2. **R-2 lease miss 原因**：`tryLease` 是否返回 `LeaseMissReason{kStopping, kPoolIdle, kCredit, kBudget}`（或 `idleCount/creatingCount` 查询）？否则 `kPoolIdle` 与 `kCredit` 合并为一桶，观测粒度变粗。
3. **R-3 K 语义**：`TaskPermitLedger` 是改为按 task 键控（`tryAcquire(task_id)`），还是删除、K 完全归本侧 `UbTask`？本文默认后者，池化侧文件保留但不接入。
4. **键名冲突**：本侧 `business_queue_budget` 与池化侧 `send_pool/task_max_inflight_attempts`；K 的配置键最终放哪、是否保留两份（`/send_pool/` 前缀那份改为 deprecated）？
5. **`AttemptGate` 去留**：池化侧的 C07/D04 门是否随合入删除？若保留，必须明确"只用于池化侧自测、生产路径不构造"。
6. **长度校验**：池化侧 `AttemptGate` 的 `length_valid` 是否应纳入完成门谓词？（D-15 默认不纳入；纳入则改 W2 已冻结语义，需单独立项。）
7. **探测预算**（M-5）：v1.7 自建探测 Jetty 是否登记进 `NativeBudget` 的 `BudgetRole::kProbe`，还是只在 `max_jetty_per_device` 里预留口数？
8. **`desired_per_context` 默认 6 的依据**：实机标定前是否担心"每 Context 6 个 Jetty × 设备数"撞 `max_jetty`？需要与 `native_budget` 的推算公式联合校验（install 时 fail-closed 最好能给出建议值）。
9. **回流的时间上界**：`defer_reflow_batch` 单批 256 是否足够消化 K 释放后的积压？极端（万级 slice 同时 defer）下是否需要多轮扫描的公平性保证。
10. **`PathLoadLedger` 的键粒度**：只按 path（含 generation）是否足够，还是需要 `(path, target)` 双键？（本文默认 path 键，理由是 target 与 path 一一对应。）

## 19. 对上游文档的回写清单

| 上游位置 | 现状 | 回写 |
| --- | --- | --- |
| 池化详设 §5.2 规则 3、§3 `JettyMember`、§7.1.7、§8.3 | PostGate ticket/close 线性化、`active_post` 计数、close 后拒绝新 Slice | **标注现状与偏离**："930 rm_pool 未实现 PostGate/active_post（0 引用），只有池级 `stopping_`；W3 以本侧 epoch 复核过渡，`tryClaimPost` 作为 R-1 待补（W3 详设 §8.2），缺口事实与判据一并登记" |
| 总设计 §5.5 | `tryClaimPost(lease, fault_epoch_snapshot)` 作为冻结接口 | 标注"池化侧 rm_pool 首版未提供（PostGate/`active_post` 均无实现），W3 用本侧 epoch 复核过渡（窗口、残漏上界＝posting 线程数与紧迫度判据见 W3 详设 §8.2），R-1 待补"；保留"claim 应落在池化侧"的目标表述，不降级为最终形态 |
| 总设计 §5.3 | K 归 `UbTask` | 补一句"池化侧的进程级 `TaskPermitLedger` 不采用"（W3 详设 D-6） |
| 总设计 §7.4 | "release 是终裁点" | 补"W3 落地为 `returnOnce(lease, gate_passed)` / `retireFaulted(lease, reason)`，stopping 时池内转退役" |
| 总设计 §4 资源模型 | 无池成员/LIFO/credit 的描述 | 补"rm_pool 下工位来自 `SendJettyPool`（LIFO＋JFC credit＋NativeBudget），endpoint 不再持有发送 Jetty" |
| 总设计 §12 配置 | 无 `send_pool/*`、`business_queue_budget` | 补 §12 的两张表与交叉校验 |
| 总设计 §13 W3 行 | 一句话验收 | 补 W3a–W3e 切片与出口（本文 §16） |
| 总设计 §15 指标 | 有 `early_cqe`/`task_K_wait`/`defer_*`，无池侧指标 | 补 §13 新增指标（`pool_lease_*`、`credit_starved`、`claim_after_isolation`、`pool_return_double`、`completion_length_mismatch`） |
| 探测详设 §15.2 | `probe_evidence_unmapped` 由 W3 写 epoch 消除 | 补"W3 已把 `fault_epoch` 写入 record（W3 详设 §5.1/§10.4）" |
| 探测详设 §3.2 | "W3 才把选路切到健康门禁" | 补 W3 的过滤规则与"图上无路径视为可用"（W3 详设 D-10/IN-6） |
| W2 详设 §1.3 | 接缝图 | 补 W3 实际形态（lease/credit/permit/三态）与本文件链接 |
| W2 详设 §15 回写清单 | 未执行完 | 与本文 §19 一并补齐（指标、异常矩阵"超时延后"行） |

## 20. 术语表

| 术语 | 含义 |
| --- | --- |
| lease（租约） | 一次 attempt 独占一个池成员（`PoolMember`）的凭据；move-only，必须一次性归还 |
| 池成员（member） | 池化侧原生 Jetty 的包装：`LaneKey`＋`PoolKey`＋预算预留＋生命周期状态 |
| LaneKey | 成员代次身份 `(jetty_id, lane_generation)`；代次永不复用 |
| PoolKey | 池分桶键 `(context_incarnation, trans_mode, priority, bind_tp_type_mask)` |
| credit | 每 JFC 的 WR 额度；lease 时预留、结算时释放，令牌制防重复 |
| 认领（claim） | 拿到"此刻允许 post"的证据；本设计里由 lease（池锁内查 stopping）＋`fault_epoch` 快照复核共同承担（过渡形态）；目标形态是 R-1 的池侧原子 `tryClaimPost` |
| 桶（bucket） | defer 的分类槽位；每个桶一个单调 epoch，作为"该资源发生变化"的版本号 |
| 回流（reflow） | 把 parked 项按新快照重新入队；回流失败者以新快照重 park |
| PathLoadLedger | 本侧按 path 聚合的在途 WR/字节台账，只用于评分 |
| 终裁（arbiter） | `returnOnce` 在池 `stopping()` 时把归还改为退役；本侧判定只是必要条件 |
| 责任证据 | `wr_terminal` / `fence_proven` / 确定 `NotAccepted` 三选一（IN-3） |

## 21. 实现状态（W3a–W3e 落地记录）

> 本节由实现侧追加，记录实际落地的形状与相对 §0–§20 的偏离。总设计/探测详设的回写清单（§19）仍单独跟踪。

### 21.1 分片状态

| 片 | 状态 | 主要落点 | 覆盖测试 |
| --- | --- | --- | --- |
| W3a 有界队列＋整 Task all-or-nothing | 已实现 | `post_scheduler.h::businessQueueAdmits`、`workers.cpp::submit` | `tent_ub_admission_test`（边界、parked 计预算、被拒 Task 零改动） |
| W3b 五桶 defer＋epoch 唤醒＋删 50µs 盲等 | 已实现 | `post_scheduler.h::DeferRegistry`、`workers.cpp` 的 `deferPending`/`reflowDeferred`/`popPending` | `tent_ub_admission_test`（桶独立、丢唤醒复现、定时项、批量上限、停止全量回流） |
| W3c `UbTask` K＋`deferAttempt` | 已实现 | `slice.h`（`inline_attempts_`、`tryAcquireAttemptSlot`、`releaseAttemptSlot`、`deferAttempt`、`queuedEligible`） | `tent_ub_admission_test`（K 只封本 Task、defer 不消耗 retry、取消语义） |
| W3d 健康门禁＋`PathLoadLedger`＋rail pause 退场 | 已实现 | `probe_link_manager.{h,cpp}` 的 `businessPathUsable`/`businessFaultEpoch`、`path_load_ledger.*`、`workers.h` 评分字段改名 | `tent_ub_path_selection_test` |
| W3e 池化文件合入＋准入/认领/三态/归还/fence 目标 | 已实现（池化文件合入 + 接线） | `admission.*`、`native_budget.*`、`send_jetty_pool.*`、`urma_adapter.h`、`context.*`、`endpoint.*`、`ub_transport.cpp`、`workers.cpp::processPendingPooled` | `tent_ub_post_claim_test`、`tent_ub_pool_integration_test`（early CQE／首错／停止中 fence 三条交错） |

### 21.2 合入时的改动（M-1 相关）

| 项 | 处理 | 原因 |
| --- | --- | --- |
| `attempt_gate.*` | **未合入** | D-3：与 W2 门重复；`pool_return_double` 恒 0 的守卫由 W2 门的三个认领位承担 |
| `admission.h` 的 `TaskPermitLedger`/`TaskPermitTicket` | **未合入**，只保留 `JfcCreditLedger` | D-6/RK3：进程级 K 是正确性缺陷；文件内注释写明"不得被误接入" |
| 池成员 `LaneKey` | 改名为 **`PoolLaneKey`** | 本侧 W1 已将 `LaneKey` 冻结为探测通道身份（`probe_resources.h`），同 namespace 重定义不可编译；形状与语义不变 |
| 池化侧 bootstrap v2 / `importRemoteTarget` / `bindRm` | **未合入**（M-4） | W3 只消费 `endpoint->rmTarget(worker_index)`：本侧 v1 bootstrap 已携带 `(eid, jetty_id, uasid)`，适配器按 WR 惰性 import 并缓存。RM endpoint 在 `bind()` 中跳过 `bindJetty`，仅保留 peer 描述作为 target 身份 |
| `runtime_profile=rm_legacy` | install 时 **NotImplemented** 失败关闭 | 它依赖 bootstrap v2 目标导入；与其半可用，不如显式拒绝（M-3 只放宽 rm_pool 的探测前置） |
| `params.h` 的 `send_pool/*`、`native_budget/*` | 已合入 | 默认值照 §12；`task_max_inflight_attempts` 的本侧 K 用根键 `task_max_inflight_attempts`，与池化侧同名键不共用 |

### 21.3 相对设计的实现选择

| # | 选择 | 与设计的关系 |
| --- | --- | --- |
| 1 | 认领复核为 `claimPost()`（D-4 过渡形态），未引入 `tryClaimPost` | 与 §8.2 一致；R-1 仍是池化侧待补项，落地后三行替换 |
| 2 | R-2 只补 `idleCount()`/`creatingCount()`，未引入 `LeaseMissReason` | §18-2 的默认：credit 与 pool-idle miss 合并为 `kPoolIdle` |
| 3 | 选路预筛用 `idle>0 \|\| creating>0 \|\| serving<desired_per_context` | 严格按 §7.1 的 `servingCount()>0` 会让冷启动池（serving=0）永久筛掉全部候选；权威仍是 `tryLease` |
| 4 | 队列预算/K/defer 的接线：队列与 defer 对所有模式生效；K 与 lease/credit/claim 只在 `pooledSendPool()!=nullptr` 时生效 | §14.2 "`pool==nullptr` 分支不触碰 lease/credit/permit"；legacy 的 `UbTask::create` 传 `max_inflight_attempts=0`（不限） |
| 5 | 定时 defer：`DeferRegistry::Item::expire_ns` + posting 循环 1ms 上界等待 | 只用于"没有释放源能 bump"的桶：设备恢复冷却、`kPathUnavailable`（PLM 的单一 wake 回调归探测调度器，W3 不抢占，见 #9）、`kPoolIdle` 的兜底；`kQuota`/`kTaskPermit` 仍纯 epoch 唤醒 |
| 6 | 长度校验只记账 `completion_length_mismatch` | D-15；W2 谓词与池归还谓词未改 |
| 7 | 首错/超时后同时保留"成员退役"与"endpoint 退役" | §10.2-2；池化模式下超时 fence 不再动 endpoint |
| 8 | `UbWorkers::stop()` 内插入 `beginStop()`（join posting 之后）与 `drainIdle()/drainQuarantined()`（join poller 之后）；`ub_transport` 的 shutdown 末尾再兜底 drain 一次 | §11.2；幂等，且覆盖"workers 尚未创建就 install 失败"的路径 |
| 9 | 健康门禁把**全部**候选排除时返回 `TooManyRequests`（`kPathUnavailable` + 1ms 定时），不是 `DeviceNotFound`；关闭期先 fence 在途记录的 lease 成员 Jetty，端点 fence 只留给非池化记录 | §7.1 的"健康只做减法"若不配这个出口，一次隔离就会让业务立即失败（RK8 的反面）；D-14/IN-10 要求 fence 目标与实际 WR 一致 |

### 21.4 未做的部分

- 实机门槛（RK11）：本机 `TENT_HAS_REAL_URMA=OFF`，共用测试替身（`ub_fake_urma.h`）证明记账与状态机；真实 URMA 上的 credit/三态仍需单独立项。
- W4 的十类事件分类、flush 提取服务、AE 上报；W5 的超时结构重构（同步 quiesce 仍在）；W6 的关闭顺序重排与接口 8/9/10。
- §19 的上游回写清单（总设计、探测详设、W2 详设）仍需按表补齐。

### 21.5 新增/变更的配置键（实现基线）

```text
transports/ub/runtime_profile            rc_legacy | rm_legacy | rm_pool   (默认 rc_legacy)
transports/ub/trans_mode                 rc | rm                            (默认 rc)
transports/ub/jetty_pool/enable          bool                               (默认 false)
transports/ub/jetty_pool/force_disable   bool                               (默认 false)
transports/ub/send_pool/desired_per_context       正整数  (默认 6)
transports/ub/send_pool/max_creating_per_key      正整数  (默认 2)
transports/ub/send_pool/jfc_credit_reserve        非负整数(默认 16)
transports/ub/native_budget/max_jetty_per_device  非负整数(默认 0=按 verified 推算)
transports/ub/native_budget/replacement_reserve   非负整数(默认 2)
transports/ub/business_queue_budget      正整数，且 >= max_slices_per_task     (默认 65536)
transports/ub/task_max_inflight_attempts 正整数  (默认 4；仅 rm_pool 生效)
transports/ub/defer_reflow_batch         正整数  (默认 256)
```

交叉校验（install 时 fail-closed，全部在 `UbParams::FromConfig` 内）：
`business_queue_budget >= max_slices_per_task`；`rm_pool ⇒ trans_mode=rm ∧ jetty_pool/enable ∧ !force_disable ∧ desired_per_context>=1`；
`rc_legacy ⇒ trans_mode=rc ∧ !jetty_pool/enable`；`rm_legacy ⇒ trans_mode=rm ∧ !jetty_pool/enable`；`rm_pool` 不再要求 `probe/enabled`（M-3）。

## 22. 代码审查修复（第二轮）

> 针对 876a3066 + a79bdd88 的审查意见逐条修复；本节记录结论与仍未闭合的部分。

### 22.1 阻塞项

| 编号 | 问题 | 修复 |
| --- | --- | --- |
| **B1** | 在 `beginAttempt` **之前**被 defer 的 slice 仍是 `kQueued`；reflow 用 `markQueued()` 重排会失败并把 slice **取消**（quota/pool 预筛/健康/outstanding 四个常见压力出口 + `deferPending` 的快照前进分支） | 新增 `UbSlice::requeueAfterDefer()`：`kQueued` 保持并刷新等待时间；`kRetryPending → kQueued`；被 attempt 持有/终态/取消 → false。`reflowDeferred()` 与 `deferPending()` 的重排分支改用它。回归测试 `DeferredSliceBeforeAttemptIsReflowedNotCanceled`（池预筛压力 → defer → 唤醒 → 两 Task 都完成，`reflow_cancelled==0`）与单测 `RequeueAfterDeferReAdmitsOnlyUnclaimedSlices` |
| **B2** | `rm_pool` 下业务 JFC 的 JFR 仍是 RC，而池成员 JFS 是 RM；UMDK 要求二者同模式 → 每次 `createJetty` 被拒 → `tryLease` 永远 miss | `ub_transport.cpp` 在建 Context 前按 `runtime_profile==kRmPool` 设 `jfc_options.transport_mode=kRm`（与 930 对齐）。集成测试 fixture 也按同一规则建 JFC，新增 `RmModePostsThroughThePooledMember` 覆盖生产 RM 分支 |

### 22.2 Major

| 编号 | 结论 |
| --- | --- |
| M1 reflow 饥饿 | `DeferRegistry` 增加轮转游标 `next_bucket_`：批次被截断时从下一个桶继续，扫描完成也前移一格；单测 `DeferRegistryRotatesSoHotBucketsCannotStarveOthers`（1000 个 K 项 + 1 个 path 项，8 轮内必然被取到） |
| M2 关闭 fence 失败仍删成员 | 池 fence 失败改为**返回错误**（同 endpoint fence 的 W2 契约）：记录与 lease 全部保留，`onUnposted` 兜底不执行，uninstall 可重试 |
| M3 生产 RM 分支零覆盖 | fake adapter 补齐注入点：`setNextPostOutcome(status, posted_count)`、RM post 交付 completion（`setRmPostDeliversCompletion`）、`completePending()`（迟到 CQE / 释放 held）、`lastQuiescedJettyId()`、`failJettyDeletes(n)`/`failNextCreateJetty()`、`postJettyIds()` 调用序列；fixture 参数化 RC/RM、K、queue budget |
| M4 健康查询成本 | PLM 增 `path_index_`（`ProbePathKey → PathVersion`），在 `initialize`/`upsertEdgeLocked`/`removePathLocked` 维护；`resolveVersionLocked` 先走索引，扫描保留为兜底。每次候选查询由 O(paths) 降为 O(1) |
| M5 池预算与 desired 无校验 | `installPooledSendPools` 按“设备上 Context 数 × desired_per_context”校验，不足即 `InvalidArgument`；预算不足以为 endpoint Jetty 留位时告警（M10 的可见化） |
| M6 rmTarget 维度/空 target | 改为 `worker_index % jettyCount()`（JFC 轮转序号与 peer Jetty 拓扑无关）；`target.eid`/`id` 为空时直接 `InternalError`，不发 RM WR |
| M7 credit 溢出 | 恢复 930 的两段式比较（`credits > capacity || outstanding > capacity - credits`）；另增 `rejectedReleases(jfc)` 让重复释放可断言 |
| M8 `UbContext::shutdown()` 不 drain 池 | `shutdownLocked()` 在删第一个 JFC 前先 `beginStop/drainIdle/drainQuarantined`（幂等）；teardown 目标补链接池模块 |
| M9 RM 能力/对端校验 | install 校验设备 `transport_modes` 含 RM；bootstrap 增加 `transport_mode:rm|rc` 能力串，RM endpoint 在 `bind()` 拒绝未声明 RM 的对端（fail-closed） |
| M10 endpoint Jetty 不进预算 | 本轮只做 M5 的 fail-fast 与告警；把 endpoint/探测 Jetty 登记进 `NativeBudget`（RK5 的具体化）仍归 W6，理由：需要给 EndpointStore 与探测供给接预算角色，属关闭/预算重构 |

### 22.3 观测与小项

- `defer_reflow_repark` 不再是死指标：`PendingSlice.deferrals` 记录 park 次数，回流后再次 park 才计数。
- `businessStats()` 有了生产出口：poller 每 5s（CAS 去重）在有负载时打一行 `UB business: queued/parked/defer/wakeup/lease/claim/retire/...`。
- 删除假测试 `ScoreCarriesNoPauseState`；`ub_core_test` 的 rail pause 行为仍由 RailMonitor 自身用例覆盖。
- ledger 以完整 `UbPostPath` 为键（不再用 64 位哈希键，避免碰撞合并），行归零后删除以限制增长；`path_load_imbalance` 饱和减法。
- `classifyPostAcceptance(ok, 0, 0)` 恢复为 Accepted；RM 默认实现遇到 `NotImplemented` 归一为 `Unknown`（不回滚、交 fence），避免“回滚+重试”掩盖配置错误。
- `UbRuntimeMode` 重复枚举删除；`PoolMember::error_epoch`（原属已删的 AttemptGate）删除；`lane()` 对无效 lease 返回静态空值；`incarnation` 起始值注释说明“进程内唯一即可”（PoolKey 不出进程）。
- `enqueueRetry` 明确不占 `business_queue_budget`（受各 slice 的 retry 预算约束，占预算会把“队列满”变成传输失败）。
- 定时 defer 的复评下限由 1ms 放宽到 10ms（`kTimedDeferNs`），避免无谓 churn；epoch bump 仍是快路径。
- 超时/停止交错用例的 `slice_timeout_ms` 从 20ms 提到 300ms，避免负载或 TSan 下 fence 先于 `beginStop()` 触发。

### 22.4 仍未闭合

- **认领复核失败的回滚仍无端到端用例**：`claimPost()` 的失败窗口（选路后、post 前）由 PLM 隔离或池停止触发，没有确定性测试钩子；目前只覆盖其两个输入（epoch 变化、stopping）与回滚代码路径的静态审查。若要可测，需要池侧 R-1 的 `tryClaimPost` 或一个仅测试可见的注入点。
- **M10**：endpoint Jetty 与探测 Jetty 尚未登记进 `NativeBudget`（RK5/W6）。
- **实机门槛**（RK11）未做：`TENT_HAS_REAL_URMA=OFF`，RM 的 JFR/JFS 同模式、credit 与三态仍只有 fake 证据。
- **上游探测提交引入的两个用例失败与本轮无关**：在 03772891（含 774d1ac2/35fea91d）上，不应用本轮任何改动也能复现 `tent_ub_probe_campaign_test::PostRejectionRollsEntryBackToQueued` 与 `tent_ub_probe_scheduler_test` 的两个用例失败（探测侧“拒绝的 campaign 不再空转”等语义变更未同步用例）。已核实与本轮 W3 改动无因果关系，归属探测侧。

## 23. 第三轮复审修复

> 针对 8a9e748e 的复审。上一轮 §22 中 M6 与 `lane()` 两行属于「文档先行、代码未落地」，
> 本轮补齐；另修 B3（RM 能力串被覆盖）与 B4（PathLoadLedger 行删除竞态）。

### 23.1 B3：bootstrap 能力串被覆盖

`makeBootstrapDesc()` 先在函数中段 `push_back` 了 `transport_mode:rm|rc`，又在末尾用
`output.capabilities = {"read", "write", "endpoint_generation"}` 整表覆盖，于是线上描述符
永远不带模式；而 `bind()` 的 RM 分支要求对端声明 RM → 双方互拒、endpoint 永不 ready、
rm_pool 整体不可用。

修复：能力列表只赋值一次，模式并入同一处（追加到初始化列表之后）。
新增用例 `BootstrapAdvertisesTransportModeAndBindRoundTrips`：
`makeBootstrapDesc()` 的输出必须含 `transport_mode:rm` 且保留 `read` 等基线能力，
并且**用该描述符自身走一遍 `bind()`** 必须 ready（这正是被破坏的闭环）；RC 对端仍被 RM endpoint 拒绝。
（上一轮没抓到是因为 fixture 手写 peer 描述并直接塞能力串，从不调用 `makeBootstrapDesc`。）

### 23.2 B4：PathLoadLedger 行删除与并发 publish 竞态

`onPublish()` 在锁外做 `fetch_add`：publisher 拿到 slot 指针但尚未 +1 时，finalizer 在锁内
复核仍见 0 → 删行 → 该 +1 落到孤儿 slot。压力复现约 1.7% 的 finalize 变成 unmatched，
`path_load_imbalance` 永久污染（W3-IN-7 不成立；只影响评分/观测，不误释放资源）。

修复：publish/finalize 的计数更新全部放进 `mutex_` 临界区（每次 attempt 两次，代价可忽略），
行删除也在同一把锁内，因此不存在「先取指针后计数」的窗口。
新增并发用例 `LedgerStaysBalancedUnderConcurrentPublishFinalize`（8 线程 × 2 万次同 path）：
断言 `published == finalized`、`unmatched == 0`、行归零、`pathCount == 0`。
该用例在修复前 5/5 失败，修复后 10/10 通过（已验证其有牙齿）。

### 23.3 §22 中「已修」但未落地的两项（本轮补上）

| 项 | 本轮实际改动 |
| --- | --- |
| M6 rmTarget 维度与空 target | `processPendingPooled` 不再 `(void)worker_index`；RM target 槽位改为 `worker_index % jettyCount()`，并在 post 前校验 `target.eid`/`target.id`，为空直接 `InternalError`。用例 `RmTargetSlotFollowsThePostingLane`：两个 peer Jetty + 两个成员（JFC 轮转序号不同），两条 WR 的 target 都必须是 slot 0（旧公式会得到 slot 1） |
| `lane()` 无效 lease | 改为返回静态空 `PoolLaneKey`，不再解引用空成员 |

### 23.4 其他

- 观测行标签 `lease{ok=` 改为 `lease{try=`（该计数含 miss/stopped/hard，原标签误导）。
- `classifyPostAcceptance` 的 0/0 分支补注释：`!ok` + 空批次同样落 Accepted（调用方不承担任何 WR 责任）。
- `enqueueRetry` 不占提交预算目前只有注释说明：要断言「队列满不会把重试打成失败」需要一个
  「重试入队瞬间 queued_ 已满」的确定性构造，现有 fixture 无法稳定制造该时序，暂列为已知未测项（与认领复核回滚同理）。
- 探测侧 `tent_ub_probe_campaign_test` / `tent_ub_probe_scheduler_test` 的 3 个断言在 03772891
  上（本轮改动前）即失败，归属探测侧提交语义变更，需由探测侧修复后分支才能全绿；本轮只做定位与说明。
