---
orphan: true
---

# Jetty ACK Timeout（status=9）单 Jetty 重建方案

状态：实现中（分支 `feat/jetty-ack-timeout-single-rebuild`）— **权威方案**（取代已废弃的 `jetty-ack-timeout-rebuild.md`）
范围：kunpeng / UB 传输路径（`UrmaEndpoint` / `UbWorkerPool`）；不涉及 tent
关联错误码：

| CR status | 枚举 | 本方案处理 |
|---|---|---|
| 4 | `URMA_CR_LOC_ACCESS_ERR` | 不重建；保持现有 slice 重试 / 失败逻辑 |
| 9 | `URMA_CR_ACK_TIMEOUT_ERR` | 单 Jetty 排空 → 删除 → 重建，**纯本地操作** |

---

## 1. 背景与问题

8 节点场景下，Jetty completion 出现 status=9（ACK 超时 / 重传超限）后，现有逻辑：

1. poll 到非 SUCCESS → slice 进入重试
2. 重试耗尽 → `deleteEndpointByPtr` → `UrmaEndpoint::deconstruct`
3. `deconstruct` 直接 `unbind / unimport / delete_jetty`，**没有** ERROR 排空与 `FLUSH_ERR_DONE` 栅栏

结果：短暂路径问题被放大成整 endpoint（甚至整节点）不可用，常需重启才能恢复。

---

## 2. 核心思路

**只重建出问题的那个 jetty，不删整个 EP，且不需要对端同步协议。**

```
status=9 出现在 jetty_list_[i]
  ① urma_modify_jetty(jetty_list_[i], ERROR)   ← 排空
  ② poll 到 FLUSH_ERR_DONE(local_id)            ← 排空完成栅栏
  ③ flush → unbind/unimport → delete → create
     → import(原 peer id) → bind → 更新槽位     ← 安全替换（仍纯本地）
```

**为什么不需要对端同步？**

kunpeng 数据面是单边 `URMA_OPC_READ/WRITE`，DMA 目标是对端 **segment**
（`slice->ub.r_seg`），不是往对端 jetty 做 SEND/RECV。

`urma_post_jetty_send_wr` 的第一个参数是**本地 jetty**。握手虽会
`import` + `bind` 并对 `wr.tjetty` 赋值，但：

- 现有路径在 `tjetty` 缺失时只 `LOG(ERROR)`，仍继续 post
- `remote_jetty == NULL` 为空分支，未当硬失败
- 重建后本端用**仍有效的对端 jetty id** 做 re-import/rebind 即可恢复本端发出方向；无需通知对端改其 import 视图

反向流量若仍绑旧本端 id，对端可能自行报错并走自己的本地重建——两侧各自恢复，不上同步握手。

---

## 3. 状态机

每个 jetty 槽位有独立状态：

```
ACTIVE ──(status=9，无其它槽在重建)──► DRAINING ──(FLUSH_ERR_DONE)──► REBUILDING
  │                                                                    │
  │                 ◄── create + 本端 rebind + 更新槽位 ───────────────┘
  │
  └──(status=9，已有槽在重建)──► PENDING_DRAIN ──(前槽重建完成后触发)──► DRAINING

                  任一环节失败 / 超时 ──► 回退：deleteEndpointByPtr
```

- `ACTIVE`：正常收发
- `DRAINING`：已 `modify(ERROR)`，禁止 post，等待 `FLUSH_ERR_DONE`
- `REBUILDING`：排空完成，正在 flush / 删建 / rebind
- `PENDING_DRAIN`：本槽也收到 status=9，但同 EP 已有 jetty 在重建；禁止 post
  （选槽跳过，避免故障槽继续吃流量），等前一个重建完成后由其尾部串行触发
  `modify(ERROR)` 进入 DRAINING
- 同 EP 任意时刻最多一个 jetty 处于 DRAINING/REBUILDING（串行）

---

## 4. 关键设计

### 4.1 per-jetty 状态

```cpp
enum JettyState { ACTIVE, DRAINING, REBUILDING, PENDING_DRAIN };
std::vector<JettyState> jetty_state_;             // 与 jetty_list_ 平行
std::unordered_map<uint32_t, int> jetty_id_map_;  // jetty_id → slot index
std::vector<uint32_t> peer_jetty_id_;             // 每槽对端 id，delete 前保留
std::vector<uint64_t> jetty_epoch_;               // 每槽重建代次，丢弃旧代次的迟到 CR
```

`peer_jetty_id_` 在握手 `doSetupConnection` 时写入；段 C 在 delete 本地 jetty
前依赖它做 re-import，不得只依赖即将销毁的 `imported_jetty_map_` 指针键。

### 4.2 选槽策略

`submitPostSend` 选槽时跳过非 ACTIVE 槽：

```
随机选一个槽 → 非 ACTIVE 则重试 → 全部不可用则返回 0（上层重试）
```

### 4.3 锁策略

| 阶段 | 是否持 `lock_` | 说明 |
|---|---|---|
| 段 A：modify(ERROR) | 是 | 与 post 互斥（UMDK 约束） |
| 段 B：等 FLUSH_ERR_DONE | 否 | 由 performPoll 顺带消费，不占锁 |
| 段 C：flush/删建/rebind | 是 | 替换 `jetty_list_` 与 `imported_jetty_map_` |

### 4.4 FLUSH_ERR_DONE 消费

走现有 `performPoll` → `UrmaContext::poll`。**必须在按 `user_ctx` 解 slice
之前分流**（假 CQE 的 `user_ctx` 无效；现有空 ctx `continue` 会丢掉栅栏）：

```
if cr.status == FLUSH_ERR_DONE:
    local_id → jetty_id_map_ → (endpoint, slot)
    endpoint->onFlushDone(slot)  // DRAINING → REBUILDING，触发段 C
    continue  // 假 CQE，不走 slice 路径，不改 outstanding slice 语义外的 depth 时需单独约定
```

### 4.5 status=9 分流与触发时机

**首次** poll 到 `ACK_TIMEOUT_ERR (9)` 即触发排空（不等重试耗尽）；`onJettyError`
必须幂等（已在 DRAINING/REBUILDING/PENDING_DRAIN 则不再 `modify`）。
若同 EP 已有 jetty 在重建，则把本槽标记为 `PENDING_DRAIN` 排队：选槽立即跳过
它，等前一个重建完成后由其尾部调用 `startDrainUnlocked` 串行启动排空。

```
if cr.status == ACK_TIMEOUT_ERR (9):
    slot ← slice->ub.jetty_depth 反查，或 cr.local_id → jetty_id_map_
    slice->ub.endpoint → UrmaEndpoint
    endpoint->onJettyError(slot)  // 段 A
    slice 计入 retry（换 ACTIVE 槽重发）
```

### 4.6 outstanding 记账

排空期间 inflight WR 会以 error/flush 类 CQE 回来。要求：

- 带有效 `user_ctx` 的失败 CQE：仍走现有 `jetty_depth_set` / retry 路径扣
  `wr_depth_list_` 与 JFC outstanding
- `FLUSH_ERR_DONE`：不当作 slice；不得 `markSuccess` / 不得当失败 slice 入队
- **每个 WR 恰好完成一次**：`modify(ERROR)` 后 inflight WR 要么以
  `WR_FLUSH_ERR` 经 JFC poll 回来，要么被段 C 的 `urma_flush_jetty` 回收；两者
  统一经 `processWrCompletion` 记账（`jetty_depth_set` 延迟扣减 + poll 返回值
  累计 JFC outstanding）。因此段 C 删除旧 jetty 后**不做**额外的
  `wr_depth_list_[slot]` / JFC 清零——延迟扣减要等 poll 返回后才 apply，在段 C
  里提前清零会双扣（实现后已删除原方案中的"归零兜底"）
- 旧 jetty 删除后 `++jetty_epoch_[slot]`；slice 在 post 时记录
  `slice->ub.jetty_epoch`，旧代次的迟到/重复 CR 在 `processWrCompletion` 里按
  epoch 不匹配直接丢弃，不参与记账、不再入 retry 队列

### 4.7 段 C 完整顺序（持锁）

```
1. urma_flush_jetty（回收残余 WR，逐条经 processWrCompletion 交付；
   注意与 poll 可能重复，见 §7）
2. unbind → unimport（旧本地 jetty 上的对端视图）
3. delete_jetty；从 jetty_id_map_ 删旧 id；++jetty_epoch_[slot]
4. create_jetty（同 JFC/JFR 配置）
5. import(peer_jetty_id_[slot]) → bind(新 jetty, imported)
6. 更新 jetty_list_[slot]、imported_jetty_map_、jetty_id_map_
7. jetty_state_[slot] = ACTIVE
8. 扫描 PENDING_DRAIN 槽：有则立即 startDrainUnlocked（modify(ERROR)），
   仍保持同 EP 串行
```

任一步失败 → 回退 `deleteEndpointByPtr`。

---

## 5. 执行计划

### Step 1：加状态与映射

- `UrmaEndpoint` 增加 `jetty_state_`、`jetty_id_map_`、`peer_jetty_id_`
- `construct()` / 握手初始化，`deconstruct()` 清理
- `submitPostSend` 选槽跳过非 ACTIVE

### Step 2：poll 分流

- `UrmaContext::poll`：先识别 `FLUSH_ERR_DONE`（`local_id`），再处理
  `ACK_TIMEOUT_ERR`（endpoint + 槽位）
- 假 CQE 不走 slice 路径

### Step 3：段 A — 触发排空

- `onJettyError(slot)`：持锁 → 幂等检查 → `modify(ERROR)` → 标 DRAINING
- 记录 drain 起始时间，供超时用

### Step 4：段 B — 等待栅栏

- `onFlushDone(slot)`：标 REBUILDING，触发段 C

### Step 5：段 C — 重建

- 按 §4.7 完整顺序执行

### Step 6：超时降级

- flush-done 等待超时（可配置，默认 3s）→ 回退 `deleteEndpointByPtr`
- create / import / bind 失败同样降级

### Step 7：日志与测试

- 关键路径日志：`jetty_id`、slot、耗时、降级原因
- 单测：状态机、选槽跳过、假 CQE 路由、幂等 `onJettyError`
- 集成 / 故障注入：status=9 后该槽恢复 ACTIVE，同 EP 其它槽可继续；超时路径删 EP
- 无 UMDK 硬件时，flush 与真实 ACK timeout 行为标为硬件验证项
- **Mock 注入 CI 测试草案**：`jetty-rebuild-mock-test-plan.md`（`mock_urma` 脚本化
  status=9 / FLUSH_ERR_DONE / flush CR，挂 `tent-ci (ub-mock)`）

---

## 6. 改动文件

| 文件 | 改动 |
|---|---|
| `urma_endpoint.h` | `JettyState`（含 `PENDING_DRAIN`）、`jetty_state_`、`jetty_id_map_`、`peer_jetty_id_`、`jetty_epoch_`、`onJettyError()` / `onFlushDone()` / `startDrainUnlocked()` |
| `urma_endpoint.cpp` | 状态机、选槽跳过、段 A/C、握手写入 `peer_jetty_id_` |
| `ub_context.cpp` | poll / worker 侧配合 status=9 与 `FLUSH_ERR_DONE`（若分流落在 context poll 则改 `urma_endpoint.cpp` 中 `UrmaContext::poll`） |

---

## 7. 风险与开放问题

1. **urma_flush_jetty 与 poll 重复**：flush 返回的 WR 级 CR 是否已在 JFC poll
   中出现过，需实测确认，避免 double-complete。实现按「每个 WR 恰好完成一次」
   记账，并用 `jetty_epoch_` 把旧代次的迟到 CR 整体丢弃兜底；若实测发现 flush
   与 poll 会重复交付同一 WR，需重新评估 slice 指针解引用的安全性（届时 CR 里
   的 `user_ctx` 可能指向已回收的 slice）。
2. **共享 JFC 假 CQE 过滤**：多 jetty 共享 JFC 时，严格按 `local_id` 匹配，不能假设顺序。
3. **超时阈值**：flush-done 等待 3s 是否合适，需结合 `err_timeout` 和现场标定。
4. **硬件 hang 场景**：本方案解决软件放大故障；若根因是设备/驱动 hang，重建仍可能失败，保留删 EP 降级路径。
5. **反向路径**：对端仍绑旧本端 id 时可能自行报 9 并本地重建；观察即可，本期不上对端协议。
