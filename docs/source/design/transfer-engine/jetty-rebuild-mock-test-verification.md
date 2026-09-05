---
orphan: true
---

# Jetty ACK Timeout 重建 — Mock 注入 CI 验证说明（Review 用）

状态：已实现并在 CI 验证通过（`tent-ci (ub-mock)` 绿）
范围：kunpeng / UB（`UrmaEndpoint` + `mock_urma.cpp` + `UbWorkerPool`）
关联文档：

- 生产方案：`jetty-single-rebuild-plan.md`
- 测试计划：`jetty-rebuild-mock-test-plan.md`

目标读者：review 这批改动的同事。本文说明**整套 CI 验证怎么设计、error9 怎么被触发、
以及为什么 CI 绿了就代表生产修改是对的**。

---

## 1. 要验证的生产改动

本次在 `feat/jetty-ack-timeout-single-rebuild` 分支上，除了最早的单 Jetty 重建
（status=9 → drain → flush → rebuild），还补了两个安全性修复：

| 编号 | 问题 | 修复 |
|------|------|------|
| #2 | drain timeout 后残留 WR 悬挂（slice 不标失败、计数不归零） | `checkDrainTimeout` 超时后 flush 并逐个交付残留 WR |
| #3 | rebuild 中 `urma_delete_jetty` 失败会丢 jetty handle（泄漏） | 保留旧 handle + 新增 `REBUILDING_FAILED` 态，`deconstruct` 可重试 delete |

**难点**：这些路径只在真机出现 ACK timeout（status=9）时才会走到，CI 里没有 URMA
硬件，`mock_urma.cpp` 原本恒返回成功，**根本覆盖不到**。本文介绍如何用 mock 注入把
这些路径在 CI 里确定性地点亮。

---

## 2. 设计思路：注入点只在 mock 层

核心原则一句话：**生产代码一行不改、不加任何测试开关，把"硬件会返回什么"做成可
编程脚本。**

依据是：`urma_poll_jfc` 是一个 C 接口，`mock_urma.cpp` 就是它的一种实现。CI 用
`-DUSE_UB=ON` 编译时链接的是 mock 而不是真驱动。因此只要让 mock 的 `urma_poll_jfc`
把某条 completion 的 `status` 填成 `URMA_CR_ACK_TIMEOUT_ERR`(9)，生产代码
`UrmaContext::poll → processWrCompletion` 就会**真实地**走进 ACK-timeout 分支。

**为什么不在生产代码里加开关**（比如读 `MOONCAKE_URMA_FAULT=9` 环境变量）：

- 测试开关容易泄漏到真实运行路径，污染环境；
- 环境变量是全局的、跨用例残留，CI 并行时不确定；
- 我们要验证的是"生产代码在收到 status=9 时的真实行为"，如果生产代码本身带
  `if (test_mode)` 分支，那跑的就不是真实路径了。

所以注入全部收敛在 mock 实现内部，通过一组 **scripted C API**（
`mock_urma_test_ctrl.h`）由测试用例显式触发，每个用例 `SetUp`/`TearDown` 调
`mock_urma_test_reset()` 复位，保证用例间不串。

---

## 3. 整体分层

```
┌─────────────────────────────────────────────┐
│  CI job: tent-ci (ub-mock)   (ubuntu-22.04) │
│  cmake: -DUSE_UB=ON -DCMAKE_BUILD_TYPE=Debug│
└──────────────────┬──────────────────────────┘
                   │ ctest -R urma_jetty_rebuild_test
                   ▼
┌─────────────────────────────────────────────┐
│  urma_jetty_rebuild_test (gtest, 单进程)     │
│  手工 seed UrmaContext + 1 JFC + 1 jetty     │
│  测试线程同步驱动 UrmaContext::poll()        │
└──────────────────┬──────────────────────────┘
                   │ 调用真实生产代码
                   ▼
┌─────────────────────────────────────────────┐
│  生产代码（不改动、不含测试分支）             │
│  poll → processWrCompletion → onJettyError   │
│  → onFlushDone → rebuildJettyUnlocked        │
│  → checkDrainTimeout                         │
└──────────────────┬──────────────────────────┘
                   │ 调用 URMA C API
                   ▼
┌─────────────────────────────────────────────┐
│  mock_urma.cpp（URMA provider 的 mock 实现） │
│  urma_poll_jfc / urma_flush_jetty / ...      │
│  + 脚本钩子 mock_urma_test_ctrl.h            │
└─────────────────────────────────────────────┘
```

测试夹具刻意**绕开 `UrmaContext::construct()`**（它会拉起 worker poll 线程），改为手工
seed 一个最小 context（1 个 JFC + 1 个 jetty），由测试线程**同步**驱动
`UrmaContext::poll()`。这样每一步都可控、可断言，无并发干扰。

---

## 4. error9 是怎么被触发的

### 4.1 注入点

生产 `UrmaContext::poll` 只认 `urma_cr_t.status`。触发 error9 = 让 mock 的
`urma_poll_jfc` 把某条 completion 的 `status` 填成 9。

### 4.2 前提：mock 必须知道每个 WR 属于哪个 jetty

原 mock 只把 `user_ctx` 存进队列，poll 时只回填 `status` + `user_ctx`。但生产代码靠
`cr.local_id` 定位 jetty（`findJettyOwner(cr.local_id, ...)`，FLUSH_ERR_DONE 和
ACK_TIMEOUT 都依赖它）。所以 mock 的队列从 `deque<uint64_t>` 扩成：

```cpp
struct PendingWr {
    uint64_t user_ctx;
    uint32_t jetty_local_id;  // post 时从 jetty->jetty_id.id 记下
    bool withhold;            // true 时 poll 跳过、只有 flush 能收
};
```

`urma_post_jetty_send_wr` 入队时记录 jetty id，`urma_poll_jfc` 回填 `local_id`。
没有这个，注入的 error9 找不到 slot，路径走不对。

### 4.3 脚本钩子

| 钩子 | 作用 | 服务的用例 |
|------|------|-----------|
| `mock_urma_set_next_poll_status(9, n)` | 接下来 n 条 poll 出的 completion 用 status=9 | 触发 ACK timeout |
| `mock_urma_enqueue_flush_done(local_id)` | 下次 poll 注入 `FLUSH_ERR_DONE` 栅栏（user_ctx=0） | 触发 onFlushDone → rebuild |
| `mock_urma_set_flush_returns_errors(k)` | rebuild 内 `urma_flush_jetty` 返回 k 条 `WR_FLUSH_ERR` | 验证 flush 交付 |
| `mock_urma_withhold_next_post(n)` | 接下来 n 个 post 的 WR 滞留：poll 跳过、仅 flush 能收 | 构造残留 WR |
| `mock_urma_fail_next_delete_jetty()` | 下次 `urma_delete_jetty` 返回错误且不释放 | 测 delete 失败保留 handle |
| `mock_urma_fail_next_create_jetty()` | 下次 `urma_create_jetty` 返回 NULL | 测 create 失败回退 |
| `mock_urma_test_reset()` | 清空全部脚本与队列 | 每个用例 SetUp/TearDown |

所有钩子默认 inert，不影响现有 `ub_transport_test`。

### 4.4 TC-1 完整时序（error9 → rebuild）

```
测试线程                        mock_urma                    生产代码
   │ post 1 slice ──► pending入队{slice_ptr, jetty_id=1}
   │ set_next_poll_status(9, 1)
   │ pollOnce() ──────► poll_jfc 弹出该WR,status=9 ──► processWrCompletion:
   │                                                   status==9 → onJettyError
   │                                                   → modify_jetty(ERROR)
   │                                                   → jetty_state=DRAINING
   │ enqueue_flush_done(1)
   │ pollOnce() ──────► poll_jfc 先吐栅栏 ──► poll 循环 → onFlushDone
   │                       FLUSH_ERR_DONE(1)              → rebuildJettyUnlocked:
   │                                                      flush→unbind→unimport
   │                                                      →delete→create→import→bind
   │                                                      → ACTIVE, epoch+1
   │ 断言: state==ACTIVE, epoch+1, 新旧jetty id不同, endpoint未删
```

注意整条链路走的是**真实生产函数**，测试没有直接调用 `onJettyError`/`onFlushDone`
——这就是"覆盖真实 poll 路径"的含义。

---

## 5. 测试用例与各自验证什么

| 用例 | 验证点 | 通过判据 |
|------|--------|----------|
| **TC-1** `AckTimeoutTriggersRebuildHappyPath` | error9 → DRAINING → fence → rebuild → ACTIVE | 状态回 ACTIVE、epoch+1、新旧 jetty id 不同、endpoint 未误删 |
| **TC-2** `FlushCompletionsDeliveredOnRebuild` | rebuild 内 flush 把残留 WR 交付出来 | 残留 slice 出现在 failed_slices、resolved 计数正确 |
| **TC-3** `StaleEpochCompletionDropped` | 旧代 CQE 被丢弃 | epoch 已推进、不重复完成 |
| **TC-4** `RebuildFailureKeepsJettyHandle` | （对应 #3）delete 失败不丢 handle | 状态 `REBUILDING_FAILED`、旧 handle 保留、endpoint deferred delete |
| **TC-5** `DrainTimeoutDeliversResidualWriters` | （对应 #2）drain timeout 收敛残留 WR | 残留 slice 交付进 failed_slices、depth 归零、deferred delete |

TC-4/TC-5 直接验证两个生产修复：TC-4 证明 delete 失败分支**保留了 handle**（修前
置空导致泄漏）；TC-5 证明 `checkDrainTimeout` 现在**会 flush 并逐个交付残留 WR**
（修前直接 defer delete、slice 悬挂）。

### 5.1 编排上的两个关键技巧

- **残留 WR 的构造（TC-2/TC-5）**：夹具是单 JFC、单 jetty 单槽，所有 WR 进同一队列。
  若只 post 两个 WR 再让第一个 error9，第一次 poll 会把**两个都弹出**（第二个按默认
  SUCCESS 完成），flush 阶段就没有残留 WR 了。为此加了 `mock_urma_withhold_next_post`
  ——被标记的 WR 在 poll 时被跳过、保持 outstanding，只有 `urma_flush_jetty` 能收，
  这更接近真实硬件"jetty 进 ERROR 后未完成的 WR 靠 flush 回收"的语义。
- **drain timeout 不等 3 秒（TC-5）**：`kJettyDrainTimeoutNs` 是 3 秒，测试不能真等。
  TestPeer 直接把 `drain_start_ns_` 改成一个很早的时间，下一次 `checkDrainTimeout`
  立即判超时。

### 5.2 一个被修正的测试编排错误

TC-4 最初用 `fail_next_create_jetty` 触发 rebuild 失败，但那是在 `recreateJettyUnlocked`
里 **create** 失败——此时旧 jetty **早已成功 delete**，handle 本来就没了、状态停在
REBUILDING 是**对的**，与"delete 失败保留 handle"是两条不同路径。后改为新增
`fail_next_delete_jetty` 钩子，让 TC-4 真正测到 #3 改的 delete 失败分支。

---

## 6. 为什么 CI 绿了就代表修改是对的

分三层信心：

1. **状态机转移正确**（TC-1/2/3）：rebuild 后 jetty 状态、epoch、jetty id、endpoint
   存活性都符合预期，且全程走真实 `poll` 路径（含锁、defer delete）。
2. **失败收敛正确**（TC-4/5）：直接打在两个生产修复点上，断言 handle 保留、WR 收敛、
   计数归零。
3. **不破坏现有行为**（隐性）：mock 钩子默认 inert；`REBUILDING_FAILED` 是非 ACTIVE
   态，不会被选槽、被 `hasNonActiveJettyUnlocked` 正确识别；计数器不重复扣减
   （`jetty_depth_set` 管 `wr_depth_list_`、`jfc_outstanding_` 单独扣，TC-2/TC-5 的
   计数断言证明了这点）；Debug 构建 + 真实锁路径覆盖了 defer delete（不在 poll 内
   deconstruct）的安全性。

### 计数器不重复扣减的说明

`processWrCompletion` 把每条 completion 累进 `jetty_depth_set`（worker 据此扣
`wr_depth_list_[slot]`，每 jetty 深度）；而 `jfc_outstanding_` 是 JFC 全局计数，由
worker 用 `poll` 返回值（`wr_completions`）扣。两者是**不同计数器、各自扣减、不重叠**。
`checkDrainTimeout` 在 `poll` 内部被调、不经过 `poll` 返回值，所以它手动补扣
`jfc_outstanding_`，而 `wr_depth_list_` 仍由 worker 经 `jetty_depth_set` 扣。

---

## 7. 验证边界（重要，review 时请注意）

这套测试用**单 JFC、单 jetty 单槽**最小夹具，证明的是**逻辑正确性**：

| 层次 | mock gtest (CI) | 真机 |
|------|----------------|------|
| 状态机转移逻辑 | ✅ 本文覆盖 | ✅ |
| flush/fence 真实时序 | 近似（脚本模拟） | ✅ 才能验证 |
| 多 jetty 并发 / PENDING_DRAIN 串行 | ❌ 未覆盖 | ✅ |
| 性能 / 长期稳定性 | ❌ | ✅ |

**provider 的真实语义**（真硬件 flush 返回什么、ACK timeout 真实时序）只有真机能证，
文档中已把"真机 ACK timeout 时序"列为硬件验证项。

### 明确的 out-of-scope（本 PR 不做）

- **completion token registry**（生产 #1）：当前 `processWrCompletion` 先解引用裸
  `Slice*` 再查 epoch，slice 回收后存在风险。改成 token registry 是架构级改动，单独立项。
- **精确按 jetty 注入**：当前 `set_next_poll_status` 对"下 n 条 poll"生效，哪个 jetty
  命中取决于 post 顺序；按 `local_jetty_id` 定向注入更严谨，留待后续。
- **peer-id 复用前提**：rebuild 复用缓存的 `peer_jetty_id_`，隐含"对端 rebuild 后
  peer ID 仍有效"的假设。若该前提不成立，需增加 re-handshake / peer ID 同步——本轮
  明确列为非目标。

---

## 8. 如何在本地 / CI 复现

CI 的 `tent-ci (ub-mock)` job 已挂上 scoped 步骤（`.github/workflows/ci.yml`）：

```yaml
- name: Test (kunpeng URMA jetty rebuild)
  if: matrix.name == 'ub-mock'
  run: |
    cd build-tent
    ctest --test-dir mooncake-transfer-engine/tests \
      -R urma_jetty_rebuild_test --output-on-failure
```

本地（需 Linux；macOS 因 `numa.h` 无法编 `urma_endpoint.cpp`）：

```bash
cmake -G Ninja .. -DUSE_UB=ON -DCMAKE_BUILD_TYPE=Debug -DBUILD_UNIT_TESTS=ON
cmake --build . --target urma_jetty_rebuild_test
ctest -R urma_jetty_rebuild_test -V
```

---

## 9. 改动文件清单

| 文件 | 改动 |
|------|------|
| `src/.../urma/mock_urma.cpp` | `PendingWr` 扩展 + poll/flush/delete/create 支持脚本注入 |
| `src/.../urma/mock_urma_test_ctrl.h` | 新增，测试脚本钩子 API |
| `include/.../urma/urma_endpoint.h` | 两个 TestPeer friend + `REBUILDING_FAILED` 枚举 |
| `src/.../urma/urma_endpoint.cpp` | #2 `checkDrainTimeout` 收敛残留 WR；#3 rebuild delete 失败保留 handle；`disconnectUnlocked` 补注释 |
| `tests/urma_jetty_rebuild_test.cpp` | 新增，TC-1～TC-5 |
| `tests/CMakeLists.txt` | 注册 `urma_jetty_rebuild_test`（`if(USE_UB)`） |
| `.github/workflows/ci.yml` | `tent-ci` 增加 scoped ctest 步骤（仅 ub-mock 腿） |
