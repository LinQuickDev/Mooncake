---
orphan: true
---

# Jetty ACK Timeout 重建路径 — Mock 注入测试草案

状态：已实现（P0：TC-1～TC-3 + scoped ctest 挂 `tent-ci (ub-mock)`）
范围：kunpeng / UB（`UrmaEndpoint` + `mock_urma.cpp` + `UbWorkerPool`）
关联方案：`jetty-single-rebuild-plan.md`（见其"验证"章节的回链）
目标：在 **无真实 URMA 硬件** 的 CI 中，覆盖 status=9 触发的单 Jetty 重建与安全修复路径。

---

## 1. 动机

当前 `mock_urma.cpp` 的 `urma_poll_jfc` **恒返回 `URMA_CR_SUCCESS`**，`urma_flush_jetty` **恒返回 0**。
因此现有 CI（含 `tent-ci (ub-mock)`）**无法执行**以下生产逻辑：

| 路径 | 文件 | 现状 |
|------|------|------|
| ACK timeout → DRAINING | `onJettyError` | mock 不产出 status=9 |
| FLUSH_ERR_DONE → rebuild | `onFlushDone` | mock 不产出 fence CQE |
| flush CR 交付 | `rebuildJettyUnlocked` | mock flush 空返回 |
| stale epoch 丢弃 | `processWrCompletion` | 无法构造跨代 CQE |
| 延迟删除 | `deferred_deletes` | 难以稳定触发失败回退 |

本草案通过 **增强 mock + 新增 gtest**，在 `USE_UB=ON` 的 CI job 中补齐覆盖。

---

## 2. 设计原则

1. **注入点只在 mock 层** — 不在 `UrmaContext::poll` 生产代码里加“伪造 status”开关。
2. **确定性优先** — 测试用 scripted 状态机；环境变量仅作调试辅助。
3. **窄而深** — 先覆盖 happy path + 两个 failure 回退，不复制 `ub_transport_test` 全链路压测。
4. **与 TENT 分层** — 测的是 `kunpeng_transport/urma/urma_endpoint.cpp`，不是 `tent/urma_adapter.cpp`。

---

## 3. Mock 增强设计

### 3.1 数据结构扩展（`mock_urma.cpp`）

在 `JfcState` 中除 `pending_ctx` 外，为每次 post 记录元数据：

```cpp
struct PendingWr {
    uint64_t user_ctx;
    uint32_t jetty_local_id;  // jetty->jetty_id.id
};

struct JfcState {
    std::mutex mutex;
    std::deque<PendingWr> pending;
};
```

`urma_post_jetty_send_wr`：把 WR 链上每个 `user_ctx` 与 `jetty->jetty_id.id` 入队。

### 3.2 测试脚本 API（仅 mock / 测试可见）

新增头文件 `mooncake-transfer-engine/src/transport/kunpeng_transport/urma/mock_urma_test_ctrl.h`：

```cpp
#pragma once
#include "urma_api.h"

#ifdef __cplusplus
extern "C" {
#endif

// 重置 mock 全局状态（每个 TEST 的 SetUp 调用）
void mock_urma_test_reset(void);

// 为下一次从该 JFC poll 出的每个 WR 指定 completion status。
// 返回后脚本清除；若未设置则默认 URMA_CR_SUCCESS。
void mock_urma_set_next_poll_status(int status);

// 在 poll 完当前 pending 后，额外注入一条 FLUSH_ERR_DONE（无 user_ctx）。
void mock_urma_enqueue_flush_done(uint32_t jetty_local_id);

// rebuild 内 urma_flush_jetty：返回 count 条 URMA_CR_WR_FLUSH_ERR，user_ctx 来自
// 该 jetty 尚未完成的 pending（或专用 flush 队列）。
void mock_urma_set_flush_returns_errors(int count);

// 让 urma_create_jetty 下一次失败（测 rebuild 失败 → defer delete）。
void mock_urma_fail_next_create_jetty(int error);

#ifdef __cplusplus
}
#endif
```

实现放在 `mock_urma.cpp` 末尾，`#ifdef BUILD_MOCK_URMA_TEST_CTRL` 或与 `USE_UB` 同条件编译。

### 3.3 `urma_poll_jfc` 行为（scripted）

伪代码：

```cpp
int urma_poll_jfc(urma_jfc_t* jfc, int n, urma_cr_t* cr) {
    // 1) 若队列头有 injected FLUSH_ERR_DONE marker，先返回它（user_ctx=0）
    // 2) 否则从 pending 弹出最多 n 条 WR
    //    status = mock_next_poll_status 或 URMA_CR_SUCCESS
    //    user_ctx / local_id 从 PendingWr 填充
    // 3) 返回条数
}
```

### 3.4 `urma_flush_jetty` 行为

当 `mock_urma_set_flush_returns_errors(k)` 生效时：

- 从该 jetty 关联的 pending（或已 ERROR 未完成的 WR）弹出最多 `cr_cnt` 条；
- 每条 `status = URMA_CR_WR_FLUSH_ERR`，`user_ctx` 有效；
- 返回实际条数；脚本计数递减至 0 后恢复“返回 0”。

### 3.5 其它 mock 钩子

| API | 钩子用途 |
|-----|----------|
| `urma_create_jetty` | `fail_next_create_jetty` → rebuild 失败路径 |
| `urma_modify_jetty` | 保持成功；可选 `fail_next_modify_error` 测 modify(ERROR) 失败 |
| `urma_import_jetty` / `bind` | 保持成功；可选失败钩子测 bind 失败清理 |

---

## 4. 测试文件与夹具

### 4.1 新测试目标

| 项 | 值 |
|----|-----|
| 源文件 | `mooncake-transfer-engine/tests/urma_jetty_rebuild_test.cpp` |
| 可执行名 | `urma_jetty_rebuild_test` |
| 条件编译 | `if(USE_UB)` |
| 链接 | `transfer_engine`, `gtest`, `gtest_main` |
| ctest 名 | `urma_jetty_rebuild_test` |

**不要**复用 `ub_transport_test`（全链路、etcd、曾有不稳定注释）；新测试应 **in-proc 双端** 或 **单 context + 合成 slice**，参考 `rdma_endpoint_state_test.cpp` 的轻量夹具风格。

### 4.2 建议夹具结构

```cpp
class UrmaJettyRebuildTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mock_urma_test_reset();
    // TransferEngine(false) + protocol=ub + mock_urma_device
    // 起 1 个 target + 1 个 initiator segment（或内存 metadata mock）
    // 等待 worker poll 线程运行
  }
  void TearDown() override {
    mock_urma_test_reset();
    // engine shutdown
  }

  void PostOneSliceAndWait(...);
  uint64_t CurrentJettyEpoch(UrmaEndpoint* ep, int slot);
};
```

若 metadata/etcd 过重，**Phase 1** 可只做 **UrmaContext + UrmaEndpoint 单元级** 测试（直接 `submitPostSend` + 手动 `performPoll` 一轮），不拉完整 `TransferEngine`。

### 4.3 可选 TestPeer（与 RDMA 测试一致）

```cpp
class UrmaEndpointTestPeer {
 public:
  static JettyState state(UrmaEndpoint& ep, int slot);
  static uint64_t epoch(UrmaEndpoint& ep, int slot);
  static bool isDraining(UrmaEndpoint& ep);
};
```

优先通过 **可观测行为**（slice 状态、post 是否恢复、endpoint 是否仍存在）断言，减少对 private 成员的直接访问。

---

## 5. 测试用例（首批）

### TC-1 `AckTimeoutTriggersRebuildHappyPath`（P0）

**目的**：status=9 → DRAINING → FLUSH_ERR_DONE → rebuild → slice 完成。

| 步骤 | Mock / 动作 | 期望 |
|------|-------------|------|
| 1 | post 1 WR | `wr_depth` +1 |
| 2 | `set_next_poll_status(URMA_CR_ACK_TIMEOUT_ERR)`，poll 一轮 | `onJettyError`；jetty → DRAINING |
| 3 | `enqueue_flush_done(jetty_id)`，poll 一轮 | `onFlushDone` → rebuild |
| 4 | `set_flush_returns_errors(1)`（rebuild 内） | slice 进 failed 或 retry，**非**永久 POSTED |
| 5 | 后续 poll SUCCESS 或 retry 成功 | jetty ACTIVE；`jetty_epoch` +1；endpoint 未 delete |

### TC-2 `FlushCompletionsDeliveredOnRebuild`（P0）

**目的**：验证 #1 修复 — rebuild 内 flush CR 必须 `processWrCompletion`。

| 步骤 | 期望 |
|------|------|
| rebuild 前 post N 个 WR | |
| flush 返回 N 条 `URMA_CR_WR_FLUSH_ERR` | 每条 slice 离开 POSTED（failed 或 success 路径） |
| `jetty_depth_set` 记账与 `jfc_outstanding` 一致 | 无双重扣减 |

### TC-3 `StaleEpochCompletionDropped`（P0）

**目的**：验证 #4 — 旧代 CQE 不二次完成、不污染 depth。

| 步骤 | 期望 |
|------|------|
| 完成 rebuild（epoch++） | |
| mock 注入 **旧 epoch** 的 SUCCESS CQE | `processWrCompletion` 返回 false；slice 不再 markSuccess |
| 新 post 的 WR 正常 SUCCESS | |

### TC-4 `DeferredDeleteAfterRebuildFailure`（P1）

**目的**：验证 #2 — poll 内不同步 `deleteEndpointByPtr`。

| 步骤 | 期望 |
|------|------|
| `fail_next_create_jetty` + 触发 rebuild | rebuild 失败 |
| 同一 poll 轮内仍有未处理 CR | 不 UAF（可用 ASAN/Debug 构建） |
| poll 返回后 | endpoint 进入 deferred delete；`wr_depth_list_` 仍有效至记账完成 |

### TC-5 `ModifyErrorFailureDefersDelete`（P1）

| 步骤 | 期望 |
|------|------|
| `fail_next_modify_error` + status=9 | `modify(ERROR)` 失败 → defer delete，不在 poll 循环内 deconstruct |

### TC-6 `PollDoesNotDeleteBeforeDepthAccounting`（P1，回归）

**目的**：钉死原 UAF。

| 步骤 | 期望 |
|------|------|
| 同批 CR：先触发 rebuild 失败，后还有 WR completion | 后者仍能安全读 `jetty_depth` |
| `performPoll` 结束后 | 才 `deleteEndpointByPtr` |

### TC-7 `DisconnectFlushesNonActiveJetty`（P2）

| 步骤 | 期望 |
|------|------|
| jetty 处于 DRAINING | `disconnectUnlocked` 先 sync flush |
| RESET 失败 | `jetty_state` 不强制 ACTIVE |

### 不在首批范围

- `PENDING_DRAIN` 多槽串行（文档有、代码未落地）
- 真机 ACK timeout 时序
- 多 worker 并发压力
- `ub_transport_test` 级别跨节点 etcd 全链路

---

## 6. CI 集成

### 6.1 挂载点

在 **`.github/workflows/ci.yml`** 的 `tent-ci (ub-mock)` job（已 `-DUSE_UB=ON -DCMAKE_BUILD_TYPE=Debug`）增加：

```yaml
- name: Test (kunpeng URMA jetty rebuild)
  if: matrix.name == 'ub-mock'
  run: |
    cd build-tent
    ctest --test-dir mooncake-transfer-engine/tests \
      -R urma_jetty_rebuild_test \
      --output-on-failure
```

### 6.2 CMake 注册

```cmake
if(USE_UB)
  add_executable(urma_jetty_rebuild_test
                 ${WORKSPACE}/urma_jetty_rebuild_test.cpp)
  target_link_libraries(urma_jetty_rebuild_test PUBLIC transfer_engine
                        gtest gtest_main glog::glog pthread)
  target_compile_definitions(urma_jetty_rebuild_test
                             PRIVATE MOCK_URMA_TEST_CTRL=1)
  add_test(NAME urma_jetty_rebuild_test COMMAND urma_jetty_rebuild_test)
endif()
```

`mock_urma_test_ctrl` 接口建议始终编入 `mock_urma.cpp`（仅 UB 构建），测试目标通过宏启用额外钩子。

### 6.3 本地运行

```bash
cmake -G Ninja .. -DUSE_UB=ON -DCMAKE_BUILD_TYPE=Debug -DBUILD_UNIT_TESTS=ON
cmake --build . --target urma_jetty_rebuild_test
ctest -R urma_jetty_rebuild_test -V
```

---

## 7. 实现分期

| 阶段 | 内容 | 预估 |
|------|------|------|
| **P0-a** | `PendingWr` + poll 可注入 status=9 / FLUSH_ERR_DONE | 1–2 天 |
| **P0-b** | `urma_flush_jetty` 返回 WR_FLUSH_ERR + TC-1/TC-2 | 1 天 |
| **P0-c** | TC-3 stale epoch + CMake/ctest/CI 挂钩 | 0.5 天 |
| **P1** | TC-4/5/6 failure 路径 + ASAN Debug | 1–2 天 |
| **P2** | TC-7 disconnect；`PENDING_DRAIN` 用例（实现代码后） | 后续 |

建议 **单独 PR**（或 PR #33 的 follow-up），标题示例：

`[TransferEngine] Add mock-injected tests for jetty ACK timeout rebuild`

---

## 8. 验收标准

- [ ] `tent-ci (ub-mock)` 稳定通过，新增 ctest ≤ 30s
- [ ] TC-1～TC-3 在本地与 CI 绿
- [ ] Debug + ASAN 构建下 TC-4/TC-6 无 UAF 报告
- [ ] mock 钩子 **默认关闭**，不影响现有 `ub_transport_test` 手动跑法
- [ ] 文档：本页 + `jetty-single-rebuild-plan.md` 增加交叉链接

---

## 9. 与真机测试的分工

| 层次 | Mock gtest（CI） | 服务器真机 |
|------|------------------|------------|
| 状态机转移 | ✅ | ✅ |
| flush/fence 时序 | 近似 | ✅ |
| 多 Jetty / 8 节点 | 可选扩展 | ✅ |
| 性能 / 长期稳定性 | ❌ | ✅ |

CI 证明 **逻辑正确性**；真机证明 **provider 语义与生产负载**。

---

## 10. 参考

- 方案：`docs/source/design/transfer-engine/jetty-single-rebuild-plan.md`
- Mock 现状：`mooncake-transfer-engine/src/transport/kunpeng_transport/urma/mock_urma.cpp`
- 轻量夹具范例：`mooncake-transfer-engine/tests/rdma_endpoint_state_test.cpp`
- CI job：`/.github/workflows/ci.yml` → `tent-ci (ub-mock)`
