# TENT UB 探测资源独立供给（与 Jetty 池化解耦）

> **v1.7 更新（2026-11-16）**：探测资源已改为**本侧自建**（发现非 bonding 物理口、自建 probe Context/JFC/Jetty/源页），不再由池化侧/provider 供给本地 probe 句柄。本文的 A/B 供给方案保留为资源归属演进记录；当前实现见 `StandaloneProbeResourceProvider` 与 v1.7 详细设计 §12.1。
>
> 日期：2026-11-13。
> 关系：补充总设计《tent-ub-post-poll-threadpool-design》§9.1/§9.1.1 与接口 2/4/7 的落地拆分；**取代**《tent-ub-probe-link-detail-design》§3.4/§16 中“真实实现依赖池化侧 P0-B / 探测资源归池化侧”的表述（该文 v1.6 提交后回写一行指向本文）。
> 结论：**dedicated 模式下，链路探测不依赖业务 Jetty 池化。**

## 1. 结论与边界

“不依赖池化”指不经过业务 Jetty 池的租借、K 准入、quota、endpoint Jetty 集合、RC bind 与补池预算。探测仍然需要原生资源（probe Jetty/JFC/源页），但它们是**每口一份的专用资源**，由独立的探测资源供给方（下称 probe provider）创建、退役与替换。

| 探测需要 | 是否来自业务 Jetty 池 |
| --- | --- |
| 每本地口一个专用 probe Jetty | 否，专用发送成员，不进池 |
| 每 Context 一个 probe JFC（或共用业务 JFC） | 否；dedicated 为独立完成队列，shared 才是可选借用 |
| 每口一页探测源页（按代隔离） | 否，一页常量内存 |
| 对端探测页 descriptor（address/length/token/epoch） | 否，独立注册/独立 token |
| 远端 probe Jetty identity（逐 WR target） | 否，发送目标身份 |
| 拓扑/邻接/代次/设备发现 | 否 |
| poller 线程 | 否，复用现有线程 |

**不依赖 ≠ W1 现在自己创建原生资源。** W1 仍以 `ProbePoster` fake 与注入式 provider 运行；本文定义的是真实供给的两种 owner 与落地路径。

## 2. 资源清单与生命周期

以 4 口、每口 2 邻接（8 条链路）为例：

| 资源 | 数量 | 创建/退役 | 代次 | 说明 |
| --- | --- | --- | --- | --- |
| probe Jetty | 4 | probe provider | `jetty_generation` | 每口一个活跃发送成员；RM 下不 bind |
| probe JFC | 4（dedicated） | probe provider | 随 Context | 深度 `probe/jfc_depth`；建议 send-only |
| 探测源页 | 每口每代 1 | probe provider | `source_generation` | 页对齐注册；内容常量；旧代孤儿清零前不注销 |
| 本端探测区页 | 每口 1 | probe provider | `area_epoch` | 供对端写入；remote-write 权限＋token；descriptor 经握手发布 |
| 对端探测页 import | ≤8（按 `(ContextKey,target)` 去重） | probe provider | `area_epoch` | 独立注册、独立 token，不复用业务数据 segment |
| 远端 probe Jetty identity | 每边 1 | 对端发布 | 随 target epoch | id/uasid/eid；逐 WR 指定（RM） |
| 控制额度 | 每口 1 | probe provider | — | 单在途，需求恒为 1 |
| peer 预算 | `probe/peer_budget` | 本侧计数 | — | 同 peer 跨口在途上限；超时孤儿释放 |

职责顺序固定：**先发布 token/record，再 post；lane 退役先停 post，再 modify/flush/delete，再发布新代次；旧源页/探测页引用计数归零前不注销。**

## 3. 两种供给归属

两种方案实现同一个 `ProbeResourceProvider`，对 PLM/调度器/workers 完全等价。

### 3.1 方案 A：池化侧“仅探测角色”（过渡，改动最小）

- 仍由池化侧进程提供，但资源机械**只服务探测**：创建/退役 probe Jetty/JFC/源页、维护探测页 import、发 `kProbeHandleReplaced`；
- 不租借业务 Jetty、不占业务 lease/K/quota、不走业务 endpoint 的 Jetty 集合；
- 本侧只需注入 provider/poster（W1c 已就绪），无需新增原生资源代码；
- 适合池化侧已经掌握 bootstrap 与设备生命周期、但想把探测从业务池剥离的场景；是 B 就绪前的过渡。

### 3.2 方案 B：本侧 `StandaloneProbeResourceProvider`（最终解耦形态）

- 本侧直接调用 adapter：`createJfc` / `createJetty` / `registerLocalSegment` / `importRemoteSegment` / `deleteJetty` / `deleteJfc`；
- 对端探测页与远端 probe Jetty 由探测专属握手获得（§6）；
- 适合池化侧短期无法提供“仅探测角色”、或希望探测完全独立演进的场景；
- 代价：本侧要负责原生资源生命周期、quarantine 与设备故障处置；需要 P0-B 同级的 adapter 能力。

| 维度 | A（池化侧仅探测角色） | B（本侧独立 provider） |
| --- | --- | --- |
| 本侧代码量 | 最小（注入即可） | 中：资源管理＋握手＋测试 |
| 对池化侧依赖 | 需要其实现仅探测角色 | 仅依赖 adapter 与握手 |
| 原生资源故障处置 | 池化侧已有机械 | 本侧新建 |
| 定位 | 过渡：复用池化侧 bootstrap/设备生命周期 | 目标：探测进度不依赖池化侧排期 |

## 4. 接口映射（W1 代码无需改）

现有代码面：[probe_resources.h](../../mooncake-transfer-engine/tent/include/tent/transport/ub/probe_resources.h)（provider/poster/event 契约）、[probe_link_manager.h](../../mooncake-transfer-engine/tent/include/tent/transport/ub/probe_link_manager.h)（PLM）、[workers.cpp](../../mooncake-transfer-engine/tent/src/transport/ub/workers.cpp)（probe 槽/分派/启停钩子）、[ub_transport.cpp](../../mooncake-transfer-engine/tent/src/transport/ub/ub_transport.cpp)（装配/注入缝）。

| 现有缝 | 落地内容 |
| --- | --- |
| `ProbeResourceProvider::snapshot` | 汇总本端每口资源 + 每边对端 descriptor，构造 `ProbeGraph` |
| `retireProbeLane` | 上报探测 lane 退役；provider 执行 modify/flush/delete 并重建，发 `kProbeHandleReplaced` |
| `handoffUnproven` | 接收 W1 的 `ProbeHandoff`，接管源页/探测页引用与 quarantine |
| `ProbePoster::post` | `UrmaProbePoster` → `UrmaAdapter::post(jetty, target, wr)`；RM 逐 WR tjetty，RC 退化走绑定式 Jetty（§7） |
| `ProbeJfcSlot` | 把 dedicated probe JFC 交给 workers 轮询（provider 保有所有权，不删除） |
| `ProbeEvent` 七种 kind | provider 在资源变化时调用；幂等键 `(graph_epoch,event_seq)` |

`ProbeLinkManager`、`ProbeScheduler`、workers 三处接缝、W1 的 39 个探测用例均不需要修改；变更集中在 provider/poster 实现、`ub_transport` 装配与握手。

## 5. 原生资源落地

### 5.1 本端每口

1. **JFC**：dedicated 用 `JfcOptions{depth=probe/jfc_depth, enable_completion_events=false, transport_mode=kRm}` 创建；UMDK 要求 JFS 与共享 JFR 传输模式一致，因此探测 JFC 同时建 **RM JFR**（虽然探测只发送）；shared 模式不建，探测完成落业务 JFC。
2. **probe Jetty**：`JettyOptions{depth=…, max_sge=1, priority=…, transport_mode=kRm}` 创建，**RM 下不 bind**；RC 退化见 §7。
3. **源页**：按 page size 对齐分配（payload=1，注册长度≥1 页），`registerLocalSegment`；访问权限只覆盖本端 DMA 读，具体 flag 待 provider 验证；`source_generation` 每代递增。
4. **本端探测区页**：每口分配一页并 `registerLocalSegment`（access=WRITE，带 probe token），供对端写入 1 字节；`area_epoch` 随探测通道代次；descriptor/token 由握手层发布给对端。
5. **lane 退役**：`retireProbeLane` → quiesce 旧 Jetty，完成经 completion sink 交回 PLM → reset/delete 旧 Jetty → **同一 JFC** 上建新 Jetty + 新源页 + 新探测区页（generation+1）→ 发 `kProbeHandleReplaced`。旧代页面保留在 retired 列表直到关闭（与 quarantine 一起释放），重建时**不注销**，避免孤儿记录或对端迟写造成 UAF。需要 completion/availability 两个 sink；缺一时只记录退役、不重建。调度器在退役窗口不会 post（PLM 置 `handle_valid=false`）。
6. **quarantine**：`handoffUnproven` 接管引用；旧代源页/探测页在所有引用（含孤儿）清零后注销；关闭时按 §9.6 移交。
7. **credit/budget**：控制额度 1/口；peer 预算由本侧按 `probe/peer_budget` 统计，provider 不参与。

### 5.2 远端每边

1. 对端通过握手发布探测页 `{address,length,token,epoch}` 与远端 probe Jetty `{eid,id,uasid,generation}`；
2. 本端按 `(ContextKey,target)` import 一次，`area_epoch` 变化走 `kAreaReplaced`，target 换代走 `kTargetEpochChanged`；
3. 邻接与 `topology_epoch` 来自配置/拓扑（与池化侧同一配置源），provider 只做形状转换，不做发现；
4. 对端不支持探测时，该 peer 不产出边（计数 `probe_peer_unsupported`），不影响其他 peer。

## 6. 对端握手（bootstrap 扩展）

建议扩展现有 bootstrap（同一控制通道，独立结构体或新增字段，版本号独立）：

| 字段 | 说明 |
| --- | --- |
| `probe_schema_version` | 探测握手版本；缺失表示对端不支持 |
| `probe_area_address` / `length` / `token` / `epoch` | 对端探测页 descriptor（每 remote target 一槽） |
| `probe_jetty_id` / `uasid` / `eid` / `generation` | 远端 probe Jetty identity |
| `probe_jetty_schema` | 寻址/绑定语义（RM 逐 WR / RC 绑定） |

- **兼容**：v1 对端不返回这些字段 → 该 peer 无探测边并告警；`probe/enabled=true` 仍可安装，但只对支持的对端探测（不因单个 peer 不支持而整体失败）。
- **幂等**：同一 `(segment_id, topology_epoch, target_epoch, probe_jetty_generation)` 只应用一次；换代时旧区域判 stale。
- **归属待定**：握手字段由池化侧 bootstrap v2 统一承载，还是由 probe provider 单独发一条控制消息；推荐同通道、独立结构，避免探测进度阻塞业务 bootstrap 版本。

## 7. Poster 与 RC 退化

### 7.1 RM（首选）

- 每口一个 probe Jetty，以 `JettyOptions::transport_mode = kRm` 创建，不 bind；`ProbePostTarget` 携带远端 identity、源页与探测页段，`ProbeWorkRequest` 携带 token/源地址/探测页地址（长度均为 1）。
- poster 映射：`UrmaProbePoster` 构造单 WR `WorkRequest{WRITE, source, area, length=1, token}`，调用 `UrmaAdapter::post(jetty, remote_target, wr)`。
- adapter 侧：按 `(eid, id, uasid, token)` 缓存 `urma_import_jetty` 得到的 target；每个 WR 设 `wr.tjetty = target`、`complete_enable=1`；不调用 `urma_bind_jetty`；清理时 unimport 全部 target。不需要 Jetty 池。

### 7.2 RC 退化（bring-up 用，不默认）

- 每**边**一个 probe Jetty，创建后 `bindJetty(remote)`；post 仍用现有绑定语义。
- 资源数量从 “每口 1” 变为 “每边 1”（4 口 8 边 → 8 个 Jetty），绑定生命周期跟随 endpoint 重建；lane 归属变为每边。
- 仍不经过业务池，但代价更高：Jetty 数量、bind 重建、失败域更细；**仅用于 RM 未就绪时的本侧验证**。
- **正式设计仍以 RM 为唯一目标**（对齐探针详设 v1.6“RC 不在本设计支持范围”）；本文的 RC 退化是 bring-up 手段，不是长期模式，也不进入默认配置。
- 建议配置：`probe/rc_fallback`（bool，默认 false，固定值语义待评审）；未验证前不默认启用。

### 7.3 契约映射

`ProbePostTarget = {probe_jetty, jetty_generation, remote_jetty, source_segment, area_segment}`；`ProbeWorkRequest = {opcode=WRITE, token, source_address/length=1, area_address/length=1}`。RM 实现使用 `remote_jetty` + 逐 WR adapter 重载；RC 退化忽略它并使用已绑定 Jetty。两种实现都不需要 PLM 变化。

## 8. 落地拆分

| 步骤 | 内容 | 出口 | 依赖 |
| --- | --- | --- | --- |
| **P1** | provider 骨架 + 本端资源管理（建/删 probe Jetty/JFC/源页），fake adapter 可跑 | 安装/拆卸不泄漏；每口资源可查 | 无 |
| **P2** | 握手字段 + 探测页 import，单 peer/单口建图 | `ProbeGraph` 含边；对端不支持时降级并计数 | 握手字段评审 |
| **P3** | Poster：RM 逐 WR（已实现）/ RC 绑定退化（bring-up） | 1 字节 WRITE 完成；token 正确回传 | RM adapter |
| **P4** | lane 退役/重建 + 接口 7 事件 + quarantine（provider 侧已实现） | 超时→退役→新代次→恢复；旧页引用清零前不注销 | P1–P3 |
| **P5** | 多口/多 peer/多代次；shared 模式可选 | 矩阵 #4/#6/#16 在真实 adapter 上通过 | P4 |
| **P6** | 实机：单机双端 → 跨机多口 | 探测完成、超时、关闭移交与日志一致 | 硬件＋池化侧 provider |

**实现状态（2026-11-16）**：P1–P5 已落地；**v1.7 进一步改为本侧自建资源**：`StandaloneProbeResourceProvider::discover()` 枚举物理口、过滤 bond（有 bonding 时使用剩余非 bonding 设备）、支持 `probe/device_filter`，每口自建 probe Context/JFC/Jetty/源页；transport 以发现出的 probe Context 构造远端 directory。测试模式 `probe/test/*` 已实现（periodic_all / interval_ms / apply_health）。剩余：bootstrap 线上字段（描述符交换）、`probe/rc_fallback`、未决资源 Unproven 语义的完整回写。注意：本机默认构建 `TENT_HAS_REAL_URMA=OFF`；RM 原生路径已用仓库内 vendored UMDK 头文件（`build/_deps/urma-src`）以 `-DTENT_HAS_REAL_URMA=1` 完成对象级编译，仍需 real liburma 链接构建与实机验证。

新增代码面（预计）：

- `include/tent/transport/ub/probe_provider_standalone.h` + `src/transport/ub/probe_provider_standalone.cpp`（方案 B）；
- `include/tent/transport/ub/probe_poster_urma.h` + `src/transport/ub/probe_poster_urma.cpp`（RM）；
- adapter：`JettyOptions::transport_mode`、`UrmaAdapter::post(jetty, target, wr)`、按 target 缓存 import；
- `params.h`：`probe/provider`（pool/standalone）、`probe/rc_fallback`（默认 false）等；
- `ub_transport.cpp`：按配置选择 provider/poster（现有注入缝不变）；
- `tests/ub_probe_standalone_provider_test.cpp`：fake adapter 生命周期、握手降级、RC 退化；
- `tests/ub_probe_handshake_test.cpp`：descriptor 校验/import/cache；
- `tests/ub_probe_poster_test.cpp`：RM 逐 WR WRITE；
- `tests/ub_probe_rebuild_test.cpp`：退役/重建、接口 7 事件、完成派发与旧页 quarantine。

不改：`probe_link_manager.*`、`probe_scheduler.*`、workers 的探测接缝、W1 既有测试。

## 9. 配置建议

| 键 | 默认 | 语义 |
| --- | --- | --- |
| `probe/provider` | `pool`（默认）/`standalone`（已实现） | `standalone` 由 transport 基于本端 Context 构造独立 provider；需注入 `ProbePeerDescriptorSource`，仅支持 dedicated JFC；`enabled=false` 时不构造 |
| `probe/rc_fallback` | false | RM 未就绪时启用每边绑定式探测；仅 bring-up |
| `probe/peer_unsupported` | 计数 | 对端不支持探测时的降级观测 |

W1d 已实现的 11 个 `probe/*` 键保持不变；`probe/provider` 已在 P5 实现（默认 `pool`）；`probe/rc_fallback` 仍未实现，暂不进入配置表。

## 11. 两节点 selftest（tent_probe_selftest）

用于没有 bootstrap/控制面时，快速验证真实 URMA 两节点探测。工具位于
`mooncake-transfer-engine/tent/tests/ub_probe_selftest.cpp`，构建目标
`tent_probe_selftest`，不经 `TransferEngine`。

**前提**：两端已用 real URMA 构建（`USE_UB=ON` + 有效 `URMA_LIBRARY`），
UMDK 设备/EID 互通；bond 会被过滤，只有没有任何非 bonding 物理口时才失败。
`--list-devices` 会列出每个设备的全部 EID（`topology=...:eidN`）；`--device-filter`
支持 `<native>`、`<native>:eidN` 和完整 `topology_name`。只写 native 时枚举该设备全部
active EID；显式 EID 只选该 EID，配对优先按 `topology_name`（含 EID）。

**交换方式**（推荐 target/initiator，替代 scp）：

```bash
# target 端
tent_probe_selftest --mode target --listen-port 19090 \
  --node a --peer b --device-filter <口名> ...

# initiator 端
tent_probe_selftest --mode initiator --target-host nodeA --target-port 19090 \
  --node b --peer a --device-filter <口名> ...
```

target 先监听，initiator 连接后双方交换同一份 JSON（4 字节长度前缀），不再依赖共享目录/文件时序。文件模式仍保留：`--mode file --exchange-dir <dir>` 或默认。

**文件模式流程**：

1. 两端选择一个共享目录（NFS/sshfs），或手工 scp JSON；
2. 每端启动工具：发现物理口 → 自建 probe Context/JFC/Jetty/源页/探测区页
   → 写 `<node>.json`（area descriptor/token/epoch + probe jetty id/uasid/eid）
   → 等 `<peer>.json`；
3. 读到对端 JSON 后按 `topology_name`（优先，含 EID）/ native 名配对，import
   对端探测页，启动 PLM + scheduler + probe JFC poll 线程；
4. 测试模式按 `--test-interval-ms` 定时探测全部链路。

```bash
# 节点 A
tent_probe_selftest --node a --peer b --exchange-dir /shared \
  --device-filter <物理口名或 native:eidN> --test-interval-ms 5000 --apply-health=false

# 节点 B
tent_probe_selftest --node b --peer a --exchange-dir /shared \
  --device-filter <物理口名或 native:eidN> --test-interval-ms 5000 --apply-health=false
```

**预期输出**：

```text
local descriptors written to /shared/a.json; waiting for /shared/b.json
probe selftest started: node=a peer=b test_interval_ms=5000 apply_health=false
[probe] t=1s inflight=0 test_post=1 ok=1 fail=0 missed=0 timeout=0 ...
```

判定：两端 `test_post/ok` 持续增长、`fail/timeout` 不增长；Ctrl-C 后
`drained=true`。若出现 `unproven`，工具返回 2 并故意保留资源到进程退出
（避免对可能仍在 DMA 的页做 unimport）。

**注意**：

- 每次运行前删除共享目录里的旧 `<node>.json`，否则会读到上一轮的
  Jetty/area 身份；
- 两端 `local_nic` 必须能配对；端口数不同时按同号配对，缺失口会告警跳过；
- 第一轮建议 `--apply-health=false`（纯链路检查），通了再 `--apply-health=true`
  观察 ISOLATED/RECOVERING/HEALTHY；
- 生产验证仍然需要正式 bootstrap 描述符交换；selftest 只用于两节点联调。

## 10. 未决问题

1. 探测页的发布方与授权：由对端业务/池化侧注册，还是探测专属注册？token 是否复用？
2. 源页/探测页的页对齐与访问 flag（`SEGMENT_ACCESS_LOCAL_ONLY` 是否满足本地 DMA 读）。
3. probe JFC 的 send-only 能力与 `max_jfc` 预算。
4. quarantine 的归属与时限（provider 还是池化侧 tombstone）。
5. 握手字段由谁承载、版本如何协商；对端不支持时“局部降级”还是“整体拒绝”。
6. RC 退化是否接受为正式模式，还是仅本侧 bring-up。
7. `probe/provider` 的默认值与迁移路径（W3 默认启用时）。
