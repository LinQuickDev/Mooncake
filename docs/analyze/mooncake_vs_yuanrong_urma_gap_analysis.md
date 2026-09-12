# Mooncake vs yuanrong-datasystem：传输层可靠性与元数据能力 Gap 分析

> 对比基线：
> - **Mooncake**：`supercache-snapshot-912` 分支（HEAD `14a9f55a`），重点考察 `mooncake-transfer-engine/src/transport/kunpeng_transport/`（UB/URMA）与 `mooncake-store`（元数据/HA）。
> - **yuanrong-datasystem**（kvc）：`master` 分支（HEAD `4b19814c3`），重点考察 `src/datasystem/common/rdma/`（URMA）、`src/datasystem/cluster/`（拓扑/元数据）、`src/datasystem/client/object_cache/transport/`（客户端传输）。
>
> 本文档为内部技术分析。源码事实尽量附 `文件:行号`；方案性结论会显式标注为“推断”或“待验证”，避免把 yuanrong 的参数直接当作 Mooncake 的正确参数。

### 证据口径与重要边界

- **已核实**：可由上述两个 commit 的源码直接确认；本文默认未标注的源码事实均属此类。
- **推断**：从数据结构、调用关系或故障语义推导，必须通过故障注入或线上 shadow 指标验证。
- **待验证**：依赖实际 UMDK/固件、部署拓扑或运行参数，静态源码不足以裁决。
- 2026-09-12 曾两次尝试只读连接 `work@mooncake-dev` 核对 UMDK ABI，均在 SSH 握手阶段被 `127.0.0.1:2223` reset，因此本文没有把远端头/库兼容性标为已验证。
- Mooncake 当前不是单一传输实现：至少同时存在 **classic UB**（`kunpeng_transport`）、**generic RDMA** 和 **TENT**。本文发现的 `volatile` 深度表、1 秒强制恢复、staging 生命周期等问题，除非另有说明，均特指 Store 在 `protocol=ub` 且未设置 `MC_USE_TENT` 时走的 **classic UB**；generic RDMA 已使用 `std::atomic<int>` 深度表，TENT 也已有策略路由、QoS、指标与原生 UB 数据面测试，不能把三者的能力混为一谈（`transfer_engine.cpp:401-417`、`client_service.cpp:771-833`、`rdma_endpoint.cpp:123-137`）。
- yuanrong 的 CQE 4/9、`BONDP_USER_CTL_QUERY_PORT_STATUS` 和 128ms 延迟释放都是**特定实现中的经验值/ABI**，可借鉴其状态机与所有权思想，不应无条件复制常量。

---

## 1. 对比范围与总体结论

| 维度 | Mooncake (supercache) | yuanrong-datasystem | 差距方向 |
|------|----------------------|---------------------|----------|
| 光口/端口故障处理 | 异步事件置 inactive + monitor 线程 1s 强制重激活 | 端口健康监测体系（epoch 围栏 + 准入状态机 + 远端验证） | **yuanrong 显著领先** |
| Jetty 共享与生命周期 | 每 endpoint 1 个 jetty，无池化无状态机 | 进程级池（200）+ PostGate 原子门 + 退役流水线 + 孤儿记账 | **yuanrong 显著领先** |
| 多网口负载均衡 | 随机切片扇出 + bondp BALANCE/IODIE | chip 级 inflight 差反馈 + 亲和策略（bondp BALANCE/PORT） | **yuanrong 领先（反馈闭环）** |
| 对端故障隔离 | 9 次重试后删 endpoint，无熔断 | 对端熔断器 + 类型化错误码 + TCP 限流降级 | **yuanrong 显著领先** |
| 超时/迟到完成处理 | TIMEOUT 对外可见但 batch 保持 busy；无界等待迟到 CQE，且部分 staging 失败分支不收敛 | 保留事件 + 孤儿 WR 记账 + 迟到 CQE 分发 | **yuanrong 显著领先** |
| 元数据均衡 | 有人工 Drain Job + 动态副本；无自动压力/热度再平衡 | 均衡哈希环 + 内存/热度再平衡 + 速率限制迁移 | **yuanrong 领先（调度闭环）** |
| Master 高可用 | 围栏选举 + 有序 OpLog + 分块快照 | etcd lease + 分区确认 + 自杀围栏（数据面无共识） | **故障模型不同，不宜单项判胜** |
| 观测性（trace/perf point） | SpDiag + trace_id 全链路 | UrmaWriteTrace 分相取证 + 慢日志建议 | **各有千秋，yuanrong 更深** |
| GPU/设备内存传输 | CPU staging 池（D2H/H2D），存在终态收敛和 teardown 风险 | RemoteH2D + Pipeline H2D（复用 URMA jetty/JFC） | **yuanrong 领先（Mooncake 先修正确性）** |
| 多设备并发 | 每 HCA 独立 context + 切片扇出 | 单设备（bonding_dev_0）顺序候选 | **Mooncake 领先** |

**总体结论**：yuanrong 在 UB 数据面的**可靠性工程**（端口健康、熔断降级、生命周期管理、超时对账）上形成了闭环，是 Mooncake supercache 最值得借鉴的部分；Mooncake 在**多设备聚合、具备日志/快照的集中式 HA、SSD 分层和副本任务框架**上有自己的基础。正确方向不是移植 yuanrong 的单设备实现，而是把其“确认事实 → 准入 → 隔离/降级 → 迟到对账 → 可观测恢复”的控制闭环，适配到 Mooncake 多设备与多副本模型中。

### 1.1 三条数据面的归属判断

| 路径 | 当前已具备 | 本文建议承担的角色 |
|------|------------|--------------------|
| classic UB | Store `protocol=ub` 的直接路径；多 HCA context、staging、SIEVE endpoint | 先承接 P0 安全修复，保证现网兼容；避免继续堆叠长期性能架构 |
| generic RDMA | 原子深度计数、rail 失败标记/替代路径、ready ACK 超时等机制 | 复用其错误分类、rail 隔离和原子资源记账思想，抽到传输中立层 |
| TENT | 配置化 `TransportSelector`、QoS、指标、NIC load stats、原生 UB 测试 | 作为中长期统一数据面候选；新建负载学习、策略路由优先在此落地 |

因此，整改要设置明确的**代码归属门**：影响内存安全/数据正确性的修复必须回补 classic UB；可复用的健康、熔断、错误分类放在公共抽象；大规模池化、调度和性能增强优先验证 TENT，避免永久维护两套 UB 控制面。

### 1.2 本轮核查对原分析的关键纠偏

1. “Mooncake 传输层 0 UT”不成立：classic/TENT 合计已有 8 个 UB 相关用例；准确 gap 是 classic 用例未进入 ctest，且缺系统故障注入。
2. “Mooncake 只有驱逐、没有迁移”不准确：已有人工 Drain Job、Move/Copy Task 和动态副本；缺的是**自动压力/热度 planner、限速反馈和 HA job 意图**。
3. “超时立即释放导致迟到 CQE UAF”不是当前直接行为：TIMEOUT 后 batch 仍 busy，实际问题是**业务终态与资源终态没有显式分离，CQE 丢失时资源无界等待**；另有未 post/重分发失败分支的确定性 staging 泄漏。
4. `volatile` 深度表只属于 classic UB，generic RDMA 已使用原子类型；整改范围必须写清，避免重复改造或错误归因。
5. Master HA 不能简单判胜负：Mooncake 强在状态复制/恢复，yuanrong 强在分区确认与 self-fencing，二者对应不同故障模型。
6. yuanrong 的 128ms、200 lane、8 个 per-peer 配额、inflight 差 15 是**样本参数而非设计真理**；应复制所有权/围栏/反馈机制，再由 Mooncake 实机标定参数。

---

## 2. yuanrong 光模块组网可靠性深度洞察

### 2.1 端口健康监测体系（光口故障处理核心）

yuanrong 构建了一套五层递进的可靠性栈，光口故障处理是其中最完整的部分：

```
┌─ L1 硬件/bonding 层：bondp BALANCE 模式多端口分担；provider failover 显式关闭
│    （urma_resource.cpp:177-238，把故障裁决权交给用户态）
├─ L2 Jetty 池层：进程级 200 jetty 池 + 退役流水线 + per-peer 上限
├─ L3 端口健康层：UbPortHealthMonitor 每秒轮询 BONDP_USER_CTL_QUERY_PORT_STATUS，
│    按 (chip_id, die_id, port_idx) 输出 GOOD/BAD，healthEpoch 围栏防陈旧
├─ L4 准入/熔断层：PeerUbAdmission 状态机（AVAILABLE/SUSPECT/UNAVAILABLE/PROBING），
│    由 CQE 4（本地端口不可用）/ CQE 9（远端 ack 超时）分类驱动
└─ L5 路由/降级层：SHM/UB/TCP 传输顾问 + 成功率驱动 worker 切换 + TCP 限流
```

**关键机制**（均含源码引用）：

1. **Provider 适配器**：`UrmaPortStatusProvider` 封装 `BONDP_USER_CTL_QUERY_PORT_STATUS`，解码时校验 `port_count`、排序去重端口身份（urma_port_status_provider.cpp:46-85）。业务代码不直接接触厂商类型。
2. **健康快照 + epoch 围栏**：不可变 `shared_ptr` 快照原子发布，每次确认变化递增 `healthEpoch`（ub_port_health.cpp:131-186）。健康时轮询循环休眠（零周期查询开销，ub_port_health.cpp:188-192）。
3. **`verificationPending` 一等状态**：未确认的观察"既不能建立隔离也不能解除隔离"（ub_port_health.h:99-110）——这是抗链路抖动（flapping）的关键设计。
4. **CQE 4/9 语义**：CQE 4 = 本地发送端口不可用；CQE 9 = 远端对端 ack 超时（fast_transport_base.h:37-38）。同步失败经 `UrmaWriteFailure{providerStatus, cqeStatus}` 上传；**迟到完成**（请求已超时）经保留事件 + 观察者分发（urma_manager.cpp:1313-1329）。
5. **客户端准入位字**：整个端口摘要打包进**单个 atomic uint64**（bit63=READY，bit32-62=总数，低 32 位=坏数）。仅 `bad == total` 才拒绝（`K_URMA_WORKER_UNAVAILABLE`），部分坏端口不阻塞（urma_manager.cpp:411-425）。
6. **验证式端口健康模式**：E4/E9 不立即置 UNAVAILABLE，而是转 **SUSPECT** 并触发验证（本地刷新或远端 `QueryUbPortHealth` RPC）；只有确认的端口事实才能改变状态（peer_ub_admission.cpp:311-335, 356-400）。`portHealthGoverned` 标志保证一旦确认事实主导隔离，普通结果不能覆盖（peer_ub_admission.cpp:863-870）。
7. **远端健康传播**：`QueryUbPortHealth` RPC 携带 incarnation 防重启竞态（worker_oc_service_impl.cpp:3639-3662）；客户端最多 4 并发验证查询（remote_ub_port_health_verifier.h:34-35）；验证后的摘要进入 `WorkerRouter` 过滤链，把全坏 worker 从路由中剔除（ub_remote_port_health_propagation_test.cpp:108-145 有端到端测试）。
8. **Piggyback**：每个 GetObjectRemote 响应附带 provider 自健康；provider 侧 UB 写失败编码为 `ProviderUbFailureDetailPb` 让请求方隔离源端（worker_worker_oc_service_impl.cpp:229-236）。

**故障场景覆盖矩阵**（摘自分析，均有测试或源码证据）：

| 场景 | 机制 | 结果 |
|------|------|------|
| 单端口故障（部分） | bonding 分担 + bad<total 不隔离 | 流量继续，`HealthyPortCount()` 供路由参考 |
| 本地全端口故障 | CQE 4 + 客户端准入位字 | UB 操作阻塞，Put 走 TCP 降级（<1MB） |
| 远端对端全端口故障 | CQE 9 → SUSPECT → 远端验证 → UNAVAILABLE | 对端隔离，**远端 Get RPC 也不再发起**（省成本） |
| 链路抖动 | epoch 围栏 + pending 态 + 探测退避（1s→32s） | 状态变化需确认的、epoch 有序的事实 |
| 对端进程重启 | incarnation 不匹配 → `K_URMA_NEED_CONNECT`；墓碑阻断陈旧摘要回放 | 连接重建，陈旧事实被拒 |
| 池耗尽（本地压力） | `K_URMA_TRY_AGAIN`（区别于业务 K_TRY_AGAIN）+ per-peer 上限 | 调用方退避，其它对端不受影响 |

**局限**（Mooncake 不必照搬的部分）：光模块诊断只有二元 GOOD/BAD，无温度/光功率/BER/DOM 遥测（全库搜索证实）；单设备运行（`bonding_dev_0`），多 NIC 并发数据面缺席。

### 2.2 Jetty 共享与池化

- **进程级池**：`SendJettyPool` 目标 200 条 lane（`urma_send_jetty_lane_pool_size`），启动时**池外预填充**（失败即 Init 失败），后台 refill 线程 50ms tick、失败强制整 tick 退避、退役余量上限 200（urma_resource.cpp:1710-1789）。
- **PostGate 单原子字**：`[63]=closing, [62]=retireArmed, [61]=finalizerScheduled, [60:0]=activePosts` 打包在一个 `std::atomic<uint64_t>`，准入与退役共享**同一线性化点**，显式注释拒绝 C++ 位域（urma_resource.h:629-649, 800-801）。
- **生命周期 FSM**：`ACTIVE → QUIESCING → MODIFYING → WAIT_FLUSH → DELETE_READY → DELETING → DESTROYED`，`QUARANTINED` 为终态。退役经 `urma_modify_jetty(STATE_ERROR)` → 等 `FLUSH_ERR_DONE` CQE → 4 线程池异步删除；删除失败则隔离（保留 registry 身份防 ABA）（urma_resource.cpp:1196-1238, 1595-1648）。
- **fail-closed**：未收敛的 jetty/context 挂到进程退出（`NoDestructor`），`providerCleanupDeferred_` 阻止 liburma 卸载（urma_resource.cpp:83-120）。
- **孤儿 WR 记账**：超时 lane 合法归还后，在途 WR 计数转入 per-jetty 孤儿账本（警告 16 / 退役 32，`static_assert` 锁定），孤儿饱和退役**归因到对端熔断器**（urma_resource.cpp:1270-1373, 146-149）。
- **per-peer 公平性**：`MAX_INFLIGHT_JETTIES=8` 并发上限（坏对端的爆炸半径），bthread CV 有界等待；`MAX_RETIRED_JETTIES=8` 触发熔断，退避 1s→30s，HALF_OPEN 单探针，代际围栏防旧代关闭半开熔断（urma_resource.h:1021-1043）。
- **RPC 级 lane 共享**：worker↔worker BatchGet 整个 RPC 只取**一次** lane，所有子请求经 `UrmaWritePayloadWithLane`/`UrmaGatherWriteWithLane` 复用，RAII 保证 seal 恰好一次（worker_worker_oc_service_impl.cpp:1183-1252）。对象级失败不退役共享 lane。

### 2.3 多网口负载均衡

- **chip 级 inflight 反馈选择**：`alignas(64) SrcChipInflightCounter`（每 chip 独立缓存行，完成线程并发更新），策略 = 轮转候选 → 若 `|chip1 − chip2| inflight 差 > 15` 则选空闲 chip → 否则在 WR 预算内保持内存亲和（urma_manager.cpp:1474-1551）。
- **选择粒度可配**：`ub_numa_rr_type`（0=关 / 1=每逻辑写 / 2=每 post）；gather write 按 SGE 主导 chip 选择（SelectDominantGatherSrcChipId）。
- **内存侧配合**：客户端 256MB（最大 2GB）传输池分 4 个 arena，页对齐 + chip 交错轮转 NUMA 绑定（`mbind` 1GB 粒度 + 逐页 touch）。
- **局限**：仅覆盖 chip 1-2；多设备并发缺席；端口级分担交给 bonding 驱动。

### 2.4 网络拓扑与路由

- **握手经 RPC**：`ExchangeUrmaConnectInfo`（非带内），支持委托上下文 blob（`rjettyBuf`/`seg_ctx`）新旧两种导入路径；连接键 = worker `host:port` 或 client `clientId`（地址回退）。
- **incarnation 全链路围栏**：哈希环 → 准入快照 → 健康注册表 → 连接实例检查全部按 incarnation 隔离；请求可携带远端 `uniqueInstanceId`，不匹配返回 `K_URMA_NEED_CONNECT`（urma_manager.cpp:3397-3405）。
- **传输顾问**：同 host worker → SHM（fd 传递零拷贝）；URMA 可用 → UB；否则 TCP。SHM 候选取自路由拓扑而非本地探测（transport_advisor.cpp:28-44）。
- **读路径重建选举**：UB 面缺失时单 rebuilder 选举 + 1000ms 冷却，非选举者内联降级 TCP（data_plane_manager.cpp:1058-1156）。
- **预热**：客户端进程内预热（20×256KB + 80×1B，16 路，500ms 预算）+ k8s 部署后 worker↔worker 定向预热 playbook（64 worker = 4032 个有向 Remote Get，验证每个方向而非假设对称）。

---

## 3. yuanrong 元数据层负载均衡与可靠性

### 3.1 架构

- **worker 嵌入式 master**：元数据按一致性哈希分片到所有 worker（`enable_distributed_master=true` 默认）；错路由返回带 `topology_version` 的重定向，客户端 16 跳预算 + 环检测 + 单调版本强制（metadata_redirect_helper.h:130-153）。
- **协调后端三选一**：外部 etcd / worker 内嵌 metastore / 独立 Coordinator（braft Raft，心跳 100ms、选举 1000ms）。
- **对象元数据**：64 路进程内分片（tbb concurrent_hash_map + bthread RWLock）；持久化 = 本地 RocksDB（k8s hostPath 跨 pod 重启存活）+ 可选 etcd（MurmurHash 前缀分表）+ L2（OBS/SFS）。

### 3.2 负载均衡能力（Mooncake 缺失部分）

1. **均衡哈希环**：每 member 4 token；新成员 token 取"当前最重成员最大弧的理想份额前缀"（balanced_ring.h:56-88），超 8000 token 退化纯哈希。
2. **扩缩容规划**：`PlanScaleOut/ScaleIn/Failure` 产出 owner 变更区间；合批窗口 scale_in 3000ms / scale_out 5000ms。
3. **数据迁移**：速率限制 40MB/s/节点；目标选择 4 级退化梯子（空闲率 ≥50% → ≥20% → 任意非离开节点 → 无条件），保证缩容必然完成（scale_down_node_selector.h:31-57）；迁移幂等（businessOperationId）+ deadline + 可取消。
4. **内存再平衡**（master 调度）：源 ≥80%、差 ≥20%、每批 300MB、30s epoch 预算（批间获取新目标内存反馈）、per-worker/对 60s 冷却、**拓扑围栏**（任何拓扑批活动期间暂停，拓扑版本稳定且全员新报告后恢复）。
5. **热度再平衡**：迁移最低热度主副本离开过载 worker；源路径 A) 使用>60% 且有主副本 B) 使用>50% 且热主副本>40%；热度指数衰减（主副本半衰期 600s）。
6. **驱逐策略集群滚动**：PRECHECK/COMMIT 两阶段 + epoch 持久化，混合策略集群暂停再平衡。

### 3.3 可靠性机制

- **故障检测分层**：etcd lease 心跳 1s / membership TTL 60s / 死亡判定 300s；`TopologyFailureClassifier` 连续缺席单调计时（后端中断时暂停）；**witness probe**——确定性选主探测者 + 3 个见证 worker 投票 REACHABLE/UNREACHABLE，防分区误判。
- **Split-brain 自杀围栏**：keepalive 失败 → 先问对端能否达 etcd（全员不行=集群级故障，不动作）；仅自己不行 → 确认后发伪 lease-DELETE 事件 + 备份自杀计时器；隔离超时且 `auto_del_dead_node` → **`raise(SIGKILL)` 自杀**（etcd_store.cpp:645-700）。硬围栏防止陈旧 worker 在哈希段已重分配后继续服务。
- **worker 死亡恢复**：lease 过期 → 分类窗口 + witness → FAILURE 批 → 主副本重选（`ReselectPrimaryCopy`）+ 元数据位置清扫 + L2 slot 轮转接管（恢复者自身失败的未完成任务全局重排继承）。
- **重启 vs 死亡区分**：重启事实携带 membership 代时间戳；`enable_reconciliation` 时 master 推元数据回重启 worker；网络恢复路径重放排队异步操作。
- **版本/围栏体系**：对象 version（恢复合并时旧版本收 `CACHE_INVALID`）、TopologyState.version（重定向拒回滚）、batchEpoch（迁移围栏）、驱逐策略 epoch、Raft term。

**诚实的局限**：多数可靠性特性**代码默认关闭**（`enable_memory_rebalance=false`、`enable_reconciliation=false` 代码默认、`enable_lossless_data_exit_mode=false`）；内存数据无冗余（副本明说是性能非可靠性）；`distributed_disk` 恢复依赖 etcd 可用；新旧两代控制面（`master/` 遗留 + `cluster/` 新）共存于一个二进制。

---

## 4. yuanrong 数据传输层优化

### 4.1 内存管理

- **整池单注册**：256MB（最大 2GB）池**一次 mmap + 一次 `RegisterSegment`**，之后纯切偏移（`urma_register_whole_arena=true` 默认，urma_manager.cpp:500-533）。对比 Mooncake：注册粒度是 segment 级（每 NUMA 段一次），量级相当，但 yuanrong 把**客户端收发缓冲全部圈进预注册池**，这是其零拷贝路径的基础。
- **池尺寸规范化**：池大小向上取整到 `pageSize × arenaNum` 的倍数，保证每 arena 整页（urma_manager.cpp:480-498）。
- **NUMA 范围表**：arena 启动时每 1MB 采样一页 `SYS_move_pages` 构建 O(log n) 地址→NUMA 查询表（arena.cpp:1074-1139），传输时查源 chip 无系统调用。
- **CUDA host 注册**：池初始化后 `RegisterCudaHostMemory` 钉住整池供 GPU 直达。

### 4.2 批处理与流水线

- **MSet 流水线**：窗口 `min(32, 池大小)`，窗口内先全部非阻塞提交再统一等待——提交与完成重叠且不超订池（ub_transporter.cpp:919-954）。
- **BatchGet 聚合 + 自适应分裂**：连续子请求打包进一个聚合缓冲（上限 32MB/1024 对象/16B 切片对齐）；**分配失败时记录 `allocationCeiling` 单向棘轮 + 平衡二分分裂**（选 `|左−右|` 最小的分割点，优先处理最大待处理区间），叶子级失败转 TCP 批量拉取（ub_transporter.cpp:362-461）。
- **Gather write**：每 WR 13 个源 SGE（硬件上限），WR 链一次 `post_jetty_send_wr` 提交——最多 13×n 对象一次驱动态切换；`badWr` 指针匹配解析实际接受数只清理未接受事件（urma_manager.cpp:2964-3248）。
- **平衡分块写**：`writeChunkCount = ceil(size/maxWriteSize)` 后按 `ceil(size/chunkCount)` 均分——8MB/5MB 上限 = 4+4 而非 5+3，每块延迟均匀（urma_manager.cpp:2433-2438）。

### 4.3 零拷贝

- **Get 路径**：worker 直接 RDMA WRITE 进客户端预注册池缓冲，结果以 `externalData + externalOwner` 零拷贝返回（ub_transporter.cpp:661-664）。
- **Create/Set 路径**：`ObjectBuffer` 直接映射到池内存（`info->pointer = handle->GetPointer()`），Set 成功后仅发**元数据 RPC**（空 payload）。
- **模糊失败延迟释放**：RPC 状态不确定（可重试错误/`K_URMA_ERROR`）时，接收缓冲交给 `DelayedReleaseShmManager` **128ms 后才回池**——防止迟到 RDMA 写落进已回收缓冲（delayed_release_shm_manager.h:32-33）。这是 Mooncake staging 泄漏问题的同类场景下更完善的答案。

### 4.4 完成路径

- 单 poll 线程 + 单 JFC；批 8 CR/次、最多 10 次/唤醒、空转 `nanosleep(1µs)`（实测唤醒 ~50µs 并记入取证）；poll 线程 `sched_setattr` 运行时 1.4ms（CFS 基础切片一半）+ nice 可调。
- **bthread CV 而非 std CV**：brpc M:N 调度下 std CV 会钉死底层 pthread；等待切片成 1s 段免疫墙钟跳变。
- **分相延迟取证**：`UrmaWriteTrace` 记录 post/wait/poll_begin/sleep_start/sleep_end/poll_end/notify/awake/observed 九个时间戳；顺序等待上下文把首块唤醒延迟归因到后续块；慢日志附"建议检查项"字符串（urma_manager.cpp:93-99）。

### 4.5 H2D/NPU 集成

- `RemoteH2DManager`：RoCE（FFTS 上下文 16384 blob 上限分批）与 HCCS/HIXL（每 NPU 独立引擎、连接身份轮转分摊 p2p）双策略。
- **Pipeline H2D**（`BUILD_PIPLN_H2D`）：**复用进程 URMA context/JFC/JFCE**，CQE 经共享 JFC poll 循环 `PiplnH2DRecvEventHook` 重定向给流水线；client-worker 间共享内存环形队列（容量 200）传递 chunk 元数据；URMA 网络写与设备侧拷贝流水化。

---

## 5. Mooncake 当前分支能力现状（对照基线）

**强项**（应保留）：
- 多设备并发：每 HCA 独立 `UbContext` + 切片随机扇出（ub_transport.cpp:925-951；topology.cpp:786-807），重试时确定性轮转所有 NIC。
- 拓扑发现：NUMA + PCI 距离 + JSON 覆盖 + `MC_NIC_PEER_AFFINITY` rail 对齐（topology.cpp:511-604）。
- Master HA：围栏选举（warming 占位 → producer_view claim → lease TTL 预热）+ 校验和有序 OpLog + 分块快照 + supervisor 状态机（master_service_supervisor.cpp:239-640）。
- SIEVE endpoint 缓存 + 延迟回收（有在途切片不释放，ub_context.cpp:40-167）；`failed_target_ids` 100ms 快速失败负缓存。
- SSD 分层：tombstone GC + bucket 压实 + push 模式 owner 直写。
- 副本运维基础：已有 `CreateCopyTask` / `CreateMoveTask`、动态副本 lease/version fence，以及人工 `CreateDrainJob`；Drain 会把源 segment 标记为 DRAINING，按并发度创建 MoveTask，并避开 hard-pin、未过期 lease、未完成副本和已有复制任务（master_service.cpp:13871-13910, 14075-14203）。
- TENT 已有策略路由、QoS、指标和 UB 原生数据面测试；classic UB 也有 mock 支撑的 `ub_transport_test`，只是该测试未注册进 ctest（tests/CMakeLists.txt:310-317；tent/tests/CMakeLists.txt:532-538）。
- 观测性：SpDiag PerfPoint + trace_id 全链路 + MC_LOG 异步环形日志。

**弱项**（gap 所在）：
- UB 故障处理：`DEV_FATAL/PORT_DOWN → set_active(false)+disconnectAll`，但 **monitor 线程每秒无条件 `set_active(true)`**（ub_context.cpp:622-625）击穿闩锁；32 次失败启发式用的是**生命周期累计计数器**（ub_context.h:84），历史成功后永远不再触发；无熔断、无 TCP 降级、无恢复探测。
- Jetty 模型粗糙：classic UB 默认 `num_jetty_per_ep=1`、每 context 仅 2 个共享 JFC、随机选 jetty、无池化无生命周期状态机、无 per-peer 公平性。这里的 `volatile int*` 仅存在于 classic UB；generic RDMA 已改用 `std::atomic<int>*`（urma_endpoint.h:205-207；rdma_endpoint.h:224）。
- NIC 选择闭环不一致：classic 路径 `getNicLoadStats()` 恒空（transfer_engine.cpp:264-267）；TENT 路径会返回 `inflight_bytes` 与 `ewma_bandwidth_bps`（transfer_engine.cpp:803-818），但 TENT 需显式通过 `MC_USE_TENT` 启用，不能据此认为 classic UB 已有反馈选路。
- 超时所有权不闭合：`MultiTransport` 检测到 slice 超时只对外返回 TIMEOUT，并未把 task 置 finished（multi_transport.cpp:274-308）；`freeBatchID` 又拒绝释放未完成 task（multi_transport.cpp:113-126）。这避免了立即 UAF，但 CQE 永不到达时 batch、slice、staging 可能永久占用。另有确定性缺陷：initial dispatch 与 redispatch 的若干直接 `markFailed()` 分支绕过 `onStagedSliceFinalFailure()`，导致 `completed_slices` 无法收敛（ub_context.cpp:242,261,269,280,530,539；ub_transport.cpp:366-392）。
- WR 单 SGE（`num_sge=1`，jfs 能力 5）；staging 池 first-fit 无合并；poll 线程阻塞式 `cudaMemcpy`。
- 元数据缺自动再平衡：已有人工 Drain Job 和动态副本，但没有按 segment 压力/热度自动生成迁移计划；当前 `drain_jobs_` 仅见进程内 map，未见 OpLog/快照持久化，HA 切主后的 job 重建需单独设计（master_service.h:2964-3007）。

---

## 6. Gap 对比矩阵

| # | 能力 | yuanrong | Mooncake | Gap |
|---|------|----------|----------|-----|
| 1 | 端口健康监测 | epoch 围栏快照 + 验证式准入 + 远端传播 | 无（1s 强制重激活） | **P0** |
| 2 | 对端熔断器 | 8 退役触发、1s→30s 退避、HALF_OPEN 探针、代际围栏 | 无 | **P0** |
| 3 | 类型化背压错误码 | `K_URMA_TRY_AGAIN` 等 5+ 专用码 | 通用失败 | **P0** |
| 4 | 迟到完成对账 | 保留事件(5s/1024) + 孤儿记账(16/32) + 观察者分发 | batch busy 保活但无界；部分 staging 失败分支不收敛 | **P0** |
| 5 | TCP 降级治理 | 进程级字节限流(10MB/1MB) + 整 RPC 钉住 | UB 内无 fallback | **P0** |
| 6 | Jetty 生命周期 | PostGate 单字 CAS + FSM + fail-closed 隔离 | classic UB 为 volatile 深度表；generic RDMA 已原子化 | **P0** |
| 7 | 负载感知选路 | chip inflight 差反馈 + 亲和策略 | classic 随机/轮转；TENT 有统计与策略框架 | **P1** |
| 8 | Gather write | 13 SGE/WR + 链式提交 + badWr 解析 | 1 SGE | **P1** |
| 9 | 批量聚合 | 32MB 聚合 + 自适应平衡分裂 | 固定 slice | **P1** |
| 10 | 模糊失败缓冲延迟释放 | 128ms 延迟回池 | 无 | **P1** |
| 11 | 流水线窗口提交 | MSet 32 深窗口 | watermark flush（较粗） | **P1** |
| 12 | 元数据再平衡 | 内存/热度自动调度 + 限速 + 4 级退化梯子 | 人工 Drain + 动态副本，无自动压力/热度闭环 | **P1** |
| 13 | 故障确认 witness | 3 见证投票防分区误判 | 无（lease 到期即判） | **P2** |
| 14 | 分相延迟取证 | 9 时间戳 trace + 首块唤醒归因 | SpDiag（粒度较粗） | **P2** |
| 15 | 预热 | 进程内 + k8s 定向预热 playbook | 无 | **P2** |
| 16 | URMA mock + 故障注入 | 独立 mock 后端 + 数十个相关测试文件/数百用例 + 大量注入点 | 有 classic mock 冒烟测试和 TENT 原生 UB 测试；缺 P0 场景系统注入 | **P0 验收地基** |
| 17 | 连接池耗尽语义 | 池化 + refill + `K_URMA_TRY_AGAIN` | jfc/jetty 深度截断提交 | **P1** |
| 18 | 多设备并发 | 单设备 | 每 HCA context + 扇出 | **Mooncake 领先** |
| 19 | Master HA | etcd lease + witness/自杀围栏，侧重分区自保护 | 围栏选举 + OpLog + 快照，侧重状态复制/恢复 | **模型互补；Mooncake 可补分区确认** |
| 20 | 设备内存传输 | Pipeline H2D（复用 JFC） | CPU staging（有泄漏 bug） | **P1**（先修 bug） |

---

## 7. 值得 Mooncake 借鉴的能力与补齐建议

### 7.1 先借鉴不变量，再借鉴实现

整改时应锁定以下五个跨系统不变量，它们比“池大小 200”“延迟 128ms”更重要：

1. **确认事实单调**：端口健康、peer incarnation、leader epoch、对象 version 都只能被更新的证据推进，pending/unknown 不得解除隔离。
2. **完成事件有唯一所有者**：一个 WR 从准入到 CQE/flush/隔离必须恰好结算一次；业务超时不等于硬件已完成。
3. **复用前证明不可再写**：slice、staging block、jetty、remote key 在可能有迟到 DMA/CQE 时不得复用；超过有界窗口仍无法证明时 fail-closed 隔离，而不是猜测成功。
4. **故障域分层**：本地 device/port 健康、peer 可达性、单 endpoint/jetty 生命周期、局部资源背压是四类状态，不能让一次池耗尽污染整张网卡，也不能让一个坏 peer 拖垮所有 peer。
5. **降级不改变写语义**：READ 可在 version/checksum 校验下换副本；one-sided WRITE 的超时结果可能是“未写、已写或部分写”，没有幂等 token/事务围栏时不得盲目 TCP 重放。

建议先定义公共的类型化结果：`LOCAL_PORT_UNHEALTHY`、`PEER_UNREACHABLE`、`RESOURCE_BACKPRESSURE`、`AMBIGUOUS_COMPLETION`、`REMOTE_VERSION_STALE`。classic UB、generic RDMA、TENT 各自把 provider/CQE 映射到这些语义，Store 只消费公共结果，不依赖厂商 CQE 数字。

### P0：传输层可靠性地基（建议最先做）

#### G0. 先补故障注入与可重复测试

- yuanrong 有独立 UDS/memfd mock、数十个 URMA/UB 相关测试文件与数百个用例；Mooncake 并非“0 UT”：classic 有 `mock_urma.cpp` 和 `ub_transport_test.cpp` 的 2 个用例，TENT `ub_native_data_path_test.cpp` 还有 6 个用例。真实差距是 classic 测试未注册 ctest，且缺端口抖动、CQE 延迟/丢失、breaker、资源退役、shutdown-inflight 等系统故障注入。
- 先扩展 mock，使其能脚本化返回 async event、post 部分接受、指定 CQE 状态、CQE 延迟/乱序/永久丢失、flush/delete 失败；随后所有 G1-G5 改动必须先有失败测试再实现。

#### G1. 端口健康监测体系

- **借鉴点**：`UbPortHealthMonitor` 的三要素——不可变快照 + epoch 围栏 + `verificationPending` 中间态。
- **Mooncake 现状**：`doProcessContextEvents`（urma_endpoint.cpp:390-412）对 PORT_DOWN 只是置 inactive + 断连，monitor 每秒强制激活造成"激活→失败→再激活"振荡；`failed_nr_polls>32 && !success_nr_polls` 因累计计数器事实上一次性。
- **补齐建议**：
  1. 引入薄 `UrmaPortStatusProvider` 适配 `BONDP_USER_CTL_QUERY_PORT_STATUS`；Mooncake 的 `FindUrma.cmake` 已要求 `urma_api.h` 与 `urma_ubagg.h`，但目前只查找/链接 `liburma`，因此必须先做 **compile+link capability probe**，不能仅凭头文件存在宣称可用；
  2. 移除 monitor 的无条件 `set_active(true)`，改为"端口健康确认后激活"；
  3. 把 `success_nr_polls/failed_nr_polls` 改为窗口化（或时间戳化）计数，替代生命周期累计；
  4. 分端口的 GOOD/BAD 进入 endpoint 选择：`selectDevice` 过滤已确认全坏的 device；部分坏口仍由 bonding 分担；
  5. 以 `MC_UB_PORT_HEALTH_MODE=off|shadow|enforce` 灰度：shadow 只记录“若执行会过滤谁”，enforce 才改变路由。
- **风险/工作量**：中等改动、高兼容风险。需在构建节点实际核对 UMDK 头/库/固件是否同时支持 opcode、输出结构和 `urma_user_ctl`；provider 不支持时必须回退事件模式并显式暴露 UNKNOWN，不能假健康。

#### G2. 对端熔断器 + 类型化背压

- **借鉴点**：`PeerState` 的"退役归因"（只有对端可归因的失败计数）+ 指数退避 + HALF_OPEN 单探针 + `PrepareReplacement` 继承预算。
- **Mooncake 现状**：slice 重试 9 次后删 endpoint，下一个 slice 立即重建并重试——坏对端会持续消耗握手与重试资源。
- **补齐建议**：
  1. 在 `UbEndPoint` 或 context 层加 per-peer 熔断状态（OPEN/OPEN 时间戳/探针标志）；
  2. `redispatch`（ub_context.cpp:516-579）查熔断状态，OPEN 期内直接失败（可配快速失败错误码），到 HALF_OPEN 放一个探针 slice；
  3. 定义 `ERR_UB_PEER_BREAKER_OPEN`/`ERR_UB_BACKPRESSURE` 专用错误码，让 store 层能区分"该换副本了"和"该退避了"（Mooncake 已有 replica_selection，缺传输层信号）。

#### G3. 迟到完成对账（顺带修 staging 泄漏）

- **借鉴点**：保留超时事件（TTL 5s/上限 1024）+ 孤儿 WR 记账 + 迟到 CQE 观察者。
- **Mooncake 现状**：其一，部分 staged slice 在 worker 失败路径直接 `markFailed()` 不经 `onStagedSliceFinalFailure()`，`completed_slices` 永不到位，形成确定性租约泄漏；其二，已 post 的 slice 超时后 batch 仍保持 busy 等 CQE，避免立即回收，但没有上界和孤儿收敛策略。
- **补齐建议**：
  1. **先修 bug**：所有 staged slice 失败路径统一走 `onStagedSliceFinalFailure()`；
  2. 把 completion identity 从裸 `Slice* user_ctx` 升级为稳定的 completion record（至少包含 request/generation、资源引用和一次性结算位）；业务 task 可以先结束，record 在 CQE/flush/有界隔离后再销毁，避免缓存复用造成 ABA；
  3. slice 超时/最终失败时若 WR 可能仍在途，转入有界 orphan 账本；poll 侧迟到完成按 generation 对账，超过条目/字节上限则退役 jetty/context，绝不能仅删除记录后复用资源；
  4. 迟到 CQE 的 provider 状态由 adapter 映射为公共故障语义，再喂给 G1/G2；业务层不直接硬编码 CQE 4/9。

#### G4. TCP 降级治理

- **借鉴点**：进程级字节限流（在途 10MB/单载荷 1MB，CAS ticket RAII）+ worker 侧整 RPC 钉住降级（防每对象重试风暴）。
- **Mooncake 现状**：UB transport 无 TCP fallback；MultiTransport 的协议选择是**静态 per-segment 属性**而非失败驱动。
- **补齐建议**：
  1. 在公共路由层加失败驱动的 per-peer 协议降级缓存（UB breaker OPEN → 临时 TCP，带 TTL 与单探针）；不能只改 `MultiTransport::selectTransport` 后静默重放；
  2. 降级流量经字节限流器，防挤占 RPC/元数据通道（Mooncake 的元数据也走 TCP，更需要这个保护）；
  3. READ：切换到其它可读副本优先于 TCP，同对象 version/checksum 校验通过后才发布；
  4. WRITE：同步 post 失败（可证明未提交）可安全改道；已 post 后 TIMEOUT/未知 CQE 属 `AMBIGUOUS_COMPLETION`，必须交 Store 的 Put/复制事务按 version/token revoke 或重试，禁止传输层直接 TCP 重放；
  5. store 批量操作采用“整批钉住一个降级决策 + 总字节预算”，避免每 slice 独立降级形成放大。

#### G5. Jetty 生命周期 PostGate 化

- **借鉴点**：单原子字打包 closing/retire/finale 标志 + activePosts 计数，准入与退役同一线性化点；删除经 `modify(ERROR) → FLUSH_ERR_DONE → 异步 delete`。
- **Mooncake 现状**：classic UB 的 `wr_depth_list_` 是 `volatile int` 数组 + `__sync_fetch_and_add`；endpoint disconnect 时手动把残余深度回退到 `jfc_outstanding_`（urma_endpoint.cpp:1039-1049）——有对账意识，但依赖手工配对。generic RDMA 已原子化，不在此整改范围。
- **补齐建议**：短期将 classic UB 深度改成 `std::atomic<int>`，并加入同一原子线性化点上的关闭/在途计数；中期才引入 per-context jetty lane 池 + PostGate（见 G7）。完成 identity（G3）必须先于资源池化，否则退役/复用会放大裸指针 ABA 风险。

### P1：性能与公平性

#### G6. 负载感知选路（把 tent 的思想带进 classic 或直接推进 tent）

- Mooncake 已有可复用框架：TENT 暴露 `inflight_bytes` 与 `ewma_bandwidth_bps`，`TransportSelector` 支持配置化策略。**gap 是 classic UB 未接入该闭环，以及两条数据面没有统一的负载模型**。
- 最小落地：在 classic UB 的 `Topology::selectDevice` 加 per-device **inflight bytes**（不是只看 WR 个数）与 EWMA 完成带宽，比较 `predicted_finish = inflight_bytes / ewma_bw`；yuanrong 的 “inflight WR 差 > 15” 仅可作为初始 shadow 对照，不应直接成为多 HCA 阈值。
- 防止反馈振荡：健康过滤先于负载评分，保留低频 probe 防冷门 NIC 永久饿死；指标按 device/peer 分层，禁止一个坏 peer 的拥塞把整个本地 HCA 判坏。

#### G7. Jetty 池化 + per-peer 公平性

- 借鉴 `SendJettyPool` 的**有界容量、后台补充、退役余量和 per-peer 配额**，而不是复制 200/50ms/8 三个常量。
- Mooncake 每 HCA 有独立 context，建议做 **per-context 池 + 全局/peer 双层配额**；lane 绑定 `(local_device, peer_nic, peer_incarnation, generation)`。若做全进程池，会破坏多设备亲和并放大单 context 故障。
- 初始容量由 `并发 peer × 每 peer 上限 × 活跃 HCA` 压测反推，并设置退役 lane/孤儿 WR/被隔离字节的硬上限与降级策略。

#### G8. 多 SGE gather + 聚合批处理

- Mooncake jfs 配置 `max_sge=5`（urma_endpoint.cpp:790）未用满（现在 num_sge=1）。同 endpoint 的同方向连续小 slice 可合并为多 SGE WR 链，一次 post。
- store 的 batch_get 多副本扇出（`TransferSubmitter::submit_batch`）是天然受益者。
- 批量聚合（yuanrong 的 32MB 聚合缓冲 + 平衡二分自适应）对 Mooncake 的 `batch_get_into` 同样适用：分配失败时棘轮降上限 + 二分，而不是整批失败。

#### G9. 模糊失败缓冲延迟释放

- staging 池 `releaseStaging` 直接回 free list（ub_transport.cpp:235-239）。若 slice 曾提交且结果不确定，应进入 quarantine，防迟到网络写污染复用块。与 G3 配套。
- **不能固定复制 128ms**。安全释放条件应是“该 generation 的所有 WR 已 CQE/flush 对账”，时间只是兜底。可定义 `deadline = max(last_post + max_completion_uncertainty, task_timeout)`，其中窗口由 URMA `err_timeout`、重试、poll 调度和实机故障注入 p99.999 推导。
- quarantine 必须按**字节**设上限并暴露水位；压力过高时停止新 staging/触发受控降级，不能提前复用。超过 deadline 仍无法证明安全时退役相关 jetty/context，并在销毁/flush 确认后释放。

#### G10. 元数据主动再平衡

- Mooncake master 已有 `RankedAllocationStrategy`、动态副本、`CreateCopyTask` / `CreateMoveTask` 和人工 Drain Job，但**没有自动存量再平衡**。借鉴 yuanrong：周期读取 segment 容量/热度报告 → 生成候选 → 每批重新读取目标余量 → 用现有 MoveTask 执行 → 限速与冷却。
- 自动调度不能只是给 Drain Job 加定时器。候选必须同时满足：租户 quota、hard-pin/soft-pin TTL、client liveness/offboarding、对象 lease、动态复制 lease/version、已有 replication task、目标故障域、内存/SSD tier、目标预留容量和对象当前可读副本数。
- **逐源副本保护**：supercache-snapshot 已明确让每个 SSD 下刷队列记录独立的受保护 `OffloadingTask`，且只有所有镜像任务尚未被领取时才允许整体取消（master_service.h:2227-2239）。自动迁移也要为每个 source 独立保持保护，只有对应目标副本首次可读且 version 匹配后才能释放该 source，不能退化为任务级单一保护。恢复路径已有 `RefreshRecoveredSoftPin()`，会复用普通 soft-pin 索引/指标并重新计算 TTL（master_service.h:1492-1505；master_service.cpp:1408-1410），应继续保留。
- 自动 job 必须可恢复：当前 `drain_jobs_` 只见进程内 map。调度意图要么进入 OpLog/快照并带 leader epoch，要么切主后可从 DRAINING segment + 未完成 MoveTask 幂等重建；重复调度以 `(tenant,key,source,target,object_version)` 去重。
- 阈值 `源≥80%/差≥20%/40MB/s` 只作初始配置，不是硬编码。先 shadow 产出“拟迁移计划”，用实际容量、请求热度、SSD 下刷和动态副本流量标定。
- **注意**：不搬哈希环或 witness 所绑定的分布式 master 模型，只搬“调度器 + 反馈 + 围栏 + 限速 + 可恢复意图”。

#### G11. 设备内存传输路径重构

- 先修 staging 三类风险：终态不收敛（G3）、析构时先注销/释放 staging 再清理 context/worker 的顺序（ub_transport.cpp:73-85；UrmaContext 析构到 `worker_pool_.reset()` 才 join，urma_endpoint.cpp:84-93）、poll 完成路径同步执行 H2D `cudaMemcpy`（ub_transport.cpp:336-363）。再评估 yuanrong 的 Pipeline H2D 思路：**复用现有 JFC/CQE 通道**做网络写与 H2D 的流水化，而不是独立池 + 全量屏障。

### P2：工程化与可运维性

#### G12. witness 故障确认

- Mooncake master 判 worker 死亡靠心跳到期。借鉴 3 见证投票（Mooncake 的 worker 相互可见，master 可委托 3 个 worker 探测疑似死亡目标），防网络分区误驱逐。优先级低于 yuanrong（其分布式 master 误判代价更高），但大规模集群值得。

#### G13. 分相延迟取证深化

- SpDiag 已有 perf point 骨架。补 yuanrong 式的"唤醒链取证"：slice 完成的 poll→notify→worker 观察三段时间戳 + 首块唤醒归因，直接定位 `URMA_QUEUE_DEPTH`/调度抖动类问题。

#### G14. 预热 playbook

- k8s 部署后 worker↔worker 定向握手预热（Mooncake handshake ~RPC + jetty import，同样有首个请求慢问题）。可做成 master 触发的预热任务或 `mooncake_master` 侧工具。

---

## 8. 不建议照搬的部分

| yuanrong 设计 | 原因 |
|---------------|------|
| 单设备单上下文（bonding_dev_0） | Mooncake 的每 HCA context + 切片扇出多网卡聚合是其 README 核心卖点，架构上更优 |
| bthread 协程绑定 | Mooncake RPC 栈是 coro_rpc，引入 bthread 无必要；但"等待不钉线程"的思想在 coro_rpc 中已有等价物 |
| 哈希环分布式 master | Mooncake 的集中式 master、OpLog/快照和副本任务模型不同；应补调度闭环，而不是替换元数据所有权模型 |
| 40-bit request-id 的具体编码 | 位宽和布局不必复制，但 **generation/代际围栏本身必须保留**；仅靠裸 `Slice*` 与深度指针不足以防缓存复用 ABA |
| 固定 128ms 延迟、池 200、per-peer 8、差值 15 | 都是 yuanrong 在特定硬件/负载下的参数；Mooncake 应由不变量、资源预算和压测推导 |
| CQE 4/9 直接写进业务状态机 | 数值属于 provider ABI；adapter 应先翻译为公共故障语义，并为未知值 fail-safe |
| 3 witness 直接用于所有故障 | yuanrong 的分布式 master 误判代价和 Mooncake 不同；只在跨故障域且 master 无法区分自身分区时启用 |
| 事件模式（一次一 CR） | 吞吐受限，yuanrong 自己也默认关闭 |

---

## 9. 面向 supercache 的目标设计

### 9.1 传输可靠性控制闭环

建议形成如下单向状态传播，禁止下层错误被字符串化后丢失：

```
URMA provider / async event / CQE
        │  adapter：ABI 解码、device/peer 归因
        ▼
HealthSnapshot(device, epoch, pending) ──► DeviceAdmission
        │                                      │
        └──────── peer evidence ───────────────┤
                                               ▼
                                  PeerBreaker(peer, incarnation)
                                               │
                     ┌─────────────────────────┼────────────────────┐
                     ▼                         ▼                    ▼
                  RETRY_LOCAL             RETRY_REPLICA      FALLBACK_TCP
                     │                         │                    │
                     └──────────── typed result + budget ──────────┘
                                               ▼
                                  CompletionLedger(generation)
                                               ▼
                               CQE/flush 对账后释放 slice/staging/lane
```

关键数据结构建议：

- `DeviceHealthSnapshot`：`device_id, ports, health_epoch, verification_pending, observed_at, provider_status`；只发布不可变快照。
- `PeerAdmissionKey`：`peer_segment + peer_nic + incarnation`，避免 peer 重启后旧 breaker/健康事实污染新实例。
- `CompletionRecord`：`request_id, generation, operation, posted_at, resource_refs, terminal_once`；URMA `user_ctx` 指向 record，不再直接指向可复用 Slice。
- `QuarantineLedger`：同时限制 orphan **条目数与字节数**；记录何时因 CQE、flush、context destroy 收敛。
- `FallbackBudget`：按进程、peer、tenant 三层记账，控制降级对元数据 RPC 与其它租户的干扰。

### 9.2 supercache 元数据再平衡闭环

```
segment 容量/热度/SSD 压力报告
        ▼
shadow planner（只产计划、不执行）
        ▼  通过 quota/pin/lease/version/故障域/可读副本过滤
durable rebalance intent + leader epoch
        ▼
CreateMoveTask(source replica -> target)
        ▼
目标副本首次可读且 version 匹配
        ▼
按 source 释放保护；必要时重新授予 snapshot soft-pin TTL
        ▼
下一批读取新容量反馈，执行限速/冷却/暂停
```

这里必须增加三个 yuanrong 原设计之外、但 supercache 必需的约束：

1. **存储层级感知**：memory、SSD、远端恢复不是等价目标，planner 要分别计算容量和迁移成本；SSD 下刷与 rebalance 共用带宽预算。
2. **逐源副本状态**：一个对象可同时迁移多个 source，每个 source 独立保护、完成和撤销，不能用任务级单一布尔值代表全部源。
3. **HA 可重建**：leader 切换后同一意图必须幂等继续或安全撤销；新 leader 不能因 job map 丢失而留下永久 DRAINING segment 或永久 soft-pin。

### 9.3 可观测性最小集合

| 类别 | 必须暴露的指标/事件 |
|------|--------------------|
| 端口健康 | 每 device 的 epoch、GOOD/BAD/UNKNOWN 数、pending 时长、状态转换原因、provider 查询失败 |
| 熔断/降级 | breaker open/half-open/close、按原因计数、fallback bytes、限流/拒绝 bytes、探针结果 |
| 完成生命周期 | outstanding WR、late CQE、orphan WR/bytes、quarantine bytes/age、generation mismatch、强制退役 |
| lane/设备负载 | lane pool 可用/在途/退役、per-peer 配额等待、per-device inflight bytes、EWMA 带宽与预测完成时间 |
| 再平衡 | shadow 候选/过滤原因、迁移 bytes、限速等待、重试/撤销、逐源保护数、leader 恢复重建数 |

日志需携带统一关联键：`trace_id + request_id/generation + peer_incarnation + device + jetty/lane + tenant/key hash + object_version`，否则跨层对账仍只能靠时间猜测。

---

## 10. 落地路线建议

```
阶段 0：建立可证伪基线
 ├─ G0  classic UB 测试注册 ctest；扩展 mock 故障脚本
 ├─ 记录现网端口事件、CQE、超时、staging 水位和设备负载基线
 └─ 构建节点执行 UMDK port-status compile/link/runtime capability probe

阶段 1：修复确定性正确性缺陷
 ├─ G3a 统一 staged slice 终态收敛；修正 shutdown/inflight teardown 顺序
 ├─ 引入公共类型化错误与 completion generation
 └─ 每项独立小 PR，先失败用例、后实现

阶段 2：形成可靠性闭环
 ├─ G1  端口健康 shadow→enforce，移除 1s 无条件激活
 ├─ G2  per-peer breaker 与 probe
 ├─ G3b completion ledger/orphan 对账 + G9 quarantine
 └─ G4  语义安全的 replica/TCP 降级与三层预算

阶段 3：公平性与性能
 ├─ G5+G7 per-context lane 池、PostGate、per-peer 配额
 ├─ G6  inflight-bytes/EWMA 反馈选路，评估推进 TENT 为默认
 └─ G8  多 SGE gather + 批量聚合自适应分裂

阶段 4：supercache 自动再平衡
 ├─ G10 shadow planner 与过滤原因指标
 ├─ 持久化 intent、leader epoch、逐源副本保护与软 pin 语义
 └─ 小比例 enforce，验证 SSD/动态副本/客户 offboarding 并发场景
```

不要把多个阶段打进一个大 PR。推荐的依赖顺序是：**测试注入 → 类型化错误 → completion identity/所有权 → 健康/熔断 → 降级 → 池化 → 性能 → 自动再平衡**。其中 G3 是 G5/G7/G9 的前置，G1/G2 是 G4 的前置，durable intent 是 G10 enforce 的前置。

---

## 11. 验收矩阵（整改完成定义）

| 故障注入场景 | 必须保持的不变量 | 通过标准 |
|--------------|------------------|----------|
| 单端口 flap，GOOD/BAD 交替 | pending/旧 epoch 不解除隔离 | 无 1s 强制复活振荡；新 epoch 确认后才恢复 |
| 本地全部端口 DOWN | 只隔离本地 device，不污染 peer | 新 UB post 被阻止；其它 HCA 继续；状态和原因可观测 |
| 单 peer 宕机/重启 | 其它 peer 不受影响；旧 incarnation 事实无效 | breaker 有界打开；单 probe；重启后新代恢复 |
| post 部分接受、CQE 乱序 | 每个 WR 恰好结算一次 | 深度、task、lane、staging 计数最终归零，无负数/双完成 |
| TIMEOUT 后迟到 CQE | 业务终态与资源终态分离 | 调用方按期返回；late CQE 命中原 generation；复用内存不被污染 |
| CQE 永久丢失 | 资源占用有上限 | 达上限后停止准入/退役 context；无无限 batch/staging 增长 |
| shutdown 时有在途 WR | 注销内存晚于 poll/flush 收敛 | ASan/TSan 无 UAF；没有 CQE 访问已释放 staging/endpoint |
| UB WRITE 模糊失败 | 不盲目重放 | 返回 `AMBIGUOUS_COMPLETION`，由对象事务 version/token 裁决 |
| TCP fallback 风暴 | 降级不能饿死元数据和其它租户 | 进程/peer/tenant 预算生效；拒绝原因可观测；无 RPC 线程池耗尽 |
| 自动 rebalance 与 hard/soft pin 并发 | pin/lease/version 不被绕过 | 被过滤对象不迁移；逐 source 保护恰好释放；切主后可重建 |

除正确性外，还要建立性能守门：无故障时 classic UB 吞吐/CPU 回退不超过约定预算；故障时恢复时间、fallback 峰值、quarantine 峰值和受影响 peer 范围必须有 SLO。具体数值应由阶段 0 基线决定，不在分析文档中拍定。

---

## 12. 实施前必须人工裁决/实机验证的决策门

1. **UMDK ABI**：远程构建环境的 `urma_api.h`、`urma_ubagg.h`、`liburma` 是否同版本，是否定义并实现 `BONDP_USER_CTL_QUERY_PORT_STATUS`；输出结构在当前固件上是否一致。
2. **主数据面方向**：supercache 未来 1-2 个版本继续以 classic UB 为主，还是把 TENT 升为默认；该决定影响 G5-G8 应落在哪条路径。
3. **WRITE 降级协议**：Store 是否已有足够的 object version / lease / transaction token 来裁决模糊完成；若没有，G4 只能先支持 READ 降级和“可证明未 post”的 WRITE。
4. **HA job 语义**：rebalance intent 写入 OpLog/快照，还是由 segment/task 状态重建；必须选一种作为唯一真相源。
5. **参数标定**：completion uncertainty、orphan/quarantine 上限、breaker 退避、lane 配额和迁移速率均需在 `work@mooncake-dev` 实机故障注入后确定。

---

## 附录 A：yuanrong 关键网络参数速查

| 参数 | 默认 | 说明 |
|------|------|------|
| `urma_send_jetty_lane_pool_size` | 200 | send jetty 池目标 |
| `urma_send_jetty_lane_refill_extra_size` | 200 | 退役余量上限 |
| `urma_max_write_size_mb` | 4 | 单写分块上限（平衡分裂） |
| `urma_poll_size` | 8 | 每次 poll CR 数（设备上限 16） |
| `urma_event_mode` | false | 事件 vs 轮询模式 |
| `enable_transport_fallback` | true | UB→TCP 降级总开关 |
| `urma_failover_success_rate_ratio` | 0.5 | worker 切换成功率阈值 |
| `ub_numa_inflight_wr_diff_threshold` | 15 | chip inflight 差反馈阈值 |
| `ub_transport_arena_num` | 4 | 客户端传输池 arena 数 |
| `urma_register_whole_arena` | true | 整池单注册 |
| 硬编码：`MAX_INFLIGHT_JETTIES` / `MAX_RETIRED_JETTIES` | 8/8 | per-peer 上限/熔断阈值 |
| 硬编码：孤儿 WR 告警/退役 | 16/32 | 超时 lane 对账 |
| 硬编码：保留事件 TTL/上限 | 5s/1024 | 迟到完成窗口 |
| 硬编码：TCP 限流在途/单载荷 | 10MB/1MB | 降级治理 |
| 硬编码：端口健康轮询 | 1s | 仅 pending 或有坏口时查询 |

## 附录 B：Mooncake 当前 UB 关键参数速查

| 参数 | 默认 | 说明 |
|------|------|------|
| `MC_URMA_TRANS_MODE` | RM | RM/RC/UM |
| `MC_URMA_BONDING_MULTIPATH_ENABLE` | false | bondp BALANCE/IODIE |
| `MC_UB_NUMA_AFFINITY_ENABLE` | false | per-WR chip 固定 |
| `MC_UB_TRANSPORT_CPU_STAGING` | true | GPU 缓冲 CPU 中转 |
| `MC_UB_STAGING_POOL_SIZE` | 1GiB | staging 池 |
| `MC_RETRY_CNT` | 9 | slice 重试 |
| `MC_MAX_WR` | 256 | 批提交上限 |
| `num_jetty_per_ep` | 1 | 每 endpoint jetty 数 |
| JFC 数/context | 2 | 共享完成队列 |
| `MC_HANDSHAKE_WORKER_THREADS` | 16 | 握手监听线程 |
| `max_ep_per_ctx` | 65536 | SIEVE endpoint 上限 |

---

*分析生成于 2026-09-12，基于两仓库当日 HEAD。源码引用行号随分支演进可能漂移。*
