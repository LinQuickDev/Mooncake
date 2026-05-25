# SSD负载均衡验证测试指南

## 前置条件

- 编译完成 mooncake_store（含 `mooncake_master` 可执行文件）
- 编译完成 mooncake-wheel（含 Python `mooncake.store` 模块）
- 安装 Python 3

## 验证脚本

验证脚本位于 `mooncake-wheel/tests/verify_ssd_balance.py`，支持 4 个测试场景：

```
python verify_ssd_balance.py --test <test_name>
```

| test_name | 验证内容 |
|-----------|---------|
| `load_balancing` | **基础测试**：2个Client不对称SSD，验证数据按SSD空闲比例分布 |
| `ssd_eviction_protection` | SSD满时禁止写入，已有数据不被驱逐 |
| `ddr_admission` | DDR满时临时禁止写入，释放空间后自动恢复 |
| `all_ssd_full` | 所有节点SSD满后全局拒绝，释放后恢复 |

## 默认规模

DDR=4GB, SSD=16GB, Key=4MB

## 注意事项

- **每次测试前清空SSD目录**：`rm -rf <SSD_PATH> && mkdir -p <SSD_PATH>`
- **MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS必须设为1**：默认10s会导致offload延迟过长
- **put()不抛异常**：`store.put()` 返回整数状态码（0=成功，非0=失败）
- **每次插入间等0.01s**：避免写入过快导致问题

---

## 验证 1：SSD负载均衡（应首先运行）

两个Client使用不同的SSD容量：
- Client 1（写入端）：DDR=4GB, SSD=8GB → effective=4GB
- Client 2：DDR=4GB, SSD=16GB → effective=12GB

写入约1200个4MB key（4.8GB），超过Client 1水位后自动溢出到Client 2。

**Terminal 1** — 启动Master：

```bash
mooncake_master \
    --port=50053 \
    --http_metadata_server_port=8880 \
    --enable_http_metadata_server=true \
    --metrics_port=9104 \
    --allocation_strategy=ssd_balance \
    --ssd_high_watermark_ratio=0.90 \
    --enable_offload=true \
    --default_kv_lease_ttl=2000
```

**Terminal 2** — 运行验证脚本：

```bash
MC_METADATA_SERVER=http://127.0.0.1:8880/metadata \
MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=1 \
MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/tmp/mooncake_ssd_balance_lb \
python mooncake-wheel/tests/verify_ssd_balance.py --test load_balancing
```

### 预期观察

- Client 1写入约1200个key
- 等待60s offload后，两个SSD目录均有文件
- **Client 2的SSD数据量 > Client 1的SSD数据量**（溢出行为正常）

### 判断标准

- 两个SSD目录文件大小均 > 0
- Client 2 SSD > Client 1 SSD（free-ratio-first分配策略有效）

---

## 验证 2：SSD驱逐保护

写入数据触发offload，然后继续写入填满SSD到高水位。
验证已有数据不被驱逐。

**Terminal 1** — 启动Master：

```bash
mooncake_master \
    --port=50053 \
    --http_metadata_server_port=8880 \
    --enable_http_metadata_server=true \
    --metrics_port=9104 \
    --allocation_strategy=ssd_balance \
    --ssd_high_watermark_ratio=0.90 \
    --enable_offload=true \
    --default_kv_lease_ttl=2000
```

**Terminal 2** — 运行验证脚本：

```bash
MC_METADATA_SERVER=http://127.0.0.1:8880/metadata \
MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES=17179869184 \
MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=1 \
MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/tmp/mooncake_ssd_balance_evict \
python mooncake-wheel/tests/verify_ssd_balance.py --test ssd_eviction_protection
```

### 预期观察

- 先写入20个初始key（80MB），等待20s offload
- 继续写入压力key直到SSD满或DDR满
- 初始key全部可读（未被驱逐）

### 判断标准

- 初始写入的20个key全部可读，0个丢失

---

## 验证 3：DDR准入控制

写入大对象填满DDR，验证DDR满时写入被临时禁止，释放后恢复。

**Terminal 1** — 启动Master：

```bash
mooncake_master \
    --port=50053 \
    --http_metadata_server_port=8880 \
    --enable_http_metadata_server=true \
    --metrics_port=9104 \
    --allocation_strategy=ssd_balance \
    --ssd_high_watermark_ratio=0.90 \
    --enable_offload=true \
    --default_kv_lease_ttl=2000
```

**Terminal 2** — 运行验证脚本：

```bash
MC_METADATA_SERVER=http://127.0.0.1:8880/metadata \
MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=1 \
MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/tmp/mooncake_ssd_balance_ddr \
python mooncake-wheel/tests/verify_ssd_balance.py --test ddr_admission
```

### 预期观察

- 写入64MB大对象直到DDR满
- DDR满后新写入失败（`NO_AVAILABLE_HANDLE`）
- 删除部分对象后写入恢复

### 判断标准

- DDR满时写入被阻止
- 释放空间后写入恢复成功

---

## 验证 4：全节点SSD满

写入大量数据填满所有节点SSD，验证全局拒绝后释放可恢复。

**Terminal 1** — 启动Master：

```bash
mooncake_master \
    --port=50053 \
    --http_metadata_server_port=8880 \
    --enable_http_metadata_server=true \
    --metrics_port=9104 \
    --allocation_strategy=ssd_balance \
    --ssd_high_watermark_ratio=0.90 \
    --enable_offload=true \
    --default_kv_lease_ttl=2000
```

**Terminal 2** — 运行验证脚本：

```bash
MC_METADATA_SERVER=http://127.0.0.1:8880/metadata \
MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES=17179869184 \
MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=1 \
MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/tmp/mooncake_ssd_balance_allfull \
python mooncake-wheel/tests/verify_ssd_balance.py --test all_ssd_full
```

### 预期观察

- 写入大量16MB对象填满SSD
- SSD满后新写入失败
- 删除部分数据后写入恢复

### 判断标准

- SSD满时写入被阻止
- 释放空间后写入恢复

---

## 关键环境变量

| 变量 | 值 | 说明 |
|------|----|------|
| `MC_METADATA_SERVER` | `http://127.0.0.1:8880/metadata` | HTTP元数据服务器地址 |
| `MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES` | `17179869184` | SSD容量16GB |
| `MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS` | `1` | **必须设为1**，默认10s太慢 |
| `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` | 测试专用目录 | 每次测试前清空 |

## 判断标准

| 日志关键词 | 含义 |
|-----------|------|
| `DDR overflow protection: rejecting allocation` | DDR准入控制生效 |
| `client_service.cpp ... NO_AVAILABLE_HANDLE` | Client端收到水位拒绝（预期行为） |
