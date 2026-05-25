# SSD负载均衡验证测试指南

## 前置条件

### 1. 启动Master

```bash
# 方式一：命令行参数
./mooncake_master \
    --allocation_strategy=ssd_balance \
    --ssd_high_watermark_ratio=0.90 \
    --master_server=0.0.0.0:50051 \
    --metadata_server=127.0.0.1:2379

# 方式二：配置文件
./mooncake_master \
    --allocation_strategy=ssd_balance \
    --ssd_high_watermark_ratio=0.90 \
    --config=master_config.json
```

### 2. 启动多个Client节点

每个节点需要启动Mooncake client并挂载segment，同时配置本地SSD目录：

```bash
# 节点A (SSD容量大)
export PROTOCOL=tcp
export DEVICE_NAME=eth0
export LOCAL_HOSTNAME=node_a
export MC_METADATA_SERVER=127.0.0.1:2379
export MASTER_SERVER=127.0.0.1:50051

# 节点B (SSD容量小)
# 同上，修改LOCAL_HOSTNAME=node_b
```

### 3. 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `PROTOCOL` | `tcp` | 传输协议 |
| `DEVICE_NAME` | `eth0` | 网卡名 |
| `LOCAL_HOSTNAME` | `localhost` | 本机主机名 |
| `MC_METADATA_SERVER` | `127.0.0.1:2379` | 元数据服务器地址 |
| `MASTER_SERVER` | `127.0.0.1:50051` | Master地址 |

---

## 运行全部测试

```bash
cd tests
python ssd_balance_verify.py --master 127.0.0.1:50051 --num-keys 100
```

---

## 单独运行每个测试

### Test 1: SSD负载均衡验证

验证数据按SSD空闲比例分布到不同节点。

```bash
python tests/ssd_balance_verify.py --master 127.0.0.1:50051 --test balance --num-keys 200 --value-size 4096
```

**预期行为：**
- 向集群写入200个key
- SSD空闲的节点应该分配到更多数据
- 所有写入的key都能成功读回

**通过条件：** 所有写入的key都能成功读回，无数据丢失。

**需要的环境：** 多节点集群，各节点SSD使用率不同。

---

### Test 2: SSD驱逐保护验证

验证SSD达到高水位时禁止写入但不驱逐已有数据。

```bash
python tests/ssd_balance_verify.py --master 127.0.0.1:50051 --test eviction --num-keys 300 --value-size 16384
```

**预期行为：**
1. 先写入一批初始对象（约20个）
2. 等待offloading完成（15秒）
3. 继续写入压力对象（300个），试图填满SSD
4. 验证初始对象仍然存在（未被驱逐）

**通过条件：** 初始写入的20个对象全部可读，无数据丢失。

**需要的环境：** 至少一个节点有SSD offload功能。

---

### Test 3: DDR准入控制验证

验证DDR达到驱逐水位时临时禁止写入，释放空间后自动恢复。

```bash
python tests/ssd_balance_verify.py --master 127.0.0.1:50051 --test ddr --num-keys 100 --value-size 67108864
```

**预期行为：**
1. 写入大对象（每个64MB）直到DDR满
2. DDR满后写入失败（返回NO_AVAILABLE_HANDLE）
3. 删除部分对象释放DDR空间
4. 写入恢复成功

**通过条件：** DDR满时写入被阻止，释放空间后写入恢复。

**需要的环境：** 单节点或集群，DDR容量有限。

---

### Test 4: 全节点SSD满验证

验证所有节点SSD都满时写入暂停，释放后恢复。

```bash
python tests/ssd_balance_verify.py --master 127.0.0.1:50051 --test allfull --num-keys 50 --value-size 16777216
```

**预期行为：**
1. 写入大量数据（每个16MB）填满所有节点SSD
2. 等待offloading（20秒）
3. SSD满后新写入失败
4. 删除部分数据释放SSD
5. 写入恢复

**通过条件：** SSD满时写入被阻止，释放后写入恢复。

**需要的环境：** 多节点集群，所有节点有SSD。

---

## 自定义参数

```bash
python tests/ssd_balance_verify.py \
    --master 192.168.1.100:50051 \  # Master地址
    --num-keys 500 \                 # 每个测试写入的key数量
    --value-size 8192 \              # 每个value的大小（字节）
    --test balance                   # 指定运行的测试
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--master` | 环境变量`MASTER_SERVER` | Master地址 |
| `--num-keys` | 100 | 每个测试写入的key数量 |
| `--value-size` | 4096 | 每个value的字节大小 |
| `--test` | `all` | 指定测试：`balance`/`eviction`/`ddr`/`allfull`/`all` |

## 输出示例

```
======================================================================
SSD Balance Allocation Strategy Verification
======================================================================
Config: num_keys=100, value_size=4096

==================================================
Test 1: SSD Load Balancing
==================================================
  Writing 100 objects (4096 bytes each)...
  Results: 100 success, 0 failure
  Read verification: 50/50 objects readable

  Result: [PASS] SSD Load Balancing

==================================================
Test 2: SSD Eviction Protection
==================================================
  ...

======================================================================
Summary
======================================================================
  [PASS] Test 1: SSD Load Balancing
  [PASS] Test 2: SSD Eviction Protection
  [PASS] Test 3: DDR Admission Control
  [PASS] Test 4: All Nodes SSD Full

Total: 4 passed, 0 failed out of 4
```
