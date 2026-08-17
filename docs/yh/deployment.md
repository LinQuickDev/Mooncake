



# POC项目Mooncake部署文档

## 1 ETCD环境搭建
### 1.1 安装

```bash
wget --no-check-certificate https://github.com/etcd-io/etcd/releases/download/v3.5.9/etcd-v3.5.9-linux-arm64.tar.gz
tar -xzvf etcd-v3.5.9-linux-arm64.tar.gz
rm etcd-v3.5.9-linux-arm64.tar.gz
mv etcd-v3.5.9-linux-arm64/ etcd
cd etcd/
cp etcd etcdctl /usr/local/bin/
```

### 1.2 配置

#### 1.2.1 方法1，一同创建（推荐）

##### 1.2.1.1 设置system脚本

```bash
vim /etc/systemd/system/etcd.service
```

`ETCD_NAME,	ETCD_INITIAL_ADVERTISE_PEER_URLS,	ETCD_ADVERTISE_CLIENT_URLS`根据本机实际配置
`ETCD_INITIAL_CLUSTER`配置集群全部节点的地址

```
[Unit]
Description=etcd key-value store
After=network.target

[Service]
Type=notify
Environment=ETCD_NAME=<nodename1>
Environment=ETCD_DATA_DIR=/var/lib/etcd
Environment=ETCD_LISTEN_PEER_URLS=http://0.0.0.0:2380
Environment=ETCD_LISTEN_CLIENT_URLS=http://0.0.0.0:2379
Environment=ETCD_INITIAL_ADVERTISE_PEER_URLS=http://<node1 IP>:2380
Environment=ETCD_ADVERTISE_CLIENT_URLS=http://<node1 IP>:2379
Environment=ETCD_INITIAL_CLUSTER="<nodename1>=http://<node1 IP>:2380,<nodename2>=http://<node2 IP>:2380"
Environment=ETCD_INITIAL_CLUSTER_STATE="new"
Environment=ETCD_INITIAL_CLUSTER_TOKEN="etcd-cluster"
ExecStart=/usr/local/bin/etcd
Restart=on-failure
RestartSec=5
User=root

[Install]
WantedBy=multi-user.target
```

##### 1.2.1.2 清除缓存，应用配置，启动etcd服务

为所有节点配置完成后，启动全部服务

```bash
systemctl disable etcd
systemctl stop etcd
rm -rf /var/lib/etcd
systemctl daemon-reload
systemctl start etcd
systemctl enable etcd
```

##### 1.2.1.3 查看集群

```bash
etcdctl --endpoints=http://<node1 IP>:2379 member list --write-out=table
```

#### 1.2.2 方法2，逐个创建

##### 1.2.2.1 设置system脚本

```bash
vim /etc/systemd/system/etcd.service
```

主机 （ip换为本机ip）

```
[Unit]
Description=etcd key-value store
After=network.target

[Service]
Type=notify
Environment=ETCD_NAME=<nodename1>
Environment=ETCD_DATA_DIR=/var/lib/etcd
Environment=ETCD_LISTEN_PEER_URLS=http://0.0.0.0:2380
Environment=ETCD_LISTEN_CLIENT_URLS=http://0.0.0.0:2379
Environment=ETCD_INITIAL_ADVERTISE_PEER_URLS=http://<node1 IP>:2380
Environment=ETCD_ADVERTISE_CLIENT_URLS=http://<node1 IP>:2379
Environment=ETCD_INITIAL_CLUSTER="<nodename1>=http://<node1 IP>:2380"
Environment=ETCD_INITIAL_CLUSTER_STATE="new"
Environment=ETCD_INITIAL_CLUSTER_TOKEN="etcd-cluster"
ExecStart=/usr/local/bin/etcd
Restart=on-failure
RestartSec=5
User=root

[Install]
WantedBy=multi-user.target
```

##### 1.2.2.2 清除缓存，应用配置，启动etcd服务

```bash
systemctl stop etcd
rm -rf /var/lib/etcd
systemctl daemon-reload
systemctl start etcd
```

##### 1.2.2.3 添加节点

```bash
etcdctl --endpoints=http://<node1 IP>:2379 member add <nodename2> --peer-urls=http://<node2 IP>:2380
```

有如下打印则为添加成功

```
[root@slot1 system]# etcdctl --endpoints=http://<node1 IP>:2379 member add <nodename2> --peer-urls=http://<node2 IP>:2380
Member 6a5201d77502faa1 added to cluster 592c5fccdb3ab88c

ETCD_NAME="<nodename2>"
ETCD_INITIAL_CLUSTER="<nodename2>=http://<node2 IP>:2380,<nodename1>=http://<node1 IP>:2380"
ETCD_INITIAL_ADVERTISE_PEER_URLS="http://<node2 IP>:2380"
ETCD_INITIAL_CLUSTER_STATE="existing"
```

##### 1.2.2.4 将打印的部分内容复制到新节点etcd的配置文件

例：<nodename2>的/etc/systemd/system/etcd.service配置文件

```
[Unit]
Description=etcd key-value store
After=network.target

[Service]
Type=notify
Environment=ETCD_NAME=<nodename2>
Environment=ETCD_DATA_DIR=/var/lib/etcd
Environment=ETCD_LISTEN_PEER_URLS=http://0.0.0.0:2380
Environment=ETCD_LISTEN_CLIENT_URLS=http://0.0.0.0:2379
Environment=ETCD_INITIAL_ADVERTISE_PEER_URLS=http://<node2 IP>:2380
Environment=ETCD_ADVERTISE_CLIENT_URLS=http://<node2 IP>:2379
Environment=ETCD_INITIAL_CLUSTER="<nodename2>=http://<node2 IP>:2380,node182=http://<node1 IP>:2380"
Environment=ETCD_INITIAL_CLUSTER_STATE="existing"
Environment=ETCD_INITIAL_CLUSTER_TOKEN="etcd-cluster"
ExecStart=/usr/local/bin/etcd
Restart=on-failure
RestartSec=5
User=root


[Install]
WantedBy=multi-user.target
```

##### 1.2.2.5 在新节点的机器上启动服务

```bash
systemctl stop etcd
rm -rf /var/lib/etcd
systemctl daemon-reload
systemctl start etcd
```

##### 1.2.2.6 查看集群

```bash
etcdctl --endpoints=http://<node1 IP>:2379 member list --write-out=table
```

##### 1.2.2.7 重复3-6步骤添加新节点进集群

### 1.3 其他

#### 1.3.1 删除节点

查看当前集群成员列表

```bash
etcdctl member list
```

删除指定ID的节点

```bash
etcdctl member remove <member-id>
```

#### 1.3.2 查看etcd集群的Leader

```bash
etcdctl get mooncake-store/mooncake_cluster/master_view --print-value-only
```



## 2 MinIO（S3）环境搭建
### 2.1 下载 MinIO 二进制文件

```bash
wget https://dl.min.io/server/minio/release/linux-amd64/minio
```

### 2.2 赋予执行权限

```bash
chmod +x minio
```

### 2.3 移动到系统路径

```bash
sudo mv minio /usr/local/bin/
```

### 2.4 创建数据目录

```bash
mkdir -p /home/minio_data
```

### 2.5 启动MinIO

设置管理员用户名和密码

```bash
export MINIO_ROOT_USER=admin
export MINIO_ROOT_PASSWORD=adminadmin
```

配置中写好所有节点的地址，在所有设备中运行

```bash
minio server \
 --address ":9000"  \
 --console-address ":9001" \
 http://<node1 IP>/home/minio_data \
 http://<node2 IP>/home/minio_data
```


### 2.6 配置自启动服务(可选)

创建 `/etc/systemd/system/minio.service`文件：

```ini
[Unit]
Description=MinIO Object Storage
Documentation=https://docs.min.io
Wants=network-online.target
After=network-online.target

[Service]
User=root
Group=root
# 直接设置环境变量，不依赖外部文件
Environment="MINIO_ROOT_USER=admin"
Environment="MINIO_ROOT_PASSWORD=admin12345"
Environment="MINIO_VOLUMES=http://<node1 IP>/home/minio_data http://<node2 IP>/home/minio_data"
ExecStart=/usr/local/bin/minio server --address ":9000" --console-address ":9001" $MINIO_VOLUMES
Restart=always
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

保存后执行：

```bash
sudo systemctl daemon-reload
sudo systemctl start minio
sudo systemctl status minio
```

### 2.8 下载MC

```bash
# 下载 mc 客户端
wget https://dl.min.io/client/mc/release/linux-arm64/mc

# 赋予执行权限
chmod +x mc

# 移动到系统路径，方便全局使用
sudo mv mc /usr/local/bin/
```

### 2.9 连接MinIO服务器并添加别名

```bash
mc alias set <别名> http://<你的服务器IP>:9000 <管理员用户名> <密码>
```

### 2.10 查看服务器信息

```bash
mc admin info myminio
```

### 2.11 创建桶

```bash
mc mb myminio/mooncake-snapshot
```

### 2.12 查看所有桶

```bash
mc ls myminio
```


## 3 Mooncake S3环境编译
### 3.1 配置AWS-S3环境

#### 3.1.1 检查环境

```bash
ls -l /usr/local/lib64/libaws-cpp-sdk-s3.so
```

或

```bash
ls -l /usr/local/lib/libaws-cpp-sdk-s3.so
```

若文件存在，则环境搭建完成

#### 3.1.2 安装（AWS SDK C++）

```bash
git clone --depth 1 --recurse-submodules --shallow-submodules https://github.com/aws/aws-sdk-cpp.git
cd aws-sdk-cpp
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_ONLY="s3" -DENABLE_TESTING=OFF .. 
make -j$(nproc)
make install
ldconfig
```

### 3.2 Mooncake安装

```bash
git config --global http.sslVerify false
git clone --recurse-submodules https://github.com/<对应仓>/Mooncake.git
cd Mooncake
git checkout HA
export GOPROXY="http://mirrors.aliyun.com/goproxy,direct"
export GOINSECURE="go.etcd.io/etcd"
export GOSUMDB="sum.golang.org"
export CXXFLAGS="${CXXFLAGS} -w"
export CFLAGS="${CFLAGS} -w"
cmake -B build -DUSE_UB=ON -DBUILD_SHARED_LIBS=ON -DWITH_TE=ON -DWITH_STORE=ON -DWITH_P2P_STORE=ON -DUSE_ETCD=ON -DCMAKE_BUILD_TYPE=Release -DBUILD_UNIT_TESTS:BOOL=OFF -DSTORE_USE_ETCD=ON -DHAVE_AWS_SDK=ON
cmake --build build -j$(nproc)
git config --global http.sslVerify true
```

## 4 Mooncake HA测试脚本
### 4.1 master运行脚本

```bash
export URMA_RPC_ENABLE=0
export URMA_RPC_DEVICE=bonding_dev_0
export URMA_RPC_EID_INDEX=0

export MOONCAKE_SNAPSHOT_LOCAL_PATH=/home/mooncake_snapshot
export MOONCAKE_AWS_ACCESS_KEY_ID="admin"
export MOONCAKE_AWS_SECRET_ACCESS_KEY="adminadmin"
export MOONCAKE_AWS_REGION="us-east-1"
export MOONCAKE_AWS_BUCKET_NAME="mooncake-snapshot"
export MOONCAKE_AWS_S3_ENDPOINT="http://<node ip>:9000"
export MOONCAKE_MASTER_SERVICE_SNAPSHOT_TEST_SKIP_CLEANUP=1
export MC_LOG_DIR="/home/master_log"

mooncake_master \
	--default_kv_lease_ttl=300000 \
	--default_kv_soft_pin_ttl=300000 \
	--metrics_port=9006 \
	--rpc_port=50052 \
	--ha_backend_type=etcd \
	--etcd_endpoints="etcd://<node1 ip>:2379;<node2 ip>:2379" \
	--enable_ha=true \
	--enable_oplog=true \
	--rpc_address=<node ip> \
	--enable_metrics_report_to_backend=true \
	--enable-offload=true \
	--enable_snapshot=false \
    --enable_snapshot_restore=false \
    --snapshot_interval_seconds=10 \
    --snapshot_retention_count=5 \
    --snapshot_object_store_type="s3" \
	--v=0 \
    --cluster_id=mooncake_cluster

```

### 4.2 client运行脚本

```bash
export MC_URMA_ACTIVE_PORT=0
export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/home/mooncake_ssd
export MC_STORE_CLIENT_METRIC=0
export MC_STORE_CLIENT_METRIC_INTERVAL=3
export MC_URMA_BONDING_MULTIPATH_ENABLE=on
export MC_HIFREQ_LOG_SAMPLE_RATE=1
export MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT=1
export GLOG_v=1
export MC_LOG_DIR="/home/client_log"

export URMA_RPC_ENABLE=0
export URMA_RPC_DEVICE=bonding_dev_0
export URMA_RPC_EID_INDEX=0

mooncake_client \
    --host=<node1 ip> \
    --metadata_server="etcd://<node1 ip>:2379;<node2 ip>:2379" \
    --master_server_address="etcd://<node1 ip>:2379;<node2 ip>:2379" \
    --protocol=ub \
    --device_names=bonding_dev_0 \
    --global_segment_size=214748364080 \
    --port=50053 \
    --threads=32 \
    --v=0 \
    --enable_offload=true
```

### 4.3 数据写入脚本

```bash
export MC_STORE_CLIENT_SETUP_RETRIES=3
export no_proxy="127.0.0.1,localhost,local,.local,192.168.0.0/16,10.0.0.0/8,172.16.0.0/12,141.61.0.0/16"
export MC_STORE_CLIENT_METRIC_BANDWIDTH=0
export MC_TCP_BIND_ADDRESS=<node1 ip>
export MC_URMA_TRANS_MODE=RM

export MC_URMA_BONDING_MULTIPATH_ENABLE=on
export MC_HIFREQ_LOG_SAMPLE_RATE=1
export MC_URMA_ACTIVE_PORT=0

export URMA_RPC_ENABLE=0
export URMA_RPC_DEVICE=bonding_dev_0
export URMA_RPC_EID_INDEX=0

export MC_LOG_DIR="/home/w00889253/client_log"

stress_cluster_bench \
    --metadata-server='etcd://<node1 ip>:2379;<node2 ip>:2379' \
    --master-server='etcd://<node1 ip>:2379;<node2 ip>:2379' \
    --local-hostname=$MC_TCP_BIND_ADDRESS \
    --global-segment-size=0 \
    --local-buffer-size=10737418240 \
    --device-name=bonding_dev_0 \
    --scenario=remote_memory \ # 四个场景segment_write,segment_read, remote_memory writer/reader
    --role=writer \
    --num-keys=10000 \
    --protocol=ub \
    --verify=true \
    --num_threads=1 \
    --batch-size=16 \
    --duration=0 \
    --master_admin_port=9006 \
    --segments="<node1 ip>,<node2 ip>" \
    --replica_num=1



```

### 4.4 数据读取脚本

```bash
export MC_STORE_CLIENT_SETUP_RETRIES=3
export no_proxy="127.0.0.1,localhost,local,.local,192.168.0.0/16,10.0.0.0/8,172.16.0.0/12,141.61.0.0/16"
export MC_STORE_CLIENT_METRIC_BANDWIDTH=0
export MC_TCP_BIND_ADDRESS=<node1 ip>
export MC_URMA_TRANS_MODE=RM

export MC_URMA_BONDING_MULTIPATH_ENABLE=on
export MC_HIFREQ_LOG_SAMPLE_RATE=1
export MC_URMA_ACTIVE_PORT=0

export URMA_RPC_ENABLE=0
export URMA_RPC_DEVICE=bonding_dev_0
export URMA_RPC_EID_INDEX=0

export MC_LOG_DIR="/home/w00889253/client_log"

stress_cluster_bench \
    --metadata-server='etcd://<node1 ip>:2379;<node2 ip>:2379' \
    --master-server='etcd://<node1 ip>:2379;<node2 ip>:2379' \
    --local-hostname=$MC_TCP_BIND_ADDRESS \
    --global-segment-size=0 \
    --local-buffer-size=10737418240 \
    --device-name=bonding_dev_0 \
    --scenario=remote_memory \
    --role=reader \
    --num-keys=10000 \
    --protocol=ub \
    --verify=true \
    --num_threads=1 \
    --batch-size=16 \
    --duration=0 \
    --master_admin_port=9006 \
    --segments="<node1 ip>,<node2 ip>" \
    --replica_num=1


```




## 5 多副本参数配置
### 配置说明：replica_num / nof_replica_num 与 client TTL

> 代码基线：kvcache-ai/Mooncake `main`
> 内容：① 多副本参数 `replica_num` / `nof_replica_num` 的设置位置与方式；② client TTL 的两种配置途径（摘自《故障感知延迟分析-master多久发现节点故障.md》）

---

### 5.1 replica_num 与 nof_replica_num

#### 5.1.1 定义

定义在 `mooncake-store/include/replica.h` L81-83 的 `ReplicateConfig` 结构体：

```cpp
struct ReplicateConfig {
    size_t replica_num{1};       // 内存副本数，默认 1
    size_t nof_replica_num{0};   // NoF（NVMe-oF SSD）副本数，默认 0
    ...
};
```

#### 5.1.2 设置方式（三种场景）

**方式 1：C++ 代码中直接赋值**

```cpp
ReplicateConfig config;
config.replica_num = 2;        // 2 个内存副本
config.nof_replica_num = 1;    // 1 个 NoF 副本
auto result = client->Put(key, slices, config);
```

参考：`mooncake-store/tests/task_integration_test.cpp`、`mooncake-store/tests/replica_count_verify_test.cpp`。

**方式 2：Python API**

通过 pybind11 暴露为可读写属性（`mooncake-integration/store/store_py.cpp` L1832-1833，`def_readwrite`）：

```python
from mooncake.store import ReplicateConfig

config = ReplicateConfig()
config.replica_num = 1         # 1 个内存副本
config.nof_replica_num = 1     # 1 个 NoF 副本
rc = store.put(key, data, config)
```

参考：`mooncake-wheel/tests/test_replicated_distributed_object_store.py`、`scripts/test_copy_move_api.py`。

**方式 3：命令行参数 / 环境变量（E2E 与 Benchmark）**

| 场景 | 参数 | 默认值 |
|---|---|---|
| E2E client（`store_client_e2e.py`） | `--memory-replica-num` / `--nof-replica-num` | 1 / 1 |
| E2E 脚本（`run_nof_heartbeat_tcp_e2e.sh`） | 环境变量 `CLIENT_MEMORY_REPLICA_NUM` / `CLIENT_NOF_REPLICA_NUM` | 1 / 1 |
| Benchmark（`store_kv_bench.py`） | `--memory-replica-num` / `--nof-replica-num` | 1 / 0 |

E2E 用法示例：

```bash
CLIENT_MEMORY_REPLICA_NUM=1 CLIENT_NOF_REPLICA_NUM=1 bash run_nof_heartbeat_tcp_e2e.sh
```

#### 5.1.3 这两个值如何决定副本写入模式

`DetermineReplicaWriteMode`（`mooncake-store/include/replica.h` L156-165）：

| `replica_num` | `nof_replica_num` | 写入模式 | 语义 |
|---|---|---|---|
| 1 | 0 | `SINGLE_REPLICA` | 单副本 |
| 1 | 1 | `FLEXIBLE_DUAL_REPLICA` | 灵活双副本：1 内存 + 1 NoF，best-effort，任一类型成功即可（`HasExpectedReplicaAllocation` 只检查 `memory+nof > 0`） |
| >1 或 >1 | 任意 | `RELIABLE_MULTI_REPLICA` | 可靠多副本：必须严格凑齐（`allocated_memory == replica_num` 且 `allocated_nof == nof_replica_num`） |
| 0 | 0 | `SINGLE_REPLICA` | 退化为单副本 |

#### 5.1.4 关键约束

1. **`nof_replica_num > 0` 必须 USE_NOF=ON 编译**：否则 master 的 `PutStart` 直接返回 `INVALID_PARAMS`（`master_service.cpp` L3081-3088，`#ifndef USE_NOF` 分支；测试见 `replica_count_verify_test.cpp` Test 4）。
2. **`replica_num` 与 `nof_replica_num` 不能同时为 0**：`store_kv_bench.py` L653 有显式校验 `ValueError`。
3. **best-effort 语义**：`nof_replica_num == 0` 时，`HasExpectedReplicaAllocation`（`master_service.cpp` L121-133）只检查 `allocated_memory > 0`，不要求严格等于 `replica_num`。实测 `replica_num=5` 而只有 4 个 segment 时，分配 4 个副本仍算成功。

---

### 5.2 client TTL 的两种配置

#### 5.2.1 背景结论

- **master 多久发现 client 故障**：默认 **9~11 秒**（= client TTL ± 1s，TTL 默认 10s）。机制是"租约到期"，不是"心跳中断检测"。
- **可配置项**：client TTL 可配置；client 心跳间隔（1s，`client_service.cpp` L3743 硬编码）和 master 检查周期（1s，`master_service.h` L1980 硬编码）不可配。
- **配置位置**：这是 **`mooncake_master` 的启动参数，不是 client 的**。

#### 5.2.2 两种配置途径

**途径 1：命令行参数 `--client_ttl`**。gflag 定义（`master.cpp` L260-264），启动时 `mooncake_master --client_ttl=5` 即生效：

```cpp
DEFINE_int64(
    client_ttl, mooncake::DEFAULT_CLIENT_LIVE_TTL_SEC,
    "Seconds a client stays considered alive after the last heartbeat. "
    "If this TTL elapses without a refresh, the master treats the "
    "client as disconnected and may unmount its segments");
```

**途径 2：master 配置文件（键名 `client_live_ttl_sec`）**。master 启动时若指定了配置文件，会从中读这个键；读不到则用 gflag 的值兜底（`master.cpp` L465-467，第三个参数就是兜底值 `FLAGS_client_ttl`）：

```cpp
    default_config.GetInt64("client_live_ttl_sec",
                            &master_config.client_live_ttl_sec,
                            FLAGS_client_ttl);
```

**优先级：命令行显式设置 > 配置文件**。命令行显式传了 `--client_ttl`（`!info.is_default` 判断非默认值），或根本没用配置文件（`!conf_set`），则用命令行值覆盖（`master.cpp` L949-953）：

```cpp
    if ((google::GetCommandLineFlagInfo("client_ttl", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.client_live_ttl_sec = FLAGS_client_ttl;
    }
```

#### 5.2.3 配置速查表

| 参数 | 默认 | 可配 | 配置在哪 | 源码位置 |
|---|---|---|---|---|
| client TTL | 10s | ✅ | `mooncake_master --client_ttl=<秒>` / master 配置文件 `client_live_ttl_sec` | `master.cpp` L260-264 / L465-467 / L949-953；`types.h` L95 `DEFAULT_CLIENT_LIVE_TTL_SEC=10` |
| client 心跳间隔 | 1s | ❌ | —（client 代码硬编码） | `client_service.cpp` L3743 |
| master 检查周期 | 1s | ❌ | —（master 代码硬编码） | `master_service.h` L1980 |

一句话：同一个参数，命令行叫 `client_ttl`、配置文件里叫 `client_live_ttl_sec`，最终都写进 `master_config.client_live_ttl_sec`，两头都设时命令行优先。

实测：E2E 中 master 以 `--client_ttl=5` 启动，kill 数据节点后感知延迟落在 4~6s 区间，与理论吻合。

### 5.3 bench方案验证

验证思路：验证内存存在副本和disk磁盘存在副本两种方式进行验证。

#### 5.3.1 内存存在副本验证思路

写入500个key，如果存在两个client，则应该两边分别写入250个key，同时应该在另外一个client上面存在副本，则每个节点client内存应该有500个key。
操作步骤：先写入500个key，然后断连一个节点client，读取全部的key，可以从另外一个节点上全部读取。

#### 5.3.2 disk磁盘存在副本验证思路

写入500个key，如果存在两个client，则两边都存在250个key，，同时应该在另外一个client上面存在副本，则每个节点client内存应该有500个key。同时将这些key卸载到本地磁盘当中，磁盘当中应该在每个节点上有完整的500个key。

操作步骤：先写入500个key，然后断连两个节点client， 重启其中一个client，此时磁盘中的卸载的数据可以在master上重新加载，有500个key，此时读取数据应该能够将所有的数据都读到。

参考脚本：
master

```
mooncake_master \
	--enable_http_metadata_server=true \
    --http_metadata_server_host=0.0.0.0 \
    --http_metadata_server_port=9109 \
    --default_kv_lease_ttl=300000 \
    --default_kv_soft_pin_ttl=300000 \
	--metrics_port=9006 \
	--rpc_port=50052 \
    --enable-offload=true

```

client

```
export MC_TCP_BIND_ADDRESS=<node1 ip>
export MC_URMA_ACTIVE_PORT=0
export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/home/mooncake_ssd
export MC_STORE_CLIENT_METRIC=0
export MC_STORE_CLIENT_METRIC_INTERVAL=3
export MC_URMA_BONDING_MULTIPATH_ENABLE=on
export MC_HIFREQ_LOG_SAMPLE_RATE=0
export MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT=1(重要，确保所有key存入到磁盘当中）

export URMA_RPC_ENABLE=0
export URMA_RPC_DEVICE=bonding_dev_0
export URMA_RPC_EID_INDEX=0
mooncake_client \
    --host=<node1 ip> \
    --metadata_server=http://<node1 ip>:9109/metadata \
    --master_server_address=<node1 ip>:50052 \
    --protocol=ub \
    --device_names=bonding_dev_0 \
    --global_segment_size=21474836408 \
    --port=50053 \
    --threads=32 \
    --v=0 \
    --enable_offload=true

```

写入/读取脚本

```
export MC_STORE_CLIENT_SETUP_RETRIES=3
export no_proxy="127.0.0.1,localhost,local,.local,192.168.0.0/16,10.0.0.0/8,172.16.0.0/12,141.61.0.0/16"
export MC_STORE_CLIENT_METRIC_BANDWIDTH=0
export MC_TCP_BIND_ADDRESS=<node1 ip>
export MC_URMA_TRANS_MODE=RM
export MC_LOG_ENABLE=off
# export MC_LOG_DIR=/home/log
export MC_URMA_BONDING_MULTIPATH_ENABLE=on
export MC_HIFREQ_LOG_SAMPLE_RATE=0
export MC_URMA_ACTIVE_PORT=0

export URMA_RPC_ENABLE=0
export URMA_RPC_DEVICE=bonding_dev_0
export URMA_RPC_EID_INDEX=0

stress_cluster_bench \
        --metadata-server='http://<node1 ip>:9109/metadata' \
        --master-server='<node1 ip>:50052' \
        --local-hostname=$MC_TCP_BIND_ADDRESS \
        --global-segment-size=0 \
        --local-buffer-size=10737418240 \
        --device-name=bonding_dev_0 \
        --scenario=remote_memory \ （使用remote memory，这样key可以以固定结构写入）
        --role=reader \ （写入数据使用writer，读取使用reader）
        --num-keys=500 \
        --protocol=ub \
        --verify=false \
        --num_threads=1 \
        --batch-size=16 \
        --duration=0 \
        --master_admin_port=9006 \
        --replica_num=2 （副本数量）

```


# 6 mooncake基础信息上报

在上述启动master的脚本中，配置```enable_metrics_report_to_backend=true``,可以通过etcd命令行查找到对应数据
参考示例如下：

```
[root@node1 Mooncake]# etcdctl --endpoints=<node1 ip>:2379,<node2 ip>:2379 get --prefix /mooncake_cluster/masters/primary
/mooncake_cluster/masters/primary
{"id":"4056379308262733915-15491753023959092137","hostname":"<node2 ip>:50052","role":"primary","mem_total_bytes":42949672816,"mem_used_bytes":4194304000,"mem_available_bytes":38755368816,"nof_total_bytes":0,"nof_used_bytes":0,"nof_available_bytes":0,"file_total_bytes":4398046511104,"file_used_bytes":9080668160,"file_available_bytes":4388965842944,"key_count":500,"active_clients":2,"updated_at":"2026-08-06T16:59:35+08:00"}
[root@node1 Mooncake]# etcdctl --endpoints=<node1 ip>:2379,<node2 ip>:2379 get --prefix /mooncake_cluster/masters/primary
/mooncake_cluster/masters/primary
{"id":"4056379308262733915-15491753023959092137","hostname":"<node2 ip>:50052","role":"primary","mem_total_bytes":42949672816,"mem_used_bytes":4194304000,"mem_available_bytes":38755368816,"nof_total_bytes":0,"nof_used_bytes":0,"nof_available_bytes":0,"file_total_bytes":4398046511104,"file_used_bytes":9080668160,"file_available_bytes":4388965842944,"key_count":500,"active_clients":2,"updated_at":"2026-08-06T17:00:40+08:00"}
```


# 7 mooncake性能信息

使用脚本```Mooncake/mooncake-store/benchmarks/cluster_mooncake_diag.py```
参考其中的使用方法，在对应脚本当中设置对应log日志，则可以进行读取。