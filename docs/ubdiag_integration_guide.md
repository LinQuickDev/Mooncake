# Mooncake UbDiag 两层集成使用指南

Mooncake 在配置阶段提供两个互斥的 UbDiag 编译层。构建完成后不能在运行时
切换层级；如需切换，应重新配置并编译。

| 层 | 配置 | UbDiag 来源 | 生成结果 |
|---|---|---|---|
| Layer 0：Mock | `MOONCAKE_ENABLE_UBDIAG=OFF` | Mooncake 拉取固定源码头文件 | PerfPoint 为空实现，无 UbDiag 运行时依赖 |
| Layer 1：System | `MOONCAKE_ENABLE_UBDIAG=ON` | 用户预装的 UbDiag RPM | Mooncake 链接系统 `.so`，并可将同版本 CLI/`.so` 打入单一 RPM |

## 1. Layer 0：默认 Mock

### 1.1 配置与编译

```bash
cmake -S . -B build \
  -DMOONCAKE_ENABLE_UBDIAG=OFF
cmake --build build --parallel
```

`MOONCAKE_ENABLE_UBDIAG` 默认值为 `OFF`，因此该参数可以省略。

Mooncake 使用 FetchContent 拉取固定提交的 UbDiag 源码，只消费公共头文件，
并通过统一目标 `UbDiag::ubdiag_lib` 传播：

```text
include/ubdiag
UBDIAG_DISABLE
```

UbDiag 头文件中的 `UBDIAG_DISABLE` 分支将 PerfPoint 构造、`Start()`、
`End()` 和 `Abandon()` 编译为空实现。因此：

- Mooncake 打点源码保持不变；
- Mooncake ELF 不依赖 `libubdiag.so`；
- 不生成或打包 UbDiag CLI；
- 不创建 UbDiag SHM；
- 系统中已安装的 UbDiag 不参与该构建。

离线环境可以指定相同提交的洁净 UbDiag 工作树：

```bash
cmake -S . -B build \
  -DMOONCAKE_ENABLE_UBDIAG=OFF \
  -DMOONCAKE_UBDIAG_SOURCE_DIR=/opt/src/ubdiag
```

## 2. Layer 1：系统 UbDiag

### 2.1 前置条件

Layer 1 不下载或编译真实 UbDiag。配置 Mooncake 前，用户必须先安装完整
UbDiag RPM。该安装必须同时提供：

- `UbDiagConfig.cmake`；
- 导入目标 `UbDiag::ubdiag_lib`；
- 共享库 `libubdiag.so`；
- 同一安装前缀下的 `bin/ubdiag`；
- `UBDIAG_ENABLE_PERCENTILE`；
- `UBDIAG_ENABLE_PERFLOG`。

UbDiag RPM 构建时应至少启用：

```text
UBDIAG_BUILD_SHARED=ON
ENABLE_PERCENTILE=ON
ENABLE_PERFLOG=ON
```

Mooncake 不固定 L1 的 UbDiag 版本或源码 SHA。它通过 RPM 数据库分别查询
真实 `libubdiag.so` 与 CLI 的所有者，并要求二者的
`VERSION-RELEASE.ARCH` 完全一致。UbDiag RPM 自身的功能正确性由 UbDiag
发布方负责。

### 2.2 配置与编译

标准系统路径：

```bash
cmake -S . -B build-ubdiag \
  -DMOONCAKE_ENABLE_UBDIAG=ON
cmake --build build-ubdiag --parallel
```

自定义 RPM 安装前缀：

```bash
cmake -S . -B build-ubdiag \
  -DMOONCAKE_ENABLE_UBDIAG=ON \
  -DCMAKE_PREFIX_PATH=/opt/ubdiag
```

也可以直接指定 CMake package：

```bash
cmake -S . -B build-ubdiag \
  -DMOONCAKE_ENABLE_UBDIAG=ON \
  -DUbDiag_DIR=/opt/ubdiag/lib64/cmake/UbDiag
```

### 2.3 配置门禁

ON 模式依次检查：

1. 找到 UbDiag CMake package；
2. package 导出导入型 `SHARED_LIBRARY` 目标；
3. 共享库解析为真实存在的 `libubdiag.so`；
4. target 传播 P99 与 PerfLog 能力宏；
5. 在同一安装前缀找到 `bin/ubdiag`；
6. `.so` 与 CLI 均由名称包含 `ubdiag` 的 RPM 拥有；
7. 两个 RPM 的 `VERSION-RELEASE.ARCH` 完全一致。

任一条件不满足都会停止配置。Mooncake 不会回退到源码构建，也不会使用
`PATH` 中另一个前缀的 CLI。

## 3. 构建 Mooncake RPM

### 3.1 Mock RPM

```bash
bash scripts/build_rpm.sh build rpm-output "$(uname -m)"
rpm -qlp rpm-output/mooncake-*.rpm
```

Mock RPM 包含 Mooncake 二进制，但不包含：

```text
/usr/bin/ubdiag
/usr/lib64/libubdiag.so*
/etc/ubdiag/ubdiag.conf
```

打包脚本还会检查所有 Mooncake ELF 均不依赖 `libubdiag.so`。

### 3.2 System RPM

```bash
bash scripts/build_rpm.sh build-ubdiag rpm-output "$(uname -m)"
rpm -qlp rpm-output/mooncake-*.rpm
```

System RPM 包含：

```text
/usr/bin/mooncake_master
/usr/bin/mooncake_client
/usr/bin/ubdiag
/usr/lib64/libubdiag.so*
/etc/ubdiag/ubdiag.conf    # 系统包提供配置时
```

打包脚本读取 `build-ubdiag/mooncake_ubdiag.env`，不会重新搜索 `PATH` 或
`LD_LIBRARY_PATH`。在复制前会再次查询 `.so` 与 CLI 的 RPM 归属；若系统
UbDiag 在配置后被升级、替换或拆成不同版本，打包立即失败。

生成 RPM 后，脚本会：

1. 检查 RPM 文件清单；
2. 检查 Mock/System 层与 ELF 依赖是否一致；
3. 清除构建目录 RPATH/RUNPATH；
4. 将 RPM 安装到隔离根目录，验证 payload 可以正常回装；
5. 不修改构建机当前已安装的软件。

## 4. 安装与运行

```bash
sudo rpm -Uvh rpm-output/mooncake-*.rpm
sudo ldconfig

ubdiag --version
ubdiag start
ubdiag status

# 启动 Mooncake master/client 并执行实际读写负载

ubdiag show
ubdiag show --detail
```

PerfLog、P99 与 CSV 参数以所安装 UbDiag CLI 的帮助为准：

```bash
ubdiag --help
ubdiag show --help
```

## 5. 常见错误

### `.so` 或 CLI 不受 RPM 管理

L1 面向系统 UbDiag RPM，不接受手工复制的散装文件。请安装完整 UbDiag
RPM 后重新配置 Mooncake。

### `.so` 与 CLI 的 RPM 版本不同

卸载冲突版本，并安装同一发布批次的 UbDiag RPM。Mooncake 比较
`VERSION-RELEASE.ARCH`，不通过文件 SHA 判断。

### 找到静态库

重新构建 UbDiag RPM，并设置 `UBDIAG_BUILD_SHARED=ON`。

### 缺少 P99 或 PerfLog

重新构建 UbDiag RPM，并设置：

```text
ENABLE_PERCENTILE=ON
ENABLE_PERFLOG=ON
```

### 从 Mock 切换到 System

推荐使用新的构建目录，避免旧模式产物混入：

```bash
cmake -S . -B build-ubdiag \
  -DMOONCAKE_ENABLE_UBDIAG=ON
```
