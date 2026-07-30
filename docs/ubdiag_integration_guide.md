# Mooncake UbDiag 两层集成使用指南

Mooncake 在配置阶段提供两个互斥的 UbDiag 编译层。构建完成后不能在运行时切换层级；如需切换，应重新配置并编译。

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

Mooncake 使用 FetchContent 拉取固定修订的 UbDiag 源码，只消费 `include/ubdiag` 公共头文件，并通过统一目标 `UbDiag::ubdiag_lib` 传播 `UBDIAG_DISABLE`。

UbDiag 头文件中的 `UBDIAG_DISABLE` 分支将 PerfPoint 构造、`Start()`、`End()` 和 `Abandon()` 编译为空实现。因此：

- Mooncake 打点源码保持不变；
- Mooncake ELF 不依赖 `libubdiag.so`；
- 不生成或打包 UbDiag CLI；
- 不创建 UbDiag SHM；
- 系统中已安装的 UbDiag 不参与该构建。

离线环境可以指定已准备好的 UbDiag 源码目录：

```bash
cmake -S . -B build \
  -DMOONCAKE_ENABLE_UBDIAG=OFF \
  -DMOONCAKE_UBDIAG_SOURCE_DIR=/opt/src/ubdiag
```

## 2. Layer 1：系统 UbDiag

### 2.1 前置条件

Layer 1 不下载或编译真实 UbDiag。配置 Mooncake 前，用户必须先安装完整的 UbDiag 运行时 RPM 和开发 RPM。安装结果必须同时提供：

- `UbDiagConfig.cmake`；
- 导入目标 `UbDiag::ubdiag_lib`；
- 共享库 `libubdiag.so`；
- 可被系统找到的 `ubdiag` CLI；
- `UBDIAG_ENABLE_PERCENTILE`；
- `UBDIAG_ENABLE_PERFLOG`。

UbDiag RPM 构建时应至少启用：

```text
UBDIAG_BUILD_SHARED=ON
ENABLE_PERCENTILE=ON
ENABLE_PERFLOG=ON
```

Mooncake 不固定 Layer 1 的 UbDiag 版本或源码 SHA。它通过 RPM 数据库分别查询 `libubdiag.so` 与 CLI 的 `VERSION-RELEASE.ARCH`，并要求二者完全一致。UbDiag RPM 中其他功能的启用情况和功能正确性由 UbDiag 发布包负责。

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
  -DCMAKE_PREFIX_PATH=/opt/ubdiag \
  -DCMAKE_PROGRAM_PATH=/opt/ubdiag/bin
```

也可以直接指定 CMake package 和 CLI：

```bash
cmake -S . -B build-ubdiag \
  -DMOONCAKE_ENABLE_UBDIAG=ON \
  -DUbDiag_DIR=/opt/ubdiag/lib64/cmake/UbDiag \
  -DMOONCAKE_UBDIAG_SYSTEM_CLI=/opt/ubdiag/bin/ubdiag
```

### 2.3 配置检查

ON 模式依次确认：

1. 找到 UbDiag CMake package 和 `UbDiag::ubdiag_lib`；
2. package 导出 `SHARED_LIBRARY` 目标；
3. target 传播 P99 与 PerfLog 能力宏；
4. 找到 `ubdiag` CLI；
5. `.so` 与 CLI 均由已安装的 RPM 提供；
6. 两者的 `VERSION-RELEASE.ARCH` 完全一致。

任一条件不满足都会停止配置。Mooncake 不会回退到源码构建；应先修复或重新安装 UbDiag RPM，再重新配置 Mooncake。

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
/etc/ubdiag/ubdiag.conf    # 系统安装提供该配置时
```

打包脚本读取 `build-ubdiag/mooncake_ubdiag.env`，并复制 CMake 配置阶段已经选中的 CLI、共享库及其符号链接。CLI 与共享库的版本一致性在 CMake 配置阶段完成检查；如果构建机上的 UbDiag RPM 随后发生变化，应重新配置后再打包。

## 4. 安装与运行

Mooncake RPM 使用标准系统路径安装 Mooncake 和 UbDiag 运行时：

```text
/usr/bin/mooncake_master
/usr/bin/mooncake_client
/usr/bin/ubdiag
/usr/lib64/libubdiag.so*
/etc/ubdiag/ubdiag.conf
```

安装和基础运行命令：

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

PerfLog、P99、历史数据和 CSV 参数以所安装 UbDiag CLI 的帮助为准：

```bash
ubdiag --help
ubdiag show --help
```

## 5. 常见错误

### `.so` 或 CLI 不受 RPM 管理

Layer 1 面向系统 UbDiag RPM，不接受手工复制的散装文件。请安装完整的 UbDiag 运行时 RPM 和开发 RPM 后重新配置 Mooncake。

### `.so` 与 CLI 的 RPM 版本不同

卸载冲突版本，并安装同一发布批次的 UbDiag RPM。Mooncake 比较 `VERSION-RELEASE.ARCH`，不通过文件 SHA 判断。

### 找到静态库

重新构建 UbDiag RPM，并设置 `UBDIAG_BUILD_SHARED=ON`。

### 缺少 P99 或 PerfLog

重新构建 UbDiag RPM，并设置：

```text
ENABLE_PERCENTILE=ON
ENABLE_PERFLOG=ON
```

### 从 Mock 切换到 System

推荐使用新的构建目录：

```bash
cmake -S . -B build-ubdiag \
  -DMOONCAKE_ENABLE_UBDIAG=ON
```
