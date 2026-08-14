# Mooncake SpDiag 两层集成使用指南

Mooncake 在配置阶段提供两个互斥的 SpDiag 编译层。构建完成后不能在运行时切换层级；如需切换，应重新配置并编译。

| 层 | 配置 | SpDiag 来源 | 生成结果 |
|---|---|---|---|
| Layer 0：Mock | `MOONCAKE_ENABLE_SPDIAG=OFF` | Mooncake 拉取固定源码头文件 | PerfPoint 为空实现，无 SpDiag 运行时依赖 |
| Layer 1：System | `MOONCAKE_ENABLE_SPDIAG=ON` | 用户预装的 SpDiag RPM | Mooncake 链接系统 `.so`，并可将同版本 CLI/`.so` 打入单一 RPM |

## 1. Layer 0：默认 Mock

### 1.1 配置与编译

```bash
cmake -S . -B build \
  -DMOONCAKE_ENABLE_SPDIAG=OFF
cmake --build build --parallel
```

`MOONCAKE_ENABLE_SPDIAG` 默认值为 `OFF`，因此该参数可以省略。

Mooncake 使用 FetchContent 拉取固定修订的 SpDiag 源码，只消费 `include/spdiag` 公共头文件，并通过统一目标 `SpDiag::spdiag_lib` 传播 `SPDIAG_DISABLE`。

SpDiag 头文件中的 `SPDIAG_DISABLE` 分支将 PerfPoint 构造、`Start()`、`End()` 和 `Abandon()` 编译为空实现。因此：

- Mooncake 打点源码保持不变；
- Mooncake ELF 不依赖 `libspdiag.so`；
- 不生成或打包 SpDiag CLI；
- 不创建 SpDiag SHM；
- 系统中已安装的 SpDiag 不参与该构建。

离线环境可以指定已准备好的 SpDiag 源码目录：

```bash
cmake -S . -B build \
  -DMOONCAKE_ENABLE_SPDIAG=OFF \
  -DMOONCAKE_SPDIAG_SOURCE_DIR=/opt/src/spdiag
```

## 2. Layer 1：系统 SpDiag

### 2.1 前置条件

Layer 1 不下载或编译真实 SpDiag。配置 Mooncake 前，用户必须先安装完整的 SpDiag 运行时 RPM 和开发 RPM。安装结果必须同时提供：

- `SpDiagConfig.cmake`；
- 导入目标 `SpDiag::spdiag_lib`；
- 共享库 `libspdiag.so`；
- 可被系统找到的 `spdiag` CLI；
- `SPDIAG_ENABLE_PERCENTILE`；
- `SPDIAG_ENABLE_PERFLOG`。

SpDiag RPM 构建时应至少启用：

```text
SPDIAG_BUILD_SHARED=ON
ENABLE_PERCENTILE=ON
ENABLE_PERFLOG=ON
```

Mooncake 不固定 Layer 1 的 SpDiag 版本或源码 SHA。它通过 RPM 数据库分别查询 `libspdiag.so` 与 CLI 的 `VERSION-RELEASE.ARCH`，并要求二者完全一致。SpDiag RPM 中其他功能的启用情况和功能正确性由 SpDiag 发布包负责。

### 2.2 配置与编译

标准系统路径：

```bash
cmake -S . -B build-spdiag \
  -DMOONCAKE_ENABLE_SPDIAG=ON
cmake --build build-spdiag --parallel
```

自定义 RPM 安装前缀：

```bash
cmake -S . -B build-spdiag \
  -DMOONCAKE_ENABLE_SPDIAG=ON \
  -DCMAKE_PREFIX_PATH=/opt/spdiag \
  -DCMAKE_PROGRAM_PATH=/opt/spdiag/bin
```

也可以直接指定 CMake package 和 CLI：

```bash
cmake -S . -B build-spdiag \
  -DMOONCAKE_ENABLE_SPDIAG=ON \
  -DSpDiag_DIR=/opt/spdiag/lib64/cmake/SpDiag \
  -DMOONCAKE_SPDIAG_SYSTEM_CLI=/opt/spdiag/bin/spdiag
```

### 2.3 配置检查

ON 模式依次确认：

1. 找到 SpDiag CMake package 和 `SpDiag::spdiag_lib`；
2. package 导出 `SHARED_LIBRARY` 目标；
3. target 传播 P99 与 PerfLog 能力宏；
4. 找到 `spdiag` CLI；
5. `.so` 与 CLI 均由已安装的 RPM 提供；
6. 两者的 `VERSION-RELEASE.ARCH` 完全一致。

任一条件不满足都会停止配置。Mooncake 不会回退到源码构建；应先修复或重新安装 SpDiag RPM，再重新配置 Mooncake。

## 3. 构建 Mooncake RPM

### 3.1 Mock RPM

```bash
bash scripts/build_rpm.sh build rpm-output "$(uname -m)"
rpm -qlp rpm-output/mooncake-*.rpm
```

Mock RPM 包含 Mooncake 二进制，但不包含：

```text
/usr/bin/spdiag
/usr/lib64/libspdiag.so*
/etc/spdiag/spdiag.conf
```

### 3.2 System RPM

```bash
bash scripts/build_rpm.sh build-spdiag rpm-output "$(uname -m)"
rpm -qlp rpm-output/mooncake-*.rpm
```

System RPM 包含：

```text
/usr/bin/mooncake_master
/usr/bin/mooncake_client
/usr/bin/spdiag
/usr/lib64/libspdiag.so*
/etc/spdiag/spdiag.conf    # 系统安装提供该配置时
```

打包脚本读取 `build-spdiag/mooncake_spdiag.env`，并复制 CMake 配置阶段已经选中的 CLI、共享库及其符号链接。CLI 与共享库的版本一致性在 CMake 配置阶段完成检查；如果构建机上的 SpDiag RPM 随后发生变化，应重新配置后再打包。

## 4. 安装与运行

Mooncake RPM 使用标准系统路径安装 Mooncake 和 SpDiag 运行时：

```text
/usr/bin/mooncake_master
/usr/bin/mooncake_client
/usr/bin/spdiag
/usr/lib64/libspdiag.so*
/etc/spdiag/spdiag.conf
```

安装和基础运行命令：

```bash
sudo rpm -Uvh rpm-output/mooncake-*.rpm
sudo ldconfig

spdiag --version
spdiag start
spdiag status

# 启动 Mooncake master/client 并执行实际读写负载

spdiag show
spdiag show --detail
```

PerfLog、P99、历史数据和 CSV 参数以所安装 SpDiag CLI 的帮助为准：

```bash
spdiag --help
spdiag show --help
```

## 5. 常见错误

### `.so` 或 CLI 不受 RPM 管理

Layer 1 面向系统 SpDiag RPM，不接受手工复制的散装文件。请安装完整的 SpDiag 运行时 RPM 和开发 RPM 后重新配置 Mooncake。

### `.so` 与 CLI 的 RPM 版本不同

卸载冲突版本，并安装同一发布批次的 SpDiag RPM。Mooncake 比较 `VERSION-RELEASE.ARCH`，不通过文件 SHA 判断。

### 找到静态库

重新构建 SpDiag RPM，并设置 `SPDIAG_BUILD_SHARED=ON`。

### 缺少 P99 或 PerfLog

重新构建 SpDiag RPM，并设置：

```text
ENABLE_PERCENTILE=ON
ENABLE_PERFLOG=ON
```

### 从 Mock 切换到 System

推荐使用新的构建目录：

```bash
cmake -S . -B build-spdiag \
  -DMOONCAKE_ENABLE_SPDIAG=ON
```
