# Mooncake UbDiag 两层分发使用指南

Mooncake 通过统一的 CMake 接口提供 UbDiag 两层分发能力。默认模式将
PerfPoint 编译为空实现；启用模式从同一份 UbDiag 源码同步构建 SDK 动态库
与命令行工具，供 Mooncake 性能采集与分析使用。

## 1. 模式选择

| 模式 | CMake 选项 | 适用场景 | 构建产物 |
|------|------------|----------|----------|
| Layer 0：禁用模式 | `MOONCAKE_ENABLE_UBDIAG=OFF`（默认） | 不需要性能采集，保持零运行时依赖 | 仅使用 UbDiag 头文件；不生成或链接 `libubdiag` |
| Layer 1：启用模式 | `MOONCAKE_ENABLE_UBDIAG=ON` | 使用 PerfPoint、分位数、PerfLog 和 CSV 分析 Mooncake | 同步生成 `libubdiag.so` 与 `ubdiag` CLI |

模式由编译期选项决定，不能在运行时切换。建议为两种模式使用独立构建目录。

## 2. 默认禁用模式

未指定 `MOONCAKE_ENABLE_UBDIAG` 时，Mooncake 默认使用 Layer 0：

```bash
cmake -S . -B build
cmake --build build -j
```

也可以显式指定：

```bash
cmake -S . -B build -DMOONCAKE_ENABLE_UBDIAG=OFF
cmake --build build -j
```

CMake 配置阶段应输出（末尾同时打印已验证提交）：

```text
UbDiag: UBDIAG_DISABLE(空函数,零依赖), verified 705c6c37da45df2be4bc64c134dca0b7f30b2113
```

该模式使用 UbDiag `v0.5.1` 的同一份公共头文件，并通过
`UBDIAG_DISABLE` 将 PerfPoint 编译为空函数。Mooncake 无需启动 UbDiag，
也不需要安装 CLI 或动态库。可使用以下命令确认目标程序未链接 UbDiag：

```bash
ldd <mooncake-binary> | grep libubdiag
```

命令应无输出。

## 3. 启用 UbDiag

需要采集 Mooncake PerfPoint 数据时，使用独立目录重新配置并构建：

```bash
cmake -S . -B build-ubdiag -DMOONCAKE_ENABLE_UBDIAG=ON
cmake --build build-ubdiag -j
```

CMake 配置阶段应输出（末尾同时打印已验证提交）：

```text
UbDiag: FetchContent 编译 v0.5.1(库+CLI), verified 705c6c37da45df2be4bc64c134dca0b7f30b2113
```

默认构建目录下的关键产物为：

```text
build-ubdiag/_deps/ubdiag-build/src/sdk/libubdiag.so
build-ubdiag/_deps/ubdiag-build/src/cli/ubdiag
```

运行前应使用本次构建生成的 CLI 和动态库：

```bash
export UBDIAG_BUILD="$PWD/build-ubdiag/_deps/ubdiag-build"
export UBDIAG_CLI="$UBDIAG_BUILD/src/cli/ubdiag"
export LD_LIBRARY_PATH="$UBDIAG_BUILD/src/sdk${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
```

不得混用系统中其他版本的 `ubdiag` CLI 或 `libubdiag.so`。CLI 与 SDK
必须来自同一源码版本和同一次构建，以保证共享内存布局及功能开关一致。

## 4. 采集与分析

启动 UbDiag 共享内存，并启用 PerfLog：

```bash
"$UBDIAG_CLI" start --perflog
"$UBDIAG_CLI" status
```

随后按 Mooncake 原有方式启动服务和业务负载。负载运行期间可执行：

```bash
"$UBDIAG_CLI" show
"$UBDIAG_CLI" show --detail
"$UBDIAG_CLI" show --perflog
"$UBDIAG_CLI" watch --interval 1000
"$UBDIAG_CLI" history -n 5
```

其中 `show` 输出聚合统计及 P99/P999/P9999，`show --detail` 输出按核数据，
`show --perflog` 输出最近的单次探针记录。`watch` 使用 `Ctrl+C` 结束。

将分析结果导出为 CSV：

```bash
mkdir -p ./ubdiag-results
"$UBDIAG_CLI" show --csv ./ubdiag-results
"$UBDIAG_CLI" show --detail --csv ./ubdiag-results
"$UBDIAG_CLI" history --csv ./ubdiag-results
```

分析结束后销毁共享内存：

```bash
"$UBDIAG_CLI" stop
```

## 5. 离线源码

构建环境无法访问 GitHub 时，预先准备精确版本且保留 `.git` 元数据的
UbDiag 洁净工作树，并通过 `MOONCAKE_UBDIAG_SOURCE_DIR` 指定绝对路径：

```bash
git clone --branch v0.5.1 https://github.com/LinQuickDev/ubdiag.git /opt/src/ubdiag
cmake -S . -B build-ubdiag \
  -DMOONCAKE_ENABLE_UBDIAG=ON \
  -DMOONCAKE_UBDIAG_SOURCE_DIR=/opt/src/ubdiag
cmake --build build-ubdiag -j
```

`MOONCAKE_UBDIAG_SOURCE_DIR` 同样适用于默认禁用模式；将上述命令中的
`MOONCAKE_ENABLE_UBDIAG` 改为 `OFF` 即可。两种模式均不会在已指定本地
源码时访问 UbDiag 远端仓库。

配置阶段会自动执行以下等价校验，不需要人工确认：

```bash
git -C /opt/src/ubdiag rev-parse HEAD
git -C /opt/src/ubdiag status --porcelain --untracked-files=all
```

提交必须为 `705c6c37da45df2be4bc64c134dca0b7f30b2113`，工作树必须无修改和
未跟踪文件。升级 UbDiag 版本时，必须同时更新
`MOONCAKE_UBDIAG_GIT_TAG` 和完整的
`MOONCAKE_UBDIAG_EXPECTED_COMMIT`，不能只更换标签。

## 6. RPM 打包与来源保证

CMake 每次配置都会在当前构建目录生成
`mooncake_ubdiag_rpm.env`。该清单记录当前层、UbDiag 源码目录、期望提交
和实际提交。CLI 与动态库路径固定从当前 Mooncake build 的
`_deps/ubdiag-build` 推导，清单不能把它们重定向到其他目录。

完成构建后执行：

```bash
bash scripts/build_rpm.sh build-ubdiag rpm-output "$(uname -m)"
```

打包脚本不会搜索 `$PATH`、`LD_LIBRARY_PATH`、`/usr/bin`、`/usr/lib64`
或 `/usr/local` 中的 UbDiag，也不会使用 `find_package(UbDiag)`。它只接受
CMake 清单指定的 FetchContent 源码和构建产物，并在出包前再次执行以下
硬校验：

1. 源码仓当前提交、配置时解析的提交和期望提交三者完全一致。
2. 源码工作树保持洁净，避免同一 SHA 下混入本地修改。
3. UbDiag 子构建目录的提交标记与当前源码一致；版本变化时只清理并重建
   该子目录。
4. CLI 和全部 `libubdiag.so*` 位于当前 Mooncake build 的
   `_deps/ubdiag-build` 内。
5. CLI 确实依赖同一构建目录生成的 `libubdiag.so`。
6. 复制到 RPM BUILDROOT 后逐文件比较，防止打包阶段替换产物。

Layer 0 的 RPM 不包含 UbDiag CLI 或动态库，并检查待打包 ELF 不依赖
`libubdiag.so`。Layer 1 的 RPM 包含同一次构建生成的 CLI、动态库和配置，
同时写入 `/usr/share/doc/mooncake/ubdiag-provenance.txt`，记录提交 SHA
以及 CLI、动态库 SHA256。

同一构建目录先配置 Layer 0、后续再切换到 Layer 1 时，必须重新执行 CMake
配置和构建，再执行 RPM 脚本：

```bash
cmake -S . -B build -DMOONCAKE_ENABLE_UBDIAG=ON
cmake --build build -j
bash scripts/build_rpm.sh build rpm-output "$(uname -m)"
```

重新配置会刷新来源清单；打包脚本只按最新清单处理。反向切回 Layer 0 时，
即使构建目录残留旧的 `libubdiag.so`，也不会将其打入 RPM。

## 7. 验收标准

| 检查项 | Layer 0 | Layer 1 |
|--------|---------|---------|
| Mooncake 可正常编译和运行 | 必须 | 必须 |
| 目标程序链接 `libubdiag` | 否 | 是 |
| 生成 `ubdiag` CLI | 否 | 是 |
| `show` 返回 Mooncake PerfPoint 数据 | 不适用 | 必须 |
| P99/P999/P9999、PerfLog、CSV | 不适用 | 必须 |

若启用模式下 CLI 无数据，应依次确认共享内存已启动、Mooncake 使用的是
`build-ubdiag` 产物、运行时加载的是同目录 `libubdiag.so`，以及业务负载已
实际经过 Mooncake PerfPoint 打点路径。
