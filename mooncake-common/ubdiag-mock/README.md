# UbDiag Mock — Mooncake 集成说明

本目录是 Mooncake 项目的 UbDiag header-only mock，提供空操作的 PerfPoint，
确保在未安装 UbDiag 的情况下 Mooncake 仍可正常编译。

## 四种情况：用户拿到 Mooncake 后，每一步发生了什么

### 情况 A：`git clone --recursive` — 完整 submodule 路径

**Step 1 — git clone**：`--recursive` 触发 Git 读取 `.gitmodules`，自动拉取三个 submodule：
```
extern/pybind11/       ← 已拉取
extern/yalantinglibs/  ← 已拉取
extern/ubdiag/         ← 已拉取，commit f0a4a13（v0.4.0-56）
```

**Step 2 — cmake**：CMake 执行到 `FindUbDiag.cmake`，按顺序判断：
```
if(TARGET UbDiag::ubdiag_lib) → 否，第一次执行
if(EXISTS "extern/ubdiag/CMakeLists.txt") → 是 → add_subdirectory(real ubdiag)
```
输出：`UbDiag: using submodule (extern/ubdiag)`，return。

`EXCLUDE_FROM_ALL` 保证只编译 `ubdiag_lib` 自身，不编译它的 30+ 个单元测试和 examples。

**Step 3 — make**：`real_client.cpp` 中 `#include "ubdiag/auto_perf.h"` 找到真实 UbDiag，`PerfPoint::Start()/End()` 走 SHM 写入，性能数据可被 `ubdiag watch` 实时采集。

---

### 情况 B：`git clone`（无 `--recursive`）— mock 回退路径

**Step 1 — git clone**：只拉 Mooncake 自有代码。`extern/ubdiag/` 目录不存在。

**Step 2 — cmake**：`FindUbDiag.cmake` 按顺序判断：
```
if(TARGET UbDiag::ubdiag_lib) → 否
if(EXISTS "extern/ubdiag/CMakeLists.txt") → 否，目录不存在
find_package(UbDiag QUIET) → 否，系统没装
→ 走 Layer 3：创建 header-only INTERFACE library
```
```cmake
add_library(ubdiag_mock INTERFACE)     # 没有 .cpp，只暴露头文件路径
add_library(UbDiag::ubdiag_lib ALIAS ubdiag_mock)
```
输出：`UbDiag: using mock (no-op PerfPoint)`。

**Step 3 — make**：编译器展开 `PerfPoint::Start()` / `End()` 的两个空函数体：
```cpp
void Start() {}         // 编译器 -O1 以上：零指令
void End(int = 0) {}    // 同上
```
93+ 处 `PerfPoint(...)` 调用全部优化为空操作。Mooncake 完整编译运行，无需 systemtap-sdt-dev、libbpf 等任何 UbDiag 依赖。

---

### 情况 C：已有 clone（无 submodule），后来补拉

**Step 1 — git submodule update**：`extern/ubdiag/` 从不存在变为存在，Git 拉取 commit `f0a4a13`。

```bash
git submodule update --init --recursive
```

**Step 2 — 重新 cmake + make**：`FindUbDiag.cmake` 再次执行时 `EXISTS "extern/ubdiag/CMakeLists.txt"` 从 false 变成 true，Layer 1 命中。mock → submodule 切换自动完成，**不需要改任何 cmake 参数。**

```bash
cd build && cmake .. && make -j
```

---

### 情况 D：CI 环境已系统预装 UbDiag

**Step 1 — 系统预装**：运维已通过 RPM 或 `make install` 部署 `libubdiag.so` + `UbDiagConfig.cmake`。

```bash
cd ubdiag && bash build.sh -r && cd build && sudo make install
```

**Step 2 — cmake**：`FindUbDiag.cmake` 执行时 Layer 1 未命中（submodule 不存在），但 Layer 2 `find_package(UbDiag QUIET)` 成功发现系统 .so。输出 `UbDiag: using system package`。链接系统 .so，不编译 submodule。适用于 CI/CD 等已预装环境。

## 用户操作指南

作为 Mooncake 新用户，**你不需要做任何选择**。

| 你的操作 | 结果 | 编译期日志 |
|---|---|---|
| `git clone --recursive` | 使用真实 UbDiag (submodule)，性能诊断完整可用 | `UbDiag: using submodule` |
| `git clone`（不拉 submodule） | 使用本目录 mock，性能探针静默失效，核心功能不受影响 | `UbDiag: using mock` |
| `git clone` + `git submodule update --init` | 从 mock 切换到 submodule | `UbDiag: using submodule` |
| 系统已预装 UbDiag | 链接系统 .so，不编译 submodule | `UbDiag: using system package` |

以上路径全自动，由 `mooncake-common/FindUbDiag.cmake` 检测路径是否存在决定，**不需要传任何 cmake 参数或环境变量**。

### 路径 1：全新 clone（推荐，一步到位）

```bash
git clone --recursive <mooncake-repo-url>
cd Mooncake && mkdir build && cd build
cmake .. && make -j
```

`--recursive` 自动拉取三个 submodule（pybind11 / yalantinglibs / ubdiag），CMake 自动检测编译。

### 路径 2：已有 clone，补拉 submodule

```bash
cd Mooncake
git submodule update --init --recursive   # 拉取 ubdiag 等 submodule
cd build && cmake .. && make -j
```

### 路径 3：不拉 submodule，也能编译（完全不使用 UbDiag）

普通 `git clone`（不带 `--recursive`、不执行 `submodule update`）直接 cmake 编译即可，不需要做任何额外操作。
CMake 自动回退至本目录的 header-only mock，编译期输出 `UbDiag: using mock`：
- 所有 `PerfPoint` 调用编译为空操作，零运行时开销
- 不依赖任何 UbDiag 的 .so 或头文件，不需要安装 systemtap-sdt-dev、libbpf 等依赖
- Mooncake 核心功能完整运行，仅性能探针失效

### 路径 4（可选）：系统预装 UbDiag

```bash
cd ubdiag && bash build.sh -r && cd build && sudo make install
cd Mooncake && mkdir build && cd build
cmake .. && make -j
```

CMake 通过 `find_package(UbDiag)` 自动发现系统已安装的库。适用于 CI/CD 等已预装环境。

## 三层优先级

```
submodule (extern/ubdiag)  >  system package (find_package QUIET)  >  mock fallback (本目录)
```

每个 `include(FindUbDiag.cmake)` 开头有 `if(TARGET ...) return()`，多次 include 幂等。

## 文件说明

| 文件 | 用途 |
|---|---|
| `ubdiag/auto_perf.h` | Header-only mock，提供空操作 PerfPoint 和 PerfKey/PerfLevel 定义 |

## PerfLevel 对齐

本 mock 的 PerfLevel 枚举值与真实 UbDiag v0.4.0 保持一致：

```cpp
#include <cstdint>

enum class PerfLevel : uint8_t {
    SUB_SYSTEM  = 1,
    KEY_MODULE  = 2,
    MODULE      = 3,
    DEBUG       = 4,
};
```
