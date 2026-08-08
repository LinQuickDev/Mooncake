#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Mooncake 集群诊断工具（单文件、单 HTML、无配置文件）

================================================================================
【使用方法】
================================================================================

1. 必改：编辑本文件的 CONFIG 区域（约第 106 行），只需填：
   - k8s：K8S_NAMESPACE + K8S_LOG_PATH（自动发现该 namespace 下所有 pod）
   - client 超节点：CLIENT_HOSTS 列表 + CLIENT_LOG_PATH
   - master 超节点：MASTER_HOSTS 列表 + MASTER_LOG_PATH
   - SSH 密码：SSH_PASSWORD（如有必要，使用 sshpass；免密则留空）
   - SSH 用户/端口：SSH_USER / SSH_PORT（默认 root:22）
   
   示例：
       K8S_NAMESPACE = "default"
       K8S_LOG_PATH = "/var/log/mooncake"
       CLIENT_HOSTS = ["10.0.1.5", "10.0.1.6"]
       CLIENT_LOG_PATH = "/var/log/mooncake"
       MASTER_HOSTS = ["10.0.1.1"]
       MASTER_LOG_PATH = "/var/log/mooncake"
       SSH_USER = "root"
       SSH_PASSWORD = ""           # 免密则留空
       SPDIAG_BIN = "spdiag"

2. 常用命令：

   # 默认模式：拉日志 + spdiag show + 分析 → 单 HTML
   python cluster_mooncake_diag.py --since 30min -o report.html

   # 仅在所有目标上执行命令（如 spdiag start / clear），不分析
   python cluster_mooncake_diag.py --exec "spdiag start"
   python cluster_mooncake_diag.py --exec "spdiag clear"

   # 只重新分析已收集的日志（不连远程）
   python cluster_mooncake_diag.py --analyze-only --since 30min

   # 生成示例 HTML（用伪造数据，不连任何目标，用于查看报告样式）
   python cluster_mooncake_diag.py --sample -o sample.html

3. 时间窗口（--since / --until 在解析阶段按行时间戳真实过滤）：
   --since 30min            # 最近 30 分钟
   --since 2h               # 最近 2 小时
   --since "2026-08-05 10:00:00"   # 绝对时间
   --until "2026-08-05 11:00:00"   # 绝对时间（--until 仅支持绝对）

4. 典型工作流：
   a. python cluster_mooncake_diag.py --exec "spdiag start"   # 启动 spdiag 共享内存
   b. 重启 Mooncake 进程（Windows 必须重启；Linux 上 spdiag 会自动激活）
   c. 跑你的 benchmark / 业务负载
   d. python cluster_mooncake_diag.py --since 30min -o report.html
   e. python cluster_mooncake_diag.py --exec "spdiag stop"    # 可选，停止 spdiag

5. 输出 HTML 包含的区块：
   - 概览卡片（targets / spdiag ok / log files / get requests / p50/p99/max / 带宽）
   - [仅 --exec 模式] Command Execution Results
   - spdiag show 概览 + top 30 慢点位表
   - QPS by slot 折线图 + 每秒 QPS 表格
   - Bandwidth by slot 折线图 + 每秒带宽表格（MB/s）
   - get_into 耗时拆解（按 slot 分面板，6 阶段折线，鼠标框选缩放）
   - 8 类操作 Summary Statistics 表（count/avg/p50/p95/p99/p999/p9999/min/max）
   - Per-pod file stats（get 多少文件，#c# chunk vs 其他，字节数）
   - 最慢 100 个请求跨角色关联表（trace_id 关联 real_client/RPC/storage/master）
   - Per-target 详情（spdiag 原始输出 + 日志文件列表）

6. 可调常量（文件顶部）：
   CHART_MAX_POINTS = 6000       # get_into 图每 slot 最大均匀抽样点数
   CHART_SLOW_POINTS = 200       # get_into 图强制保留的最慢点数
   SLOW_TRACE_COUNT = 100        # 最慢请求表行数
   DEFAULT_CHUNK_SIZE = 4194304  # chunk 字节数，默认 4MB

7. 前置依赖：
   - Python 3.10+
   - kubectl（仅当 TARGETS 中有 k8s_pod 时）
   - ssh / scp / rsync（仅当 TARGETS 中有 supernode 时）
   - 目标主机上 spdiag 可执行文件已就位

================================================================================
"""

from __future__ import annotations

import argparse
import html
import json
import math
import re
import shlex
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable


# ===========================================================================
# CONFIG - just set these, then run. No other configuration needed.
# ===========================================================================

# --- SSH settings ---
SSH_USER = "root"               # SSH login user for all supernode targets
SSH_PORT = 22                   # SSH port
SSH_PASSWORD = ""               # Leave empty to use SSH keys.
                                # If set, sshpass is used (install: apt-get install sshpass).

# --- k8s: auto-discover all pods in this namespace ---
K8S_NAMESPACE = "e2b"              # e.g. "default". Empty = skip k8s.
K8S_LOG_PATH = "/var/log/mooncake"

# --- client supernodes (store/client reader nodes) ---
CLIENT_HOSTS: list[str] = [
    # "10.0.1.5",
    # "10.0.1.6",
]
CLIENT_LOG_PATH = "/var/log/mooncake"

# --- master supernodes (master service nodes) ---
MASTER_HOSTS: list[str] = [
    # "10.0.1.1",
]
MASTER_LOG_PATH = "/var/log/mooncake"

# --- spdiag binary path on remote targets ---
SPDIAG_BIN = "spdiag"

# ===========================================================================


# ---------------------------------------------------------------------------
# Defaults tunable via CLI
# ---------------------------------------------------------------------------
DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024
DEFAULT_CHUNK_MARKER = "#c#"
DEFAULT_PATH_SEPARATOR = "/chunk/"
DEFAULT_CHUNK_STYLE = "auto"
CHART_MAX_POINTS = 6000
CHART_SLOW_POINTS = 200
SLOW_TRACE_COUNT = 100
TOP_TEMPLATE_TABLE_N = 30

# Operations recognized in breakdown logs
OPS = (
    "get_into_breakdown",
    "batch_get_into_breakdown",
    "put_into_breakdown",
    "batch_put_into_breakdown",
    "get_breakdown",
    "batch_get_breakdown",
    "put_breakdown",
    "batch_put_breakdown",
)
OP_LABEL = {
    "get_into_breakdown": "get_into",
    "batch_get_into_breakdown": "batch_get_into",
    "put_into_breakdown": "put_into",
    "batch_put_into_breakdown": "batch_put_into",
    "get_breakdown": "get",
    "batch_get_breakdown": "batch_get",
    "put_breakdown": "put",
    "batch_put_breakdown": "batch_put",
}

# get_into stages for stacked area chart (from bench script)
GET_INTO_STAGES = (
    "query_us",
    "select_us",
    "offload_rpc_us",
    "transfer_data_us",
    "release_buffer_us",
    "read_overhead_us",
)
GET_INTO_STAGE_LABEL = {
    "query_us": "query",
    "select_us": "select",
    "offload_rpc_us": "offload RPC",
    "transfer_data_us": "transfer",
    "release_buffer_us": "release",
    "read_overhead_us": "overhead",
}

# bench-style cross-role events
BENCH_EVENTS = {
    "get_into_breakdown",
    "offload_rpc_client_breakdown",
    "offload_rpc_server_breakdown",
    "storage_read_breakdown",
    "storage_release_breakdown",
    "master_rpc_client_breakdown",
}
BENCH_STAGES = (
    "query_us", "select_us", "read_us", "offload_rpc_us",
    "transfer_data_us", "release_buffer_us", "read_overhead_us", "total_us",
)
RPC_STAGES = ("pool_lookup_us", "rpc_call_us", "result_get_us", "result_parse_us")

# regexes
GLOG_TS_RE = re.compile(
    r"^[IWEF](\d{4})(\d{2})(\d{2}) (\d{2}:\d{2}:\d{2})\.(\d{6})"
)
ISO_TS_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.(\d+))?"
)
BRACKET_RE = re.compile(r"([A-Za-z_][\w]*)\[([^\]]*)\]")
FIELD_RE = re.compile(r"([A-Za-z_][\w]*)=([^\s]+)")
TRACE_RE = re.compile(r"trace_id\[(\d+)\]")
BENCH_LOG_RE = re.compile(
    r"^[IWEF](?P<date>\d{8}) (?P<time>\d\d:\d\d:\d\d\.\d+).*?"
    r"trace_id\[(?P<trace>\d+)\] (?P<event>[a-z_]+) (?P<body>.*)$"
)
MASTER_SERVER_RE = re.compile(
    r"^[IWEF](?P<date>\d{8}) (?P<time>\d\d:\d\d:\d\d\.\d+).*?"
    r"trace_id\[(?P<trace>\d+)\] GetReplicaList (?P<body>.*)$"
)
OP_LINE_RE = re.compile(r"\b([a-z_]+breakdown)\b")
KV_FIELD_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)=([^\s]+)")
SLOT_RE = re.compile(r"(?:^|[\\/])slot(\d+)_")

# spdiag show row parser
SPDIAG_ROW_RE = re.compile(
    r"^\s*(?P<idx>\d+)\s+(?P<program>\S+)\s+(?P<module>\S+)\s+"
    r"(?P<point>\S+)\s+(?P<lvl>\d+)\s+(?P<ticks>\d+)\s+(?P<good>\d+)\s+"
    r"(?P<bad>\d+)\s+(?P<not>\d+)\s+(?P<total>\d+)\s+(?P<avg>\d+)\s+"
    r"(?P<min>\d+)\s+(?P<max>\d+)"
    r"(?:\s+(?P<p99>\d+|N/A))?"
    r"(?:\s+(?P<p999>\d+|N/A))?"
    r"(?:\s+(?P<p9999>\d+|N/A))?"
)


# ---------------------------------------------------------------------------
# Target abstraction
# ---------------------------------------------------------------------------
@dataclass
class Target:
    name: str
    kind: str            # "k8s_pod" or "supernode"
    namespace: str = ""
    pod: str = ""
    container: str = ""
    host: str = ""
    log_path: str = ""
    spdiag_bin: str = "spdiag"

    # ----- ssh helpers (as private class vars, set once) -----
    _ssh_port: int = 22
    _ssh_user: str = "root"
    _ssh_password: str = ""

    def _ssh_base(self) -> list[str]:
        """Build the ssh/sshpass command prefix."""
        if self._ssh_password:
            args = ["sshpass", "-p", self._ssh_password, "ssh"]
        else:
            args = ["ssh"]
        args += ["-p", str(self._ssh_port)]
        args += ["-o", "StrictHostKeyChecking=no",
                 "-o", "UserKnownHostsFile=/dev/null",
                 "-o", "ConnectTimeout=10",
                 "-o", "BatchMode=yes"]
        return args

    def _ssh_target(self) -> str:
        return f"{self._ssh_user}@{self.host}" if self._ssh_user else self.host

    def exec_command(self, command: str, timeout: int = 60) -> tuple[int, str]:
        if self.kind == "k8s_pod":
            args = ["kubectl", "-n", self.namespace, "exec", self.pod]
            if self.container:
                args += ["-c", self.container]
            args += ["--", "sh", "-c", command]
        else:
            args = self._ssh_base() + [self._ssh_target(), command]
        try:
            proc = subprocess.run(
                args, capture_output=True, text=True,
                timeout=timeout, check=False,
            )
            return proc.returncode, proc.stdout + proc.stderr
        except FileNotFoundError as exc:
            return 127, f"command not found: {exc}"
        except subprocess.TimeoutExpired:
            return 124, f"timeout after {timeout}s"
        except Exception as exc:
            return 1, f"exec error: {exc}"

    def pull_logs(self, dest_dir: Path) -> list[Path]:
        dest_dir.mkdir(parents=True, exist_ok=True)
        target_dir = dest_dir / self.name
        target_dir.mkdir(parents=True, exist_ok=True)
        if not self.log_path:
            return []
        if self.kind == "k8s_pod":
            return self._pull_k8s(target_dir)
        return self._pull_ssh(target_dir)

    def _pull_k8s(self, target_dir: Path) -> list[Path]:
        cp_args = ["kubectl", "-n", self.namespace, "cp"]
        if self.container:
            cp_args += ["-c", self.container]
        cp_args += [f"{self.pod}:{self.log_path}", str(target_dir / "logs")]
        subprocess.run(cp_args, capture_output=True, text=True, check=False)
        files = list(_walk_files(target_dir / "logs"))
        if not files:
            # cat fallback for single file
            cat_args = ["kubectl", "-n", self.namespace, "exec", self.pod]
            if self.container:
                cat_args += ["-c", self.container]
            cat_args += ["--", "cat", self.log_path]
            rc = subprocess.run(cat_args, capture_output=True, text=True, check=False)
            if rc.returncode == 0:
                f = target_dir / "pod.log"
                f.write_text(rc.stdout, encoding="utf-8", errors="replace")
                files = [f]
        return files

    def _pull_ssh(self, target_dir: Path) -> list[Path]:
        target = self._ssh_target()
        ssh_args = self._ssh_base() + [target]
        detect = ssh_args.copy()
        if self._ssh_password:
            # sshpass takes the command as a single arg after the ssh flags
            detect = self._ssh_base() + [target, f"test -d {shlex.quote(self.log_path)} && echo DIR || echo FILE"]
        else:
            detect = self._ssh_base() + [target, f"test -d {shlex.quote(self.log_path)} && echo DIR || echo FILE"]
        rc = subprocess.run(detect, capture_output=True, text=True, check=False)
        is_dir = "DIR" in rc.stdout
        # Build rsync or scp command
        if is_dir:
            ssh_cmd_str = " ".join(self._ssh_base())
            rsync_args = ["rsync", "-az", "-e", ssh_cmd_str,
                          f"{target}:{self.log_path}/", str(target_dir / "logs")]
            rc2 = subprocess.run(rsync_args, capture_output=True, text=True, check=False)
            if rc2.returncode != 0:
                scp_args = ["scp", "-r"] + self._ssh_base()[1:] + \
                    [f"{target}:{self.log_path}", str(target_dir / "logs")]
                subprocess.run(scp_args, capture_output=True, text=True, check=False)
        else:
            scp_args = ["scp"] + self._ssh_base()[1:] + \
                [f"{target}:{self.log_path}", str(target_dir / "remote.log")]
            subprocess.run(scp_args, capture_output=True, text=True, check=False)
        return list(_walk_files(target_dir))


def _walk_files(root: Path) -> Iterable[Path]:
    if not root.exists():
        return []
    if root.is_file():
        return [root]
    return [p for p in root.rglob("*") if p.is_file()]


def discover_k8s_pods(namespace: str) -> list[str]:
    """List pod names in the given k8s namespace via kubectl."""
    try:
        proc = subprocess.run(
            ["kubectl", "-n", namespace, "get", "pods", "-o", "name"],
            capture_output=True, text=True, timeout=30, check=False,
        )
    except FileNotFoundError:
        print("error: kubectl not found", file=sys.stderr)
        return []
    if proc.returncode != 0:
        print(f"kubectl get pods failed: {proc.stderr.strip()}", file=sys.stderr)
        return []
    pods = [line.strip().removeprefix("pod/") for line in proc.stdout.splitlines() if line.strip()]
    return pods


def build_targets() -> list[Target]:
    """Build Target list from CONFIG globals. Auto-discovers k8s pods."""
    targets: list[Target] = []

    # --- k8s pods ---
    if K8S_NAMESPACE:
        pods = discover_k8s_pods(K8S_NAMESPACE)
        print(f"[k8s] {K8S_NAMESPACE}: found {len(pods)} pods")
        for pod_name in pods:
            targets.append(Target(
                name=pod_name,
                kind="k8s_pod",
                namespace=K8S_NAMESPACE,
                pod=pod_name,
                log_path=K8S_LOG_PATH,
                spdiag_bin=SPDIAG_BIN,
            ))
    else:
        print("[k8s] namespace not set, skipping")

    # --- client supernodes ---
    for host in CLIENT_HOSTS:
        targets.append(Target(
            name=f"client-{host}",
            kind="supernode",
            host=host,
            log_path=CLIENT_LOG_PATH,
            spdiag_bin=SPDIAG_BIN,
        ))

    # --- master supernodes ---
    for host in MASTER_HOSTS:
        targets.append(Target(
            name=f"master-{host}",
            kind="supernode",
            host=host,
            log_path=MASTER_LOG_PATH,
            spdiag_bin=SPDIAG_BIN,
        ))

    # Apply SSH settings to all supernode targets
    for t in targets:
        if t.kind == "supernode":
            t._ssh_user = SSH_USER
            t._ssh_port = SSH_PORT
            t._ssh_password = SSH_PASSWORD

    return targets


# ---------------------------------------------------------------------------
# Time parsing
# ---------------------------------------------------------------------------
def parse_since(value: str) -> datetime:
    m = re.fullmatch(r"(\d+)\s*(s|min|h)", value)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        delta = (timedelta(seconds=n) if unit == "s"
                 else timedelta(minutes=n) if unit == "min"
                 else timedelta(hours=n))
        return datetime.now() - delta
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise SystemExit(f"unrecognized --since format: {value!r}")


def parse_until(value: str) -> datetime:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise SystemExit(f"--until requires absolute time, got: {value!r}")


def line_timestamp(line: str) -> datetime | None:
    """Extract timestamp from a log line (glog prefix or ISO inside fields)."""
    m = GLOG_TS_RE.match(line)
    if m:
        return datetime.strptime(
            f"{m.group(1)}-{m.group(2)}-{m.group(3)} {m.group(4)}.{m.group(5)}",
            "%Y-%m-%d %H:%M:%S.%f",
        )
    # Fallback: ISO timestamp inside the line
    m = ISO_TS_RE.search(line)
    if m:
        micros = m.group(3) or "0"
        micros = (micros + "000000")[:6]
        return datetime.strptime(
            f"{m.group(1)} {m.group(2)}.{micros}", "%Y-%m-%d %H:%M:%S.%f"
        )
    return None


# ---------------------------------------------------------------------------
# Field / key parsing
# ---------------------------------------------------------------------------
def parse_fields(body: str) -> dict[str, str]:
    result = {k: v for k, v in BRACKET_RE.findall(body)}
    result.update({k: v for k, v in FIELD_RE.findall(body)})
    return result


def split_chunk_key(
    key: str, *, marker: str, path_separator: str, chunk_size: int, style: str
) -> tuple[str, bool, int | None]:
    if style in {"auto", "offset-marker"} and marker in key:
        base, offset_text = key.rsplit(marker, 1)
        try:
            offset = int(offset_text)
            return base, True, offset // chunk_size
        except ValueError:
            pass
    if style in {"auto", "path-index"} and path_separator in key:
        base, index_text = key.rsplit(path_separator, 1)
        try:
            idx = int(index_text)
            if idx >= 0:
                return base, True, idx
        except ValueError:
            pass
    return key, False, None


def number(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    pos = (len(ordered) - 1) * q
    lo, hi = math.floor(pos), math.ceil(pos)
    if lo == hi:
        return ordered[lo]
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (pos - lo)


def stats(values: list[float]) -> dict[str, float | int]:
    data = [v for v in values if v is not None]
    if not data:
        return {"count": 0, "avg": 0, "p50": 0, "p95": 0, "p99": 0,
                "p999": 0, "p9999": 0, "min": 0, "max": 0}
    return {
        "count": len(data),
        "avg": sum(data) / len(data),
        "p50": percentile(data, 0.50),
        "p95": percentile(data, 0.95),
        "p99": percentile(data, 0.99),
        "p999": percentile(data, 0.999),
        "p9999": percentile(data, 0.9999),
        "min": min(data),
        "max": max(data),
    }


def slot_from_source(source_file: str) -> str:
    m = SLOT_RE.search(source_file)
    if m:
        return f"slot{m.group(1)}"
    # fallback: use parent dir name
    return Path(source_file).parent.name or Path(source_file).stem


# ---------------------------------------------------------------------------
# Data records
# ---------------------------------------------------------------------------
@dataclass
class Sample:
    """One breakdown line (any of the 8 ops)."""
    op: str
    ts: datetime
    fields: dict[str, str]
    timings: dict[str, float]
    raw_line: str
    source_file: str
    line_no: int
    slot: str
    target: str


@dataclass
class BenchRequest:
    """get_into_breakdown record for bench-style analysis."""
    ts: datetime
    slot: str
    target: str
    trace_id: str
    key: str
    fields: dict[str, str]
    timings: dict[str, float]
    raw_log: str


@dataclass
class PodFileStat:
    """Per-pod file statistics for get ops (item #5)."""
    target: str
    slot: str
    total_files: int = 0
    chunk_files: int = 0           # key contains #c# or /chunk/
    other_files: int = 0
    chunk_bytes: int = 0           # chunk_files * chunk_size
    other_bytes_known: int = 0     # sum of total_bytes for non-chunk keys
    chunk_keys: set[str] = field(default_factory=set)        # unique base keys
    other_key_samples: list[str] = field(default_factory=list)  # sample other keys


@dataclass
class SpDiagRow:
    program: str
    module: str
    point: str
    level: int
    ticks: int
    good: int
    bad: int
    not_done: int
    total_ns: int
    avg_ns: int
    min_ns: int
    max_ns: int
    p99_ns: int | None = None
    p999_ns: int | None = None
    p9999_ns: int | None = None


@dataclass
class TargetSnapshot:
    target: Target
    spdiag_rows: list[SpDiagRow] = field(default_factory=list)
    spdiag_raw: str = ""
    spdiag_rc: int = 0
    log_files: list[Path] = field(default_factory=list)
    exec_rc: int | None = None
    exec_out: str = ""
    error: str = ""


# ---------------------------------------------------------------------------
# Spdiag parsing
# ---------------------------------------------------------------------------
def parse_spdiag_show(text: str) -> list[SpDiagRow]:
    rows: list[SpDiagRow] = []
    for line in text.splitlines():
        m = SPDIAG_ROW_RE.match(line)
        if not m:
            continue
        p99 = m.group("p99")
        p999 = m.group("p999")
        p9999 = m.group("p9999")
        rows.append(SpDiagRow(
            program=m.group("program"),
            module=m.group("module"),
            point=m.group("point"),
            level=int(m.group("lvl")),
            ticks=int(m.group("ticks")),
            good=int(m.group("good")),
            bad=int(m.group("bad")),
            not_done=int(m.group("not")),
            total_ns=int(m.group("total")),
            avg_ns=int(m.group("avg")),
            min_ns=int(m.group("min")),
            max_ns=int(m.group("max")),
            p99_ns=None if p99 in (None, "N/A") else int(p99),
            p999_ns=None if p999 in (None, "N/A") else int(p999),
            p9999_ns=None if p9999 in (None, "N/A") else int(p9999),
        ))
    return rows


# ---------------------------------------------------------------------------
# Log parsing
# ---------------------------------------------------------------------------
def discover_log_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    patterns = ("*.log", "*.log.*", "*.txt", "*.out", "*.err", "*.INFO*", "*.WARNING*")
    files: list[Path] = []
    for pat in patterns:
        files.extend(root.rglob(pat))
    # Dedupe and sort
    seen = set()
    result = []
    for f in sorted(files):
        rp = f.resolve()
        if rp in seen:
            continue
        seen.add(rp)
        result.append(f)
    return result


def parse_breakdown_line(line: str) -> tuple[str, dict[str, str]] | None:
    """If line contains an op breakdown, return (op, fields). Else None."""
    m = OP_LINE_RE.search(line)
    if not m:
        return None
    op = m.group(1)
    if op not in OPS:
        return None
    fields = parse_fields(line)
    fields.update(dict(KV_FIELD_RE.findall(line)))
    return op, fields


def parse_logs(
    log_root: Path,
    *,
    since: datetime | None,
    until: datetime | None,
    chunk_size: int,
    chunk_marker: str,
    path_separator: str,
    chunk_style: str,
) -> tuple[list[Sample], list[BenchRequest], list[PodFileStat], dict[str, int]]:
    """Parse all log files under log_root. Returns (samples, bench_requests,
    pod_file_stats, counts)."""
    samples: list[Sample] = []
    bench_requests: list[BenchRequest] = []
    counts: Counter[str] = Counter()
    # pod_file_stats keyed by (target, slot)
    pod_stats: dict[tuple[str, str], PodFileStat] = {}

    for path in discover_log_files(log_root):
        target_name = _infer_target_name(path, log_root)
        slot = slot_from_source(str(path))
        counts["files_seen"] += 1
        try:
            with path.open("r", encoding="utf-8", errors="replace") as fh:
                for line_no, line in enumerate(fh, 1):
                    counts["lines_seen"] += 1
                    parsed = parse_breakdown_line(line)
                    if parsed is None:
                        continue
                    op, fields = parsed
                    ts = line_timestamp(line)
                    if ts is None:
                        continue
                    if since and ts < since:
                        continue
                    if until and ts > until:
                        continue
                    counts["target_lines"] += 1
                    timings = {
                        k: float(v) for k, v in fields.items()
                        if k.endswith("_us") and _is_number(v)
                    }
                    if not timings:
                        counts["malformed"] += 1
                        continue
                    samples.append(Sample(
                        op=op, ts=ts, fields=fields, timings=timings,
                        raw_line=line.rstrip(), source_file=str(path),
                        line_no=line_no, slot=slot, target=target_name,
                    ))
                    counts[op] += 1

                    # Bench-style: get_into_breakdown for trace correlation
                    if op == "get_into_breakdown":
                        trace_m = TRACE_RE.search(line)
                        trace_id = trace_m.group(1) if trace_m else ""
                        bench_requests.append(BenchRequest(
                            ts=ts, slot=slot, target=target_name,
                            trace_id=trace_id, key=fields.get("key", "-"),
                            fields=fields, timings=timings,
                            raw_log=line.rstrip(),
                        ))

                    # Pod file stats for get-class ops
                    if op.startswith("get") and "key" in fields:
                        _accumulate_pod_file_stat(
                            pod_stats, target_name, slot, fields["key"],
                            fields, chunk_size, chunk_marker,
                            path_separator, chunk_style,
                        )
        except OSError:
            counts["files_failed"] += 1

    samples.sort(key=lambda s: s.ts)
    bench_requests.sort(key=lambda r: r.ts)
    pod_file_stats = list(pod_stats.values())
    pod_file_stats.sort(key=lambda p: (p.target, p.slot))
    return samples, bench_requests, pod_file_stats, dict(counts)


def _infer_target_name(path: Path, log_root: Path) -> str:
    """Infer target name from path relative to log_root (first dir component)."""
    try:
        rel = path.relative_to(log_root)
        parts = rel.parts
        if len(parts) > 1:
            return parts[0]
    except ValueError:
        pass
    return path.parent.name or "unknown"


def _is_number(value: str) -> bool:
    try:
        float(value)
        return True
    except ValueError:
        return False


def _accumulate_pod_file_stat(
    pod_stats: dict[tuple[str, str], PodFileStat],
    target: str,
    slot: str,
    key: str,
    fields: dict[str, str],
    chunk_size: int,
    chunk_marker: str,
    path_separator: str,
    chunk_style: str,
) -> None:
    stat = pod_stats.setdefault((target, slot), PodFileStat(
        target=target, slot=slot
    ))
    stat.total_files += 1
    base_key, is_chunk, _ = split_chunk_key(
        key, marker=chunk_marker, path_separator=path_separator,
        chunk_size=chunk_size, style=chunk_style,
    )
    total_bytes = number(fields.get("total_bytes")) or 0
    if is_chunk:
        stat.chunk_files += 1
        stat.chunk_bytes += chunk_size  # standard 4MB chunk
        stat.chunk_keys.add(base_key)
    else:
        stat.other_files += 1
        stat.other_bytes_known += int(total_bytes)
        if len(stat.other_key_samples) < 10:
            stat.other_key_samples.append(key)


# ---------------------------------------------------------------------------
# Bench-style cross-role correlation (from analyze_mooncake_bench_web.py)
# ---------------------------------------------------------------------------
def parse_bench_aux_logs(
    log_root: Path,
    *,
    since: datetime | None,
    until: datetime | None,
) -> tuple[
    list[BenchRequest],
    dict[str, dict[str, str]],   # rpc (client)
    dict[str, dict[str, str]],   # storage_read
    dict[str, dict[str, str]],   # storage_rpc (server)
    dict[str, dict[str, str]],   # release
    dict[str, dict[str, str]],   # master_rpc
    dict[str, int],              # counts
]:
    requests: list[BenchRequest] = []
    rpc: dict[str, dict[str, str]] = {}
    storage_read: dict[str, dict[str, str]] = {}
    storage_rpc: dict[str, dict[str, str]] = {}
    release: dict[str, dict[str, str]] = {}
    master_rpc: dict[str, dict[str, str]] = {}
    counts: Counter[str] = Counter()

    for path in discover_log_files(log_root):
        slot = slot_from_source(str(path))
        try:
            fh = path.open("r", encoding="utf-8", errors="replace")
        except OSError:
            counts["files_failed"] += 1
            continue
        with fh:
            for line in fh:
                m = BENCH_LOG_RE.match(line)
                if not m:
                    mm = MASTER_SERVER_RE.match(line)
                    if mm:
                        ts = datetime.strptime(
                            mm.group("date") + " " + mm.group("time"),
                            "%Y%m%d %H:%M:%S.%f",
                        )
                        if since and ts < since:
                            continue
                        if until and ts > until:
                            continue
                        counts["master_rpc_server_breakdown"] += 1
                        trace = mm.group("trace")
                        fields = parse_fields(mm.group("body"))
                        target = master_rpc.setdefault(trace, {})
                        target["server_latency_us"] = fields.get("latency_us", "")
                        target["server_status"] = fields.get("status", "")
                    continue
                event = m.group("event")
                if event not in BENCH_EVENTS:
                    continue
                ts = datetime.strptime(
                    m.group("date") + " " + m.group("time"),
                    "%Y%m%d %H:%M:%S.%f",
                )
                if since and ts < since:
                    continue
                if until and ts > until:
                    continue
                counts[event] += 1
                trace = m.group("trace")
                fields = parse_fields(m.group("body"))
                fields["_slot"] = slot
                if event == "get_into_breakdown":
                    timings = {
                        k: float(v) for k, v in fields.items()
                        if k in BENCH_STAGES and _is_number(v)
                    }
                    requests.append(BenchRequest(
                        ts=ts, slot=slot, target=_infer_target_name(path, log_root),
                        trace_id=trace, key=fields.get("key", "-"),
                        fields=fields, timings=timings,
                        raw_log=line.rstrip(),
                    ))
                elif event == "offload_rpc_client_breakdown":
                    rpc[trace] = fields
                elif event == "storage_read_breakdown":
                    storage_read[trace] = fields
                elif event == "offload_rpc_server_breakdown":
                    storage_rpc[trace] = fields
                elif event == "storage_release_breakdown":
                    release[trace] = fields
                else:
                    master_rpc.setdefault(trace, {}).update(fields)
    return requests, rpc, storage_read, storage_rpc, release, master_rpc, dict(counts)


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------
def aggregate_op_summary(samples: list[Sample]) -> list[dict]:
    """8-op summary: each op x each *_us segment -> stats row."""
    # group by (op, segment)
    buckets: dict[tuple[str, str], list[float]] = defaultdict(list)
    for s in samples:
        for seg, val in s.timings.items():
            buckets[(s.op, seg)].append(val)
    rows = []
    for op in OPS:
        # collect all segments observed for this op
        segs = sorted({seg for (o, seg) in buckets if o == op})
        for seg in segs:
            values = buckets[(op, seg)]
            st = stats(values)
            rows.append({
                "op": OP_LABEL.get(op, op),
                "segment": seg,
                **st,
            })
    return rows


def aggregate_qps_by_slot(samples: list[Sample]) -> dict[str, dict[str, int]]:
    """QPS by slot for get_into/batch_get_into ops (per-second buckets)."""
    by_slot: dict[str, list[Sample]] = defaultdict(list)
    for s in samples:
        if s.op in ("get_into_breakdown", "batch_get_into_breakdown"):
            by_slot[f"{s.target}/{s.slot}"].append(s)
    qps: dict[str, dict[str, int]] = {}
    for slot_key, items in sorted(by_slot.items()):
        buckets = Counter(item.ts.strftime("%H:%M:%S") for item in items)
        qps[slot_key] = dict(sorted(buckets.items()))
    return qps


def aggregate_bandwidth_by_slot(samples: list[Sample], *, chunk_size: int = DEFAULT_CHUNK_SIZE) -> dict[str, dict[str, float]]:
    """Bandwidth by slot for get_into/batch_get_into ops (per-second bytes/s -> MB/s).
    Falls back to chunk_size when total_bytes is missing/zero for get-class ops."""
    by_slot: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for s in samples:
        if s.op in ("get_into_breakdown", "batch_get_into_breakdown"):
            key = f"{s.target}/{s.slot}"
            ts_bucket = s.ts.strftime("%H:%M:%S")
            val = float(s.fields.get("total_bytes", 0))
            if val == 0 and s.op.startswith("get"):
                val = float(chunk_size)  # fallback: assume 4MB per get request
            by_slot[key][ts_bucket] += val
    bw: dict[str, dict[str, float]] = {}
    for slot_key in sorted(by_slot):
        bw[slot_key] = {t: v / (1024 * 1024) for t, v in sorted(by_slot[slot_key].items())}
    return bw


def aggregate_get_into_chart(
    requests: list[BenchRequest],
    *,
    max_points: int = CHART_MAX_POINTS,
    slow_points: int = CHART_SLOW_POINTS,
) -> tuple[list[dict], int]:
    """Downsample get_into requests for the stacked area chart.

    Returns (chart_points, stride). Each chart point has time, slot, trace_id,
    key, the 6 stage values, and total_us.
    """
    ordered = sorted(requests, key=lambda r: r.ts)
    max_points = max(1, max_points)
    slow_points = max(0, slow_points)
    stride = max(1, math.ceil(len(ordered) / max_points))
    uniform = ordered[::stride]
    slow = sorted(
        requests,
        key=lambda r: r.timings.get("total_us", 0),
        reverse=True,
    )[:slow_points]
    combined = sorted(
        {id(r): r for r in uniform + slow}.values(),
        key=lambda r: r.ts,
    )
    chart = [
        {
            "time": r.ts.strftime("%H:%M:%S.%f")[:-3],
            "slot": f"{r.target}/{r.slot}",
            "trace_id": r.trace_id,
            "key": r.key,
            **{seg: r.timings.get(seg, 0) for seg in GET_INTO_STAGES},
            "total_us": r.timings.get("total_us", 0),
            "log": r.raw_log,
        }
        for r in combined
    ]
    return chart, stride


def aggregate_slowest_requests(
    requests: list[BenchRequest],
    *,
    rpc: dict[str, dict[str, str]],
    storage_read: dict[str, dict[str, str]],
    storage_rpc: dict[str, dict[str, str]],
    release: dict[str, dict[str, str]],
    master_rpc: dict[str, dict[str, str]],
    n: int = SLOW_TRACE_COUNT,
) -> list[dict]:
    slowest = sorted(
        requests, key=lambda r: r.timings.get("total_us", 0), reverse=True
    )[:n]
    rows = []
    for rank, r in enumerate(slowest, 1):
        rpc_f = rpc.get(r.trace_id, {})
        read_f = storage_read.get(r.trace_id, {})
        server_f = storage_rpc.get(r.trace_id, {})
        release_f = release.get(r.trace_id, {})
        master_f = master_rpc.get(r.trace_id, {})
        rows.append({
            "rank": rank,
            "time": r.ts.strftime("%H:%M:%S.%f")[:-3],
            "slot": f"{r.target}/{r.slot}",
            "trace_id": r.trace_id,
            "key": r.key,
            "remote": r.fields.get("remote_endpoint", "-"),
            **{k: r.timings.get(k) for k in BENCH_STAGES},
            "rpc_pool_lookup_us": number(rpc_f.get("pool_lookup_us")),
            "rpc_rpc_call_us": number(rpc_f.get("rpc_call_us")),
            "rpc_result_get_us": number(rpc_f.get("result_get_us")),
            "storage_queue_us": number(server_f.get("queue_us")),
            "storage_file_open_us": number(read_f.get("file_open_us")),
            "storage_disk_read_us": number(read_f.get("disk_read_us")),
            "storage_total_us": number(read_f.get("total_us")),
            "storage_release_us": number(release_f.get("total_us")),
            "master_pool_lookup_us": number(master_f.get("pool_lookup_us")),
            "master_rpc_call_us": number(master_f.get("rpc_call_us")),
            "master_result_get_us": number(master_f.get("result_get_us")),
            "master_rpc_total_us": number(master_f.get("total_us")),
            "master_server_us": number(master_f.get("server_latency_us")),
        })
    return rows


def aggregate_latency_overview(requests: list[BenchRequest]) -> dict:
    total_values = [r.timings["total_us"] for r in requests if "total_us" in r.timings]
    bytes_total = sum(number(r.fields.get("total_bytes")) or 0 for r in requests)
    successful = [r for r in requests if r.fields.get("status") == "read_ok"]
    correlated = sum(
        1 for r in requests
        if r.trace_id in rpc_dummy and r.trace_id in storage_read_dummy
    ) if False else 0  # correlated computed by caller
    return {
        "request_count": len(requests),
        "success_count": len(successful),
        "failed_count": len(requests) - len(successful),
        "bytes_total": bytes_total,
        "latency": stats(total_values),
    }

# dummy refs (avoid unused warning in aggregate_latency_overview)
rpc_dummy: dict = {}
storage_read_dummy: dict = {}


# ---------------------------------------------------------------------------
# Collection
# ---------------------------------------------------------------------------
def collect_snapshots(
    targets: list[Target],
    work_dir: Path,
    *,
    run_spdiag: bool,
    pull_logs: bool,
    timeout: int,
) -> list[TargetSnapshot]:
    snapshots: list[TargetSnapshot] = []
    logs_root = work_dir / "collected_logs"
    spdiag_root = work_dir / "spdiag_show"
    logs_root.mkdir(parents=True, exist_ok=True)
    spdiag_root.mkdir(parents=True, exist_ok=True)

    for idx, target in enumerate(targets, 1):
        snap = TargetSnapshot(target=target)
        print(f"[{idx}/{len(targets)}] {target.name}:", flush=True)

        if pull_logs and target.log_path:
            print(f"  pulling logs from {target.log_path} ...", flush=True)
            files = target.pull_logs(logs_root)
            snap.log_files = files
            print(f"  got {len(files)} file(s)", flush=True)

        if run_spdiag:
            cmd = f"{target.spdiag_bin} show"
            print(f"  running: {cmd}", flush=True)
            rc, out = target.exec_command(cmd, timeout=timeout)
            snap.spdiag_rc = rc
            snap.spdiag_raw = out
            if rc == 0:
                snap.spdiag_rows = parse_spdiag_show(out)
                print(f"  spdiag show: {len(snap.spdiag_rows)} rows", flush=True)
            else:
                print(f"  spdiag show failed (rc={rc})", flush=True)
            (spdiag_root / f"{target.name}.txt").write_text(
                out, encoding="utf-8", errors="replace"
            )

        snapshots.append(snap)
    return snapshots


def exec_on_targets(
    targets: list[Target], command: str, timeout: int
) -> list[TargetSnapshot]:
    snapshots: list[TargetSnapshot] = []
    for idx, target in enumerate(targets, 1):
        snap = TargetSnapshot(target=target)
        print(f"[{idx}/{len(targets)}] {target.name}: exec {command!r}", flush=True)
        rc, out = target.exec_command(command, timeout=timeout)
        snap.exec_rc = rc
        snap.exec_out = out
        print(f"  rc={rc}", flush=True)
        snapshots.append(snap)
    return snapshots


# ===========================================================================
# HTML rendering
# ===========================================================================
CSS = """
:root {
  --bg: #f4f7fb;
  --panel: #ffffff;
  --line: #dbe3ee;
  --text: #172033;
  --muted: #667085;
  --blue: #1769aa;
  --red: #c9362b;
  --green: #1f9d55;
  --amber: #b7791f;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  background: var(--bg);
  color: var(--text);
  font: 14px/1.55 -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC",
        "Microsoft YaHei", sans-serif;
}
header {
  padding: 26px 32px;
  background: linear-gradient(135deg, #102a43 0%, #1f3a5f 100%);
  color: #fff;
}
header h1 { margin: 0 0 6px; font-size: 24px; }
header .meta { opacity: 0.85; font-size: 13px; }
header .meta span {
  display: inline-block;
  padding: 3px 9px;
  margin-right: 8px;
  margin-top: 6px;
  background: rgba(255,255,255,0.12);
  border-radius: 6px;
}
main { padding: 22px 30px 60px; }
h2 {
  margin: 28px 0 13px;
  font-size: 18px;
  font-weight: 650;
  border-left: 4px solid var(--blue);
  padding-left: 10px;
}
h3 { margin: 18px 0 8px; font-size: 15px; }
.cards {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 12px;
}
.card {
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 9px;
  padding: 14px;
}
.card b { display: block; font-size: 22px; color: var(--blue); }
.card span { color: var(--muted); font-size: 12px; }
.panel {
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 9px;
  margin-top: 14px;
  overflow: hidden;
}
.panel-head {
  padding: 12px 16px;
  background: #edf3f8;
  border-bottom: 1px solid var(--line);
  font-weight: 650;
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.panel-head .count {
  color: var(--muted);
  font-size: 12px;
  font-weight: 400;
}
.panel-body { padding: 14px 16px; overflow-x: auto; }
.panel-body.no-pad { padding: 0; }
table { border-collapse: collapse; width: 100%; font-variant-numeric: tabular-nums; }
th, td {
  padding: 7px 10px;
  border-bottom: 1px solid var(--line);
  text-align: right;
  white-space: nowrap;
}
th:first-child, td:first-child, th.col-name, td.col-name { text-align: left; }
th {
  position: sticky; top: 0;
  background: #edf3f8;
  color: #344054;
  font-weight: 650;
}
tr:hover td { background: #f8fafc; }
.rc-ok { color: var(--green); font-weight: 600; }
.rc-fail { color: var(--red); font-weight: 600; }
pre {
  margin: 0;
  padding: 10px 12px;
  max-height: 320px;
  overflow: auto;
  background: #0f1e2e;
  color: #d6e4f0;
  font: 12px/1.5 "SFMono-Regular", Consolas, "Liberation Mono", monospace;
  white-space: pre-wrap;
  word-break: break-all;
}
.muted { color: var(--muted); }
.target-section { margin-top: 18px; }
.target-section h3 {
  display: flex;
  align-items: center;
  gap: 8px;
}
.badge {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 11px;
  font-size: 11px;
  font-weight: 600;
  background: #e1eaf5;
  color: var(--blue);
}
.badge.k8s { background: #326ce5; color: #fff; }
.badge.ssh { background: #1f9d55; color: #fff; }
.empty {
  padding: 40px;
  text-align: center;
  color: var(--muted);
  font-style: italic;
}
.legend {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  margin: 8px 0;
  color: var(--muted);
  font-size: 13px;
}
svg {
  width: 100%;
  height: 330px;
  background: #fbfdff;
  border: 1px solid var(--line);
  display: block;
}
.slot-chart { margin-top: 16px; padding-top: 12px; border-top: 1px solid var(--line); }
.slot-chart h3 { margin: 0 0 5px; font-size: 15px; }
.slot-chart svg { height: 300px; }
.chart-wrap { position: relative; }
.tooltip {
  display: none;
  position: absolute;
  z-index: 5;
  max-width: 720px;
  padding: 10px 12px;
  background: rgba(16,42,67,.96);
  color: #fff;
  border-radius: 7px;
  box-shadow: 0 5px 20px #0004;
  pointer-events: none;
  font-size: 12px;
  white-space: normal;
}
.tooltip code {
  display: block;
  margin-top: 6px;
  color: #d9efff;
  word-break: break-all;
}
"""


def esc(value) -> str:
    return html.escape(str(value) if value is not None else "")


def fmt_us(value: float | int | None) -> str:
    if value is None:
        return "-"
    return f"{float(value):,.1f}"


def fmt_bytes(value: int | float) -> str:
    v = float(value)
    if v < 1024:
        return f"{v:.0f} B"
    if v < 1024 * 1024:
        return f"{v/1024:.1f} KB"
    if v < 1024 * 1024 * 1024:
        return f"{v/(1024*1024):.1f} MB"
    return f"{v/(1024*1024*1024):.2f} GB"


def _render_qps_table(qps_by_slot: dict[str, dict[str, int]]) -> str:
    """Render per-second QPS table with one row per second, columns per slot."""
    if not qps_by_slot:
        return '<table><tr><td class="empty">no QPS data</td></tr></table>'
    slots = sorted(qps_by_slot.keys())
    all_times = sorted({t for s in slots for t in qps_by_slot[s]})
    if not all_times:
        return '<table><tr><td class="empty">no QPS data</td></tr></table>'
    th = "<tr><th>time</th>" + "".join(f"<th>{esc(s)}</th>" for s in slots) + "</tr>"
    rows = []
    for t in all_times:
        cells = f"<td class='col-name'>{esc(t)}</td>"
        for s in slots:
            v = qps_by_slot[s].get(t, 0)
            cells += f"<td>{v:,}</td>"
        rows.append(f"<tr>{cells}</tr>")
    return f'<table><thead>{th}</thead><tbody>{"".join(rows)}</tbody></table>'


def _render_bandwidth_table(bw_by_slot: dict[str, dict[str, float]]) -> str:
    """Render per-second bandwidth table with one row per second, columns per slot."""
    if not bw_by_slot:
        return '<table><tr><td class="empty">no bandwidth data</td></tr></table>'
    slots = sorted(bw_by_slot.keys())
    all_times = sorted({t for s in slots for t in bw_by_slot[s]})
    if not all_times:
        return '<table><tr><td class="empty">no bandwidth data</td></tr></table>'
    th = "<tr><th>time</th>" + "".join(f"<th>{esc(s)}</th>" for s in slots) + "</tr>"
    rows = []
    for t in all_times:
        cells = f"<td class='col-name'>{esc(t)}</td>"
        for s in slots:
            v = bw_by_slot[s].get(t, 0)
            cells += f"<td>{v:,.1f}</td>"
        rows.append(f"<tr>{cells}</tr>")
    return f'<table><thead>{th}</thead><tbody>{"".join(rows)}</tbody></table>'


HTML_TEMPLATE = """<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Mooncake Cluster Diagnostics</title>
<style>__CSS__</style></head><body>
<header>
  <h1>Mooncake Cluster Diagnostics</h1>
  <div class="meta">
    <span>work dir: __WORK_DIR__</span>
    <span>window: __TIME_WINDOW__</span>
    <span>generated: __GENERATED_AT__</span>
    <span>targets: __TARGETS_COUNT__</span>
    <span>log files: __LOG_FILES_COUNT__</span>
    <span>samples: __SAMPLES_COUNT__</span>
  </div>
</header>
<main>
__OVERVIEW_CARDS__
__EXEC_SECTION__
__SPDIAG_OVERVIEW__
__SPDIAG_TOP_SLOW__
__QPS_SECTION__
__QPS_TABLE_SECTION__
__BANDWIDTH_SECTION__
__GET_INTO_CHART_SECTION__
__OP_SUMMARY_SECTION__
__POD_FILE_STATS_SECTION__
__SLOW_REQUESTS_SECTION__
__PER_TARGET_SECTION__
</main>
<script>__SCRIPT__</script>
</body></html>
"""


def render_html(
    *,
    output: Path,
    work_dir: Path,
    since: datetime | None,
    until: datetime | None,
    snapshots: list[TargetSnapshot],
    samples: list[Sample],
    bench_requests: list[BenchRequest],
    pod_file_stats: list[PodFileStat],
    op_summary: list[dict],
    qps_by_slot: dict[str, dict[str, int]],
    bandwidth_by_slot: dict[str, dict[str, float]],
    get_into_chart: list[dict],
    chart_stride: int,
    slow_requests: list[dict],
    latency_overview: dict,
    correlated_count: int,
    parse_counts: dict[str, int],
    chunk_size: int,
) -> None:
    """Render the single self-contained HTML report."""
    time_desc = "all time"
    if since and until:
        time_desc = f"{since.isoformat(sep=' ')} -> {until.isoformat(sep=' ')}"
    elif since:
        time_desc = f"since {since.isoformat(sep=' ')}"
    elif until:
        time_desc = f"until {until.isoformat(sep=' ')}"

    total_targets = len(snapshots)
    ok_spdiag = sum(1 for s in snapshots if s.spdiag_rc == 0 and s.spdiag_rows)
    fail_spdiag = sum(1 for s in snapshots if s.spdiag_rc != 0)
    total_log_files = sum(len(s.log_files) for s in snapshots)
    total_perf_points = sum(len(s.spdiag_rows) for s in snapshots)

    # Overview cards
    cards = ['<div class="cards">']
    cards.append(f'<div class="card"><b>{total_targets}</b><span>targets</span></div>')
    cards.append(f'<div class="card"><b>{ok_spdiag}</b><span>spdiag ok</span></div>')
    cards.append(f'<div class="card"><b>{fail_spdiag}</b><span>spdiag failed</span></div>')
    cards.append(f'<div class="card"><b>{total_log_files}</b><span>log files</span></div>')
    cards.append(f'<div class="card"><b>{total_perf_points}</b><span>perf points</span></div>')
    cards.append(f'<div class="card"><b>{latency_overview["request_count"]:,}</b><span>get requests</span></div>')
    cards.append(f'<div class="card"><b>{latency_overview["success_count"]:,}</b><span>successful</span></div>')
    cards.append(f'<div class="card"><b>{latency_overview["failed_count"]:,}</b><span>failed</span></div>')
    cards.append(f'<div class="card"><b>{correlated_count:,}</b><span>full trace correlation</span></div>')
    cards.append(f'<div class="card"><b>{fmt_us(latency_overview["latency"]["p50"])}</b><span>p50 (us)</span></div>')
    cards.append(f'<div class="card"><b>{fmt_us(latency_overview["latency"]["p99"])}</b><span>p99 (us)</span></div>')
    cards.append(f'<div class="card"><b>{fmt_us(latency_overview["latency"]["max"])}</b><span>max (us)</span></div>')
    cards.append(f'<div class="card"><b>{fmt_bytes(latency_overview["bytes_total"])}</b><span>total bytes</span></div>')
    # average bandwidth
    total_bw = sum(sum(v for v in b.values()) for b in bandwidth_by_slot.values())
    elapsed_s = latency_overview.get("elapsed_s", 1) or 1
    avg_bw = total_bw / elapsed_s if elapsed_s > 0 else 0
    cards.append(f'<div class="card"><b>{avg_bw:,.1f}</b><span>avg bandwidth (MB/s)</span></div>')
    cards.append('</div>')
    overview_cards_html = "\n".join(cards)

    # Exec section (if --exec used)
    exec_section = ""
    exec_snaps = [s for s in snapshots if s.exec_rc is not None]
    if exec_snaps:
        rows_html = []
        for s in exec_snaps:
            rc_cls = "rc-ok" if s.exec_rc == 0 else "rc-fail"
            preview = s.exec_out
            if len(preview) > 4000:
                preview = preview[:4000] + "\n... [truncated]"
            rows_html.append(
                f"<tr><td class='col-name'>{esc(s.target.name)}</td>"
                f"<td>{esc(s.target.kind)}</td>"
                f"<td class='{rc_cls}'>{esc(s.exec_rc)}</td>"
                f"<td><pre>{esc(preview)}</pre></td></tr>"
            )
        exec_section = f"""
<h2>Command Execution Results (--exec)</h2>
<div class="panel"><div class="panel-head"><span>--exec results</span></div>
<div class="panel-body no-pad"><table>
<thead><tr><th>target</th><th>kind</th><th>rc</th><th>output</th></tr></thead>
<tbody>{''.join(rows_html)}</tbody>
</table></div></div>"""

    # spdiag overview
    spdiag_overview_rows = []
    for s in snapshots:
        if not s.spdiag_rows:
            continue
        ticks = sum(r.ticks for r in s.spdiag_rows)
        max_avg = max((r.avg_ns for r in s.spdiag_rows), default=0)
        max_max = max((r.max_ns for r in s.spdiag_rows), default=0)
        programs = sorted({r.program for r in s.spdiag_rows})
        spdiag_overview_rows.append(
            f"<tr><td class='col-name'>{esc(s.target.name)}</td>"
            f"<td>{esc(s.target.kind)}</td>"
            f"<td>{esc(', '.join(programs))}</td>"
            f"<td>{len(s.spdiag_rows)}</td>"
            f"<td>{ticks:,}</td>"
            f"<td>{fmt_us(max_avg)}</td>"
            f"<td>{fmt_us(max_max)}</td></tr>"
        )
    if spdiag_overview_rows:
        spdiag_overview_html = f"""
<h2>spdiag show 概览</h2>
<div class="panel"><div class="panel-head"><span>per-target perf points</span>
<span class="count">{ok_spdiag}/{total_targets} targets</span></div>
<div class="panel-body no-pad"><table>
<thead><tr><th>target</th><th>kind</th><th>program</th><th>points</th>
<th>total ticks</th><th>max avg (us)</th><th>max max (us)</th></tr></thead>
<tbody>{''.join(spdiag_overview_rows)}</tbody>
</table></div></div>"""
    else:
        spdiag_overview_html = """
<h2>spdiag show 概览</h2>
<div class="panel"><div class="panel-body empty">No spdiag show data. Run `spdiag start` on targets first.</div></div>"""

    # spdiag top slow points
    all_rows: list[tuple[str, SpDiagRow]] = []
    for s in snapshots:
        for r in s.spdiag_rows:
            all_rows.append((s.target.name, r))
    all_rows.sort(key=lambda pair: pair[1].avg_ns, reverse=True)
    top_rows_html = []
    for target_name, r in all_rows[:30]:
        top_rows_html.append(
            f"<tr><td class='col-name'>{esc(target_name)}</td>"
            f"<td>{esc(r.program)}</td><td>{esc(r.module)}</td>"
            f"<td>{esc(r.point)}</td><td>{r.level}</td>"
            f"<td>{r.ticks:,}</td><td>{r.good:,}</td><td>{r.bad:,}</td>"
            f"<td>{fmt_us(r.avg_ns)}</td><td>{fmt_us(r.min_ns)}</td>"
            f"<td>{fmt_us(r.max_ns)}</td><td>{fmt_us(r.p99_ns)}</td>"
            f"<td>{fmt_us(r.p999_ns)}</td><td>{fmt_us(r.p9999_ns)}</td></tr>"
        )
    if top_rows_html:
        spdiag_top_html = f"""
<div class="panel"><div class="panel-head"><span>top 30 slowest perf points (by avg)</span>
<span class="count">{len(all_rows)} total</span></div>
<div class="panel-body no-pad"><table>
<thead><tr><th>target</th><th>program</th><th>module</th><th>point</th>
<th>lvl</th><th>ticks</th><th>good</th><th>bad</th>
<th>avg (us)</th><th>min (us)</th><th>max (us)</th>
<th>p99 (us)</th><th>p999 (us)</th><th>p9999 (us)</th></tr></thead>
<tbody>{''.join(top_rows_html)}</tbody>
</table></div></div>"""
    else:
        spdiag_top_html = ""

    # QPS chart data
    qps_json = json.dumps(qps_by_slot, ensure_ascii=False)

    # bandwidth chart data
    bw_json = json.dumps(bandwidth_by_slot, ensure_ascii=False)

    # get_into chart data
    get_into_json = json.dumps(get_into_chart, ensure_ascii=False)

    # QPS section
    qps_section = f"""
<h2>QPS by slot</h2>
<div class="panel"><div class="panel-body">
<div id="qps-legend" class="legend"></div>
<div class="chart-wrap"><svg id="qps-svg"></svg><div id="qps-tip" class="tooltip"></div></div>
</div></div>"""

    # QPS per-second table
    qps_table_html = _render_qps_table(qps_by_slot)
    qps_table_section = f"""
<h2>每秒 QPS 明细表</h2>
<div class="panel"><div class="panel-body no-pad">
{qps_table_html}
</div></div>"""

    # Bandwidth section (chart + table)
    bw_section = f"""
<h2>Bandwidth by slot (MB/s)</h2>
<div class="panel"><div class="panel-body">
<div id="bw-legend" class="legend"></div>
<div class="chart-wrap"><svg id="bw-svg"></svg><div id="bw-tip" class="tooltip"></div></div>
</div></div>"""
    bw_table_html = _render_bandwidth_table(bandwidth_by_slot)
    bw_section += f"""
<div class="panel"><div class="panel-head"><span>每秒带宽明细表 (MB/s)</span></div>
<div class="panel-body no-pad">
{bw_table_html}
</div></div>"""

    # get_into chart section
    get_into_section = f"""
<h2>get_into 耗时拆解 (us)</h2>
<div class="panel"><div class="panel-body">
<div id="get-legend" class="legend"></div>
<div id="get-charts"></div>
<div class="muted" style="margin-top:8px">
  按 __CHART_STRIDE__ 个请求抽样（目标最多 {CHART_MAX_POINTS:,} 个基础点），
  并强制加入全局耗时最高的 {CHART_SLOW_POINTS} 个请求。
  按住鼠标左键拖拽框选放大，双击恢复。
</div>
</div></div>"""

    # Op summary section
    op_rows_html = []
    for row in op_summary:
        op_rows_html.append(
            f"<tr><td class='col-name'>{esc(row['op'])}</td>"
            f"<td>{esc(row['segment'])}</td>"
            f"<td>{row['count']:,}</td>"
            f"<td>{row['avg']:,.1f}</td>"
            f"<td>{row['p50']:,.1f}</td>"
            f"<td>{row['p95']:,.1f}</td>"
            f"<td>{row['p99']:,.1f}</td>"
            f"<td>{row['p999']:,.1f}</td>"
            f"<td>{row['p9999']:,.1f}</td>"
            f"<td>{row['min']:,.1f}</td>"
            f"<td>{row['max']:,.1f}</td></tr>"
        )
    op_summary_section = f"""
<h2>8 类操作 Summary Statistics (us)</h2>
<div class="panel"><div class="panel-body no-pad"><table>
<thead><tr><th>operation</th><th>segment</th><th>count</th><th>avg</th>
<th>p50</th><th>p95</th><th>p99</th><th>p999</th><th>p9999</th>
<th>min</th><th>max</th></tr></thead>
<tbody>{''.join(op_rows_html) or "<tr><td colspan='11' class='empty'>no breakdown logs found</td></tr>"}</tbody>
</table></div></div>"""

    # Pod file stats section
    pod_rows_html = []
    for p in pod_file_stats:
        chunk_keys_str = ", ".join(sorted(p.chunk_keys)[:3])
        if len(p.chunk_keys) > 3:
            chunk_keys_str += f", ... (+{len(p.chunk_keys)-3} more)"
        other_samples_str = ", ".join(p.other_key_samples[:3])
        if len(p.other_key_samples) > 3:
            other_samples_str += f", ... (+{len(p.other_key_samples)-3} more)"
        pod_rows_html.append(
            f"<tr><td class='col-name'>{esc(p.target)}</td>"
            f"<td>{esc(p.slot)}</td>"
            f"<td>{p.total_files:,}</td>"
            f"<td>{p.chunk_files:,}</td>"
            f"<td>{fmt_bytes(p.chunk_bytes)}</td>"
            f"<td>{len(p.chunk_keys):,}</td>"
            f"<td>{p.other_files:,}</td>"
            f"<td>{fmt_bytes(p.other_bytes_known)}</td>"
            f"<td style='max-width:280px;overflow:hidden;text-overflow:ellipsis'>{esc(chunk_keys_str)}</td>"
            f"<td style='max-width:280px;overflow:hidden;text-overflow:ellipsis'>{esc(other_samples_str)}</td></tr>"
        )
    pod_file_stats_section = f"""
<h2>Per-pod file stats (get class)</h2>
<div class="panel"><div class="panel-head"><span>每个 pod get 了多少文件，#c# chunk vs 其他</span>
<span class="count">chunk size = {fmt_bytes(chunk_size)}</span></div>
<div class="panel-body no-pad"><table>
<thead><tr><th>target</th><th>slot</th><th>total files</th>
<th>chunk files (#c#)</th><th>chunk bytes</th><th>unique chunk keys</th>
<th>other files</th><th>other bytes (known)</th>
<th>sample chunk keys</th><th>sample other keys</th></tr></thead>
<tbody>{''.join(pod_rows_html) or "<tr><td colspan='10' class='empty'>no get-class logs found</td></tr>"}</tbody>
</table></div></div>"""

    # Slow requests section
    slow_rows_html = []
    for row in slow_requests:
        slow_rows_html.append(
            f"<tr><td>{row['rank']}</td>"
            f"<td>{esc(row['time'])}</td>"
            f"<td class='col-name'>{esc(row['slot'])}</td>"
            f"<td>{esc(row['trace_id'])}</td>"
            f"<td style='max-width:280px;overflow:hidden;text-overflow:ellipsis'>{esc(row['key'])}</td>"
            f"<td>{esc(row['remote'])}</td>"
            + "".join(f"<td>{fmt_us(row.get(k))}</td>" for k in BENCH_STAGES)
            + f"<td>{fmt_us(row.get('rpc_pool_lookup_us'))}</td>"
            + f"<td>{fmt_us(row.get('rpc_rpc_call_us'))}</td>"
            + f"<td>{fmt_us(row.get('rpc_result_get_us'))}</td>"
            + f"<td>{fmt_us(row.get('storage_queue_us'))}</td>"
            + f"<td>{fmt_us(row.get('storage_file_open_us'))}</td>"
            + f"<td>{fmt_us(row.get('storage_disk_read_us'))}</td>"
            + f"<td>{fmt_us(row.get('storage_total_us'))}</td>"
            + f"<td>{fmt_us(row.get('storage_release_us'))}</td>"
            + f"<td>{fmt_us(row.get('master_pool_lookup_us'))}</td>"
            + f"<td>{fmt_us(row.get('master_rpc_call_us'))}</td>"
            + f"<td>{fmt_us(row.get('master_result_get_us'))}</td>"
            + f"<td>{fmt_us(row.get('master_rpc_total_us'))}</td>"
            + f"<td>{fmt_us(row.get('master_server_us'))}</td></tr>"
        )
    slow_requests_section = f"""
<h2>最慢 {SLOW_TRACE_COUNT} 个请求：跨角色关联 (trace_id)</h2>
<div class="panel"><div class="panel-body no-pad"><table>
<thead><tr><th>rank</th><th>time</th><th>slot</th><th>trace id</th><th>key</th><th>remote</th>
<th>query</th><th>select</th><th>read</th><th>offload RPC</th><th>transfer</th>
<th>release</th><th>overhead</th><th>total</th>
<th>RPC pool</th><th>RPC call</th><th>RPC result</th>
<th>ST queue</th><th>ST file open</th><th>ST disk read</th><th>ST total</th><th>ST release</th>
<th>M pool</th><th>M rpc</th><th>M result</th><th>M total</th><th>M server</th>
</tr></thead>
<tbody>{''.join(slow_rows_html) or "<tr><td colspan='27' class='empty'>no get_into_breakdown records</td></tr>"}</tbody>
</table></div></div>"""

    # Per-target section
    per_target_parts = ['<h2>Per-Target Details</h2>']
    for s in snapshots:
        badge_cls = "k8s" if s.target.kind == "k8s_pod" else "ssh"
        per_target_parts.append(f'<div class="target-section">')
        per_target_parts.append(
            f"<h3>{esc(s.target.name)} "
            f"<span class='badge {badge_cls}'>{esc(s.target.kind)}</span></h3>"
        )
        if s.spdiag_raw:
            rc_cls = "rc-ok" if s.spdiag_rc == 0 else "rc-fail"
            per_target_parts.append(
                f"<div class='panel'><div class='panel-head'>"
                f"<span>spdiag show</span>"
                f"<span class='count'>rc=<span class='{rc_cls}'>{s.spdiag_rc}</span>"
                f" · {len(s.spdiag_rows)} rows</span></div>"
                f"<div class='panel-body'><pre>{esc(s.spdiag_raw)}</pre></div></div>"
            )
        if s.log_files:
            file_rows = []
            for f in s.log_files[:50]:
                try:
                    size = f.stat().st_size
                except OSError:
                    size = -1
                file_rows.append(
                    f"<tr><td class='col-name'>{esc(f)}</td><td>{size:,}</td></tr>"
                )
            extra = ""
            if len(s.log_files) > 50:
                extra = (f"<tr><td colspan='2' class='muted'>"
                         f"... and {len(s.log_files) - 50} more</td></tr>")
            per_target_parts.append(
                f"<div class='panel'><div class='panel-head'>"
                f"<span>collected log files</span>"
                f"<span class='count'>{len(s.log_files)} files</span></div>"
                f"<div class='panel-body no-pad'><table>"
                f"<thead><tr><th>path</th><th>size</th></tr></thead>"
                f"<tbody>{''.join(file_rows)}{extra}</tbody></table></div></div>"
            )
        if s.error:
            per_target_parts.append(
                f"<div class='panel'><div class='panel-body'>"
                f"<span class='rc-fail'>error: {esc(s.error)}</span>"
                f"</div></div>"
            )
        per_target_parts.append('</div>')
    per_target_section = "\n".join(per_target_parts)

    # JavaScript for charts
    script = _build_script(qps_json, bw_json, get_into_json, chart_stride)

    # Assemble
    html_text = HTML_TEMPLATE
    replacements = {
        "__CSS__": CSS,
        "__WORK_DIR__": esc(work_dir),
        "__TIME_WINDOW__": esc(time_desc),
        "__GENERATED_AT__": esc(datetime.now().isoformat(sep=" ")),
        "__TARGETS_COUNT__": str(total_targets),
        "__LOG_FILES_COUNT__": str(total_log_files),
        "__SAMPLES_COUNT__": f"{len(samples):,}",
        "__OVERVIEW_CARDS__": overview_cards_html,
        "__EXEC_SECTION__": exec_section,
        "__SPDIAG_OVERVIEW__": spdiag_overview_html,
        "__SPDIAG_TOP_SLOW__": spdiag_top_html,
        "__QPS_SECTION__": qps_section,
        "__QPS_TABLE_SECTION__": qps_table_section,
        "__BANDWIDTH_SECTION__": bw_section,
        "__GET_INTO_CHART_SECTION__": get_into_section.replace(
            "__CHART_STRIDE__", str(chart_stride)
        ),
        "__OP_SUMMARY_SECTION__": op_summary_section,
        "__POD_FILE_STATS_SECTION__": pod_file_stats_section,
        "__SLOW_REQUESTS_SECTION__": slow_requests_section,
        "__PER_TARGET_SECTION__": per_target_section,
        "__SCRIPT__": script,
    }
    for marker, value in replacements.items():
        html_text = html_text.replace(marker, value)
    output.write_text(html_text, encoding="utf-8")


def _build_script(qps_json: str, bw_json: str, get_into_json: str, chart_stride: int) -> str:
    return f"""
const QPS_DATA = {qps_json};
const BW_DATA = {bw_json};
const GET_INTO_DATA = {get_into_json};
const STRIDE = {chart_stride};
const COLORS = ["#1769aa","#e07a1f","#27915a","#a23eb0","#d14a61","#6b7280","#00a5a5","#7357c8"];
const STAGES = {list(GET_INTO_STAGES)};
const STAGE_LABELS = {dict(GET_INTO_STAGE_LABEL)};

function esc(s) {{
  return String(s ?? "").replace(/[&<>"']/g, c => ({{"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}}[c]));
}}

function attachHover(svg, tip, count, xAt, htmlAt) {{
  svg.addEventListener("mousemove", e => {{
    if (!count) return;
    const r = svg.getBoundingClientRect();
    const vw = svg.viewBox.baseVal.width || 1200;
    const vx = (e.clientX - r.left) * vw / r.width;
    let i = Math.round((vx - 58) * Math.max(1, count - 1) / (vw - 58 - 18));
    i = Math.max(0, Math.min(count - 1, i));
    tip.innerHTML = htmlAt(i);
    tip.style.display = "block";
    tip.style.left = Math.min(e.offsetX + 14, r.width - 400) + "px";
    tip.style.top = Math.max(4, e.offsetY - 30) + "px";
  }});
  svg.addEventListener("mouseleave", () => tip.style.display = "none");
}}

function drawQps() {{
  const svg = document.getElementById("qps-svg");
  const qps = QPS_DATA || {{}};
  const slots = Object.keys(qps);
  if (!slots.length) {{ svg.parentElement.innerHTML = "<div class='empty'>no get_into data</div>"; return; }}
  const times = [...new Set(slots.flatMap(s => Object.keys(qps[s])))].sort();
  const tip = document.getElementById("qps-tip");
  const W = 1200, H = 330, p = {{l:58, r:18, t:16, b:42}};
  svg.setAttribute("viewBox", `0 0 ${{W}} ${{H}}`);
  const ymax = Math.max(1, ...slots.flatMap(s => Object.values(qps[s])));
  const x = i => p.l + i * (W - p.l - p.r) / Math.max(1, times.length - 1);
  const y = v => H - p.b - v * (H - p.t - p.b) / ymax;
  let out = `<line x1="${{p.l}}" y1="${{H-p.b}}" x2="${{W-p.r}}" y2="${{H-p.b}}" stroke="#9aa8b8"/>`;
  out += `<line x1="${{p.l}}" y1="${{p.t}}" x2="${{p.l}}" y2="${{H-p.b}}" stroke="#9aa8b8"/>`;
  for (let i = 0; i <= 5; i++) {{
    const v = ymax * i / 5, yy = y(v);
    out += `<line x1="${{p.l}}" y1="${{yy}}" x2="${{W-p.r}}" y2="${{yy}}" stroke="#e5ebf2"/>`;
    out += `<text x="${{p.l-7}}" y="${{yy+4}}" text-anchor="end" font-size="11">${{Math.round(v)}}</text>`;
  }}
  slots.forEach((s, si) => {{
    const pts = times.map((t, i) => `${{x(i)}},${{y(qps[s][t] || 0)}}`).join(" ");
    out += `<polyline points="${{pts}}" fill="none" stroke="${{COLORS[si % COLORS.length]}}" stroke-width="2"/>`;
  }});
  if (times.length) {{
    out += `<text x="${{p.l}}" y="${{H-12}}" font-size="11">${{times[0]}}</text>`;
    out += `<text x="${{W-p.r}}" y="${{H-12}}" text-anchor="end" font-size="11">${{times[times.length-1]}}</text>`;
  }}
  svg.innerHTML = out;
  document.getElementById("qps-legend").innerHTML = slots.map((s, i) =>
    `<span style="color:${{COLORS[i % COLORS.length]}}">● ${{esc(s)}}</span>`).join("");
  attachHover(svg, tip, times.length, x, i =>
    `<b>${{times[i]}}</b><br>` + slots.map((s, si) =>
      `<span style="color:${{COLORS[si % COLORS.length]}}">●</span> ${{s}}: <b>${{qps[s][times[i]] || 0}} QPS</b>`).join("<br>"));
}}

function drawBandwidth() {{
  const svg = document.getElementById("bw-svg");
  const bw = BW_DATA || {{}};
  const slots = Object.keys(bw);
  if (!slots.length) {{ svg.parentElement.innerHTML = "<div class='empty'>no bandwidth data</div>"; return; }}
  const times = [...new Set(slots.flatMap(s => Object.keys(bw[s])))].sort();
  const tip = document.getElementById("bw-tip");
  const W = 1200, H = 330, p = {{l:58, r:18, t:16, b:42}};
  svg.setAttribute("viewBox", `0 0 ${{W}} ${{H}}`);
  const ymax = Math.max(1, ...slots.flatMap(s => Object.values(bw[s])));
  const x = i => p.l + i * (W - p.l - p.r) / Math.max(1, times.length - 1);
  const y = v => H - p.b - v * (H - p.t - p.b) / ymax;
  let out = `<line x1="${{p.l}}" y1="${{H-p.b}}" x2="${{W-p.r}}" y2="${{H-p.b}}" stroke="#9aa8b8"/>`;
  out += `<line x1="${{p.l}}" y1="${{p.t}}" x2="${{p.l}}" y2="${{H-p.b}}" stroke="#9aa8b8"/>`;
  for (let i = 0; i <= 5; i++) {{
    const v = ymax * i / 5, yy = y(v);
    out += `<line x1="${{p.l}}" y1="${{yy}}" x2="${{W-p.r}}" y2="${{yy}}" stroke="#e5ebf2"/>`;
    out += `<text x="${{p.l-7}}" y="${{yy+4}}" text-anchor="end" font-size="11">${{(v).toFixed(1)}}</text>`;
  }}
  slots.forEach((s, si) => {{
    const pts = times.map((t, i) => `${{x(i)}},${{y(bw[s][t] || 0)}}`).join(" ");
    out += `<polyline points="${{pts}}" fill="none" stroke="${{COLORS[si % COLORS.length]}}" stroke-width="2"/>`;
  }});
  if (times.length) {{
    out += `<text x="${{p.l}}" y="${{H-12}}" font-size="11">${{times[0]}}</text>`;
    out += `<text x="${{W-p.r}}" y="${{H-12}}" text-anchor="end" font-size="11">${{times[times.length-1]}}</text>`;
  }}
  svg.innerHTML = out;
  document.getElementById("bw-legend").innerHTML = slots.map((s, i) =>
    `<span style="color:${{COLORS[i % COLORS.length]}}">● ${{esc(s)}}</span>`).join("");
  attachHover(svg, tip, times.length, x, i =>
    `<b>${{times[i]}}</b><br>` + slots.map((s, si) =>
      `<span style="color:${{COLORS[si % COLORS.length]}}">●</span> ${{s}}: <b>${{(bw[s][times[i]] || 0).toFixed(1)}} MB/s</b>`).join("<br>"));
}}

function drawGetInto() {{
  const all = GET_INTO_DATA || [];
  const host = document.getElementById("get-charts");
  if (!all.length) {{ host.innerHTML = "<div class='empty'>no get_into_breakdown records</div>"; return; }}
  const labels = STAGE_LABELS;
  document.getElementById("get-legend").innerHTML = STAGES.map((s, i) =>
    `<span style="color:${{COLORS[i]}}">● ${{labels[s]}}</span>`).join("");
  const slots = [...new Set(all.map(v => v.slot))].sort((a, b) =>
    Number(a.replace(/\\D/g, "")) - Number(b.replace(/\\D/g, "")));
  const niceMax = v => {{
    if (v <= 0) return 1;
    const power = 10 ** Math.floor(Math.log10(v));
    const n = v / power;
    return (n <= 1 ? 1 : n <= 2 ? 2 : n <= 5 ? 5 : 10) * power;
  }};
  slots.forEach(slot => {{
    const pts = all.filter(v => v.slot === slot);
    const section = document.createElement("div");
    section.className = "slot-chart";
    section.innerHTML = `<h3>${{esc(slot)}} · ${{pts.length.toLocaleString()}} plotted points</h3>
      <div class="chart-wrap"><svg></svg><div class="tooltip"></div></div>`;
    host.appendChild(section);
    const svg = section.querySelector("svg");
    const tip = section.querySelector(".tooltip");
    const H = 300, p = {{l:68, r:18, t:16, b:44}};
    let zoomStart = 0, zoomEnd = Math.max(0, pts.length - 1), dragX = null;
    let W = 1200, currentVisible = [], currentX = () => 0, currentY = () => 0;
    const updateWidth = () => {{
      W = Math.max(700, Math.round(svg.getBoundingClientRect().width || 1200));
      svg.setAttribute("viewBox", `0 0 ${{W}} ${{H}}`);
    }};
    const toViewX = e => {{
      const r = svg.getBoundingClientRect();
      return (e.clientX - r.left) * W / r.width;
    }};
    const toViewY = e => {{
      const r = svg.getBoundingClientRect();
      return (e.clientY - r.top) * H / r.height;
    }};
    function render() {{
      updateWidth();
      const visible = pts.slice(zoomStart, zoomEnd + 1);
      const rawMax = Math.max(1, ...visible.map(v => v.total_us || 0));
      const ymax = niceMax(rawMax);
      const x = i => p.l + i * (W - p.l - p.r) / Math.max(1, visible.length - 1);
      const y = v => H - p.b - v * (H - p.t - p.b) / ymax;
      currentVisible = visible; currentX = x; currentY = y;
      let out = `<line x1="${{p.l}}" y1="${{H-p.b}}" x2="${{W-p.r}}" y2="${{H-p.b}}" stroke="#9aa8b8"/>`;
      out += `<line x1="${{p.l}}" y1="${{p.t}}" x2="${{p.l}}" y2="${{H-p.b}}" stroke="#9aa8b8"/>`;
      for (let i = 0; i <= 5; i++) {{
        const v = ymax * i / 5, yy = y(v);
        out += `<line x1="${{p.l}}" y1="${{yy}}" x2="${{W-p.r}}" y2="${{yy}}" stroke="#e5ebf2"/>`;
        out += `<text x="${{p.l-8}}" y="${{yy+4}}" text-anchor="end" font-size="11">${{Math.round(v).toLocaleString()}}</text>`;
      }}
      STAGES.forEach((s, si) => {{
        out += `<polyline points="${{visible.map((v, i) => `${{x(i)}},${{y(v[s] || 0)}}`).join(" ")}}" fill="none" stroke="${{COLORS[si]}}" stroke-width="1.4" opacity=".88"/>`;
      }});
      if (visible.length) {{
        for (let i = 0; i < 5; i++) {{
          const idx = Math.round(i * (visible.length - 1) / 4);
          const xx = x(idx);
          out += `<text x="${{xx}}" y="${{H-13}}" text-anchor="${{i === 0 ? "start" : i === 4 ? "end" : "middle"}}" font-size="11">${{esc(visible[idx].time)}}</text>`;
        }}
      }}
      out += `<rect class="get-selection" x="0" y="${{p.t}}" width="0" height="${{H-p.t-p.b}}" fill="#1769aa" opacity=".18"/>`;
      svg.innerHTML = out;
    }}
    svg.addEventListener("mousedown", e => {{
      if (toViewX(e) >= p.l) {{ dragX = toViewX(e); tip.style.display = "none"; e.preventDefault(); }}
    }});
    svg.addEventListener("mousemove", e => {{
      const vx = toViewX(e), vy = toViewY(e), visible = currentVisible;
      if (dragX !== null) {{
        const rect = svg.querySelector(".get-selection");
        const a = Math.max(p.l, Math.min(dragX, vx));
        const b = Math.min(W - p.r, Math.max(dragX, vx));
        rect.setAttribute("x", a);
        rect.setAttribute("width", Math.max(0, b - a));
        return;
      }}
      if (!visible.length || vx < p.l || vx > W - p.r || vy < p.t || vy > H - p.b) {{
        tip.style.display = "none"; return;
      }}
      let center = Math.round((vx - p.l) * Math.max(1, visible.length - 1) / (W - p.l - p.r));
      let best = null;
      for (let i = Math.max(0, center - 2); i <= Math.min(visible.length - 1, center + 2); i++) {{
        for (const s of STAGES) {{
          const dx = currentX(i) - vx, dy = currentY(visible[i][s] || 0) - vy;
          const d = Math.hypot(dx, dy);
          if (!best || d < best.d) best = {{i, d}};
        }}
      }}
      if (!best || best.d > 9) {{ tip.style.display = "none"; return; }}
      const v = visible[best.i];
      const wrap = svg.parentElement, wr = wrap.getBoundingClientRect();
      const mx = e.clientX - wr.left, my = e.clientY - wr.top;
      tip.innerHTML = `<b>${{esc(v.time)}} · ${{esc(v.slot)}}</b><br>` +
        `trace_id: ${{esc(v.trace_id)}}<br>key: ${{esc(v.key)}}<br>` +
        STAGES.map((s, si) =>
          `<span style="color:${{COLORS[si]}}">●</span> ${{labels[s]}}: <b>${{Number(v[s] || 0).toLocaleString()}} us</b>`).join("<br>") +
        `<br>total: <b>${{Number(v.total_us || 0).toLocaleString()}} us</b>` +
        `<code>${{esc(v.log)}}</code>`;
      tip.style.display = "block";
      const gap = 24, tw = tip.offsetWidth, th = tip.offsetHeight;
      const useRB = mx < wr.width / 2;
      tip.style.left = Math.max(4, Math.min(wr.width - tw - 4, useRB ? mx + gap : mx - gap - tw)) + "px";
      tip.style.top = Math.max(4, Math.min(wr.height - th - 4, useRB ? my + gap : my - gap - th)) + "px";
    }});
    svg.addEventListener("mouseup", e => {{
      if (dragX === null) return;
      const endX = toViewX(e);
      const a = Math.max(p.l, Math.min(dragX, endX));
      const b = Math.min(W - p.r, Math.max(dragX, endX));
      const count = zoomEnd - zoomStart + 1;
      if (b - a > 8 && count > 2) {{
        const oldStart = zoomStart;
        zoomStart = oldStart + Math.floor((a - p.l) * Math.max(1, count - 1) / (W - p.l - p.r));
        zoomEnd = Math.min(pts.length - 1, oldStart + Math.ceil((b - p.l) * Math.max(1, count - 1) / (W - p.l - p.r)));
        zoomEnd = Math.max(zoomStart + 1, zoomEnd);
      }}
      dragX = null;
      render();
    }});
    svg.addEventListener("mouseleave", () => {{
      tip.style.display = "none";
      if (dragX !== null) {{ dragX = null; render(); }}
    }});
    svg.addEventListener("dblclick", () => {{
      zoomStart = 0; zoomEnd = Math.max(0, pts.length - 1);
      tip.style.display = "none"; render();
    }});
    render();
  }});
}}

drawQps();
drawBandwidth();
drawGetInto();
"""


# ===========================================================================
# CLI
# ===========================================================================
def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Cluster-wide Mooncake diagnostics: collect + analyze + single HTML.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Edit the CONFIG section at the top of this file, then run:\n"
            "  python cluster_mooncake_diag.py\n\n"
            "Time window:\n"
            "  --since 30min | --since '2026-08-05 10:00:00'\n"
            "  --until '2026-08-05 11:00:00'\n\n"
            "Exec mode (only run a command, no analysis):\n"
            "  --exec 'spdiag start'\n"
            "  --exec 'spdiag clear'\n"
        ),
    )
    p.add_argument("-w", "--work-dir", type=Path,
                   default=Path("./cluster_diag_work"),
                   help="Working dir for collected files and reports.")
    p.add_argument("-o", "--output", type=Path,
                   default=Path("cluster_diag_report.html"),
                   help="Output HTML path.")
    p.add_argument("--since", default=None,
                   help="Start of time window. Relative (30min, 2h, 90s) "
                        "or absolute (YYYY-MM-DD HH:MM:SS).")
    p.add_argument("--until", default=None,
                   help="End of time window (absolute only).")
    p.add_argument("--exec", dest="exec_cmd", default=None,
                   help="Only execute this command on every target, then exit. "
                        "Example: --exec 'spdiag start'")
    p.add_argument("--analyze-only", action="store_true",
                   help="Skip collection, analyze already-collected logs under --work-dir.")
    p.add_argument("--no-spdiag", action="store_true",
                   help="Skip running spdiag show.")
    p.add_argument("--no-logs", action="store_true",
                   help="Skip pulling logs.")
    p.add_argument("--exec-timeout", type=int, default=60,
                   help="Timeout (s) for remote commands. Default 60.")
    p.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE,
                   help=f"Chunk size in bytes. Default {DEFAULT_CHUNK_SIZE}.")
    p.add_argument("--chunk-marker", default=DEFAULT_CHUNK_MARKER,
                   help=f"Chunk key marker. Default {DEFAULT_CHUNK_MARKER!r}.")
    p.add_argument("--path-separator", default=DEFAULT_PATH_SEPARATOR,
                   help=f"Chunk path separator. Default {DEFAULT_PATH_SEPARATOR!r}.")
    p.add_argument("--chunk-style", default=DEFAULT_CHUNK_STYLE,
                   choices=["auto", "offset-marker", "path-index"],
                   help=f"Chunk key style. Default {DEFAULT_CHUNK_STYLE!r}.")
    p.add_argument("--sample", action="store_true",
                   help="Generate sample HTML using synthetic data.")
    return p


def cmd_sample(args) -> int:
    """Generate a sample HTML report using synthetic data."""
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    # Synthetic targets
    targets = [
        Target(name="slot1_master", kind="k8s_pod", namespace="default",
               pod="mooncake-master-xxxxx", log_path="/var/log/mooncake"),
        Target(name="slot2_store", kind="k8s_pod", namespace="default",
               pod="mooncake-store-yyyyy", log_path="/var/log/mooncake"),
        Target(name="supernode-10.0.1.5", kind="supernode", host="10.0.1.5",
               log_path="/var/log/mooncake"),
    ]

    def make_rows(program: str, points: list[tuple[str, str, int, int, int]]) -> list[SpDiagRow]:
        out = []
        for module, point, ticks, avg_ns, max_ns in points:
            out.append(SpDiagRow(
                program=program, module=module, point=point,
                level=3, ticks=ticks, good=ticks, bad=0, not_done=0,
                total_ns=ticks * avg_ns, avg_ns=avg_ns,
                min_ns=max(100, avg_ns // 3), max_ns=max_ns,
                p99_ns=int(avg_ns * 1.5), p999_ns=int(avg_ns * 2.0),
                p9999_ns=int(avg_ns * 3.0),
            ))
        return out

    snapshots = [
        TargetSnapshot(
            target=targets[0],
            spdiag_rows=make_rows("mooncake_master", [
                ("rpc_service.cpp::PutStart", "PutStart", 12500, 8500, 42000),
                ("rpc_service.cpp::PutEnd", "PutEnd", 12500, 12300, 58000),
                ("rpc_service.cpp::GetReplicaList", "GetReplicaList", 24800, 4200, 18900),
                ("master_service.cpp::AllocateAndInsertMetadata", "AllocateMemory", 12500, 2100, 8800),
                ("master_service.cpp::PutStart", "AcquireShardLock", 12500, 180, 950),
            ]),
            spdiag_raw=(
                "spdiag started (SHM: /spdiag_shm_default)\n"
                "#  Program         Module                                       Point              Lvl  Ticks  Good  Bad  Not  Total(ns)  Avg(ns)  Min(ns)  Max(ns)  P99      P999     P9999\n"
                "1  mooncake_master  rpc_service.cpp::PutStart                    PutStart           3    12500  12500 0    0    106250000  8500     1200     42000    12750    18000    31000\n"
                "2  mooncake_master  rpc_service.cpp::PutEnd                      PutEnd             3    12500  12500 0    0    153750000  12300    2100     58000    18450    26000    44000\n"
                "3  mooncake_master  rpc_service.cpp::GetReplicaList              GetReplicaList     3    24800  24800 0    0    104160000  4200     800      18900    6300     8900     15000\n"
            ),
            spdiag_rc=0,
            log_files=[
                Path("/tmp/cluster_logs/slot1_master/mooncake_master.log.INFO.20260805-101530.12345"),
            ],
        ),
        TargetSnapshot(
            target=targets[1],
            spdiag_rows=make_rows("mooncake_store", [
                ("real_client.cpp::get_into_internal", "GetIntoInternal", 25000, 45000, 230000),
                ("real_client.cpp::get_buffer_internal", "AllocBuffer", 25000, 3200, 18500),
                ("real_client.cpp::get_buffer_internal", "SSDRead", 18000, 28000, 145000),
                ("client_service.cpp::Get", "TransferGet", 25000, 52000, 260000),
            ]),
            spdiag_raw=(
                "#  Program         Module                                       Point              Lvl  Ticks  Good  Bad  Not  Total(ns)  Avg(ns)  Min(ns)  Max(ns)  P99      P999     P9999\n"
                "1  mooncake_store  real_client.cpp::get_into_internal          GetIntoInternal    3    25000  25000 0    0    1125000000 45000    5200     230000   67500    95000    160000\n"
                "2  mooncake_store  real_client.cpp::get_buffer_internal        AllocBuffer        3    25000  25000 0    0    80000000    3200     800      18500    4800     6700     12000\n"
            ),
            spdiag_rc=0,
            log_files=[
                Path("/tmp/cluster_logs/slot2_store/mooncake_client.log.INFO.20260805-101600.23456"),
            ],
        ),
        TargetSnapshot(
            target=targets[2],
            spdiag_rows=[],
            spdiag_raw="spdiag is not running (SHM: /spdiag_shm_default)\n",
            spdiag_rc=1,
            log_files=[],
            error="spdiag not started",
        ),
    ]

    # Synthetic samples for op summary
    base_ts = datetime(2026, 8, 5, 10, 0, 0)
    samples: list[Sample] = []
    for i in range(500):
        ts = base_ts + timedelta(seconds=i)
        for op in OPS[:4]:  # first 4 ops
            samples.append(Sample(
                op=op, ts=ts,
                fields={"key": f"e2b-dev-fc-templates/abc/rootfs.ext4#c#{i*4194304}",
                        "total_bytes": str(4 * 1024 * 1024),
                        "query_us": str(500 + i % 100),
                        "select_us": str(200 + i % 50),
                        "read_us": str(8000 + i % 2000),
                        "total_us": str(45000 + i % 5000)},
                timings={"query_us": 500 + i % 100, "select_us": 200 + i % 50,
                         "read_us": 8000 + i % 2000, "total_us": 45000 + i % 5000},
                raw_line=f"I20260805 10:00:{i:02d}.000000 trace_id[{i}] {op} ...",
                source_file="synthetic.log", line_no=i, slot="slot1",
                target="slot2_store",
            ))

    # Synthetic bench requests
    bench_requests: list[BenchRequest] = []
    for i in range(2000):
        ts = base_ts + timedelta(milliseconds=i * 30)
        is_chunk = i % 5 != 0  # 80% chunk, 20% other
        if is_chunk:
            key = f"e2b-dev-fc-templates/tmpl-{i%5}/rootfs.ext4#c#{i * 4194304}"
        else:
            key = f"e2b-dev-fc-templates/tmpl-{i%5}/snapfile.bin"
        bench_requests.append(BenchRequest(
            ts=ts, slot="slot2_store", target="slot2_store",
            trace_id=str(1000 + i), key=key,
            fields={"status": "read_ok", "total_bytes": str(4 * 1024 * 1024),
                    "remote_endpoint": "10.0.1.5:50051"},
            timings={
                "query_us": 500 + i % 200,
                "select_us": 200 + i % 80,
                "read_us": 8000 + (i * 17) % 3000,
                "offload_rpc_us": 3000 + (i * 13) % 1500,
                "transfer_data_us": 15000 + (i * 23) % 8000,
                "release_buffer_us": 800 + i % 200,
                "read_overhead_us": 200 + i % 100,
                "total_us": 45000 + (i * 37) % 30000,
            },
            raw_log=f"I20260805 ... trace_id[{1000+i}] get_into_breakdown ...",
        ))

    # Pod file stats
    pod_file_stats = [
        PodFileStat(
            target="slot2_store", slot="slot2",
            total_files=2000, chunk_files=1600, other_files=400,
            chunk_bytes=1600 * 4 * 1024 * 1024,
            other_bytes_known=400 * 1024 * 1024,
            chunk_keys={"e2b-dev-fc-templates/tmpl-0/rootfs.ext4",
                        "e2b-dev-fc-templates/tmpl-1/rootfs.ext4",
                        "e2b-dev-fc-templates/tmpl-2/rootfs.ext4",
                        "e2b-dev-fc-templates/tmpl-3/rootfs.ext4",
                        "e2b-dev-fc-templates/tmpl-4/rootfs.ext4"},
            other_key_samples=["e2b-dev-fc-templates/tmpl-0/snapfile.bin",
                              "e2b-dev-fc-templates/tmpl-1/memfile.bin"],
        ),
    ]

    op_summary = aggregate_op_summary(samples)
    qps_by_slot = aggregate_qps_by_slot(samples)
    bandwidth_by_slot = aggregate_bandwidth_by_slot(samples, chunk_size=args.chunk_size)
    get_into_chart, chart_stride = aggregate_get_into_chart(bench_requests)
    slow_requests = aggregate_slowest_requests(
        bench_requests,
        rpc={}, storage_read={}, storage_rpc={}, release={}, master_rpc={},
    )
    latency_overview = {
        "request_count": len(bench_requests),
        "success_count": len(bench_requests),
        "failed_count": 0,
        "bytes_total": sum(4 * 1024 * 1024 for _ in bench_requests),
        "latency": stats([r.timings["total_us"] for r in bench_requests]),
        "elapsed_s": (bench_requests[-1].ts - bench_requests[0].ts).total_seconds() if bench_requests else 1,
    }

    render_html(
        output=output,
        work_dir=Path("/tmp/cluster_diag_work"),
        since=base_ts,
        until=base_ts + timedelta(hours=1),
        snapshots=snapshots,
        samples=samples,
        bench_requests=bench_requests,
        pod_file_stats=pod_file_stats,
        op_summary=op_summary,
        qps_by_slot=qps_by_slot,
        bandwidth_by_slot=bandwidth_by_slot,
        get_into_chart=get_into_chart,
        chart_stride=chart_stride,
        slow_requests=slow_requests,
        latency_overview=latency_overview,
        correlated_count=0,
        parse_counts={"files_seen": 5, "lines_seen": 50000, "target_lines": 12000},
        chunk_size=args.chunk_size,
    )
    print(f"sample report: {output}")
    return 0


def main(argv: list[str]) -> int:
    args = build_arg_parser().parse_args(argv)

    if args.sample:
        return cmd_sample(args)

    since_dt = parse_since(args.since) if args.since else None
    until_dt = parse_until(args.until) if args.until else None
    work_dir = args.work_dir.resolve()
    work_dir.mkdir(parents=True, exist_ok=True)

    targets = build_targets()

    # --exec mode: only run command, no analysis
    if args.exec_cmd:
        snapshots = exec_on_targets(targets, args.exec_cmd, args.exec_timeout)
        # Render a minimal exec-only HTML
        render_html(
            output=args.output.resolve(),
            work_dir=work_dir,
            since=since_dt,
            until=until_dt,
            snapshots=snapshots,
            samples=[], bench_requests=[], pod_file_stats=[],
            op_summary=[], qps_by_slot={}, bandwidth_by_slot={}, get_into_chart=[], chart_stride=1,
            slow_requests=[], latency_overview={
                "request_count": 0, "success_count": 0, "failed_count": 0,
                "bytes_total": 0, "latency": stats([]), "elapsed_s": 1,
            },
            correlated_count=0, parse_counts={}, chunk_size=args.chunk_size,
        )
        print(f"\nexec report: {args.output.resolve()}")
        return 0

    # Collection phase
    if not args.analyze_only:
        snapshots = collect_snapshots(
            targets, work_dir,
            run_spdiag=not args.no_spdiag,
            pull_logs=not args.no_logs,
            timeout=args.exec_timeout,
        )
    else:
        # Reconstruct snapshots from existing files
        spdiag_dir = work_dir / "spdiag_show"
        snapshots = []
        for target in targets:
            spdiag_file = spdiag_dir / f"{target.name}.txt"
            snap = TargetSnapshot(target=target)
            if spdiag_file.exists():
                snap.spdiag_raw = spdiag_file.read_text(encoding="utf-8", errors="replace")
                snap.spdiag_rows = parse_spdiag_show(snap.spdiag_raw)
                snap.spdiag_rc = 0
            log_root = work_dir / "collected_logs" / target.name
            if log_root.exists():
                snap.log_files = list(_walk_files(log_root))
            snapshots.append(snap)

    # Analysis phase
    collected_logs = work_dir / "collected_logs"
    print(f"\nparsing logs under {collected_logs} ...", flush=True)
    samples, bench_requests, pod_file_stats, parse_counts = parse_logs(
        collected_logs,
        since=since_dt, until=until_dt,
        chunk_size=args.chunk_size, chunk_marker=args.chunk_marker,
        path_separator=args.path_separator, chunk_style=args.chunk_style,
    )
    print(f"  samples: {len(samples)}, bench_requests: {len(bench_requests)}, "
          f"pod_file_stats: {len(pod_file_stats)}", flush=True)

    # Bench-style aux parsing for trace correlation
    bench_req2, rpc, storage_read, storage_rpc, release, master_rpc, bench_counts = (
        parse_bench_aux_logs(
            collected_logs, since=since_dt, until=until_dt,
        )
    )
    # Use bench_requests from main parse if bench parse found nothing
    if bench_req2:
        bench_requests_final = bench_req2
    else:
        bench_requests_final = bench_requests
        rpc, storage_read, storage_rpc, release, master_rpc = {}, {}, {}, {}, {}

    correlated_count = sum(
        1 for r in bench_requests_final
        if r.trace_id in rpc and r.trace_id in storage_read and r.trace_id in storage_rpc
    )

    # Aggregations
    op_summary = aggregate_op_summary(samples)
    qps_by_slot = aggregate_qps_by_slot(samples)
    bandwidth_by_slot = aggregate_bandwidth_by_slot(samples, chunk_size=args.chunk_size)
    get_into_chart, chart_stride = aggregate_get_into_chart(bench_requests_final)
    slow_requests = aggregate_slowest_requests(
        bench_requests_final,
        rpc=rpc, storage_read=storage_read, storage_rpc=storage_rpc,
        release=release, master_rpc=master_rpc,
    )
    elapsed_s = (
        (bench_requests_final[-1].ts - bench_requests_final[0].ts).total_seconds()
        if bench_requests_final else 1
    )
    latency_overview = {
        "request_count": len(bench_requests_final),
        "success_count": sum(1 for r in bench_requests_final
                             if r.fields.get("status") == "read_ok"),
        "failed_count": sum(1 for r in bench_requests_final
                            if r.fields.get("status") != "read_ok"),
        "bytes_total": sum(number(r.fields.get("total_bytes")) or 0
                           for r in bench_requests_final),
        "latency": stats([r.timings.get("total_us", 0) for r in bench_requests_final
                          if "total_us" in r.timings]),
        "elapsed_s": elapsed_s,
    }

    # Render
    render_html(
        output=args.output.resolve(),
        work_dir=work_dir,
        since=since_dt,
        until=until_dt,
        snapshots=snapshots,
        samples=samples,
        bench_requests=bench_requests_final,
        pod_file_stats=pod_file_stats,
        op_summary=op_summary,
        qps_by_slot=qps_by_slot,
        bandwidth_by_slot=bandwidth_by_slot,
        get_into_chart=get_into_chart,
        chart_stride=chart_stride,
        slow_requests=slow_requests,
        latency_overview=latency_overview,
        correlated_count=correlated_count,
        parse_counts=parse_counts,
        chunk_size=args.chunk_size,
    )
    print(f"\ncluster report: {args.output.resolve()}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
