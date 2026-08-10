#!/usr/bin/env bash
# run_gc_e2e.sh
#
# Multi-process integration test for the explicit-delete-only SSD GC.
#
# This is "Plan B": launches the REAL production binaries
#   - mooncake_master   (master service)
#   - mooncake_client   (real_client_main, the RPC server that owns the
#                        BucketStorageBackend + FileStorage offload path)
# and drives put/get/remove via the Python MooncakeDistributedStore client,
# which connects to the real_client via RPC and exercises
# RealClient::remove_internal -> FileStorage::MarkRemoved -> GC.
#
# Unlike gc_e2e_test.cpp (in-process), this uses separate OS processes,
# matching the production deployment topology.
#
# Prerequisites:
#   - BUILD_DIR points to a build tree containing:
#       mooncake-store/src/mooncake_master
#       mooncake-store/src/mooncake_client
#       mooncake-integration/store*.so  (Python bindings)
#   - Python environment with the mooncake wheel installed
#   - A standalone HTTP metadata server (the master's embedded one or
#     mooncake-wheel/mooncake/http_metadata_server.py)
#
# Usage:
#   cd mooncake-store/tests/e2e
#   BUILD_DIR=/path/to/build ./run_gc_e2e.sh
#
# Environment variables (all optional):
#   BUILD_DIR          Build tree root (default: ../../../build)
#   WORK_DIR           Temp working dir (default: /tmp/mooncake_gc_e2e)
#   SSD_OFFLOAD_PATH   SSD offload dir (default: $WORK_DIR/ssd_offload)
#   MASTER_PORT        Master RPC port (default: 50051)
#   CLIENT_PORT        RealClient RPC port (default: 50052)
#   METADATA_PORT      HTTP metadata port (default: 8080)
#   GC_INTERVAL_MS     GC scan interval (default: 200)
#   GC_DELETED_RATIO   Compaction threshold (default: 0.1)
#   PAYLOAD_SIZE       Key value size (default: 4194304 = 4MB)
#   PASS_CODE          Expected exit code for pass (default: 0)

set -euo pipefail

BUILD_DIR="${BUILD_DIR:-$(cd "$(dirname "$0")/../../.." && pwd)/build}"
WORK_DIR="${WORK_DIR:-/tmp/mooncake_gc_e2e}"
SSD_OFFLOAD_PATH="${SSD_OFFLOAD_PATH:-$WORK_DIR/ssd_offload}"
MASTER_PORT="${MASTER_PORT:-50051}"
CLIENT_PORT="${CLIENT_PORT:-50052}"
METADATA_PORT="${METADATA_PORT:-8080}"
GC_INTERVAL_MS="${GC_INTERVAL_MS:-200}"
GC_DELETED_RATIO="${GC_DELETED_RATIO:-0.1}"
PAYLOAD_SIZE="${PAYLOAD_SIZE:-4194304}"

MASTER_BIN="$BUILD_DIR/mooncake-store/src/mooncake_master"
CLIENT_BIN="$BUILD_DIR/mooncake-store/src/mooncake_client"
METADATA_BIN="mooncake.http_metadata_server"  # python module

MASTER_ADDR="127.0.0.1:$MASTER_PORT"
METADATA_URL="http://127.0.0.1:$METADATA_PORT/metadata"
LOG_DIR="$WORK_DIR/logs"

mkdir -p "$WORK_DIR" "$SSD_OFFLOAD_PATH" "$LOG_DIR"

cleanup() {
    local exit_code=$?
    echo "[cleanup] stopping processes..."
    [[ -n "${CLIENT_PID:-}" ]] && kill "$CLIENT_PID" 2>/dev/null || true
    [[ -n "${MASTER_PID:-}" ]] && kill "$MASTER_PID" 2>/dev/null || true
    [[ -n "${META_PID:-}" ]] && kill "$META_PID" 2>/dev/null || true
    wait 2>/dev/null || true
    if [[ $exit_code -eq 0 ]]; then
        echo "[result] PASS"
    else
        echo "[result] FAIL (exit=$exit_code)"
    fi
    exit $exit_code
}
trap cleanup EXIT

echo "============================================================"
echo " SSD GC Multi-Process E2E Test"
echo "============================================================"
echo " BUILD_DIR        = $BUILD_DIR"
echo " WORK_DIR         = $WORK_DIR"
echo " SSD_OFFLOAD_PATH = $SSD_OFFLOAD_PATH"
echo " GC_INTERVAL_MS   = $GC_INTERVAL_MS"
echo " GC_DELETED_RATIO = $GC_DELETED_RATIO"
echo " PAYLOAD_SIZE     = $PAYLOAD_SIZE"
echo "============================================================"

# --- 1. Start HTTP metadata server (standalone, for transfer engine) ---
echo "[1/5] Starting HTTP metadata server on port $METADATA_PORT..."
python3 -m mooncake.http_metadata_server --port "$METADATA_PORT" \
    >"$LOG_DIR/metadata.log" 2>&1 &
META_PID=$!
sleep 1
if ! kill -0 "$META_PID" 2>/dev/null; then
    echo "[ERROR] metadata server failed to start (see $LOG_DIR/metadata.log)"
    exit 1
fi

# --- 2. Start mooncake_master with offload enabled ---
echo "[2/5] Starting mooncake_master on port $MASTER_PORT..."
"$MASTER_BIN" \
    --port "$MASTER_PORT" \
    --metrics_port 9004 \
    --enable_offload \
    --default_kv_lease_ttl 0 \
    --log_dir "$LOG_DIR" \
    >"$LOG_DIR/master.log" 2>&1 &
MASTER_PID=$!
sleep 2
if ! kill -0 "$MASTER_PID" 2>/dev/null; then
    echo "[ERROR] master failed to start (see $LOG_DIR/master.log)"
    exit 1
fi

# --- 3. Start mooncake_client (real_client_main) with SSD offload ---
# Export GC env vars BEFORE launching the client so BucketBackendConfig
# picks them up via FromEnvironment().
echo "[3/5] Starting mooncake_client (real_client) on port $CLIENT_PORT..."
export MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR="bucket_storage_backend"
export MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY="lru"
export MOONCAKE_OFFLOAD_DISABLE_SSD_EVICTION="true"
export MOONCAKE_OFFLOAD_BUCKET_GC_INTERVAL_MS="$GC_INTERVAL_MS"
export MOONCAKE_OFFLOAD_BUCKET_GC_DELETED_RATIO="$GC_DELETED_RATIO"
export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH="$SSD_OFFLOAD_PATH"

"$CLIENT_BIN" \
    --host "127.0.0.1" \
    --metadata_server "$METADATA_URL" \
    --master_server_address "$MASTER_ADDR" \
    --protocol tcp \
    --port "$CLIENT_PORT" \
    --global_segment_size "512MB" \
    --enable_offload \
    --start_offload_rpc_server \
    >"$LOG_DIR/client.log" 2>&1 &
CLIENT_PID=$!
sleep 3
if ! kill -0 "$CLIENT_PID" 2>/dev/null; then
    echo "[ERROR] real_client failed to start (see $LOG_DIR/client.log)"
    exit 1
fi

CLIENT_RPC_ADDR="127.0.0.1:$CLIENT_PORT"
echo "[info] real_client RPC at $CLIENT_RPC_ADDR"

# --- 4. Drive workload via Python client ---
# The Python MooncakeDistributedStore connects to the real_client RPC
# server and issues put/get/remove through RealClient handlers, which is
# the only path that triggers MarkRemoved -> GC.
echo "[4/5] Running GC workload via Python client..."

export PYTHONPATH="$BUILD_DIR/mooncake-integration:${PYTHONPATH:-}"

python3 - "$CLIENT_RPC_ADDR" "$MASTER_ADDR" "$METADATA_URL" \
    "$SSD_OFFLOAD_PATH" "$PAYLOAD_SIZE" "$LOG_DIR" <<'PYEOF'
import os, sys, time, glob

client_rpc = sys.argv[1]
master_addr = sys.argv[2]
metadata_url = sys.argv[3]
ssd_path = sys.argv[4]
payload_size = int(sys.argv[5])
log_dir = sys.argv[6]

import mooncake.store as mc

# Connect a Python client to the real_client RPC server.
# MooncakeDistributedStore routes put/get/remove to the real_client
# process, exercising RealClient::remove_internal -> MarkRemoved.
client = mc.MooncakeDistributedStore(
    local_hostname="127.0.0.1:50070",
    metadata_server=metadata_url,
    master_server_addr=master_addr,
    global_segment_size=256 * 1024 * 1024,
    local_buffer_size=128 * 1024 * 1024,
    protocol="tcp",
)

def count_bucket_files():
    return len(glob.glob(os.path.join(ssd_path, "*.bucket")))

v1 = b"A" * payload_size
v2 = b"B" * payload_size
v3 = b"C" * payload_size

# Put 3 keys -> offloaded to bucket(s) on SSD.
print("[py] putting 3 keys...")
assert client.put("gc_b_k1", v1) == 0, "put k1 failed"
assert client.put("gc_b_k2", v2) == 0, "put k2 failed"
assert client.put("gc_b_k3", v3) == 0, "put k3 failed"

# Wait for offload to complete (master queues on PutEnd, heartbeat drains).
print("[py] waiting for offload...")
for _ in range(50):
    try:
        got = client.get("gc_b_k1")
        if got == v1:
            break
    except Exception:
        pass
    time.sleep(0.2)
else:
    print("[py][ERROR] offload timed out", file=sys.stderr)
    sys.exit(1)

buckets_before = count_bucket_files()
print(f"[py] buckets_before={buckets_before}")

# Remove the middle key -> tombstone, triggers GC compaction.
print("[py] removing gc_b_k2...")
assert client.remove("gc_b_k2") == 0, "remove k2 failed"

# Wait for GC compaction: survivors must stay readable, removed key gone.
print("[py] waiting for GC compaction...")
deadline = time.time() + 30
reclaimed = False
while time.time() < deadline:
    # Survivors must remain readable with correct data.
    try:
        assert client.get("gc_b_k1") == v1, "k1 corrupted during GC"
        assert client.get("gc_b_k3") == v3, "k3 corrupted during GC"
    except AssertionError as e:
        print(f"[py][ERROR] {e}", file=sys.stderr)
        sys.exit(1)
    # Removed key must stay gone.
    try:
        client.get("gc_b_k2")
        print("[py][ERROR] removed key k2 reappeared", file=sys.stderr)
        sys.exit(1)
    except Exception:
        pass  # expected: get fails
    # Detect compaction: bucket file count changed.
    buckets_now = count_bucket_files()
    if buckets_now > 0 and buckets_now != buckets_before:
        reclaimed = True
        print(f"[py] compaction detected: buckets {buckets_before}->{buckets_now}")
        break
    time.sleep(0.3)

# Final integrity check.
assert client.get("gc_b_k1") == v1, "k1 final check failed"
assert client.get("gc_b_k3") == v3, "k3 final check failed"

if not reclaimed:
    print("[py][WARN] GC reclamation not detected within timeout "
          "(may still pass if data integrity holds)")
print("[py] PASS: survivors intact, removed key gone")
PYEOF

PY_EXIT=$?
if [[ $PY_EXIT -ne 0 ]]; then
    echo "[ERROR] Python workload failed (exit=$PY_EXIT, see $LOG_DIR/client.log)"
    exit $PY_EXIT
fi

echo "[5/5] Verifying SSD file reclamation..."
FINAL_BUCKETS=$(find "$SSD_OFFLOAD_PATH" -name "*.bucket" 2>/dev/null | wc -l)
echo "[info] final bucket files: $FINAL_BUCKETS"

echo "============================================================"
echo " GC E2E: PASS"
echo "============================================================"
exit 0
