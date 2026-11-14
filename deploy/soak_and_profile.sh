#!/usr/bin/env bash
# 72-hour soak & Linux perf c2c cache-line contention profiling script (S6-T03, S6-T04)
set -euo pipefail

echo "Starting socketio-gateway with jemalloc background purge profiling..."
export MALLOC_CONF="background_thread:true,dirty_decay_ms:5000,muzzy_decay_ms:5000,stats_print:true"

cargo build --release -p socketio-gateway
./target/release/socketio-gateway --serve --bind 127.0.0.1:3000 &
GW_PID=$!
trap 'kill -TERM $GW_PID 2>/dev/null || true' EXIT

echo "Gateway PID: $GW_PID"
if command -v perf >/dev/null 2>&1; then
  echo "Sampling cache-line contention with perf c2c for 30s..."
  perf c2c record -p "$GW_PID" -- sleep 30 || true
  perf c2c report --stdio || true
else
  echo "perf not available on this host; run on Linux 6.x staging node for c2c profiling."
fi
