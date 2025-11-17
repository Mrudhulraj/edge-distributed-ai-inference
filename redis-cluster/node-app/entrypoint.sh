#!/bin/sh
set -eu

redis-server /usr/local/etc/redis/redis.conf &
REDIS_PID=$!

python3 -u /opt/node-app/service.py &
PYTHON_PID=$!

term_handler() {
  kill "$REDIS_PID" "$PYTHON_PID" >/dev/null 2>&1 || true
  wait >/dev/null 2>&1 || true
}

trap term_handler INT TERM

wait

