#!/usr/bin/env bash
# Start/stop the two local services (single-node MongoDB and single-node Kafka in KRaft mode).
# Run inside the activated conda env. Data and logs live in $CONDA_PREFIX/var/ecgpipe (delete it for
# a clean slate); keeping them inside the env also avoids paths with spaces, which Kafka's scripts break on.
set -euo pipefail
: "${CONDA_PREFIX:?activate the ecgpipe env first}"
RUN="$CONDA_PREFIX/var/ecgpipe"
KAFKA="$CONDA_PREFIX/opt/kafka"

wait_port() { for _ in $(seq 60); do nc -z localhost "$1" 2>/dev/null && return 0; sleep 1; done; echo "port $1 not up"; exit 1; }
wait_closed() { for _ in $(seq 60); do nc -z localhost "$1" 2>/dev/null || return 0; sleep 1; done; }

start() {
  mkdir -p "$RUN/mongo" "$RUN/kafka"
  if ! nc -z localhost 27017 2>/dev/null; then
    mongod --dbpath "$RUN/mongo" --bind_ip 127.0.0.1 --port 27017 --fork --logpath "$RUN/mongod.log" \
      --pidfilepath "$RUN/mongod.pid" >/dev/null
  fi
  wait_port 27017 && echo "MongoDB  up  (localhost:27017)"

  if ! nc -z localhost 9092 2>/dev/null; then
    CONF="$RUN/kafka/server.properties"
    cp "$KAFKA/config/server.properties" "$CONF"
    echo "log.dirs=$RUN/kafka/data" >> "$CONF"   # later keys override earlier ones
    if [ ! -f "$RUN/kafka/data/meta.properties" ]; then
      "$KAFKA/bin/kafka-storage.sh" format --standalone -t "$("$KAFKA/bin/kafka-storage.sh" random-uuid)" -c "$CONF" >/dev/null
    fi
    LOG_DIR="$RUN/kafka/logs" "$KAFKA/bin/kafka-server-start.sh" -daemon "$CONF"
  fi
  wait_port 9092 && echo "Kafka    up  (localhost:9092)"
}

stop() {
  "$KAFKA/bin/kafka-server-stop.sh" >/dev/null 2>&1 || true
  [ -f "$RUN/mongod.pid" ] && kill "$(cat "$RUN/mongod.pid")" 2>/dev/null || true  # SIGTERM = clean shutdown
  wait_closed 9092 && wait_closed 27017 && echo "services stopped"
}

status() {
  for p in 27017:MongoDB 9092:Kafka; do
    nc -z localhost "${p%%:*}" 2>/dev/null && echo "${p##*:} up" || echo "${p##*:} down"
  done
}

case "${1:-status}" in
  start|stop|status) "${1:-status}" ;;
  *) echo "usage: $0 start|stop|status"; exit 1 ;;
esac
