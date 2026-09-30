#!/usr/bin/env bash
# One-command demo: services -> batch ingestion -> stream processor -> dashboard -> producer.
#
#   scripts/demo.sh [speed]     speed = ECG seconds streamed per real second (default 2:
#                               the trace stays readable, the 5-min HR trend fills in ~2.5 min)
#
# The dashboard opens in the browser; the stream starts a few seconds later. Ctrl+C stops everything.
set -eo pipefail
cd "$(dirname "$0")/.."
SPEED="${1:-2}"
if nc -z localhost 8501 2>/dev/null; then  # never tear down services another demo is using
  echo "A dashboard is already running on http://localhost:8501 (another demo?). Stop it first."; exit 1
fi

eval "$("${CONDA_EXE:-conda}" shell.bash hook)"
# first run on a new machine: create the env and fetch Kafka (setup.sh skips whatever is already there)
if ! conda activate ecgpipe 2>/dev/null || [ ! -x "$CONDA_PREFIX/opt/kafka/bin/kafka-server-start.sh" ]; then
  scripts/setup.sh
  conda activate ecgpipe
fi
LOGS="$CONDA_PREFIX/var/ecgpipe/demo-logs"
mkdir -p "$LOGS"

PIDS=()
cleanup() {
  trap - EXIT INT TERM
  echo -e "\nstopping demo..."
  kill "${PIDS[@]}" 2>/dev/null || true
  wait 2>/dev/null || true
  scripts/services.sh stop
}
trap cleanup EXIT INT TERM

scripts/services.sh start
python -m ecgpipe.ingest_batch --reset --check-minutes 2
# fresh demo: skip any backlog left in the topic by earlier runs
"$CONDA_PREFIX/opt/kafka/bin/kafka-consumer-groups.sh" --bootstrap-server localhost:9092 --group ecg-processor \
  --topic ecg.raw --reset-offsets --to-latest --execute >/dev/null 2>&1 || true

python -u -m ecgpipe.consumer >"$LOGS/consumer.log" 2>&1 &
PIDS+=($!)
( until curl -s localhost:8501/_stcore/health | grep -q ok; do sleep 1; done
  sleep 3   # let the page load before the first message
  exec python -u -m ecgpipe.producer --speed "$SPEED" ) >"$LOGS/producer.log" 2>&1 &
PIDS+=($!)

echo "dashboard: http://localhost:8501 · speed ${SPEED}x · logs in $LOGS · Ctrl+C to stop"
streamlit run dashboard.py --server.headless "${HEADLESS:-false}"
