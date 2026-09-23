#!/usr/bin/env bash
# One-time setup: conda env (Python libs + MongoDB + Java) and Kafka unpacked inside the env.
set -euo pipefail
cd "$(dirname "$0")/.."
KAFKA_VERSION=4.3.1
ENV_NAME=ecgpipe

if ! conda env list | grep -qE "^${ENV_NAME}\s"; then
  conda env create -f environment.yml
fi
PREFIX="$(conda run -n "$ENV_NAME" python -c 'import sys; print(sys.prefix)')"

if [ ! -x "$PREFIX/opt/kafka/bin/kafka-server-start.sh" ]; then
  echo "Downloading Apache Kafka $KAFKA_VERSION into $PREFIX/opt/kafka"
  mkdir -p "$PREFIX/opt/kafka"
  TGZ="kafka_2.13-$KAFKA_VERSION.tgz"   # fast CDN first, permanent archive as fallback
  { curl -fsSL "https://dlcdn.apache.org/kafka/$KAFKA_VERSION/$TGZ" \
      || curl -fsSL "https://archive.apache.org/dist/kafka/$KAFKA_VERSION/$TGZ"; } \
    | tar -xz -C "$PREFIX/opt/kafka" --strip-components 1
fi
echo "Setup complete. Next: conda activate $ENV_NAME && scripts/services.sh start"
