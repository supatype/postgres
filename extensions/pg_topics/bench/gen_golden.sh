#!/bin/bash
set -euo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
CORE_DIR="$SCRIPT_DIR/../core"
IMAGE=confluentinc/cp-kafka:7.7.1
OUT="$CORE_DIR/testdata/murmur2_golden.tsv"

docker run --rm -v "$SCRIPT_DIR:/g:ro" "$IMAGE" \
  java -cp "/usr/share/java/kafka/*" /g/Golden.java > "$OUT"

echo "wrote $(wc -l < "$OUT") rows to $OUT"
