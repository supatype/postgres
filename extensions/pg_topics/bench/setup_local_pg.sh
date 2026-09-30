#!/bin/bash
set -euo pipefail

PREFIX="$HOME/.cache/pg_topics/pg17"
PGRX_HOME="$HOME/.cache/pg_topics/pgrx"

copy_tree() {
  local src=$1 dst=$2
  if [ -d "$dst" ]; then
    return 0
  fi
  mkdir -p "$(dirname "$dst")"
  cp -a "$src" "$dst"
}

copy_tree /usr/lib/postgresql/17 "$PREFIX/usr/lib/postgresql/17"
copy_tree /usr/share/postgresql/17 "$PREFIX/usr/share/postgresql/17"
copy_tree /usr/include/postgresql/17 "$PREFIX/usr/include/postgresql/17"

PG_CONFIG="$PREFIX/usr/lib/postgresql/17/bin/pg_config"
echo "user-owned PostgreSQL 17 copy at $PREFIX"
echo "pg_config --sharedir: $("$PG_CONFIG" --sharedir)"

mkdir -p "$PGRX_HOME"
PGRX_HOME="$PGRX_HOME" cargo pgrx init --pg17 "$PG_CONFIG"

echo "pgrx initialised against the copy, PGRX_HOME=$PGRX_HOME"
