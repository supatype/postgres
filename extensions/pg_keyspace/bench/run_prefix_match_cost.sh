#!/usr/bin/env bash
# What per-key durability costs the write path (#164), as a gate.
#
# `Policy::tier_for` runs on every write and sits in front of an op measured in
# nanoseconds, so "a few byte compares, it will be fine" is not something to
# assert in a comment. This runs the matcher at 0, 1, 4, 8 and 64 rules and
# fails the build if either path leaves its budget.
#
# The two budgets are not the same question:
#
#   miss  a key matching no rule, which is nearly every key on a
#         mostly-ephemeral instance -- the cache write that per-key durability
#         exists to keep cheap. Rejected by a first-byte bitmap and a minimum
#         length before any comparison, and flat in the rule count.
#   hit   a key the operator asked to treat specially. It pays this and then
#         goes on to stage a ring record, so it has room the miss does not.
#
# The budgets have headroom over the numbers on a developer machine (miss
# ~4 ns, hit ~10 ns up to 8 rules, ~65 ns at 64) because a shared CI runner is
# slower and noisier, but not so much headroom that they stop meaning
# anything. The first version of this matcher scanned every rule and cost
# 154 ns/op on a hit against 64 same-length prefixes; the hit budget is set
# below that deliberately, so the regression this harness was written to find
# is one it would still fail on.
#
# No Postgres. Note that supacache.bench_set() cannot be used for this: it
# writes straight to the store from the calling backend and never reaches the
# RESP dispatch where the policy is consulted.
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
CORE_DIR=${PGKS_CORE_DIR:-$SCRIPT_DIR/../core}
ITERS=${PGKS_BENCH_ITERS:-3000000}
MAX_MISS_NS=${PGKS_MAX_MISS_NS:-25}
MAX_HIT_NS=${PGKS_MAX_HIT_NS:-120}

cd "$CORE_DIR"
cargo run --release --quiet --bin prefix_match_bench -- \
  --iters "$ITERS" --max-ns "$MAX_MISS_NS" --max-hit-ns "$MAX_HIT_NS"
