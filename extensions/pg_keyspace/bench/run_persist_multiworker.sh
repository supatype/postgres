#!/usr/bin/env bash
# Durability + scale-out together: N shared-nothing RESP slot workers, each with
# its own keyspace segment AND its own persistence ring set, all writing through
# to supacache.kv. Proves the two are no longer mutually exclusive
# (pg_keyspace.workers > 1 used to force the ephemeral tier).
#
# The contract under test: worker w serves exactly the CRC16 slot range
# supacache.slot_ranges() reports for it, and after a full restart every key
# comes back on the worker that took the write — not merely somewhere.
#
# Expects a running cluster with pg_keyspace loaded, workers > 1 and a persisted
# durability tier. PGPORT/PGDATA/PGBIN are overridable.
set -u
PGPORT=${PGPORT:-5433}
PGBIN=${PGBIN:-/usr/lib/postgresql/16/bin}
PGDATA=${PGDATA:-/tmp/pgks_data}
PGUSER=${PGUSER:-postgres}
KEYS=${KEYS:-200}
P="psql -h 127.0.0.1 -p $PGPORT -U $PGUSER -d postgres -X -q -A -t"

pass=0; fail=0
chk() { # chk <label> <expected> <actual>
  if [ "$2" = "$3" ]; then pass=$((pass+1)); printf "ok   %s\n" "$1"
  else fail=$((fail+1)); printf "FAIL %s (expected '%s', got '%s')\n" "$1" "$2" "$3"; fi
}

# TLS autodetect on the base port, matching run_scaleout_inpg.sh.
BASE=$($P -c "SHOW pg_keyspace.port" | tr -d '[:space:]')
T=""
if ! timeout 2 redis-cli -p "$BASE" PING >/dev/null 2>&1; then T="--tls --insecure"; fi
R() { local port=$1; shift; timeout 5 redis-cli $T -p "$port" "$@" 2>/dev/null; }

N=$($P -c "SELECT count(*) FROM supacache.slot_ranges();" | tr -d '[:space:]')
TIER=$($P -c "SHOW pg_keyspace.durability" | tr -d '[:space:]')
echo "# multi-worker persistence — $N slot worker(s) from port $BASE, tier=$TIER"
if [ "${N:-0}" -lt 2 ]; then
  echo "SKIP: needs pg_keyspace.workers > 1 (got ${N:-0})"; exit 0
fi
case "$TIER" in ephemeral|"") echo "SKIP: needs a persisted durability tier (got '$TIER')"; exit 0;; esac

# Ranges must partition the whole 16384-slot space with no gaps or overlaps.
covered=$($P -c "SELECT bool_and(ok) FROM (
  SELECT slot_lo = coalesce(lag(slot_hi) OVER (ORDER BY worker), 0) AS ok
  FROM supacache.slot_ranges()) t;" | tr -d '[:space:]')
chk "slot ranges are contiguous from 0" "t" "$covered"
chk "slot ranges cover all 16384 slots" "16384" \
    "$($P -c "SELECT max(slot_hi) FROM supacache.slot_ranges();" | tr -d '[:space:]')"

# --- write: every key goes to the worker that owns its slot -----------------
# supacache.key_worker is the same mapping the RESP path and recovery use, so
# routing by it is what a slot-aware client does.
declare -A OWNER
$P -c "SELECT i||' '||supacache.key_worker('mw:'||i) FROM generate_series(1,$KEYS) i;" > /tmp/mw_owners.txt
while read -r i w; do [ -n "${i:-}" ] && OWNER[$i]=$w; done < /tmp/mw_owners.txt

spread=$(awk '{print $2}' /tmp/mw_owners.txt | sort -u | wc -l | tr -d ' ')
chk "keys spread across all $N workers" "$N" "$spread"

for i in $(seq 1 $KEYS); do
  w=${OWNER[$i]}
  R $((BASE+w)) SET "mw:$i" "val-$i" >/dev/null
done
# one TTL'd key per worker: TTL rows persist to kv_ttl, a different recovery path
for w in $(seq 0 $((N-1))); do
  k=$($P -c "SELECT 'mwttl:'||i FROM generate_series(1,5000) i WHERE supacache.key_worker('mwttl:'||i) = $w LIMIT 1;" | tr -d '[:space:]')
  [ -n "$k" ] && R $((BASE+w)) SET "$k" "ttl-$w" EX 3600 >/dev/null && echo "$w $k" >> /tmp/mw_ttl.txt
done

# --- shared-nothing: only the owning worker will serve the key --------------
# A non-owning worker must not answer with a value (its own miss or, worse, a
# copy): it redirects. That redirect is what keeps a key in exactly one segment.
probe=1; pw=${OWNER[1]}
redirects=0
for w in $(seq 0 $((N-1))); do
  [ "$w" = "$pw" ] && continue
  R $((BASE+w)) GET "mw:$probe" | grep -q MOVED && redirects=$((redirects+1))
done
chk "$((N-1)) non-owning worker(s) redirect rather than serve" "$((N-1))" "$redirects"

# --- misrouting is refused, not silently accepted ---------------------------
# A key sent to a worker that does not own it must come back as MOVED, naming
# the owning worker's port. Accepting it would ack a durable write that the next
# restart routes into another worker's segment — silent loss.
wrong_w=$(( (pw + 1) % N ))
err=$(R $((BASE+wrong_w)) SET "mw:$probe" nope)
chk "misrouted write refused with MOVED" "MOVED" "$(echo "$err" | grep -o MOVED | head -1)"
chk "MOVED names the owning worker's port" "$((BASE+pw))"     "$(echo "$err" | grep -oE '[0-9]+$' | tail -1)"
chk "misrouted read refused too" "MOVED"     "$(R $((BASE+wrong_w)) GET "mw:$probe" | grep -o MOVED | head -1)"
chk "owning worker still serves the key" "val-$probe" "$(R $((BASE+pw)) GET "mw:$probe")"

# --- drain: every worker's ring reaches supacache.kv ------------------------
for _ in $(seq 1 200); do
  b=$($P -c "SELECT backlog_bytes FROM supacache.ring_stats();" | tr -d '[:space:]')
  [ "${b:-1}" = "0" ] && break; sleep 0.1
done
chk "all rings drained to supacache.kv" "0" "${b:-unknown}"
chk "dropped records" "0" "$($P -c "SELECT dropped FROM supacache.ring_stats();" | tr -d '[:space:]')"
chk "rows persisted from every worker" "$KEYS" \
    "$($P -c "SELECT count(*) FROM supacache.kv WHERE key LIKE 'mw:%';" | tr -d '[:space:]')"

# Each row's stored slot must fall in its writing worker's advertised range —
# this is what makes recovery route the key back to the same worker.
chk "every persisted slot sits in its worker's range" "0" \
    "$($P -c "SELECT count(*) FROM supacache.kv k WHERE k.key LIKE 'mw:%' AND NOT EXISTS (
        SELECT 1 FROM supacache.slot_ranges() r
        WHERE k.slot >= r.slot_lo AND k.slot < r.slot_hi);" | tr -d '[:space:]')"

# --- restart: the real test ------------------------------------------------
echo "# restarting the cluster..."
runuser -u "$PGUSER" -- $PGBIN/pg_ctl -D "$PGDATA" -w stop >/dev/null 2>&1
runuser -u "$PGUSER" -- $PGBIN/pg_ctl -D "$PGDATA" -l "$PGDATA/server.log" -w start >/dev/null 2>&1
for _ in $(seq 1 60); do timeout 2 redis-cli $T -p "$BASE" PING >/dev/null 2>&1 && break; sleep 0.5; done
sleep 2  # let every worker finish its recovery scan

back=0; wrong=0
for i in $(seq 1 $KEYS); do
  w=${OWNER[$i]}
  v=$(R $((BASE+w)) GET "mw:$i")
  if [ "$v" = "val-$i" ]; then back=$((back+1))
  elif [ -n "$v" ]; then wrong=$((wrong+1)); fi
done
chk "all $KEYS keys recovered on their owning worker" "$KEYS" "$back"
chk "no key recovered with a wrong value" "0" "$wrong"

# Recovery must shard by slot range, not load the whole table into every
# segment. Summing live entries across the segments shows that directly: N
# workers each loading everything would give N x the key count.
ttl_keys=$( [ -f /tmp/mw_ttl.txt ] && wc -l < /tmp/mw_ttl.txt | tr -d ' ' || echo 0 )
expect_entries=$((KEYS + ttl_keys))
chk "recovered rows sharded, not duplicated across segments" "$expect_entries" \
    "$($P -c "SELECT sum(entries) FROM supacache.stats();" | tr -d '[:space:]')"
chk "every worker recovered a non-empty share" "0" \
    "$($P -c "SELECT count(*) FROM supacache.stats() WHERE entries = 0;" | tr -d '[:space:]')"

if [ -f /tmp/mw_ttl.txt ]; then
  tback=0; tcount=0
  while read -r w k; do
    tcount=$((tcount+1))
    [ "$(R $((BASE+w)) GET "$k")" = "ttl-$w" ] && tback=$((tback+1))
    ttl=$(R $((BASE+w)) TTL "$k")
    [ "${ttl:-0}" -gt 0 ] || echo "note: TTL not preserved for $k (got '${ttl:-}')"
  done < /tmp/mw_ttl.txt
  chk "TTL'd keys recovered on their owning worker" "$tcount" "$tback"
fi

# cleanup
for i in $(seq 1 $KEYS); do R $((BASE+${OWNER[$i]})) DEL "mw:$i" >/dev/null; done
[ -f /tmp/mw_ttl.txt ] && while read -r w k; do R $((BASE+w)) DEL "$k" >/dev/null; done < /tmp/mw_ttl.txt
rm -f /tmp/mw_owners.txt /tmp/mw_ttl.txt

echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
