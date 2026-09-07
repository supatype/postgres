#!/usr/bin/env bash
# P6 slice 3 — Mode B row cache for rows with out-of-line (TOASTed) values.
#
# The cache stores raw heap-tuple bytes. If a column is TOASTed the raw tuple
# holds a POINTER into the table's toast relation, not the value — caching that
# pointer would dangle once the toast chunks are vacuumed. The cache now
# flattens the tuple at store time (toast_flatten_tuple), pulling every external
# value inline so the cached row is fully self-contained.
#
# Proven here: a row with a genuinely out-of-line 20 KB column is cached; the
# LIVE row is external (its toast table has chunks) but the CACHED copy is NOT
# (HEAP_HASEXTERNAL cleared — the flatten diagnostic); the Custom Scan serves the
# complete value byte-for-byte; and an UPDATE stays coherent and re-flattened.
set -u
PGPORT=${PGPORT:-5434}
A="psql -h 127.0.0.1 -p $PGPORT -U supatype_admin -d postgres -X -q -A -t"
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-54s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-54s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }
plan() { $A -c "EXPLAIN (COSTS OFF) $1" 2>/dev/null | head -1; }
refill="$($A -c 'SHOW pg_keyspace.rowcache_refill' | tr -d '[:space:]')"

echo "# P6 — Mode B row cache for TOASTed rows; refill=$refill"

# A 20 KB value in an EXTERNAL-storage column is forced out of line (no
# compression, and the tuple exceeds the page's toast threshold).
$A >/dev/null 2>&1 <<'SQL'
DROP TABLE IF EXISTS public.big CASCADE;
CREATE TABLE public.big(id bigint primary key, blob text);
ALTER TABLE public.big ALTER COLUMN blob SET STORAGE EXTERNAL;
INSERT INTO public.big VALUES (1, repeat('A', 20000));
SELECT supacache.rowcache_register('public.big', 1);
SQL
sleep 1

# ground truth (cache is cold -> a normal scan)
gt_len="$($A -c 'SELECT length(blob) FROM public.big WHERE id=1')"
gt_md5="$($A -c 'SELECT md5(blob) FROM public.big WHERE id=1')"
chk "setup: value is 20000 bytes" "20000" "$gt_len"

# the LIVE row really is out of line: its toast relation holds chunks
toastrel="$($A -c "SELECT reltoastrelid::regclass::text FROM pg_class WHERE oid='public.big'::regclass")"
chunks="$($A -c "SELECT count(*) FROM $toastrel")"
chk "live row is genuinely TOASTed (toast chunks > 0)" "1" "$([ "${chunks:-0}" -gt 0 ] && echo 1 || echo 0)"

# cache it
$A -c "SELECT supacache.rowcache_put('public.big', 1::bigint);" >/dev/null 2>&1

chk "cached copy is FLATTENED (no external pointer)" \
    "f" "$($A -c "SELECT supacache.rowcache_cached_has_external('public.big', 1::bigint)")"
chk "cached row uses the Custom Scan" \
    "Custom Scan (pg_keyspace_rowcache) on big" "$(plan 'SELECT * FROM public.big WHERE id=1')"

# the Custom Scan serves the COMPLETE value, byte-for-byte
chk "Custom Scan serves the full value (length)" "$gt_len" \
    "$($A -c 'SELECT length(blob) FROM public.big WHERE id=1')"
chk "Custom Scan serves the full value (md5 == ground truth)" "$gt_md5" \
    "$($A -c 'SELECT md5(blob) FROM public.big WHERE id=1')"

# coherence: UPDATE to a new large value -> refill re-caches (still flattened)
$A -c "UPDATE public.big SET blob = repeat('B', 30000) WHERE id=1;" >/dev/null 2>&1
sleep 1
new_md5="$($A -c "SELECT md5(repeat('B',30000))")"
chk "after UPDATE: read is the fresh 30000-byte value (coherent)" \
    "30000|$new_md5" "$($A -c 'SELECT length(blob)||$$|$$||md5(blob) FROM public.big WHERE id=1')"
if [ "$refill" = "on" ]; then
  chk "refill: cached copy is still flattened after UPDATE" \
      "f" "$($A -c "SELECT supacache.rowcache_cached_has_external('public.big', 1::bigint)")"
  chk "refill: still served from the Custom Scan" \
      "Custom Scan (pg_keyspace_rowcache) on big" "$(plan 'SELECT * FROM public.big WHERE id=1')"
fi

# cleanup
$A >/dev/null 2>&1 <<'SQL'
SELECT supacache.rowcache_unregister('public.big');
DROP TABLE IF EXISTS public.big CASCADE;
SQL
echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
