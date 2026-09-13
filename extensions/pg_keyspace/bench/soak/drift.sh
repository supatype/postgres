#!/usr/bin/env bash
# Sample the slow-moving numbers during a soak, then judge them (#113).
#
# The headline rate is the easy part of a load test and the least informative.
# Production-readiness questions are almost all TIME questions -- memory growth,
# arena occupancy, eviction rate, WAL growth, replication slot lag -- and a
# two-minute run at a good rate answers none of them. This samples them on an
# interval into a CSV and then EVALUATES the series against thresholds, so a
# soak can fail on drift rather than only on latency.
#
# It reads pg_keyspace's own pg_stat_keyspace* views (#111), which is what
# those views are for. Everything else comes from the OS and from Postgres.
#
# Two modes:
#   drift.sh sample <out.csv>   -- sample until killed (run it beside the server)
#   drift.sh judge  <out.csv>   -- read a finished CSV and pass/fail it
#
# `sample` is deliberately separable from the generator: #113 requires the load
# generator to live on a different machine from the server, and drift has to be
# measured where the server is.
#
# Env: PGBIN PGPORT PGDB SAMPLE_SECS, and for `judge` the thresholds below.
set -uo pipefail
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGPORT=${PGPORT:-5432}
PGHOST=${PGHOST:-/tmp}
PGDB=${PGDB:-postgres}
PGUSER=${PGUSER:-postgres}
SAMPLE_SECS=${SAMPLE_SECS:-10}

Q() { $PGBIN/psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDB" -tAF, -c "$1" 2>/dev/null; }

COLS="t_s,rss_kb,entries,hits,misses,evictions,sets,arena_used,arena_cap,\
persist_lag,persist_backlog,persist_dropped,persist_failed,\
decode_lag,retained_wal,wal_bytes,rowcache_entries,rowcache_coherent,\
ttl_partitions,kv_rows,slot_count"

sample() {
  local out=$1
  echo "$COLS" > "$out"
  local t0 sample_line
  t0=$(date +%s)
  while :; do
    # Postmaster RSS plus every child: the cache lives in shared memory, so a
    # single process's RSS is not the number that grows.
    local pm rss
    pm=$(head -1 "$PGDATA/postmaster.pid" 2>/dev/null)
    rss=0
    if [ -n "${pm:-}" ]; then
      # Shared pages are counted once by reading the postmaster's shared total
      # separately from each child's private set; summing RSS across children
      # would count the segment once per backend and manufacture growth.
      rss=$(awk '/^VmRSS:/{print $2}' /proc/"$pm"/status 2>/dev/null)
      local priv
      priv=$(for p in $(pgrep -P "$pm" 2>/dev/null); do
               awk '/^RssAnon:/{print $2}' /proc/"$p"/status 2>/dev/null
             done | awk '{s+=$1} END{print s+0}')
      rss=$(( ${rss:-0} + ${priv:-0} ))
    fi

    local ks pers inval misc
    ks=$(Q "SELECT entries, hits, misses, evictions, sets, arena_used_bytes, arena_capacity_bytes
            FROM supacache.pg_stat_keyspace")
    pers=$(Q "SELECT lag, backlog_bytes, dropped, failed_batches
              FROM supacache.pg_stat_keyspace_persist_total")
    inval=$(Q "SELECT COALESCE(max(decode_lag_bytes),-1), COALESCE(max(retained_bytes),-1)
               FROM supacache.pg_stat_keyspace_invalidation")
    misc=$(Q "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0')::bigint,
                     (SELECT entries FROM supacache.pg_stat_keyspace_rowcache),
                     (SELECT coherent::int FROM supacache.pg_stat_keyspace_rowcache),
                     (SELECT count(*) FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhparent
                       WHERE c.relname='kv_ttl'),
                     (SELECT count(*) FROM supacache.kv),
                     (SELECT count(*) FROM pg_replication_slots)")

    sample_line="$(( $(date +%s) - t0 )),${rss:-0},${ks:-,,,,,,},${pers:-,,,},${inval:-,},${misc:-,,,,,}"
    echo "$sample_line" >> "$out"
    sleep "$SAMPLE_SECS"
  done
}

# --- thresholds -------------------------------------------------------------
# Deliberately loose enough that ordinary warm-up does not trip them and tight
# enough that an actual leak does. Each is a RATE or a RATIO over the run, never
# an absolute, so the same numbers work for a 10-minute run and a 6-hour one.
#
# TWO KINDS OF METRIC, JUDGED TWO WAYS. RSS and arena occupancy are smooth and
# the question about them is a trend, so they are compared as half-averages. The
# queue metrics -- persist lag, decode lag -- are SAWTOOTHS: they fill and drain,
# and a fault empties and refills them. Averaging halves of a sawtooth lets one
# spike decide the verdict, which is how the first version of this file reported
# "decoder falling behind" for a decoder that was fine: a single 34 MB sample,
# taken while the invalidation worker was being killed on purpose by
# faults.sh, dragged the second-half average up 23x. The series either side of it
# sat between 100 KB and 800 KB.
#
# So queues are judged on the MEDIAN (a spike cannot move it) plus the LAST
# sample (which is what says it recovered), and in SECONDS rather than bytes --
# a lag of 8 MB means nothing without the rate it is draining against, and
# seconds-behind is the number an operator actually has an opinion about.
RSS_GROWTH_PCT=${RSS_GROWTH_PCT:-25}       # RSS, second half vs first half
ENTRIES_GROWTH_PCT=${ENTRIES_GROWTH_PCT:-10}  # LIVE entries must plateau, not climb
MAX_PERSIST_DROPPED=${MAX_PERSIST_DROPPED:-0}
MAX_PERSIST_FAILED=${MAX_PERSIST_FAILED:-0}
MAX_PERSIST_LAG=${MAX_PERSIST_LAG:-5000}   # median acked-but-uncommitted writes
MAX_DECODE_LAG_SECS=${MAX_DECODE_LAG_SECS:-30}   # median, against measured WAL rate
MAX_FINAL_DECODE_SECS=${MAX_FINAL_DECODE_SECS:-60} # last sample: it came back
MIN_SAMPLES=${MIN_SAMPLES:-12}

judge() {
  local csv=$1 pass=0 fail=0
  chk() {
    if [ "$2" = "pass" ]; then echo "PASS  $1"; pass=$((pass+1));
    else echo "FAIL  $1"; echo "        $3"; fail=$((fail+1)); fi
  }
  local n
  n=$(( $(wc -l < "$csv") - 1 ))
  # A drift verdict from three samples is a coin flip dressed up as a check.
  if [ "$n" -lt "$MIN_SAMPLES" ]; then
    echo "FAIL  the run produced $n samples, fewer than the $MIN_SAMPLES needed to judge drift"
    echo "      (raise the duration or lower SAMPLE_SECS; a verdict from this many samples is noise)"
    return 1
  fi
  echo "drift over $n samples:"
  echo

  local verdicts
  verdicts=$(awk -F, '
    NR==1 { next }
    { rss[n+1]=$2; ent[n+1]=$3; evi[n+1]=$6; arena[n+1]=$8; lag[n+1]=$10;
      drop[n+1]=$12; failed[n+1]=$13; dec[n+1]=$14; wal[n+1]=$16; coh[n+1]=$18;
      ts[n+1]=$1; n++ }
    function half_avg(a, lo, hi,   s,c,i) { s=0; c=0; for(i=lo;i<=hi;i++){s+=a[i];c++} return c?s/c:0 }
    function grow_pct(a,   f,l) { f=half_avg(a,1,int(n/2)); l=half_avg(a,int(n/2)+1,n);
                                  return f>0 ? (l-f)*100/f : (l>0?999:0) }
    # Growth across the TAIL only (third quarter vs fourth). A cache filling to
    # its ceiling is not drift, it is the warm-up, and including it made a
    # healthy run that evicted 53,679 keys report "eviction is not keeping up"
    # purely because the first half contained the fill.
    function tail_pct(a,   f,l,q) { q=int(n/4); if(q<1) q=1;
                                    f=half_avg(a,n-2*q+1,n-q); l=half_avg(a,n-q+1,n);
                                    return f>0 ? (l-f)*100/f : (l>0?999:0) }
    # Median, so one fault-induced spike cannot decide a verdict.
    function median(a,   c,i,j,t,b) {
      for(i=1;i<=n;i++) b[i]=a[i]
      for(i=1;i<n;i++) for(j=1;j<=n-i;j++) if(b[j]>b[j+1]){t=b[j];b[j]=b[j+1];b[j+1]=t}
      return (n%2) ? b[int(n/2)+1] : (b[n/2]+b[n/2+1])/2
    }
    function maxof(a,   i,m) { m=a[1]; for(i=2;i<=n;i++) if(a[i]>m) m=a[i]; return m }
    END {
      if (n==0) { print "nodata"; exit }
      printf "rss %.1f %d %d\n",   grow_pct(rss),   half_avg(rss,1,int(n/2)),   half_avg(rss,int(n/2)+1,n)
      printf "entries %.1f %d %d\n", tail_pct(ent), half_avg(ent,n-2*int(n/4)+1,n-int(n/4)), half_avg(ent,n-int(n/4)+1,n)
      printf "arena %d %d\n",       arena[1], arena[n]
      printf "evictions %d %d\n",   evi[1], evi[n]
      printf "lag %d %d %d\n",     median(lag), maxof(lag), lag[n]
      printf "decode %d %d %d\n",  median(dec), maxof(dec), dec[n]
      # DELTAS over the sampled window, not the final cumulative value. These
      # counters are cumulative since the segment was created (which is right
      # for a collector), so the last sample also carries whatever happened
      # during cluster bootstrap -- before CREATE EXTENSION the supacache tables
      # do not exist yet and the first persist attempts fail. Judging the raw
      # total charged those two startup failures to the run.
      printf "dropped %d\n",       drop[n] - drop[1]
      printf "failed %d\n",        failed[n] - failed[1]
      printf "wal %d %d %d\n",     wal[1], wal[n], (ts[n]-ts[1] > 0 ? ts[n]-ts[1] : 1)
      inco=0; for(i=1;i<=n;i++) if (coh[i]!=1 && coh[i]!="") inco++
      printf "incoherent %d\n",    inco
    }' "$csv")

  local rss_pct rss_a rss_b ent_pct ent_a ent_b arena_a arena_b evi_a evi_b
  local lag_med lag_max lag_last dec_med dec_max dec_last
  local dropped failed wal_a wal_b wal_secs incoh
  read -r _ rss_pct rss_a rss_b   <<< "$(grep '^rss '       <<< "$verdicts")"
  read -r _ ent_pct ent_a ent_b   <<< "$(grep '^entries '   <<< "$verdicts")"
  read -r _ arena_a arena_b       <<< "$(grep '^arena '     <<< "$verdicts")"
  read -r _ evi_a evi_b           <<< "$(grep '^evictions ' <<< "$verdicts")"
  read -r _ lag_med lag_max lag_last  <<< "$(grep '^lag '        <<< "$verdicts")"
  read -r _ dec_med dec_max dec_last  <<< "$(grep '^decode '     <<< "$verdicts")"
  read -r _ dropped                   <<< "$(grep '^dropped '    <<< "$verdicts")"
  read -r _ failed                    <<< "$(grep '^failed '     <<< "$verdicts")"
  read -r _ wal_a wal_b wal_secs      <<< "$(grep '^wal '        <<< "$verdicts")"
  read -r _ incoh                     <<< "$(grep '^incoherent ' <<< "$verdicts")"

  # Bytes per second of WAL actually generated, which is what a decode lag in
  # bytes has to be divided by before it means anything.
  local wal_rate
  wal_rate=$(awk -v a="$wal_a" -v b="$wal_b" -v s="$wal_secs" 'BEGIN{r=(b-a)/s; print (r>0?r:1)}')

  # RSS is the leak check. Shared memory is reserved up front, so a cache at
  # steady state should be flat here; a second half meaningfully above the first
  # is memory that is not being given back.
  chk "RSS is flat between halves (${rss_a}KB -> ${rss_b}KB, ${rss_pct}%)" \
      "$(awk -v a="$rss_pct" -v t="$RSS_GROWTH_PCT" 'BEGIN{print (a<=t)?"pass":"fail"}')" \
      "grew ${rss_pct}%, over the ${RSS_GROWTH_PCT}% allowed -- memory not returned"

  # LIVE ENTRIES must plateau, judged over the TAIL of the run (third quarter
  # against fourth). A bounded cache under sustained load reaches its ceiling
  # and evicts; an entry count still climbing at the END means eviction is not
  # keeping up. Comparing the two HALVES instead would be measuring the warm-up,
  # which is why an earlier version reported a cache that evicted 53,679 keys as
  # "eviction is not keeping up".
  #
  # Entries, NOT arena bytes. `arena_used_bytes` is the slab allocator's bump
  # pointer, and store.rs is explicit that a freed block goes "onto a LIFO list
  # with no coalescing and no way to move data_bump back" -- so it is a
  # HIGH-WATER MARK that only stops rising when it reaches capacity. Judging it
  # as occupancy reported "eviction is not keeping up" for a cache that was
  # evicting hundreds of thousands of keys, which is a drift check that would
  # cry wolf on every healthy run.
  chk "live entries have plateaued over the tail (${ent_a} -> ${ent_b}, ${ent_pct}%)" \
      "$(awk -v a="$ent_pct" -v t="$ENTRIES_GROWTH_PCT" 'BEGIN{print (a<=t)?"pass":"fail"}')" \
      "still climbing at ${ent_pct}% -- eviction is not keeping up"

  # ...and it must have plateaued because it hit its CEILING, not because the
  # workload went quiet. Without this, a cache nobody wrote to passes the check
  # above perfectly.
  chk "the cache was at its ceiling and recycling ($(( evi_b - evi_a )) evictions)" \
      "$([ "$(( ${evi_b:-0} - ${evi_a:-0} ))" -gt 0 ] && echo pass || echo fail)" \
      "no eviction happened, so 'plateaued' here just means 'never filled'"

  # Persistence lag is a queue, so it is allowed to be nonzero and to spike. What
  # it must not do is SIT high: the median is the steady state, and a steady
  # state that is large means the cache is outrunning Postgres, with dropped
  # writes as the next thing that happens.
  chk "persist lag's steady state is low (median ${lag_med}, peak ${lag_max}, final ${lag_last})" \
      "$([ "${lag_med:-0}" -le "$MAX_PERSIST_LAG" ] && echo pass || echo fail)" \
      "median ${lag_med} acked-but-uncommitted writes, over the ${MAX_PERSIST_LAG} allowed"

  chk "no acknowledged write was dropped during the run (dropped=$dropped)" \
      "$([ "${dropped:-0}" -le "$MAX_PERSIST_DROPPED" ] && echo pass || echo fail)" \
      "$dropped dropped -- the ring overflowed and acks were discarded"
  chk "no persistence batch failed during the run (failed=$failed)" \
      "$([ "${failed:-0}" -le "$MAX_PERSIST_FAILED" ] && echo pass || echo fail)" \
      "$failed failed batches"

  # -1 is the sentinel for "no invalidation row", i.e. decoding is off. Judging
  # a decoder that is not running would be judging nothing.
  if [ "${dec_last:-0}" -ge 0 ] && [ "${dec_med:-0}" -ge 0 ]; then
    local dec_med_s dec_last_s dec_max_s
    dec_med_s=$(awk -v b="$dec_med"  -v r="$wal_rate" 'BEGIN{printf "%.1f", b/r}')
    dec_last_s=$(awk -v b="$dec_last" -v r="$wal_rate" 'BEGIN{printf "%.1f", b/r}')
    dec_max_s=$(awk -v b="$dec_max"  -v r="$wal_rate" 'BEGIN{printf "%.1f", b/r}')
    chk "WAL decode lag's steady state is bounded (median ${dec_med_s}s, peak ${dec_max_s}s)" \
        "$(awk -v a="$dec_med_s" -v t="$MAX_DECODE_LAG_SECS" 'BEGIN{print (a<=t)?"pass":"fail"}')" \
        "median ${dec_med_s}s behind, over the ${MAX_DECODE_LAG_SECS}s allowed"
    # The peak is expected -- faults.sh kills the decoder on purpose. What must
    # be true is that it CAME BACK, which only the last sample can say.
    chk "...and it recovered by the end (final ${dec_last_s}s)" \
        "$(awk -v a="$dec_last_s" -v t="$MAX_FINAL_DECODE_SECS" 'BEGIN{print (a<=t)?"pass":"fail"}')" \
        "still ${dec_last_s}s behind at the end, over the ${MAX_FINAL_DECODE_SECS}s allowed"
    chk "the row cache stayed coherent for every sample" \
        "$([ "${incoh:-0}" -eq 0 ] && echo pass || echo fail)" \
        "$incoh of $n samples reported coherent=false"
  else
    echo "SKIP  WAL decode lag (rowcache_decode is off in this run)"
  fi

  echo
  echo "arena high-water: $(( (arena_b - arena_a) / 1024 )) KB carved over the run (bump pointer, not live bytes)"
  echo "WAL generated: $(( (wal_b - wal_a) / 1024 / 1024 )) MB over ${wal_secs}s"
  echo "            rate: $(awk -v r="$wal_rate" 'BEGIN{printf "%.1f", r/1024/1024}') MB/s"
  echo
  echo "drift: $pass passed, $fail failed"
  [ "$fail" -eq 0 ]
}

case "${1:-}" in
  sample) sample "${2:?usage: drift.sh sample <out.csv>}" ;;
  judge)  judge  "${2:?usage: drift.sh judge <out.csv>}" ;;
  *) echo "usage: drift.sh {sample|judge} <csv>"; exit 2 ;;
esac
