// A sustained, closed-loop cache workload — deliberately NOT a pipelined
// throughput bench.
//
// bench/ measures ceilings: how fast the machinery goes when one client
// pipelines as deeply as it can. That is the right question for capacity
// planning and the wrong one for "what will my application see". Real traffic
// is closed-loop — each request waits for its reply — and concurrency comes
// from many such clients, not from depth on one. This measures that, for
// minutes rather than seconds, so drift and eviction have somewhere to show up.
//
// Timing note, which is the whole reason this file is shaped the way it is:
// k6 exposes no sub-millisecond clock to scripts (no `performance.now()`), so
// any latency computed in JS is quantised to 1 ms and useless against a store
// that answers in ~40 µs. `iteration_duration` is timed by k6 itself, in Go, at
// microsecond resolution. So each scenario does exactly ONE operation per
// iteration and the operation's latency IS the iteration duration. The
// thresholds below name tagged sub-metrics, which is also what materialises the
// per-scenario breakdown in the summary.
//
// The same script runs against pg_keyspace and against redis-server, so the two
// are compared on one box under one generator.
import redis from 'k6/experimental/redis';
import { Counter } from 'k6/metrics';

const HOT_KEYS = Number(__ENV.HOT_KEYS || 5000);
const TARGET   = __ENV.TARGET || 'redis://127.0.0.1:6444';
const LABEL    = __ENV.LABEL  || 'unnamed';
const PEAK     = Number(__ENV.PEAK || 60);

const errs = new Counter('op_errors');
const miss = new Counter('unexpected_miss');
// Tagged sub-metrics of a Trend do not carry a sample count in the summary, so
// each scenario counts its own iterations.
const n = {
  get_hit:  new Counter('iters_get_hit'),
  get_miss: new Counter('iters_get_miss'),
  write:    new Counter('iters_write'),
  hash:     new Counter('iters_hash'),
};

const ramp = (peak) => ([
  { duration: '15s', target: Math.ceil(peak / 2) },
  { duration: '40s', target: Math.ceil(peak / 2) },
  { duration: '10s', target: peak },
  { duration: '40s', target: peak },
  { duration: '5s',  target: 0 },
]);

// Weighted by VU allocation rather than by a random branch, so each op type
// gets its own timed iteration. Read-dominant, as a cache is.
export const options = {
  scenarios: {
    get_hit:  { executor: 'ramping-vus', exec: 'getHit',  startVUs: 2, stages: ramp(PEAK),               gracefulRampDown: '5s' },
    get_miss: { executor: 'ramping-vus', exec: 'getMiss', startVUs: 1, stages: ramp(Math.ceil(PEAK/8)),  gracefulRampDown: '5s' },
    write:    { executor: 'ramping-vus', exec: 'write',   startVUs: 1, stages: ramp(Math.ceil(PEAK/4)),  gracefulRampDown: '5s' },
    hash:     { executor: 'ramping-vus', exec: 'hash',    startVUs: 1, stages: ramp(Math.ceil(PEAK/8)),  gracefulRampDown: '5s' },
  },
  // Assertions, not decoration: a load test that cannot fail measures nothing.
  thresholds: {
    'op_errors':       ['count==0'],
    'unexpected_miss': ['count==0'],
    'iteration_duration{scenario:get_hit}':  ['p(95)<2000', 'p(99)<10000'],
    'iteration_duration{scenario:get_miss}': ['p(95)<2000'],
    'iteration_duration{scenario:write}':    ['p(95)<25000'],
    'iteration_duration{scenario:hash}':     ['p(95)<25000'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(95)', 'p(99)', 'max'],
};

const client = new redis.Client(TARGET);

export async function setup() {
  // Seed a working set so reads are hits. Without this the run measures miss
  // handling and reports it as cache latency.
  const c = new redis.Client(TARGET);
  const val = 'x'.repeat(256);
  for (let i = 0; i < HOT_KEYS; i++) await c.set(`app:item:${i}`, val, 0);
  const probe = await c.get(`app:item:${HOT_KEYS - 1}`);
  if (!probe || probe.length !== 256) {
    throw new Error('seed failed — this run would have measured an empty cache');
  }
  return { seeded: HOT_KEYS };
}

export async function getHit() {
  n.get_hit.add(1);
  try {
    const v = await client.get(`app:item:${Math.floor(Math.random() * HOT_KEYS)}`);
    // A silent miss here would look like excellent latency, so it is counted
    // and it fails the run.
    if (!v || v.length !== 256) miss.add(1);
  } catch (e) { errs.add(1); }
}

export async function getMiss() {
  n.get_miss.add(1);
  // Miss handling shares the event loop with hits, so it belongs in the mix.
  // A nil reply surfaces as a rejection in this client; that is the expected
  // outcome here and is not an error.
  try { await client.get(`app:absent:${Math.floor(Math.random() * 1e6)}`); } catch (e) { /* expected */ }
}

export async function write() {
  n.write.add(1);
  try {
    await client.set(`app:sess:${__VU}:${Math.floor(Math.random() * 1000)}`, `v${Date.now()}`, 300);
  } catch (e) { errs.add(1); }
}

export async function hash() {
  n.hash.add(1);
  try {
    await client.hset(`app:h:${__VU}`, 'last', String(Date.now()));
  } catch (e) { errs.add(1); }
}

export function handleSummary(data) {
  const m = data.metrics;
  const sub = (s, stat) => {
    const k = `iteration_duration{scenario:${s}}`;
    return m[k] && m[k].values[stat] !== undefined ? (m[k].values[stat] * 1000).toFixed(0) : 'n/a';
  };
  const cnt = (s) => (m[`iters_${s}`] ? m[`iters_${s}`].values.count : 0);
  const L = [];
  L.push(`\n=== ${LABEL} ===`);
  L.push(`total iterations: ${m.iterations.values.count}   sustained: ${m.iterations.values.rate.toFixed(0)} ops/s`);
  L.push(`errors: ${m.op_errors ? m.op_errors.values.count : 0}   unexpected misses: ${m.unexpected_miss ? m.unexpected_miss.values.count : 0}`);
  L.push('');
  L.push('scenario      n         min      p50      p95      p99      max    (microseconds)');
  for (const s of ['get_hit', 'get_miss', 'write', 'hash']) {
    L.push(
      s.padEnd(12) +
      String(cnt(s)).padStart(9) + '  ' +
      sub(s, 'min').padStart(8) + ' ' +
      sub(s, 'med').padStart(8) + ' ' +
      sub(s, 'p(95)').padStart(8) + ' ' +
      sub(s, 'p(99)').padStart(8) + ' ' +
      sub(s, 'max').padStart(8)
    );
  }
  L.push('');
  return { stdout: L.join('\n') + '\n' };
}
