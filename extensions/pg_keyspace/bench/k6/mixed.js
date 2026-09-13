// The mixed, multi-tenant, self-verifying workload a soak actually needs (#113).
//
// soak.js measures latency under a read-dominant mix. That is worth having and
// it is not what #113 asks for. The gap it names is that a two-minute
// happy-path run which finds nothing is WEAK evidence: everything real found
// recently came from the durability suite or from unit tests, not from load.
// So this script is shaped to find things rather than to produce a number.
//
// Four differences from soak.js, each answering one bullet of #113:
//
//   * EVERY read verifies a checksum. The value of `k` is a deterministic
//     function of `k`, so a reader needs no shared state to know what it should
//     have got back. A value that comes back wrong -- the wrong key's value, a
//     spliced read, a stale row after an invalidation -- fails the run. A soak
//     that only checks `v !== null` cannot tell a correct cache from one that
//     is confidently returning the wrong bytes.
//   * MULTI-TENANT. Each VU authenticates as one of several tenants, so keys
//     are force-scoped server-side and the #43/#102 fairness paths are under
//     real contention instead of being measured in isolation.
//   * WORKLOAD VARIETY. Hot keys as well as uniform ones, aggregates at size,
//     TTL churn heavy enough to cycle partitions, and pub/sub concurrent with
//     writes -- the combination, not each in its own run.
//   * It does not seed a corpus it then only reads. Writers and readers overlap
//     on the same keys, which is where a concurrency bug would live.
//
// The row cache (Mode B) is NOT driven from here: it is a SQL path, and the
// redis module cannot speak to Postgres. run_soak.sh drives it with pgbench
// alongside this script, which is also closer to how it is really used.
//
// Timing note inherited from soak.js and still load-bearing: k6 gives scripts
// no sub-millisecond clock, so each scenario does exactly ONE operation per
// iteration and the operation's latency IS `iteration_duration`.
import redis from 'k6/experimental/redis';
import { Counter } from 'k6/metrics';

const HOST      = __ENV.HOST || '127.0.0.1';
const PORT      = Number(__ENV.PORT || 6380);
// pg_keyspace shards the keyspace across WORKERS shared-nothing RESP workers,
// worker w on PORT + w, and answers a key it does not own with MOVED. A plain
// client does not follow that, so against two workers every operation failed
// with `MOVED 8488 127.0.0.1:6442` and the run reported 0 iterations while the
// checksum threshold passed vacuously -- exactly the shape of non-result #113
// was filed about. k6's redis client takes a cluster node list and follows the
// redirect, which is also what a real client would be configured with.
const WORKERS   = Number(__ENV.WORKERS || 1);
const TENANTS   = Number(__ENV.TENANTS || 3);
const SECRET    = __ENV.SECRET || 'soakpw';
const HOT_KEYS  = Number(__ENV.HOT_KEYS  || 2000);
const COLD_KEYS = Number(__ENV.COLD_KEYS || 200000);
const VAL_BYTES = Number(__ENV.VAL_BYTES || 256);
const PEAK      = Number(__ENV.PEAK || 40);
const DURATION  = __ENV.DURATION || '10m';
const LABEL     = __ENV.LABEL || 'mixed';
// Killing a RESP worker mid-flight drops the connections its clients are
// holding, and those surface as real errors -- so a run WITH fault injection
// cannot demand zero. run_soak.sh sets this from the fault schedule; with faults
// off it stays 0. The invariant that never relaxes is checksum_mismatch.
const MAX_ERRORS = Number(__ENV.MAX_ERRORS || 0);

const errs      = new Counter('op_errors');
const miss      = new Counter('unexpected_miss');
const corrupt   = new Counter('checksum_mismatch');
const verified  = new Counter('checksum_verified');
const published = new Counter('pubsub_published');

const n = {
  hot: new Counter('iters_hot'), cold: new Counter('iters_cold'),
  write: new Counter('iters_write'), ttl: new Counter('iters_ttl'),
  aggr: new Counter('iters_aggr'), pub: new Counter('iters_pub'),
};

// FNV-1a, 32-bit. Any hash would do; what matters is that it is cheap, pure,
// and computable identically by the writer and by every later reader.
function fnv1a(s) {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = (h + ((h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24))) >>> 0;
  }
  return h >>> 0;
}
// The value is derived from the key alone, so a reader can check it standalone.
// The checksum sits at the FRONT: a read that returns the right length of the
// wrong value, or two values spliced at a boundary, fails on the first bytes.
function valueFor(key) {
  const tag = ('00000000' + fnv1a(key).toString(16)).slice(-8);
  const body = (key + '|').repeat(Math.ceil(VAL_BYTES / (key.length + 1)));
  return (tag + '|' + body).slice(0, VAL_BYTES);
}
// A nil reply arrives as a REJECTION in this client, with the message
// `redis: nil`. That is a miss, and a miss is always allowed: a cache under
// eviction pressure is entitled to have dropped any key, including a hot one.
// Counting it as an error made `op_errors` 1.4M on a perfectly healthy run and
// would have buried a real protocol or connection failure among them.
function isNil(e) { return String(e).indexOf('redis: nil') !== -1; }

// An error count with no error text is a number nobody can act on. Log the
// first few per VU, with the scenario that produced them, so an unexplained
// floor of errors can be identified instead of budgeted around -- which is how
// #130 was found and how the remainder after it was.
let logged = 0;
function noteErr(where, e) {
  errs.add(1);
  if (logged < 3) { logged++; console.error(`op_error[${where}] ${e}`); }
}

function verify(key, got) {
  if (got === null || got === undefined) { miss.add(1); return false; }
  const want = valueFor(key);
  if (got.length !== want.length || got !== want) { corrupt.add(1); return false; }
  verified.add(1);
  return true;
}

// Each VU is one tenant's client. Tenants are numbered, and the server scopes
// their keys, so two tenants writing "item:7" are writing different keys.
function tenantOf(vu) { return `t${vu % TENANTS}`; }
function urlsFor(t) {
  const u = [];
  for (let w = 0; w < WORKERS; w++) u.push(`redis://${t}:${SECRET}@${HOST}:${PORT + w}`);
  return u;
}
function clientFor(vu) {
  const t = tenantOf(vu);
  const urls = urlsFor(t);
  // One worker is not a cluster; asking for cluster mode against a single node
  // works but adds a slot-map round trip per client for nothing.
  return urls.length === 1
    ? new redis.Client(urls[0])
    : new redis.Client({ cluster: { nodes: urls } });
}
let _c = null;
function client() { if (_c === null) _c = clientFor(__VU); return _c; }

const stage = (peak) => ([
  { duration: '20s',   target: Math.ceil(peak / 3) },
  { duration: DURATION, target: peak },
  { duration: '10s',   target: 0 },
]);

export const options = {
  scenarios: {
    // Hot keys: a small set hammered by everyone, which is where contention
    // and any read/write tearing would show.
    hot:   { executor: 'ramping-vus', exec: 'hot',   startVUs: 2, stages: stage(PEAK),                 gracefulRampDown: '10s' },
    // Cold keys: a large uniform set, which is what drives eviction.
    cold:  { executor: 'ramping-vus', exec: 'cold',  startVUs: 2, stages: stage(Math.ceil(PEAK / 2)),  gracefulRampDown: '10s' },
    write: { executor: 'ramping-vus', exec: 'write', startVUs: 1, stages: stage(Math.ceil(PEAK / 3)),  gracefulRampDown: '10s' },
    ttl:   { executor: 'ramping-vus', exec: 'ttl',   startVUs: 1, stages: stage(Math.ceil(PEAK / 6)),  gracefulRampDown: '10s' },
    aggr:  { executor: 'ramping-vus', exec: 'aggr',  startVUs: 1, stages: stage(Math.ceil(PEAK / 8)),  gracefulRampDown: '10s' },
    pub:   { executor: 'ramping-vus', exec: 'pub',   startVUs: 1, stages: stage(Math.ceil(PEAK / 8)),  gracefulRampDown: '10s' },
  },
  // Assertions, not decoration. A checksum mismatch is the one that matters:
  // it is the only threshold here that can catch a silent correctness failure,
  // and #113 exists because nothing was checking for one under load.
  thresholds: {
    // The one invariant a cache must never break: never return the WRONG bytes.
    // A miss is always permitted -- eviction is allowed to drop anything -- so
    // `unexpected_miss` is reported, not thresholded. Making it fatal would mean
    // the harness could only pass on a cache big enough never to evict, which is
    // the opposite of what a soak should exercise.
    'checksum_mismatch': ['count==0'],
    'op_errors':         [`count<=${MAX_ERRORS}`],
    'checksum_verified': ['count>0'],   // a run that verified nothing passed nothing
    'iteration_duration{scenario:hot}':   ['p(95)<5000'],
    'iteration_duration{scenario:cold}':  ['p(95)<5000'],
    'iteration_duration{scenario:write}': ['p(95)<60000'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(95)', 'p(99)', 'max'],
};

export async function setup() {
  // Seed the hot set only. The cold set is written by `write` as the run goes,
  // so readers and writers overlap on it -- which is the point. A fully
  // pre-seeded corpus that is then only read cannot expose a race.
  const c = clientFor(0);
  for (let i = 0; i < HOT_KEYS; i++) {
    const k = `hot:${i}`;
    await c.set(k, valueFor(k), 0);
  }
  // Prove the seed landed AND that the checksum scheme survives a round trip
  // through the server. If the value comes back altered, every later mismatch
  // would be this bug rather than a finding.
  const k = `hot:${HOT_KEYS - 1}`;
  const got = await c.get(k);
  if (got !== valueFor(k)) {
    throw new Error(`seed round-trip failed: the checksum scheme does not survive the server, so this run could not tell a real corruption from this bug`);
  }
  // Every op the run performs must be reachable through this client BEFORE the
  // run, or a missing client method becomes millions of instant failures that
  // look like load. That is not hypothetical: `publish` does not exist on this
  // client, and the first version of this script found out by running for four
  // minutes and reporting 5.1M iterations of nothing.
  for (const [label, fn] of [
    ['SET',     () => c.set('probe:set', valueFor('probe:set'), 0)],
    ['SET EX',  () => c.set('probe:ttl', valueFor('probe:ttl'), 30)],
    ['HSET',    () => c.hset('probe:h', 'f', 'v')],
    ['PUBLISH', () => c.sendCommand('PUBLISH', 'probe:ch', 'x')],
  ]) {
    try { await fn(); } catch (e) {
      throw new Error(`${label} is not usable through this client (${e}) -- the run would have counted its failures as load`);
    }
  }
  return { seeded: HOT_KEYS };
}

export async function hot() {
  n.hot.add(1);
  const k = `hot:${Math.floor(Math.random() * HOT_KEYS)}`;
  try { verify(k, await client().get(k)); }
  catch (e) { if (isNil(e)) miss.add(1); else noteErr('hot', e); }
}

export async function cold() {
  n.cold.add(1);
  // Reads a key `write` may not have written yet. A nil is expected and is not
  // a miss worth failing on; a WRONG value is.
  const k = `cold:${Math.floor(Math.random() * COLD_KEYS)}`;
  try {
    const v = await client().get(k);
    if (v !== null && v !== undefined && v !== valueFor(k)) corrupt.add(1);
    else if (v) verified.add(1);
  } catch (e) { if (!isNil(e)) noteErr('cold', e); }
}

export async function write() {
  n.write.add(1);
  const k = `cold:${Math.floor(Math.random() * COLD_KEYS)}`;
  try { await client().set(k, valueFor(k), 0); } catch (e) { noteErr('write', e); }
}

export async function ttl() {
  n.ttl.add(1);
  // Short TTLs at a decent rate, so kv_ttl partitions are created and dropped
  // during the run rather than accumulating unexercised.
  const k = `eph:${__VU}:${Math.floor(Math.random() * 5000)}`;
  try { await client().set(k, valueFor(k), 30); } catch (e) { noteErr('ttl', e); }
}

export async function aggr() {
  n.aggr.add(1);
  // Aggregates AT SIZE: a hash that keeps growing, so the indexed encoding and
  // its reallocation path are under load rather than measured on a fresh key.
  const k = `agg:h:${__VU % 16}`;
  const f = `f${Math.floor(Math.random() * 5000)}`;
  try { await client().hset(k, f, valueFor(f)); } catch (e) { noteErr('aggr', e); }
}

export async function pub() {
  n.pub.add(1);
  // Pub/sub concurrent with writes, which is the combination #113 calls out as
  // never having been exercised together.
  //
  // sendCommand, not `publish`: the k6 redis client has no publish method, and
  // calling one threw `TypeError: Object has no member 'publish'` instantly --
  // so this scenario ran 5.1 MILLION no-op iterations, inflating the iteration
  // count and the error count while publishing nothing. The setup guard below
  // is what stops that recurring silently.
  try {
    await client().sendCommand('PUBLISH', `ch:${__VU % 8}`, valueFor(`m${__ITER}`));
    published.add(1);
  } catch (e) { noteErr('pub', e); }
}

export function handleSummary(data) {
  const m = data.metrics;
  const v = (k, s) => (m[k] && m[k].values[s] !== undefined ? m[k].values[s] : 0);
  const sub = (s, stat) => {
    const k = `iteration_duration{scenario:${s}}`;
    return m[k] && m[k].values[stat] !== undefined ? (m[k].values[stat] * 1000).toFixed(0) : 'n/a';
  };
  const L = [];
  L.push(`\n=== ${LABEL} (${TENANTS} tenants, ${WORKERS} worker(s)) ===`);
  L.push(`iterations: ${v('iterations','count')}   sustained: ${v('iterations','rate').toFixed(0)} ops/s`);
  L.push(`checksums verified: ${v('checksum_verified','count')}   MISMATCHES: ${v('checksum_mismatch','count')}`);
  const it = v('iterations','count') || 1;
  L.push(`errors: ${v('op_errors','count')} (${(v('op_errors','count')*100/it).toFixed(3)}%, budget ${MAX_ERRORS})   misses: ${v('unexpected_miss','count')}   published: ${v('pubsub_published','count')}`);
  L.push('');
  L.push('scenario      n           min      p50      p95      p99      max   (microseconds)');
  for (const s of ['hot','cold','write','ttl','aggr','pub']) {
    L.push(s.padEnd(12) + String(v(`iters_${s}`,'count')).padStart(9) + '  ' +
      sub(s,'min').padStart(8) + ' ' + sub(s,'med').padStart(8) + ' ' +
      sub(s,'p(95)').padStart(8) + ' ' + sub(s,'p(99)').padStart(8) + ' ' + sub(s,'max').padStart(8));
  }
  L.push('');
  return {
    stdout: L.join('\n') + '\n',
    [__ENV.SUMMARY_JSON || '/dev/null']: JSON.stringify(data),
  };
}
