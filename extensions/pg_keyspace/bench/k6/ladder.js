// Latency against offered load, open model.
//
// The ramping-VU soak could not tell pg_keyspace from redis: both landed within
// 0.6% of each other because the k6 process, not either server, was the ceiling
// (4 cores, shared with the server under test). A closed-loop generator at
// saturation measures itself.
//
// This asks the question that survives that: at a FIXED offered rate well below
// the generator's ceiling, what latency does each target produce? Arrival rate
// is held by k6 regardless of how fast replies come back, so a target that is
// struggling shows up as latency rather than as a lower rate. One GET per
// iteration, timed by k6 in Go — see the note in soak.js about why JS timing is
// useless here.
import redis from 'k6/experimental/redis';
import { Counter } from 'k6/metrics';

const RATE     = Number(__ENV.RATE || 5000);
const HOT_KEYS = Number(__ENV.HOT_KEYS || 5000);
const TARGET   = __ENV.TARGET || 'redis://127.0.0.1:6444';
const LABEL    = __ENV.LABEL  || 'unnamed';

const OP = __ENV.OP || 'get';
const errs = new Counter('op_errors');
const miss = new Counter('unexpected_miss');

export const options = {
  discardResponseBodies: true,
  scenarios: {
    fixed: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: '20s',
      // Sized for the SLOWEST target under test, not the fastest: a durable
      // write is held until its batch commits (~ms), so the VU pool has to be
      // deep enough to keep the arrival rate offered. Too few VUs and k6 drops
      // iterations and reports its own starvation as the server's throughput.
      preAllocatedVUs: Number(__ENV.VUS || Math.max(200, Math.ceil(RATE / 10))),
      maxVUs: Number(__ENV.MAXVUS || Math.max(600, Math.ceil(RATE / 2))),
    },
  },
  thresholds: { 'op_errors': ['count==0'], 'unexpected_miss': ['count==0'] },
  summaryTrendStats: ['min', 'med', 'p(95)', 'p(99)', 'max'],
};

const client = new redis.Client(TARGET);

export async function setup() {
  const c = new redis.Client(TARGET);
  const val = 'x'.repeat(256);
  for (let i = 0; i < HOT_KEYS; i++) await c.set(`app:item:${i}`, val, 0);
  const probe = await c.get(`app:item:${HOT_KEYS - 1}`);
  if (!probe || probe.length !== 256) throw new Error('seed failed — would have measured an empty cache');
  return {};
}

const VAL = 'x'.repeat(256);

export default async function () {
  try {
    if (OP === 'set') {
      // Distinct keys: same-key writes collapse within a persist window, which
      // would measure dedup rather than the cost of durability.
      await client.set(`app:w:${__VU}:${__ITER}`, VAL, 600);
    } else {
      const v = await client.get(`app:item:${Math.floor(Math.random() * HOT_KEYS)}`);
      if (!v || v.length !== 256) miss.add(1);
    }
  } catch (e) { errs.add(1); }
}

export function handleSummary(data) {
  const m = data.metrics, d = m.iteration_duration.values;
  const us = (x) => (x * 1000).toFixed(0);
  // achieved vs offered is the honest saturation check: if k6 could not keep up,
  // the latency numbers below describe a queue in the generator, not the server.
  // Computed from the scenario's own duration, not from the whole process:
  // seeding runs before the timed window and would otherwise deflate the rate.
  const achieved = m.iterations.values.count / 20;
  return { stdout:
    `${LABEL.padEnd(30)} op=${OP} offered=${String(RATE).padStart(6)}/s  achieved=${achieved.toFixed(0).padStart(6)}/s` +
    `  min=${us(d.min).padStart(5)}  p50=${us(d.med).padStart(6)}  p95=${us(d['p(95)']).padStart(7)}` +
    `  p99=${us(d['p(99)']).padStart(7)}  max=${us(d.max).padStart(7)}  (us)  errors=${m.op_errors ? m.op_errors.values.count : 0}\n` };
}
