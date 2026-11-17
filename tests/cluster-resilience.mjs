#!/usr/bin/env node
/**
 * Writes during a node kill on an S3 option B cluster. W writers STORE new keys, each in
 * one of N namespaces, through any node (any node takes any write; one that doesn't
 * answer is skipped). Someone else kills and restarts a node meanwhile (the runner,
 * over ssh). Reports writes per second, the longest stretch with no write
 * acknowledged (the failover as clients see it), errors, and then reads back every
 * acknowledged key through the nodes still up: a missing one is a lost write.
 *
 *   ADB_AUTH_TOKEN=… node cluster-resilience.mjs --nodes http://a:3000,http://b:3000,http://c:3000 [--writers 32] [--secs 90] [--namespaces 64]
 */
const args = Object.fromEntries(process.argv.slice(2).reduce((a, v, i, all) => (v.startsWith('--') ? [...a, [v.slice(2), all[i + 1]]] : a), []));
const NODES = String(args.nodes).split(',');
const W = Number(args.writers ?? 32);
const SECS = Number(args.secs ?? 90);
const NSN = Number(args.namespaces ?? 64);
const token = process.env.ADB_AUTH_TOKEN;
const H = { 'content-type': 'application/json', ...(token ? { authorization: `Bearer ${token}` } : {}) };
const run = Date.now().toString(36);
const ns = k => `perf-res-${run}-${k % NSN}`;
console.log(`run ${run} (namespaces perf-res-${run}-0..${NSN - 1})`);

// As a load balancer with health checks: a node that couldn't be reached is skipped
// for 2 s (a dead node no longer holds a writer for its whole timeout).
const downUntil = new Map();
const pick = () => {
  const now = Date.now();
  for (let k = 0; k < NODES.length; k++) {
    const n = NODES[turn++ % NODES.length];
    if ((downUntil.get(n) ?? 0) <= now) return n;
  }
  return NODES[turn++ % NODES.length];
};
const unreachable = e => /fetch failed|ECONNREFUSED|EHOSTUNREACH|aborted due to timeout/.test(String(e.message ?? e));

async function q(url, aql, ms = 10000) {
  const r = await fetch(`${url}/query`, { method: 'POST', headers: H, body: JSON.stringify({ query: aql }), signal: AbortSignal.timeout(ms) });
  const j = await r.json();
  if (!j.success) throw new Error(j.error ?? r.status);
  return j.data;
}

const acked = [];
let errors = 0, lastAck = Date.now(), maxGap = 0, gapAt = 0, next = 0, turn = 0;
const perSec = new Map();
const errs = new Map();
const t0 = Date.now();
const end = t0 + SECS * 1000;

async function writer() {
  while (Date.now() < end) {
    const i = next++;
    const key = `${run}k${i}`;
    for (let attempt = 0; attempt < NODES.length * 4 && Date.now() < end; attempt++) {
      const node = pick();
      try {
        await q(node, `STORE INTO SEMANTIC (key = "${key}", n = ${i}) NAMESPACE "${ns(i)}"`, 8000);
        const now = Date.now();
        const gap = now - lastAck;
        if (gap > maxGap) { maxGap = gap; gapAt = Math.round((lastAck - t0) / 1000); }
        lastAck = Math.max(lastAck, now);
        acked.push(i);
        const s = Math.floor((now - t0) / 1000);
        perSec.set(s, (perSec.get(s) ?? 0) + 1);
        break;
      } catch (e) {
        errors++;
        if (unreachable(e)) downUntil.set(node, Date.now() + 2000);
        const m = String(e.message ?? e).slice(0, 60);
        errs.set(m, (errs.get(m) ?? 0) + 1);
        await new Promise(r => setTimeout(r, 20));
      }
    }
  }
}

console.log(`${W} writers, ${NSN} namespaces, ${SECS} s, through ${NODES.length} nodes`);
await Promise.all(Array.from({ length: W }, writer));
const secs = [...perSec.keys()].sort((a, b) => a - b);
console.log('writes/s by second: ' + secs.map(s => `${s}:${perSec.get(s)}`).join(' '));
console.log(`acked ${acked.length}, errors ${errors}, longest stretch without an ack ${maxGap} ms (from ~${gapAt} s)`);
for (const [m, n] of [...errs].sort((a, b) => b[1] - a[1]).slice(0, 5)) console.log(`  error x${n}: ${m}`);

// Read every acknowledged key back, through whichever node answers.
let missing = 0, checked = 0;
const lost = [];
const batch = 64;
for (let b = 0; b < acked.length; b += batch) {
  await Promise.all(acked.slice(b, b + batch).map(async i => {
    for (let attempt = 0; attempt < NODES.length * 3; attempt++) {
      const node = NODES[(i + attempt) % NODES.length];
      try {
        const d = await q(node, `LOOKUP FROM SEMANTIC KEY id = "${run}k${i}" NAMESPACE "${ns(i)}"`);
        checked++;
        if ((d?.data?.count ?? d?.data?.records?.length ?? d?.count ?? d?.records?.length ?? 0) < 1) { missing++; if (lost.length < 10) lost.push(i); }
        return;
      } catch {}
    }
    missing++;
    if (lost.length < 10) lost.push(i);
  }));
}
console.log(`read back ${checked} of ${acked.length}; missing ${missing}${lost.length ? ` (e.g. ${lost.join(', ')})` : ''}`);
console.log(missing === 0 ? 'NO ACKNOWLEDGED WRITE LOST' : 'ACKNOWLEDGED WRITES LOST');
