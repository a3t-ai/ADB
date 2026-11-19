#!/usr/bin/env node
/**
 * Writes during a node kill, on a running replica set (AWS). W writers STORE new keys
 * for --secs, always at the current leader (found through every node's /ready). Someone
 * else kills and restarts a node meanwhile (the AWS runner, over ssh). Reports writes
 * per second, the longest stretch with no write acknowledged (the takeover as clients
 * see it), errors, and then reads back every acknowledged key on the leader.
 *
 *   ADB_AUTH_TOKEN=… node aws-resilience.mjs --nodes http://a:3000,http://b:3000,http://c:3000 [--writers 16] [--secs 90]
 */
const args = Object.fromEntries(process.argv.slice(2).reduce((a, v, i, all) => (v.startsWith('--') ? [...a, [v.slice(2), all[i + 1]]] : a), []));
const NODES = String(args.nodes).split(',');
const W = Number(args.writers ?? 16);
const SECS = Number(args.secs ?? 90);
const token = process.env.ADB_AUTH_TOKEN;
const H = { 'content-type': 'application/json', ...(token ? { authorization: `Bearer ${token}` } : {}) };
const run = Date.now().toString(36);
const sleep = ms => new Promise(r => setTimeout(r, ms));

let leader = null;
async function findLeader() {
  for (;;) {
    for (const n of NODES) {
      try { if ((await fetch(`${n}/ready`, { signal: AbortSignal.timeout(500) })).ok) return n; } catch {}
    }
    await sleep(50);
  }
}
async function q(url, aql) {
  const r = await fetch(`${url}/query`, { method: 'POST', headers: H, body: JSON.stringify({ query: aql }), signal: AbortSignal.timeout(10000) });
  const j = await r.json();
  if (!j.success) throw new Error(j.error ?? r.status);
  return j.data;
}

leader = await findLeader();
console.log(`leader at start: ${leader}`);
const acked = [];
let errors = 0, lastAck = Date.now(), maxGap = 0, gapAt = 0, i = 0;
const perSec = new Map();
const t0 = Date.now();
const end = t0 + SECS * 1000;
await Promise.all(Array.from({ length: W }, async () => {
  while (Date.now() < end) {
    const key = `${run}k${i++}`;
    try {
      await q(leader, `STORE INTO SEMANTIC (key = "${key}", topic = "r", content = "resilience ${key}")`);
      const now = Date.now();
      const gap = now - lastAck;
      if (gap > maxGap) { maxGap = gap; gapAt = Math.round((lastAck - t0) / 1000); }
      lastAck = now;
      acked.push(key);
      const s = Math.floor((now - t0) / 1000);
      perSec.set(s, (perSec.get(s) ?? 0) + 1);
    } catch {
      errors++;
      const before = leader;
      leader = await findLeader();
      if (leader !== before) console.log(`  ${((Date.now() - t0) / 1000).toFixed(1)} s: leader now ${leader}`);
    }
  }
}));
const series = Array.from({ length: SECS }, (_, s) => perSec.get(s) ?? 0);
console.log(`writes/s by second: ${series.join(' ')}`);
console.log(`acknowledged ${acked.length}, errors ${errors}, longest stretch with no acknowledged write ${maxGap} ms (from ${gapAt} s)`);
// Every acknowledged key must be readable on the leader (after a moment to settle).
await sleep(2000);
leader = await findLeader();
let missing = 0, checked = 0;
const keys = acked.slice();
await Promise.all(Array.from({ length: 32 }, async () => {
  while (keys.length) {
    const k = keys.pop();
    try {
      const d = await q(leader, `RECALL FROM SEMANTIC WHERE id = "${k}"`);
      if (!d || (Array.isArray(d.records) && d.records.length === 0)) missing++;
    } catch { missing++; }
    checked++;
  }
}));
console.log(`read back on ${leader}: ${checked} checked, ${missing} missing`);
console.log(missing === 0 ? '✓ no acknowledged write lost' : `✗ ${missing} acknowledged writes missing`);
