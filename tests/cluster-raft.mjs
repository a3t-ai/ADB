#!/usr/bin/env node
/**
 * M3 gate (.ai/M3_RAFT.md): a 3-node ADB replica set loses no acknowledged write.
 *
 * Three `adb serve-http` processes with ADB_NODE_ID / ADB_PEERS, each on its own
 * --data-dir, under a supervisor that restarts a node when it exits (as
 * Kubernetes does: a leader that loses the lead exits with 75). Concurrent
 * writers STORE into EPISODIC through whichever node answers /ready (the
 * leader). The leader is SIGKILLed K times; each time:
 *   - a new leader must answer /ready within the failover budget,
 *   - the killed node is restarted and must catch up as a follower.
 * At the end every acknowledged write must be readable on the final leader, and
 * every node's WAL must reach the same offset.
 *
 *   node tests/cluster-raft.mjs --adb <adb bin> [--kills 3] [--writers 4] [--budget-ms 5000]
 */
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer as netServer } from 'node:net';

const args = Object.fromEntries(process.argv.slice(2).reduce((a, v, i, all) => (v.startsWith('--') ? [...a, [v.slice(2), all[i + 1]]] : a), []));
if (!args.adb) {
  console.error('usage: cluster-raft.mjs --adb <adb bin> [--kills 3] [--writers 4]');
  process.exit(2);
}
const KILLS = Number(args.kills ?? 3);
const WRITERS = Number(args.writers ?? 4);
const BUDGET = Number(args['budget-ms'] ?? 5000);
const sleep = ms => new Promise(r => setTimeout(r, ms));
const freePort = () => new Promise(res => { const s = netServer().listen(0, () => { const p = s.address().port; s.close(() => res(p)); }); });
let failures = 0;
const check = (label, ok, detail = '') => { console.log(`${ok ? '✓' : '✗'} ${label}${ok || !detail ? '' : `  — ${detail}`}`); if (!ok) failures++; };

const tmp = mkdtempSync(join(tmpdir(), 'cluster-raft-'));
const nodes = [];
let stopping = false;

for (let id = 1; id <= 3; id++) nodes.push({ id, http: await freePort(), flight: await freePort(), raft: await freePort(), dir: join(tmp, `n${id}`), restarts: 0 });
const peers = nodes.map(n => `${n.id}=http://127.0.0.1:${n.raft}`).join(',');

function launch(n) {
  const c = spawn(args.adb, ['--data-dir', n.dir, 'serve-http', '--host', '127.0.0.1', '--port', String(n.http), '--flight-port', String(n.flight)], {
    env: { ...process.env, RUST_LOG: process.env.RUST_LOG ?? 'warn,adb_cli=info', ADB_NODE_ID: String(n.id), ADB_PEERS: peers, ADB_RAFT_PORT: String(n.raft) },
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  n.log = '';
  c.stderr.on('data', d => { n.log = (n.log + d).slice(-20000); });
  c.on('exit', (code, signal) => {
    n.exit = { code, signal };
    if (stopping || n.child !== c) return;
    // The supervisor: start it again (a lost lead exits 75; a kill is restarted by the test itself).
    if (code === 75) {
      n.restarts++;
      setTimeout(() => { if (!stopping) launch(n); }, 200);
    }
  });
  n.child = c;
  return c;
}

const url = n => `http://127.0.0.1:${n.http}`;
async function ready(n) {
  try { return (await fetch(`${url(n)}/ready`, { signal: AbortSignal.timeout(500) })).ok; } catch { return false; }
}
async function leader(timeoutMs = 20000) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    for (const n of nodes) if (await ready(n)) return n;
    await sleep(50);
  }
  throw new Error(`no leader within ${timeoutMs} ms:\n${nodes.map(n => `--- node ${n.id}\n${n.log.slice(-3000)}`).join('\n')}`);
}
async function status(n) {
  try { return await (await fetch(`http://127.0.0.1:${n.raft}/cluster/status`, { signal: AbortSignal.timeout(1000) })).json(); } catch { return null; }
}
async function query(n, aql) {
  const r = await fetch(`${url(n)}/query`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ query: aql }), signal: AbortSignal.timeout(5000) });
  return r.json();
}

const acked = new Set();
let attempts = 0, errors = 0;
let current = null;
async function writer(w) {
  let i = 0;
  while (!stopping) {
    const key = `w${w}-${i}`;
    attempts++;
    try {
      const n = current;
      if (!n) { await sleep(50); continue; }
      const r = await query(n, `STORE INTO EPISODIC (bid_id = "${key}", kind = "raft", writer = ${w})`);
      if (r.success) { acked.add(key); i++; } else { errors++; await sleep(50); }
    } catch { errors++; await sleep(50); }
  }
}

try {
  for (const n of nodes) launch(n);
  current = await leader();
  console.log(`  leader: node ${current.id}`);
  const ws = Array.from({ length: WRITERS }, (_, w) => writer(w));

  const failovers = [];
  for (let k = 0; k < KILLS; k++) {
    await sleep(2500);
    const old = current;
    const before = acked.size;
    current = null;
    const t0 = Date.now();
    old.child.kill('SIGKILL');
    // Writers pause until the new leader is ready.
    let next;
    try { next = await leader(BUDGET * 4); } catch (e) { check(`kill ${k + 1}: a new leader`, false, e.message); break; }
    const took = Date.now() - t0;
    failovers.push(took);
    current = next;
    console.log(`  kill ${k + 1}: node ${old.id} killed with ${before} writes acked; node ${next.id} leads after ${took} ms`);
    // The killed node comes back (as a follower).
    await sleep(500);
    launch(old);
  }
  await sleep(2500);
  stopping = true;
  await Promise.all(ws);
  stopping = false;

  check(`writes acknowledged across ${KILLS} leader kills (${acked.size} acked, ${errors} errors while switching)`, acked.size > 0);
  const worst = Math.max(...failovers, 0);
  check(`every failover within ${BUDGET} ms (worst ${worst} ms; ${failovers.join(', ')})`, failovers.length === KILLS && worst <= BUDGET);

  const L = await leader();
  const r = await query(L, 'RECALL FROM EPISODIC WHERE kind = "raft" LIMIT 1000000 RETURN bid_id');
  const present = new Set((r.data?.data?.records ?? []).map(rec => rec.data?.bid_id));
  const lost = [...acked].filter(k => !present.has(k));
  check(`no acknowledged write lost (${lost.length} lost of ${acked.size})`, lost.length === 0, lost.slice(0, 10).join(', '));

  // Every node's WAL reaches the leader's (followers catch up).
  let offsets = [];
  for (let t = 0; t < 100; t++) {
    offsets = (await Promise.all(nodes.map(status))).map(s => s?.wal_offset ?? -1);
    if (offsets.every(o => o === offsets[0] && o > 0)) break;
    await sleep(200);
  }
  check(`all three WALs at the same offset (${offsets.join(', ')})`, offsets.every(o => o === offsets[0] && o > 0));
  const restarts = nodes.reduce((a, n) => a + n.restarts, 0);
  console.log(`  nodes restarted after losing the lead (exit 75): ${restarts}`);
} catch (e) {
  console.error(e);
  failures++;
} finally {
  stopping = true;
  for (const n of nodes) n.child?.kill('SIGKILL');
  await sleep(300);
  if (failures && process.env.KEEP) console.log(`  data kept in ${tmp}`);
  else rmSync(tmp, { recursive: true, force: true });
}
console.log(failures ? `✗ ${failures} check(s) failed` : '✓ replica set test passed');
process.exit(failures ? 1 : 0);
