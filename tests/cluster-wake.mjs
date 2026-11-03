#!/usr/bin/env node
/**
 * Waking a hibernated replica set: how long until it serves?
 *
 * Starts a 3-node replica set, loads it (--records STOREs), then repeatedly stops
 * all three (SIGTERM, as a hibernating pod is stopped) and wakes them, timing:
 *   up      every node answers /cluster/status (process started, Raft endpoint up)
 *   leader  a node reports itself Leader
 *   ready   the leader answers /ready (engine open: the Service would route to it)
 *   write   the first acknowledged STORE
 *   read    a RECALL sees every record written before the stop
 * Wake patterns: all three at once; one first and the other two --stagger-ms later
 * (what a single woken node can do on its own: nothing, it has no majority).
 *
 *   node tests/cluster-wake.mjs --adb <adb bin> [--records 5000] [--cycles 3] [--stagger-ms 3000]
 */
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:net';

const args = Object.fromEntries(process.argv.slice(2).reduce((a, v, i, all) => (v.startsWith('--') ? [...a, [v.slice(2), all[i + 1]]] : a), []));
if (!args.adb) { console.error('usage: cluster-wake.mjs --adb <adb bin> [--records 5000] [--cycles 3] [--stagger-ms 3000]'); process.exit(2); }
const RECORDS = Number(args.records ?? 5000);
const CYCLES = Number(args.cycles ?? 3);
const STAGGER = Number(args['stagger-ms'] ?? 3000);
const sleep = ms => new Promise(r => setTimeout(r, ms));
const freePort = () => new Promise(res => { const s = createServer().listen(0, () => { const p = s.address().port; s.close(() => res(p)); }); });
let failures = 0;
const check = (label, ok, detail = '') => { console.log(`${ok ? '✓' : '✗'} ${label}${ok || !detail ? '' : `  — ${detail}`}`); if (!ok) failures++; };

const tmp = mkdtempSync(join(tmpdir(), 'cluster-wake-'));
const nodes = [];
for (let id = 1; id <= 3; id++) nodes.push({ id, http: await freePort(), flight: await freePort(), raft: await freePort(), dir: join(tmp, `n${id}`) });
const peers = nodes.map(n => `${n.id}=http://127.0.0.1:${n.raft}`).join(',');
function launch(n) {
  const c = spawn(args.adb, ['--data-dir', n.dir, 'serve-http', '--host', '127.0.0.1', '--port', String(n.http), '--flight-port', String(n.flight)], {
    env: { ...process.env, RUST_LOG: process.env.RUST_LOG ?? 'warn', ADB_NODE_ID: String(n.id), ADB_PEERS: peers, ADB_RAFT_PORT: String(n.raft) },
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  n.log = '';
  c.stderr.on('data', d => { n.log = (n.log + d).slice(-20000); });
  // A leader that loses the lead exits 75: start it again (as Kubernetes would).
  c.on('exit', code => { if (code === 75 && n.child === c && !n.stopping) setTimeout(() => launch(n), 200); });
  c.exited = new Promise(r => c.on('exit', r));
  n.child = c;
}
const get = async (url, ms = 300) => { try { const r = await fetch(url, { signal: AbortSignal.timeout(ms) }); return r.ok ? await r.json().catch(() => ({})) : null; } catch { return null; } };
const status = n => get(`http://127.0.0.1:${n.raft}/cluster/status`);
const ready = async n => { try { return (await fetch(`http://127.0.0.1:${n.http}/ready`, { signal: AbortSignal.timeout(300) })).ok; } catch { return false; } };
async function query(n, aql, ms = 5000) {
  const r = await fetch(`http://127.0.0.1:${n.http}/query`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ query: aql }), signal: AbortSignal.timeout(ms) });
  return r.json();
}
async function readyLeader(ms = 60000) {
  const t0 = Date.now();
  while (Date.now() - t0 < ms) { for (const n of nodes) if (await ready(n)) return n; await sleep(20); }
  throw new Error(`no ready leader within ${ms} ms:\n${nodes.map(n => `--- node ${n.id}\n${n.log.slice(-2000)}`).join('\n')}`);
}
async function stopAll() {
  for (const n of nodes) n.stopping = true;
  for (const n of nodes) n.child?.kill('SIGTERM');
  await Promise.all(nodes.map(n => n.child?.exited));
  for (const n of nodes) n.stopping = false;
}

// Wake: start nodes (all at once, or one then the rest), time each stage.
async function wake(staggered) {
  const t0 = Date.now();
  const at = {};
  const mark = (k, cond) => { if (at[k] === undefined && cond) at[k] = Date.now() - t0; };
  launch(nodes[0]);
  if (!staggered) { launch(nodes[1]); launch(nodes[2]); }
  let alone = null;
  let rest = staggered ? setTimeout(() => { launch(nodes[1]); launch(nodes[2]); }, STAGGER) : null;
  while (Date.now() - t0 < 90000) {
    const sts = await Promise.all(nodes.map(status));
    const started = staggered && Date.now() - t0 < STAGGER ? 1 : 3;
    mark('up', sts.slice(0, started).every(Boolean) && started === 3);
    mark('leader', sts.some(s => s?.state === 'Leader'));
    const r = await Promise.all(nodes.map(ready));
    // A lone node may report Leader (openraft restores its last vote) but must not serve.
    if (staggered && Date.now() - t0 < STAGGER - 100) alone = { serves: alone?.serves || r[0], state: sts[0]?.state ?? 'down' };
    mark('ready', r.some(Boolean));
    if (at.ready !== undefined) break;
    await sleep(20);
  }
  clearTimeout(rest);
  const L = await readyLeader(5000);
  // First acknowledged write (retry while the engine settles).
  for (let i = 0; i < 500 && at.write === undefined; i++) {
    try { if ((await query(L, `STORE INTO EPISODIC (bid_id = "wake-${t0}", kind = "wake")`)).success) at.write = Date.now() - t0; } catch {}
    if (at.write === undefined) await sleep(20);
  }
  const r = await query(L, 'RECALL FROM EPISODIC WHERE kind = "load" LIMIT 1000000 RETURN bid_id', 30000);
  at.read = Date.now() - t0;
  const seen = new Set((r.data?.data?.records ?? []).map(x => x.data?.bid_id)).size;
  return { at, seen, alone, leader: L.id };
}

try {
  for (const n of nodes) launch(n);
  let L = await readyLeader();
  const tl = Date.now();
  const load = [];
  for (let w = 0; w < 8; w++) load.push((async () => { for (let i = w; i < RECORDS; i += 8) { const r = await query(L, `STORE INTO EPISODIC (bid_id = "load-${i}", kind = "load", n = ${i})`); if (!r.success) throw new Error(JSON.stringify(r).slice(0, 300)); } })());
  await Promise.all(load);
  const offs = (await Promise.all(nodes.map(status))).map(s => s?.wal_offset);
  console.log(`  loaded ${RECORDS} records in ${Date.now() - tl} ms (WALs ${offs.join(', ')})`);

  const rows = [];
  for (let c = 0; c < CYCLES; c++) {
    for (const staggered of [false, true]) {
      await stopAll();
      await sleep(500);
      const w = await wake(staggered);
      rows.push({ pattern: staggered ? `one first, +${STAGGER} ms` : 'all at once', ...w });
      console.log(`  wake ${staggered ? `one first, others +${STAGGER} ms` : 'all three at once'}: up ${w.at.up ?? '-'} · leader ${w.at.leader} · ready ${w.at.ready} · first write ${w.at.write} · full read ${w.at.read} ms (node ${w.leader} leads; ${w.seen}/${RECORDS} records)${w.alone ? `; the lone node reported ${w.alone.state}, served: ${w.alone.serves}` : ''}`);
      check(`all ${RECORDS} records readable after the wake`, w.seen === RECORDS, `${w.seen}`);
    }
  }
  const med = (k, p) => { const v = rows.filter(r => r.pattern === p).map(r => r.at[k]).sort((a, b) => a - b); return v[Math.floor(v.length / 2)]; };
  for (const p of [...new Set(rows.map(r => r.pattern))]) console.log(`  median (${p}): leader ${med('leader', p)} · ready ${med('ready', p)} · first write ${med('write', p)} ms`);
  check('a lone woken node never serves (no majority)', rows.filter(r => r.alone).every(r => !r.alone.serves));
} catch (e) {
  console.error(e);
  failures++;
} finally {
  for (const n of nodes) { n.stopping = true; n.child?.kill('SIGKILL'); }
  await sleep(300);
  rmSync(tmp, { recursive: true, force: true });
}
console.log(failures ? `✗ ${failures} check(s) failed` : '✓ wake test passed');
process.exit(failures ? 1 : 0);
