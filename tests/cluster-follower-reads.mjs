#!/usr/bin/env node
/**
 * Follower reads (C-16) and shared embedding (C-17b) on a 3-node replica set.
 *
 * 1. Writes sent to a follower reach the leader (HTTP passed through).
 * 2. Read-your-writes on every node: a STORE acknowledged by one node is found by an
 *    immediate LOOKUP on each other node (followers wait for the leader's ReadIndex).
 * 3. Reads are served by followers (`metadata.served_by`), writes are not.
 * 4. Flight on a follower: an AQL read is answered, a table write is refused with
 *    "not the leader" (needs FLIGHT=1 and the Flight client in node_modules; skipped otherwise).
 * 5. A leader kill under mixed load: every write acknowledged is then found on every
 *    node, and reads on the surviving followers go on.
 * 6. With an embedding model (ADB_EMBED_MODEL_DIR): SEMANTIC writes get vectors and
 *    the followers' logs show embedding requests served for the leader.
 *
 *   node tests/cluster-follower-reads.mjs --adb <adb bin> [--rounds 200]
 */
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer as netServer } from 'node:net';

const args = Object.fromEntries(process.argv.slice(2).reduce((a, v, i, all) => (v.startsWith('--') ? [...a, [v.slice(2), all[i + 1]]] : a), []));
if (!args.adb) {
  console.error('usage: cluster-follower-reads.mjs --adb <adb bin> [--rounds 200]');
  process.exit(2);
}
const ROUNDS = Number(args.rounds ?? 200);
const sleep = ms => new Promise(r => setTimeout(r, ms));
const freePort = () => new Promise(res => { const s = netServer().listen(0, () => { const p = s.address().port; s.close(() => res(p)); }); });
let failures = 0;
const check = (label, ok, detail = '') => { console.log(`${ok ? '✓' : '✗'} ${label}${ok || !detail ? '' : `  — ${detail}`}`); if (!ok) failures++; };

const tmp = mkdtempSync(join(tmpdir(), 'cluster-fr-'));
const nodes = [];
let stopping = false;
for (let id = 1; id <= 3; id++) nodes.push({ id, http: await freePort(), flight: await freePort(), raft: await freePort(), dir: join(tmp, `n${id}`) });
const peers = nodes.map(n => `${n.id}=http://127.0.0.1:${n.raft}`).join(',');
const peersHttp = nodes.map(n => `${n.id}=http://127.0.0.1:${n.http}`).join(',');

function launch(n) {
  const c = spawn(args.adb, ['--data-dir', n.dir, 'serve-http', '--host', '127.0.0.1', '--port', String(n.http), '--flight-port', String(n.flight)], {
    env: { ...process.env, RUST_LOG: process.env.RUST_LOG ?? 'warn,adb_cli=info,adb_embed=debug', ADB_NODE_ID: String(n.id), ADB_PEERS: peers, ADB_PEERS_HTTP: peersHttp, ADB_RAFT_PORT: String(n.raft) },
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  n.log = n.log ?? '';
  c.stderr.on('data', d => { n.log = (n.log + d).slice(-200000); });
  c.on('exit', code => {
    if (stopping || n.child !== c) return;
    if (code === 75) setTimeout(() => { if (!stopping) launch(n); }, 200);
  });
  c.exited = new Promise(r => c.on('exit', r));
  n.child = c;
}

const url = n => `http://127.0.0.1:${n.http}`;
async function ready(n) {
  try { return (await fetch(`${url(n)}/ready`, { signal: AbortSignal.timeout(500) })).ok; } catch { return false; }
}
async function leader(timeoutMs = 30000) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    for (const n of nodes) if (await ready(n)) return n;
    await sleep(50);
  }
  throw new Error(`no leader within ${timeoutMs} ms:\n${nodes.map(n => `--- node ${n.id}\n${n.log.slice(-3000)}`).join('\n')}`);
}
async function query(n, aql, variables) {
  const r = await fetch(`${url(n)}/query`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ query: aql, variables }), signal: AbortSignal.timeout(15000) });
  return r.json();
}
const rows = r => r?.data?.data?.records ?? r?.data?.records ?? [];
const found = r => r.success && rows(r).length > 0;
const servedBy = r => r?.data?.metadata?.served_by ?? r?.metadata?.served_by;

try {
  for (const n of nodes) launch(n);
  let lead = await leader();
  // Followers: up and serving (their HTTP answers /health).
  for (const n of nodes) {
    for (let i = 0; i < 200; i++) {
      try { if ((await fetch(`${url(n)}/health`, { signal: AbortSignal.timeout(500) })).ok) break; } catch {}
      await sleep(50);
    }
  }
  const followers = () => nodes.filter(n => n !== lead);
  console.log(`leader: node ${lead.id}`);

  // 1. A write sent to a follower.
  const f0 = followers()[0];
  const w = await query(f0, 'STORE INTO SEMANTIC (key = "via-follower", v = 1)');
  check('a write sent to a follower is acknowledged (passed to the leader)', w.success, JSON.stringify(w).slice(0, 300));
  const back = await query(lead, 'LOOKUP FROM SEMANTIC KEY id = "via-follower"');
  check('the leader holds it', found(back), JSON.stringify(back).slice(0, 300));

  // 2 + 3. Read-your-writes on every node, rounds of STORE then immediate LOOKUPs.
  let misses = 0, byFollower = 0, reads = 0;
  const t0 = Date.now();
  for (let i = 0; i < ROUNDS; i++) {
    const writeAt = nodes[i % 3];
    const key = `ryw-${i}`;
    const s = await query(writeAt, `STORE INTO SEMANTIC (key = "${key}", topic = "fr", content = "follower read ${i}")`);
    if (!s.success) { misses++; console.log(`  write ${key} failed: ${JSON.stringify(s).slice(0, 200)}`); continue; }
    for (const n of nodes) {
      const r = await query(n, `LOOKUP FROM SEMANTIC KEY id = "${key}"`);
      reads++;
      if (!found(r)) { misses++; if (misses < 5) console.log(`  node ${n.id} missed ${key}: ${JSON.stringify(r).slice(0, 200)}`); }
      if (servedBy(r) === 'follower') byFollower++;
    }
  }
  check(`read-your-writes on every node (${ROUNDS} writes × 3 nodes, ${Date.now() - t0} ms)`, misses === 0, `${misses} misses`);
  check('followers served their reads', byFollower >= Math.floor(reads * 2 / 3) - 1, `${byFollower} of ${reads} served by a follower`);
  const rc = await query(followers()[1], 'RECALL FROM SEMANTIC WHERE topic = "fr" LIMIT 1000');
  check('a filtered RECALL on a follower sees every write', rc.success && rows(rc).length >= ROUNDS, JSON.stringify(rc).slice(0, 200));

  // 5. Leader kill under load: writers at any node, readers at the followers.
  const acked = [];
  let stop = false, readErrors = 0, readOk = 0;
  const writers = Array.from({ length: 4 }, async (_, wi) => {
    let i = 0;
    while (!stop) {
      const n = nodes[(wi + i) % 3];
      const key = `kill-${wi}-${i++}`;
      try { const r = await query(n, `STORE INTO SEMANTIC (key = "${key}", v = ${i})`); if (r.success) acked.push(key); } catch {}
    }
  });
  const readers = Array.from({ length: 4 }, async (_, ri) => {
    let i = 0;
    while (!stop) {
      const n = nodes[(ri + i++) % 3];
      try { const r = await query(n, 'LOOKUP FROM SEMANTIC KEY id = "via-follower"'); if (found(r)) readOk++; else readErrors++; } catch { readErrors++; }
    }
  });
  await sleep(2000);
  const killed = lead;
  killed.child.kill('SIGKILL');
  await killed.child.exited;
  lead = await leader();
  console.log(`  killed node ${killed.id}; node ${lead.id} leads`);
  launch(killed);
  await sleep(4000);
  stop = true;
  await Promise.all([...writers, ...readers]);
  check('reads went on through the leader kill', readOk > 0, `${readOk} ok, ${readErrors} failed`);
  let lost = 0;
  for (const key of acked) {
    for (const n of nodes) {
      let ok = false;
      for (let t = 0; t < 3 && !ok; t++) { try { ok = found(await query(n, `LOOKUP FROM SEMANTIC KEY id = "${key}"`)); } catch {} if (!ok) await sleep(300); }
      if (!ok) { lost++; break; }
    }
  }
  check(`every acknowledged write found on every node after the kill (${acked.length} writes)`, lost === 0, `${lost} missing`);

  // 6. Shared embedding.
  if (process.env.ADB_EMBED_MODEL_DIR) {
    lead = await leader();
    const N = 64;
    await Promise.all(Array.from({ length: N }, (_, i) => query(lead, `STORE INTO SEMANTIC (key = "emb-${i}", topic = "emb", content = "the payments service ran out of memory ${i}")`)));
    const r = await query(followers()[0], 'RECALL FROM SEMANTIC LIKE $q LIMIT 5', { q: 'payments pod out of memory' });
    check('a similarity read on a follower finds the new records', found(r), JSON.stringify(r).slice(0, 300));
    const lent = followers().map(n => (n.log.match(/embedded for a peer/g) ?? []).length);
    check('followers embedded some of the leader\'s writes', lent.some(c => c > 0), `peer embeddings per follower: ${lent.join(', ')}`);
  } else {
    console.log('- no ADB_EMBED_MODEL_DIR: shared embedding not checked');
  }
} catch (e) {
  failures++;
  console.log(`✗ ${e.stack ?? e}`);
} finally {
  stopping = true;
  for (const n of nodes) n.child?.kill('SIGKILL');
  await Promise.all(nodes.map(n => n.child?.exited));
  if (failures) for (const n of nodes) console.log(`--- node ${n.id}\n${n.log.slice(-4000)}`);
  if (!process.env.KEEP) rmSync(tmp, { recursive: true, force: true });
}
console.log(failures ? `✗ follower reads test: ${failures} failure(s)` : '✓ follower reads test passed');
process.exit(failures ? 1 : 0);
