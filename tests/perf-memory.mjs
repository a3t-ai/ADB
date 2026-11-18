#!/usr/bin/env node
/**
 * Agent memory performance on a 3-node ADB replica set (HTTP /query on the leader):
 * writes, reads and deletes per memory type, with reads timed three ways:
 *
 *   lance    everything flushed and compacted into Lance files
 *   merged   newer versions (UPDATE) and new keys still in the MemWAL memtable over
 *            older versions in Lance files: the read merges both (LSM)
 *   after    after FORGETs
 *
 * Each read is also checked: an updated key must come back with its new value, once;
 * a forgotten key must be gone; counts must not double.
 *
 *   node tests/perf-memory.mjs --adb <bin> [--rows 10000] [--c 8] [--runs 200] [--secs 15]
 *     [--embed <model dir>]   (embeddings on, e.g. /var/tmp/a3t-build/models/bge-small-en-v1.5)
 *     [--single]              (one node, no Raft)
 *     [--compact-before-merged] (POST /admin/compact after the writes of step 4)
 */
import { spawn } from 'node:child_process';
import { appendFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import net from 'node:net';

const args = Object.fromEntries(process.argv.slice(2).reduce((a, v, i, all) => (v.startsWith('--') ? [...a, [v.slice(2), all[i + 1]?.startsWith('--') || all[i + 1] === undefined ? 'true' : all[i + 1]]] : a), []));
if (!args.adb) { console.error('usage: perf-memory.mjs --adb <bin> [--rows 10000] [--c 8] [--runs 200] [--secs 15] [--embed <model dir>] [--single]'); process.exit(2); }
const ROWS = Number(args.rows ?? 10000), C = Number(args.c ?? 8), RUNS = Number(args.runs ?? 200), SECS = Number(args.secs ?? 15);
const SINGLE = 'single' in args;
const sleep = ms => new Promise(r => setTimeout(r, ms));
const freePort = () => new Promise(r => { const s = net.createServer().listen(0, '127.0.0.1', () => { const p = s.address().port; s.close(() => r(p)); }); });

const tmp = mkdtempSync(join(tmpdir(), 'adb-perf-mem-'));
const kids = [];
const done = code => { for (const k of kids) { try { k.kill('SIGKILL'); } catch {} } rmSync(tmp, { recursive: true, force: true }); process.exit(code); };
process.on('SIGINT', () => done(130));
const env = { ADB_EMBED: args.embed ? 'on' : 'off', ...(args.embed ? { ADB_EMBED_MODEL_DIR: args.embed } : {}), RUST_LOG: process.env.PERF_RUST_LOG ?? 'warn' };
const nodes = [];
for (let id = 1; id <= (SINGLE ? 1 : 3); id++) nodes.push({ id, http: await freePort(), raft: await freePort(), dir: join(tmp, `adb${id}`) });
const peers = nodes.map(n => `${n.id}=http://127.0.0.1:${n.raft}`).join(',');
for (const n of nodes) {
  const c = spawn(args.adb, ['--data-dir', n.dir, 'serve-http', '--host', '127.0.0.1', '--port', String(n.http), '--flight-port', '0'], {
    env: { ...process.env, ...env, ...(SINGLE ? {} : { ADB_NODE_ID: String(n.id), ADB_PEERS: peers, ADB_RAFT_PORT: String(n.raft) }) },
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  c.stderr.on('data', d => { if (process.env.LOGS) appendFileSync(join(process.env.LOGS, `adb${n.id}.log`), d); });
  kids.push(c);
}
let leader = null;
const ready = async n => { try { return (await fetch(`http://127.0.0.1:${n.http}/${SINGLE ? 'health' : 'ready'}`, { signal: AbortSignal.timeout(300) })).ok; } catch { return false; } };
for (let i = 0; i < 600 && !leader; i++) { for (const n of nodes) if (await ready(n)) { leader = n; break; } await sleep(100); }
if (!leader) { console.error('no ADB leader'); done(1); }
const url = `http://127.0.0.1:${leader.http}/query`;

async function q(query) {
  const r = await (await fetch(url, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ query }) })).json();
  if (!r.success || r.data?.success === false) throw new Error(`${query.slice(0, 80)}: ${JSON.stringify(r.error ?? r.data ?? r).slice(0, 300)}`);
  return r.data?.data; // {type: records | aggregation | stored | affected, …}
}
const rowsOf = d => d?.records ?? [];
const stat = xs => { const s = [...xs].sort((a, b) => a - b); const p = f => s[Math.min(s.length - 1, Math.floor(s.length * f))]; return { n: s.length, p50: p(0.5), p90: p(0.9), p99: p(0.99) }; };
const ms = x => x.toFixed(1).padStart(7);
const results = {};
function line(label, s, rate) {
  console.log(`  ${label.padEnd(44)} p50 ${ms(s.p50)} ms  p90 ${ms(s.p90)}  p99 ${ms(s.p99)}${rate ? `   → ${rate.toFixed(0)}/s` : ''}  (n=${s.n})`);
}
/** `count` queries from `make(i)`, `C` at a time; latency and throughput. */
async function parallel(label, count, make) {
  const xs = []; let next = 0; const t0 = performance.now();
  await Promise.all(Array.from({ length: C }, async () => { while (next < count) { const i = next++; const t = performance.now(); await q(make(i)); xs.push(performance.now() - t); } }));
  const rate = count / ((performance.now() - t0) / 1000);
  const s = stat(xs); line(label, s, rate); results[label] = { ...s, per_s: rate };
  return s;
}
/** `RUNS` sequential runs of one query. */
async function seq(label, query, check) {
  const xs = []; let last;
  for (let i = 0; i < RUNS; i++) { const t = performance.now(); last = await q(typeof query === 'function' ? query(i) : query); xs.push(performance.now() - t); }
  if (check) check(last);
  const s = stat(xs); line(label, s); results[label] = s;
}
/** 8 readers on `queries` round-robin for SECS: read throughput. */
async function readers(label, queries) {
  const xs = []; const end = performance.now() + SECS * 1000; let k = 0;
  await Promise.all(Array.from({ length: C }, async () => { while (performance.now() < end) { const t = performance.now(); await q(queries[k++ % queries.length]); xs.push(performance.now() - t); } }));
  const rate = xs.length / SECS; const s = stat(xs); line(label, s, rate); results[label] = { ...s, per_s: rate };
}
let failures = 0;
const check = (ok, what) => { if (!ok) { failures++; console.log(`  ✗ ${what}`); } };
const valueOf = (r, f) => r?.data?.[f];
const nonEmpty = label => d => check((d?.records?.length ?? 0) > 0 || (d?.values && Object.keys(d.values).length > 0), `${label}: empty answer`);

console.log(`ADB ${SINGLE ? '1 node' : '3 nodes (Raft)'}, embeddings ${env.ADB_EMBED}, ${ROWS} SEMANTIC rows, ${C} clients`);

console.log('\n1. Writes (STORE, new keys)');
await parallel('STORE SEMANTIC', ROWS, i => `STORE INTO SEMANTIC (key = "f${i}", topic = "t${i % 20}", n = ${i % 997}, v = 1, content = "fact number ${i} about topic t${i % 20}")`);
await parallel('STORE EPISODIC', ROWS / 5, i => `STORE INTO EPISODIC (key = "e${i}", kind = "event", n = ${i}, content = "event ${i} happened")`);
await parallel('STORE WORKING', ROWS / 5, i => `STORE INTO WORKING (key = "w${i}", step = 1, priority = ${i % 10})`);

console.log('\n2. Flush and compact (POST /admin/compact)');
{ const t = performance.now(); const r = await (await fetch(`http://127.0.0.1:${leader.http}/admin/compact`, { method: 'POST' })).json(); if (!r.success) throw new Error(`compact: ${JSON.stringify(r)}`); const s = (performance.now() - t) / 1000; console.log(`  compacted ${ROWS} rows in ${s.toFixed(1)} s`); results.compact_s = s; }

const reads = (tag) => [
  [`${tag}: LOOKUP KEY (SEMANTIC)`, i => `RECALL FROM SEMANTIC WHERE id = "f${(i * 37) % ROWS}"`],
  [`${tag}: RECALL WHERE topic LIMIT 10`, i => `RECALL FROM SEMANTIC WHERE topic = "t${i % 20}" LIMIT 10`],
  [`${tag}: RECALL ALL ORDER BY n LIMIT 10`, `RECALL FROM SEMANTIC ORDER BY n DESC LIMIT 10`],
  [`${tag}: AGGREGATE COUNT topic`, i => `RECALL FROM SEMANTIC WHERE topic = "t${i % 20}" AGGREGATE COUNT(*) AS c`],
  [`${tag}: RECALL EPISODIC newest 10`, `RECALL FROM EPISODIC LIMIT 10`],
  [`${tag}: RECALL EPISODIC KEY`, i => `RECALL FROM EPISODIC WHERE id = "e${(i * 7) % (ROWS / 5)}"`],
  [`${tag}: RECALL WORKING KEY`, i => `RECALL FROM WORKING WHERE id = "w${(i * 7) % (ROWS / 5)}"`],
];
const mixQueries = Array.from({ length: 64 }, (_, i) => [
  `RECALL FROM SEMANTIC WHERE id = "f${(i * 97) % ROWS}"`,
  `RECALL FROM SEMANTIC WHERE topic = "t${i % 20}" LIMIT 10`,
  `RECALL FROM EPISODIC WHERE id = "e${(i * 13) % (ROWS / 5)}"`,
  `RECALL FROM WORKING WHERE id = "w${(i * 13) % (ROWS / 5)}"`,
][i % 4]);
const countTopic = async t => Number((await q(`RECALL FROM SEMANTIC WHERE topic = "${t}" AGGREGATE COUNT(*) AS c`))?.values?.c);

console.log('\n3. Reads, all in Lance files');
for (const [label, query] of reads('lance')) await seq(label, query, nonEmpty(label));
await readers('lance: 8 readers, mixed point + filter', mixQueries);
const before = await countTopic('t3');

console.log('\n4. Writes into the memtable: UPDATE keys that are in Lance, STORE new keys');
const UPD = Math.min(2000, ROWS / 5);
await parallel('UPDATE SEMANTIC (existing key, in Lance)', UPD, i => `UPDATE INTO SEMANTIC WHERE key = "f${i}" (v = 2, content = "fact ${i} updated")`);
await parallel('STORE SEMANTIC (new key, memtable)', UPD, i => `STORE INTO SEMANTIC (key = "g${i}", topic = "t${i % 20}", n = ${i % 997}, v = 1, content = "new fact ${i}")`);
await parallel('UPDATE WORKING', UPD, i => `UPDATE INTO WORKING WHERE key = "w${i}" (step = 2)`);
await parallel('UPDATE EPISODIC', UPD, i => `UPDATE INTO EPISODIC WHERE key = "e${i}" (note = "seen")`);

if ('compact-before-merged' in args) {
  const t = performance.now(); const r = await (await fetch(`http://127.0.0.1:${leader.http}/admin/compact`, { method: 'POST' })).json();
  if (!r.success) throw new Error(`compact: ${JSON.stringify(r)}`);
  console.log(`\n   (compacted before the merged reads in ${((performance.now() - t) / 1000).toFixed(1)} s)`);
}
console.log('\n5. Reads, merged: newer versions and new keys in the memtable over Lance files');
for (const [label, query] of reads('merged')) await seq(label, query, nonEmpty(label));
await seq('merged: LOOKUP updated key (memtable over Lance)', i => `RECALL FROM SEMANTIC WHERE id = "f${i % UPD}"`, d => {
  const r = rowsOf(d); check(r.length === 1, `updated key: ${r.length} rows, want 1`); check(String(valueOf(r[0], 'v')) === '2', `updated key: v = ${valueOf(r[0], 'v')}, want 2`);
});
await seq('merged: LOOKUP new key (memtable only)', i => `RECALL FROM SEMANTIC WHERE id = "g${i % UPD}"`, d => check(rowsOf(d).length === 1, 'new key not found'));
await readers('merged: 8 readers, mixed point + filter', mixQueries);
{ const after = await countTopic('t3'); const added = Array.from({ length: UPD }, (_, i) => i).filter(i => i % 20 === 3).length; check(after === before + added, `count t3: ${after}, want ${before} + ${added} (an update must not count twice)`); }

console.log('\n6. Deletes (FORGET: a tombstone; bytes leave at the next compaction)');
await (async () => { const xs = []; for (let i = 0; i < 20; i++) { const t = performance.now(); await q(`FORGET FROM SEMANTIC WHERE key = "f${ROWS - 1 - i}"`); xs.push(performance.now() - t); } const s = stat(xs); line('FORGET SEMANTIC (one key, sequential)', s); results.forget_semantic = s; })();
await parallel('FORGET SEMANTIC (one key each, 8 at a time)', 40, i => `FORGET FROM SEMANTIC WHERE key = "f${ROWS - 100 - i}"`);
await parallel('FORGET WORKING (one key each)', 200, i => `FORGET FROM WORKING WHERE key = "w${i}"`);
await seq('after: LOOKUP forgotten key', i => `RECALL FROM SEMANTIC WHERE id = "f${ROWS - 1 - (i % 20)}"`, d => check(rowsOf(d).length === 0, 'a forgotten key came back'));
for (const [label, query] of reads('after')) await seq(label, query, nonEmpty(label));

console.log(`\n7. Mixed agent load: ${C} agents, 70% reads / 25% STORE / 5% UPDATE, ${SECS * 2} s`);
{
  const xs = { read: [], store: [], update: [] }; const end = performance.now() + SECS * 2000; let k = 0;
  await Promise.all(Array.from({ length: C }, async (_, a) => { while (performance.now() < end) { const i = k++; const r = i % 20; const kind = r < 14 ? 'read' : r < 19 ? 'store' : 'update';
    const query = kind === 'read' ? mixQueries[i % mixQueries.length] : kind === 'store' ? `STORE INTO EPISODIC (key = "m${a}-${i}", kind = "turn", content = "agent ${a} turn ${i}")` : `UPDATE INTO WORKING WHERE key = "w${200 + (i % 1000)}" (step = ${i})`;
    const t = performance.now(); await q(query); xs[kind].push(performance.now() - t); } }));
  const total = xs.read.length + xs.store.length + xs.update.length;
  for (const kind of ['read', 'store', 'update']) line(`mixed: ${kind}`, stat(xs[kind]), xs[kind].length / (SECS * 2));
  console.log(`  ${'mixed: all operations'.padEnd(44)} → ${(total / (SECS * 2)).toFixed(0)} ops/s`);
  results.mixed_ops_s = total / (SECS * 2);
}

console.log(failures ? `\n✗ ${failures} check(s) failed` : '\n✓ every read check passed');
console.log('RESULTS ' + JSON.stringify(results));
done(failures ? 1 : 0);
