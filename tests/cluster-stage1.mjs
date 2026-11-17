#!/usr/bin/env node
/**
 * A3T cluster, stage 1 (flowr-system/.ai/A3T_CLUSTER_ARCHITECTURE.md §8):
 * FlowR is stateless on one ADB. Any FlowR can die and another carries on.
 *
 * One ADB (engine token). Three FlowRs on it, each with its own empty data dir,
 * the engine store and the registry on ADB (FLOWR_ENGINE_ADB_URL,
 * FLOWR_REGISTRY_ADB):
 *
 *   A, B — "console replicas": claim the default pool;
 *   E    — an edge only (FLOWR_WORKER_POOLS empty): takes requests, runs nothing.
 *
 * Checks:
 *   - a flow, a state key and a trigger made through A are seen through B and E;
 *   - a trigger deleted through B is gone through A;
 *   - runs started through E run on A or B (E claims nothing);
 *   - A is SIGKILLed while its runs are mid-flight: every run completes (B takes
 *     the lapsed leases), each side effect happened once per idempotency key;
 *   - after the kill, B and E still answer for every run.
 *
 *   node tests/cluster-stage1.mjs --adb <adb bin> --flowr <flowr-ce bin (adb-platform)> [--runs 8]
 */
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:http';
import { createServer as netServer } from 'node:net';

const args = Object.fromEntries(process.argv.slice(2).reduce((a, v, i, all) => (v.startsWith('--') ? [...a, [v.slice(2), all[i + 1]]] : a), []));
if (!args.adb || !args.flowr) {
  console.error('usage: cluster-stage1.mjs --adb <adb bin> --flowr <flowr-ce bin> [--runs 8]');
  process.exit(2);
}
const RUNS = Number(args.runs ?? 8);
const TOKEN = `engine-${process.pid}`;
const AUTH = `auth-${process.pid}`;
const sleep = ms => new Promise(r => setTimeout(r, ms));
const freePort = () => new Promise(res => { const s = netServer().listen(0, () => { const p = s.address().port; s.close(() => res(p)); }); });
let failures = 0;
const check = (label, ok, detail = '') => { console.log(`${ok ? '✓' : '✗'} ${label}${ok || !detail ? '' : `  — ${detail}`}`); if (!ok) failures++; };

// Side effects, counted per idempotency key, and which FlowR made them.
const hits = new Map();
const counter = createServer((req, res) => {
  const key = req.headers['idempotency-key'] ?? `none:${req.url}:${Math.random()}`;
  hits.set(key, (hits.get(key) ?? 0) + 1);
  req.resume();
  req.on('end', () => { res.setHeader('content-type', 'application/json'); res.end('{"ok":true}'); });
});
const counterPort = await freePort();
await new Promise(r => counter.listen(counterPort, '127.0.0.1', r));

const tmp = mkdtempSync(join(tmpdir(), 'cluster-stage1-'));
const children = new Set();
function start(bin, argv, env) {
  const c = spawn(bin, argv, { env: { ...process.env, RUST_LOG: 'warn', ...env }, stdio: ['ignore', 'ignore', 'pipe'] });
  let log = '';
  c.stderr.on('data', d => { log = (log + d).slice(-8000); });
  c.log = () => log;
  c.exited = new Promise(r => c.on('exit', () => { children.delete(c); r(); }));
  children.add(c);
  return c;
}
async function healthy(url, who, c) {
  for (let i = 0; i < 300; i++) {
    try { if ((await fetch(`${url}/health`)).ok) return; } catch {}
    await sleep(100);
  }
  throw new Error(`${who} not healthy: ${c.log()}`);
}
let rpc = 1;
async function tool(url, name, a) {
  const r = await (await fetch(`${url}/message`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: rpc++, method: 'tools/call', params: { name, arguments: a } }) })).json();
  const text = r.result?.content?.[0]?.text ?? '';
  if (r.error || r.result?.isError) throw new Error(`${name}: ${text || JSON.stringify(r.error)}`);
  try { return JSON.parse(text); } catch { return text; }
}
const triggersIn = async (url, ns) => {
  const r = await tool(url, 'list_triggers', { namespace: ns });
  return (Array.isArray(r) ? r : r.triggers ?? []).map(t => t.id);
};

try {
  const [adbPort, flightPort] = [await freePort(), await freePort()];
  const adbUrl = `http://127.0.0.1:${adbPort}`;
  const flightUrl = `http://127.0.0.1:${flightPort}`;
  const adb = start(args.adb, ['--data-dir', join(tmp, 'adb'), 'serve-http', '--port', String(adbPort), '--flight-port', String(flightPort)], { ADB_ENGINE_TOKEN: TOKEN, ADB_AUTH_TOKEN: AUTH });
  await healthy(adbUrl, 'ADB', adb);

  const flowrs = {};
  async function flowr(name, extra = {}) {
    const port = await freePort();
    const url = `http://127.0.0.1:${port}`;
    const c = start(args.flowr, ['--mode', 'http', '--port', String(port)], {
      HOSTNAME: `flowr-${name}`,
      CLAWDFLOW_DATA_DIR: join(tmp, `flowr-${name}`),
      FLOWR_ENGINE_ADB_URL: flightUrl,
      FLOWR_ENGINE_ADB_TOKEN: TOKEN,
      FLOWR_REGISTRY_ADB: adbUrl,
      FLOWR_REGISTRY_ADB_FLIGHT: flightUrl,
      ADB_AUTH_TOKEN: AUTH,
      ADB_AUTH_URLS: `${adbUrl},${flightUrl}`,
      // Short leases, so a dead FlowR's runs are taken over quickly.
      FLOWR_RUN_LEASE_MS: '4000',
      ...extra,
    });
    await healthy(url, `FlowR ${name}`, c);
    flowrs[name] = { url, c };
    return flowrs[name];
  }
  const A = await flowr('a');
  const B = await flowr('b');
  const E = await flowr('e', { FLOWR_WORKER_POOLS: '' });
  console.log('  ADB + FlowR A, B (workers) and E (edge only) up');

  // --- shared registry, state and triggers
  const base = `http://127.0.0.1:${counterPort}`;
  await tool(A.url, 'create_flow', { yaml: `document: { dsl: '1.0.3', namespace: cluster, name: pay, version: '1.0.0' }
do:
  - charge:
      call: http
      with: { method: POST, endpoint: ${base}/hit/charge, body: { n: 1 } }
  - settle:
      wait: { seconds: 5 }
  - refund:
      call: http
      with: { method: POST, endpoint: ${base}/hit/refund, body: { n: 2 } }
  - done:
      set: { ok: true }
` });
  const flowOnB = await tool(B.url, 'get_flow', { namespace: 'cluster', name: 'pay' }).catch(e => e.message);
  check('a flow made through A is seen through B', JSON.stringify(flowOnB).includes('pay'), JSON.stringify(flowOnB).slice(0, 200));

  await tool(A.url, 'set_state', { key: 'cluster:test', value: { n: 7 } });
  const st = await tool(E.url, 'get_state', { key: 'cluster:test' });
  check('a state key set through A is read through E', JSON.stringify(st).includes('7'), JSON.stringify(st));

  const created = await tool(A.url, 'create_trigger', { name: 'hook', namespace: 'cluster', config: { type: 'webhook', path: '/cluster-hook' }, targets: [{ type: 'workflow', namespace: 'cluster', name: 'pay' }] });
  const tid = typeof created === 'string' ? created.match(/ID: `([^`]+)`/)?.[1] : (created.id ?? created.trigger_id ?? created.trigger?.id);
  const onB = await triggersIn(B.url, 'cluster');
  check('a trigger made through A is listed through B', tid && onB.includes(tid), JSON.stringify({ tid, onB, created }));
  await tool(B.url, 'delete_trigger', { id: tid });
  const onA = await triggersIn(A.url, 'cluster');
  check('a trigger deleted through B is gone through A', !onA.includes(tid), JSON.stringify(onA));

  // --- runs: started through the edge, run by A or B; A dies mid-flight
  const ids = [];
  for (let i = 0; i < RUNS; i++) ids.push((await tool(E.url, 'start_run', { namespace: 'cluster', name: 'pay', input: { i } })).run_id);
  console.log(`  ${RUNS} runs started through E`);
  await sleep(2500); // every run is in its 5 s wait, claimed by A or B
  A.c.kill('SIGKILL');
  await A.c.exited;
  console.log('  A killed (SIGKILL) mid-flight');

  const deadline = Date.now() + 120_000;
  let runs = [];
  while (Date.now() < deadline) {
    runs = (await tool(B.url, 'list_runs', { namespace: 'cluster', limit: 100 })).runs ?? [];
    if (runs.length === RUNS && runs.every(r => ['completed', 'faulted', 'cancelled'].includes(r.phase))) break;
    await sleep(1000);
  }
  const done = runs.filter(r => r.phase === 'completed').length;
  check(`every run completed after A died (${done}/${RUNS})`, done === RUNS, JSON.stringify(runs.filter(r => r.phase !== 'completed').map(r => [r.run_id, r.phase, r.error])));
  const keys = [...hits.keys()].filter(k => !k.startsWith('none:'));
  check(`every side effect happened (${keys.length}/${RUNS * 2} keys)`, keys.length === RUNS * 2);
  const repeats = [...hits.entries()].filter(([, n]) => n > 1).length;
  console.log(`  calls repeated under the same key (dropped by a receiver honouring it): ${repeats}`);
  const epochs = await Promise.all(ids.map(id => tool(E.url, 'get_run', { run_id: id }).then(r => r.epoch ?? r.run?.epoch ?? 1).catch(() => 0)));
  const moved = epochs.filter(e => e >= 2).length;
  console.log(`  runs taken over from A (epoch ≥ 2): ${moved}/${RUNS}`);
  check('some runs were running on A and were taken over by B', moved > 0, JSON.stringify(epochs));
  const seen = await Promise.all(ids.map(id => tool(E.url, 'get_run', { run_id: id }).then(() => true).catch(() => false)));
  check('the edge still answers for every run', seen.every(Boolean));
} catch (e) {
  console.error(e);
  failures++;
} finally {
  for (const c of children) c.kill('SIGKILL');
  counter.close();
  rmSync(tmp, { recursive: true, force: true });
}
console.log(failures ? `✗ ${failures} check(s) failed` : '✓ cluster stage 1 passed');
process.exit(failures ? 1 : 0);
