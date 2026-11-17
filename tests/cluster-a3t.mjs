#!/usr/bin/env node
/**
 * A3T as one cluster (flowr-system .ai/A3T_CLUSTER_ARCHITECTURE.md, stages 1 + 3):
 * stateless FlowRs on a 3-node ADB replica set.
 *
 * - ADB: 3 nodes (ADB_NODE_ID / ADB_PEERS), engine and platform tokens, under a
 *   supervisor that restarts a node that exits (as Kubernetes does).
 * - A TCP proxy stands in for the Kubernetes Service `adb`: it forwards HTTP and
 *   Flight connections to whichever node answers /ready (the leader).
 * - FlowR A and B (workers) and E (edge only, FLOWR_WORKER_POOLS empty), engine
 *   store and registry on ADB through the proxy.
 *
 * Runs are started through E; then the ADB leader is SIGKILLed mid-flight, and
 * later FlowR A too. Every run must complete, each side effect once per
 * idempotency key, the flow and a state key still readable, and the ADB nodes'
 * WALs equal at the end.
 *
 *   node tests/cluster-a3t.mjs --adb <adb bin> --flowr <flowr-ce bin (adb-platform)> [--runs 8]
 */
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:http';
import net from 'node:net';

const args = Object.fromEntries(process.argv.slice(2).reduce((a, v, i, all) => (v.startsWith('--') ? [...a, [v.slice(2), all[i + 1]]] : a), []));
if (!args.adb || !args.flowr) {
  console.error('usage: cluster-a3t.mjs --adb <adb bin> --flowr <flowr-ce bin> [--runs 8]');
  process.exit(2);
}
const RUNS = Number(args.runs ?? 8);
const TOKEN = `engine-${process.pid}`;
const AUTH = `auth-${process.pid}`;
const sleep = ms => new Promise(r => setTimeout(r, ms));
const freePort = () => new Promise(res => { const s = net.createServer().listen(0, () => { const p = s.address().port; s.close(() => res(p)); }); });
let failures = 0;
const check = (label, ok, detail = '') => { console.log(`${ok ? '✓' : '✗'} ${label}${ok || !detail ? '' : `  — ${detail}`}`); if (!ok) failures++; };

// Side effects, counted per idempotency key.
const hits = new Map();
const counter = createServer((req, res) => {
  const key = req.headers['idempotency-key'] ?? `none:${req.url}:${Math.random()}`;
  hits.set(key, (hits.get(key) ?? 0) + 1);
  req.resume();
  req.on('end', () => { res.setHeader('content-type', 'application/json'); res.end('{"ok":true}'); });
});
const counterPort = await freePort();
await new Promise(r => counter.listen(counterPort, '127.0.0.1', r));

const tmp = mkdtempSync(join(tmpdir(), 'cluster-a3t-'));
const children = new Set();
let stopping = false;
function start(bin, argv, env, onExit) {
  const c = spawn(bin, argv, { env: { ...process.env, RUST_LOG: process.env.RUST_LOG ?? 'warn', ...env }, stdio: ['ignore', 'ignore', 'pipe'] });
  let log = '';
  c.stderr.on('data', d => { log = (log + d).slice(-12000); });
  c.log = () => log;
  c.exited = new Promise(r => c.on('exit', (code, signal) => { children.delete(c); if (onExit) onExit(code, signal); r(); }));
  children.add(c);
  return c;
}

// ---- ADB replica set
const adbs = [];
for (let id = 1; id <= 3; id++) adbs.push({ id, http: await freePort(), flight: await freePort(), raft: await freePort(), dir: join(tmp, `adb${id}`) });
const peers = adbs.map(n => `${n.id}=http://127.0.0.1:${n.raft}`).join(',');
function launchAdb(n) {
  n.child = start(args.adb, ['--data-dir', n.dir, 'serve-http', '--host', '127.0.0.1', '--port', String(n.http), '--flight-port', String(n.flight)], {
    ADB_NODE_ID: String(n.id), ADB_PEERS: peers, ADB_RAFT_PORT: String(n.raft), ADB_ENGINE_TOKEN: TOKEN, ADB_AUTH_TOKEN: AUTH,
  }, code => { if (!stopping && code === 75) setTimeout(() => !stopping && launchAdb(n), 200); });
}
async function isReady(n) {
  try { return (await fetch(`http://127.0.0.1:${n.http}/ready`, { signal: AbortSignal.timeout(400) })).ok; } catch { return false; }
}
let leader = null;
(async function watchLeader() {
  while (!stopping) {
    let found = null;
    for (const n of adbs) if (await isReady(n)) { found = n; break; }
    leader = found;
    await sleep(100);
  }
})();
async function waitLeader(ms = 20000) {
  const t0 = Date.now();
  while (Date.now() - t0 < ms) { if (leader) return leader; await sleep(50); }
  throw new Error(`no ADB leader:\n${adbs.map(n => `--- adb ${n.id}\n${n.child?.log().slice(-2000)}`).join('\n')}`);
}

// ---- the "Service": TCP proxy to the leader, one per port kind
function proxy(kind) {
  const server = net.createServer(client => {
    const target = leader;
    if (!target) { client.destroy(); return; }
    const up = net.connect(target[kind], '127.0.0.1');
    const close = () => { client.destroy(); up.destroy(); };
    client.on('error', close); up.on('error', close);
    client.on('close', close); up.on('close', close);
    client.pipe(up).pipe(client);
  });
  return new Promise(r => server.listen(0, '127.0.0.1', () => r({ server, port: server.address().port })));
}

let rpc = 1;
async function tool(url, name, a) {
  const r = await (await fetch(`${url}/message`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: rpc++, method: 'tools/call', params: { name, arguments: a } }) })).json();
  const text = r.result?.content?.[0]?.text ?? '';
  if (r.error || r.result?.isError) throw new Error(`${name}: ${text || JSON.stringify(r.error)}`);
  try { return JSON.parse(text); } catch { return text; }
}
async function healthy(url, who, c) {
  for (let i = 0; i < 400; i++) {
    try { if ((await fetch(`${url}/health`)).ok) return; } catch {}
    await sleep(100);
  }
  throw new Error(`${who} not healthy: ${c.log()}`);
}

try {
  for (const n of adbs) launchAdb(n);
  const first = await waitLeader();
  console.log(`  ADB leader: node ${first.id}`);
  const httpProxy = await proxy('http');
  const flightProxy = await proxy('flight');
  const adbUrl = `http://127.0.0.1:${httpProxy.port}`;
  const flightUrl = `http://127.0.0.1:${flightProxy.port}`;

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
  console.log('  FlowR A, B (workers) and E (edge) up on the replica set');

  const base = `http://127.0.0.1:${counterPort}`;
  await tool(A.url, 'create_flow', { yaml: `document: { dsl: '1.0.3', namespace: cluster, name: pay, version: '1.0.0' }
do:
  - charge:
      call: http
      with: { method: POST, endpoint: ${base}/hit/charge, body: { n: 1 } }
  - settle:
      wait: { seconds: 6 }
  - refund:
      call: http
      with: { method: POST, endpoint: ${base}/hit/refund, body: { n: 2 } }
  - done:
      set: { ok: true }
` });
  await tool(A.url, 'set_state', { key: 'cluster:test', value: { n: 7 } });

  const ids = [];
  for (let i = 0; i < RUNS; i++) ids.push((await tool(E.url, 'start_run', { namespace: 'cluster', name: 'pay', input: { i } })).run_id);
  console.log(`  ${RUNS} runs started through E`);

  await sleep(2000);
  const old = leader;
  const t0 = Date.now();
  old.child.kill('SIGKILL');
  leader = null;
  const next = await waitLeader();
  console.log(`  ADB leader node ${old.id} killed; node ${next.id} leads after ${Date.now() - t0} ms`);
  setTimeout(() => !stopping && launchAdb(old), 500);

  await sleep(1500);
  A.c.kill('SIGKILL');
  console.log('  FlowR A killed');

  const deadline = Date.now() + 150_000;
  let runs = [];
  while (Date.now() < deadline) {
    try { runs = (await tool(B.url, 'list_runs', { namespace: 'cluster', limit: 100 })).runs ?? []; } catch {}
    if (runs.length === RUNS && runs.every(r => ['completed', 'faulted', 'cancelled'].includes(r.phase))) break;
    await sleep(1000);
  }
  const done = runs.filter(r => r.phase === 'completed').length;
  check(`every run completed through an ADB failover and a FlowR kill (${done}/${RUNS})`, done === RUNS, JSON.stringify(runs.filter(r => r.phase !== 'completed').map(r => [r.run_id, r.phase, r.error])));
  const keys = [...hits.keys()].filter(k => !k.startsWith('none:'));
  check(`every side effect happened (${keys.length}/${RUNS * 2} keys)`, keys.length === RUNS * 2);
  console.log(`  calls repeated under the same key (dropped by a receiver honouring it): ${[...hits.values()].filter(n => n > 1).length}`);

  const flow = await tool(E.url, 'get_flow', { namespace: 'cluster', name: 'pay' }).catch(e => e.message);
  check('the flow is still in the registry after the failover', JSON.stringify(flow).includes('pay'));
  const st = await tool(B.url, 'get_state', { key: 'cluster:test' }).catch(e => e.message);
  check('a state key written before the failover is read after it', JSON.stringify(st).includes('7'), JSON.stringify(st));

  let offsets = [];
  for (let t = 0; t < 100; t++) {
    offsets = await Promise.all(adbs.map(async n => { try { return (await (await fetch(`http://127.0.0.1:${n.raft}/cluster/status`)).json()).wal_offset; } catch { return -1; } }));
    if (offsets.every(o => o === offsets[0] && o > 0)) break;
    await sleep(200);
  }
  check(`all three ADB WALs at the same offset (${offsets.join(', ')})`, offsets.every(o => o === offsets[0] && o > 0));
} catch (e) {
  console.error(e);
  failures++;
} finally {
  stopping = true;
  for (const c of children) c.kill('SIGKILL');
  counter.close();
  await sleep(300);
  if (failures && process.env.KEEP) console.log(`  data kept in ${tmp}`);
  else rmSync(tmp, { recursive: true, force: true });
}
console.log(failures ? `✗ ${failures} check(s) failed` : '✓ A3T cluster test passed');
process.exit(failures ? 1 : 0);
