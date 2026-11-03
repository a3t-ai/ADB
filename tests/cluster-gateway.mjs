#!/usr/bin/env node
/**
 * Basic gateway + router (flowr-system .ai/A3T_CLUSTER_ARCHITECTURE.md §10, G5).
 *
 * - ADB: one node (engine, registry, notices) — or `--raft` for a 3-node replica set
 *   behind a TCP proxy to the leader, with the leader killed mid-test.
 * - Console E: the store's router, starts runs (claims nothing).
 * - Runtime R: pool `worker`, FLOWR_GATEWAY=pool. Its own outbound HTTP goes to a dead
 *   proxy (HTTP_PROXY=127.0.0.1:9, NO_PROXY=127.0.0.1 for ADB), and the target listens
 *   on 127.0.0.2: R can only reach it through the gateway.
 * - Gateways G1, G2: pool `gateway`, FLOWR_GATEWAY_DENY=/deny.
 *
 * Checks: runs on R complete and every call reached the target once per idempotency
 * key; a denied call fails its step with the gateway's reason and never reaches the
 * target; killing the gateway holding a slow call (G2 takes over) still completes the
 * run with the call made once; every call has a gateway decision in a gateway's log.
 *
 *   node tests/cluster-gateway.mjs --adb <adb bin> --flowr <flowr-ce bin (adb-platform)> [--runs 6] [--raft]
 */
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:http';
import net from 'node:net';

const args = Object.fromEntries(process.argv.slice(2).reduce((a, v, i, all) => (v.startsWith('--') ? [...a, [v.slice(2), all[i + 1]?.startsWith('--') ? 'true' : all[i + 1] ?? 'true']] : a), []));
if (!args.adb || !args.flowr) {
  console.error('usage: cluster-gateway.mjs --adb <adb bin> --flowr <flowr-ce bin> [--runs 6] [--raft]');
  process.exit(2);
}
const RUNS = Number(args.runs ?? 6);
const RAFT = args.raft === 'true';
const TOKEN = `engine-${process.pid}`;
const AUTH = `auth-${process.pid}`;
const sleep = ms => new Promise(r => setTimeout(r, ms));
const freePort = () => new Promise(res => { const s = net.createServer().listen(0, () => { const p = s.address().port; s.close(() => res(p)); }); });
let failures = 0;
const check = (label, ok, detail = '') => { console.log(`${ok ? '✓' : '✗'} ${label}${ok || !detail ? '' : `  — ${detail}`}`); if (!ok) failures++; };

// The outside world, on 127.0.0.2: counts calls per idempotency key; /slow takes 4 s.
const hits = new Map();
const counter = createServer((req, res) => {
  const key = `${req.url}|${req.headers['idempotency-key'] ?? 'none:' + Math.random()}`;
  hits.set(key, (hits.get(key) ?? 0) + 1);
  req.resume();
  req.on('end', () => setTimeout(() => { res.setHeader('content-type', 'application/json'); res.end('{"ok":true}'); }, req.url.includes('/slow') ? 4000 : 0));
});
const counterPort = await freePort();
await new Promise(r => counter.listen(counterPort, '127.0.0.2', r));
const base = `http://127.0.0.2:${counterPort}`;
const calls = path => [...hits.entries()].filter(([k]) => k.startsWith(path));

const tmp = mkdtempSync(join(tmpdir(), 'cluster-gateway-'));
const children = new Set();
let stopping = false;
function start(bin, argv, env, onExit) {
  const c = spawn(bin, argv, { env: { ...process.env, RUST_LOG: process.env.RUST_LOG ?? 'warn,flowr_ce::gateway=info,flowr_ce::engine_worker=info', ...env }, stdio: ['ignore', 'ignore', 'pipe'] });
  let log = '';
  c.stderr.on('data', d => { log = (log + d).slice(-60000); });
  c.log = () => log.replace(/\x1b\[[0-9;]*m/g, '');
  c.exited = new Promise(r => c.on('exit', (code, signal) => { children.delete(c); if (onExit) onExit(code, signal); r(); }));
  children.add(c);
  return c;
}

// ---- ADB: one node, or a replica set behind a proxy to the leader
const adbs = [];
const n = RAFT ? 3 : 1;
for (let id = 1; id <= n; id++) adbs.push({ id, http: await freePort(), flight: await freePort(), raft: await freePort(), dir: join(tmp, `adb${id}`) });
const peers = adbs.map(a => `${a.id}=http://127.0.0.1:${a.raft}`).join(',');
function launchAdb(a) {
  const raftEnv = RAFT ? { ADB_NODE_ID: String(a.id), ADB_PEERS: peers, ADB_RAFT_PORT: String(a.raft) } : {};
  a.child = start(args.adb, ['--data-dir', a.dir, 'serve-http', '--host', '127.0.0.1', '--port', String(a.http), '--flight-port', String(a.flight)], {
    ...raftEnv, ADB_ENGINE_TOKEN: TOKEN, ADB_AUTH_TOKEN: AUTH,
  }, code => { if (!stopping && code === 75) setTimeout(() => !stopping && launchAdb(a), 200); });
}
let leader = null;
async function isReady(a) { try { return (await fetch(`http://127.0.0.1:${a.http}/${RAFT ? 'ready' : 'health'}`, { signal: AbortSignal.timeout(400) })).ok; } catch { return false; } }
(async function watch() { while (!stopping) { let f = null; for (const a of adbs) if (await isReady(a)) { f = a; break; } leader = f; await sleep(100); } })();
async function waitLeader(ms = 20000) { const t0 = Date.now(); while (Date.now() - t0 < ms) { if (leader) return leader; await sleep(50); } throw new Error('no ADB leader'); }
function proxy(kind) {
  const server = net.createServer(client => {
    const t = leader;
    if (!t) { client.destroy(); return; }
    const up = net.connect(t[kind], '127.0.0.1');
    const close = () => { client.destroy(); up.destroy(); };
    client.on('error', close); up.on('error', close); client.on('close', close); up.on('close', close);
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
  for (let i = 0; i < 400; i++) { try { if ((await fetch(`${url}/health`)).ok) return; } catch {} await sleep(100); }
  throw new Error(`${who} not healthy: ${c.log().slice(-3000)}`);
}
async function waitRuns(url, ids, ms = 120000) {
  const deadline = Date.now() + ms;
  let rows = [];
  while (Date.now() < deadline) {
    rows = await Promise.all(ids.map(id => tool(url, 'get_run', { run_id: id }).then(r => r.run ?? r).catch(() => ({ phase: '?' }))));
    if (rows.every(r => ['completed', 'faulted', 'cancelled'].includes(r.phase))) break;
    await sleep(500);
  }
  return rows;
}

try {
  for (const a of adbs) launchAdb(a);
  await waitLeader();
  const httpP = await proxy('http');
  const flightP = await proxy('flight');
  const adbUrl = `http://127.0.0.1:${httpP.port}`;
  const flightUrl = `http://127.0.0.1:${flightP.port}`;

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
      FLOWR_RUN_LEASE_MS: '3000',
      ...extra,
    });
    await healthy(url, `FlowR ${name}`, c);
    return { url, c, name };
  }
  const E = await flowr('console', { FLOWR_WORKER_POOLS: '', FLOWR_DEFAULT_POOL: 'worker' });
  const R = await flowr('runtime', {
    FLOWR_WORKER_POOLS: 'worker', FLOWR_GATEWAY: 'pool',
    HTTP_PROXY: 'http://127.0.0.1:9', http_proxy: 'http://127.0.0.1:9', NO_PROXY: '127.0.0.1,localhost', no_proxy: '127.0.0.1,localhost',
  });
  const G1 = await flowr('gw1', { FLOWR_WORKER_POOLS: 'gateway', FLOWR_GATEWAY_DENY: '/deny' });
  const G2 = await flowr('gw2', { FLOWR_WORKER_POOLS: 'gateway', FLOWR_GATEWAY_DENY: '/deny' });
  console.log(`  ADB ${RAFT ? 'replica set (3)' : 'single node'}; console, runtime (worker, FLOWR_GATEWAY=pool, no direct egress), gateways G1 + G2`);

  const flow = (name, path) => `document: { dsl: '1.0.3', namespace: acme, name: ${name}, version: '1.0.0' }
do:
  - call-out:
      call: http
      with: { method: POST, endpoint: ${base}${path}, body: { n: 1 } }
  - done:
      set: { ok: true }
`;
  await tool(E.url, 'create_flow', { yaml: flow('ok', '/ok') });
  await tool(E.url, 'create_flow', { yaml: flow('denied', '/deny') });
  await tool(E.url, 'create_flow', { yaml: flow('slow', '/slow') });
  for (let i = 0; i < 40 && !(await tool(E.url, 'get_flow', { namespace: 'system-gateway', name: 'call' }).then(() => true).catch(() => false)); i++) await sleep(250);

  // 1. Runs on the runtime reach the target only through the gateway.
  const ok = [];
  for (let i = 0; i < RUNS; i++) ok.push((await tool(E.url, 'start_run', { namespace: 'acme', name: 'ok', input: { i } })).run_id);
  let rows = await waitRuns(E.url, ok);
  check(`runs with an outbound call complete on the runtime (${rows.filter(r => r.phase === 'completed').length}/${RUNS})`, rows.every(r => r.phase === 'completed'), JSON.stringify(rows.filter(r => r.phase !== 'completed').map(r => [r.phase, r.error])));
  check(`each call reached the target once (${calls('/ok').length} keys, max ${Math.max(0, ...calls('/ok').map(([, c]) => c))} per key)`, calls('/ok').length === RUNS && calls('/ok').every(([, c]) => c === 1));

  // 2. Denied.
  const d = (await tool(E.url, 'start_run', { namespace: 'acme', name: 'denied' })).run_id;
  [rows] = [await waitRuns(E.url, [d], 60000)];
  check('a denied call fails its step with the gateway\'s reason', rows[0].phase === 'faulted' && /denied by the gateway/.test(rows[0].error ?? ''), JSON.stringify([rows[0].phase, rows[0].error]));
  check('the denied call never reached the target', calls('/deny').length === 0);

  // 3. The gateway holding a slow call dies; the other one takes over.
  const before = [G1, G2].map(g => (g.c.log().match(/run claimed[^\n]*namespace=system-gateway/g) ?? []).length);
  const s = (await tool(E.url, 'start_run', { namespace: 'acme', name: 'slow' })).run_id;
  for (let i = 0; i < 100 && calls('/slow').length === 0; i++) await sleep(100);
  const claimed = g => (g.c.log().match(/run claimed[^\n]*namespace=system-gateway/g) ?? []).length > before[[G1, G2].indexOf(g)];
  const victim = claimed(G1) ? G1 : G2;
  victim.c.kill('SIGKILL');
  console.log(`  ${victim.name} killed with the slow call in flight (call reached the target: ${calls('/slow').length > 0})`);
  if (RAFT) {
    await sleep(500);
    const old = leader; old.child.kill('SIGKILL'); leader = null;
    const t0 = Date.now(); const nl = await waitLeader();
    console.log(`  ADB leader node ${old.id} killed too; node ${nl.id} leads after ${Date.now() - t0} ms`);
    setTimeout(() => !stopping && launchAdb(old), 500);
  }
  [rows] = [await waitRuns(E.url, [s], 120000)];
  check('the run with the slow call completes after its gateway died', rows[0].phase === 'completed', JSON.stringify([rows[0].phase, rows[0].error]));
  const survivor = victim === G1 ? G2 : G1;
  if (process.env.GW_DEBUG) for (const g of [G1, G2]) console.log(`  [${g.name}] ` + (g.c.log().match(/[^\n]*(run claimed|gateway decision)[^\n]*/g) ?? []).map(l => l.replace(/\x1b\[[0-9;]*m/g, '').slice(0, 230)).join(`\n  [${g.name}] `));
  check(`the other gateway resumed the gateway run (${survivor.name})`, /run claimed[^\n]*namespace=system-gateway[^\n]*resumed=true/.test(survivor.c.log()), 'no resumed claim in its log');
  const slow = calls('/slow');
  check(`the slow call was made under one idempotency key (${slow.length} key(s), ${slow.map(([, c]) => c).join('/')} hit(s))`, slow.length === 1);
  console.log(`  repeats of the slow call under its key (dropped by a receiver honouring it): ${Math.max(0, (slow[0]?.[1] ?? 1) - 1)}`);

  // 4. Decisions.
  const logs = [G1, G2].map(g => g.c.log()).join('\n');
  const decisions = (logs.match(/gateway decision/g) ?? []).length;
  check(`gateway decisions logged (${decisions}, at least ${RUNS + 2})`, decisions >= RUNS + 2);
  check('the deny decision names its reason', /allow=false.*denied by FLOWR_GATEWAY_DENY/.test(logs) || /denied by FLOWR_GATEWAY_DENY/.test(logs));
  check('the runtime made no outbound call itself', !/gateway: this process has no engine store/.test(R.c.log()));
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
console.log(failures ? `✗ ${failures} check(s) failed` : '✓ gateway test passed');
process.exit(failures ? 1 : 0);
