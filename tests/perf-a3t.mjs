#!/usr/bin/env node
/**
 * A3T call latency and write throughput on a 3-node ADB replica set (perf baseline
 * for CA-33 transient flows and CA-24 group commit).
 *
 * One FlowR (console role: claims `default`, engine + registry + state on ADB) in
 * front of a 3-node ADB behind a TCP proxy to the leader. Measured, all over the
 * FlowR's own /message (localhost, no network):
 *
 *   read      get_state, list_flows                 (one ADB read each)
 *   flow      execute_flow of bench/read3:          three call:state:get + set
 *             (a platform-style API flow), sequential
 *   flow xC   the same flow, C callers at once      (throughput)
 *   write     set_state, C writers for --secs       (Raft commits/s, latency)
 *
 * Flags: --adb <bin> --flowr <bin> [--n 40] [--c 8] [--secs 8] [--transient] [--ee]
 *   --ee: the FlowR binary is EE (flowr-server), which needs a bearer token: the
 *         console gets a throwaway signing secret for this run and the driver signs its own.
 * (--transient marks bench/read3 `metadata.a3t.durability: transient`).
 */
import { spawn } from 'node:child_process';
import { appendFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import net from 'node:net';
import { createHmac, randomBytes } from 'node:crypto';

const args = Object.fromEntries(process.argv.slice(2).reduce((a, v, i, all) => (v.startsWith('--') ? [...a, [v.slice(2), all[i + 1]?.startsWith('--') || all[i + 1] === undefined ? 'true' : all[i + 1]]] : a), []));
if (!args.adb || !args.flowr) { console.error('usage: perf-a3t.mjs --adb <bin> --flowr <bin> [--n 40] [--c 8] [--secs 8] [--transient]'); process.exit(2); }
const N = Number(args.n ?? 40), C = Number(args.c ?? 8), SECS = Number(args.secs ?? 8);
const TRANSIENT = args.transient === 'true';
const SINGLE = args.single === 'true'; // one ADB node, no Raft
const TOKEN = `engine-${process.pid}`, AUTH = `auth-${process.pid}`;
const sleep = ms => new Promise(r => setTimeout(r, ms));
const freePort = () => new Promise(res => { const s = net.createServer().listen(0, () => { const p = s.address().port; s.close(() => res(p)); }); });
const tmp = mkdtempSync(join(tmpdir(), 'perf-a3t-'));
const kids = new Set();
function start(bin, argv, env) {
  const c = spawn(bin, argv, { env: { ...process.env, RUST_LOG: process.env.PERF_RUST_LOG ?? 'warn', ...env }, stdio: ['ignore', 'ignore', 'pipe'] });
  const tag = env.HOSTNAME ?? `adb${env.ADB_NODE_ID ?? ''}`;
  let log = ''; c.stderr.on('data', d => { log = (log + d).slice(-8000); if (process.env.LOGS) appendFileSync(join(process.env.LOGS, `${tag}.log`), d); }); c.log = () => log;
  kids.add(c); return c;
}
const adbs = [];
for (let id = 1; id <= (SINGLE ? 1 : 3); id++) adbs.push({ id, http: await freePort(), flight: await freePort(), raft: await freePort(), dir: join(tmp, `adb${id}`) });
const peers = adbs.map(a => `${a.id}=http://127.0.0.1:${a.raft}`).join(',');
for (const a of adbs) a.c = start(args.adb, ['--data-dir', a.dir, 'serve-http', '--host', '127.0.0.1', '--port', String(a.http), '--flight-port', String(a.flight)], { ...(SINGLE ? {} : { ADB_NODE_ID: String(a.id), ADB_PEERS: peers, ADB_RAFT_PORT: String(a.raft) }), ADB_ENGINE_TOKEN: TOKEN, ADB_AUTH_TOKEN: AUTH });
let leader = null;
const ready = async a => { try { return (await fetch(`http://127.0.0.1:${a.http}/${SINGLE ? 'health' : 'ready'}`, { signal: AbortSignal.timeout(300) })).ok; } catch { return false; } };
for (let i = 0; i < 300 && !leader; i++) { for (const a of adbs) if (await ready(a)) { leader = a; break; } await sleep(50); }
if (!leader) { console.error('no ADB leader'); process.exit(1); }
const proxy = kind => new Promise(r => { const s = net.createServer({ noDelay: true }, cl => { cl.setNoDelay(true); const up = net.connect({ port: leader[kind], host: '127.0.0.1', noDelay: true }); const x = () => { cl.destroy(); up.destroy(); }; cl.on('error', x); up.on('error', x); cl.on('close', x); up.on('close', x); cl.pipe(up).pipe(cl); }); s.listen(0, '127.0.0.1', () => r(s.address().port)); });
const httpP = await proxy('http'), flightP = await proxy('flight');
const port = await freePort();
const url = `http://127.0.0.1:${port}`;
const EE = 'ee' in args;
const JWT_SECRET = randomBytes(32).toString('hex');
const b64u = b => Buffer.from(b).toString('base64url');
const jwt = claims => { const h = b64u(JSON.stringify({ alg: 'HS256', typ: 'JWT' })), p = b64u(JSON.stringify(claims)); return `${h}.${p}.${createHmac('sha256', JWT_SECRET).update(`${h}.${p}`).digest('base64url')}`; };
const F = start(args.flowr, ['--mode', 'http', '--port', String(port)], {
  HOSTNAME: 'flowr-console', CLAWDFLOW_DATA_DIR: join(tmp, 'flowr'),
  FLOWR_ENGINE_ADB_URL: `http://127.0.0.1:${flightP}`, FLOWR_ENGINE_ADB_TOKEN: TOKEN,
  FLOWR_REGISTRY_ADB: `http://127.0.0.1:${httpP}`, FLOWR_REGISTRY_ADB_FLIGHT: `http://127.0.0.1:${flightP}`,
  ADB_AUTH_TOKEN: AUTH, ADB_AUTH_URLS: `http://127.0.0.1:${httpP},http://127.0.0.1:${flightP}`,
  ...(EE ? { FLOWR_JWT_SECRET: JWT_SECRET, MCP_RESOURCE_URI: url } : {}),
});
const BEARER = EE ? jwt({ sub: 'perf', org: 'org-default', role: 'superadmin', email: 'perf@localhost', name: 'perf', aud: url, iat: Math.floor(Date.now() / 1000), exp: Math.floor(Date.now() / 1000) + 86400 }) : null;
for (let i = 0; i < 400; i++) { try { if ((await fetch(`${url}/health`)).ok) break; } catch {} await sleep(100); }
let id = 1;
async function tool(name, a) {
  const r = await (await fetch(`${url}/message`, { method: 'POST', headers: { 'content-type': 'application/json', ...(BEARER ? { authorization: `Bearer ${BEARER}` } : {}) }, body: JSON.stringify({ jsonrpc: '2.0', id: id++, method: 'tools/call', params: { name, arguments: a } }) })).json();
  if (r.error || r.result?.isError) throw new Error(`${name}: ${r.result?.content?.[0]?.text ?? JSON.stringify(r.error)}`);
  return r.result?.content?.[0]?.text ?? '';
}
const stat = xs => { const s = [...xs].sort((a, b) => a - b); const q = p => s[Math.min(s.length - 1, Math.floor(s.length * p))]; return { n: s.length, p50: q(0.5), p90: q(0.9), p99: q(0.99), mean: s.reduce((a, b) => a + b, 0) / s.length }; };
const fmt = (label, s) => console.log(`  ${label.padEnd(30)} p50 ${s.p50.toFixed(1).padStart(7)} ms  p90 ${s.p90.toFixed(1).padStart(7)}  p99 ${s.p99.toFixed(1).padStart(7)}  (n=${s.n})`);
async function timed(fn, n) { const xs = []; for (let i = 0; i < n; i++) { const t = performance.now(); await fn(i); xs.push(performance.now() - t); } return stat(xs); }

const results = {};
try {
  await tool('set_state', { key: 'bench:a', value: { n: 1 } });
  await tool('set_state', { key: 'bench:b', value: { n: 2 } });
  await tool('set_state', { key: 'bench:c', value: { n: 3 } });
  const meta = TRANSIENT ? "\nmetadata: { a3t: { durability: transient } }" : '';
  await tool('create_flow', { yaml: `document: { dsl: '1.0.3', namespace: bench, name: read3, version: '1.0.0' }${meta}
do:
  - a: { call: "state:get", with: { key: "bench:a" } }
  - b: { call: "state:get", with: { key: "bench:b" } }
  - c: { call: "state:get", with: { key: "bench:c" } }
  - done: { set: { ok: true } }
` });
  await tool('create_flow', { yaml: `document: { dsl: '1.0.3', namespace: bench, name: noop, version: '1.0.0' }${meta}
do:
  - done: { set: { ok: true } }
` });
  // Warm up.
  for (let i = 0; i < 5; i++) { await tool('get_state', { key: 'bench:a' }); await tool('execute_flow', { namespace: 'bench', name: 'read3' }); }
  console.log(`  ADB ${SINGLE ? '1 node (no Raft)' : `3 nodes (leader ${leader.id})`}; FlowR console; bench/read3${TRANSIENT ? ' (transient)' : ''}`);
  results.read_state = await timed(() => tool('get_state', { key: 'bench:a' }), N); fmt('read: get_state', results.read_state);
  results.read_flows = await timed(() => tool('list_flows', { namespace: 'bench' }), N); fmt('read: list_flows', results.read_flows);
  results.noop = await timed(async () => { const t = await tool('execute_flow', { namespace: 'bench', name: 'noop' }); if (!t.includes('completed')) throw new Error(t.slice(0, 200)); }, N); fmt('flow: noop (one set step)', results.noop);
  results.flow = await timed(async () => { const t = await tool('execute_flow', { namespace: 'bench', name: 'read3' }); if (!t.includes('completed')) throw new Error(t.slice(0, 200)); }, N); fmt('flow: read3 (sequential)', results.flow);
  { const t0 = performance.now(); const xs = []; await Promise.all(Array.from({ length: C }, async () => { for (let i = 0; i < Math.ceil(N / 2); i++) { const t = performance.now(); await tool('execute_flow', { namespace: 'bench', name: 'read3' }); xs.push(performance.now() - t); } })); const s = stat(xs); results.flow_c = { ...s, per_s: xs.length / ((performance.now() - t0) / 1000) }; fmt(`flow: read3 x${C} callers`, s); console.log(`  ${''.padEnd(30)} → ${results.flow_c.per_s.toFixed(1)} flows/s`); }
  { const xs = []; const end = performance.now() + SECS * 1000; let k = 0; await Promise.all(Array.from({ length: C }, async (_, w) => { while (performance.now() < end) { const t = performance.now(); await tool('set_state', { key: `bench:w${w}:${k++}`, value: { k } }); xs.push(performance.now() - t); } })); const s = stat(xs); results.write = { ...s, per_s: xs.length / SECS }; fmt(`write: set_state x${C} writers`, s); console.log(`  ${''.padEnd(30)} → ${results.write.per_s.toFixed(0)} writes/s`); }
  console.log('RESULTS ' + JSON.stringify(results));
} catch (e) { console.error(e, F.log()); process.exitCode = 1; }
finally { for (const k of kids) k.kill('SIGKILL'); await sleep(300); rmSync(tmp, { recursive: true, force: true }); process.exit(); }
