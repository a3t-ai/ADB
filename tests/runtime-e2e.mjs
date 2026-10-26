#!/usr/bin/env node
/**
 * MVP W2 gate: a FlowR runtime runs through ADB — shared registry, work from the
 * canvas topic, replies and memory in ADB, stateless restarts.
 *
 * One ADB, a "console" FlowR and a "runtime" FlowR, both with FLOWR_REGISTRY_ADB
 * (registry and agent state on ADB platform tables). The console creates the ADB
 * resource and the flow; the runtime is started like the a3t-controller starts a
 * Sandbox pod (A3T_CANVAS, A3T_FLOW, an empty data dir) and must:
 *
 *   - see the console's flow and resources (shared registry),
 *   - subscribe to the canvas topic `canvas.<ns>.<name>`,
 *   - run the flow once per request published there, storing memory in ADB and
 *     publishing the result to the request's `_reply_topic`,
 *   - after a SIGKILL and a restart on a NEW empty data dir, resume its
 *     subscription (kept in ADB) and run the request published while it was down.
 *
 * Then the console runs A3T's own test flow `a3t/canvas-dispatch` (flowr-system/a3t/
 * backend/flows/test/canvas_dispatch.yaml) against the runtime, as a user would.
 *
 *   node tests/runtime-e2e.mjs --adb <adb bin> --flowr <flowr-ce bin (adb-platform)> [--flows <a3t flows dir>]
 */
import { spawn } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:net';

const args = Object.fromEntries(process.argv.slice(2).reduce((acc, a, i, all) => {
  if (a.startsWith('--')) acc.push([a.slice(2), all[i + 1]]);
  return acc;
}, []));
if (!args.adb || !args.flowr) {
  console.error('usage: runtime-e2e.mjs --adb <adb bin> --flowr <flowr-ce bin>');
  process.exit(2);
}

const tmp = mkdtempSync(join(tmpdir(), 'adb-runtime-'));
const children = new Set();
let failures = 0;
const check = (label, ok, detail = '') => { console.log(`${ok ? '✓' : '✗'} ${label}${ok || !detail ? '' : `  — ${detail}`}`); if (!ok) failures++; };
const sleep = ms => new Promise(r => setTimeout(r, ms));
const freePort = () => new Promise(res => {
  const s = createServer().listen(0, '127.0.0.1', () => { const p = s.address().port; s.close(() => res(p)); });
});

async function waitHealthy(url, what, child) {
  for (let i = 0; i < 600; i++) {
    try { if ((await fetch(`${url}/health`)).ok) return; } catch {}
    await sleep(100);
  }
  throw new Error(`${what} did not become healthy at ${url}: ${child?.log() ?? ''}`);
}

function start(bin, argv, env) {
  const child = spawn(bin, argv, { env: { ...process.env, RUST_LOG: 'warn', ...env }, stdio: ['ignore', 'ignore', 'pipe'] });
  let log = '';
  child.stderr.on('data', d => { log = (log + d).slice(-8000); });
  child.exited = new Promise(res => child.on('exit', (code, signal) => { children.delete(child); res({ code, signal }); }));
  child.log = () => log;
  children.add(child);
  return child;
}

async function aql(url, query) {
  const r = await (await fetch(`${url}/query`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ query }) })).json();
  if (!r.success) throw new Error(`${query}: ${JSON.stringify(r)}`);
  return r.data.data;
}

let rpcId = 1;
async function adbTool(url, name, toolArgs) {
  const r = await (await fetch(`${url}/mcp`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: rpcId++, method: 'tools/call', params: { name, arguments: toolArgs } }),
  })).json();
  const text = r.result?.content?.[0]?.text ?? '';
  if (r.error || r.result?.isError) throw new Error(`${name}: ${text || JSON.stringify(r)}`);
  return JSON.parse(text);
}

async function flowrTool(url, name, toolArgs) {
  const r = await (await fetch(`${url}/message`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: rpcId++, method: 'tools/call', params: { name, arguments: toolArgs } }),
  })).json();
  if (r.error || r.result?.isError) throw new Error(`${name}: ${JSON.stringify(r.error ?? r.result)}`);
  return r.result?.content?.[0]?.text ?? '';
}

const CANVAS = { ns: 'a3t', name: 'echo-canvas' };
const TOPIC = `canvas.${CANVAS.ns}.${CANVAS.name}`;
const TRIGGER = `runtime-${CANVAS.ns}-${CANVAS.name}`;

// The deployed flow: remember the request in ADB, answer with an echo.
const FLOW = `document:
  dsl: 1.0.3
  namespace: a3t
  name: runtime-echo
  version: 1.0.0
do:
- init:
    set:
      _in: \${ .input // . }
    export:
      as: '\${ $context + . }'
- remember:
    call: adb
    with:
      query: '\${ "STORE INTO SEMANTIC (key = \\"echo-" + $context._in.request_id + "\\", content = \\"" + $context._in.text + "\\")" }'
      _resource: a3t/agent-memory
    export:
      as: '\${ $context + {remember: .} }'
- answer:
    set:
      echo: \${ $context._in.text }
      stored: \${ $context.remember.data.type }
`;

// A3T's copies of the flows, when the flows dir is there (default: ../flowr-system).
const flowsDir = args.flows ?? new URL('../../flowr-system/a3t/backend/flows', import.meta.url).pathname;
const a3tFlow = f => existsSync(join(flowsDir, 'test', f)) ? readFileSync(join(flowsDir, 'test', f), 'utf8') : null;
const ECHO = a3tFlow('runtime_echo.yaml') ?? FLOW;
const DISPATCH = a3tFlow('canvas_dispatch.yaml');

function flowOutput(text) {
  const i = text.indexOf('Output:\n');
  return i < 0 ? undefined : JSON.parse(text.slice(i + 8));
}

let adbUrl, flightUrl;
const registryEnv = () => ({ FLOWR_REGISTRY_ADB: adbUrl, FLOWR_REGISTRY_ADB_FLIGHT: flightUrl });

async function startFlowr(dataDir, env) {
  const port = await freePort();
  const url = `http://127.0.0.1:${port}`;
  const child = start(args.flowr, ['--mode', 'http', '--port', String(port)], { CLAWDFLOW_DATA_DIR: dataDir, ...registryEnv(), ...env });
  await waitHealthy(url, 'FlowR', child);
  return { url, child };
}

const startRuntime = dataDir => startFlowr(dataDir, { A3T_CANVAS: `${CANVAS.ns}/${CANVAS.name}`, A3T_FLOW: 'a3t/runtime-echo' });

async function subscription(f) {
  for (let i = 0; i < 150; i++) {
    const r = await fetch(`${f.url}/triggers/${TRIGGER}`);
    if (r.ok) {
      const t = await r.json();
      if (t.config?.subscription) return t.config.subscription;
    }
    await sleep(200);
  }
  throw new Error(`runtime never subscribed: ${f.child.log()}`);
}

/** A reply's run output (`output_json`). */
const out = r => { try { return JSON.parse(r?.output_json ?? 'null'); } catch { return null; } };

/** Send a request to the canvas (its reply comes on `replies.<prefix>`). */
async function request(id, text) {
  await adbTool(adbUrl, 'publish', { topic: TOPIC, fields: { request_id: id, text, _reply_topic: `replies.${id.split('-')[0]}` } });
}

async function reply(replySub, id, waitMs = 30000) {
  const deadline = Date.now() + waitMs;
  while (Date.now() < deadline) {
    const b = await adbTool(adbUrl, 'next_events', { subscription: replySub, wait_seconds: 5, max: 50 });
    for (const d of b.deliveries ?? []) {
      for (const c of d.changes ?? []) {
        if (c.op === 'publish' && c.value?.request_id === id) return c.value;
      }
    }
  }
  return null;
}

try {
  const adbPort = await freePort();
  adbUrl = `http://127.0.0.1:${adbPort}`;
  const flightPort = await freePort();
  flightUrl = `http://127.0.0.1:${flightPort}`;
  const adbProc = start(args.adb, ['--data-dir', join(tmp, 'adb'), 'serve-http', '--port', String(adbPort), '--flight-port', String(flightPort)], {});
  await waitHealthy(adbUrl, 'ADB', adbProc);

  // Console: resource + flow, into the shared registry.
  const C = await startFlowr(join(tmp, 'console'), {});
  await flowrTool(C.url, 'create_resource', { namespace: 'a3t', name: 'agent-memory', type_ref: 'adb', config: { url: adbUrl, flight_url: flightUrl } });
  await flowrTool(C.url, 'create_resource', { namespace: 'a3t', name: 'agent-memory-mcp', type_ref: 'mcp', config: { url: `${adbUrl}/mcp/sse`, transport: 'sse' } });
  await flowrTool(C.url, 'create_flow', { yaml: ECHO });
  if (DISPATCH) await flowrTool(C.url, 'create_flow', { yaml: DISPATCH });
  const tables = await (await fetch(`${adbUrl}/health`)).ok;
  check('console is up on the shared registry', tables);

  // Runtime, as the controller starts it.
  let R = await startRuntime(join(tmp, 'runtime-1'));
  const flows = await flowrTool(R.url, 'list_flows', { namespace: 'a3t' });
  check('runtime sees the console\'s flow (shared registry)', flows.includes('runtime-echo'), flows.slice(0, 300));
  const sub1 = await subscription(R);
  check('runtime subscribed to the canvas topic', !!sub1);

  // A reply subscription for this test, then a request.
  const replySub = (await aql(adbUrl, 'SUBSCRIBE TOPIC "replies.r"')).subscription;
  await request('r-1', 'hello runtime');
  const r1 = await reply(replySub, 'r-1');
  check('request 1: the runtime ran the flow and replied', out(r1)?.echo === 'hello runtime', JSON.stringify(r1));
  check('request 1: status completed', /complete/i.test(JSON.stringify(r1?.status ?? '')), JSON.stringify(r1?.status));
  check('request 1: reply names the canvas', r1?.canvas === `${CANVAS.ns}/${CANVAS.name}`, r1?.canvas);
  const m1 = await aql(adbUrl, 'LOOKUP FROM SEMANTIC KEY id = "echo-r-1"');
  check('request 1: memory stored in ADB', m1.records?.[0]?.data?.content === 'hello runtime', JSON.stringify(m1));

  // Kill the runtime; a request arrives while it is down; a fresh pod (empty disk) takes over.
  R.child.kill('SIGKILL');
  await R.child.exited;
  await request('r-2', 'while you were out');
  R = await startRuntime(join(tmp, 'runtime-2'));
  const sub2 = await subscription(R);
  check('a new runtime pod resumes the same subscription (kept in ADB)', sub2 === sub1, `${sub2} vs ${sub1}`);
  const r2 = await reply(replySub, 'r-2');
  check('request 2 (sent while down) handled after the restart', out(r2)?.echo === 'while you were out', JSON.stringify(r2));
  await request('r-3', 'and again');
  const r3 = await reply(replySub, 'r-3');
  check('request 3 handled', out(r3)?.echo === 'and again', JSON.stringify(r3));

  // A3T's dispatch flow, run on the console, round-trips through the runtime.
  if (DISPATCH) {
    const text = await flowrTool(C.url, 'execute_flow', { namespace: 'a3t', name: 'canvas-dispatch', input: { input: { canvas: `${CANVAS.ns}/${CANVAS.name}`, text: 'via canvas-dispatch' } } });
    const o = flowOutput(text);
    check('a3t/canvas-dispatch: ok, echo and memory', o?.ok === true && o?.output?.echo === 'via canvas-dispatch' && o?.memory === 'via canvas-dispatch', text.slice(-600));
  } else {
    console.log(`  (no ${flowsDir}/test/canvas_dispatch.yaml; dispatch flow skipped)`);
  }

  // The console still sees one flow (nothing duplicated by the runtimes).
  const cflows = JSON.parse(await flowrTool(C.url, 'list_flows', { namespace: 'a3t' }) || '{}');
  const n = JSON.stringify(cflows).split('"runtime-echo"').length - 1;
  check('the registry holds the flow once', n === 1, JSON.stringify(cflows).slice(0, 300));
} catch (e) {
  check('session', false, e.stack);
} finally {
  for (const c of children) c.kill();
  await sleep(300);
  rmSync(tmp, { recursive: true, force: true });
}
console.log(failures === 0 ? '\n✓ runtime e2e test passed' : `\n✗ ${failures} check(s) failed`);
process.exit(failures === 0 ? 0 : 1);
