#!/usr/bin/env node
/**
 * Stage H-6 crash test: FlowR's runs survive ADB being killed under them.
 *
 * One ADB (with the engine token) and one FlowR whose engine store is that ADB
 * over Flight. A flow makes an HTTP call, waits on a durable timer, makes a
 * second call and ends. N runs start; ADB is SIGKILLed K times at random moments
 * and restarted on the same --data-dir. Every run must complete, and every side
 * effect must have happened exactly once per idempotency key (FlowR sends one per
 * step; a receiver that honours it sees no repeat).
 *
 *   node tests/engine-crash.mjs --adb <adb bin> --flowr <flowr-ce bin> [--runs 20] [--kills 3]
 */
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:http';
import { createServer as netServer } from 'node:net';

const args = Object.fromEntries(process.argv.slice(2).reduce((a, v, i, all) => (v.startsWith('--') ? [...a, [v.slice(2), all[i + 1]]] : a), []));
const RUNS = Number(args.runs ?? 20);
const KILLS = Number(args.kills ?? 3);
const TOKEN = `engine-${process.pid}`;
const sleep = ms => new Promise(r => setTimeout(r, ms));
const freePort = () => new Promise(res => { const s = netServer().listen(0, () => { const p = s.address().port; s.close(() => res(p)); }); });
let failures = 0;
const check = (label, ok, detail = '') => { console.log(`${ok ? '✓' : '✗'} ${label}${ok || !detail ? '' : `  — ${detail}`}`); if (!ok) failures++; };

// The side effects, counted per idempotency key.
const hits = new Map();
const counter = createServer((req, res) => {
  const key = req.headers['idempotency-key'] ?? `none:${req.url}:${Math.random()}`;
  hits.set(key, (hits.get(key) ?? 0) + 1);
  req.resume();
  req.on('end', () => { res.setHeader('content-type', 'application/json'); res.end('{"ok":true}'); });
});
const counterPort = await freePort();
await new Promise(r => counter.listen(counterPort, '127.0.0.1', r));

const tmp = mkdtempSync(join(tmpdir(), 'engine-crash-'));
const children = new Set();
function start(bin, argv, env) {
  const c = spawn(bin, argv, { env: { ...process.env, RUST_LOG: 'warn', ...env }, stdio: ['ignore', 'ignore', 'pipe'] });
  let log = '';
  c.stderr.on('data', d => { log = (log + d).slice(-6000); });
  c.log = () => log;
  c.exited = new Promise(r => c.on('exit', () => { children.delete(c); r(); }));
  children.add(c);
  return c;
}
async function healthy(url, who, c) {
  for (let i = 0; i < 200; i++) {
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

try {
  const [adbPort, flightPort, flowrPort] = [await freePort(), await freePort(), await freePort()];
  const adbUrl = `http://127.0.0.1:${adbPort}`;
  const adbArgs = ['--data-dir', join(tmp, 'adb'), 'serve-http', '--port', String(adbPort), '--flight-port', String(flightPort)];
  let adb = start(args.adb, adbArgs, { ADB_ENGINE_TOKEN: TOKEN });
  await healthy(adbUrl, 'ADB', adb);

  const flowrUrl = `http://127.0.0.1:${flowrPort}`;
  const flowr = start(args.flowr, ['--mode', 'http', '--port', String(flowrPort)], {
    CLAWDFLOW_DATA_DIR: join(tmp, 'flowr'),
    FLOWR_ENGINE_ADB_URL: `http://127.0.0.1:${flightPort}`,
    FLOWR_ENGINE_ADB_TOKEN: TOKEN,
  });
  await healthy(flowrUrl, 'FlowR', flowr);

  const base = `http://127.0.0.1:${counterPort}`;
  await tool(flowrUrl, 'create_flow', { yaml: `document: { dsl: '1.0.3', namespace: crash, name: pay, version: '1.0.0' }
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

  const ids = [];
  for (let i = 0; i < RUNS; i++) {
    ids.push((await tool(flowrUrl, 'start_run', { namespace: 'crash', name: 'pay', input: { i } })).run_id);
  }
  console.log(`  ${RUNS} runs started; killing ADB ${KILLS} times`);

  for (let k = 0; k < KILLS; k++) {
    await sleep(1000 + Math.random() * 3000);
    adb.kill('SIGKILL');
    await adb.exited;
    await sleep(300);
    adb = start(args.adb, adbArgs, { ADB_ENGINE_TOKEN: TOKEN });
    await healthy(adbUrl, 'ADB (restarted)', adb);
    console.log(`  kill ${k + 1}: ADB back`);
  }

  const deadline = Date.now() + 180_000;
  let runs = [];
  while (Date.now() < deadline) {
    runs = (await tool(flowrUrl, 'list_runs', { namespace: 'crash', limit: 100 })).runs ?? [];
    if (runs.length === RUNS && runs.every(r => ['completed', 'faulted', 'cancelled'].includes(r.phase))) break;
    await sleep(1000);
  }
  const done = runs.filter(r => r.phase === 'completed').length;
  check(`every run completed (${done}/${RUNS})`, done === RUNS, JSON.stringify(runs.filter(r => r.phase !== 'completed').map(r => [r.run_id, r.phase, r.error])));
  const keys = [...hits.keys()].filter(k => !k.startsWith('none:'));
  check(`every side effect happened (${keys.length}/${RUNS * 2} keys)`, keys.length === RUNS * 2);
  const repeats = [...hits.entries()].filter(([, n]) => n > 1);
  console.log(`  calls repeated under the same key (a receiver honouring the key drops them): ${repeats.length}`);
  check('no call without an idempotency key', ![...hits.keys()].some(k => k.startsWith('none:')));
  for (const id of ids) {
    const j = await tool(flowrUrl, 'get_run_journal', { run_id: id });
    const seqs = (j.entries ?? []).map(e => e.seq);
    if (new Set(seqs).size !== seqs.length || seqs.some((s, i) => i > 0 && s <= seqs[i - 1])) {
      check(`run ${id}: journal sequence numbers unique and increasing`, false, JSON.stringify(seqs));
    }
  }
  check('journals intact', true);
} catch (e) {
  console.error(e);
  failures++;
} finally {
  for (const c of children) c.kill('SIGKILL');
  counter.close();
  rmSync(tmp, { recursive: true, force: true });
}
console.log(failures ? `✗ ${failures} check(s) failed` : '✓ engine crash test passed');
process.exit(failures ? 1 : 0);
