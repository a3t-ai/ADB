#!/usr/bin/env node
/**
 * Stage F gate: FlowR `adb_topic` triggers — cross-instance notifications on topics.
 *
 * One ADB and two FlowR CE instances (A, B). Each FlowR has an `adb_topic` trigger on
 * topic "orders" whose flow records, back into ADB, which instance handled which
 * message. Messages are published; then FlowR A is SIGKILLed, more messages are
 * published while it is down, and A is restarted on the same data dir.
 *
 * Every instance must have run its flow for every message (A after catching up from
 * its subscription's ack); duplicates are allowed (at-least-once) and reported.
 *
 *   node tests/topic-trigger.mjs --adb <adb bin> --flowr <flowr-ce bin>
 */
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:net';

const args = Object.fromEntries(process.argv.slice(2).reduce((acc, a, i, all) => {
  if (a.startsWith('--')) acc.push([a.slice(2), all[i + 1]]);
  return acc;
}, []));
if (!args.adb || !args.flowr) {
  console.error('usage: topic-trigger.mjs --adb <adb bin> --flowr <flowr-ce bin>');
  process.exit(2);
}

const tmp = mkdtempSync(join(tmpdir(), 'adb-topic-'));
const children = new Set();
let failures = 0;
const check = (label, ok, detail = '') => { console.log(`${ok ? '✓' : '✗'} ${label}${ok || !detail ? '' : `  — ${detail}`}`); if (!ok) failures++; };
const sleep = ms => new Promise(r => setTimeout(r, ms));
const freePort = () => new Promise(res => {
  const s = createServer().listen(0, '127.0.0.1', () => { const p = s.address().port; s.close(() => res(p)); });
});

async function waitHealthy(url, what) {
  for (let i = 0; i < 600; i++) {
    try { if ((await fetch(`${url}/health`)).ok) return; } catch {}
    await sleep(100);
  }
  throw new Error(`${what} did not become healthy at ${url}`);
}

function start(bin, argv, env, name) {
  const child = spawn(bin, argv, { env: { ...process.env, RUST_LOG: 'warn', ...env }, stdio: ['ignore', 'ignore', 'pipe'] });
  let log = '';
  child.stderr.on('data', d => { log = (log + d).slice(-6000); });
  child.exited = new Promise(res => child.on('exit', (code, signal) => { children.delete(child); res({ code, signal, log }); }));
  child.log = () => log;
  children.add(child);
  return child;
}

async function adb(url, query) {
  const r = await (await fetch(`${url}/query`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ query }) })).json();
  if (!r.success) throw new Error(`${query}: ${JSON.stringify(r)}`);
  return r.data.data;
}

let rpcId = 1;
async function flowrTool(url, name, toolArgs) {
  const r = await fetch(`${url}/message`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: rpcId++, method: 'tools/call', params: { name, arguments: toolArgs } }),
  });
  const body = await r.json();
  if (body.error || body.result?.isError) throw new Error(`${name}: ${JSON.stringify(body.error ?? body.result)}`);
  return body.result?.content?.[0]?.text ?? '';
}

// The flow a trigger runs: record (instance, order, offset) in ADB.
const flowYaml = (instance) => `document:
  dsl: 1.0.3
  namespace: default
  name: on-order
  version: 1.0.0
do:
- record:
    call: adb
    with:
      query: '\${ (.input // .) as $m | "STORE INTO EPISODIC (instance = \\"${instance}\\", order_id = \\"" + $m.order_id + "\\", topic_offset = " + ($m._offset | tostring) + ")" }'
      _resource: default/adb
`;

async function startFlowr(name, dataDir) {
  const port = await freePort();
  const url = `http://127.0.0.1:${port}`;
  const child = start(args.flowr, ['--mode', 'http', '--port', String(port)], { CLAWDFLOW_DATA_DIR: dataDir }, `flowr ${name}`);
  await waitHealthy(url, `FlowR ${name}`);
  return { name, url, child, dataDir };
}

let adbFlightUrl;
async function setup(f, adbUrl) {
  await flowrTool(f.url, 'create_resource', { namespace: 'default', name: 'adb', type_ref: 'adb', config: { url: adbUrl, flight_url: adbFlightUrl } });
  await flowrTool(f.url, 'create_flow', { yaml: flowYaml(f.name) });
  const r = await fetch(`${f.url}/triggers`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      name: 'orders', namespace: 'default',
      config: { type: 'adb_topic', resource: 'default/adb', topic: 'orders' },
      targets: [{ type: 'workflow', namespace: 'default', name: 'on-order' }],
    }),
  });
  const j = await r.json();
  if (!r.ok) throw new Error(`create trigger on ${f.name}: ${JSON.stringify(j)}`);
  f.trigger = j.id;
}

async function subscribed(f) {
  for (let i = 0; i < 100; i++) {
    const t = await (await fetch(`${f.url}/triggers/${f.trigger}`)).json();
    if (t.config?.subscription) return t.config.subscription;
    await sleep(200);
  }
  throw new Error(`FlowR ${f.name} never subscribed: ${f.child.log()}`);
}

/** Offsets FlowR `instance` has handled, with repeats. */
async function handled(adbUrl, instance) {
  const r = await adb(adbUrl, `RECALL FROM EPISODIC WHERE instance = "${instance}" LIMIT 100000`);
  return (r.records ?? []).map(x => x.data.topic_offset);
}

async function waitHandled(adbUrl, instance, want) {
  let got = [];
  for (let i = 0; i < 300; i++) {
    got = await handled(adbUrl, instance);
    if (want.every(o => got.includes(o))) return got;
    await sleep(200);
  }
  return got;
}

try {
  const adbPort = await freePort();
  const adbUrl = `http://127.0.0.1:${adbPort}`;
  const flightPort = await freePort();
  adbFlightUrl = `http://127.0.0.1:${flightPort}`;
  start(args.adb, ['--data-dir', join(tmp, 'adb'), 'serve-http', '--port', String(adbPort), '--flight-port', String(flightPort)], {}, 'adb');
  await waitHealthy(adbUrl, 'ADB');
  let A = await startFlowr('A', join(tmp, 'flowr-a'));
  const B = await startFlowr('B', join(tmp, 'flowr-b'));
  await setup(A, adbUrl);
  await setup(B, adbUrl);
  const [subA, subB] = [await subscribed(A), await subscribed(B)];
  check('each FlowR instance holds its own subscription', subA && subB && subA !== subB, `${subA} ${subB}`);

  const published = [];
  const publish = async (n, from) => {
    for (let i = from; i < from + n; i++) published.push((await adb(adbUrl, `PUBLISH TOPIC "orders" (order_id = "o-${i}")`)).offset);
  };
  await publish(20, 0);
  const a1 = await waitHandled(adbUrl, 'A', published);
  const b1 = await waitHandled(adbUrl, 'B', published);
  check(`A ran its flow for all ${published.length} messages`, published.every(o => a1.includes(o)), `${a1.length} handled`);
  check(`B ran its flow for all ${published.length} messages`, published.every(o => b1.includes(o)), `${b1.length} handled`);

  // A dies; messages published while it is down reach it after the restart.
  A.child.kill('SIGKILL');
  await A.child.exited;
  await publish(10, 20);
  A = { ...(await startFlowr('A', A.dataDir)), trigger: A.trigger };
  const resumed = await subscribed(A);
  check('A resumes its subscription after a restart', resumed === subA, `${resumed} vs ${subA}`);
  const a2 = await waitHandled(adbUrl, 'A', published);
  const b2 = await waitHandled(adbUrl, 'B', published);
  check(`A caught up: all ${published.length} messages`, published.every(o => a2.includes(o)), `missing ${published.filter(o => !a2.includes(o))}`);
  check(`B: all ${published.length} messages`, published.every(o => b2.includes(o)), `missing ${published.filter(o => !b2.includes(o))}`);
  const extraA = a2.length - new Set(a2).size, extraB = b2.length - new Set(b2).size;
  console.log(`  repeats (at-least-once): A ${extraA}, B ${extraB}`);
  check('nothing but published messages ran', [...a2, ...b2].every(o => published.includes(o)));
} catch (e) {
  check('session', false, e.stack);
} finally {
  for (const c of children) c.kill();
  await sleep(300);
  rmSync(tmp, { recursive: true, force: true });
}
console.log(failures === 0 ? '\n✓ topic trigger test passed' : `\n✗ ${failures} check(s) failed`);
process.exit(failures === 0 ? 0 : 1);
