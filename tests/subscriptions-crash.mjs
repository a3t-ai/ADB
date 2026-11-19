#!/usr/bin/env node
/**
 * Stage E gate: subscribers of one topic see identical sequences across ADB crashes.
 *
 * Each round starts ADB on the same --data-dir, publishes to a topic (and does other
 * writes) while three subscribers read, then SIGKILLs ADB at a random moment:
 *
 *   A  MCP `next_events`, keeps its own position (`from` = last seen + 1)
 *   B  MCP `next_events` + `ack` every 10 deliveries, resumes from its ack after a
 *      restart (so it sees some offsets twice; it drops offsets it already has)
 *   C  SSE `GET /subscriptions/{id}/events`, resumes with `Last-Event-ID`
 *
 * After the last round, every subscriber must hold the same offsets, in order, with
 * the same payloads; every acknowledged PUBLISH must be among them; and a fresh
 * subscription read from offset 1 must see exactly the same sequence (the log oracle).
 *
 *   node tests/subscriptions-crash.mjs --adb target/debug/adb [--rounds 10]
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
if (!args.adb) {
  console.error('usage: subscriptions-crash.mjs --adb <bin> [--rounds 10] [--data-dir <dir>]');
  process.exit(2);
}
const rounds = Number(args.rounds ?? 10);
const ownDir = !args['data-dir'];
const dataDir = args['data-dir'] ?? mkdtempSync(join(tmpdir(), 'adb-subs-'));
const TOPIC = 'orders';

const freePort = () => new Promise(res => {
  const s = createServer().listen(0, '127.0.0.1', () => { const p = s.address().port; s.close(() => res(p)); });
});

async function waitHealthy(url) {
  for (let i = 0; i < 6000; i++) {
    try { if ((await fetch(`${url}/health`)).ok) return; } catch {}
    await new Promise(r => setTimeout(r, 50));
  }
  throw new Error(`ADB did not become healthy at ${url}`);
}

async function start() {
  const port = await freePort();
  const url = `http://127.0.0.1:${port}`;
  // `--flight-port 0`: no fixed port, so runs side by side don't collide on 3001.
  const child = spawn(args.adb, ['--data-dir', dataDir, 'serve-http', '--port', String(port), '--flight-port', '0'], {
    stdio: ['ignore', 'ignore', 'pipe'], env: { ...process.env, RUST_LOG: 'warn' },
  });
  let log = '';
  child.stderr.on('data', d => { log = (log + d).slice(-4000); });
  const exited = new Promise(res => child.on('exit', (code, signal) => res({ code, signal, log })));
  // Fail at once, with ADB's own log, if it exits instead of becoming healthy.
  const died = exited.then(e => { throw new Error(`ADB exited during startup (code ${e.code}, signal ${e.signal}):\n${e.log}`); });
  await Promise.race([waitHealthy(url), died]);
  died.catch(() => {});
  return { url, child, exited };
}

async function query(url, aql) {
  const r = await fetch(`${url}/query`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ query: aql }),
  });
  const j = await r.json();
  if (!j.success) throw new Error(`${aql}: ${JSON.stringify(j)}`);
  return j.data.data;
}

let rpcId = 1;
async function tool(url, name, args, signal) {
  const r = await fetch(`${url}/mcp`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, signal,
    body: JSON.stringify({ jsonrpc: '2.0', id: rpcId++, method: 'tools/call', params: { name, arguments: args } }),
  });
  const j = await r.json();
  const text = j.result?.content?.[0]?.text;
  if (j.error || j.result?.isError) throw new Error(`${name}: ${text ?? JSON.stringify(j.error)}`);
  return JSON.parse(text);
}

/** A subscriber's view: offset → payload, in arrival order. */
class Seen {
  constructor(name) { this.name = name; this.order = []; this.payload = new Map(); this.last = 0; }
  add(d) {
    if (this.payload.has(d.offset)) return false; // at-least-once: drop repeats
    if (d.offset <= this.last) throw new Error(`${this.name}: offset ${d.offset} after ${this.last} (out of order)`);
    this.order.push(d.offset); this.payload.set(d.offset, JSON.stringify(d.changes)); this.last = d.offset;
    return true;
  }
}

// MCP subscriber: A keeps its own position, B resumes from its ack.
async function mcpReader(url, sub, seen, { acking }, stop) {
  let from = acking ? undefined : seen.last + 1;
  let sinceAck = 0;
  while (!stop.done) {
    let b;
    try { b = await tool(url, 'next_events', { subscription: sub, from, max: 50, wait_seconds: 1 }); }
    catch { return; } // killed
    for (const d of b.deliveries) if (seen.add(d)) sinceAck++;
    from = b.next;
    if (acking && sinceAck >= 10 && seen.last) {
      try { await tool(url, 'ack', { subscription: sub, offset: seen.last }); sinceAck = 0; } catch { return; }
    }
  }
}

// SSE subscriber: Last-Event-ID resumes after the last delivery received.
async function sseReader(url, sub, seen, stop) {
  const headers = seen.last ? { 'last-event-id': String(seen.last) } : {};
  let res;
  try { res = await fetch(`${url}/subscriptions/${sub}/events`, { headers, signal: stop.signal }); } catch { return; }
  if (!res.ok) throw new Error(`SSE ${res.status}: ${await res.text()}`);
  const reader = res.body.getReader();
  const dec = new TextDecoder();
  let buf = '';
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) return;
      buf += dec.decode(value, { stream: true });
      let i;
      while ((i = buf.indexOf('\n\n')) >= 0) {
        const block = buf.slice(0, i); buf = buf.slice(i + 2);
        let event = '', data = '';
        for (const line of block.split('\n')) {
          if (line.startsWith('event:')) event = line.slice(6).trim();
          else if (line.startsWith('data:')) data += line.slice(5).trim();
        }
        if (event === 'delivery') seen.add(JSON.parse(data));
        else if (event === 'error') throw new Error(`SSE error: ${data}`);
      }
    }
  } catch (e) { if (!stop.done && !String(e).includes('abort') && !String(e).includes('terminated')) throw e; }
}

const A = new Seen('A (MCP)'), B = new Seen('B (MCP + ack)'), C = new Seen('C (SSE)');
const acked = []; // offsets of acknowledged PUBLISHes
let subs;
const t0 = Date.now();

for (let round = 1; round <= rounds; round++) {
  const adb = await start();
  if (!subs) {
    subs = {};
    for (const k of ['A', 'B', 'C']) subs[k] = (await query(adb.url, `SUBSCRIBE TOPIC "${TOPIC}"`)).subscription;
  }
  const stop = { done: false, ctl: new AbortController() };
  stop.signal = stop.ctl.signal;
  const readers = [
    mcpReader(adb.url, subs.A, A, { acking: false }, stop),
    mcpReader(adb.url, subs.B, B, { acking: true }, stop),
    sseReader(adb.url, subs.C, C, stop),
  ];
  const publisher = (async () => {
    for (let i = 0; !stop.done; i++) {
      try {
        const r = await query(adb.url, `PUBLISH TOPIC "${TOPIC}" (round = ${round}, i = ${i})`);
        if (!stop.done) acked.push(r.offset);
        await query(adb.url, `STORE INTO WORKING (key = "w-${round}-${i}", i = ${i})`); // noise
      } catch { return; }
    }
  })();
  await new Promise(r => setTimeout(r, 400 + Math.random() * 1200));
  adb.child.kill('SIGKILL');
  stop.done = true;
  stop.ctl.abort();
  await adb.exited;
  await Promise.allSettled([publisher, ...readers]);
  console.log(`round ${round}/${rounds}: ${acked.length} publishes acknowledged; seen A ${A.order.length}, B ${B.order.length}, C ${C.order.length}`);
}

// Final: drain everyone to the tip, plus a fresh oracle read from offset 1.
const adb = await start();
const drain = async (sub, seen, from) => {
  for (;;) {
    const b = await tool(adb.url, 'next_events', { subscription: sub, from, max: 500, wait_seconds: 1 });
    b.deliveries.forEach(d => seen.add(d));
    from = b.next;
    if (b.deliveries.length === 0 && b.next > b.tip) return;
  }
};
const O = new Seen('oracle');
// (AQL v0.13: a subscription starts now; reading it `from` 1 replays the log.)
const oracle = (await query(adb.url, `SUBSCRIBE TOPIC "${TOPIC}"`)).subscription;
await drain(subs.A, A, A.last + 1);
await drain(subs.B, B, undefined);
await drain(subs.C, C, C.last + 1);
await drain(oracle, O, 1);
adb.child.kill('SIGTERM');
await adb.exited;
if (ownDir) rmSync(dataDir, { recursive: true, force: true });

let failures = 0;
const check = (label, ok, detail = '') => { console.log(`${ok ? '✓' : '✗'} ${label}${ok ? '' : `  — ${detail}`}`); if (!ok) failures++; };
const same = (x, y) => x.order.length === y.order.length && x.order.every((o, i) => o === y.order[i] && x.payload.get(o) === y.payload.get(o));
check(`A matches the log oracle (${O.order.length} messages)`, same(A, O), `${A.order.length} vs ${O.order.length}`);
check('B (acks, resumes from its cursor) matches', same(B, O), `${B.order.length} vs ${O.order.length}`);
check('C (SSE, Last-Event-ID) matches', same(C, O), `${C.order.length} vs ${O.order.length}`);
const missing = acked.filter(o => !O.payload.has(o));
check(`every acknowledged publish was delivered (${acked.length})`, missing.length === 0, `missing ${missing.slice(0, 5)}`);
console.log(`${rounds} kills, ${((Date.now() - t0) / 1000).toFixed(1)} s`);
if (failures) { console.log(`✗ ${failures} check(s) failed`); process.exit(1); }
console.log('✓ subscriptions crash test passed: identical sequences, nothing acknowledged missed');
