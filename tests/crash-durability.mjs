#!/usr/bin/env node
/**
 * Stage C crash test: no acknowledged write is lost when ADB is killed mid-write.
 *
 * Each round starts ADB on the same --data-dir, runs concurrent writers (STORE INTO
 * EPISODIC, one key per write), kills ADB with SIGKILL at a random moment, and
 * restarts it. After every restart, every write that got `success: true` must be
 * there. Unacknowledged writes may or may not be (both are correct).
 *
 *   node tests/crash-durability.mjs --adb target/release/adb [--rounds 50] [--writers 8]
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
  console.error('usage: crash-durability.mjs --adb <bin> [--rounds 50] [--writers 8] [--data-dir <dir>]');
  process.exit(2);
}
const rounds = Number(args.rounds ?? 50);
const writers = Number(args.writers ?? 8);
const dataDir = args['data-dir'] ?? mkdtempSync(join(tmpdir(), 'adb-crash-'));

const freePort = () => new Promise(res => {
  const s = createServer().listen(0, '127.0.0.1', () => { const p = s.address().port; s.close(() => res(p)); });
});

async function waitHealthy(url) {
  // Replay grows with the log: a debug build replays ~10k records/s, so allow 5 minutes.
  for (let i = 0; i < 6000; i++) {
    try { if ((await fetch(`${url}/health`)).ok) return; } catch {}
    await new Promise(r => setTimeout(r, 50));
  }
  throw new Error(`ADB did not become healthy at ${url}`);
}

async function query(url, aql) {
  const r = await fetch(`${url}/query`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ query: aql }),
  });
  return r.json();
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

/** Every bid_id stored so far, as ADB sees it now. */
async function present(url) {
  const r = await query(url, 'RECALL FROM EPISODIC WHERE kind = "crash" LIMIT 1000000 RETURN bid_id');
  if (!r.success) throw new Error(`recall failed: ${JSON.stringify(r)}`);
  // ClawDB wire format: records sit at data.data.records
  return new Set((r.data?.data?.records ?? []).map(rec => rec.data?.bid_id));
}

const acked = new Set();
let lost = 0, totalAcked = 0, unackedButPresent = 0;
const t0 = Date.now();

for (let round = 1; round <= rounds; round++) {
  const adb = await start();

  // Everything acknowledged in earlier rounds must have survived the last kill.
  const have = await present(adb.url);
  const missing = [...acked].filter(k => !have.has(k));
  lost += missing.length;
  if (missing.length) console.log(`✗ round ${round}: ${missing.length} acknowledged writes lost, e.g. ${missing.slice(0, 3)}`);
  unackedButPresent = have.size - acked.size;

  // Concurrent writers until the kill.
  let stop = false;
  const tasks = Array.from({ length: writers }, async (_, w) => {
    for (let i = 0; !stop; i++) {
      const key = `r${round}-w${w}-${i}`;
      try {
        const r = await query(adb.url, `STORE INTO EPISODIC (bid_id = "${key}", kind = "crash", round = ${round}, writer = ${w})`);
        if (r.success && !stop) { acked.add(key); totalAcked++; }
      } catch { return; } // connection reset by the kill
    }
  });
  await new Promise(r => setTimeout(r, 100 + Math.random() * 500));
  adb.child.kill('SIGKILL');
  stop = true;
  const { signal } = await adb.exited;
  await Promise.all(tasks);
  if (signal !== 'SIGKILL') console.log(`! round ${round}: ADB exited with ${signal}`);
  if (round % 10 === 0 || round === rounds) {
    console.log(`round ${round}/${rounds}: ${acked.size} acknowledged so far, lost ${lost}`);
  }
}

// Final check after the last kill.
const adb = await start();
const have = await present(adb.url);
const missing = [...acked].filter(k => !have.has(k));
lost += missing.length;
adb.child.kill('SIGTERM');
await adb.exited;

console.log(`\n${rounds} kills, ${acked.size} acknowledged writes, ${lost} lost, ` +
  `${have.size - acked.size} unacknowledged writes also survived (allowed), ${((Date.now() - t0) / 1000).toFixed(1)} s`);
if (!args['data-dir']) rmSync(dataDir, { recursive: true, force: true });
if (lost) { console.log('✗ crash durability FAILED'); process.exit(1); }
console.log('✓ crash durability passed: no acknowledged write lost');
