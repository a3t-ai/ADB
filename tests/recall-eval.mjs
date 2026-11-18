#!/usr/bin/env node
/**
 * MVP W4 gate: relevance recall quality, and the embedding backfill.
 *
 * 1. ADB starts with embeddings OFF and the corpus (tests/recall-eval/corpus.json,
 *    93 synthetic facts over six topics) is stored: no vectors.
 * 2. ADB restarts with the model: the backfill embeds every fact (checked).
 * 3. The labelled queries (queries.json: paraphrases, and code / name lookups)
 *    run as RECALL FROM SEMANTIC LIKE $q LIMIT 5, in three ranking modes: hybrid
 *    (the default), vector only, text only (ADB_RECALL_MODE). Reports recall@1,
 *    recall@5 and MRR; the gate wants hybrid recall@5 ≥ 0.90, MRR ≥ 0.75, and
 *    hybrid not worse than either list alone.
 *
 *   node tests/recall-eval.mjs --adb <adb bin> [--model <dir>]
 */
import { spawn } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:net';

const args = Object.fromEntries(process.argv.slice(2).reduce((acc, a, i, all) => {
  if (a.startsWith('--')) acc.push([a.slice(2), all[i + 1]]);
  return acc;
}, []));
const model = args.model ?? process.env.ADB_EMBED_MODEL_DIR;
if (!args.adb || !model) {
  console.error('usage: recall-eval.mjs --adb <adb bin> --model <model dir> (or ADB_EMBED_MODEL_DIR)');
  process.exit(2);
}
const here = new URL('./recall-eval/', import.meta.url).pathname;
const corpus = JSON.parse(readFileSync(join(here, 'corpus.json'), 'utf8'));
const queries = JSON.parse(readFileSync(join(here, 'queries.json'), 'utf8'));
const tmp = mkdtempSync(join(tmpdir(), 'adb-recall-'));
let failures = 0;
const check = (label, ok, detail = '') => { console.log(`${ok ? '✓' : '✗'} ${label}${ok || !detail ? '' : `  — ${detail}`}`); if (!ok) failures++; };
const sleep = ms => new Promise(r => setTimeout(r, ms));
const freePort = () => new Promise(res => { const s = createServer().listen(0, '127.0.0.1', () => { const p = s.address().port; s.close(() => res(p)); }); });

async function start(env) {
  const port = await freePort();
  const url = `http://127.0.0.1:${port}`;
  const child = spawn(args.adb, ['--data-dir', join(tmp, 'adb'), 'serve-http', '--port', String(port), '--flight-port', '0'],
    { env: { ...process.env, RUST_LOG: 'warn', ADB_EMBED_MODEL_DIR: model, ...env }, stdio: ['ignore', 'ignore', 'pipe'] });
  let log = '';
  child.stderr.on('data', d => { log = (log + d).slice(-4000); });
  child.exited = new Promise(r => child.on('exit', r));
  for (let i = 0; i < 300; i++) { try { if ((await fetch(`${url}/health`)).ok) return { url, child }; } catch {} await sleep(100); }
  throw new Error(`ADB did not start: ${log}`);
}
async function query(url, q, variables) {
  const r = await (await fetch(`${url}/query`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ query: q, variables }) })).json();
  if (!r.success) throw new Error(`${q}: ${JSON.stringify(r)}`);
  return r.data.data;
}
const esc = s => s.replace(/\\/g, '\\\\').replace(/"/g, '\\"');

async function evaluate(mode) {
  const a = await start(mode ? { ADB_RECALL_MODE: mode } : {});
  let r1 = 0, r5 = 0, mrr = 0;
  const misses = [];
  for (const { query: text, relevant } of queries) {
    const got = (await query(a.url, 'RECALL FROM SEMANTIC LIKE $q LIMIT 5', { q: text })).records.map(r => r.id);
    const rank = got.findIndex(k => relevant.includes(k));
    if (rank === 0) r1++;
    if (rank >= 0) { r5++; mrr += 1 / (rank + 1); } else misses.push(`${text} → ${got.slice(0, 3).join(', ')}`);
  }
  a.child.kill(); await a.child.exited;
  const n = queries.length;
  return { mode: mode ?? 'hybrid', r1: r1 / n, r5: r5 / n, mrr: mrr / n, misses };
}

try {
  // 1. No model: stored without vectors.
  let a = await start({ ADB_EMBED: 'off' });
  for (const { key, content } of corpus) await query(a.url, `STORE INTO SEMANTIC (key = "${esc(key)}", content = "${esc(content)}")`);
  const plain = await fetch(`${a.url}/query`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ query: 'RECALL FROM SEMANTIC LIKE $q LIMIT 1', variables: { q: 'acme' } }) }).then(r => r.json());
  check('without the model, LIKE with text still ranks by words', plain.success && plain.data.data.records?.[0]?.id?.startsWith('acme'), JSON.stringify(plain).slice(0, 200));
  a.child.kill(); await a.child.exited;

  // 2. With the model: the backfill embeds everything.
  a = await start({});
  let embedded = 0;
  for (let i = 0; i < 120 && embedded < corpus.length; i++) {
    const r = await query(a.url, 'RECALL FROM SEMANTIC LIKE $q CONFIDENCE 0.01 LIMIT 1000', { q: 'company' });
    embedded = r.records.length;
    if (embedded < corpus.length) await sleep(1000);
  }
  check(`backfill embedded all ${corpus.length} facts`, embedded === corpus.length, `${embedded}`);
  a.child.kill(); await a.child.exited;

  // 3. Quality, per mode.
  const results = [];
  for (const mode of [null, 'vector', 'text']) results.push(await evaluate(mode));
  console.log('\n  mode     recall@1  recall@5  MRR');
  for (const r of results) console.log(`  ${r.mode.padEnd(8)} ${r.r1.toFixed(2).padStart(8)}  ${r.r5.toFixed(2).padStart(8)}  ${r.mrr.toFixed(2)}`);
  const [h, v, t] = results;
  if (h.misses.length) console.log('\n  hybrid misses:\n' + h.misses.map(m => `    ${m}`).join('\n'));
  console.log();
  check('hybrid recall@5 ≥ 0.90', h.r5 >= 0.9, h.r5.toFixed(2));
  check('hybrid MRR ≥ 0.75', h.mrr >= 0.75, h.mrr.toFixed(2));
  check('hybrid MRR not worse than vector or text alone', h.mrr >= Math.max(v.mrr, t.mrr) - 0.01, `${h.mrr.toFixed(2)} vs ${v.mrr.toFixed(2)} / ${t.mrr.toFixed(2)}`);
} catch (e) {
  check('session', false, e.stack);
} finally {
  rmSync(tmp, { recursive: true, force: true });
}
console.log(failures === 0 ? '\n✓ recall eval passed' : `\n✗ ${failures} check(s) failed`);
process.exit(failures === 0 ? 0 : 1);
