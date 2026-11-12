#!/usr/bin/env node
/**
 * Throughput and latency against systems that are already running (AWS, vc-dev):
 * nothing is started here. Closed-loop clients, one phase at a time, each phase run at
 * every concurrency in --c for --secs seconds. Reports ops/s, p50 / p90 / p99, errors.
 *
 *   ADB alone (HTTP /query on the leader; ADB_AUTH_TOKEN from the environment):
 *     node tests/perf-remote.mjs adb --adb http://<leader>:3000 [--c 8,32,64] [--secs 20] [--rows 20000]
 *
 *   The whole system through FlowR (MCP /message; FLOWR_JWT_SECRET from the environment,
 *   the FlowRs' MCP_RESOURCE_URI is their URL): calls spread over every --flowr URL.
 *     node tests/perf-remote.mjs a3t --flowr http://<f1>:3002,http://<f2>:3002 [--c 8,32,64] [--secs 20]
 *
 * Statements are perf-memory.mjs's (memory) and perf-a3t.mjs's (A3T), so numbers compare.
 */
import { createHmac } from 'node:crypto';

const [mode, ...rest] = process.argv.slice(2);
const args = Object.fromEntries(rest.reduce((a, v, i, all) => (v.startsWith('--') ? [...a, [v.slice(2), all[i + 1]]] : a), []));
const CS = String(args.c ?? '8,32,64').split(',').map(Number);
const SECS = Number(args.secs ?? 20);
const ROWS = Number(args.rows ?? 20000);
// Several client processes (`--procs P --worker k`, started by perf-multi.mjs): each
// takes its share of every phase's concurrency, with its own keys.
const PROCS = Math.max(1, Number(args.procs ?? 1));
const WORKER = Number(args.worker ?? 0);
const run = Date.now().toString(36) + (PROCS > 1 ? `w${WORKER}` : '');
const share = c => Math.floor(c / PROCS) + (WORKER < c % PROCS ? 1 : 0);

const pct = (v, p) => v.length ? v[Math.min(v.length - 1, Math.floor(v.length * p))] : 0;
const results = [];

const ONLY = args.phases ? new RegExp(args.phases, 'i') : null;
/** Server-side metrics before and after each phase: `--metrics url` (+ METRICS_TOKEN), a
 * histogram family and its label to group by. The phase's server mean is the delta. */
let serverMetrics = null;
async function snapshot() {
  if (!serverMetrics) return null;
  const get = async u => { try { return await (await fetch(u, { headers: serverMetrics.token ? { authorization: `Bearer ${serverMetrics.token}` } : {} })).json(); } catch { return null; } };
  if (!serverMetrics.urls) return get(serverMetrics.url);
  // Every node's (reads spread over followers): counts and sums added up per label set.
  const fam = serverMetrics.family;
  const merged = new Map();
  for (const s of await Promise.all(serverMetrics.urls.map(get))) {
    for (const x of s?.[fam] ?? []) {
      const k = JSON.stringify(x.labels);
      const m = merged.get(k) ?? { labels: x.labels, value: { count: 0, sum: 0 } };
      m.value.count += x.value.count; m.value.sum += x.value.sum;
      merged.set(k, m);
    }
  }
  return { [fam]: [...merged.values()] };
}
function serverDelta(before, after) {
  if (!before || !after) return '';
  const fam = serverMetrics.family;
  const sum = s => Object.fromEntries((s?.[fam] ?? []).map(x => [JSON.stringify(x.labels), x.value]));
  const a = sum(before), b = sum(after);
  const parts = [];
  for (const [k, v] of Object.entries(b)) {
    const n = v.count - (a[k]?.count ?? 0), t = v.sum - (a[k]?.sum ?? 0);
    if (n > 0) parts.push(`${Object.values(JSON.parse(k)).join('/')} ${(t / n * 1000).toFixed(1)}ms×${n}`);
  }
  return parts.length ? `  server: ${parts.join(', ')}` : '';
}

/** `c` closed-loop clients calling `op(i)` for SECS seconds. */
async function phase(name, c, op) {
  if (ONLY && !ONLY.test(name)) return { name, c, ops: 0, skipped: true };
  const total = c;
  c = share(total);
  if (c === 0) {
    // No share of this phase: wait it out, so the workers stay in step.
    await new Promise(r => setTimeout(r, SECS * 1000));
    const r = { name, c: total, ops: 0, p50: 0, p90: 0, p99: 0, errors: 0, idle: true, server: '' };
    results.push(r);
    return r;
  }
  const before = await snapshot();
  const lat = [];
  let errors = 0, firstError = null, i = 0, ok = 0;
  const end = Date.now() + SECS * 1000;
  const t0 = Date.now();
  await Promise.all(Array.from({ length: c }, async () => {
    while (Date.now() < end) {
      const n = i++;
      const s = performance.now();
      try { await op(n); ok++; if (lat.length < 200000) lat.push(performance.now() - s); } catch (e) { errors++; firstError ??= String(e).slice(0, 160); }
    }
  }));
  const secs = (Date.now() - t0) / 1000;
  lat.sort((a, b) => a - b);
  const r = { name, c: total, ops: Math.round(ok / secs), p50: pct(lat, 0.5), p90: pct(lat, 0.9), p99: pct(lat, 0.99), errors };
  r.server = serverDelta(before, await snapshot());
  results.push(r);
  console.log(`  ${name.padEnd(38)} c=${String(c).padEnd(3)} ${String(r.ops).padStart(6)}/s  p50 ${r.p50.toFixed(1).padStart(6)} ms  p90 ${r.p90.toFixed(1).padStart(6)}  p99 ${r.p99.toFixed(1).padStart(7)}${errors ? `  errors ${errors} (${firstError})` : ''}${r.server}`);
  return r;
}

async function adb() {
  // --adb <url>, or --nodes <url,url,url>: then the current leader is found (each node's
  // /ready), and found again when it changes; every change is reported.
  const nodes = String(args.nodes ?? args.adb ?? '').split(',').filter(Boolean).map(u => u.replace(/\/$/, ''));
  const token = process.env.ADB_AUTH_TOKEN;
  if (!nodes.length) throw new Error('--adb <leader url> or --nodes <urls> needed');
  let url = nodes[0];
  let finding = null;
  const findLeader = () => finding ??= (async () => {
    const t0 = Date.now();
    for (;;) {
      for (const n of nodes) { try { if ((await fetch(`${n}/ready`, { signal: AbortSignal.timeout(500) })).ok) { if (n !== url) console.log(`  ! leader now ${n} (found after ${Date.now() - t0} ms)`); url = n; finding = null; return; } } catch {} }
      await new Promise(r => setTimeout(r, 100));
    }
  })();
  if (nodes.length > 1) await findLeader();
  // C-16: reads (RECALL / LOOKUP / SCAN) go to every node in turn — followers answer
  // them — and writes to the leader; `--spread 0` sends everything to the leader.
  const spread = nodes.length > 1 && args.spread !== '0';
  let turn = 0;
  const isRead = aql => /^\s*(RECALL|LOOKUP|SCAN)\b/i.test(aql);
  if (spread) console.log(`reads spread over ${nodes.length} nodes; writes to the leader`);
  const q = async aql => {
    let r;
    const target = spread && isRead(aql) ? nodes[turn++ % nodes.length] : url;
    try {
      r = await fetch(`${target}/query`, { method: 'POST', headers: { 'content-type': 'application/json', ...(token ? { authorization: `Bearer ${token}` } : {}) }, body: JSON.stringify({ query: aql }), signal: AbortSignal.timeout(30000) });
    } catch (e) { if (nodes.length > 1) await findLeader(); throw e; }
    const j = await r.json();
    if (!j.success) {
      if (nodes.length > 1 && /not the leader/.test(j.error ?? '')) await findLeader();
      throw new Error(j.error ?? r.status);
    }
    return j.data;
  };
  console.log(`ADB ${url}, run ${run}, ${SECS} s per phase, concurrency ${CS.join(', ')}`);
  // The leader's own timing per statement (looked up afresh each phase: the leader can move).
  serverMetrics = { get url() { return `${url}/admin/stats/metrics`; }, urls: spread ? nodes.map(n => `${n}/admin/stats/metrics`) : null, token, family: 'adb_query_duration_seconds' };
  // Writes first: each phase stores under its own prefix; reads and updates use the first
  // STORE SEMANTIC phase's keys, counted as they succeed.
  let keys = 0, ekeys = 0, wkeys = 0;
  const episodic = []; // every stored EPISODIC key: FORGET takes each once
  for (const c of CS) {
    await phase('STORE SEMANTIC', c, async i => {
      const key = c === CS[0] ? `${run}f${i}` : `${run}s${c}_${i}`;
      await q(`STORE INTO SEMANTIC (key = "${key}", topic = "t${i % 20}", n = ${i % 997}, v = 1, content = "fact number ${i} about topic t${i % 20}")`);
      if (c === CS[0]) keys = Math.max(keys, i + 1);
    });
  }
  for (const c of CS) {
    await phase('STORE EPISODIC', c, async i => {
      const key = c === CS[0] ? `${run}e${i}` : `${run}e${c}_${i}`;
      await q(`STORE INTO EPISODIC (key = "${key}", kind = "event", n = ${i}, content = "event ${i} happened")`);
      episodic.push(key);
      if (c === CS[0]) ekeys = Math.max(ekeys, i + 1);
    });
  }
  for (const c of CS) {
    await phase('STORE WORKING', c, async i => {
      const key = c === CS[0] ? `${run}w${i}` : `${run}w${c}_${i}`;
      await q(`STORE INTO WORKING (key = "${key}", step = 1, priority = ${i % 10})`);
      if (c === CS[0]) wkeys = Math.max(wkeys, i + 1);
    });
  }
  keys = Math.max(1, Math.min(keys, ROWS)); ekeys = Math.max(1, ekeys); wkeys = Math.max(1, wkeys);
  const reads = [
    ['LOOKUP KEY (SEMANTIC)', i => `LOOKUP FROM SEMANTIC KEY id = "${run}f${(i * 37) % keys}"`],
    ['RECALL WHERE topic LIMIT 10', i => `RECALL FROM SEMANTIC WHERE topic = "t${i % 20}" LIMIT 10`],
    ['RECALL ALL ORDER BY n LIMIT 10', () => `RECALL FROM SEMANTIC ALL ORDER BY n DESC LIMIT 10`],
    ['AGGREGATE COUNT by topic', i => `RECALL FROM SEMANTIC WHERE topic = "t${i % 20}" AGGREGATE COUNT(*) AS c`],
    ['RECALL EPISODIC newest 10', () => `RECALL FROM EPISODIC ALL LIMIT 10`],
    ['RECALL EPISODIC KEY', i => `RECALL FROM EPISODIC KEY id = "${run}e${(i * 7) % ekeys}"`],
    ['RECALL WORKING KEY', i => `RECALL FROM WORKING KEY id = "${run}w${(i * 7) % wkeys}"`],
  ];
  for (const [name, f] of reads) for (const c of CS) await phase(name, c, i => q(f(i)));
  for (const c of CS) await phase('UPDATE SEMANTIC (existing key)', c, i => q(`UPDATE INTO SEMANTIC WHERE id = "${run}f${(i * 53) % keys}" (v = 2, content = "fact ${i} updated")`));
  for (const c of CS) await phase('UPDATE WORKING', c, i => q(`UPDATE INTO WORKING WHERE id = "${run}w${(i * 11) % wkeys}" (step = 2)`));
  // FORGET each key once: the c-th phase takes its own slice of the episodic keys.
  // FORGET each stored EPISODIC key once (from the newest; the reads above used the first
  // phase's); a phase that runs out of keys stops counting.
  for (const c of CS) await phase('FORGET EPISODIC (one key each)', c, async () => {
    const k = episodic.pop();
    if (!k) throw new Error('out of keys');
    await q(`FORGET FROM EPISODIC WHERE id = "${k}"`);
  });
  for (const c of CS) {
    await phase('mixed 70% read / 25% STORE / 5% UPDATE', c, i => {
      const k = i % 20;
      if (k < 14) return q(k % 2 ? `LOOKUP FROM SEMANTIC KEY id = "${run}f${(i * 97) % keys}"` : `RECALL FROM SEMANTIC WHERE topic = "t${i % 20}" LIMIT 10`);
      if (k < 19) return q(`STORE INTO SEMANTIC (key = "${run}m${c}x${i}", topic = "t${i % 20}", n = 1, content = "mixed ${i}")`);
      return q(`UPDATE INTO SEMANTIC WHERE id = "${run}f${(i * 31) % keys}" (v = 3)`);
    });
  }
}

async function a3t() {
  const urls = String(args.flowr ?? '').split(',').filter(Boolean).map(u => u.replace(/\/$/, ''));
  const secret = process.env.FLOWR_JWT_SECRET;
  // A3T_TOKEN_FILE: a token already issued (e.g. a superadmin login) instead of minting one with the secret.
  const given = process.env.A3T_TOKEN_FILE ? (await import('node:fs')).readFileSync(process.env.A3T_TOKEN_FILE, 'utf8').trim() : null;
  if (!urls.length || !(secret || given)) throw new Error('--flowr <url,url> and FLOWR_JWT_SECRET (or A3T_TOKEN_FILE) needed');
  const b64u = b => Buffer.from(b).toString('base64url');
  const jwt = aud => {
    const now = Math.floor(Date.now() / 1000);
    const h = b64u(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));
    const p = b64u(JSON.stringify({ sub: 'perf', org: 'org-default', role: 'superadmin', email: 'perf@localhost', name: 'perf', aud, iat: now, exp: now + 86400 }));
    return `${h}.${p}.${createHmac('sha256', secret).update(`${h}.${p}`).digest('base64url')}`;
  };
  const bearer = Object.fromEntries(urls.map(u => [u, given ?? jwt(u)]));
  let id = 1;
  const tool = async (u, name, a) => {
    const r = await (await fetch(`${u}/message`, { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${bearer[u]}` }, body: JSON.stringify({ jsonrpc: '2.0', id: id++, method: 'tools/call', params: { name, arguments: a } }) })).json();
    const c = r.result?.content?.[0];
    if (!r.result || r.result.isError) throw new Error(c?.text ?? JSON.stringify(r.error ?? r).slice(0, 200));
    return c?.text;
  };
  const pick = i => urls[i % urls.length];
  console.log(`A3T via ${urls.length} FlowR (${urls.join(', ')}), ${SECS} s per phase, concurrency ${CS.join(', ')}`);
  for (const k of ['a', 'b', 'c']) await tool(urls[0], 'set_state', { key: `bench:${k}`, value: { n: 1 } });
  await tool(urls[0], 'create_flow', { yaml: `document: { dsl: '1.0.3', namespace: bench, name: read3, version: '1.0.0' }
do:
  - a: { call: "state:get", with: { key: "bench:a" } }
  - b: { call: "state:get", with: { key: "bench:b" } }
  - c: { call: "state:get", with: { key: "bench:c" } }
  - out: { set: { ok: true } }
` }).catch(e => { if (!String(e).includes('exist')) throw e; });
  const flow = async (name, meta, body) => tool(urls[0], 'create_flow', { yaml: `document: { dsl: '1.0.3', namespace: bench, name: ${name}, version: '1.0.0' }${meta}\ndo:\n${body}` }).catch(e => { if (!String(e).includes('exist')) throw e; });
  await flow('read3t', `\nmetadata: { a3t: { durability: transient } }`, `  - a: { call: "state:get", with: { key: "bench:a" } }\n  - b: { call: "state:get", with: { key: "bench:b" } }\n  - c: { call: "state:get", with: { key: "bench:c" } }\n  - out: { set: { ok: true } }\n`);
  await flow('noop', '', `  - s: { call: "state:set", with: { key: "bench:noop", value: { n: 1 } } }\n`);
  for (const u of urls) for (const f of ['read3', 'read3t', 'noop']) await tool(u, 'execute_flow', { namespace: 'bench', name: f });
  // FlowR 1's own timing of each tool (calls split evenly, so it stands for both).
  serverMetrics = { url: `${urls[0]}/admin/stats/metrics`, token: bearer[urls[0]], family: 'flowr_tool_call_duration_seconds' };
  for (const c of CS) await phase('get_state', c, i => tool(pick(i), 'get_state', { key: `bench:${'abc'[i % 3]}` }));
  for (const c of CS) await phase('list_flows', c, i => tool(pick(i), 'list_flows', {}));
  for (const c of CS) await phase('set_state', c, i => tool(pick(i), 'set_state', { key: `bench:w${i % 500}`, value: { n: i } }));
  for (const c of CS) await phase('flow noop: one set step (durable)', c, i => tool(pick(i), 'execute_flow', { namespace: 'bench', name: 'noop' }));
  for (const c of CS) await phase('flow read3 (durable)', c, i => tool(pick(i), 'execute_flow', { namespace: 'bench', name: 'read3' }));
  for (const c of CS) await phase('flow read3 (transient)', c, i => tool(pick(i), 'execute_flow', { namespace: 'bench', name: 'read3t' }));
}

try {
  if (mode === 'adb') await adb();
  else if (mode === 'a3t') await a3t();
  else throw new Error('mode: adb | a3t');
  console.log('RESULTS ' + JSON.stringify(results));
} catch (e) {
  console.error(String(e));
  process.exitCode = 1;
}
