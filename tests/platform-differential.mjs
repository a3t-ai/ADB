#!/usr/bin/env node
/**
 * Stage G gate: A3T's platform flows give the same answers on ADB platform tables as
 * on the embedded LanceDB.
 *
 * Starts one ADB and two FlowR (CE built with `--features adb-platform`):
 *   L — `a3t/a3t-platform-db` = embedded LanceDB, a copy of --lancedb
 *   A — `a3t/a3t-platform-db` = `backend: adb`, after `adb import-lancedb` of the same copy
 * loads A3T's own CRUD flows (a3t/backend/flows/*_crud.yaml) into both, and runs the
 * same calls on each: every list / get, then a template create → duplicate check →
 * update → delete cycle, a settings write, and a delete the integrity check refuses
 * (a template with agents). Every answer must match; then ADB is SIGKILLed and
 * restarted and A's answers must still match.
 *
 *   node tests/platform-differential.mjs --adb <adb> --flowr <flowr-ce with adb-platform> \
 *        --lancedb <a3t-platform.lance> --flows <flowr-system/a3t/backend/flows>
 */
import { spawn, execFileSync } from 'node:child_process';
import { cpSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:net';

const args = Object.fromEntries(process.argv.slice(2).reduce((acc, a, i, all) => {
  if (a.startsWith('--')) acc.push([a.slice(2), all[i + 1]]);
  return acc;
}, []));
for (const k of ['adb', 'flowr', 'lancedb', 'flows']) {
  if (!args[k]) { console.error('usage: platform-differential.mjs --adb <bin> --flowr <bin> --lancedb <dir> --flows <dir>'); process.exit(2); }
}

const tmp = mkdtempSync(join(tmpdir(), 'adb-platform-'));
const children = new Set();
let failures = 0, checks = 0;
const check = (label, ok, detail = '') => { checks++; if (!ok) { console.log(`✗ ${label}  — ${detail}`); failures++; } };
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
function start(bin, argv, env) {
  const child = spawn(bin, argv, { env: { ...process.env, RUST_LOG: 'warn', ...env }, stdio: ['ignore', 'ignore', 'pipe'] });
  let log = '';
  child.stderr.on('data', d => { log = (log + d).slice(-6000); });
  child.exited = new Promise(res => child.on('exit', (code, signal) => { children.delete(child); res({ code, signal, log }); }));
  child.log = () => log;
  children.add(child);
  return child;
}

let rpcId = 1;
async function tool(url, name, toolArgs) {
  const r = await fetch(`${url}/message`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: rpcId++, method: 'tools/call', params: { name, arguments: toolArgs } }),
  });
  const body = await r.json();
  if (body.error) throw new Error(`${name}: ${JSON.stringify(body.error)}`);
  return { isError: !!body.result?.isError, text: body.result?.content?.[0]?.text ?? '' };
}
function output(text) {
  const i = text.indexOf('Output:\n');
  if (i < 0) return { error: text.split('\n').find(l => /error|fail|refus|used by/i.test(l)) ?? text.slice(0, 200) };
  return JSON.parse(text.slice(i + 8));
}
/** Rows as a comparable value: sorted, timestamps the same on both sides anyway. */
const canon = v => Array.isArray(v) ? JSON.stringify([...v].map(r => JSON.stringify(r, Object.keys(r).sort())).sort()) : JSON.stringify(v);

async function flowrWith(name, resourceConfig) {
  const port = await freePort();
  const url = `http://127.0.0.1:${port}`;
  const child = start(args.flowr, ['--mode', 'http', '--port', String(port)], { CLAWDFLOW_DATA_DIR: join(tmp, `flowr-${name}`) });
  await waitHealthy(url, `FlowR ${name}`);
  const r = await tool(url, 'create_resource', { namespace: 'a3t', name: 'a3t-platform-db', type_ref: 'lancedb', config: resourceConfig });
  if (r.isError) throw new Error(`resource on ${name}: ${r.text}`);
  for (const f of ['templates', 'agents', 'canvas', 'users', 'settings', 'resources', 'audit']) {
    const c = await tool(url, 'create_flow', { yaml: readFileSync(join(args.flows, `${f}_crud.yaml`), 'utf8') });
    if (c.isError) throw new Error(`flow ${f} on ${name}: ${c.text}`);
  }
  return { name, url, child };
}

const run = (f, flow, input) => tool(f.url, 'execute_flow', { namespace: 'a3t', name: flow, input: { input } });

try {
  // One LanceDB copy each side starts from.
  cpSync(args.lancedb, join(tmp, 'lance-L'), { recursive: true });
  cpSync(args.lancedb, join(tmp, 'lance-import'), { recursive: true });
  const adbPort = await freePort(), flightPort = await freePort();
  const adbUrl = `http://127.0.0.1:${adbPort}`, flightUrl = `http://127.0.0.1:${flightPort}`;
  const adbArgv = ['--data-dir', join(tmp, 'adb'), 'serve-http', '--port', String(adbPort), '--flight-port', String(flightPort)];
  let adb = start(args.adb, adbArgv, {});
  await waitHealthy(adbUrl, 'ADB');
  console.log(execFileSync(args.adb, ['import-lancedb', join(tmp, 'lance-import'), '--flight', flightUrl]).toString().trim().split('\n').pop());

  const L = await flowrWith('L', { db_path: join(tmp, 'lance-L') });
  const A = await flowrWith('A', { backend: 'adb', url: adbUrl, flight_url: flightUrl });

  const both = async (label, flow, input, pick = x => x) => {
    const [l, a] = await Promise.all([run(L, flow, input), run(A, flow, input)]);
    const lo = pick(output(l.text)), ao = pick(output(a.text));
    check(label, canon(lo) === canon(ao), `LanceDB ${canon(lo).slice(0, 300)}\n     ADB     ${canon(ao).slice(0, 300)}`);
    return { l: lo, a: ao, lErr: l.isError, aErr: a.isError };
  };

  const ORG = 'org-default';
  const ts = '2026-10-26T09:00:00Z';
  const reads = async (when) => {
    for (const [flow, idField] of [['a3t-templates', 'id'], ['a3t-agents', 'id'], ['a3t-canvas', 'id'], ['a3t-users', 'id'], ['a3t-settings', 'key'], ['a3t-resources', 'id'], ['a3t-audit', 'id']]) {
      const r = await both(`${when}: ${flow} list`, flow, { action: 'list', org_id: ORG });
      const first = Array.isArray(r.l) ? r.l[0] : null;
      if (first?.[idField]) await both(`${when}: ${flow} get`, flow, { action: 'get', org_id: ORG, id: first[idField], key: first[idField] });
    }
  };
  await reads('imported data');
  const imported = (await both('imported templates', 'a3t-templates', { action: 'list', org_id: ORG })).a;
  check('the imported data is really there (ADB lists templates)', Array.isArray(imported) && imported.length > 0, JSON.stringify(imported).slice(0, 200));

  // A template's life: create, a duplicate refused, update, read, delete.
  const tpl = { id: 'tpl-diff-1', name: "Diff's template", version: '1.0.0', source: 'my', spec_yaml: 'document: {}', tags: 'a,b', vertical: 'test' };
  await both('create template', 'a3t-templates', { action: 'create', org_id: ORG, user_id: 'usr-test', timestamp: ts, data: tpl });
  await both('duplicate template refused', 'a3t-templates', { action: 'create', org_id: ORG, user_id: 'usr-test', timestamp: ts, data: tpl });
  await both('update template', 'a3t-templates', { action: 'update', org_id: ORG, id: tpl.id, timestamp: ts, data: { ...tpl, name: 'Renamed', published: 'true' } });
  const got = (await both('get updated template', 'a3t-templates', { action: 'get', org_id: ORG, id: tpl.id })).a;
  check('the update landed (on ADB)', Array.isArray(got) && got[0]?.name === 'Renamed' && got[0]?.published === 'true', JSON.stringify(got).slice(0, 300));
  await both('list after update', 'a3t-templates', { action: 'list', org_id: ORG });
  // Integrity: a template that agents use can't be deleted (if the data has one).
  const agents = (await both('agents list', 'a3t-agents', { action: 'list', org_id: ORG })).l;
  const used = Array.isArray(agents) ? agents.find(a => a.template_id)?.template_id : undefined;
  console.log(`  ${imported.length} templates, ${Array.isArray(agents) ? agents.length : 0} agents; a used template: ${used ?? 'none'}`);
  if (used) {
    const r = await both('delete a used template is refused', 'a3t-templates', { action: 'delete', org_id: ORG, id: used });
    check('…and refused on both', r.lErr && r.aErr, `LanceDB error=${r.lErr} ADB error=${r.aErr}`);
  }
  await both('delete template', 'a3t-templates', { action: 'delete', org_id: ORG, id: tpl.id });
  const after = (await both('list after delete', 'a3t-templates', { action: 'list', org_id: ORG })).a;
  check('the delete landed (on ADB)', Array.isArray(after) && !after.some(t => t.id === tpl.id) && after.length === imported.length, `${after?.length} vs ${imported.length}`);

  // ADB crash: A's answers are still the same.
  adb.kill('SIGKILL');
  await adb.exited;
  adb = start(args.adb, adbArgv, {});
  await waitHealthy(adbUrl, 'ADB after SIGKILL');
  await reads('after an ADB SIGKILL');
  await both('the deleted template stays deleted', 'a3t-templates', { action: 'get', org_id: ORG, id: tpl.id });
} catch (e) {
  check('session', false, e.stack);
} finally {
  for (const c of children) c.kill();
  await sleep(300);
  rmSync(tmp, { recursive: true, force: true });
}
console.log(failures === 0 ? `\n✓ platform differential passed: ${checks} checks, LanceDB and ADB agree` : `\n✗ ${failures} of ${checks} checks failed`);
process.exit(failures === 0 ? 0 : 1);
