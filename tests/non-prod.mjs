#!/usr/bin/env node
/**
 * D4 end to end: prod ADB, one non-prod ADB pod, and a FlowR told about the pod. A branch made
 * through FlowR lands on the pod; a run on it writes there directly; prod's log doesn't move
 * and prod's main doesn't change; prod still reads the branch through forwarding.
 *
 *   node tests/non-prod.mjs --adb <adb bin> --flowr <flowr-ce bin (adb-platform)>
 *
 * AQL v0.13: the `BRANCH "b"` statement modifier this script uses is removed; branches
 * become namespaces (BRANCH / FORK / MERGE, see .ai/BRANCH_NAMESPACES.md). The branch
 * steps fail until the script is reworked for that; the rest is v0.13 AQL.
 */
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer as netServer } from 'node:net';

const args = Object.fromEntries(process.argv.slice(2).reduce((a, v, i, all) => (v.startsWith('--') ? [...a, [v.slice(2), all[i + 1]]] : a), []));
const AUTH = `auth-${process.pid}`, NP = `np-${process.pid}`;
const sleep = ms => new Promise(r => setTimeout(r, ms));
const freePort = () => new Promise(res => { const s = netServer().listen(0, () => { const p = s.address().port; s.close(() => res(p)); }); });
let failures = 0;
const check = (label, ok, detail = '') => { console.log(`${ok ? '✓' : '✗'} ${label}${ok || !detail ? '' : `  — ${detail}`}`); if (!ok) failures++; };
const tmp = mkdtempSync(join(tmpdir(), 'non-prod-'));
const children = new Set();
function start(bin, argv, env) {
  const c = spawn(bin, argv, { env: { ...process.env, RUST_LOG: 'warn', ...env }, stdio: ['ignore', 'ignore', 'pipe'] });
  let log = '';
  c.stderr.on('data', d => { log = (log + d).slice(-8000); });
  c.log = () => log;
  c.on('exit', () => children.delete(c));
  children.add(c);
  return c;
}
async function healthy(url, who, c) {
  for (let i = 0; i < 300; i++) {
    try { if ((await fetch(`${url}/health`)).ok) return; } catch {}
    await sleep(100);
  }
  throw new Error(`${who} not healthy: ${c.log()}`);
}

try {
  const [pHttp, pFlight, nHttp, nFlight, fPort] = await Promise.all([freePort(), freePort(), freePort(), freePort(), freePort()]);
  const prodUrl = `http://127.0.0.1:${pHttp}`, prodFlight = `http://127.0.0.1:${pFlight}`;
  const podUrl = `http://127.0.0.1:${nHttp}`, podFlight = `http://127.0.0.1:${nFlight}`;
  const scoped = JSON.stringify([{ name: 'non-prod', token: NP, read: ['*'] }]);
  const prod = start(args.adb, ['--data-dir', join(tmp, 'prod'), 'serve-http', '--port', String(pHttp), '--flight-port', String(pFlight)], { ADB_AUTH_TOKEN: AUTH, ADB_SCOPED_TOKENS: scoped, ADB_EMBED: 'off' });
  await healthy(prodUrl, 'prod ADB', prod);
  const pod = start(args.adb, ['--data-dir', join(tmp, 'pod'), 'serve-http', '--port', String(nHttp), '--flight-port', String(nFlight)], { ADB_AUTH_TOKEN: AUTH, ADB_NON_PROD_OF: prodFlight, ADB_NON_PROD_URL: podFlight, ADB_NON_PROD_TOKEN: NP, ADB_EMBED: 'off' });
  await healthy(podUrl, 'non-prod ADB', pod);
  const url = `http://127.0.0.1:${fPort}`;
  const flowr = start(args.flowr, ['--mode', 'http', '--port', String(fPort)], {
    CLAWDFLOW_DATA_DIR: join(tmp, 'flowr'), FLOWR_REGISTRY_ADB: prodUrl, FLOWR_REGISTRY_ADB_FLIGHT: prodFlight,
    ADB_AUTH_TOKEN: AUTH, ADB_AUTH_URLS: `${prodUrl},${prodFlight},${podUrl},${podFlight}`,
    A3T_NONPROD_ADB_RESOURCE: 'a3t/agent-memory-nonprod', A3T_NONPROD_ADB_FLIGHT: podFlight,
  });
  await healthy(url, 'FlowR', flowr);

  let rpc = 1;
  const tool = async (name, a) => {
    const r = await (await fetch(`${url}/message`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: rpc++, method: 'tools/call', params: { name, arguments: a } }) })).json();
    const text = r.result?.content?.[0]?.text ?? '';
    if (r.error || r.result?.isError) throw new Error(`${name}: ${text || JSON.stringify(r.error)}`);
    try { return JSON.parse(text); } catch { return text; }
  };
  const api = async (method, path, body) => {
    const r = await fetch(`${url}${path}`, { method, headers: { 'content-type': 'application/json' }, body: body === undefined ? undefined : JSON.stringify(body) });
    const t = await r.text();
    let j; try { j = JSON.parse(t); } catch { j = t; }
    return { status: r.status, body: j };
  };
  const aql = async (base, query) => {
    const r = await (await fetch(`${base}/query`, { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${AUTH}` }, body: JSON.stringify({ query }) })).json();
    return r;
  };
  const text = async (base, q) => JSON.stringify(await aql(base, q));

  await tool('create_resource', { namespace: 'a3t', name: 'a3t-platform-db', type_ref: 'platform-db', config: { backend: 'adb', url: prodUrl, flight_url: prodFlight } });
  await tool('create_resource', { namespace: 'a3t', name: 'agent-memory', type_ref: 'adb', config: { url: prodUrl, flight_url: prodFlight } });
  await tool('create_resource', { namespace: 'a3t', name: 'agent-memory-nonprod', type_ref: 'adb', config: { url: podUrl, flight_url: podFlight } });
  await tool('create_flow', { yaml: `document: { dsl: '1.0.3', namespace: t, name: seed, version: '1.0.0' }
do:
  - insert:
      call: lancedb
      with:
        operation: insert
        table: agent_configs
        records:
          - { id: bot, org_id: org-default, name: Bot, template_id: tpl-1, template_version: '1.0.0', namespace: acme, mode: reactive, variables: '{}', resource_bindings: '{}', status: active, created_by: usr-1, created_at: '2026-11-05T00:00:00Z' }
` });
  await tool('execute_flow', { namespace: 't', name: 'seed' });
  await api('PUT', '/agents/bot/memory', {});
  await aql(prodUrl, 'STORE INTO SEMANTIC (key = "parent", fact = "from-prod") NAMESPACE "acme--bot"');

  let r = await api('POST', '/agents/bot/branches', { name: 'dev' });
  check('a new branch lands on the pod', r.status === 201 && r.body.placement === podFlight, JSON.stringify(r));

  await tool('create_flow', { yaml: `document: { dsl: '1.0.3', namespace: t, name: writer, version: '1.0.0' }
do:
  - store:
      call: adb
      with:
        _resource: a3t/agent-memory
        query: 'STORE INTO SEMANTIC (key = "on-branch", fact = "from-the-run") NAMESPACE "acme--bot"'
` });
  await tool('execute_flow', { namespace: 't', name: 'writer', memory_branch: 'dev' });
  let onPod = '';
  for (let i = 0; i < 100 && !onPod.includes('from-the-run'); i++) { onPod = await text(podUrl, 'RECALL FROM SEMANTIC NAMESPACE "acme--bot" BRANCH "dev"'); await sleep(100); }
  check('the run writes on the pod', onPod.includes('from-the-run'), onPod);
  check('and reads its parent from prod', onPod.includes('from-prod'), onPod);
  check('prod\'s main is unchanged', !(await text(prodUrl, 'RECALL FROM SEMANTIC NAMESPACE "acme--bot"')).includes('from-the-run'));
  check('prod still reads the branch by forwarding', (await text(prodUrl, 'RECALL FROM SEMANTIC NAMESPACE "acme--bot" BRANCH "dev"')).includes('from-the-run'));
  check('the pod has no main', (await aql(podUrl, 'RECALL FROM SEMANTIC NAMESPACE "acme--bot"')).success === false);
  r = await api('POST', '/agents/bot/branches/dev/promote', { new_agent: { id: 'bot2' }, allow_stale: true });
  check('promote reads the branch from the pod', r.status === 201 && (await text(prodUrl, 'RECALL FROM SEMANTIC NAMESPACE "acme--bot2"')).includes('from-the-run'), JSON.stringify(r));
} catch (e) {
  console.error(e);
  failures++;
} finally {
  for (const c of children) c.kill('SIGKILL');
  rmSync(tmp, { recursive: true, force: true });
}
console.log(failures ? `${failures} failed` : 'all passed');
process.exit(failures ? 1 : 0);
