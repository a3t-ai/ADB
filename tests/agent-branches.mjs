#!/usr/bin/env node
/**
 * Agents as repos, end to end: FlowR's agent memory endpoints (flowr-ce
 * `agent_space.rs`, flowr-system `.ai/A3T_BRANCHING_API.md` §6) on a real ADB.
 *
 * One ADB (durable, auth token). One FlowR (adb-platform) with its registry and the
 * platform tables on that ADB, and `a3t/agent-memory` = the org's ADB. An agent row
 * in `agent_configs`, then:
 *
 *   - no space yet: GET memory says so (default `<project>--<agent>`), branches → 409;
 *   - PUT memory gives it `acme--bot`;
 *   - branch: create, list, write on the branch (ADB `BRANCH`), main unchanged;
 *   - promote the branch as agent bot2: its memory = the branch view, its row a copy;
 *   - fork bot as bot3: its memory = main; an existing agent id is refused;
 *   - tags and layers (a layer named by agent id → its namespace);
 *   - layers: a raw space must be an agent's, in this org; history layers stay;
 *   - kind 2 merge: conflicts, refusals, resolve, merge back, revert (ADB MERGE.md);
 *   - kind 1: promote refused until rebased, then a new agent with its history layer;
 *   - ADB refusals come back as {error, code} with a fitting status; drop.
 *
 *   node tests/agent-branches.mjs --adb <adb bin> --flowr <flowr-ce bin (adb-platform)>
 */
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer as netServer } from 'node:net';

const args = Object.fromEntries(process.argv.slice(2).reduce((a, v, i, all) => (v.startsWith('--') ? [...a, [v.slice(2), all[i + 1]]] : a), []));
if (!args.adb || !args.flowr) {
  console.error('usage: agent-branches.mjs --adb <adb bin> --flowr <flowr-ce bin>');
  process.exit(2);
}
const AUTH = `auth-${process.pid}`;
const sleep = ms => new Promise(r => setTimeout(r, ms));
const freePort = () => new Promise(res => { const s = netServer().listen(0, () => { const p = s.address().port; s.close(() => res(p)); }); });
let failures = 0;
const check = (label, ok, detail = '') => { console.log(`${ok ? '✓' : '✗'} ${label}${ok || !detail ? '' : `  — ${detail}`}`); if (!ok) failures++; };

const tmp = mkdtempSync(join(tmpdir(), 'agent-branches-'));
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
  const [adbPort, flightPort, flowrPort] = [await freePort(), await freePort(), await freePort()];
  const adbUrl = `http://127.0.0.1:${adbPort}`;
  const flightUrl = `http://127.0.0.1:${flightPort}`;
  const adb = start(args.adb, ['--data-dir', join(tmp, 'adb'), 'serve-http', '--port', String(adbPort), '--flight-port', String(flightPort)], { ADB_AUTH_TOKEN: AUTH });
  await healthy(adbUrl, 'ADB', adb);
  const url = `http://127.0.0.1:${flowrPort}`;
  const flowr = start(args.flowr, ['--mode', 'http', '--port', String(flowrPort)], {
    CLAWDFLOW_DATA_DIR: join(tmp, 'flowr'),
    FLOWR_REGISTRY_ADB: adbUrl,
    FLOWR_REGISTRY_ADB_FLIGHT: flightUrl,
    ADB_AUTH_TOKEN: AUTH,
    ADB_AUTH_URLS: `${adbUrl},${flightUrl}`,
  });
  await healthy(url, 'FlowR', flowr);
  console.log('  ADB + FlowR up');

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
  const aql = async query => {
    const r = await (await fetch(`${adbUrl}/query`, { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${AUTH}` }, body: JSON.stringify({ query }) })).json();
    if (!r.success) throw new Error(`${query}: ${r.error}`);
    return r.data;
  };
  const facts = async ns => {
    const d = await aql(`RECALL FROM SEMANTIC ALL NAMESPACE "${ns}"`);
    return ((d.data ?? d).records ?? []).map(r => r.data?.fact).sort();
  };

  // --- resources: the platform tables and the org's memory on this ADB
  await tool('create_resource', { namespace: 'a3t', name: 'a3t-platform-db', type_ref: 'platform-db', config: { backend: 'adb', url: adbUrl, flight_url: flightUrl } });
  await tool('create_resource', { namespace: 'a3t', name: 'agent-memory', type_ref: 'adb', config: { url: adbUrl, flight_url: flightUrl } });

  // --- an agent row, as the a3t-agents flow writes it
  await tool('create_flow', { yaml: `document: { dsl: '1.0.3', namespace: t, name: seed-agent, version: '1.0.0' }
do:
  - insert:
      call: lancedb
      with:
        operation: insert
        table: agent_configs
        records:
          - { id: bot, org_id: org-default, name: Bot, template_id: tpl-1, template_version: '1.0.0', namespace: acme, mode: reactive, variables: '{}', resource_bindings: '{}', status: active, created_by: usr-1, created_at: '2026-11-05T00:00:00Z' }
          - { id: rival, org_id: org-rival, name: Rival, template_id: tpl-1, template_version: '1.0.0', namespace: rivalco, mode: reactive, variables: '{}', resource_bindings: '{}', status: active, created_by: usr-2, created_at: '2026-11-05T00:00:00Z' }
` });
  await tool('execute_flow', { namespace: 't', name: 'seed-agent' });

  let r = await api('GET', '/agents/bot/memory');
  check('an agent without a space: own false, default acme--bot', r.status === 200 && r.body.own === false && r.body.default_namespace === 'acme--bot', JSON.stringify(r));
  r = await api('GET', '/agents/bot/branches');
  check('branches of an agent without a space → 409 with the default', r.status === 409 && r.body.default_namespace === 'acme--bot', JSON.stringify(r));
  r = await api('GET', '/agents/nobody/memory');
  check('unknown agent → 404', r.status === 404, JSON.stringify(r));

  r = await api('PUT', '/agents/bot/memory', {});
  check('PUT memory: acme--bot', r.status === 200 && r.body.namespace === 'acme--bot' && r.body.resource === 'a3t/agent-memory', JSON.stringify(r));
  r = await api('GET', '/agents/bot/memory');
  check('GET memory after PUT: own', r.body.own === true && r.body.namespace === 'acme--bot', JSON.stringify(r));

  await aql('STORE INTO SEMANTIC (key = "rule:a", fact = "main-a") NAMESPACE "acme--bot"');

  // --- branch
  r = await api('POST', '/agents/bot/branches', { name: 'try' });
  check('create branch → 201 Branch', r.status === 201 && r.body.id === 'br:acme--bot/try' && r.body.parent === 'main', JSON.stringify(r));
  r = await api('GET', '/agents/bot/branches');
  check('list branches', r.status === 200 && r.body.branches?.length === 1 && r.body.branches[0].name === 'try', JSON.stringify(r));
  r = await api('POST', '/agents/bot/branches', { name: 'try' });
  check('a taken name → 409 with ADB\'s sentence and code', r.status === 409 && r.body.code === 'branch_exists' && /already has a branch/.test(r.body.error), JSON.stringify(r));
  r = await api('POST', '/agents/bot/branches', { name: 'bad name' });
  check('a bad name → 400', r.status === 400 && /branch name/.test(r.body.error), JSON.stringify(r));

  await aql('STORE INTO SEMANTIC (key = "rule:b", fact = "branch-b") NAMESPACE "acme--bot" BRANCH "try"');
  check('main does not see the branch write', JSON.stringify(await facts('acme--bot')) === JSON.stringify(['main-a']));

  // --- promote as bot2
  r = await api('POST', '/agents/bot/branches/try/promote', { new_agent: { id: 'bot2', name: 'Bot 2' } });
  check('promote → 201, 2 records, new agent row', r.status === 201 && r.body.memory?.records === 2 && r.body.agent?.id === 'bot2' && r.body.agent?.template_id === 'tpl-1', JSON.stringify(r));
  r = await api('GET', '/agents/bot2/memory');
  check('bot2\'s space: acme--bot2', r.body.own === true && r.body.namespace === 'acme--bot2', JSON.stringify(r));
  check('bot2\'s memory = the branch view', JSON.stringify(await facts('acme--bot2')) === JSON.stringify(['branch-b', 'main-a']));
  check('bot\'s main untouched', JSON.stringify(await facts('acme--bot')) === JSON.stringify(['main-a']));
  r = await api('POST', '/agents/bot/branches/try/promote', { new_agent: { id: 'bot2' } });
  check('promote into an existing agent → 409', r.status === 409, JSON.stringify(r));

  // --- fork as bot3
  r = await api('POST', '/agents/bot/fork', { new_agent: { id: 'bot3' } });
  check('fork → 201, 1 record, default name', r.status === 201 && r.body.memory?.records === 1 && r.body.agent?.name === 'Bot (copy)', JSON.stringify(r));
  check('bot3\'s memory = bot\'s main', JSON.stringify(await facts('acme--bot3')) === JSON.stringify(['main-a']));

  // --- tags and layers
  r = await api('POST', '/agents/bot/tags', { name: 'v1' });
  check('create tag → 201', r.status === 201 && r.body.id === 'tag:acme--bot/v1', JSON.stringify(r));
  r = await api('GET', '/agents/bot/tags');
  check('list tags: v1, and the tag promote keeps on the old space', r.body.tags?.length === 2 && r.body.tags.some(t => t.name === 'v1') && r.body.tags.some(t => t.name === 'promoted-acme--bot2'), JSON.stringify(r));
  r = await api('PUT', '/agents/bot3/layers', { layers: [{ agent: 'bot', mode: 'pin', tag: 'v1', memories: ['SEMANTIC'], sealed: true }] });
  check('a layer named by agent id → its namespace', r.status === 200 && r.body.layers?.[0]?.space === 'acme--bot', JSON.stringify(r));
  r = await api('GET', '/agents/bot3/layers');
  check('get layers', r.body.layers?.[0]?.tag === 'v1', JSON.stringify(r));
  r = await api('DELETE', '/agents/bot/tags/v1');
  check('a pinned tag can\'t be dropped → 409 tag_pinned', r.status === 409 && r.body.code === 'tag_pinned', JSON.stringify(r));
  r = await api('PUT', '/agents/bot3/layers', { layers: [{ agent: 'bot', mode: 'mount', memories: ['EPISODIC'] }] });
  check('a layer of EPISODIC → 400', r.status === 400, JSON.stringify(r));

  // --- layers can't reach outside the org, and history layers stay
  r = await api('PUT', '/agents/rival/memory', {});
  check('a second org\'s agent has a space', r.status === 200 && r.body.namespace === 'rivalco--rival', JSON.stringify(r));
  await aql('STORE INTO SEMANTIC (key = "secret", fact = "rival-only") NAMESPACE "rivalco--rival"');
  r = await api('PUT', '/agents/bot3/layers', { layers: [{ space: 'rivalco--rival', mode: 'mount', memories: ['SEMANTIC'] }] });
  check('a raw space in another org → 400', r.status === 400 && /another org/.test(r.body.error), JSON.stringify(r));
  r = await api('PUT', '/agents/bot3/layers', { layers: [{ agent: 'rival', mode: 'mount', memories: ['SEMANTIC'] }] });
  check('an agent in another org → 400', r.status === 400 && /another org/.test(r.body.error), JSON.stringify(r));
  r = await api('PUT', '/agents/bot3/layers', { layers: [{ space: 'nobody--owns-this', mode: 'mount', memories: ['SEMANTIC'] }] });
  check('a space no agent owns → 400', r.status === 400 && /no agent has/.test(r.body.error), JSON.stringify(r));
  r = await api('PUT', '/agents/bot3/layers', { layers: [{ space: 'acme--bot', mode: 'pin', tag: 'v1', memories: ['SEMANTIC'], sealed: true }] });
  check('a raw space of an agent in this org is accepted (GET → PUT round trip)', r.status === 200 && r.body.layers?.[0]?.space === 'acme--bot', JSON.stringify(r));

  r = await api('GET', '/agents/bot2/layers');
  const hist = l => (l.body.layers?.layers ?? l.body.layers ?? []).filter(x => x.mode === 'history');
  check('a promoted agent has a history layer on the old space', hist(r).length === 1 && hist(r)[0].space === 'acme--bot', JSON.stringify(r));
  r = await api('PUT', '/agents/bot2/layers', { layers: [] });
  check('PUT without it keeps the history layer', r.status === 200 && hist(r).length === 1, JSON.stringify(r));
  r = await api('PUT', '/agents/bot2/layers', { layers: [{ space: 'rivalco--rival', mode: 'history', tag: 't', memories: ['EPISODIC'] }] });
  check('a forged history layer is dropped', r.status === 200 && hist(r).length === 1 && hist(r)[0].space === 'acme--bot', JSON.stringify(r));

  // --- kind 2: procedures back into the same agent (a conflict, resolved, then reverted)
  const steps = async ns => {
    const d = await aql(`RECALL FROM PROCEDURAL WHERE key = "refund" NAMESPACE "${ns}"`);
    return JSON.stringify(((d.data ?? d).records ?? [])[0]?.data?.steps ?? null);
  };
  await aql('STORE INTO PROCEDURAL (key = "refund", steps = "v1") NAMESPACE "acme--bot"');
  r = await api('POST', '/agents/bot/branches', { name: 'teach' });
  check('branch for a kind 2 merge', r.status === 201, JSON.stringify(r));
  await aql('UPDATE INTO PROCEDURAL WHERE key = "refund" (steps = "from-branch") NAMESPACE "acme--bot" BRANCH "teach"');
  await aql('STORE INTO PROCEDURAL (key = "escalate", steps = "new") NAMESPACE "acme--bot" BRANCH "teach"');
  await aql('UPDATE INTO PROCEDURAL WHERE key = "refund" (steps = "from-main") NAMESPACE "acme--bot"');

  r = await api('GET', '/agents/bot/branches/teach/conflicts?memories=procedural');
  const c = r.body;
  check('conflicts: one edit_edit, one clean add', r.status === 200 && c.conflicts?.length === 1 && c.conflicts[0].type === 'edit_edit' && c.conflicts[0].key === 'refund' && c.clean?.length === 1 && c.clean[0].key === 'escalate', JSON.stringify(r));
  r = await api('POST', '/agents/bot/branches/teach/merge', { main_offset: c.main.offset });
  check('merge with an open conflict → 409 merge_conflicts', r.status === 409 && r.body.code === 'merge_conflicts', JSON.stringify(r));
  const resolutions = [{ lineage: c.conflicts[0].lineage, memory: 'PROCEDURAL', take: 'branch' }];
  r = await api('POST', '/agents/bot/branches/teach/merge', { main_offset: 1, resolutions });
  check('merge at an old main_offset → 409 merge_stale', r.status === 409 && r.body.code === 'merge_stale', JSON.stringify(r));
  check('nothing applied yet', (await steps('acme--bot')).includes('from-main'), await steps('acme--bot'));
  r = await api('POST', '/agents/bot/branches/teach/merge', { main_offset: c.main.offset, resolutions, merged_by: 'someone-else', name: 'other' });
  const merged = r.body;
  check('merge → merge id, the same agent', r.status === 200 && merged.merge_id && merged.added === 1 && merged.changed === 1, JSON.stringify(r));
  check('main has the resolved procedure', (await steps('acme--bot')).includes('from-branch'), await steps('acme--bot'));
  r = await api('POST', '/agents/bot/merges/nope/revert');
  check('revert of an unknown merge → 404', r.status === 404 && r.body.code === 'merge_not_found', JSON.stringify(r));
  r = await api('POST', `/agents/bot/merges/${merged.merge_id}/revert`);
  check('revert → previous versions back', r.status === 200 && r.body.reverted === 2 && (await steps('acme--bot')).includes('from-main'), JSON.stringify(r));

  // A body can't aim a merge at another agent's memory.
  await aql('STORE INTO PROCEDURAL (key = "refund", steps = "rival") NAMESPACE "rivalco--rival"');
  r = await api('GET', '/agents/bot/branches/teach/conflicts');
  r = await api('POST', '/agents/bot/branches/teach/merge', { namespace: 'rivalco--rival', main_offset: r.body.main?.offset, resolutions: [] });
  check('a body\'s namespace is ignored: the other org is untouched', (await steps('rivalco--rival')).includes('rival'), await steps('rivalco--rival'));

  // --- kind 1: refused until rebased, then a new agent
  r = await api('POST', '/agents/bot/branches', { name: 'behave' });
  check('branch for a kind 1 merge', r.status === 201, JSON.stringify(r));
  await aql('STORE INTO SEMANTIC (key = "rule:c", fact = "branch-c") NAMESPACE "acme--bot" BRANCH "behave"');
  await aql('STORE INTO SEMANTIC (key = "rule:d", fact = "prod-newer") NAMESPACE "acme--bot"');
  r = await api('POST', '/agents/bot/branches/behave/promote', { new_agent: { id: 'bot4' } });
  check('promote while main moved → 409 merge_stale, no agent', r.status === 409 && r.body.code === 'merge_stale', JSON.stringify(r));
  check('no agent was created', (await api('GET', '/agents/bot4/memory')).status === 404);
  r = await api('GET', '/agents/bot/branches/behave/conflicts');
  r = await api('POST', '/agents/bot/branches/behave/rebase', { main_offset: r.body.main.offset });
  check('rebase → the branch point moves', r.status === 200 && r.body.point?.offset != null, JSON.stringify(r));
  r = await api('POST', '/agents/bot/branches/behave/promote', { new_agent: { id: 'bot4', name: 'Bot 4' } });
  check('promote after the rebase → 201', r.status === 201 && r.body.agent?.id === 'bot4', JSON.stringify(r));
  const f4 = await facts('acme--bot4');
  check('the new agent keeps prod\'s newer memory and the branch\'s', f4.includes('prod-newer') && f4.includes('branch-c'), JSON.stringify(f4));
  await api('POST', '/agents/bot/branches', { name: 'stale' });
  await aql('STORE INTO SEMANTIC (key = "rule:e", fact = "later") NAMESPACE "acme--bot"');
  r = await api('POST', '/agents/bot/branches/stale/promote', { new_agent: { id: 'bot5' }, allow_stale: true });
  check('allow_stale passes the gate', r.status === 201, JSON.stringify(r));

  // --- drop
  await api('DELETE', '/agents/bot/branches/teach');
  await api('DELETE', '/agents/bot/branches/behave');
  await api('DELETE', '/agents/bot/branches/stale');
  r = await api('DELETE', '/agents/bot/branches/try');
  check('drop branch', r.status === 200 && r.body.dropped === true, JSON.stringify(r));
  r = await api('GET', '/agents/bot/branches');
  check('no branches left', r.body.branches?.length === 0, JSON.stringify(r));
} catch (e) {
  console.error(e);
  failures++;
} finally {
  for (const c of children) c.kill('SIGKILL');
  rmSync(tmp, { recursive: true, force: true });
}
console.log(failures ? `${failures} failed` : 'all passed');
process.exit(failures ? 1 : 0);
