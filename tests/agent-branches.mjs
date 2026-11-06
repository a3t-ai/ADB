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
 *   - ADB refusals come back as {error, code} with a fitting status; drop;
 *   - a run on a branch (AC-6): its writes reach the branch only; a run on main as before;
 *   - agent versions and deployment pins (AC-5), with lineage on promote and fork;
 *   - merge requests, both kinds: conflicts, gate, apply, rollback (AC-22, 23, 10, 11a);
 *   - an agent turn on a branch, recorded with its version and memory point (AC-6, AC-20).
 *
 *   node tests/agent-branches.mjs --adb <adb bin> --flowr <flowr-ce bin (adb-platform)>
 */
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer as netServer } from 'node:net';
import { createServer as httpServer } from 'node:http';

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

  // --- AC-6: a run on a memory branch
  r = await api('POST', '/agents/bot/branches', { name: 'run' });
  check('a branch to run on', r.status === 201, JSON.stringify(r));
  await tool('create_flow', { yaml: `document: { dsl: '1.0.3', namespace: t, name: branch-writer, version: '1.0.0' }
do:
  - store:
      call: adb
      with:
        _resource: a3t/agent-memory
        query: 'STORE INTO SEMANTIC (key = "\${ .key }", fact = "\${ .key }") NAMESPACE "acme--bot"'
` });
  const branchFacts = async () => {
    const d = await aql('RECALL FROM SEMANTIC ALL NAMESPACE "acme--bot" BRANCH "run"');
    return ((d.data ?? d).records ?? []).map(x => x.data?.fact);
  };
  const until = async f => { for (let i = 0; i < 100; i++) { if (await f()) return true; await sleep(100); } return false; };
  await tool('execute_flow', { namespace: 't', name: 'branch-writer', memory_branch: 'run', input: { key: 'run-on-branch' } });
  check('a run on a branch writes to the branch', await until(async () => (await branchFacts()).includes('run-on-branch')), JSON.stringify(await branchFacts()));
  check('main is unchanged by it', !(await facts('acme--bot')).includes('run-on-branch'), JSON.stringify(await facts('acme--bot')));
  await tool('execute_flow', { namespace: 't', name: 'branch-writer', input: { key: 'run-on-main' } });
  check('a run on main writes to main', await until(async () => (await facts('acme--bot')).includes('run-on-main')), JSON.stringify(await facts('acme--bot')));
  await api('DELETE', '/agents/bot/branches/run');

  // --- AC-5: versions, pins, lineage
  r = await api('GET', '/agents/bot/versions');
  check('no versions yet', r.status === 200 && r.body.versions?.length === 0, JSON.stringify(r));
  r = await api('POST', '/agents/bot/versions', { flows: [{ namespace: 't', name: 'branch-writer', version: '9.9.9' }] });
  check('a flow version that does not exist is refused', r.status === 400, JSON.stringify(r));
  r = await api('POST', '/agents/bot/versions', { flows: [{ namespace: 't', name: 'branch-writer', version: '1.0.0' }], note: 'first' });
  check('version 1 holds its flows and memory point', r.status === 201 && r.body.version === '1' && r.body.flows?.[0]?.name === 'branch-writer' && r.body.memory?.namespace === 'acme--bot' && r.body.memory?.tag === 'agent-v1' && r.body.memory?.point?.offset > 0, JSON.stringify(r));
  await aql('STORE INTO SEMANTIC (key = "after-v1", fact = "after-v1") NAMESPACE "acme--bot"');
  r = await api('POST', '/agents/bot/versions', {});
  check('the next one is version 2', r.status === 201 && r.body.version === '2' && r.body.flows?.length === 0, JSON.stringify(r));
  r = await api('GET', '/agents/bot/versions/1');
  check('version 1 reads back unchanged', r.status === 200 && r.body.note === 'first' && r.body.flows?.length === 1, JSON.stringify(r));
  check('a missing version → 404', (await api('GET', '/agents/bot/versions/99')).status === 404);
  r = await api('GET', '/projects/acme/environments');
  check('a project has dev, qa and prod', r.body.environments?.map(e => e.name).join() === 'dev,qa,prod' && r.body.environments[2].production === 'true' && r.body.environments[2].required_approvals === '1', JSON.stringify(r));
  r = await api('PUT', '/agents/bot/deployments/prod', { version: '1' });
  check('prod takes no direct pin', r.status === 409 && r.body.code === 'approval_required', JSON.stringify(r));
  check('an environment the project lacks → 404', (await api('PUT', '/agents/bot/deployments/nowhere', { version: '1' })).status === 404);
  r = await api('PUT', '/agents/bot/deployments/qa', { version: '1' });
  check('pin qa to version 1', r.status === 200 && r.body.version === '1' && r.body.previous === '', JSON.stringify(r));
  await api('PUT', '/agents/bot/deployments/dev', { version: '2' });
  r = await api('PUT', '/agents/bot/deployments/qa', { version: '2' });
  check('a new pin remembers the previous', r.status === 200 && r.body.version === '2' && r.body.previous === '1', JSON.stringify(r));
  r = await api('GET', '/agents/bot/deployments');
  check('one pin per environment', r.body.deployments?.length === 2, JSON.stringify(r));
  r = await api('PUT', '/projects/acme/environments/qa', { required_approvals: 1 });
  check('an environment\'s rule can change', r.status === 200 && r.body.required_approvals === '1', JSON.stringify(r));
  check('then it takes no direct pin either', (await api('PUT', '/agents/bot/deployments/qa', { version: '1' })).status === 409);
  await api('PUT', '/projects/acme/environments/qa', { required_approvals: 0 });
  check('and the other environments stayed', (await api('GET', '/projects/acme/environments')).body.environments?.length === 3);
  check('pinning a missing version → 404', (await api('PUT', '/agents/bot/deployments/qa', { version: '99' })).status === 404);
  await api('POST', '/agents/bot/branches', { name: 'ship' });
  r = await api('POST', '/agents/bot/branches/ship/promote', { new_agent: { id: 'bot6' } });
  check('a promoted agent starts at version 1 with its lineage', r.status === 201 && r.body.version?.version === '1' && r.body.version?.lineage?.promoted_from?.agent === 'bot' && r.body.version?.lineage?.promoted_from?.branch === 'ship', JSON.stringify(r));
  r = await api('POST', '/agents/bot/fork', { new_agent: { id: 'bot7' } });
  check('a forked agent too', r.status === 201 && r.body.version?.lineage?.forked_from?.agent === 'bot', JSON.stringify(r));
  await api('DELETE', '/agents/bot/branches/ship');

  // --- merge request, kind 2: procedures back into this agent
  const procs = async () => JSON.stringify(await aql('RECALL FROM PROCEDURAL ALL NAMESPACE "acme--bot"'));
  await aql('STORE INTO PROCEDURAL (key = "mr-proc", steps = "v1") NAMESPACE "acme--bot"');
  await api('POST', '/agents/bot/branches', { name: 'mr2' });
  await aql('UPDATE INTO PROCEDURAL WHERE key = "mr-proc" (steps = "from-branch") NAMESPACE "acme--bot" BRANCH "mr2"');
  await aql('STORE INTO PROCEDURAL (key = "mr-new", steps = "added-on-branch") NAMESPACE "acme--bot" BRANCH "mr2"');
  await aql('UPDATE INTO PROCEDURAL WHERE key = "mr-proc" (steps = "from-main") NAMESPACE "acme--bot"');
  r = await api('POST', '/agents/bot/merge-requests', { branch: 'nope', kind: 2 });
  check('a merge request for no such branch is refused', r.status >= 400, JSON.stringify(r));
  check('a kind must be 1 or 2', (await api('POST', '/agents/bot/merge-requests', { branch: 'mr2', kind: 3 })).status === 400);
  r = await api('POST', '/agents/bot/merge-requests', { branch: 'mr2', kind: 2 });
  check('kind 2 opens with its conflict and what blocks it', r.status === 201 && r.body.status === 'open' && r.body.conflicts?.length === 1 && r.body.blocked_by?.length === 3, JSON.stringify(r));
  const mr2 = r.body.id;
  const conflict = r.body.conflicts[0];
  r = await api('POST', `/agents/bot/merge-requests/${mr2}/apply`);
  check('apply waits for the gate', r.status === 409 && r.body.code === 'merge_blocked' && r.body.blocked_by?.length === 3, JSON.stringify(r));
  check('nothing landed', (await procs()).includes('from-main') && !(await procs()).includes('added-on-branch'));
  r = await api('PUT', `/agents/bot/merge-requests/${mr2}/resolutions`, { resolutions: [{ lineage: conflict.lineage, memory: conflict.memory, take: 'edit', value: { key: 'mr-proc', steps: 'resolved' } }] });
  check('a resolution clears the conflict', r.status === 200 && r.body.blocked_by?.length === 2, JSON.stringify(r));
  r = await api('POST', `/agents/bot/merge-requests/${mr2}/approvals`, { note: 'looks right' });
  check('an approval counts', r.status === 200 && r.body.approvals?.length === 1 && r.body.blocked_by?.length === 1, JSON.stringify(r));
  check('evidence needs a ref', (await api('POST', `/agents/bot/merge-requests/${mr2}/evidence`, { kind: 'run' })).status === 400);
  r = await api('POST', `/agents/bot/merge-requests/${mr2}/evidence`, { kind: 'run', ref: 'run-1' });
  check('evidence opens the gate', r.status === 200 && r.body.blocked_by?.length === 0, JSON.stringify(r));
  r = await api('POST', `/agents/bot/merge-requests/${mr2}/apply`);
  check('kind 2 applies and names its merge', r.status === 200 && r.body.status === 'merged' && !!r.body.result?.merge_id, JSON.stringify(r));
  check('main has the resolution and the addition', (await procs()).includes('resolved') && (await procs()).includes('added-on-branch'), await procs());
  check('applying twice is refused', (await api('POST', `/agents/bot/merge-requests/${mr2}/apply`)).body.code === 'merge_not_open');
  r = await api('GET', `/agents/bot/merge-requests/${mr2}`);
  check('the record keeps it all', r.body.kind === '2' && r.body.resolutions?.length === 1 && r.body.approvals?.length === 1 && r.body.evidence?.length === 1 && !!r.body.result?.merge_id, JSON.stringify(r));
  r = await api('POST', `/agents/bot/merge-requests/${mr2}/rollback`);
  check('kind 2 rolls back by its merge id', r.status === 200 && r.body.status === 'rolled_back', JSON.stringify(r));
  check('main is back', (await procs()).includes('from-main') && !(await procs()).includes('added-on-branch'), await procs());
  await api('DELETE', '/agents/bot/branches/mr2');

  // --- merge request, kind 1: behaviour, as a new agent
  const ruleFlow = v => `document: { dsl: '1.0.3', namespace: t, name: rule, version: '${v}' }\ndo:\n  - a:\n      set:\n        v: '${v}'\n`;
  await tool('create_flow', { yaml: ruleFlow('1.0.0') });
  r = await api('POST', '/agents/bot/versions', { flows: [{ namespace: 't', name: 'rule', version: '1.0.0' }] });
  check('a version holding flow rule@1.0.0', r.status === 201, JSON.stringify(r));
  await api('POST', '/agents/bot/branches', { name: 'mr1' });
  r = await api('POST', '/agents/bot/branches/mr1/flows', { yaml: ruleFlow('2.0.0') });
  check('a draft on the branch keeps its base', r.status === 201 && r.body.base_version === '1.0.0' && r.body.branch_version === '2.0.0', JSON.stringify(r));
  const active = async () => (await tool('get_flow', { namespace: 't', name: 'rule' })).version ?? (await tool('get_flow', { namespace: 't', name: 'rule' }));
  check('a draft is not active', String(JSON.stringify(await active())).includes('1.0.0'), JSON.stringify(await active()));
  await tool('create_flow', { yaml: ruleFlow('1.1.0') });
  r = await api('POST', '/triggers', { name: 'mr-hook', namespace: 'acme', config: { type: 'webhook', path: '/mr-hook' }, targets: [{ type: 'agent', agent_id: 'bot' }] });
  check('a trigger aimed at the agent', r.status === 201, JSON.stringify(r));
  const trigger = r.body.id;
  r = await api('POST', '/agents/bot/merge-requests', { branch: 'mr1', kind: 1 });
  const flowRow = r.body.flows?.find(f => f.name === 'rule');
  check('a flow fixed in prod after the branch point is a conflict', r.status === 201 && flowRow?.state === 'conflict' && flowRow?.prod_version === '1.1.0', JSON.stringify(r));
  const mr1 = r.body.id;
  check('it blocks the merge', r.body.blocked_by?.some(b => b.includes('t/rule')), JSON.stringify(r.body.blocked_by));
  await api('POST', '/agents/bot/branches/mr1/flows', { yaml: ruleFlow('2.1.0') });
  check('an edit must name a version that exists', (await api('PUT', `/agents/bot/merge-requests/${mr1}/resolutions`, { flow_resolutions: [{ namespace: 't', name: 'rule', take: 'edit', version: '9.9.9' }] })).status === 400);
  await api('PUT', `/agents/bot/merge-requests/${mr1}/resolutions`, { flow_resolutions: [{ namespace: 't', name: 'rule', take: 'branch' }] });
  await api('POST', `/agents/bot/merge-requests/${mr1}/approvals`);
  r = await api('POST', `/agents/bot/merge-requests/${mr1}/evidence`, { kind: 'run', ref: 'run-2' });
  check('kind 1 is ready', r.body.blocked_by?.length === 0, JSON.stringify(r.body.blocked_by));
  r = await api('POST', `/agents/bot/merge-requests/${mr1}/apply`, { new_agent: { id: 'bot8' } });
  check('kind 1 ships as a new agent', r.status === 200 && r.body.status === 'merged' && r.body.result?.promoted_agent === 'bot8', JSON.stringify(r));
  check('the chosen flow is the new agent\'s version', r.body.result?.agent?.version?.flows?.[0]?.version === '2.1.0', JSON.stringify(r.body.result?.agent?.version));
  check('with its lineage', r.body.result?.agent?.version?.lineage?.promoted_from?.branch === 'mr1');
  check('the trigger moved', r.body.result?.moved_triggers?.includes(trigger), JSON.stringify(r.body.result?.moved_triggers));
  const aimedAt = async () => (await api('GET', `/triggers/${trigger}`)).body.targets?.map(t => t.agent_id);
  check('it now aims at the new agent', JSON.stringify(await aimedAt()) === '["bot8"]', JSON.stringify(await aimedAt()));
  const t0 = Date.now();
  r = await api('POST', `/agents/bot/merge-requests/${mr1}/rollback`);
  check('kind 1 rolls back the triggers', r.status === 200 && r.body.status === 'rolled_back' && JSON.stringify(await aimedAt()) === '["bot"]', JSON.stringify(r));
  check('in under 5 minutes', Date.now() - t0 < 300000);
  r = await api('GET', '/agents/bot/merge-requests');
  check('both records are listed', r.body.merge_requests?.length === 2, JSON.stringify(r.body.merge_requests?.length));
  await api('DELETE', '/agents/bot/branches/mr1');

  // --- an agent turn: on a branch, and recorded (AC-6, AC-20). A stub model answers.
  const llm = httpServer((req, res) => {
    let body = '';
    req.on('data', d => { body += d; });
    req.on('end', () => {
      const content = 'stub answer';
      if (JSON.parse(body || '{}').stream) {
        res.writeHead(200, { 'content-type': 'text/event-stream' });
        res.write(`data: ${JSON.stringify({ choices: [{ delta: { content } }] })}\n\n`);
        res.end('data: [DONE]\n\n');
      } else {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ choices: [{ message: { role: 'assistant', content } }] }));
      }
    });
  });
  const llmPort = await freePort();
  await new Promise(res => llm.listen(llmPort, '127.0.0.1', res));
  await tool('create_resource', { namespace: 'a3t', name: 'stub-llm', type_ref: 'llm', config: { provider: 'openai', api_url: `http://127.0.0.1:${llmPort}`, api_key: 'stub', model: 'stub' } });
  await tool('create_flow', { yaml: `document: { dsl: '1.0.3', namespace: t, name: agent-turn, version: '1.0.0' }
do:
  - run_agent:
      call: a3t:agent
      with:
        config: \${ .config }
        session_id: \${ .session_id }
        message: \${ .message }
` });
  await api('POST', '/agents/bot/branches', { name: 'chat' });
  // A deploy to prod goes through the gate.
  check('a deploy names a version that exists', (await api('POST', '/agents/bot/merge-requests', { kind: 'deploy', environment: 'prod', version: '99' })).status === 404);
  r = await api('POST', '/agents/bot/merge-requests', { kind: 'deploy', environment: 'prod', version: '1' });
  const dep1 = r.body.id;
  check('a deploy request waits on prod\'s rule', r.status === 201 && r.body.blocked_by?.length === 2, JSON.stringify(r));
  check('so applying it now is refused', (await api('POST', `/agents/bot/merge-requests/${dep1}/apply`)).body.code === 'merge_blocked');
  await api('POST', `/agents/bot/merge-requests/${dep1}/approvals`);
  await api('POST', `/agents/bot/merge-requests/${dep1}/evidence`, { kind: 'run', ref: 'run-3' });
  r = await api('POST', `/agents/bot/merge-requests/${dep1}/apply`);
  check('approved, it pins prod', r.status === 200 && r.body.result?.pin?.version === '1', JSON.stringify(r));
  check('a first pin has nothing to roll back to', (await api('POST', `/agents/bot/merge-requests/${dep1}/rollback`)).body.code === 'no_previous');
  r = await api('POST', '/agents/bot/merge-requests', { kind: 'deploy', environment: 'prod', version: '2' });
  const dep2 = r.body.id;
  await api('POST', `/agents/bot/merge-requests/${dep2}/approvals`);
  await api('POST', `/agents/bot/merge-requests/${dep2}/evidence`, { kind: 'run', ref: 'run-4' });
  r = await api('POST', `/agents/bot/merge-requests/${dep2}/apply`);
  check('the next deploy remembers the previous', r.body.result?.pin?.previous === '1', JSON.stringify(r));
  r = await api('POST', `/agents/bot/merge-requests/${dep2}/rollback`);
  check('rolling it back pins the version before', r.status === 200 && r.body.result?.rollback?.pin?.version === '1', JSON.stringify(r));
  r = await api('POST', '/agents/bot/merge-requests', { kind: 'deploy', environment: 'prod', version: '2' });
  await api('POST', `/agents/bot/merge-requests/${r.body.id}/approvals`);
  await api('POST', `/agents/bot/merge-requests/${r.body.id}/evidence`, { kind: 'run', ref: 'run-5' });
  await api('POST', `/agents/bot/merge-requests/${r.body.id}/apply`);
  const session = 'bot:session-chat';
  const config = { llm_resource: 'a3t/stub-llm', memory_resource: 'a3t/agent-memory', memory_namespace: 'unused', system_prompt: 'Test agent.', mcp_servers: [] };
  await tool('execute_flow', { namespace: 't', name: 'agent-turn', memory_branch: 'chat', input: { session_id: session, message: 'hello on the branch', config } });
  const turns = async b => JSON.stringify(await aql(`RECALL FROM WORKING WHERE session_id = "${session}" NAMESPACE "acme--bot"${b ? ` BRANCH "${b}"` : ''}`));
  check('an agent turn on a branch stores on the branch', await until(async () => (await turns('chat')).includes('hello on the branch')), await turns('chat'));
  check('main has none of it', !(await turns()).includes('hello on the branch'), await turns());
  r = await api('GET', '/agents/bot/runs');
  const run = r.body.runs?.find(x => x.memory_branch === 'chat');
  check('the turn names its version, environment and branch', run?.agent_version === '2' && run?.environment === 'prod' && run?.memory_namespace === 'acme--bot', JSON.stringify(r));
  check('and the memory point it read', JSON.parse(run?.memory_point || '{}').offset > 0, JSON.stringify(run));
  llm.close();
  await api('DELETE', '/agents/bot/branches/chat');

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
