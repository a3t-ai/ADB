#!/usr/bin/env node
/**
 * FlowR ↔ ADB integration test: a real FlowR-CE runs A3T's `a3t-test-resource` flow
 * against ADB two ways — through an `mcp` resource configured as A3T's
 * `agent-memory-mcp` is (`url: …/mcp/sse`), and through an `adb` resource with
 * FlowR's ADB driver (`call:adb`) — then checks the DB console's `query_resource`
 * tool and editing the resource's config (registry PUT). No dependencies.
 *
 *   # spawn both (temp data dirs, free ports):
 *   node tests/flowr-integration.mjs --adb target/release/adb --flowr <flowr-ce binary>
 *   # or against running servers (containers, compose):
 *   node tests/flowr-integration.mjs --adb-url http://localhost:3000 --flowr-url http://localhost:3002
 *
 * FlowR EE requires a Bearer token: set FLOWR_TOKEN (spawned servers inherit the environment,
 * e.g. FLOWR_JWT_SECRET and MCP_RESOURCE_URI for a local EE run).
 *
 * With `--adb-url`, the URL is also what FlowR is told to call, so it must be reachable
 * from FlowR (e.g. `http://adb:3000` inside compose: pass `--adb-url-for-flowr`).
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
if (!(args.adb || args['adb-url']) || !(args.flowr || args['flowr-url'])) {
  console.error('usage: flowr-integration.mjs (--adb <bin> | --adb-url <url> [--adb-url-for-flowr <url>]) (--flowr <bin> | --flowr-url <url>)');
  process.exit(2);
}

const children = [];
const tmp = mkdtempSync(join(tmpdir(), 'adb-flowr-'));
let failures = 0;

function check(label, ok, detail = '') {
  console.log(`${ok ? '✓' : '✗'} ${label}${ok || !detail ? '' : `  — ${detail}`}`);
  if (!ok) failures++;
}

const freePort = () => new Promise(res => {
  const s = createServer().listen(0, '127.0.0.1', () => { const p = s.address().port; s.close(() => res(p)); });
});

async function waitHealthy(url, what) {
  for (let i = 0; i < 300; i++) {
    try { if ((await fetch(`${url}/health`)).ok) return; } catch {}
    await new Promise(r => setTimeout(r, 100));
  }
  throw new Error(`${what} did not become healthy at ${url}`);
}

function start(bin, argv, env, name) {
  const child = spawn(bin, argv, { env: { ...process.env, ...env }, stdio: ['ignore', 'ignore', 'pipe'] });
  let log = '';
  child.stderr.on('data', d => { log = (log + d).slice(-4000); });
  child.on('exit', code => { if (code) console.error(`${name} exited ${code}:\n${log}`); });
  children.push(child);
  return child;
}

// FlowR's own MCP endpoint: POST /message, JSON-RPC in, JSON-RPC out.
let flowrUrl;
let rpcId = 1;
const auth = process.env.FLOWR_TOKEN ? { authorization: `Bearer ${process.env.FLOWR_TOKEN}` } : {};
async function flowr(name, toolArgs) {
  const r = await fetch(`${flowrUrl}/message`, {
    method: 'POST', headers: { 'content-type': 'application/json', ...auth },
    body: JSON.stringify({ jsonrpc: '2.0', id: rpcId++, method: 'tools/call', params: { name, arguments: toolArgs } }),
  });
  const body = await r.json();
  if (body.error) throw new Error(`${name}: ${body.error.message ?? body.error} (HTTP ${r.status})`);
  const text = body.result?.content?.[0]?.text ?? '';
  return { isError: !!body.result?.isError, text };
}

// execute_flow returns markdown with the output JSON after "Output:".
function flowOutput(text) {
  const i = text.indexOf('Output:\n');
  if (i < 0) return undefined;
  const out = JSON.parse(text.slice(i + 8));
  // call:mcp returns content[0].text, i.e. ADB's JSON as a string
  if (typeof out === 'string') { try { return JSON.parse(out); } catch { return out; } }
  return out;
}

// A3T's a3t-test-resource flow (flowr-system/a3t/backend/flows/test_resource.yaml), MCP part.
const TEST_RESOURCE_FLOW = `document:
  dsl: 1.0.3
  namespace: a3t
  name: a3t-test-resource
  version: 1.0.0
do:
- route:
    switch:
    - case:
        when: \${ .input.test_type == "adb" }
        then: test-adb
    - case:
        when: \${ .input.test_type == "mcp" }
        then: test-mcp
    - case:
        when: \${ .input.test_type == "mcp_call" }
        then: test-mcp-call
- test-adb:
    call: adb
    with:
      query: \${ .input.query // "RECALL FROM WORKING LIMIT 5" }
      _resource: \${ .input.resource }
    then: end
- test-mcp:
    call: mcp
    with:
      method: "tools/list"
      _resource: \${ .input.resource }
    then: end
- test-mcp-call:
    call: mcp
    with:
      tool: \${ .input.tool }
      arguments: \${ .input.arguments // {} }
      _resource: \${ .input.resource }
    then: end
`;

// A3T wraps the run payload as `{input: …}`; its flows read `.input.*`.
const run = (input) => flowr('execute_flow', { namespace: 'a3t', name: 'a3t-test-resource', input: { input: { resource: 'a3t/agent-memory-mcp', ...input } } });
const callTool = (tool, toolArgs) => run({ test_type: 'mcp_call', tool, arguments: toolArgs });

try {
  // ---------------------------------------------------------------- start
  let adbUrl = args['adb-url'];
  if (!adbUrl) {
    const port = await freePort();
    adbUrl = `http://127.0.0.1:${port}`;
    start(args.adb, ['serve', '--http', '--host', '127.0.0.1', '--port', String(port)], {}, 'adb');
  }
  flowrUrl = args['flowr-url'];
  if (!flowrUrl) {
    const port = await freePort();
    flowrUrl = `http://127.0.0.1:${port}`;
    start(args.flowr, ['--mode', 'http', '--port', String(port)], { CLAWDFLOW_DATA_DIR: join(tmp, 'flowr'), RUST_LOG: 'warn' }, 'flowr');
  }
  await waitHealthy(adbUrl, 'ADB');
  await waitHealthy(flowrUrl, 'FlowR');
  const adbForFlowr = args['adb-url-for-flowr'] ?? adbUrl;
  console.log(`ADB ${adbUrl} · FlowR ${flowrUrl}\n`);

  // ---------------------------------------------------------------- set up, as A3T's resources.yaml does
  let r = await flowr('create_resource', {
    namespace: 'a3t', name: 'agent-memory-mcp', type_ref: 'mcp',
    description: 'ADB MCP server — AQL memory tools',
    config: { url: `${adbForFlowr}/mcp/sse`, transport: 'sse' },
  });
  check('create agent-memory-mcp resource in FlowR', !r.isError, r.text);
  r = await flowr('create_flow', { yaml: TEST_RESOURCE_FLOW });
  check('create a3t-test-resource flow', !r.isError, r.text);

  // ---------------------------------------------------------------- the flow, through FlowR → ADB
  r = await run({ test_type: 'mcp' });
  const tools = (flowOutput(r.text) ?? []).map(t => t.name);
  check('tools/list through FlowR', ['aql_query', 'remember', 'recall', 'store_memory', 'get_stats'].every(t => tools.includes(t)), r.text);

  r = await callTool('remember', { content: 'Acme renews on 1 Nov', key: 'acme-renewal', fields: { account: 'acme' } });
  check('remember a fact', !r.isError && flowOutput(r.text)?.data?.type === 'stored', r.text);

  r = await callTool('query', { query: 'STORE INTO EPISODIC (key = "acme-call-1", content = "Called Acme about renewal", account = "acme")' });
  check('AQL STORE via `query` (A3T tool name)', !r.isError && flowOutput(r.text)?.data?.type === 'stored', r.text);

  r = await callTool('aql_query', { query: 'RECALL FROM ALL WHERE account = "acme"' });
  check('RECALL FROM ALL finds both', flowOutput(r.text)?.data?.count === 2, r.text);

  r = await callTool('recall', { contains: 'renews' });
  check('recall by content', flowOutput(r.text)?.data?.records?.[0]?.data?.key === 'acme-renewal', r.text);

  r = await callTool('stats', {});
  check('stats via `stats` (A3T tool name)', flowOutput(r.text)?.semantic === 1 && flowOutput(r.text)?.episodic === 1, r.text);

  r = await callTool('aql_query', { query: 'FORGET FROM SEMANTIC' });
  check('bad AQL surfaces as an error', /WHERE predicate/.test(r.text), r.text);

  // ---------------------------------------------------------------- ADB resource + driver (call:adb)
  r = await flowr('create_resource', {
    namespace: 'a3t', name: 'adb', type_ref: 'adb', description: 'ADB agent memory',
    config: { url: 'http://127.0.0.1:1' }, // wrong on purpose; fixed by the edit below
  });
  check('create adb resource', !r.isError, r.text);
  r = await run({ test_type: 'adb', resource: 'a3t/adb', query: 'RECALL FROM WORKING LIMIT 5' });
  check('call:adb with a bad url fails clearly', r.isError && /ADB request to http:\/\/127.0.0.1:1 failed/.test(r.text), r.text);

  // Edit the config as the Resources page does: registry PUT with the new config.
  const put = await fetch(`${flowrUrl}/registry/resources/a3t/adb`, {
    method: 'PUT', headers: { 'content-type': 'application/json', ...auth }, body: JSON.stringify({ url: adbForFlowr, scope: 'private' }),
  });
  check('edit adb resource config (registry PUT)', put.ok, `HTTP ${put.status} ${await put.text()}`);

  r = await run({ test_type: 'adb', resource: 'a3t/adb', query: 'STORE INTO WORKING (name = "alice", score = 95, active = true)' });
  check('call:adb STORE', !r.isError && flowOutput(r.text)?.data?.type === 'stored', r.text);
  r = await run({ test_type: 'adb', resource: 'a3t/adb', query: 'RECALL FROM WORKING WHERE name = "alice"' });
  check('call:adb RECALL', flowOutput(r.text)?.data?.records?.[0]?.data?.score === 95, r.text);
  r = await run({ test_type: 'adb', resource: 'a3t/adb', query: 'FORGET FROM WORKING' });
  check('call:adb bad AQL fails the step with ADB’s message', r.isError && /WHERE predicate/.test(r.text), r.text);

  // DB console: query_resource returns flat rows for the table view.
  r = await flowr('query_resource', { resource: 'adb', namespace: 'a3t', query: 'RECALL FROM WORKING WHERE name = "alice"' });
  let rows; try { rows = JSON.parse(r.text); } catch {}
  check('query_resource rows', !r.isError && rows?.[0]?.name === 'alice' && rows[0].score === 95 && rows[0].memory_type === 'WORKING', r.text);
  r = await flowr('query_resource', { resource: 'agent-memory-mcp', namespace: 'a3t', query: 'RECALL FROM WORKING' });
  check('query_resource refuses non-ADB types', r.isError && /does not support 'mcp'/.test(r.text), r.text);

  // ---------------------------------------------------------------- cross-check: what FlowR wrote is in ADB
  const q = await (await fetch(`${adbUrl}/query`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ query: 'RECALL FROM SEMANTIC WHERE key = "acme-renewal"' }),
  })).json();
  check('ADB /query sees FlowR’s write (ClawDbMemory shape)', q.success && q.data?.data?.records?.[0]?.data?.content === 'Acme renews on 1 Nov', JSON.stringify(q));
} catch (e) {
  check('session', false, e.stack);
} finally {
  for (const c of children) c.kill();
  rmSync(tmp, { recursive: true, force: true });
}

console.log(failures === 0 ? '\nFlowR integration test passed' : `\n${failures} check(s) failed`);
process.exit(failures === 0 ? 0 : 1);
