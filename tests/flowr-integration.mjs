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
 *   # stage C gate: ADB durable; it is SIGKILLed mid-conversation and restarted:
 *   node tests/flowr-integration.mjs --adb target/release/adb --adb-data-dir /tmp/adb-data --flowr <flowr-ce binary>
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
import { createServer as createHttpServer } from 'node:http';

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
  let adbChild, adbArgv, flightPort;
  if (!adbUrl) {
    const port = await freePort();
    adbUrl = `http://127.0.0.1:${port}`;
    flightPort = await freePort();
    adbArgv = ['serve', '--http', '--host', '127.0.0.1', '--port', String(port), '--flight-port', String(flightPort)];
    if (args['adb-data-dir']) adbArgv = ['--data-dir', args['adb-data-dir'], ...adbArgv];
    adbChild = start(args.adb, adbArgv, {}, 'adb');
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
  // ADB's Flight endpoint (call:adb's wire): ours when we spawned ADB, else --adb-flight-url or ADB's default port.
  const adbFlightForFlowr = args['adb-flight-url'] ?? (flightPort ? `http://127.0.0.1:${flightPort}` : adbForFlowr.replace(/:\d+$/, ':3001'));
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

  // FlowR's own discovery tool (A3T's Permissions tab, the chat LLM) must ask ADB too.
  r = await flowr('list_mcp_tools', { namespace: 'a3t', name: 'agent-memory-mcp' });
  let listed; try { listed = JSON.parse(r.text); } catch {}
  check('list_mcp_tools discovers ADB tools from an …/mcp/sse resource', !r.isError && ['aql_query', 'remember', 'recall'].every(t => listed?.tools?.includes(t)), r.text);

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
    // Wrong on purpose (nothing listens on port 1); fixed by the edit below. call:adb
    // talks Flight (ADB stage G), so the resource names the Flight endpoint too.
    config: { url: 'http://127.0.0.1:1', flight_url: 'http://127.0.0.1:1' },
  });
  check('create adb resource', !r.isError, r.text);
  r = await run({ test_type: 'adb', resource: 'a3t/adb', query: 'RECALL FROM WORKING LIMIT 5' });
  check('call:adb with a bad url fails clearly', r.isError && /ADB request to http:\/\/127.0.0.1:1 failed/.test(r.text), r.text);

  // Edit the config as the Resources page does: registry PUT with the new config.
  const put = await fetch(`${flowrUrl}/registry/resources/a3t/adb`, {
    method: 'PUT', headers: { 'content-type': 'application/json', ...auth }, body: JSON.stringify({ url: adbForFlowr, flight_url: adbFlightForFlowr, scope: 'private' }),
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

  // ---------------------------------------------------------------- stage F: call:a3t:agent memory on ADB
  // A stub OpenAI-compatible LLM that answers with the system prompt it was given, so
  // the test sees what memory reached the agent, and counts its calls.
  let llmCalls = 0;
  const llm = createHttpServer((req, res) => {
    let body = '';
    req.on('data', d => { body += d; });
    req.on('end', () => {
      llmCalls++;
      const b = JSON.parse(body || '{}');
      const system = (b.messages ?? []).find(m => m.role === 'system')?.content ?? '';
      const content = `ECHO ${system}`;
      if (b.stream) {
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
  await new Promise(r => llm.listen(llmPort, '127.0.0.1', r));
  r = await flowr('create_resource', {
    namespace: 'a3t', name: 'stub-llm', type_ref: 'llm', description: 'stub LLM (echoes its system prompt)',
    config: { provider: 'openai', api_url: `http://127.0.0.1:${llmPort}`, api_key: 'stub', model: 'stub' },
  });
  check('create stub LLM resource', !r.isError, r.text);
  r = await flowr('create_flow', { yaml: `document:
  dsl: 1.0.3
  namespace: a3t
  name: agent-memory-test
  version: 1.0.0
do:
- run_agent:
    call: a3t:agent
    with:
      config: \${ .input.config }
      session_id: \${ .input.session_id }
      message: \${ .input.message }
` });
  check('create agent-memory-test flow', !r.isError, r.text);
  const agentConfig = (extra = {}) => ({
    llm_resource: 'a3t/stub-llm', memory_resource: 'a3t/adb', memory_namespace: 'f-test',
    system_prompt: 'You are a test agent.', mcp_servers: [], ...extra,
  });
  const ask = (session_id, message, extra) => flowr('execute_flow', {
    namespace: 'a3t', name: 'agent-memory-test', input: { input: { session_id, message, config: agentConfig(extra) } },
  });
  const adbQ = async (query) => (await (await fetch(`${adbUrl}/query`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ query }),
  })).json()).data?.data;
  await adbQ('STORE INTO SEMANTIC (key = "fav-colour", content = "The user\'s favourite colour is teal") NAMESPACE "f-test"');
  await adbQ('STORE INTO SEMANTIC (key = "elsewhere", content = "Another agent\'s fact: purple") NAMESPACE "other-ns"');
  r = await ask('f-s1', 'What is my favourite colour?');
  const reply = flowOutput(r.text);
  const text = JSON.stringify(reply ?? r.text);
  check('agent recalls a fact from ADB into its prompt', !r.isError && text.includes('favourite colour is teal'), r.text.slice(0, 600));
  check('only its own namespace', !text.includes('purple'), text.slice(0, 600));
  const turns = await adbQ('RECALL FROM WORKING WHERE session_id = "f-s1" NAMESPACE "f-test"');
  check('the user turn is stored in WORKING', turns?.records?.[0]?.data?.content === 'What is my favourite colour?', JSON.stringify(turns));
  const eps = await adbQ('RECALL FROM EPISODIC WHERE session_id = "f-s1" NAMESPACE "f-test"');
  check('the reply is stored in EPISODIC', eps?.records?.[0]?.data?.role === 'assistant' && String(eps.records[0].data.content).startsWith('ECHO'), JSON.stringify(eps).slice(0, 400));
  // Procedural short-circuit: off by default, on with memory_config.procedural.read.
  await adbQ('STORE INTO PROCEDURAL (pattern_id = "pw-reset", pattern = "reset my password", steps = ["Open Settings", "Choose Reset password"])');
  const before = llmCalls;
  r = await ask('f-s2', 'please reset my password now');
  check('procedures stay out of the way by default (LLM answers)', llmCalls === before + 1 && !String(flowOutput(r.text)?.content ?? '').startsWith('Open Settings'), r.text.slice(0, 400));
  const before2 = llmCalls;
  r = await ask('f-s3', 'please reset my password now', { memory_config: { procedural: { read: true } } });
  const out = flowOutput(r.text);
  check('a matching procedure answers without the LLM when enabled', llmCalls === before2 && out?.content === 'Open Settings; Choose Reset password' && out?.procedural_hit === true, r.text.slice(0, 400));
  // A broken memory resource never fails the turn.
  r = await ask('f-s4', 'hello', { memory_resource: 'a3t/no-such-adb' });
  check('a missing memory resource doesn\'t fail the turn', !r.isError && JSON.stringify(flowOutput(r.text)).includes('ECHO'), r.text.slice(0, 400));
  llm.close();

  // ---------------------------------------------------------------- stage C: ADB crashes mid-conversation
  if (args['adb-data-dir'] && adbChild) {
    r = await run({ test_type: 'adb', resource: 'a3t/adb', query: 'STORE INTO WORKING (conversation = "c-42", turn = 3, topic = "renewal")' });
    check('conversation turn stored in WORKING', !r.isError && flowOutput(r.text)?.data?.type === 'stored', r.text);

    const exited = new Promise(res => adbChild.once('exit', res));
    adbChild.kill('SIGKILL');
    await exited;
    adbChild = start(args.adb, adbArgv, {}, 'adb (restarted)');
    await waitHealthy(adbUrl, 'ADB after SIGKILL');

    r = await run({ test_type: 'adb', resource: 'a3t/adb', query: 'RECALL FROM WORKING WHERE conversation = "c-42"' });
    check('WORKING memory survives an ADB crash (call:adb)', flowOutput(r.text)?.data?.records?.[0]?.data?.turn === 3, r.text);
    r = await callTool('recall', { contains: 'renews' });
    check('facts survive the crash too (MCP recall)', flowOutput(r.text)?.data?.records?.[0]?.data?.key === 'acme-renewal', r.text);
    r = await run({ test_type: 'adb', resource: 'a3t/adb', query: 'RECALL FROM WORKING WHERE name = "alice"' });
    check('earlier call:adb writes survive', flowOutput(r.text)?.data?.records?.[0]?.data?.score === 95, r.text);    const facts = await adbQ('RECALL FROM SEMANTIC WHERE key = "fav-colour"');
    check('agent memory survives the crash (stage F)', facts?.records?.[0]?.data?.content?.includes('teal'), JSON.stringify(facts));
  }
} catch (e) {
  check('session', false, e.stack);
} finally {
  for (const c of children) c.kill();
  rmSync(tmp, { recursive: true, force: true });
}

console.log(failures === 0 ? '\nFlowR integration test passed' : `\n${failures} check(s) failed`);
process.exit(failures === 0 ? 0 : 1);
