#!/usr/bin/env node
/**
 * MCP smoke test for ADB — replays the `memory_smoke_test` session (the steps an LLM or
 * an A3T chat agent takes) and checks every response. No dependencies.
 *
 *   node tests/mcp-smoke.mjs --stdio ./target/release/adb        # spawns `adb serve`
 *   node tests/mcp-smoke.mjs --http  http://localhost:3000        # POST /mcp
 *   node tests/mcp-smoke.mjs --sse   http://localhost:3000        # /mcp/sse (A3T's transport)
 */
import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';

const [mode, target] = process.argv.slice(2);
if (!['--stdio', '--http', '--sse'].includes(mode) || !target) {
  console.error('usage: mcp-smoke.mjs --stdio <adb-binary> | --http <base-url> | --sse <base-url>');
  process.exit(2);
}

// ---------------------------------------------------------------- transports

function stdioClient(bin) {
  const child = spawn(bin, ['serve'], { stdio: ['pipe', 'pipe', 'inherit'] });
  const pending = new Map();
  createInterface({ input: child.stdout }).on('line', line => {
    const msg = JSON.parse(line);
    pending.get(msg.id)?.(msg);
    pending.delete(msg.id);
  });
  return {
    request: (msg) => new Promise(res => { pending.set(msg.id, res); child.stdin.write(JSON.stringify(msg) + '\n'); }),
    notify: async (msg) => child.stdin.write(JSON.stringify(msg) + '\n'),
    close: () => child.kill(),
  };
}

function httpClient(base) {
  const post = async (msg) => {
    const r = await fetch(`${base}/mcp`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(msg) });
    return r.status === 202 ? null : r.json();
  };
  return { request: post, notify: post, close() {} };
}

async function sseClient(base) {
  const res = await fetch(`${base}/mcp/sse`);
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  const pending = new Map();
  let endpoint, buf = '';
  let resolveEndpoint;
  const gotEndpoint = new Promise(r => (resolveEndpoint = r));
  (async () => {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) return;
      buf += decoder.decode(value, { stream: true });
      let i;
      while ((i = buf.indexOf('\n\n')) >= 0) {
        const block = buf.slice(0, i); buf = buf.slice(i + 2);
        let event = '', data = '';
        for (const line of block.split('\n')) {
          if (line.startsWith('event:')) event = line.slice(6).trim();
          else if (line.startsWith('data:')) data += line.slice(5).trim();
        }
        if (event === 'endpoint') { endpoint = data; resolveEndpoint(); }
        else if (event === 'message') { const m = JSON.parse(data); pending.get(m.id)?.(m); pending.delete(m.id); }
      }
    }
  })();
  await gotEndpoint;
  const send = (msg) => fetch(`${base}${endpoint}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(msg) });
  return {
    request: (msg) => new Promise(res => { pending.set(msg.id, res); send(msg); }),
    notify: (msg) => send(msg),
    close: () => reader.cancel(),
  };
}

// ---------------------------------------------------------------- session

let nextId = 1;
let failures = 0;
const client = mode === '--stdio' ? stdioClient(target) : mode === '--http' ? httpClient(target) : await sseClient(target);

async function rpc(method, params) {
  const r = await client.request({ jsonrpc: '2.0', id: nextId++, method, params });
  if (r.error) throw new Error(`${method}: ${r.error.message}`);
  return r.result;
}

async function tool(name, args) {
  const r = await rpc('tools/call', { name, arguments: args });
  const text = r.content[0].text;
  let data; try { data = JSON.parse(text); } catch { data = text; }
  return { isError: !!r.isError, data };
}

function check(label, ok, detail = '') {
  console.log(`${ok ? '✓' : '✗'} ${label}${ok || !detail ? '' : `  — ${detail}`}`);
  if (!ok) failures++;
}

try {
  const init = await rpc('initialize', { protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 'mcp-smoke', version: '1' } });
  check('initialize', init.serverInfo?.name === 'adb', JSON.stringify(init));
  await client.notify({ jsonrpc: '2.0', method: 'notifications/initialized' });

  const tools = (await rpc('tools/list', {})).tools.map(t => t.name);
  check('tools/list has ClawDB + LLM tools', ['aql_query', 'store_memory', 'recall_memory', 'get_stats', 'remember', 'recall', 'forget', 'link'].every(t => tools.includes(t)), tools.join(','));

  const guide = await rpc('resources/read', { uri: 'adb://guide/memory' });
  check('guide resource', guide.contents[0].text.includes('Memory types'));

  let r = await tool('remember', { content: 'Acme renews on 1 Nov', key: 'acme-renewal', fields: { account: 'acme' } });
  check('1. remember fact', !r.isError, JSON.stringify(r.data));
  r = await tool('remember', { content: 'Called Acme about renewal', memory_type: 'EPISODIC', key: 'acme-call-1' });
  check('2. remember event', !r.isError, JSON.stringify(r.data));
  r = await tool('recall', { contains: 'renews' });
  check('3. recall finds the fact', r.data?.data?.count === 1 && r.data.data.records[0].data.account === 'acme', JSON.stringify(r.data));
  r = await tool('link', { from_type: 'SEMANTIC', from_key: 'acme-renewal', to_type: 'EPISODIC', to_key: 'acme-call-1', link_type: 'discussed_in' });
  check('4. link fact → event', !r.isError, JSON.stringify(r.data));
  r = await tool('aql_query', { query: 'RECALL FROM SEMANTIC WHERE key = "acme-renewal" WITH LINKS' });
  check('5. aql_query with links', !r.isError, JSON.stringify(r.data));
  r = await tool('forget', { memory_type: 'SEMANTIC', key: 'acme-renewal' });
  check('6. forget the fact', !r.isError, JSON.stringify(r.data));
  r = await tool('recall', { contains: 'renews' });
  check('6. fact is gone', r.data?.data?.count === 0, JSON.stringify(r.data));
  r = await tool('get_stats', {});
  check('7. get_stats', r.data?.episodic === 1, JSON.stringify(r.data));

  // ClawDB-compatible shortcuts, as A3T agents call them
  r = await tool('store_memory', { memory_type: 'WORKING', data: { task: 'draft reply', step: 1 }, namespace: 'agent-7' });
  check('store_memory (ClawDB)', !r.isError && r.data?.data?.type === 'stored', JSON.stringify(r.data));
  r = await tool('recall_memory', { memory_type: 'WORKING', filter: { task: 'draft reply' } });
  check('recall_memory (ClawDB)', r.data?.data?.count === 1, JSON.stringify(r.data));
  r = await tool('aql_query', { query: 'FORGET FROM WORKING' });
  check('bad AQL is an error with a clear message', r.isError && /WHERE/.test(r.data), String(r.data));
} catch (e) {
  check('session', false, e.message);
} finally {
  client.close();
}

console.log(failures === 0 ? '\nMCP smoke test passed' : `\n${failures} check(s) failed`);
process.exit(failures === 0 ? 0 : 1);
