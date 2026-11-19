#!/usr/bin/env node
/**
 * Read-after-write under load on an S3 option B cluster. W writers, each owning its own
 * keys (so it knows every key's latest value), spread over N namespaces. Each round a
 * writer STOREs a key (`c = 1`) or UPDATEs it (`c = n`), and as soon as the write is
 * acknowledged reads the key back through ANOTHER node than the one it wrote through —
 * the owner, the replica's reader or a third node, whichever the router picks there.
 * Every read must see the value just written: anything older is `stale`, nothing at all
 * is `missing`. Reports writes and checks per second, the outcome counts, and which kind
 * of node answered.
 *
 *   ADB_AUTH_TOKEN=… node cluster-raw.mjs --nodes http://a:3000,http://b:3000,http://c:3000 [--writers 64] [--secs 60] [--namespaces 64] [--keys 50]
 */
const args = Object.fromEntries(process.argv.slice(2).reduce((a, v, i, all) => (v.startsWith('--') ? [...a, [v.slice(2), all[i + 1]]] : a), []));
const NODES = String(args.nodes).split(',');
const W = Number(args.writers ?? 64);
const SECS = Number(args.secs ?? 60);
const NSN = Number(args.namespaces ?? 64);
const KEYS = Number(args.keys ?? 50);
const token = process.env.ADB_AUTH_TOKEN;
const H = { 'content-type': 'application/json', ...(token ? { authorization: `Bearer ${token}` } : {}) };
const run = Date.now().toString(36) + Math.random().toString(36).slice(2, 5);
console.log(`run ${run}: ${W} writers × ${KEYS} keys, ${NSN} namespaces, ${SECS} s, through ${NODES.length} nodes`);

async function q(url, aql, ms = 10000) {
  const r = await fetch(`${url}/query`, { method: 'POST', headers: H, body: JSON.stringify({ query: aql }), signal: AbortSignal.timeout(ms) });
  const j = await r.json();
  if (!j.success) throw new Error(j.error ?? r.status);
  return j.data;
}

const count = { writes: 0, ok: 0, stale: 0, missing: 0, write_errors: 0, read_errors: 0 };
const served = new Map();
const examples = [];
const errs = new Map();
const perSec = new Map();
const t0 = Date.now();
const end = t0 + SECS * 1000;
const note = (m, e) => { const k = `${m}: ${String(e.message ?? e).slice(0, 70)}`; errs.set(k, (errs.get(k) ?? 0) + 1); };

async function writer(w) {
  const value = new Array(KEYS).fill(0);
  let turn = w;
  for (let round = 0; Date.now() < end; round++) {
    const j = round % KEYS;
    const key = `raw${run}w${w}k${j}`;
    const ns = `raw-${run}-${(w * KEYS + j) % NSN}`;
    const next = value[j] + 1;
    const wnode = NODES[turn++ % NODES.length];
    try {
      if (value[j] === 0) await q(wnode, `STORE INTO SEMANTIC (key = "${key}", c = ${next}) NAMESPACE "${ns}"`);
      else await q(wnode, `UPDATE INTO SEMANTIC WHERE id = "${key}" (c = ${next}) NAMESPACE "${ns}"`);
    } catch (e) {
      count.write_errors++;
      note('write', e);
      // Unknown whether it landed: read back what is there before writing this key again.
      try {
        const d = await q(NODES[(turn + 1) % NODES.length], `RECALL FROM SEMANTIC WHERE id = "${key}" NAMESPACE "${ns}"`);
        const c = (d?.data?.records ?? d?.records)?.[0]?.data?.c;
        if (typeof c === 'number') value[j] = c;
      } catch {}
      continue;
    }
    value[j] = next;
    count.writes++;
    // Read back at once, through another node than the write's.
    const rnode = NODES[(NODES.indexOf(wnode) + 1 + (round % (NODES.length - 1))) % NODES.length];
    try {
      const d = await q(rnode, `RECALL FROM SEMANTIC WHERE id = "${key}" NAMESPACE "${ns}"`);
      const c = (d?.data?.records ?? d?.records)?.[0]?.data?.c;
      const by = d?.metadata?.served_by ?? 'owner';
      served.set(by, (served.get(by) ?? 0) + 1);
      if (c === undefined) {
        count.missing++;
        if (examples.length < 10) examples.push(`missing ${key} (wrote c=${next} via ${wnode}, read via ${rnode})`);
      } else if (c < next) {
        count.stale++;
        if (examples.length < 10) examples.push(`stale ${key}: read c=${c}, wrote c=${next} (via ${wnode}, read via ${rnode}, served by ${by})`);
      } else {
        count.ok++;
      }
      const s = Math.floor((Date.now() - t0) / 1000);
      perSec.set(s, (perSec.get(s) ?? 0) + 1);
    } catch (e) {
      count.read_errors++;
      note('read', e);
    }
  }
}

await Promise.all(Array.from({ length: W }, (_, w) => writer(w)));
const secs = (Date.now() - t0) / 1000;
console.log(`writes acknowledged ${count.writes} (${Math.round(count.writes / secs)}/s), read back ${count.ok + count.stale + count.missing}`);
console.log(`ok ${count.ok}, stale ${count.stale}, missing ${count.missing}, write errors ${count.write_errors}, read errors ${count.read_errors}`);
console.log(`answered by: ${[...served].map(([k, v]) => `${k} ${v}`).join(', ')}`);
console.log(`checks/s by second: ${[...perSec].sort((a, b) => a[0] - b[0]).map(([s, n]) => `${s}:${n}`).join(' ')}`);
for (const [m, n] of [...errs].sort((a, b) => b[1] - a[1]).slice(0, 6)) console.log(`  ${m} ×${n}`);
for (const e of examples) console.log(`  ${e}`);
console.log(count.stale + count.missing === 0 ? 'EVERY READ SAW THE WRITE BEFORE IT' : 'READ-AFTER-WRITE VIOLATED');
