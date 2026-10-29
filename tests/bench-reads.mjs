#!/usr/bin/env node
// W7: read latency on a seeded ADB (HTTP /query). Seeds N SEMANTIC facts (topic,
// n, content), 200 EPISODIC events with links from the first 50 facts, and 200
// WORKING items; then times each read shape (median and p95 of R runs).
//
//   node tests/bench-reads.mjs --adb target/debug/adb [--rows 10000] [--runs 30]
//     [--cache all|default|off]   (W10: hot cache rules applied before timing)
//   (extra env for the server, e.g. ADB_CACHE=shadow, is passed through)

import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import net from "node:net";

const arg = (k, d) => {
  const i = process.argv.indexOf(`--${k}`);
  return i > 0 ? process.argv[i + 1] : d;
};
const ADB = arg("adb");
const ROWS = Number(arg("rows", 10000));
const RUNS = Number(arg("runs", 30));
if (!ADB) {
  console.error("--adb <binary> is required");
  process.exit(2);
}

const port = await new Promise((res) => {
  const s = net.createServer().listen(0, () => {
    const p = s.address().port;
    s.close(() => res(p));
  });
});
const dir = mkdtempSync(join(tmpdir(), "adb-bench-"));
const server = spawn(ADB, ["serve-http", "--port", String(port), "--flight-port", "0"], {
  env: { ...process.env, ADB_DATA_DIR: dir, ADB_EMBED: "off", RUST_LOG: process.env.RUST_LOG ?? "warn" },
  stdio: ["ignore", "inherit", "inherit"],
});
const url = `http://127.0.0.1:${port}`;
// A failed read must not leave the server running (it holds our stdout).
const die = (e) => {
  console.error(e);
  server.kill("SIGKILL");
  process.exit(1);
};
process.on("uncaughtException", die);
process.on("unhandledRejection", die);
process.on("exit", () => server.kill("SIGKILL"));
for (let i = 0; i < 200; i++) {
  try {
    if ((await fetch(`${url}/health`)).ok) break;
  } catch {}
  await new Promise((r) => setTimeout(r, 100));
}

async function q(query, variables) {
  const r = await fetch(`${url}/query`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ query, variables }) });
  const j = await r.json();
  if (!j.success) throw new Error(`${query}: ${j.error}`);
  return j;
}

// Seed, 32 writes in flight at a time.
async function seed(queries) {
  for (let i = 0; i < queries.length; i += 32) await Promise.all(queries.slice(i, i + 32).map((s) => q(s)));
}
const t0 = Date.now();
await seed(Array.from({ length: ROWS }, (_, i) => `STORE INTO SEMANTIC (key = "f${i}", topic = "t${i % 20}", n = ${i % 997}, content = "fact number ${i} about topic t${i % 20}")`));
await seed(Array.from({ length: 200 }, (_, i) => `STORE INTO EPISODIC (key = "e${i}", kind = "event", content = "event ${i}")`));
await seed(Array.from({ length: 200 }, (_, i) => `STORE INTO WORKING (key = "w${i}", priority = ${i % 10})`));
await seed(Array.from({ length: 50 }, (_, i) => `LINK FROM SEMANTIC WHERE key = "f${i}" TO EPISODIC WHERE key = "e${i}" TYPE "about"`));
console.log(`seeded ${ROWS} facts in ${((Date.now() - t0) / 1000).toFixed(1)} s (ADB_QUERY=${process.env.ADB_QUERY ?? "default"})`);
if (process.argv.includes("--compact")) {
  // FORGET compacts before it returns (W5): one throwaway record.
  const t1 = Date.now();
  await q(`STORE INTO SEMANTIC (key = "bench-throwaway", content = "x")`);
  await q(`FORGET FROM SEMANTIC WHERE key = "bench-throwaway"`);
  console.log(`compacted in ${((Date.now() - t1) / 1000).toFixed(1)} s`);
}

// W10: `all` keeps every memory type whole; `off` keeps nothing; `default` as is.
const CACHE = arg("cache", "default");
if (CACHE !== "default") {
  const memories = ["working", "tools", "procedural", "semantic", "episodic"];
  const keep = CACHE === "all" ? "all" : "none";
  const r = await fetch(`${url}/admin/cache`, {
    method: "PUT",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ max_bytes: "1GB", rules: memories.map((memory) => ({ memory, keep })) }),
  });
  const j = await r.json();
  if (!j.success) throw new Error(`cache rules: ${j.error}`);
  console.log(`cache ${CACHE}: ${j.data.stats?.rows ?? 0} rows, ${((j.data.stats?.bytes ?? 0) / 1048576).toFixed(1)} MB, loaded in ${j.data.took_ms} ms`);
}

const reads = {
  "LOOKUP KEY": `LOOKUP FROM SEMANTIC KEY id = "f4242"`,
  "RECALL WHERE (pushed down)": `RECALL FROM SEMANTIC WHERE topic = "t7" LIMIT 10`,
  "RECALL WHERE != (not pushed)": `RECALL FROM SEMANTIC WHERE topic != "t7" LIMIT 10`,
  "RECALL ALL ORDER BY LIMIT 10": `RECALL FROM SEMANTIC ALL ORDER BY n DESC LIMIT 10`,
  "WITH LINKS (50 records)": `RECALL FROM SEMANTIC WHERE n < 50 AND topic = "t1" WITH LINKS ALL`,
  "FOLLOW LINKS": `RECALL FROM SEMANTIC WHERE topic = "t3" FOLLOW LINKS TYPE "about"`,
  "SCAN WORKING WINDOW": `SCAN FROM WORKING WINDOW LAST 10`,
  "RECALL WORKING KEY": `RECALL FROM WORKING KEY id = "w42"`,
  "RECALL EPISODIC newest 10": `RECALL FROM EPISODIC ALL LIMIT 10`,
  "RECALL EPISODIC KEY": `RECALL FROM EPISODIC KEY id = "e42"`,
  "AGGREGATE COUNT": `RECALL FROM SEMANTIC WHERE topic = "t5" AGGREGATE COUNT(*) AS c`,
  // Words only (the server runs with ADB_EMBED=off): ranks every fact.
  "LIKE (words, 10k)": [`RECALL FROM SEMANTIC LIKE $q LIMIT 10`, { q: "fact about topic t7" }],
};
const stat = (xs) => {
  const s = [...xs].sort((a, b) => a - b);
  return { median: s[Math.floor(s.length / 2)], p95: s[Math.floor(s.length * 0.95)] };
};
console.log("read".padEnd(32), "median ms".padStart(10), "p95 ms".padStart(10));
for (const [name, query] of Object.entries(reads)) {
  const [aql, vars] = Array.isArray(query) ? query : [query];
  await q(aql, vars); // warm
  const times = [];
  for (let i = 0; i < RUNS; i++) {
    const t = performance.now();
    await q(aql, vars);
    times.push(performance.now() - t);
  }
  const { median, p95 } = stat(times);
  console.log(name.padEnd(32), median.toFixed(1).padStart(10), p95.toFixed(1).padStart(10));
}
server.kill("SIGTERM");
await new Promise((r) => server.on("exit", r));
rmSync(dir, { recursive: true, force: true });
