// Run perf-remote.mjs as P client processes (one Node thread can't load an 8-vCPU
// cluster) and merge their results: throughput and errors summed, latency
// percentiles averaged over the processes that took part, server timings from worker 0.
//   node tests/perf-multi.mjs --procs 4 <perf-remote arguments…>
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
const argv = process.argv.slice(2);
const i = argv.indexOf('--procs');
const P = i >= 0 ? Number(argv[i + 1]) : 4;
const rest = i >= 0 ? [...argv.slice(0, i), ...argv.slice(i + 2)] : argv;
const tool = path.join(path.dirname(fileURLToPath(import.meta.url)), 'perf-remote.mjs');
const runs = Array.from({ length: P }, (_, k) => new Promise(resolve => {
  const ch = spawn(process.execPath, [tool, ...rest, '--procs', String(P), '--worker', String(k)], { stdio: ['ignore', 'pipe', 'inherit'] });
  let out = '';
  ch.stdout.on('data', d => {
    out += d;
    // Worker 0's header and progress lines, as they come.
    if (k === 0) for (const l of String(d).split('\n')) if (l && !l.startsWith('RESULTS') && !/^\s+\S.*c=\d/.test(l)) console.log(l);
  });
  ch.on('close', () => resolve(out));
}));
const outs = await Promise.all(runs);
const all = outs.map(o => JSON.parse((o.split('\n').find(l => l.startsWith('RESULTS ')) ?? 'RESULTS []').slice(8)));
const merged = all[0].map((r0, j) => {
  const rs = all.map(a => a[j]).filter(Boolean);
  const busy = rs.filter(r => !r.idle && r.ops > 0);
  const avg = f => busy.length ? busy.reduce((s, r) => s + r[f], 0) / busy.length : 0;
  return { name: r0.name, c: r0.c, ops: rs.reduce((s, r) => s + r.ops, 0), p50: avg('p50'), p90: avg('p90'), p99: avg('p99'), errors: rs.reduce((s, r) => s + (r.errors ?? 0), 0), server: (rs.find(r => r.server) ?? {}).server ?? '' };
});
for (const r of merged) {
  console.log(`  ${r.name.padEnd(38)} c=${String(r.c).padEnd(3)} ${String(r.ops).padStart(6)}/s  p50 ${r.p50.toFixed(1).padStart(6)} ms  p90 ${r.p90.toFixed(1).padStart(6)}  p99 ${r.p99.toFixed(1).padStart(7)}${r.errors ? `  errors ${r.errors}` : ''}${r.server ?? ''}  [${P} procs]`);
}
console.log('RESULTS ' + JSON.stringify(merged));
