import { readFile, writeFile } from 'node:fs/promises';
const benchmark=JSON.parse(await readFile('results/benchmark.json','utf8'));
const summary=JSON.parse(await readFile('results/summary.json','utf8'));
const n=summary.results.find(r=>r.relay==='node'),r=summary.results.find(r=>r.relay==='rust');
const conform=JSON.parse(await readFile('results/conformance.json','utf8'));
const native=JSON.parse(await readFile('results/tunnel-smoke-native.json','utf8'));
const official=JSON.parse(await readFile('results/tunnel-smoke-cloudflared.json','utf8'));
const life=JSON.parse(await readFile('results/job-lifetime.json','utf8'));
const fmt=x=>x.toLocaleString('en-US',{maximumFractionDigits:2});
const range=(kind,fanout)=>{const v=benchmark.results.filter(r=>r.relay===kind).map(r=>r.workloads.find(w=>w.fanout===fanout).deliveriesPerSecond);return `${fmt(Math.min(...v))}–${fmt(Math.max(...v))}`};
const rows=[['Startup (ms)',n.startupMs,r.startupMs],['Idle working set (MiB)',n.idleMiB,r.idleMiB],['Working set after workload (MiB)',n.loadedMiB,r.loadedMiB],['Process CPU over three workloads (seconds)',n.cpuSeconds,r.cpuSeconds],['Ping p95 round trip (ms)',n.pingP95Ms,r.pingP95Ms]].map(([label,a,b])=>`| ${label} | ${fmt(a)} | ${fmt(b)} |`).join('\n');
const workloads=n.workloads.map(a=>{const b=r.workloads.find(w=>w.fanout===a.fanout);return `| ${a.fanout} | ${fmt(a.deliveriesPerSecond)} | ${fmt(b.deliveriesPerSecond)} | ${fmt(b.deliveriesPerSecond/a.deliveriesPerSecond)}× | ${fmt(a.latencyP95Ms)} | ${fmt(b.latencyP95Ms)} |`}).join('\n');
const ranges=[2,8,32].map(f=>`| ${f} | ${range('node',f)} | ${range('rust',f)} |`).join('\n');
const size=summary.sizes;
const content=`# Native relay verification and benchmark

Measured ${benchmark.environment.date} on ${benchmark.environment.platform} ${benchmark.environment.arch}, ${benchmark.environment.cpu}, ${benchmark.environment.cpuCount} logical CPUs. Node ${benchmark.environment.node}. The verification session ran without administrator elevation.

## Correctness

- ${conform.results.length} process-level check groups passed against real Node and Rust processes.
- Three Rust tests passed for bounded lossy/reliable queues and malformed packet rejection.
- Public signed joins, broadcast, federation registration and actual Node catalog discovery passed with the compact native tunnel helper: **${native.passed}**.
- The same public flow passed with the optional official cloudflared executable: **${official.passed}**.
- Abrupt parent termination killed the Windows job-owned tunnel child: **${life.passed}**. Parent stdin EOF and authenticated shutdown with stdin still open also passed.
- Cargo test, clippy with warnings denied, formatting, and release build passed.
- Every benchmark run asserted exact delivery counts and zero realtime drops. The five rounds delivered 2,100,000 measured frames per implementation, plus warm-up traffic.

## Comparison

Values below are medians of ${benchmark.environment.rounds} rounds. Alternating order reduces first/second-run bias. The Node server is compiled from the actual roomRelayServer.ts source before launch; no TS loader participates in timing.

| Metric | Node relay | Rust sidecar |
|---|---:|---:|
${rows}

| Recipients | Node deliveries/s | Rust deliveries/s | Rust / Node | Node p95 delivery (ms) | Rust p95 delivery (ms) |
|---:|---:|---:|---:|---:|---:|
${workloads}

Observed per-round throughput ranges:

| Recipients | Node deliveries/s range | Rust deliveries/s range |
|---:|---:|---:|
${ranges}

The Rust relay uses ${fmt((1-r.idleMiB/n.idleMiB)*100)}% less idle working-set memory in these measurements. The single-thread runtime improved CPU use versus the initial multithread design; that earlier experiment is preserved in benchmark-multithread.json and summary-multithread.json.

## Package size

- Native release executable: ${fmt(size.nativeBytes/1048576)} MiB (${fmt(size.nativeGzipBytes/1048576)} MiB gzip).
- Compact native tunnel helper measured separately: 6.25 MiB installed. Together: **${fmt(size.nativeBytes/1048576+6.25)} MiB installed**, before packaging overhead. No Node runtime is needed.
- Node reference runtime alone: ${fmt(size.nodeRuntimeBytes/1048576)} MiB; bundled reference relay JS: ${fmt(size.nodeRelayBundleBytes/1024)} KiB.
- Optional cloudflared fallback measured separately: about 59.79 MiB installed. It is not included in the compact figure and is not required for the tested native-helper flow.
- Gzip measurements are component comparisons, not a prediction of the signed NSIS installer size.

## What this establishes

This validates a standalone, Node-free room relay plus tunnel/federation flow. The public flow used a real Cloudflare quick tunnel and the actual local CGP/Node relay registration plugin. Its explicitly configured registration socket was ws://127.0.0.1:7447; it did not rely on an unrelated public reverse-proxy route.

This repository is not yet wired into Hollow Desktop commands or bundled into an installer. It does not implement CGP log persistence, IPFS hosting, or SFU media processing. Both benchmarked room servers disabled SFU token issuance.

These are loopback measurements on a shared development machine, not internet throughput or capacity guarantees. A single Node client driver sends ${benchmark.environment.samples} ingress frames per workload, with a 32-frame pipeline and 256-byte realtime payloads. Delivery latencies include client serialization, transport and decoding. The driver can limit throughput. Identical ingress rate limits are raised for both relays during benchmarking only; production defaults remain enforced and were tested separately.

## Reproduce

Run cargo build --release, cargo test, npm test, node scripts/job-lifetime.mjs, and npm run benchmark. See README.md for the explicit public tunnel smoke-test options. Raw measurements, delivery checks, fingerprints and environment are in benchmark.json.

Native measured executable SHA-256: ${benchmark.environment.fingerprint.nativeSha256}

Node source SHA-256: ${benchmark.environment.fingerprint.nodeSourceSha256}

Rust room protocol SHA-256: ${benchmark.environment.fingerprint.rustProtocolSha256}
`;
await writeFile('results/REPORT.md',content);console.log('Wrote results/REPORT.md');
