# Native relay verification and benchmark

Measured 2026-10-02T22:50:14.519Z on win32 x64, 11th Gen Intel(R) Core(TM) i9-11900 @ 2.50GHz, 16 logical CPUs. Node v22.14.0. The verification session ran without administrator elevation.

## Correctness

- 17 process-level check groups passed against real Node and Rust processes.
- Three Rust tests passed for bounded lossy/reliable queues and malformed packet rejection.
- Public signed joins, broadcast, federation registration and actual Node catalog discovery passed with the compact native tunnel helper: **true**.
- The same public flow passed with the optional official cloudflared executable: **true**.
- Abrupt parent termination killed the Windows job-owned tunnel child: **true**. Parent stdin EOF and authenticated shutdown with stdin still open also passed.
- Cargo test, clippy with warnings denied, formatting, and release build passed.
- Every benchmark run asserted exact delivery counts and zero realtime drops. The five rounds delivered 2,100,000 measured frames per implementation, plus warm-up traffic.

## Comparison

Values below are medians of 5 rounds. Alternating order reduces first/second-run bias. The Node server is compiled from the actual roomRelayServer.ts source before launch; no TS loader participates in timing.

| Metric | Node relay | Rust sidecar |
|---|---:|---:|
| Startup (ms) | 175.93 | 44.75 |
| Idle working set (MiB) | 62.16 | 6.34 |
| Working set after workload (MiB) | 71.29 | 8.5 |
| Process CPU over three workloads (seconds) | 11.25 | 8.27 |
| Ping p95 round trip (ms) | 0.21 | 0.16 |

| Recipients | Node deliveries/s | Rust deliveries/s | Rust / Node | Node p95 delivery (ms) | Rust p95 delivery (ms) |
|---:|---:|---:|---:|---:|---:|
| 2 | 21,431.88 | 31,194.85 | 1.46× | 4.89 | 3.16 |
| 8 | 32,005.54 | 40,072.6 | 1.25× | 12.52 | 9.17 |
| 32 | 32,638.84 | 44,596.67 | 1.37× | 47.22 | 29.76 |

Observed per-round throughput ranges:

| Recipients | Node deliveries/s range | Rust deliveries/s range |
|---:|---:|---:|
| 2 | 16,557.31–24,704.6 | 21,668.24–39,127.54 |
| 8 | 28,085.18–36,107.93 | 28,860.51–42,908.08 |
| 32 | 27,125.07–34,070.65 | 42,085.65–45,533.84 |

The Rust relay uses 89.81% less idle working-set memory in these measurements. The single-thread runtime improved CPU use versus the initial multithread design; that earlier experiment is preserved in benchmark-multithread.json and summary-multithread.json.

## Package size

- Native release executable: 6.01 MiB (3.2 MiB gzip).
- Compact native tunnel helper measured separately: 6.25 MiB installed. Together: **12.26 MiB installed**, before packaging overhead. No Node runtime is needed.
- Node reference runtime alone: 79.48 MiB; bundled reference relay JS: 123.01 KiB.
- Optional cloudflared fallback measured separately: about 59.79 MiB installed. It is not included in the compact figure and is not required for the tested native-helper flow.
- Gzip measurements are component comparisons, not a prediction of the signed NSIS installer size.

## What this establishes

This validates a standalone, Node-free room relay plus tunnel/federation flow. The public flow used a real Cloudflare quick tunnel and the actual local CGP/Node relay registration plugin. Its explicitly configured registration socket was ws://127.0.0.1:7447; it did not rely on an unrelated public reverse-proxy route.

This repository is not yet wired into Hollow Desktop commands or bundled into an installer. It does not implement CGP log persistence, IPFS hosting, or SFU media processing. Both benchmarked room servers disabled SFU token issuance.

These are loopback measurements on a shared development machine, not internet throughput or capacity guarantees. A single Node client driver sends 10000 ingress frames per workload, with a 32-frame pipeline and 256-byte realtime payloads. Delivery latencies include client serialization, transport and decoding. The driver can limit throughput. Identical ingress rate limits are raised for both relays during benchmarking only; production defaults remain enforced and were tested separately.

## Reproduce

Run cargo build --release, cargo test, npm test, node scripts/job-lifetime.mjs, and npm run benchmark. See README.md for the explicit public tunnel smoke-test options. Raw measurements, delivery checks, fingerprints and environment are in benchmark.json.

Native measured executable SHA-256: 9c5cdbbb249527693bcf822f9fe0793d294a1c391d5cbaee845b7eb77d2f7bd2

Node source SHA-256: 7c87a063f0d725cc35b4508d01182eefaef5c7504e18e075f71e1d985be48a58

Rust room protocol SHA-256: 35ef43cbd91e0055cd04f19c94f0e54d2c26e2a73e5bc2e299e1cece4e2307f2
