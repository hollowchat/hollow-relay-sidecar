# Hollow relay sidecar

A native Rust implementation of Hollow's signed WebSocket room relay. Node is
not needed to run it. Node is used by the verification scripts to launch the
existing `hollow-relay-plugin` implementation as the reference server.

This repository supplies the native relay process bundled by Hollow Desktop.
It also provides standalone verification and benchmark scripts. It implements
the room transport; it does not replace the complete
CGP event/log server, IPFS, or an SFU media server. Both sides of the benchmark
run the room relay with SFU token issuance disabled.

## Build and run

```powershell
cargo build --release
.\target\release\hollow-relay-sidecar.exe --port 0
```

The relay binds only to `127.0.0.1`, requires signed joins by default, and emits
one JSON `ready` line with its address and PID. It launches no tunnel unless
explicitly configured. It needs no elevation, Windows service installation, or
firewall rule for the localhost plus outbound-tunnel design.

For a parent-managed process, pass `--exit-on-stdin-close` and leave its stdin
pipe open. The relay shuts down when the parent closes or loses that pipe.
Tauri can launch this executable directly, parse its readiness line, and own the
stdin pipe; it does not need to launch JavaScript or a Node runtime.

## Control interface

Pass `--state-file` inside the current user's private AppData directory to save
the local control address and randomly generated token. Alternatively pass the
token in the child's `HOLLOW_RELAY_CONTROL_TOKEN` environment variable. Never
put that token in a public URL, relay catalog, or frontend-served file.

Local endpoints:

- `GET /health`, `/`, `/info`: public protocol and room counters.
- `GET /service`: authenticated local hosting/tunnel/registration status.
- `POST /refresh`: authenticated tunnel refresh request.
- `POST /shutdown`: authenticated graceful shutdown.
- `GET /relay`: WebSocket upgrade for the Hollow room protocol.

Control requests require the `x-hollow-relay-service-token` header. No CORS
permission is granted for the control interface. Use a private state directory
on Windows (inherited per-user ACL); newly created Unix state files use mode 0600.

## Tunnel and registration

The existing Rust tunnel helper can be supplied explicitly:

```powershell
.\target\release\hollow-relay-sidecar.exe `
  --exit-on-stdin-close `
  --state-file "$env:LOCALAPPDATA\Hollow\relay\state.json" `
  --tunnel-helper "..\hollow-relay-plugin\bin\hollow-relay-tunnel.exe" `
  --upstream https://relay.hollow.to
```

The helper must pass a real public WebSocket/WELCOME probe before the native
relay advertises it or requests federation registration. Registration uses the
existing session endpoint and `HOLLOW_WS_RELAY_REGISTER` / heartbeat frames.
The upstream must acknowledge registration; merely sending a frame does not
make status say registered. Failed tunnels and registration retry with delay.

The tunnel child is killed on managed shutdown. On Windows it starts suspended,
is assigned to a kill-on-close Job Object, and only then resumes. Abrupt sidecar
termination also ends its tunnel child. Job assignment failures kill the
suspended child and report a setup error instead of launching an unmanaged host.

Pass `--cloudflared PATH` to use the official tunnel executable. With both flags,
it is an optional fallback when the compact native helper fails. The official
path uses HTTP/2 for networks where QUIC is unavailable. Neither path requires
Node. Public hostname publication can lag the helper's ready line: probes retry
for up to two minutes, with local status remaining unavailable until WELCOME
succeeds. A failed hostname is not advertised as a healthy relay.

`--upstream-ws URL` overrides the registration WebSocket endpoint returned by the
upstream session API. This is useful for a local reference relay whose metadata
advertises a separate public proxy; configure it explicitly, not by silently
rebasing an upstream's advertised URL.

## Protocol and limits

Supported frames: `PING`, `JOIN`, `LEAVE`, `UPDATE`, `DIRECT`, `BROADCAST`,
`REALTIME`, and `ROOMS`, with the existing response envelopes and identity fields.

- SHA-256/secp256k1 compact low-S signed joins, scoped to room/peer/nonce/time.
- Replay rejection, identity ownership, same-identity reconnect replacement.
- Five realtime lanes, payload/deadline validation, canonical sender identity.
- Targeted, bounded, and distance-filtered fan-out; metadata position updates.
- Per-connection message/byte token buckets and connection/room/member limits.
- Lossy queues bounded to 256 KiB; reliable queues bounded to 2 MiB. Reliable
  slow consumers close with code 1013 instead of accumulating unlimited memory.

Environment settings match the Node room relay's `HOLLOW_RELAY_*` limit names.
Oversized WebSocket frames can close at the transport boundary rather than
producing a JSON error. Malformed base64 is rejected strictly; the native relay
does not copy the Node implementation's permissive malformed-base64 behavior.

## Verification and benchmarking

Install the reference repo's dependencies first. Its default location is the
sibling `../hollow-relay-plugin`; override with `HOLLOW_NODE_REPO` if needed.

```powershell
cargo test
npm test
npm run benchmark
node scripts/tunnel-smoke.mjs
node scripts/job-lifetime.mjs
```

The conformance script starts real Node and Rust relay processes and exercises
signed authorization, ownership, routing, all realtime lanes, positions,
limits, cleanup, and authenticated native controls. Cargo tests verify queue
bounds and malformed packet rejection. The tunnel smoke test explicitly creates
a temporary public tunnel and relay registration; its default upstream is the
local CGP relay at `http://127.0.0.1:7447`. Override `HOLLOW_TEST_UPSTREAM` to test
another configured relay. All test-owned processes are stopped afterward.
Set `HOLLOW_TEST_CLOUDFLARED` to test the optional official tunnel executable and
`HOLLOW_TEST_UPSTREAM_WS` to configure the explicit registration socket override.

The benchmark compiles the actual Node room-relay source before launching Node,
so TypeScript tooling is not included in startup measurements. Five rounds use
alternating implementation order, identical raised ingress limits, signed joins,
256-byte realtime payloads, a 32-message pipeline, and 2/8/32 recipients.
Latency includes client serialization, loopback transport, and client decoding.
The single Node load generator may cap measured throughput; these are local
comparisons, not internet capacity claims. Default production rate limits are
not disabled outside the benchmark. Results include delivery assertions and
drop counters, process CPU and Windows working-set/private-memory samples.

Raw measurements and summaries are in `results/`. Initial multithread-runtime
results are retained separately; the current default uses a single-thread Tokio
runtime to avoid scheduler and shared-state contention for this workload.

See [the measured verification report](results/REPORT.md) for final results,
package sizes, test coverage, and the limits of the comparison.
