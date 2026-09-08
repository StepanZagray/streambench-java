# Distributed shared-stream protocol v1

All four implementations can be source servers or remote clients. This is shared
text replay, not model inference or GPU request batching. The existing `/stream`
contract remains unchanged. Only one distributed source batch and one client job
may be active per service; these are bounded demonstration workloads, not capacity limits.

The browser selects a source, wakes/verifies all four backends, creates a batch,
starts four remote clients distributed 2+1+1 across the other three servers, and
opens client 0 itself. Thus five SSE connections reach the source, but only one
text stream traverses the browser's network. A single producer advances the fixture
once and broadcasts each immutable pre-encoded frame to the five subscribers.

## Routes (identical on every backend)

Control requests have no body. Query integers are strict unsigned decimal, IDs
and unknown/repeated query keys must be validated. Responses are JSON with
`Cache-Control: no-store`, public CORS; OPTIONS returns 204. New routes allow
GET, POST, DELETE, OPTIONS as appropriate, with 405 for unsupported methods.
Errors are `{ "error": "message" }` with 400/404/405/409/429 as appropriate.
Identifiers are cryptographically random 32 lowercase hex characters.

- `POST /batch?mode=paced&rate=50&count=500`: validate the ordinary stream parameters,
  allocate one batch, return 201 and the batch snapshot below. Reject with 409 if
  another batch still has active or waiting work. Exactly five clients are required.
- `GET /batch/{session}`: return its current snapshot (404 if unknown).
- `DELETE /batch/{session}`: cancel producer and close its subscribers, return 200
  snapshot. Idempotent for a known terminal session; do not rewrite a completed
  result as cancelled. Browser calls this on Stop/error/cleanup.
- `GET /batch/{session}/stream?client=N`: N is 0..4 and may join once only. Admit
  against the same MAX_STREAMS cap as /stream. Flush SSE headers and a comment
  `: waiting\n\n` immediately, register the subscriber, and wait for all five.
  Duplicate IDs and joining after production starts return 409. Unknown ID 404.
  Once all five have joined, one producer starts its monotonic paced/burst/unpaced
  schedule. Text/done frame bytes and counts match the existing /stream exactly.
- `POST /clients?source=go&session=HEX&ids=1,2&count=500`: start an asynchronous
  remote-client job and return 202 snapshot. Source is one of go/java/rust/bun;
  IDs are unique members of 1..4, at most four per job. Count 1..min(8192,fixture).
  Unknown/repeated keys, an invalid source/session/IDs/count, or any target URL
  supplied as a query parameter must fail before making an outbound request.
  The same active session returns its snapshot only if source/IDs/count match;
  a different job while active returns 409. Terminal jobs may be replaced.
- `GET /clients/{session}`: return remote client metrics (404 if unknown).
- `DELETE /clients/{session}`: cancel all outstanding HTTP clients, return 200
  snapshot; preserve already terminal results. Idempotent for known session.

Batch snapshot:
```json
{"session":"32hex","state":"waiting","expected_clients":5,"connected_clients":0,"produced_events":0,"error":null}
```
`state`: waiting, streaming, complete, failed, cancelled. connected_clients counts
currently connected subscribers, not historical arrivals. produced_events counts
producer iterations (count for a successful batch), never sum of fanout writes.
Complete only after all five subscribers successfully finish; partial disconnect,
queue overflow or timeout makes the final outcome failed. Keep at most the latest
batch snapshot; do not evict a still-active batch to make room for a new one.

Client-job snapshot:
```json
{"session":"32hex","state":"running","clients":[{"id":1,"state":"connecting","events":0,"first_event_ms":null,"p95_gap_ms":null,"elapsed_ms":0,"error":null}]}
```
Job state: running, complete, failed, cancelled. Client state: connecting,
streaming, complete, failed, cancelled. Metrics use each client's own monotonic
clock from request initiation, first COMPLETE text event, nearest-rank p95 of
complete text event gaps, elapsed to completion/failure. Do not subtract clocks
across machines. Consume incrementally, validate seq/text against local fixture,
require exact terminal done count and no extra/truncated frames. Count failures;
never report premature EOF as complete. Ignore SSE comments and handle split or
coalesced network reads. Bound unparsed input to 1 MiB; retain numeric gaps only,
not received text. Disable redirects and use a finite <=180s request deadline.

## Source lifecycle and backpressure

Join window is 45 seconds from creation. If five clients do not arrive, fail and
close all subscribers. The producer schedule is <=120 seconds (ordinary bounds).
An absolute <=180 second deadline from creation closes every socket and releases
all slots, including nonreading consumers. Cancel/timeout must work independently
of blocked HTTP writes. Completed/disconnected subscribers release admission once.
One producer broadcasts to bounded per-client queues of at most 256 frames. Queue
overflow fails/disconnects that client; it never silently drops frames or blocks
the producer indefinitely. Yield between unpaced producer iterations so healthy
readers can drain. The source records failure while letting remaining subscribers
finish. Queues carry references to shared immutable frames, not whole-stream copies.

## Peer origins

Only a fixed backend-key allowlist may be contacted. Defaults:
- go: https://streambench-go-1.onrender.com
- java: https://streambench-java.onrender.com
- rust: https://streambench-rust.onrender.com
- bun: https://streambench-bun.onrender.com

Operators may override PEER_GO_URL, PEER_JAVA_URL, PEER_RUST_URL, PEER_BUN_URL at
startup for local integration tests. Require HTTP(S) origins, no credentials,
query, fragment or non-root path. Never accept arbitrary outbound URLs from callers.
No worker recursively creates another worker job: it only GETs the source's batch
stream. Client/source jobs may coexist on a server; health must remain responsive.

Only client summary JSON is polled by the browser (about once a second), not remote
text. Source generation count and five individual outcomes are saved in exports.
A stopped browser request must trigger DELETE cleanup of source and worker jobs;
server deadlines bound orphaned work if the browser disappears entirely.

Stop can race with a subscriber disconnect or premature EOF. Preserve failures
already recorded by a source or worker; the browser labels the requested run
Stopped and still cleans up every outstanding connection.
