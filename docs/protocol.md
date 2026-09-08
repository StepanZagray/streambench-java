# Streaming contract v1

All implementations replay the same pre-encoded UTF-8 fixture. This measures HTTP
stream delivery, scheduling, and concurrency; JSON serialization and model inference
are deliberately excluded. Each server loads `data/events.jsonl` once at startup.
Every line is compact JSON `{"seq":0,"text":"..."}` with sequential zero-based IDs.
No line contains literal newlines. Preserve bytes exactly. Reject empty fixtures.

## Configuration

- `PORT`: listen on 0.0.0.0, default 8080.
- `DATA_DIR`: default `data` relative to working directory.
- `MAX_STREAMS`: positive integer, default 32; active stream limit, admission returns
  429 without queueing when full. `/health` and `/info` must remain available.
- All endpoints set `Access-Control-Allow-Origin: *` (public read-only fixture),
  `Access-Control-Allow-Methods: GET, OPTIONS`, and `Cache-Control: no-store`.
- OPTIONS on known endpoints returns 204. Unsupported methods return 405; unknown
  paths return 404. Errors have JSON `{"error":"human-readable reason"}`.

## Endpoints

- `GET /health`: JSON `{"status":"ok"}`.
- `GET /info`: JSON with `backend` (java/go/rust/bun), `fixture_sha256` (SHA-256 of
  raw events.jsonl bytes), `fixture_events`, `max_streams`, and `active_streams`.
- `GET /stream?mode=paced&rate=50&count=500`.
  Defaults shown. Modes: `paced`, `burst`, `unpaced`. Rate is integer 1..1000.
  Count is integer 1..min(8192, fixture_events). Reject unknown or repeated query
  keys, empty values, non-decimal integers (only ASCII digits), and unsupported
  values with 400 **before** starting SSE. Last scheduled event must be <=120 seconds
  after the first (unpaced has no scheduled duration). Bound actual stream lifetime
  to 130 seconds, including slow writes; stop producing on client disconnection.

## Wire format

Content-Type: `text/event-stream; charset=utf-8`; `X-Accel-Buffering: no`.
No compression. Flush/offer each complete event promptly. Do not buffer the whole
response or use an unbounded producer queue. Streaming responses use
`Connection: close` on HTTP/1.1 in all four backends; connection reuse across
streams is excluded from this baseline. Pre-encode complete frames at startup.
The first event is immediate, with no metadata or heartbeat before it:

```text
event: text
data: {"seq":0,"text":"the first fragment"}

event: text
data: {"seq":1,"text":"the next fragment"}

event: done
data: {"count":2}

```

Exact framing is `event: text\ndata: ` + fixture line + `\n\n`.
Done framing is `event: done\ndata: {"count":N}\n\n`, then close response.
For `count=N`, send the first N fixture lines, once each, in order.
On timeout/disconnect end without a done event; clients must classify it as incomplete.

Use monotonic time and absolute deadlines relative to stream start:
- paced event i: i / rate seconds;
- burst event i: floor(i / 10) * 10 / rate seconds (groups of 10);
- unpaced: no intentional delay.

If behind schedule, proceed immediately; do not add a fresh interval to every
write. Cancellation interrupts timers. An active slot is released on completion,
failure, cancellation, or timeout. No per-event logging. Health logging may be omitted.

## Scope of comparison

Framework I/O and worker models differ and must be documented. Compare identified
stack versions under these settings, not languages in the abstract. Browser read
chunks are not event boundaries. SHA-256 identifies fixtures across deployments.
