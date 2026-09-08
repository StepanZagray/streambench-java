# Java backend

JDK 21's standard `jdk.httpserver` HTTP/1.1 server, a virtual thread per request,
and no third-party dependencies. Compile with `javac`; no Maven or Gradle.

From the repository root, using the included `data/events.jsonl`:

```sh
PORT=8080 DATA_DIR=data MAX_STREAMS=32 ./run.sh
docker build -f Dockerfile -t streambench-java .
```

`run.sh` selects a JDK whose `javac` reports version 21, first from `JAVA_HOME`,
then `/usr/lib/jvm/java-21-openjdk`. It compiles into `build` and
executes Java without changing the caller's working directory. The Dockerfile
uses `eclipse-temurin:21-jdk-jammy` to compile and
`eclipse-temurin:21-jre-jammy` to run as UID 10001. Its build context is the
repository root; it copies the integration fixture to `/app/data/events.jsonl`.

Defaults are `PORT=8080`, `DATA_DIR=data` relative to the working directory,
and `MAX_STREAMS=32`. Ports must be 1–65535 and stream limits positive Java
integers. Invalid configuration, unreadable/empty fixtures, blank records, or
invalid UTF-8 stop startup. The supplied fixture is assumed to follow the JSON
schema and sequential IDs in `docs/protocol.md`; records are not parsed or rewritten.
LF delimits records; all remaining bytes are preserved, including any CR.
The SHA-256 covers the entire original file, including its final LF if present.
The default request count remains 500, so it is rejected on shorter fixtures;
callers can explicitly request a smaller count.

All text frames and done frames for permitted counts are encoded once at
startup. Each request retains an index and references to shared frame arrays.
Each complete text frame is written and flushed immediately when due. There is
no producer queue or whole-response buffer. Paced/burst schedules use absolute
`System.nanoTime()` deadlines, with a 120-second maximum scheduled duration.
SSE responses use HTTP/1.1 chunking, disable proxy buffering, and close the
connection after the response. HTTP chunks are not SSE event boundaries.

A platform-thread watchdog limits each admitted stream to 130 seconds, covering
headers, writes, flushes, and close. It interrupts the virtual writer, which
closes a blocked interruptible `SocketChannel`, and releases admission exactly
once. Cancellation interrupts scheduled sleeps; write failures exit without
done. With this blocking API, disconnect detection occurs on the next I/O
operation (up to the next burst deadline), rather than via an independent peer
disconnect callback. Completed requests remove their watchdog tasks.

JDK 21's HttpServer writes hold a monitor and can pin virtual-thread carriers.
At startup the scheduler parallelism is set to the greater of available CPUs
and `MAX_STREAMS + 2` (capped at the JDK's 32767 carrier limit), preserving
capacity for health/info/admission while streams block. Its maximum pool size
is at least 256 and at least that parallelism. These are real worker-model
costs of this stack; comparisons should report them and the exact JDK version.

## Verification

```sh
/usr/lib/jvm/java-21-openjdk/bin/javac --release 21 --add-modules jdk.httpserver \
  -Xlint:all -d build \
  src/*.java tests/*.java
/usr/lib/jvm/java-21-openjdk/bin/java -XX:ActiveProcessorCount=2 \
  --add-modules jdk.httpserver -cp build StreambenchTest --stalled
sh -n run.sh
```

The native tests use temporary UTF-8 fixtures and an in-memory `HttpExchange`.
They cover routing, CORS/cache headers, strict queries, framing/hash, pacing,
admission, cancellation, empty/short fixtures, and two stalled virtual writers.
`--stalled` exercises the real 130-second watchdog; omit it for quick checks.
This verifies handler/watchdog behavior, not the JDK's real socket transport.

On a machine that permits loopback sockets, the shared Python stdlib harness
also exercises actual HTTP streaming and two nonreading TCP clients:

```sh
python3 tests/contract.py --backend java --stalled -- ./run.sh
```

The harness creates/removes temporary fixtures and captures, stops, and reaps
its server processes. Native compilation used OpenJDK 21.0.12.1. Real HTTP, disconnect and stalled-reader tests passed in the main workspace, including slot release and socket closure at the 130-second deadline. Docker image builds could not be run because the session has no Docker socket access.

Distributed source/client protocol and limits: [docs/distributed.md](distributed.md). Run `DistributedTest` after compiling the Java tests for additional lifecycle checks.
