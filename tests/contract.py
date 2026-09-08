#!/usr/bin/env python3
"""Black-box protocol checks for either native backend; stdlib only.

Run from this repository root, after building:
  python3 tests/contract.py --backend java --stalled -- ./run.sh
The optional stalled-reader check takes the real 130-second lifetime.
"""

import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import tempfile
import time


def check(condition, detail):
    if not condition:
        raise AssertionError(detail)


class Server:
    def __init__(self, command, data, work, extra=None):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            self.port = probe.getsockname()[1]
        env = os.environ.copy()
        env.update(PORT=str(self.port), DATA_DIR=str(data), MAX_STREAMS="2")
        env.update(extra or {})
        self.log = tempfile.TemporaryFile()
        self.process = subprocess.Popen(command, cwd=work, env=env, stdout=self.log,
                                        stderr=self.log, start_new_session=True)
        print(f"started test PID {self.process.pid} on port {self.port}", flush=True)

    def request(self, path, status=200, method="GET"):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        try:
            connection.request(method, path)
            response = connection.getresponse()
            body = response.read()
            check(response.status == status, (method, path, response.status, body[:200]))
            for name, value in [("Access-Control-Allow-Origin", "*"),
                                ("Access-Control-Allow-Methods", "GET, OPTIONS"),
                                ("Cache-Control", "no-store")]:
                check(response.getheader(name) == value, (path, name, response.getheaders()))
            if status >= 400 and method != "HEAD":
                check(response.getheader("Content-Type").startswith("application/json"), path)
                error = json.loads(body)
                check(set(error) == {"error"} and isinstance(error["error"], str), error)
            return response, body
        finally:
            connection.close()

    def ready(self):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            check(self.process.poll() is None, "server exited during startup")
            try:
                self.request("/health")
                return
            except (ConnectionError, OSError):
                time.sleep(0.05)
        raise AssertionError("server did not become ready")

    def active(self):
        return json.loads(self.request("/info")[1])["active_streams"]

    def wait_active(self, expected, timeout=5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = self.active()
            if value == expected:
                return
            time.sleep(0.025)
        raise AssertionError(f"active_streams={value}, expected {expected}")

    def stream(self, query):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        connection.request("GET", "/stream?" + query)
        response = connection.getresponse()
        if response.status != 200:
            raise AssertionError((query, response.status, response.read()))
        return connection, response

    def stop(self):
        pid = self.process.pid
        if self.process.poll() is None:
            os.killpg(pid, signal.SIGTERM)
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(pid, signal.SIGKILL)
                self.process.wait(timeout=5)
        check(not Path(f"/proc/{pid}").exists(), f"test PID {pid} survived cleanup")
        self.log.seek(0)
        log = self.log.read().decode(errors="replace")
        self.log.close()
        print(f"stopped and reaped test PID {pid}", flush=True)
        return log


def expected(lines, count):
    return b"".join(b"event: text\ndata: " + line + b"\n\n" for line in lines[:count]) + (
        f'event: done\ndata: {{"count":{count}}}\n\n'.encode())


def event(response):
    parts = []
    while True:
        line = response.readline()
        check(line, "unexpected EOF inside an SSE event")
        parts.append(line)
        if line == b"\n":
            return b"".join(parts)


def abort(connection, response):
    # Close the exact stream socket with RST, including on HTTP Connection: close.
    sock = response.fp.raw._sock
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    response.close()
    connection.close()


def contract(server, backend, lines, raw):
    check(json.loads(server.request("/health")[1]) == {"status": "ok"}, "health")
    info = json.loads(server.request("/info")[1])
    check(info == {"backend": backend, "fixture_sha256": hashlib.sha256(raw).hexdigest(),
                   "fixture_events": len(lines), "max_streams": 2, "active_streams": 0}, info)
    for path in ["/health", "/info", "/stream"]:
        check(server.request(path, 204, "OPTIONS")[1] == b"", "OPTIONS body")
        for method in ["POST", "PUT", "PATCH", "DELETE", "HEAD"]:
            response, _ = server.request(path, 405, method)
            check(response.getheader("Allow") == "GET, OPTIONS", "Allow")
    for path in ["/", "/stream/", "/health-extra", "/%68ealth", "/unknown"]:
        for method in ["GET", "OPTIONS", "POST"]:
            server.request(path, 404, method)

    invalid = ["mode=", "rate=", "count=", "mode=fast", "mode=Paced", "x=1", "rate",
               "rate=1&rate=2", "rate=1&%72ate=2", "mode=burst&mode=paced", "count=1&count=2",
               "rate=0", "rate=1001", "rate=-1", "rate=+1", "rate=1.0", "rate=1e2",
               "rate=%201", "rate=1%20", "rate=%EF%BC%91", "rate=%FF", "count=0", "count=8193",
               "count=-1", "count=1.0", "count=+1", "count=9999999999999999999999999999",
               "rate=1&count=122", "mode=burst&rate=1&count=131", "count=2&", "&count=2",
               "count=2&&rate=1", "=1", "count=1=2", "mode=unpaced&rate=1001"]
    for query in invalid:
        server.request("/stream?" + query, 400)
    server.wait_active(0)

    for query, count in [("mode=unpaced&count=1", 1), ("mode=unpaced", 500),
                         ("mode=unpaced&count=8192", 8192),
                         ("mode=unpaced&%63ount=0002&rate=0001", 2)]:
        response, body = server.request("/stream?" + query)
        check(response.getheader("Content-Type") == "text/event-stream; charset=utf-8", "SSE type")
        check(response.getheader("X-Accel-Buffering") == "no", "buffering")
        check(response.getheader("Content-Encoding") is None, "compression")
        check(body == expected(lines, count), f"wire bytes: {query}")
        server.wait_active(0)

    for query, offsets in [("mode=paced&rate=20&count=6", [i / 20 for i in range(6)]),
                           ("mode=burst&rate=20&count=21", [(i // 10) / 2 for i in range(21)]),
                           ("count=6", [i / 50 for i in range(6)])]:
        start = time.monotonic()
        connection, response = server.stream(query)
        try:
            for i, offset in enumerate(offsets):
                check(event(response) == b"event: text\ndata: " + lines[i] + b"\n\n", query)
                elapsed = time.monotonic() - start
                check(offset - 0.03 <= elapsed <= offset + 0.5, (query, i, elapsed, offset))
            check(event(response) == f'event: done\ndata: {{"count":{len(offsets)}}}\n\n'.encode(), query)
            check(response.read() == b"", "bytes after done")
        finally:
            response.close()
            connection.close()
        server.wait_active(0)

    # Inclusive schedule boundaries must be accepted. Disconnect immediately,
    # avoiding a two-minute wait while still verifying pre-SSE validation.
    for query in ["rate=1&count=121", "mode=burst&rate=1&count=130"]:
        connection, response = server.stream(query)
        event(response)
        abort(connection, response)
        server.wait_active(0, timeout=12)

    streams = []
    try:
        for _ in range(2):
            connection, response = server.stream("rate=10&count=1000")
            event(response)
            streams.append((connection, response))
        server.wait_active(2)
        server.request("/stream?count=1", 429)
        server.request("/stream?count=0", 400)  # Validate before admission.
        server.request("/health")
        server.request("/stream", 204, "OPTIONS")
    finally:
        for connection, response in streams:
            abort(connection, response)
    server.wait_active(0)
    print(f"{backend}: framing, hash, routing, query bounds, schedules, saturation and disconnect PASS", flush=True)


def stalled(server, backend):
    sockets = []
    started = time.monotonic()
    try:
        for _ in range(2):
            sock = socket.socket()
            sockets.append(sock)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
            sock.settimeout(5)
            sock.connect(("127.0.0.1", server.port))
            sock.sendall(b"GET /stream?mode=unpaced&count=8192 HTTP/1.1\r\nHost: localhost\r\n\r\n")
        server.wait_active(2)
        server.request("/stream?count=1", 429)
        print(f"{backend}: two nonreading sockets held; waiting for real 130-second deadline", flush=True)
        while time.monotonic() - started < 128:
            check(server.active() == 2, "stalled slot was released before completion/deadline")
            server.request("/health")
            time.sleep(0.5)
        server.wait_active(0, timeout=5)
        elapsed = time.monotonic() - started
        check(129.5 <= elapsed <= 133, ("lifetime", elapsed))
        server.request("/stream?mode=unpaced&count=1")
        for sock in sockets:
            # A reset is allowed on timeout. A completed done event is not.
            received = bytearray()
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
            drain_deadline = time.monotonic() + 10
            try:
                while chunk := sock.recv(65536):
                    received.extend(chunk)
                    check(time.monotonic() < drain_deadline, "timed-out TCP connection did not close")
            except ConnectionResetError:
                pass
            check(b"event: done\n" not in received, "timeout emitted done")
        print(f"{backend}: stalled sockets closed, no done event, slots reusable after {elapsed:.2f}s PASS", flush=True)
    finally:
        for sock in sockets:
            sock.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=["java", "rust"], required=True)
    parser.add_argument("--stalled", action="store_true")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    check(command, "supply a built binary or run.sh after --")
    command[0] = str(Path(command[0]).resolve())
    with tempfile.TemporaryDirectory(prefix=f"streambench-{args.backend}-test-") as work:
        data = Path(work) / "data"
        data.mkdir()
        lines = [json.dumps({"seq": i, "text": "héllo 🌍 \\ \" " + "x" * 2048}, ensure_ascii=False,
                            separators=(",", ":")).encode() for i in range(8193)]
        raw = b"\n".join(lines) + b"\n"
        (data / "events.jsonl").write_bytes(raw)
        server = Server(command, data, work)
        try:
            server.ready()
            contract(server, args.backend, lines, raw)
            if args.stalled:
                stalled(server, args.backend)
        finally:
            log = server.stop()
            print(log, end="", flush=True)
        # Empty fixture startup rejection and a short fixture without final LF.
        for contents, valid in [(b"", False), (b"\n", False), (lines[0], True)]:
            (data / "events.jsonl").write_bytes(contents)
            server = Server(command, data, work)
            try:
                if valid:
                    server.ready()
                    server.request("/stream", 400)  # Default count=500 is not silently clamped.
                    _, body = server.request("/stream?count=1")
                    check(body == expected(lines, 1), "no-final-newline fixture")
                    info = json.loads(server.request("/info")[1])
                    check(info["fixture_sha256"] == hashlib.sha256(contents).hexdigest(), "raw hash")
                else:
                    check(server.process.wait(timeout=20) != 0, "empty fixture accepted")
            finally:
                server.stop()
        print(f"{args.backend}: ALL CHECKS PASSED; all test PIDs reaped", flush=True)


if __name__ == "__main__":
    main()
