import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.regex.Pattern;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

/** Native, socket-free lifecycle tests. The transport implements JDK subscriber callbacks. */
public final class DistributedTest {
    private static final String UNKNOWN = "0".repeat(32);
    private static final Map<String, URI> PEERS = Distributed.peers(Map.of());
    private static Path fixture;
    private static byte[][] frames;
    private static byte[][] done;

    static void check(boolean condition, Object detail) {
        if (!condition) throw new AssertionError(detail);
    }

    static void await(BooleanSupplier condition, String detail) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(5);
        check(condition.getAsBoolean(), detail);
    }

    static String string(String json, String name) {
        var matcher = Pattern.compile("\"" + name + "\":\"([^\"]*)\"").matcher(json);
        check(matcher.find(), json);
        return matcher.group(1);
    }

    static int number(String json, String name) {
        var matcher = Pattern.compile("\"" + name + "\":(\\d+)").matcher(json);
        check(matcher.find(), json);
        return Integer.parseInt(matcher.group(1));
    }

    private static final class Exchange extends HttpExchange {
        final Headers headers = new Headers();
        final String method;
        final URI uri;
        final OutputStream output;
        volatile int status;
        volatile boolean closed;
        Exchange(String method, String path, OutputStream output) {
            this.method = method;
            this.uri = URI.create(path);
            this.output = output;
        }
        @Override public Headers getRequestHeaders() { return new Headers(); }
        @Override public Headers getResponseHeaders() { return headers; }
        @Override public URI getRequestURI() { return uri; }
        @Override public String getRequestMethod() { return method; }
        @Override public HttpContext getHttpContext() { return null; }
        @Override public void close() { try { output.close(); } catch (IOException ignored) { } closed = true; }
        @Override public InputStream getRequestBody() { return InputStream.nullInputStream(); }
        @Override public OutputStream getResponseBody() { return output; }
        @Override public void sendResponseHeaders(int status, long length) { this.status = status; }
        @Override public InetSocketAddress getRemoteAddress() { return null; }
        @Override public int getResponseCode() { return status; }
        @Override public InetSocketAddress getLocalAddress() { return null; }
        @Override public String getProtocol() { return "HTTP/1.1"; }
        @Override public Object getAttribute(String name) { return null; }
        @Override public void setAttribute(String name, Object value) { }
        @Override public void setStreams(InputStream input, OutputStream output) { }
        @Override public HttpPrincipal getPrincipal() { return null; }
    }

    private static String request(Streambench app, String method, String path, int status) throws Exception {
        var bytes = new ByteArrayOutputStream();
        var exchange = new Exchange(method, path, bytes);
        app.handle(exchange);
        String body = bytes.toString(StandardCharsets.UTF_8);
        check(exchange.status == status, method + " " + path + ": " + exchange.status + " " + body);
        check(exchange.closed, "control exchange left open");
        check("*".equals(exchange.headers.getFirst("Access-Control-Allow-Origin")), "CORS");
        check("no-store".equals(exchange.headers.getFirst("Cache-Control")), "cache");
        if (path.startsWith("/batch") || path.startsWith("/clients")) {
            check("GET, POST, DELETE, OPTIONS".equals(exchange.headers.getFirst("Access-Control-Allow-Methods")), "CORS methods");
        }
        if (status >= 400) check(body.matches("\\{\"error\":\"[^\"]+\"\\}"), body);
        return body;
    }

    private static String create(Streambench app, String parameters) throws Exception {
        return string(request(app, "POST", "/batch?" + parameters, 201), "session");
    }

    private static String batch(Streambench app, String session) {
        try { return request(app, "GET", "/batch/" + session, 200); }
        catch (Exception failure) { throw new AssertionError(failure); }
    }

    private static int active(Streambench app) {
        try { return number(request(app, "GET", "/info", 200), "active_streams"); }
        catch (Exception failure) { throw new AssertionError(failure); }
    }

    private static void noTimers(Streambench app) throws Exception {
        var field = Streambench.class.getDeclaredField("deadlines");
        field.setAccessible(true);
        var deadlines = (java.util.concurrent.ScheduledThreadPoolExecutor) field.get(app);
        await(() -> deadlines.getQueue().isEmpty(), "terminal work retained deadline tasks");
    }

    private static final class Running implements AutoCloseable {
        final Exchange exchange;
        final Thread thread;
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        Running(Streambench app, String path, OutputStream output) {
            exchange = new Exchange("GET", path, output);
            thread = Thread.ofVirtual().unstarted(() -> {
                try { app.handle(exchange); }
                catch (Throwable error) { failure.set(error); }
            });
            thread.start();
        }
        void join() throws InterruptedException {
            thread.join(5000);
            check(!thread.isAlive(), "writer survived termination");
            check(failure.get() == null, failure.get());
            check(exchange.closed, "subscriber exchange left open");
        }
        @Override public void close() {
            thread.interrupt();
            try { join(); }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("test cleanup interrupted", interrupted);
            }
        }
    }

    private static class Capture extends OutputStream {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final CountDownLatch waiting = new CountDownLatch(1);
        final ArrayList<byte[]> references = new ArrayList<>();
        @Override public void write(int value) { throw new AssertionError("expected complete frame writes"); }
        @Override public synchronized void write(byte[] value) throws IOException {
            if (value[0] == ':') waiting.countDown();
            else references.add(value);
            bytes.write(value);
        }
        String text() { return bytes.toString(StandardCharsets.UTF_8); }
    }

    private static final class Blocked extends Capture {
        final CountDownLatch entered = new CountDownLatch(1);
        final boolean blockWaiting;
        final boolean blockClose;
        Blocked(boolean blockWaiting, boolean blockClose) {
            this.blockWaiting = blockWaiting;
            this.blockClose = blockClose;
        }
        private void block() throws IOException {
            entered.countDown();
            try { new CountDownLatch(1).await(); }
            catch (InterruptedException cancelled) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("write interrupted");
            }
        }
        @Override public synchronized void write(byte[] value) throws IOException {
            super.write(value);
            if (!blockClose && (blockWaiting || value[0] != ':')) block();
        }
        @Override public synchronized void close() throws IOException { if (blockClose) block(); }
    }

    private static void validation() throws Exception {
        var transport = new Transport();
        var app = new Streambench(fixture, 5, PEERS, Distributed.Limits.DEFAULT, transport);
        try {
            for (String path : List.of("/batch", "/clients", "/batch/" + UNKNOWN,
                "/clients/" + UNKNOWN, "/batch/" + UNKNOWN + "/stream")) {
                request(app, "OPTIONS", path, 204);
                for (String method : List.of("PUT", "PATCH")) request(app, method, path, 405);
            }
            request(app, "GET", "/batch", 405);
            request(app, "DELETE", "/clients", 405);
            request(app, "POST", "/batch/" + UNKNOWN, 405);
            for (String id : List.of("x", "A".repeat(32), "1".repeat(31), "%30" + "0".repeat(31))) {
                request(app, "GET", "/batch/" + id, 400);
                request(app, "GET", "/clients/" + id, 400);
            }
            request(app, "GET", "/batch/" + UNKNOWN, 404);
            request(app, "DELETE", "/clients/" + UNKNOWN, 404);
            for (String query : List.of("x=1", "count=0", "count=8193", "count=+1", "count=1.0",
                "count=1&%63ount=2", "rate=1&count=122", "mode=burst&rate=1&count=131", "count=1&")) {
                request(app, "POST", "/batch?" + query, 400);
            }
            String valid = "source=go&session=" + UNKNOWN + "&ids=1,2&count=2";
            for (String query : List.of("", valid + "&url=http://localhost", valid + "&source=java",
                valid + "&%63ount=2", valid.replace("source=go", "source=http://localhost"),
                valid.replace(UNKNOWN, "a"), valid.replace("ids=1,2", "ids=1,01"),
                valid.replace("ids=1,2", "ids=0"), valid.replace("ids=1,2", "ids=5"),
                valid.replace("ids=1,2", "ids=1,"), valid.replace("count=2", "count=0"),
                valid.replace("count=2", "count=8193"), valid.replace("count=2", "count=%FF"))) {
                request(app, "POST", "/clients?" + query, 400);
            }
            check(transport.requests.isEmpty(), "invalid request attempted outbound HTTP");
            String id = create(app, "count=2");
            check(id.matches("[0-9a-f]{32}"), id);
            request(app, "POST", "/batch?count=2", 409);
            for (String query : List.of("", "client=-1", "client=5", "client=+1", "client=1&client=2", "client=0&x=1")) {
                request(app, "GET", "/batch/" + id + "/stream?" + query, 400);
            }
            request(app, "GET", "/batch/" + id + "?x=1", 400);
            String cancelled = request(app, "DELETE", "/batch/" + id, 200);
            check(cancelled.equals(request(app, "DELETE", "/batch/" + id, 200)), "idempotent batch delete");
            String replacement = create(app, "count=1");
            check(!replacement.equals(id), "session reused");
            request(app, "GET", "/batch/" + id, 404);
            for (String origin : List.of("file:///tmp", "http://user:pass@localhost", "http://localhost/path",
                "https://localhost?x=1", "https://localhost#fragment", "http://localhost:0", "http://localhost:65536",
                "http://localhost:", "http://localhost/%2F", "")) {
                try { Distributed.peers(Map.of("PEER_GO_URL", origin)); throw new AssertionError("accepted " + origin); }
                catch (IllegalArgumentException expected) { }
            }
            for (String origin : List.of("http://127.0.0.1:8081", "https://localhost/", "http://[::1]:8081/")) {
                check(Distributed.peers(Map.of("PEER_GO_URL", origin)).get("go").isAbsolute(), origin);
            }
        } finally { app.close(); }
        System.out.println("Distributed: strict routes, control validation, origin allowlist and replacement PASS");
    }

    private static void fanout(String mode, boolean disconnect, boolean overflow) throws Exception {
        var app = new Streambench(fixture, 5, PEERS, Distributed.Limits.DEFAULT);
        var running = new ArrayList<Running>();
        var captures = new ArrayList<Capture>();
        int count = overflow ? 400 : 21;
        String id = create(app, "mode=" + mode + "&rate=" + (overflow ? 1000 : 100) + "&count=" + count);
        try {
            for (int client = 0; client < 5; client++) {
                Capture capture;
                if (client == 0 && overflow) capture = new Blocked(false, false);
                else if (client == 0 && disconnect) capture = new Capture() {
                    @Override public synchronized void write(byte[] bytes) throws IOException {
                        if (bytes[0] != ':') throw new IOException("peer disconnected");
                        super.write(bytes);
                    }
                };
                else capture = new Capture();
                captures.add(capture);
                running.add(new Running(app, "/batch/" + id + "/stream?client=" + client, capture));
                check(capture.waiting.await(5, TimeUnit.SECONDS), "waiting comment not flushed");
                if (client < 4) {
                    check(number(batch(app, id), "produced_events") == 0, "producer started early");
                    request(app, "GET", "/batch/" + id + "/stream?client=" + client, 409);
                    check(number(batch(app, id), "connected_clients") == client + 1, "current subscribers");
                }
            }
            for (Running writer : running) writer.join();
            await(() -> !string(batch(app, id), "state").equals("streaming"), "producer did not finish");
            String result = batch(app, id);
            check(string(result, "state").equals(disconnect || overflow ? "failed" : "complete"), result);
            check(number(result, "produced_events") == count, "producer must count once: " + result);
            check(number(result, "connected_clients") == 0 && active(app) == 0, "slots leaked");
            if (overflow) check(result.contains("queue overflow"), result);
            for (int client = disconnect || overflow ? 1 : 0; client < 5; client++) {
                var capture = captures.get(client);
                var expected = new ByteArrayOutputStream();
                expected.write(": waiting\n\n".getBytes(StandardCharsets.UTF_8));
                for (int i = 0; i < count; i++) expected.write(frames[i]);
                expected.write(done[count]);
                check(Arrays.equals(capture.bytes.toByteArray(), expected.toByteArray()), "fanout wire bytes");
                if (client > 1) for (int i = 0; i <= count; i++) {
                    check(capture.references.get(i) == captures.get(1).references.get(i), "frames were copied per subscriber");
                }
            }
            check(result.equals(request(app, "DELETE", "/batch/" + id, 200)), "terminal result rewritten");
            noTimers(app);
            request(app, "GET", "/batch/" + id + "/stream?client=0", 409);
            request(app, "GET", "/stream?count=1", 200);
        } finally {
            app.close();
            for (Running writer : running) writer.close();
        }
        System.out.println("Distributed: " + mode + " five-way fanout" + (disconnect ? " disconnect" : overflow ? " overflow" : "") + " PASS");
    }

    private static void cancellation(boolean joinTimeout, boolean absoluteTimeout, boolean blockClose) throws Exception {
        var limits = new Distributed.Limits(Duration.ofMillis(joinTimeout ? 150 : 1000),
            Duration.ofMillis(joinTimeout ? 2000 : 1500), Duration.ofSeconds(2));
        var app = new Streambench(fixture, 5, PEERS, limits);
        var writers = new ArrayList<Running>();
        var blocked = new ArrayList<Blocked>();
        String id = create(app, "mode=unpaced&count=1");
        try {
            int count = absoluteTimeout || blockClose ? 5 : 2;
            for (int client = 0; client < count; client++) {
                var output = new Blocked(joinTimeout || (!absoluteTimeout && !blockClose), blockClose);
                blocked.add(output);
                writers.add(new Running(app, "/batch/" + id + "/stream?client=" + client, output));
            }
            for (Blocked output : blocked) check(output.entered.await(5, TimeUnit.SECONDS), "writer did not block");
            check(active(app) == count, "shared slots not held");
            request(app, "GET", "/health", 200);
            if (!joinTimeout && !absoluteTimeout) {
                request(app, "DELETE", "/batch/" + id, 200);
                request(app, "DELETE", "/batch/" + id, 200);
            }
            for (Running writer : writers) writer.join();
            check(active(app) == 0, "deadline/cancel did not release exactly once");
            check(number(batch(app, id), "connected_clients") == 0, "closed subscribers counted");
            check(string(batch(app, id), "state").equals(joinTimeout || absoluteTimeout ? "failed" : "cancelled"), batch(app, id));
            request(app, "GET", "/stream?count=1", 200);
            create(app, "count=1");
        } finally {
            app.close();
            for (Running writer : writers) writer.close();
        }
        System.out.println("Distributed: " + (joinTimeout ? "join deadline" : absoluteTimeout ? "absolute deadline" : "DELETE")
            + " interrupts blocked " + (blockClose ? "closes" : "writes") + " and releases once PASS");
    }

    private static void admission() throws Exception {
        var app = new Streambench(fixture, 2, PEERS, Distributed.Limits.DEFAULT);
        var blocked = new Blocked(true, false);
        var capture = new Capture();
        String id = create(app, "count=1");
        try (var ordinary = new Running(app, "/stream?count=1", blocked);
             var shared = new Running(app, "/batch/" + id + "/stream?client=0", capture)) {
            check(blocked.entered.await(5, TimeUnit.SECONDS) && capture.waiting.await(5, TimeUnit.SECONDS), "admission setup");
            check(active(app) == 2, "ordinary and shared admission must be common");
            request(app, "GET", "/stream?count=1", 429);
            request(app, "GET", "/batch/" + id + "/stream?client=1", 429);
            request(app, "GET", "/batch/" + id + "/stream?client=0", 409);
            request(app, "GET", "/batch/" + id + "/stream?client=5", 400);
            ordinary.thread.interrupt();
            ordinary.join();
            try (var retry = new Running(app, "/batch/" + id + "/stream?client=1", new Capture())) {
                await(() -> active(app) == 2, "429 must not consume join ID");
                request(app, "DELETE", "/batch/" + id, 200);
                retry.join();
            }
            shared.join();
            check(active(app) == 0, "shared admission over/under released");
        } finally { app.close(); }
        System.out.println("Distributed: shared /stream admission and retry after 429 PASS");
    }

    private static void parser() throws Exception {
        var wire = new ByteArrayOutputStream();
        wire.write(": waiting\n\n".getBytes(StandardCharsets.UTF_8));
        wire.write(frames[0]);
        wire.write(": comment\n".getBytes(StandardCharsets.UTF_8));
        wire.write(frames[1]);
        wire.write(done[2]);
        for (int size : List.of(1, 2, 7, 1024)) {
            var events = new AtomicInteger();
            var reader = new SseReader(frames, done[2], 2, now -> events.incrementAndGet());
            byte[] bytes = wire.toByteArray();
            for (int offset = 0; offset < bytes.length; offset += size) {
                reader.accept(ByteBuffer.wrap(bytes, offset, Math.min(size, bytes.length - offset)));
            }
            reader.end();
            check(events.get() == 2, "complete events vs network chunks");
        }
        var crlf = new SseReader(frames, done[2], 2, now -> { });
        crlf.accept(ByteBuffer.wrap(wire.toString(StandardCharsets.UTF_8).replace("\n", "\r\n").getBytes(StandardCharsets.UTF_8)));
        crlf.end();
        var invalid = new ArrayList<byte[]>();
        invalid.add(frames[0]);
        invalid.add(done[2]);
        invalid.add(concat(frames[0], done[1]));
        invalid.add(concat(frames[1], frames[0], done[2]));
        invalid.add(concat(frames[0], frames[1], done[2], frames[0]));
        invalid.add(concat(frames[0], frames[1], done[2], new byte[]{'x'}));
        invalid.add(Arrays.copyOf(wire.toByteArray(), wire.size() - 1));
        invalid.add(new byte[SseReader.MAX_UNPARSED + 1]);
        invalid.add(new String(frames[0], StandardCharsets.UTF_8).replace("hello", "wrong").getBytes(StandardCharsets.UTF_8));
        for (byte[] bytes : invalid) {
            var reader = new SseReader(frames, done[2], 2, now -> { });
            try { reader.accept(ByteBuffer.wrap(bytes)); reader.end(); throw new AssertionError("invalid SSE accepted"); }
            catch (IOException expected) { }
        }
        System.out.println("Distributed: split/coalesced UTF-8 SSE, comments, counts, EOF and 1 MiB bound PASS");
    }

    private static void completionRace() throws Exception {
        var app = new Streambench(fixture, 5, PEERS, Distributed.Limits.DEFAULT);
        try {
            for (int iteration = 0; iteration < 25; iteration++) {
                String id = create(app, "mode=unpaced&count=1");
                var writers = new ArrayList<Running>();
                try {
                    for (int client = 0; client < 5; client++) {
                        var output = new Capture();
                        writers.add(new Running(app, "/batch/" + id + "/stream?client=" + client, output));
                        if (client < 4) check(output.waiting.await(5, TimeUnit.SECONDS), "race join setup");
                    }
                    request(app, "DELETE", "/batch/" + id, 200);
                    for (Running writer : writers) writer.join();
                    String result = batch(app, id);
                    check(List.of("complete", "cancelled").contains(string(result, "state")), result);
                    check(active(app) == 0 && number(result, "connected_clients") == 0, "completion race leaked admission");
                    check(result.equals(request(app, "DELETE", "/batch/" + id, 200)), "late cancel rewrote terminal state");
                    noTimers(app);
                } finally { for (Running writer : writers) writer.close(); }
            }
        } finally { app.close(); }
        System.out.println("Distributed: last-join/completion/DELETE races release exactly once PASS");
    }

    private static void cancelSchedule() throws Exception {
        var app = new Streambench(fixture, 5, PEERS, Distributed.Limits.DEFAULT);
        var writers = new ArrayList<Running>();
        String id = create(app, "mode=burst&rate=1&count=11");
        try {
            for (int client = 0; client < 5; client++) {
                writers.add(new Running(app, "/batch/" + id + "/stream?client=" + client, new Capture()));
            }
            await(() -> number(batch(app, id), "produced_events") == 10, "producer did not reach scheduled wait");
            long start = System.nanoTime();
            request(app, "DELETE", "/batch/" + id, 200);
            for (Running writer : writers) writer.join();
            check(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(1), "cancel waited for burst deadline");
            check(number(batch(app, id), "produced_events") == 10 && active(app) == 0, "cancelled producer continued");
            noTimers(app);
        } finally {
            app.close();
            for (Running writer : writers) writer.close();
        }
        System.out.println("Distributed: cancellation interrupts producer schedule and waiting queues PASS");
    }

    private static byte[] concat(byte[]... parts) throws IOException {
        var result = new ByteArrayOutputStream();
        for (byte[] part : parts) result.write(part);
        return result.toByteArray();
    }

    /** Controllable asynchronous HTTP boundary: no sockets, DNS or external requests. */
    private static final class Transport extends HttpClient {
        final List<Pending<?>> requests = new ArrayList<>();
        @Override public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request, HttpResponse.BodyHandler<T> handler) {
            var pending = new Pending<T>(request, handler);
            requests.add(pending);
            return pending.future;
        }
        @Override public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request, HttpResponse.BodyHandler<T> handler,
                                                                          HttpResponse.PushPromiseHandler<T> push) {
            return sendAsync(request, handler);
        }
        @Override public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> handler) {
            throw new AssertionError("remote requests must be asynchronous");
        }
        @Override public Optional<CookieHandler> cookieHandler() { return Optional.empty(); }
        @Override public Optional<Duration> connectTimeout() { return Optional.of(Duration.ofSeconds(10)); }
        @Override public Redirect followRedirects() { return Redirect.NEVER; }
        @Override public Optional<ProxySelector> proxy() { return Optional.empty(); }
        @Override public SSLContext sslContext() { throw new UnsupportedOperationException(); }
        @Override public SSLParameters sslParameters() { return new SSLParameters(); }
        @Override public Optional<Authenticator> authenticator() { return Optional.empty(); }
        @Override public Version version() { return Version.HTTP_1_1; }
        @Override public Optional<Executor> executor() { return Optional.empty(); }
        @Override public void shutdownNow() {
            for (Pending<?> pending : requests) pending.future.cancel(true);
        }
    }

    private static final class Pending<T> {
        final HttpRequest request;
        final HttpResponse.BodyHandler<T> handler;
        final CompletableFuture<HttpResponse<T>> future = new CompletableFuture<>();
        HttpResponse.BodySubscriber<T> subscriber;
        volatile boolean cancelled;
        long demand;
        Pending(HttpRequest request, HttpResponse.BodyHandler<T> handler) {
            this.request = request;
            this.handler = handler;
        }
        void headers(int status, String type) {
            subscriber = handler.apply(new HttpResponse.ResponseInfo() {
                @Override public int statusCode() { return status; }
                @Override public HttpHeaders headers() { return HttpHeaders.of(Map.of("Content-Type", List.of(type)), (a, b) -> true); }
                @Override public HttpClient.Version version() { return HttpClient.Version.HTTP_1_1; }
            });
            subscriber.onSubscribe(new Flow.Subscription() {
                @Override public void request(long count) { demand += count; }
                @Override public void cancel() { cancelled = true; }
            });
            subscriber.getBody().whenComplete((body, failure) -> {
                if (failure != null) future.completeExceptionally(failure);
                else future.complete(null);
            });
        }
        void bytes(byte[]... bytes) {
            check(!cancelled && demand-- > 0, "body must apply demand");
            subscriber.onNext(Arrays.stream(bytes).map(ByteBuffer::wrap).toList());
        }
        void end() { subscriber.onComplete(); }
    }

    private static String job(Streambench app, String id) {
        try { return request(app, "GET", "/clients/" + id, 200); }
        catch (Exception failure) { throw new AssertionError(failure); }
    }

    private static void remote() throws Exception {
        var transport = new Transport();
        var app = new Streambench(fixture, 5, PEERS, Distributed.Limits.DEFAULT, transport);
        String params = "source=go&session=" + UNKNOWN + "&ids=1,2&count=3";
        try {
            String snapshot = request(app, "POST", "/clients?" + params, 202);
            check(string(snapshot, "state").equals("running") && transport.requests.size() == 2, snapshot);
            check(snapshot.contains("\"first_event_ms\":null") && snapshot.contains("\"p95_gap_ms\":null"), snapshot);
            request(app, "POST", "/clients?" + params.replace("ids=1,2", "ids=2,1"), 202);
            check(transport.requests.size() == 2, "retry created duplicate clients");
            for (String other : List.of(params.replace("source=go", "source=java"), params.replace("count=3", "count=2"),
                params.replace("ids=1,2", "ids=1"), params.replace(UNKNOWN, "1".repeat(32)))) {
                request(app, "POST", "/clients?" + other, 409);
            }
            // Source and worker jobs may coexist without sharing worker admission slots.
            String batch = create(app, "count=1");
            request(app, "GET", "/health", 200);
            check(active(app) == 0, "remote jobs must not consume local source slots");
            request(app, "DELETE", "/batch/" + batch, 200);
            for (int i = 0; i < 2; i++) {
                var pending = transport.requests.get(i);
                check(pending.request.uri().toString().equals("https://streambench-go-1.onrender.com/batch/" + UNKNOWN + "/stream?client=" + (i + 1)), "outbound allowlist/path");
                check(pending.request.method().equals("GET"), "recursive worker job");
                check(pending.request.timeout().orElseThrow().compareTo(Duration.ofSeconds(180)) <= 0, "request deadline");
                pending.headers(200, "text/event-stream; charset=utf-8");
            }
            var first = transport.requests.getFirst();
            Thread.sleep(35);
            first.bytes(": waiting\n\n".getBytes(StandardCharsets.UTF_8), Arrays.copyOf(frames[0], frames[0].length - 1));
            check(number(job(app, UNKNOWN), "events") == 0, "partial frame counted");
            first.bytes(new byte[]{'\n'});
            Thread.sleep(20);
            first.bytes(frames[1]);
            Thread.sleep(40);
            first.bytes(frames[2], done[3]);
            check(string(job(app, UNKNOWN), "state").equals("running"), "done without EOF must not complete");
            first.end();
            String metrics = job(app, UNKNOWN);
            var p95 = Pattern.compile("\"p95_gap_ms\":([0-9.]+)").matcher(metrics);
            check(p95.find() && Double.parseDouble(p95.group(1)) >= 30, "nearest-rank gap: " + metrics);
            var firstMs = Pattern.compile("\"first_event_ms\":([0-9.]+)").matcher(metrics);
            check(firstMs.find() && Double.parseDouble(firstMs.group(1)) >= 25, "first complete-event time: " + metrics);
            var second = transport.requests.get(1);
            second.bytes(frames[0]);
            second.end();
            metrics = job(app, UNKNOWN);
            check(string(metrics, "state").equals("failed") && metrics.contains("premature EOF"), metrics);
            check(metrics.contains("\"id\":1,\"state\":\"complete\",\"events\":3"), metrics);
            check(metrics.equals(request(app, "DELETE", "/clients/" + UNKNOWN, 200)), "terminal job rewritten");
            noTimers(app);
            Thread.sleep(15);
            check(metrics.equals(job(app, UNKNOWN)), "terminal elapsed time changed");

            // Cancel after one child succeeds; preserve that result and cancel the blocked body.
            request(app, "POST", "/clients?" + params.replace("count=3", "count=1"), 202);
            var completed = transport.requests.get(2);
            var blocked = transport.requests.get(3);
            completed.headers(200, "text/event-stream");
            completed.bytes(frames[0], done[1]);
            completed.end();
            blocked.headers(200, "text/event-stream");
            String cancelled = request(app, "DELETE", "/clients/" + UNKNOWN, 200);
            check(string(cancelled, "state").equals("cancelled"), cancelled);
            check(cancelled.contains("\"id\":1,\"state\":\"complete\"") && cancelled.contains("\"id\":2,\"state\":\"cancelled\""), cancelled);
            check(blocked.cancelled && blocked.future.isCancelled(), "body and outstanding request not cancelled");
            check(cancelled.equals(request(app, "DELETE", "/clients/" + UNKNOWN, 200)), "idempotent worker cancellation");

            // Cancel before response headers, including a late onSubscribe callback.
            request(app, "POST", "/clients?" + params.replace("ids=1,2", "ids=4"), 202);
            var connecting = transport.requests.getLast();
            request(app, "DELETE", "/clients/" + UNKNOWN, 200);
            check(connecting.future.isCancelled(), "connecting request not cancelled");
            connecting.headers(200, "text/event-stream");
            check(connecting.cancelled, "late body subscription escaped cancellation");

            // Non-200 / wrong MIME responses terminate without following redirects or draining bodies.
            for (int status : List.of(302, 500, 200)) {
                request(app, "POST", "/clients?" + params.replace("ids=1,2", "ids=3"), 202);
                var bad = transport.requests.getLast();
                int requests = transport.requests.size();
                bad.headers(status, status == 200 ? "application/json" : "text/event-stream");
                check(string(job(app, UNKNOWN), "state").equals("failed"), job(app, UNKNOWN));
                check(bad.cancelled && transport.requests.size() == requests, "bad response was drained or redirected");
            }
            String replacement = "1".repeat(32);
            request(app, "POST", "/clients?" + params.replace("ids=1,2", "ids=1").replace(UNKNOWN, replacement), 202);
            request(app, "GET", "/clients/" + UNKNOWN, 404);
            var success = transport.requests.getLast();
            success.headers(200, "text/event-stream");
            success.bytes(frames[0], frames[1], frames[2], done[3]);
            success.end();
            String result = job(app, replacement);
            check(string(result, "state").equals("complete"), result);
            check(result.equals(request(app, "DELETE", "/clients/" + replacement, 200)), "completed job changed on delete");
            noTimers(app);
        } finally { app.close(); }
        System.out.println("Distributed: asynchronous clients, metrics, retries, EOF failures and cancellation races PASS");
    }

    private static void remoteDeadline(boolean headers, boolean doneWithoutEof) throws Exception {
        var transport = new Transport();
        var limits = new Distributed.Limits(Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofMillis(200));
        var app = new Streambench(fixture, 5, PEERS, limits, transport);
        try {
            request(app, "POST", "/clients?source=bun&session=" + UNKNOWN + "&ids=1&count=1", 202);
            var pending = transport.requests.getFirst();
            if (headers) {
                pending.headers(200, "text/event-stream");
                if (doneWithoutEof) pending.bytes(frames[0], done[1]);
            }
            await(() -> string(job(app, UNKNOWN), "state").equals("failed"), "remote deadline did not fire");
            await(() -> pending.future.isCancelled(), "request deadline did not cancel HTTP");
            check(!headers || pending.cancelled, "request deadline did not cancel body");
            check(job(app, UNKNOWN).contains("client deadline exceeded"), job(app, UNKNOWN));
        } finally { app.close(); }
        System.out.println("Distributed: remote deadline through " + (doneWithoutEof ? "EOF" : headers ? "body" : "connection") + " PASS");
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("jdk.virtualThreadScheduler.parallelism", "12");
        Path temporary = Files.createTempDirectory("streambench-distributed-native-");
        fixture = temporary.resolve("events.jsonl");
        try {
            var raw = new StringBuilder();
            frames = new byte[8192][];
            done = new byte[8193][];
            for (int i = 0; i < frames.length; i++) {
                String line = "{\"seq\":" + i + ",\"text\":\"hello héllo 🌍 \\\"quoted\\\"\"}";
                raw.append(line).append('\n');
                frames[i] = ("event: text\ndata: " + line + "\n\n").getBytes(StandardCharsets.UTF_8);
            }
            for (int i = 0; i < done.length; i++) done[i] = ("event: done\ndata: {\"count\":" + i + "}\n\n").getBytes(StandardCharsets.UTF_8);
            Files.writeString(fixture, raw);
            validation();
            for (String mode : List.of("paced", "burst", "unpaced")) fanout(mode, false, false);
            fanout("paced", true, false);
            fanout("paced", false, true);
            admission();
            cancellation(false, false, false);
            cancellation(true, false, false);
            cancellation(false, true, false);
            cancellation(false, true, true);
            cancellation(false, false, true);
            completionRace();
            cancelSchedule();
            parser();
            remote();
            remoteDeadline(false, false);
            remoteDeadline(true, false);
            remoteDeadline(true, true);
            System.out.println("Distributed: ALL NATIVE CHECKS PASSED");
        } finally {
            Files.deleteIfExists(fixture);
            Files.delete(temporary);
        }
    }
}
