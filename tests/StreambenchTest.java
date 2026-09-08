import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/** Socket-free handler and watchdog tests. --stalled runs the real 130s deadline. */
public final class StreambenchTest {
    private static final Method HANDLE;
    static {
        try {
            HANDLE = Streambench.class.getDeclaredMethod("handle", HttpExchange.class);
            HANDLE.setAccessible(true);
        } catch (ReflectiveOperationException error) {
            throw new ExceptionInInitializerError(error);
        }
    }

    private static final class Exchange extends HttpExchange {
        final Headers headers = new Headers();
        final String method;
        final URI uri;
        final OutputStream output;
        int status;
        boolean closed;
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
        @Override public void close() {
            try { output.close(); } catch (IOException ignored) { }
            closed = true;
        }
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

    private static void check(boolean condition, Object detail) {
        if (!condition) throw new AssertionError(detail);
    }

    private static Object field(Object app, String name) throws Exception {
        var field = Streambench.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(app);
    }

    private static Streambench load(Path path) throws Exception {
        var constructor = Streambench.class.getDeclaredConstructor(Path.class, int.class);
        constructor.setAccessible(true);
        return constructor.newInstance(path, 2);
    }

    private static void handle(Streambench app, Exchange exchange) throws Exception {
        try { HANDLE.invoke(app, exchange); }
        catch (InvocationTargetException error) { throw new AssertionError(error.getCause()); }
    }

    private static Exchange request(Streambench app, String method, String path, int status) throws Exception {
        var exchange = new Exchange(method, path, new ByteArrayOutputStream());
        handle(app, exchange);
        check(exchange.status == status, path + ": " + exchange.status);
        check(exchange.closed, "exchange must close");
        check("*".equals(exchange.headers.getFirst("Access-Control-Allow-Origin")), "CORS origin");
        check("GET, OPTIONS".equals(exchange.headers.getFirst("Access-Control-Allow-Methods")), "CORS methods");
        check("no-store".equals(exchange.headers.getFirst("Cache-Control")), "cache");
        if (status >= 400 && !method.equals("HEAD")) {
            check(body(exchange).matches("\\{\"error\":\"[^\"]+\"\\}"), body(exchange));
        }
        return exchange;
    }

    private static String body(Exchange exchange) {
        return ((ByteArrayOutputStream) exchange.output).toString(StandardCharsets.UTF_8);
    }

    private static void contract(Streambench app, byte[] raw, String line) throws Exception {
        check(body(request(app, "GET", "/health", 200)).equals("{\"status\":\"ok\"}"), "health");
        String info = body(request(app, "GET", "/info", 200));
        check(info.contains(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(raw))), "hash");
        check(info.contains("\"fixture_events\":8193") && info.contains("\"active_streams\":0"), info);
        for (String path : List.of("/health", "/info", "/stream")) {
            request(app, "OPTIONS", path, 204);
            for (String method : List.of("POST", "PUT", "PATCH", "DELETE", "HEAD")) {
                check("GET, OPTIONS".equals(request(app, method, path, 405).headers.getFirst("Allow")), "Allow");
            }
        }
        for (String path : List.of("/", "/stream/", "/health-extra", "/%68ealth", "/unknown")) {
            for (String method : List.of("GET", "OPTIONS", "POST")) request(app, method, path, 404);
        }
        for (String query : List.of("mode=", "rate=", "count=", "mode=fast", "mode=Paced", "x=1", "rate",
            "rate=1&rate=2", "rate=1&%72ate=2", "mode=burst&mode=paced", "count=1&count=2",
            "rate=0", "rate=1001", "rate=-1", "rate=+1", "rate=1.0", "rate=1e2", "rate=%201",
            "rate=1%20", "rate=%EF%BC%91", "rate=%FF", "count=0", "count=8193", "count=-1",
            "count=1.0", "count=+1", "count=9999999999999999999999999999", "rate=1&count=122",
            "mode=burst&rate=1&count=131", "count=2&", "&count=2", "count=2&&rate=1", "=1",
            "count=1=2", "mode=unpaced&rate=1001")) {
            request(app, "GET", "/stream?" + query, 400);
        }
        for (int count : List.of(1, 500, 8192)) {
            var exchange = request(app, "GET", "/stream?mode=unpaced&count=" + count, 200);
            check("text/event-stream; charset=utf-8".equals(exchange.headers.getFirst("Content-Type")), "SSE type");
            check("no".equals(exchange.headers.getFirst("X-Accel-Buffering")), "buffering");
            byte[][] frames = (byte[][]) field(app, "frames");
            var expected = new ByteArrayOutputStream();
            for (int i = 0; i < count; i++) expected.write(frames[i]);
            expected.write(("event: done\ndata: {\"count\":" + count + "}\n\n").getBytes(StandardCharsets.UTF_8));
            check(java.util.Arrays.equals(((ByteArrayOutputStream) exchange.output).toByteArray(), expected.toByteArray()), "framing");
        }
        check(body(request(app, "GET", "/stream?count=0001&%72ate=0001", 200))
            .equals("event: text\ndata: " + line + "\n\nevent: done\ndata: {\"count\":1}\n\n"), "raw UTF-8");

        for (String mode : List.of("paced", "burst")) {
            var times = new ArrayList<Long>();
            var output = new OutputStream() {
                @Override public void write(int value) { throw new AssertionError("complete frames required"); }
                @Override public void write(byte[] bytes) { times.add(System.nanoTime()); }
            };
            long start = System.nanoTime();
            handle(app, new Exchange("GET", "/stream?mode=" + mode + "&rate=20&count=21", output));
            check(times.size() == 22, "text and done frames");
            for (int i = 0; i < 21; i++) {
                double elapsed = (times.get(i) - start) / 1e9;
                double scheduled = (mode.equals("paced") ? i : (i / 10) * 10) / 20.0;
                check(elapsed >= scheduled - 0.02 && elapsed <= scheduled + 0.5, mode + " deadline " + i + ": " + elapsed);
            }
        }
        check(((Semaphore) field(app, "slots")).availablePermits() == 2, "completion releases slots");
        check(((ScheduledThreadPoolExecutor) field(app, "deadlines")).getQueue().isEmpty(), "completion cancels timers");
        System.out.println("Java: routing, headers, query validation, wire bytes, hash, schedules and completion PASS");
    }

    private static void stalled(Streambench app, boolean fullLifetime) throws Exception {
        var entered = new CountDownLatch(2);
        var exchanges = new ArrayList<Exchange>();
        var writers = new ArrayList<Thread>();
        long start = System.nanoTime();
        try {
            for (int i = 0; i < 2; i++) {
                var output = new OutputStream() {
                    @Override public void write(int value) { throw new AssertionError("complete frames required"); }
                    @Override public synchronized void write(byte[] bytes) throws IOException {
                        check(Thread.currentThread().isVirtual(), "writer must be virtual");
                        entered.countDown();
                        try { new CountDownLatch(1).await(); }
                        catch (InterruptedException cancelled) {
                            Thread.currentThread().interrupt();
                            throw new InterruptedIOException("blocked write interrupted");
                        }
                    }
                };
                var exchange = new Exchange("GET", "/stream?mode=unpaced&count=8192", output);
                exchanges.add(exchange);
                var writer = Thread.ofVirtual().unstarted(() -> {
                    try { handle(app, exchange); }
                    catch (Exception error) { throw new AssertionError(error); }
                });
                writers.add(writer);
                writer.start();
            }
            check(entered.await(5, TimeUnit.SECONDS), "writers did not reach blocked output");
            var slots = (Semaphore) field(app, "slots");
            check(slots.availablePermits() == 0, "both slots must be held");
            request(app, "GET", "/stream?count=1", 429);
            request(app, "GET", "/stream?count=0", 400);
            request(app, "GET", "/health", 200);
            if (fullLifetime) {
                System.out.println("Java: waiting for real 130-second watchdog with two blocked virtual writers");
                while (System.nanoTime() - start < TimeUnit.SECONDS.toNanos(128)) {
                    check(slots.availablePermits() == 0, "premature release");
                    request(app, "GET", "/health", 200);
                    Thread.sleep(500);
                }
            } else {
                writers.forEach(Thread::interrupt);
            }
            for (Thread writer : writers) writer.join(5000);
            for (Thread writer : writers) check(!writer.isAlive(), "writer survived cancellation/deadline");
            for (Exchange exchange : exchanges) check(exchange.closed, "timed-out exchange not closed");
            check(slots.availablePermits() == 2, "blocked writes did not release admission");
            if (fullLifetime) {
                double elapsed = (System.nanoTime() - start) / 1e9;
                check(elapsed >= 129.5 && elapsed < 133, "lifetime: " + elapsed);
            }
            request(app, "GET", "/stream?count=1", 200);
            System.out.println("Java: " + (fullLifetime ? "130-second watchdog" : "cancellation") + " closes blocked writers and releases slots PASS");
        } finally {
            writers.forEach(Thread::interrupt);
            for (Thread writer : writers) {
                writer.join(5000);
                check(!writer.isAlive(), "test writer was not stopped");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("jdk.virtualThreadScheduler.parallelism", "4");
        Path temporary = Files.createTempDirectory("streambench-java-native-");
        Path fixture = temporary.resolve("events.jsonl");
        Streambench app = null;
        try {
            String line = "{\"seq\":0,\"text\":\"héllo 🌍\"}";
            var contents = new StringBuilder();
            for (int i = 0; i < 8193; i++) {
                contents.append("{\"seq\":").append(i).append(",\"text\":\"héllo 🌍\"}\n");
            }
            byte[] raw = contents.toString().getBytes(StandardCharsets.UTF_8);
            Files.write(fixture, raw);
            app = load(fixture);
            contract(app, raw, line);
            stalled(app, false);
            if (List.of(args).contains("--stalled")) stalled(app, true);
            for (String invalid : List.of("", "\n")) {
                Files.writeString(fixture, invalid);
                try { load(fixture); throw new AssertionError("empty fixture accepted"); }
                catch (InvocationTargetException rejected) {
                    check(rejected.getCause() instanceof IllegalArgumentException, rejected);
                }
            }
            Files.writeString(fixture, line);
            var shortFixture = load(fixture);
            try {
                request(shortFixture, "GET", "/stream", 400);
                check(body(request(shortFixture, "GET", "/stream?count=1", 200))
                    .equals("event: text\ndata: " + line + "\n\nevent: done\ndata: {\"count\":1}\n\n"), "no final LF");
            } finally { ((ScheduledThreadPoolExecutor) field(shortFixture, "deadlines")).shutdownNow(); }
            System.out.println("Java: ALL SOCKET-FREE CHECKS PASSED");
        } finally {
            if (app != null) {
                var deadlines = (ScheduledThreadPoolExecutor) field(app, "deadlines");
                deadlines.shutdownNow();
                check(deadlines.awaitTermination(5, TimeUnit.SECONDS), "deadline executor survived cleanup");
            }
            Files.deleteIfExists(fixture);
            Files.delete(temporary);
        }
    }
}
