import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class Streambench {
    private static final int MAX_COUNT = 8192;
    private static final long SECOND = 1_000_000_000L;
    private static final long LIFETIME = 130 * SECOND;
    private static final byte[] PREFIX = "event: text\ndata: ".getBytes(StandardCharsets.UTF_8);
    private static final byte[] HEALTH = "{\"status\":\"ok\"}".getBytes(StandardCharsets.UTF_8);

    private final byte[][] frames;
    private final byte[][] done;
    private final String sha256;
    private final int maxStreams;
    private final Semaphore slots;
    private final ScheduledThreadPoolExecutor deadlines = new ScheduledThreadPoolExecutor(
        1, Thread.ofPlatform().daemon().name("stream-deadline").factory());

    private Streambench(Path fixture, int maxStreams) throws Exception {
        byte[] raw = Files.readAllBytes(fixture);
        // Validate UTF-8 without decoding/re-encoding individual JSON records.
        StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(raw));
        sha256 = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(raw));
        var encoded = new ArrayList<byte[]>();
        int end = raw.length;
        if (end > 0 && raw[end - 1] == '\n') end--;
        if (end == 0) throw new IllegalArgumentException("fixture must not be empty");
        int start = 0;
        for (int i = 0; i <= end; i++) {
            if (i != end && raw[i] != '\n') continue;
            if (i == start) throw new IllegalArgumentException("fixture contains an empty line");
            byte[] frame = new byte[PREFIX.length + i - start + 2];
            System.arraycopy(PREFIX, 0, frame, 0, PREFIX.length);
            System.arraycopy(raw, start, frame, PREFIX.length, i - start);
            frame[frame.length - 2] = '\n';
            frame[frame.length - 1] = '\n';
            encoded.add(frame);
            start = i + 1;
        }
        frames = encoded.toArray(byte[][]::new);
        done = new byte[Math.min(MAX_COUNT, frames.length) + 1][];
        for (int count = 0; count < done.length; count++) {
            done[count] = ("event: done\ndata: {\"count\":" + count + "}\n\n")
                .getBytes(StandardCharsets.UTF_8);
        }
        this.maxStreams = maxStreams;
        slots = new Semaphore(maxStreams);
        deadlines.setRemoveOnCancelPolicy(true);
    }

    private enum Mode { PACED, BURST, UNPACED }

    private record Parameters(Mode mode, int rate, int count) {
        long ticks(int index) {
            return switch (mode) {
                case PACED -> index;
                case BURST -> (index / 10) * 10L;
                case UNPACED -> 0;
            };
        }

        long offset(int index) { return ticks(index) * SECOND / rate; }
    }

    private static int decimal(String value, int maximum) {
        if (value.isEmpty()) throw new IllegalArgumentException("integer must not be empty");
        int number = 0;
        for (int i = 0; i < value.length(); i++) {
            char digit = value.charAt(i);
            if (digit < '0' || digit > '9') {
                throw new IllegalArgumentException("integers must contain only ASCII digits");
            }
            int next = digit - '0';
            if (number > maximum / 10 || (number == maximum / 10 && next > maximum % 10)) {
                throw new IllegalArgumentException("integer is out of range");
            }
            number = number * 10 + next;
        }
        if (number == 0) throw new IllegalArgumentException("integer must be positive");
        return number;
    }

    private Parameters parameters(String query) {
        Mode mode = Mode.PACED;
        int rate = 50;
        int count = 500;
        Set<String> seen = new HashSet<>();
        if (query != null && !query.isEmpty()) {
            for (String pair : query.split("&", -1)) {
                int separator = pair.indexOf('=');
                if (separator < 0) throw new IllegalArgumentException("query keys require a value");
                String key = decode(pair.substring(0, separator));
                String value = decode(pair.substring(separator + 1));
                if (value.isEmpty()) throw new IllegalArgumentException("query values must not be empty");
                if (!seen.add(key)) throw new IllegalArgumentException("repeated query key");
                switch (key) {
                    case "mode" -> mode = switch (value) {
                        case "paced" -> Mode.PACED;
                        case "burst" -> Mode.BURST;
                        case "unpaced" -> Mode.UNPACED;
                        default -> throw new IllegalArgumentException("unsupported mode");
                    };
                    case "rate" -> rate = decimal(value, 1000);
                    case "count" -> count = decimal(value, Math.min(MAX_COUNT, frames.length));
                    default -> throw new IllegalArgumentException("unknown query key");
                }
            }
        }
        if (count > Math.min(MAX_COUNT, frames.length)) {
            throw new IllegalArgumentException("count exceeds the fixture or 8192 events");
        }
        var parameters = new Parameters(mode, rate, count);
        if (parameters.ticks(count - 1) > 120L * rate) {
            throw new IllegalArgumentException("last scheduled event must be within 120 seconds");
        }
        return parameters;
    }

    private static String decode(String value) {
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException invalidEncoding) {
            throw new IllegalArgumentException("invalid query encoding");
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        var headers = exchange.getResponseHeaders();
        headers.set("Access-Control-Allow-Origin", "*");
        headers.set("Access-Control-Allow-Methods", "GET, OPTIONS");
        headers.set("Cache-Control", "no-store");
        // HttpServer contexts are prefix matches; dispatch exact raw paths here.
        String path = exchange.getRequestURI().getRawPath();
        if (!path.equals("/health") && !path.equals("/info") && !path.equals("/stream")) {
            error(exchange, 404, "unknown path");
            return;
        }
        String method = exchange.getRequestMethod();
        if (method.equals("OPTIONS")) {
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
            return;
        }
        if (!method.equals("GET")) {
            headers.set("Allow", "GET, OPTIONS");
            error(exchange, 405, "method must be GET or OPTIONS");
            return;
        }
        switch (path) {
            case "/health" -> json(exchange, 200, HEALTH);
            case "/info" -> json(exchange, 200, ("{\"backend\":\"java\",\"fixture_sha256\":\"" + sha256
                + "\",\"fixture_events\":" + frames.length + ",\"max_streams\":" + maxStreams
                + ",\"active_streams\":" + (maxStreams - slots.availablePermits()) + "}")
                .getBytes(StandardCharsets.UTF_8));
            default -> {
                Parameters parameters;
                try {
                    parameters = parameters(exchange.getRequestURI().getRawQuery());
                } catch (IllegalArgumentException invalid) {
                    error(exchange, 400, invalid.getMessage());
                    return;
                }
                if (!slots.tryAcquire()) {
                    error(exchange, 429, "active stream limit reached");
                    return;
                }
                stream(exchange, parameters);
            }
        }
    }

    private void stream(HttpExchange exchange, Parameters parameters) {
        long start = System.nanoTime();
        Thread writer = Thread.currentThread();
        var released = new AtomicBoolean();
        Runnable release = () -> {
            if (released.compareAndSet(false, true)) slots.release();
        };
        var timeout = deadlines.schedule(() -> {
            // HttpServer writes through an interruptible SocketChannel. Closing
            // only the exchange can block behind a write; interrupt its virtual
            // thread instead, which closes a blocked channel and wakes sleeps.
            writer.interrupt();
            release.run();
        }, Math.max(0, LIFETIME - (System.nanoTime() - start)), TimeUnit.NANOSECONDS);
        try {
            var headers = exchange.getResponseHeaders();
            headers.set("Content-Type", "text/event-stream; charset=utf-8");
            headers.set("X-Accel-Buffering", "no");
            headers.set("Connection", "close");
            checkDeadline(start);
            exchange.sendResponseHeaders(200, 0);
            OutputStream output = exchange.getResponseBody();
            for (int index = 0; index < parameters.count(); index++) {
                long remaining;
                while ((remaining = parameters.offset(index) - (System.nanoTime() - start)) > 0) {
                    TimeUnit.NANOSECONDS.sleep(remaining);
                }
                checkDeadline(start);
                output.write(frames[index]);
                output.flush();
            }
            checkDeadline(start);
            output.write(done[parameters.count()]);
            output.flush();
        } catch (IOException disconnected) {
            // A disconnected client is normal; no done event or per-event log.
        } catch (InterruptedException timedOut) {
            writer.interrupt();
        } finally {
            try {
                // Keep the watchdog armed through close: its final write or
                // draining a malicious request body can also block.
                exchange.close();
            } finally {
                timeout.cancel(false);
                release.run();
            }
        }
    }

    private static void checkDeadline(long start) throws InterruptedException {
        if (Thread.currentThread().isInterrupted() || System.nanoTime() - start >= LIFETIME) {
            throw new InterruptedException("stream lifetime exceeded");
        }
    }

    private static void json(HttpExchange exchange, int status, byte[] body) throws IOException {
        try {
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            // HEAD has status/headers but, as required by HTTP, no response body.
            exchange.sendResponseHeaders(status, exchange.getRequestMethod().equals("HEAD") ? -1 : body.length);
            if (!exchange.getRequestMethod().equals("HEAD")) exchange.getResponseBody().write(body);
        } finally {
            exchange.close();
        }
    }

    private static void error(HttpExchange exchange, int status, String reason) throws IOException {
        // Every reason is a fixed string; never echo untrusted input into JSON.
        json(exchange, status, ("{\"error\":\"" + reason + "\"}").getBytes(StandardCharsets.UTF_8));
    }

    private static int setting(String name, String fallback, int maximum) {
        String value = System.getenv(name);
        try {
            return decimal(value == null ? fallback : value, maximum);
        } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException(name + ": " + invalid.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        int port = setting("PORT", "8080", 65535);
        int maxStreams = setting("MAX_STREAMS", "32", Integer.MAX_VALUE);
        // JDK 21 HttpServer writes hold a monitor, so a slow socket can pin a
        // virtual thread's carrier. Reserve carriers for health/info/admission
        // even when every admitted stream is blocked. ForkJoinPool caps at 32767.
        int carriers = (int) Math.min(32767L,
            Math.max(Runtime.getRuntime().availableProcessors(), (long) maxStreams + 2));
        System.setProperty("jdk.virtualThreadScheduler.parallelism", Integer.toString(carriers));
        System.setProperty("jdk.virtualThreadScheduler.maxPoolSize", Integer.toString(Math.max(256, carriers)));
        String dataDir = System.getenv("DATA_DIR");
        var app = new Streambench(Path.of(dataDir == null ? "data" : dataDir, "events.jsonl"), maxStreams);
        var server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
        var workers = Executors.newVirtualThreadPerTaskExecutor();
        server.setExecutor(workers);
        server.createContext("/", app::handle);
        Runtime.getRuntime().addShutdownHook(Thread.ofPlatform().unstarted(() -> {
            server.stop(0);
            workers.shutdownNow();
            app.deadlines.shutdownNow();
        }));
        server.start();
        System.err.println("streambench-java listening on 0.0.0.0:" + port);
    }
}
