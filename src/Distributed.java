import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.function.Function;

/** Distributed protocol routing and the two independently bounded job registries. */
final class Distributed implements AutoCloseable {
    record Limits(Duration join, Duration batch, Duration client) {
        static final Limits DEFAULT = new Limits(Duration.ofSeconds(45), Duration.ofSeconds(180),
            Duration.ofSeconds(180));
        Limits {
            if (join.isZero() || join.isNegative() || join.compareTo(Duration.ofSeconds(45)) > 0
                || batch.compareTo(join) < 0 || batch.compareTo(Duration.ofSeconds(180)) > 0
                || client.isZero() || client.isNegative() || client.compareTo(Duration.ofSeconds(180)) > 0) {
                throw new IllegalArgumentException("invalid distributed deadlines");
            }
        }
    }

    private static final Set<String> SOURCES = Set.of("go", "java", "rust", "bun");
    private static final SecureRandom RANDOM = new SecureRandom();
    private final byte[][] frames;
    private final byte[][] done;
    private final Semaphore slots;
    private final ScheduledThreadPoolExecutor deadlines;
    private final Function<String, Streambench.Parameters> parameters;
    private final Map<String, URI> peers;
    private final Limits limits;
    private final HttpClient http;
    private SharedBatch batch;
    private RemoteClients clients;

    Distributed(byte[][] frames, byte[][] done, Semaphore slots, ScheduledThreadPoolExecutor deadlines,
                Function<String, Streambench.Parameters> parameters, Map<String, URI> peers, Limits limits,
                HttpClient transport) {
        this.frames = frames;
        this.done = done;
        this.slots = slots;
        this.deadlines = deadlines;
        this.parameters = parameters;
        this.peers = Map.copyOf(peers);
        this.limits = limits;
        http = transport != null ? transport : HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER)
            .version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(10)).build();
    }

    static Map<String, URI> peers(Map<String, String> environment) {
        var result = new HashMap<String, URI>();
        for (String key : SOURCES) {
            String defaultHost = key.equals("go") ? "streambench-go-1" : "streambench-" + key;
            String value = environment.getOrDefault("PEER_" + key.toUpperCase(java.util.Locale.ROOT) + "_URL",
                "https://" + defaultHost + ".onrender.com");
            URI uri;
            try { uri = URI.create(value); }
            catch (IllegalArgumentException invalid) { throw new IllegalArgumentException("invalid peer origin"); }
            if (!("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
                || uri.getHost() == null || uri.getRawUserInfo() != null || uri.getRawQuery() != null
                || uri.getRawFragment() != null || (!uri.getRawPath().isEmpty() && !uri.getRawPath().equals("/"))
                || uri.getPort() == 0 || uri.getPort() > 65535 || uri.getRawAuthority().endsWith(":")) {
                throw new IllegalArgumentException("peer URL must be an HTTP(S) origin");
            }
            result.put(key, uri.resolve("/"));
        }
        return Map.copyOf(result);
    }

    static Map<String, String> query(String raw, Set<String> allowed) {
        var values = new HashMap<String, String>();
        if (raw == null || raw.isEmpty()) return values;
        for (String part : raw.split("&", -1)) {
            int separator = part.indexOf('=');
            if (separator < 0) throw new IllegalArgumentException("query keys require a value");
            String key = Streambench.decode(part.substring(0, separator));
            String value = Streambench.decode(part.substring(separator + 1));
            if (!allowed.contains(key)) throw new IllegalArgumentException("unknown query key");
            if (value.isEmpty()) throw new IllegalArgumentException("query values must not be empty");
            if (values.putIfAbsent(key, value) != null) throw new IllegalArgumentException("repeated query key");
        }
        return values;
    }

    private static String required(Map<String, String> query, String key) {
        String value = query.get(key);
        if (value == null) throw new IllegalArgumentException("missing required query key");
        return value;
    }

    static void session(String value) {
        if (!value.matches("[0-9a-f]{32}")) throw new IllegalArgumentException("invalid session identifier");
    }

    private static String newSession() {
        byte[] bytes = new byte[16];
        RANDOM.nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }

    private static void json(HttpExchange exchange, int status, String value) throws IOException {
        Streambench.json(exchange, status, value.getBytes(StandardCharsets.UTF_8));
    }

    // Do not hold the registry monitor during response writes, including errors.
    boolean handle(HttpExchange exchange, String path) throws IOException {
        String[] segments = path.split("/", -1);
        boolean isBatch = segments.length >= 2 && segments[1].equals("batch");
        boolean isClients = segments.length >= 2 && segments[1].equals("clients");
        if (!isBatch && !isClients) return false;
        boolean collection = segments.length == 2;
        boolean stream = isBatch && segments.length == 4 && segments[3].equals("stream");
        if (!collection && segments.length != 3 && !stream) {
            Streambench.error(exchange, 404, "unknown path");
            return true;
        }
        String allowed = collection ? "POST, OPTIONS" : stream ? "GET, OPTIONS" : "GET, DELETE, OPTIONS";
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
        try {
            if (!collection) session(segments[2]);
            String method = exchange.getRequestMethod();
            if (method.equals("OPTIONS")) {
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
                return true;
            }
            if (!List.of(allowed.split(", ")).contains(method)) {
                exchange.getResponseHeaders().set("Allow", allowed);
                Streambench.error(exchange, 405, "unsupported method");
                return true;
            }
            String raw = exchange.getRequestURI().getRawQuery();
            if (collection && isBatch) {
                var params = parameters.apply(raw);
                String snapshot;
                synchronized (this) {
                    if (batch != null && batch.active()) throw new Conflict("batch still has active work");
                    batch = new SharedBatch(newSession(), params, frames, done, slots, deadlines, limits);
                    snapshot = batch.snapshot();
                }
                json(exchange, 201, snapshot);
            } else if (collection) {
                var values = query(raw, Set.of("source", "session", "ids", "count"));
                String source = required(values, "source");
                if (!SOURCES.contains(source)) throw new IllegalArgumentException("invalid source");
                String id = required(values, "session");
                session(id);
                var ids = new ArrayList<Integer>();
                var seen = new HashSet<Integer>();
                for (String entry : required(values, "ids").split(",", -1)) {
                    int number = Streambench.decimal(entry, 4);
                    if (!seen.add(number)) throw new IllegalArgumentException("duplicate client identifier");
                    ids.add(number);
                }
                if (ids.size() > 4) throw new IllegalArgumentException("too many clients");
                int count = Streambench.decimal(required(values, "count"), done.length - 1);
                String snapshot;
                synchronized (this) {
                    if (clients != null && clients.active()) {
                        if (!clients.matches(source, id, ids, count)) throw new Conflict("client job still active");
                    } else {
                        clients = new RemoteClients(source, id, ids, count, peers.get(source), frames, done,
                            http, deadlines, limits.client());
                        clients.start();
                    }
                    snapshot = clients.snapshot();
                }
                json(exchange, 202, snapshot);
            } else if (isBatch) {
                var values = query(raw, stream ? Set.of("client") : Set.of());
                int client = 0;
                if (stream) {
                    String value = required(values, "client");
                    // The shared browser client is the one protocol integer allowed to be zero.
                    client = value.matches("0+") ? 0 : Streambench.decimal(value, 4);
                }
                SharedBatch selected;
                SharedBatch.Subscriber subscriber = null;
                synchronized (this) {
                    selected = batch;
                    if (selected == null || !selected.session.equals(segments[2])) throw new Missing();
                    if (stream) subscriber = selected.admit(client, exchange);
                    else if (method.equals("DELETE")) selected.cancel();
                }
                if (stream) subscriber.write();
                else json(exchange, 200, selected.snapshot());
            } else {
                query(raw, Set.of());
                RemoteClients selected;
                synchronized (this) {
                    selected = clients;
                    if (selected == null || !selected.session.equals(segments[2])) throw new Missing();
                    if (method.equals("DELETE")) selected.cancel();
                }
                json(exchange, 200, selected.snapshot());
            }
        } catch (IllegalArgumentException invalid) {
            Streambench.error(exchange, 400, invalid.getMessage());
        } catch (Conflict conflict) {
            Streambench.error(exchange, 409, conflict.getMessage());
        } catch (Missing missing) {
            Streambench.error(exchange, 404, "unknown session");
        } catch (Full full) {
            Streambench.error(exchange, 429, "active stream limit reached");
        }
        return true;
    }

    @Override public synchronized void close() {
        if (batch != null) batch.cancel();
        if (clients != null) clients.cancel();
        http.shutdownNow();
    }

    static final class Conflict extends RuntimeException {
        private static final long serialVersionUID = 1;
        Conflict(String message) { super(message); }
    }
    static final class Missing extends RuntimeException { private static final long serialVersionUID = 1; }
    static final class Full extends RuntimeException { private static final long serialVersionUID = 1; }
}
