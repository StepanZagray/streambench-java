import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** A bounded job of asynchronous JDK HTTP subscribers, each with its own monotonic clock. */
final class RemoteClients {
    final String session;
    private final String source;
    private final List<Integer> ids;
    private final int count;
    private final List<Client> clients;
    private boolean cancelled;

    RemoteClients(String source, String session, List<Integer> ids, int count, URI origin,
                  byte[][] frames, byte[][] done, HttpClient http,
                  ScheduledThreadPoolExecutor deadlines, Duration lifetime) {
        this.source = source;
        this.session = session;
        this.ids = List.copyOf(ids);
        this.count = count;
        clients = ids.stream().map(id -> new Client(id,
            origin.resolve("/batch/" + session + "/stream?client=" + id), frames, done[count],
            count, http, deadlines, lifetime)).toList();
    }

    void start() { clients.forEach(Client::start); }

    synchronized boolean active() { return state().equals("running"); }

    boolean matches(String source, String session, List<Integer> ids, int count) {
        return this.source.equals(source) && this.session.equals(session) && this.count == count
            && this.ids.size() == ids.size() && this.ids.containsAll(ids);
    }

    synchronized void cancel() {
        if (!active()) return;
        cancelled = true;
        for (Client client : clients) client.finish("cancelled", null);
    }

    private String state() {
        if (cancelled) return "cancelled";
        if (clients.stream().anyMatch(Client::active)) return "running";
        return clients.stream().allMatch(client -> client.state().equals("complete")) ? "complete" : "failed";
    }

    synchronized String snapshot() {
        // Capture each outcome once so job and child states agree even during completion.
        var snapshots = clients.stream().map(Client::snapshot).toList();
        String state = cancelled ? "cancelled" : snapshots.stream().anyMatch(Snapshot::active) ? "running"
            : snapshots.stream().allMatch(snapshot -> snapshot.state().equals("complete")) ? "complete" : "failed";
        return "{\"session\":\"" + session + "\",\"state\":\"" + state + "\",\"clients\":["
            + String.join(",", snapshots.stream().map(Snapshot::json).toList()) + "]}";
    }

    private record Snapshot(String state, String json) {
        boolean active() { return state.equals("connecting") || state.equals("streaming"); }
    }

    private static final class Client implements HttpResponse.BodySubscriber<Void> {
        private final int id;
        private final URI uri;
        private final SseReader reader;
        private final HttpClient http;
        private final ScheduledThreadPoolExecutor deadlines;
        private final Duration lifetime;
        private final double[] gaps;
        private final CompletableFuture<Void> body = new CompletableFuture<>();
        private CompletableFuture<HttpResponse<Void>> request;
        private Flow.Subscription subscription;
        private ScheduledFuture<?> timeout;
        private String state = "connecting";
        private String error;
        private long start;
        private long end;
        private long first;
        private long previous;
        private int events;

        Client(int id, URI uri, byte[][] frames, byte[] done, int count, HttpClient http,
               ScheduledThreadPoolExecutor deadlines, Duration lifetime) {
            this.id = id;
            this.uri = uri;
            this.http = http;
            this.deadlines = deadlines;
            this.lifetime = lifetime;
            gaps = new double[Math.max(0, count - 1)];
            reader = new SseReader(frames, done, count, now -> {
                if (events == 0) first = now;
                else gaps[events - 1] = (now - previous) / 1e6;
                previous = now;
                events++;
            });
        }

        void start() {
            synchronized (this) {
                start = System.nanoTime();
                timeout = deadlines.schedule(() -> finish("failed", "client deadline exceeded"),
                    lifetime.toNanos(), TimeUnit.NANOSECONDS);
            }
            var outgoing = HttpRequest.newBuilder(uri).timeout(lifetime)
                .header("Accept", "text/event-stream").GET().build();
            try {
                var pending = http.sendAsync(outgoing, info -> {
                    String type = info.headers().firstValue("Content-Type").orElse("").split(";", 2)[0].trim();
                    if (info.statusCode() != 200 || !type.equalsIgnoreCase("text/event-stream")
                        || info.headers().firstValue("Content-Encoding").filter(value -> !value.equalsIgnoreCase("identity")).isPresent()) {
                        finish("failed", "source returned a non-SSE response");
                    } else {
                        synchronized (this) { if (active()) state = "streaming"; }
                    }
                    return this;
                });
                boolean cancel;
                synchronized (this) {
                    request = pending;
                    cancel = !active();
                }
                if (cancel) pending.cancel(true);
                pending.whenComplete((response, failure) -> {
                    if (failure != null) finish("failed", "source request failed");
                });
            } catch (RuntimeException failed) {
                finish("failed", "source request failed");
            }
        }

        synchronized String state() { return state; }
        synchronized boolean active() { return state.equals("connecting") || state.equals("streaming"); }

        synchronized Snapshot snapshot() {
            Double p95 = null;
            if (events > 1) {
                double[] sorted = Arrays.copyOf(gaps, events - 1);
                Arrays.sort(sorted);
                p95 = sorted[(int) Math.ceil(sorted.length * 0.95) - 1];
            }
            double elapsed = start == 0 ? 0 : ((active() ? System.nanoTime() : end) - start) / 1e6;
            return new Snapshot(state, "{\"id\":" + id + ",\"state\":\"" + state + "\",\"events\":" + events
                + ",\"first_event_ms\":" + (events == 0 ? "null" : Double.toString((first - start) / 1e6))
                + ",\"p95_gap_ms\":" + p95 + ",\"elapsed_ms\":" + elapsed + ",\"error\":"
                + (error == null ? "null" : "\"" + error + "\"") + "}");
        }

        void finish(String terminal, String reason) {
            Flow.Subscription cancelBody;
            CompletableFuture<HttpResponse<Void>> cancelRequest;
            synchronized (this) {
                if (!active()) return;
                state = terminal;
                error = reason;
                end = System.nanoTime();
                if (timeout != null) timeout.cancel(false);
                cancelBody = subscription;
                cancelRequest = request;
            }
            if (terminal.equals("complete")) body.complete(null);
            else {
                // Neither cancellation path waits for a blocked response body read.
                if (cancelBody != null) cancelBody.cancel();
                if (cancelRequest != null) cancelRequest.cancel(true);
                body.completeExceptionally(new IOException(reason == null ? "client cancelled" : reason));
            }
        }

        @Override public CompletionStage<Void> getBody() { return body; }

        @Override public void onSubscribe(Flow.Subscription incoming) {
            boolean cancel;
            synchronized (this) {
                cancel = !active() || subscription != null;
                if (!cancel) subscription = incoming;
            }
            if (cancel) incoming.cancel();
            else incoming.request(1);
        }

        @Override public void onNext(List<ByteBuffer> buffers) {
            String failure = null;
            Flow.Subscription next;
            synchronized (this) {
                if (!active()) return;
                try {
                    for (ByteBuffer buffer : buffers) reader.accept(buffer);
                } catch (IOException invalid) { failure = invalid.getMessage(); }
                next = subscription;
            }
            if (failure != null) finish("failed", failure);
            else if (next != null) next.request(1);
        }

        @Override public void onError(Throwable failure) { finish("failed", "source response failed"); }

        @Override public void onComplete() {
            String failure = null;
            synchronized (this) {
                if (!active()) return;
                try { reader.end(); }
                catch (IOException invalid) { failure = invalid.getMessage(); }
            }
            if (failure == null) finish("complete", null);
            else finish("failed", failure);
        }
    }
}
