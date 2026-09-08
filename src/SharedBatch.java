import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** One monotonic producer; locks protect state only, never socket operations. */
final class SharedBatch {
    private static final byte[] WAITING = ": waiting\n\n".getBytes(StandardCharsets.UTF_8);
    final String session;
    private final Streambench.Parameters parameters;
    private final byte[][] frames;
    private final byte[] done;
    private final Semaphore slots;
    private final Subscriber[] subscribers = new Subscriber[5];
    private final ScheduledFuture<?> joinTimeout;
    private final ScheduledFuture<?> absoluteTimeout;
    private final long created = System.nanoTime();
    private final long joinNanos;
    private final long lifetimeNanos;
    private String state = "waiting";
    private String error;
    private int connected;
    private int ready;
    private int openWriters;
    private int successful;
    private int produced;
    private Thread producer;
    private boolean producing;

    SharedBatch(String session, Streambench.Parameters parameters, byte[][] frames, byte[][] done,
                Semaphore slots, ScheduledThreadPoolExecutor deadlines, Distributed.Limits limits) {
        this.session = session;
        this.parameters = parameters;
        this.frames = frames;
        this.done = done[parameters.count()];
        this.slots = slots;
        joinNanos = limits.join().toNanos();
        lifetimeNanos = limits.batch().toNanos();
        // Callbacks cannot observe partly initialized futures while holding this monitor.
        synchronized (this) {
            joinTimeout = deadlines.schedule(this::expireJoin,
                Math.max(0, joinNanos - (System.nanoTime() - created)), TimeUnit.NANOSECONDS);
            absoluteTimeout = deadlines.schedule(this::expire,
                Math.max(0, lifetimeNanos - (System.nanoTime() - created)), TimeUnit.NANOSECONDS);
        }
    }

    synchronized boolean active() {
        return state.equals("waiting") || state.equals("streaming") || openWriters != 0 || producing;
    }

    synchronized String snapshot() {
        return "{\"session\":\"" + session + "\",\"state\":\"" + state
            + "\",\"expected_clients\":5,\"connected_clients\":" + connected
            + ",\"produced_events\":" + produced + ",\"error\":"
            + (error == null ? "null" : "\"" + error + "\"") + "}";
    }

    synchronized Subscriber admit(int id, HttpExchange exchange) {
        if (state.equals("waiting") && System.nanoTime() - created >= joinNanos) expireJoin();
        if (!state.equals("waiting") || subscribers[id] != null) {
            throw new Distributed.Conflict("client already joined or batch no longer waiting");
        }
        if (!slots.tryAcquire()) throw new Distributed.Full();
        var subscriber = new Subscriber(exchange);
        subscribers[id] = subscriber;
        connected++;
        openWriters++;
        return subscriber;
    }

    synchronized void cancel() {
        if (state.equals("waiting") || state.equals("streaming")) stop("cancelled", null);
    }

    private synchronized void expireJoin() {
        if (state.equals("waiting")) stop("failed", "join window expired");
    }

    private synchronized void expire() {
        if (state.equals("waiting") || state.equals("streaming")) stop("failed", "batch deadline exceeded");
    }

    // Interrupt the owning writers instead of closing exchanges here. HttpExchange.close()
    // may wait on the very output monitor that a blocked socket write is holding.
    private void stop(String terminal, String reason) {
        state = terminal;
        if (error == null) error = reason;
        if (producer != null) producer.interrupt();
        for (Subscriber subscriber : subscribers) if (subscriber != null) subscriber.abort();
        joinTimeout.cancel(false);
        absoluteTimeout.cancel(false);
    }

    private synchronized void ready(Subscriber subscriber) throws InterruptedException {
        if (subscriber.stopped || !state.equals("waiting")) throw new InterruptedException();
        if (System.nanoTime() - created >= joinNanos) {
            expireJoin();
            throw new InterruptedException();
        }
        ready++;
        if (ready == 5) {
            state = "streaming";
            joinTimeout.cancel(false);
            producing = true;
            producer = Thread.ofVirtual().name("batch-producer").unstarted(this::produce);
            producer.start();
        }
    }

    private void produce() {
        long start = System.nanoTime();
        try {
            for (int index = 0; index < parameters.count(); index++) {
                long remaining;
                while ((remaining = parameters.offset(index) - (System.nanoTime() - start)) > 0) {
                    TimeUnit.NANOSECONDS.sleep(remaining);
                }
                synchronized (this) {
                    if (!canProduce()) return;
                    produced++;
                    broadcast(frames[index]);
                }
                if (parameters.mode() == Streambench.Mode.UNPACED) Thread.yield();
            }
            synchronized (this) {
                if (canProduce()) broadcast(done);
            }
        } catch (InterruptedException cancelled) {
            Thread.currentThread().interrupt();
        } finally {
            synchronized (this) {
                producing = false;
                finish();
            }
        }
    }

    private boolean canProduce() {
        if (!state.equals("streaming") || Thread.currentThread().isInterrupted()) return false;
        if (System.nanoTime() - created >= lifetimeNanos) {
            expire();
            return false;
        }
        return connected > 0;
    }

    private void broadcast(byte[] frame) {
        for (Subscriber subscriber : subscribers) {
            if (!subscriber.stopped && !subscriber.queue.offer(frame)) {
                if (error == null) error = "subscriber queue overflow";
                subscriber.abort();
            }
        }
    }

    private void finish() {
        if (!producing && openWriters == 0 && state.equals("streaming")) {
            state = successful == 5 && error == null ? "complete" : "failed";
            if (state.equals("failed") && error == null) error = "subscriber disconnected";
            absoluteTimeout.cancel(false);
        }
    }

    final class Subscriber {
        private final HttpExchange exchange;
        private final Thread writer = Thread.currentThread();
        private final ArrayBlockingQueue<byte[]> queue = new ArrayBlockingQueue<>(256);
        private final AtomicBoolean released = new AtomicBoolean();
        private volatile boolean stopped;

        Subscriber(HttpExchange exchange) { this.exchange = exchange; }

        // All callers hold the batch monitor. Idempotence also covers writer-finally
        // racing with DELETE, queue overflow, the join timer and the absolute timer.
        private void release() {
            if (released.compareAndSet(false, true)) {
                connected--;
                slots.release();
            }
        }

        private void abort() {
            stopped = true;
            writer.interrupt();
            release();
            queue.clear();
        }

        void write() {
            boolean success = false;
            try {
                checkRunning();
                var headers = exchange.getResponseHeaders();
                headers.set("Content-Type", "text/event-stream; charset=utf-8");
                headers.set("X-Accel-Buffering", "no");
                headers.set("Connection", "close");
                exchange.sendResponseHeaders(200, 0);
                var output = exchange.getResponseBody();
                checkRunning();
                output.write(WAITING);
                output.flush();
                ready(this);
                while (true) {
                    byte[] frame = queue.take();
                    checkRunning();
                    output.write(frame);
                    output.flush();
                    if (frame == done) break;
                }
                // Detect errors in the final chunk, and retain the watchdog through close.
                output.close();
                success = true;
            } catch (IOException disconnected) {
                // The source records the failed outcome; healthy subscribers keep draining.
            } catch (InterruptedException cancelled) {
                writer.interrupt();
            } finally {
                try { exchange.close(); }
                finally {
                    synchronized (SharedBatch.this) {
                        if (success && !stopped) successful++;
                        else if (state.equals("waiting")) stop("failed", "subscriber disconnected");
                        else if (state.equals("streaming") && error == null) error = "subscriber disconnected";
                        stopped = true;
                        release();
                        queue.clear();
                        openWriters--;
                        if (connected == 0 && producing) producer.interrupt();
                        finish();
                    }
                }
            }
        }

        private void checkRunning() throws InterruptedException {
            if (stopped || writer.isInterrupted() || System.nanoTime() - created >= lifetimeNanos) {
                throw new InterruptedException();
            }
        }
    }
}
