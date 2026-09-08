import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/** Incremental SSE validation against the immutable local fixture; retains no received text. */
final class SseReader {
    static final int MAX_UNPARSED = 1024 * 1024;
    interface TextEvent { void complete(long now); }
    private final byte[][] frames;
    private final byte[] expectedDone;
    private final int count;
    private final TextEvent onText;
    private final ByteArrayOutputStream line = new ByteArrayOutputStream();
    private final ByteArrayOutputStream frame = new ByteArrayOutputStream();
    private int unparsed;
    private int events;
    private boolean done;

    SseReader(byte[][] frames, byte[] expectedDone, int count, TextEvent onText) {
        this.frames = frames;
        this.expectedDone = expectedDone;
        this.count = count;
        this.onText = onText;
    }

    void accept(ByteBuffer input) throws IOException {
        while (input.hasRemaining()) {
            byte value = input.get();
            if (++unparsed > MAX_UNPARSED) throw new IOException("unparsed SSE exceeds 1 MiB");
            if (value != '\n') {
                line.write(value);
                continue;
            }
            byte[] bytes = line.toByteArray();
            line.reset();
            int length = bytes.length;
            if (length > 0 && bytes[length - 1] == '\r') length--;
            if (length == 0) {
                if (frame.size() != 0) {
                    frame.write('\n');
                    event(frame.toByteArray());
                    frame.reset();
                }
                unparsed = 0;
            } else if (bytes[0] != ':') {
                frame.write(bytes, 0, length);
                frame.write('\n');
            } else {
                // A parsed comment does not consume the pending event budget forever.
                unparsed -= bytes.length + 1;
            }
        }
    }

    private void event(byte[] bytes) throws IOException {
        if (done) throw new IOException("extra event after done");
        if (events < count && Arrays.equals(bytes, frames[events])) {
            events++;
            onText.complete(System.nanoTime());
        } else if (events == count && Arrays.equals(bytes, expectedDone)) {
            done = true;
        } else {
            throw new IOException("SSE event does not match fixture or done count");
        }
    }

    void end() throws IOException {
        if (line.size() != 0 || frame.size() != 0) throw new IOException("truncated SSE frame");
        if (!done || events != count) throw new IOException("premature EOF before done");
    }
}
