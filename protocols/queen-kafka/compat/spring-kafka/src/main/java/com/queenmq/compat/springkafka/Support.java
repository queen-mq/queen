package com.queenmq.compat.springkafka;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;

/**
 * Assertion / deadline / log-capture plumbing shared by every phase.
 *
 * <p>Three things live here because every compat suite in this tree needs them:
 * <ul>
 *   <li>{@link Check} - the "  ok" / "  FAIL" line format and the failure counter that
 *       decides the process exit code.</li>
 *   <li>{@link #await} - a deadline around every blocking wait.  A hang is a RESULT,
 *       not a reason to sit forever; nothing in this suite blocks without one.</li>
 *   <li>{@link LogCapture} - an in-memory logback appender bolted onto
 *       {@code org.apache.kafka.clients} at DEBUG so the suite can print the API
 *       versions kafka-clients ACTUALLY negotiated (out of NetworkClient's own
 *       "Recorded ApiVersions for node" line) rather than assuming them, and so a
 *       failing phase can dump the last few hundred client-internal lines.</li>
 * </ul>
 */
public final class Support {

    private Support() {
    }

    // ---------------------------------------------------------------- Check

    public static final class Check {
        private int failures = 0;
        private int passes = 0;

        public void section(String title) {
            System.out.println();
            System.out.println("=== " + title);
        }

        public boolean ok(boolean condition, String message) {
            if (condition) {
                passes++;
                System.out.println("  ok   " + message);
            } else {
                failures++;
                System.out.println("  FAIL " + message);
            }
            return condition;
        }

        public void pass(String message) {
            ok(true, message);
        }

        public void fail(String message) {
            ok(false, message);
        }

        /** Informational line: not an assertion, never affects the exit code. */
        public void note(String message) {
            System.out.println("  note " + message);
        }

        public void eq(long expected, long actual, String message) {
            ok(expected == actual, message + " (expected=" + expected + " actual=" + actual + ")");
        }

        public int failures() {
            return failures;
        }

        public int passes() {
            return passes;
        }
    }

    // -------------------------------------------------------------- deadline

    /**
     * Poll {@code condition} until it is true or {@code timeoutMs} elapses.
     * Returns true if the condition held before the deadline.
     */
    public static boolean await(String what, long timeoutMs, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            sleep(100);
        }
        boolean last = condition.getAsBoolean();
        if (!last) {
            System.out.println("  ---- deadline: '" + what + "' did not become true within " + timeoutMs + "ms");
        }
        return last;
    }

    /**
     * Wait for a count to stop growing: useful when "everything that is coming has
     * arrived" cannot be expressed as an exact number (e.g. duplicates across a
     * rebalance).  Returns the settled value.
     */
    public static long awaitQuiescence(java.util.function.LongSupplier counter, long quietMs, long maxMs) {
        long deadline = System.nanoTime() + maxMs * 1_000_000L;
        long last = counter.getAsLong();
        long lastChange = System.nanoTime();
        while (System.nanoTime() < deadline) {
            sleep(100);
            long now = counter.getAsLong();
            if (now != last) {
                last = now;
                lastChange = System.nanoTime();
            } else if (System.nanoTime() - lastChange > quietMs * 1_000_000L) {
                return last;
            }
        }
        return counter.getAsLong();
    }

    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    // ------------------------------------------------------------ LogCapture

    /** Ring buffer over kafka-clients' own DEBUG stream. */
    public static final class LogCapture extends AppenderBase<ILoggingEvent> {

        private static final int RING = 400;
        private final Deque<String> ring = new ArrayDeque<>();
        private final Set<String> apiVersions = new LinkedHashSet<>();
        private final Set<String> markers = new LinkedHashSet<>();

        @Override
        protected void append(ILoggingEvent event) {
            String line = event.getLoggerName() + " " + event.getFormattedMessage();
            synchronized (ring) {
                ring.addLast(line);
                while (ring.size() > RING) {
                    ring.removeFirst();
                }
            }
            String msg = event.getFormattedMessage();
            if (msg != null) {
                // kafka-clients 3.x logs the negotiated table as
                //   "Node -1 has finalized features epoch: ..., API versions: (Produce(0): 3 to 9 [usable: 9], ...)"
                // Older lines said "Recorded ApiVersions for node ...".  Match either, and
                // keep only the table so every client collapses to the same entry.
                int i = msg.indexOf("API versions: ");
                if (i < 0) {
                    i = msg.indexOf("Recorded ApiVersions");
                }
                if (i >= 0) {
                    synchronized (apiVersions) {
                        apiVersions.add(msg.substring(i));
                    }
                }
                latch(msg);
            }
        }

        private void latch(String msg) {
            synchronized (markers) {
                // The facade answers an out-of-window ApiVersions with UNSUPPORTED_VERSION
                // (35) and a v0-encoded body; the client then retries at a version it
                // offered.  Latching this proves the downgrade actually happened.
                if (msg.contains("apiKey=API_VERSIONS") && msg.contains("errorCode=35")) {
                    markers.add("apiversions-downgrade");
                }
                if (msg.contains("does not support INIT_PRODUCER_ID")) {
                    markers.add("initproducerid-unsupported");
                }
                if (msg.contains("MEMBER_ID_REQUIRED")) {
                    markers.add("member-id-required");
                }
                if (msg.contains("Discovered group coordinator")) {
                    markers.add("group-coordinator-found");
                }
            }
        }

        public List<String> negotiated() {
            synchronized (apiVersions) {
                return new ArrayList<>(apiVersions);
            }
        }

        /**
         * True if a line matching this marker was EVER seen (markers are latched in
         * {@link #append}, so unlike {@link #tail} they do not age out of the ring).
         */
        public boolean saw(String marker) {
            synchronized (markers) {
                return markers.contains(marker);
            }
        }

        public List<String> tail(int n) {
            synchronized (ring) {
                List<String> all = new ArrayList<>(ring);
                return all.size() <= n ? all : all.subList(all.size() - n, all.size());
            }
        }

        /**
         * Attach to {@code org.apache.kafka.clients} at DEBUG with additivity off, so
         * the DEBUG torrent lands here and NOT on the console.  Called after Spring
         * Boot has finished configuring logback (otherwise Boot's own initialisation
         * would drop the appender again).
         */
        public static LogCapture install() {
            LogCapture capture = new LogCapture();
            ch.qos.logback.classic.LoggerContext ctx =
                    (ch.qos.logback.classic.LoggerContext) LoggerFactory.getILoggerFactory();
            capture.setContext(ctx);
            capture.setName("queen-compat-capture");
            capture.start();
            ch.qos.logback.classic.Logger clients =
                    (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("org.apache.kafka.clients");
            clients.setLevel(Level.DEBUG);
            clients.setAdditive(false);
            clients.addAppender(capture);
            return capture;
        }
    }
}
