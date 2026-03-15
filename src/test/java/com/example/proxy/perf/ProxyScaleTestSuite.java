package com.example.proxy.perf;

import com.example.proxy.KafkaProxy;

import com.example.proxy.interceptor.KafkaInterceptorChain;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end scale test suite for proxy correctness and performance.
 *
 * Use system properties to tune load:
 * -Dscale.correctness.threads=16
 * -Dscale.correctness.messages=1000
 * -Dscale.correctness.payloadBytes=512
 * -Dscale.perf.threads=32
 * -Dscale.perf.messages=2000
 * -Dscale.perf.payloadBytes=1024
 * -Dscale.perf.minThroughput=3000
 * -Dscale.perf.maxP95Ms=15
 */
public class ProxyScaleTestSuite {

    private static final String BACKEND_HOST = "localhost";

    private ExecutorService executor;
    private ServerSocket backendServer;
    private volatile boolean running;
    private int backendPort;
    private int proxyPort;

    @Before
    public void setUp() throws Exception {
        executor = Executors.newCachedThreadPool();
        running = true;

        backendPort = findFreePort();
        proxyPort = findFreePort();

        backendServer = new ServerSocket(backendPort);
        startEchoBackend();
        startProxy();
        Thread.sleep(1000);
    }

    @After
    public void tearDown() throws Exception {
        running = false;
        if (backendServer != null && !backendServer.isClosed()) {
            backendServer.close();
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    public void shouldPreservePayloadIntegrityUnderScale() throws Exception {
        int threads = Integer.getInteger("scale.correctness.threads", 12);
        int messagesPerThread = Integer.getInteger("scale.correctness.messages", 500);
        int payloadBytes = Integer.getInteger("scale.correctness.payloadBytes", 256);

        ScenarioResult result = runScenario(threads, messagesPerThread, payloadBytes, false);

        assertEquals("No I/O failures expected", 0, result.ioFailures);
        assertEquals("No payload mismatches expected", 0, result.mismatches);
        assertEquals("All requests should receive responses", threads * messagesPerThread, result.completedRequests);

        System.out.printf(
                Locale.ROOT,
                "Correctness scale run complete: requests=%d, durationMs=%d, throughput=%.2f req/s%n",
                result.completedRequests,
                TimeUnit.NANOSECONDS.toMillis(result.durationNanos),
                result.throughputReqPerSec
        );
    }

    @Test
    public void shouldReportPerformanceAtScale() throws Exception {
        int threads = Integer.getInteger("scale.perf.threads", 16);
        int messagesPerThread = Integer.getInteger("scale.perf.messages", 800);
        int payloadBytes = Integer.getInteger("scale.perf.payloadBytes", 512);
        double minThroughput = Double.parseDouble(System.getProperty("scale.perf.minThroughput", "0"));
        double maxP95Ms = Double.parseDouble(System.getProperty("scale.perf.maxP95Ms", "0"));

        ScenarioResult result = runScenario(threads, messagesPerThread, payloadBytes, true);

        assertEquals("No I/O failures expected", 0, result.ioFailures);
        assertEquals("No payload mismatches expected", 0, result.mismatches);

        System.out.printf(
                Locale.ROOT,
                "Performance scale run: requests=%d throughput=%.2f req/s p50=%.3fms p95=%.3fms p99=%.3fms max=%.3fms%n",
                result.completedRequests,
                result.throughputReqPerSec,
                result.p50Ms,
                result.p95Ms,
                result.p99Ms,
                result.maxMs
        );

        if (minThroughput > 0) {
            assertTrue(
                    String.format(Locale.ROOT, "Throughput %.2f req/s below required %.2f req/s", result.throughputReqPerSec, minThroughput),
                    result.throughputReqPerSec >= minThroughput
            );
        }
        if (maxP95Ms > 0) {
            assertTrue(
                    String.format(Locale.ROOT, "P95 %.3f ms above required %.3f ms", result.p95Ms, maxP95Ms),
                    result.p95Ms <= maxP95Ms
            );
        }
    }

    private ScenarioResult runScenario(int threads, int messagesPerThread, int payloadBytes, boolean collectLatency) throws Exception {
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch doneGate = new CountDownLatch(threads);

        AtomicInteger mismatches = new AtomicInteger();
        AtomicInteger ioFailures = new AtomicInteger();
        AtomicInteger completed = new AtomicInteger();
        ConcurrentLinkedQueue<Long> latencyNanos = collectLatency ? new ConcurrentLinkedQueue<Long>() : null;

        long start = System.nanoTime();

        for (int t = 0; t < threads; t++) {
            final int clientId = t;
            executor.submit(() -> {
                try (Socket socket = new Socket(BACKEND_HOST, proxyPort);
                     DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                     DataInputStream in = new DataInputStream(socket.getInputStream())) {

                    startGate.await();

                    for (int i = 0; i < messagesPerThread; i++) {
                        byte[] payload = buildPayload(clientId, i, payloadBytes);
                        long begin = System.nanoTime();
                        out.writeInt(payload.length);
                        out.write(payload);
                        out.flush();

                        int responseLength = in.readInt();
                        byte[] response = new byte[responseLength];
                        in.readFully(response);
                        long end = System.nanoTime();

                        if (collectLatency) {
                            latencyNanos.add(end - begin);
                        }

                        if (responseLength != payload.length || !Arrays.equals(payload, response)) {
                            mismatches.incrementAndGet();
                        }
                        completed.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    ioFailures.incrementAndGet();
                } finally {
                    doneGate.countDown();
                }
            });
        }

        startGate.countDown();
        long minTimeout = Math.max(120, (long) threads * messagesPerThread / 100);
        long timeoutSeconds = Long.getLong("scale.timeoutSeconds", minTimeout);
        assertTrue("Scenario timed out", doneGate.await(timeoutSeconds, TimeUnit.SECONDS));

        long duration = System.nanoTime() - start;
        int total = completed.get();
        double throughput = total / (duration / 1_000_000_000.0);

        ScenarioResult result = new ScenarioResult();
        result.completedRequests = total;
        result.ioFailures = ioFailures.get();
        result.mismatches = mismatches.get();
        result.durationNanos = duration;
        result.throughputReqPerSec = throughput;

        if (collectLatency && latencyNanos != null && !latencyNanos.isEmpty()) {
            List<Long> values = new ArrayList<Long>(latencyNanos);
            Collections.sort(values);
            result.p50Ms = percentileMs(values, 0.50);
            result.p95Ms = percentileMs(values, 0.95);
            result.p99Ms = percentileMs(values, 0.99);
            result.maxMs = values.get(values.size() - 1) / 1_000_000.0;
        }

        return result;
    }

    private void startEchoBackend() {
        executor.submit(() -> {
            while (running) {
                try {
                    final Socket client = backendServer.accept();
                    executor.submit(() -> handleBackendClient(client));
                } catch (Exception e) {
                    if (running) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    private void handleBackendClient(Socket client) {
        try (Socket socket = client;
             DataInputStream in = new DataInputStream(socket.getInputStream());
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
            while (running) {
                int length = in.readInt();
                byte[] payload = new byte[length];
                in.readFully(payload);

                out.writeInt(length);
                out.write(payload);
                out.flush();
            }
        } catch (Exception ignored) {
            // Client disconnected or teardown in progress.
        }
    }

    private void startProxy() {
        executor.submit(() -> {
            try {
                KafkaInterceptorChain chain = new KafkaInterceptorChain();
                KafkaProxy proxy = new KafkaProxy(proxyPort, BACKEND_HOST, backendPort, chain);
                proxy.run();
            } catch (Exception e) {
                if (running) {
                    e.printStackTrace();
                }
            }
        });
    }

    private static int findFreePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static byte[] buildPayload(int clientId, int sequence, int payloadBytes) {
        int bodyBytes = Math.max(payloadBytes - 12, 1);
        byte[] body = new byte[bodyBytes];
        for (int i = 0; i < body.length; i++) {
            body[i] = (byte) ((clientId * 31 + sequence * 17 + i) & 0x7F);
        }

        CRC32 crc32 = new CRC32();
        crc32.update(body);
        long crc = crc32.getValue();

        ByteBuffer buffer = ByteBuffer.allocate(12 + body.length);
        buffer.putInt(clientId);
        buffer.putInt(sequence);
        buffer.putInt((int) crc);
        buffer.put(body);
        return buffer.array();
    }

    private static double percentileMs(List<Long> sortedNanos, double percentile) {
        int index = (int) Math.ceil(percentile * sortedNanos.size()) - 1;
        index = Math.max(0, Math.min(index, sortedNanos.size() - 1));
        return sortedNanos.get(index) / 1_000_000.0;
    }

    private static final class ScenarioResult {
        private int completedRequests;
        private int ioFailures;
        private int mismatches;
        private long durationNanos;
        private double throughputReqPerSec;
        private double p50Ms;
        private double p95Ms;
        private double p99Ms;
        private double maxMs;
    }
}
