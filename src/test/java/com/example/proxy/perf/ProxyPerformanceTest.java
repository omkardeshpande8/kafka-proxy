package com.example.proxy.perf;

import com.example.proxy.KafkaProxy;
import com.example.proxy.interceptor.KafkaInterceptorChain;
import com.example.proxy.ProxyConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import java.io.DataInputStream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Performance test for Kafka Proxy.
 * Measures latency and throughput under load.
 */
public class ProxyPerformanceTest {

    private static final int PROXY_PORT = 30092;
    private static final int BACKEND_PORT = 30093;
    private static final String BACKEND_HOST = "localhost";

    private ExecutorService executor;
    private ServerSocket backendSocket;
    private volatile boolean running = true;

    @Before
    public void setUp() throws Exception {
        executor = Executors.newCachedThreadPool();
        running = true;

        // Start dummy backend
        backendSocket = new ServerSocket(BACKEND_PORT);
        executor.submit(() -> {
            try {
                while (running) {
                    try (Socket client = backendSocket.accept();
                         InputStream in = client.getInputStream();
                         OutputStream out = client.getOutputStream()) {

                        DataInputStream din = new DataInputStream(in);
                        while (running) {
                            int len = din.readInt();
                            byte[] payload = new byte[len];
                            din.readFully(payload);

                            // Echo back
                            ByteBuffer b = ByteBuffer.allocate(4 + len);
                            b.putInt(len);
                            b.put(payload);
                            out.write(b.array());
                            out.flush();
                        }
                    } catch (Exception e) {
                        if (running) e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                if (running) e.printStackTrace();
            }
        });

        // Start Proxy
        KafkaInterceptorChain chain = ProxyConfig.loadInterceptors("proxy.properties");
        KafkaProxy proxy = new KafkaProxy(PROXY_PORT, BACKEND_HOST, BACKEND_PORT, chain);
        executor.submit(() -> {
            try {
                proxy.run();
            } catch (Exception e) {
                if (running) e.printStackTrace();
            }
        });

        // Give them time to start
        Thread.sleep(1000);
    }

    @After
    public void tearDown() throws Exception {
        running = false;
        if (backendSocket != null) {
            backendSocket.close();
        }
        executor.shutdownNow();
    }

    @Test
    public void measureThroughput() throws Exception {
        int numThreads = 2;
        int messagesPerThread = 500;
        byte[] payload = "Hello Performance!".getBytes(StandardCharsets.UTF_8);
        CountDownLatch latch = new CountDownLatch(numThreads);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try (Socket socket = new Socket("localhost", PROXY_PORT);
                     OutputStream out = socket.getOutputStream();
                     DataInputStream din = new DataInputStream(socket.getInputStream())) {

                    for (int j = 0; j < messagesPerThread; j++) {
                        ByteBuffer b = ByteBuffer.allocate(4 + payload.length);
                        b.putInt(payload.length);
                        b.put(payload);
                        out.write(b.array());
                        out.flush();

                        int respLen = din.readInt();
                        byte[] respPayload = new byte[respLen];
                        din.readFully(respPayload);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue("Timeout waiting for performance test to complete", latch.await(60, TimeUnit.SECONDS));
        long endTime = System.currentTimeMillis();

        long duration = endTime - startTime;
        int totalMessages = numThreads * messagesPerThread;
        double throughput = (double) totalMessages / (duration / 1000.0);

        System.out.printf("Throughput: %.2f requests/sec (Total: %d messages in %d ms)\n",
                          throughput, totalMessages, duration);
    }

    @Test
    public void measureLatency() throws Exception {
        int numRequests = 1000;
        byte[] payload = "Hello Latency!".getBytes(StandardCharsets.UTF_8);
        long totalLatency = 0;

        try (Socket socket = new Socket("localhost", PROXY_PORT);
             OutputStream out = socket.getOutputStream();
             DataInputStream din = new DataInputStream(socket.getInputStream())) {

            for (int i = 0; i < numRequests; i++) {
                long start = System.nanoTime();

                ByteBuffer b = ByteBuffer.allocate(4 + payload.length);
                b.putInt(payload.length);
                b.put(payload);
                out.write(b.array());
                out.flush();

                int respLen = din.readInt();
                byte[] respPayload = new byte[respLen];
                din.readFully(respPayload);

                long end = System.nanoTime();
                totalLatency += (end - start);
            }
        }

        double avgLatencyMs = (double) totalLatency / numRequests / 1_000_000.0;
        System.out.printf("Average Latency: %.3f ms\n", avgLatencyMs);
    }
}
