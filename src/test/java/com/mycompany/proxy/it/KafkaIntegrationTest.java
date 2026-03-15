package com.mycompany.proxy.it;

import com.mycompany.proxy.KafkaProxy;
import com.mycompany.proxy.interceptor.KafkaInterceptorChain;
import com.mycompany.proxy.ProxyConfig;
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
 * End-to-end integration test for Kafka Proxy.
 * Simulates a client and a broker to test proxy behavior.
 */
public class KafkaIntegrationTest {

    private static final int PROXY_PORT = 20092;
    private static final int BACKEND_PORT = 20093;
    private static final String BACKEND_HOST = "localhost";

    private ExecutorService executor;
    private ServerSocket backendSocket;
    private volatile boolean running = true;
    private final AtomicInteger requestCount = new AtomicInteger(0);

    @Before
    public void setUp() throws Exception {
        executor = Executors.newCachedThreadPool();
        running = true;
        requestCount.set(0);

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

                            requestCount.incrementAndGet();

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
    public void testFullRoundTrip() throws Exception {
        String testMessage = "Integration Test Message";
        byte[] payload = testMessage.getBytes(StandardCharsets.UTF_8);

        try (Socket socket = new Socket("localhost", PROXY_PORT);
             OutputStream out = socket.getOutputStream();
             DataInputStream din = new DataInputStream(socket.getInputStream())) {

            // Send Kafka-framed message
            ByteBuffer b = ByteBuffer.allocate(4 + payload.length);
            b.putInt(payload.length);
            b.put(payload);
            out.write(b.array());
            out.flush();

            // Read response
            int respLen = din.readInt();
            assertEquals(payload.length, respLen);

            byte[] respPayload = new byte[respLen];
            din.readFully(respPayload);
            assertEquals(testMessage, new String(respPayload, StandardCharsets.UTF_8));
        }

        assertEquals("Backend should have received exactly 1 request", 1, requestCount.get());
    }

    @Test
    public void testMultipleRequests() throws Exception {
        int numRequests = 10;
        try (Socket socket = new Socket("localhost", PROXY_PORT);
             OutputStream out = socket.getOutputStream();
             DataInputStream din = new DataInputStream(socket.getInputStream())) {

            for (int i = 0; i < numRequests; i++) {
                String msg = "Request-" + i;
                byte[] payload = msg.getBytes(StandardCharsets.UTF_8);
                ByteBuffer b = ByteBuffer.allocate(4 + payload.length);
                b.putInt(payload.length);
                b.put(payload);
                out.write(b.array());
                out.flush();

                int respLen = din.readInt();
                byte[] respPayload = new byte[respLen];
                din.readFully(respPayload);
                assertEquals(msg, new String(respPayload, StandardCharsets.UTF_8));
            }
        }
        assertEquals(numRequests, requestCount.get());
    }
}
