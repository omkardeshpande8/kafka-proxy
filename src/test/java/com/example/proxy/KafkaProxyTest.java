package com.example.proxy;

import org.junit.Test;
import static org.junit.Assert.*;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaProxyTest {

    @Test
    public void testProxyForwardsData() throws Exception {
        int backendPort = 9093;
        int proxyPort = 9092;
        String testMessage = "Hello Kafka Proxy!";

        // 1. Start a Dummy Backend (Echo Server)
        CountDownLatch backendStarted = new CountDownLatch(1);
        ExecutorService backendExecutor = Executors.newSingleThreadExecutor();
        backendExecutor.submit(() -> {
            try (ServerSocket serverSocket = new ServerSocket(backendPort)) {
                backendStarted.countDown();
                Socket clientSocket = serverSocket.accept();
                InputStream in = clientSocket.getInputStream();
                OutputStream out = clientSocket.getOutputStream();
                byte[] buffer = new byte[1024];
                int read;
                while ((read = in.read(buffer)) != -1) {
                    out.write(buffer, 0, read);
                    out.flush();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        assertTrue("Backend failed to start", backendStarted.await(5, TimeUnit.SECONDS));

        // 2. Start Proxy
        ExecutorService proxyExecutor = Executors.newSingleThreadExecutor();
        proxyExecutor.submit(() -> {
            try {
                new KafkaProxy(proxyPort, "localhost", backendPort).run();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // Give proxy a moment to bind
        Thread.sleep(1000);

        // 3. Connect Client to Proxy
        try (Socket socket = new Socket("localhost", proxyPort)) {
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            // Send data
            out.write(testMessage.getBytes(StandardCharsets.UTF_8));
            out.flush();

            // Read response (should be echoed back)
            byte[] buffer = new byte[1024];
            int read = in.read(buffer);
            assertTrue("Should have read some data", read > 0);
            String response = new String(buffer, 0, read, StandardCharsets.UTF_8);

            assertEquals("Response should match sent message", testMessage, response);
        } finally {
            backendExecutor.shutdownNow();
            proxyExecutor.shutdownNow();
        }
    }
}
