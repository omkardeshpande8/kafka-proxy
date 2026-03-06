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

            // Send data with Kafka-like framing [length (4 bytes)] [payload]
            byte[] payload = testMessage.getBytes(StandardCharsets.UTF_8);
            java.nio.ByteBuffer b = java.nio.ByteBuffer.allocate(4 + payload.length);
            b.putInt(payload.length);
            b.put(payload);

            out.write(b.array());
            out.flush();

            // Read response (should be echoed back with framing)
            // First 4 bytes length
            byte[] lenBuf = new byte[4];
            int readLen = in.read(lenBuf);
            assertEquals(4, readLen);
            int responseLen = java.nio.ByteBuffer.wrap(lenBuf).getInt();
            assertEquals(payload.length, responseLen);

            byte[] buffer = new byte[responseLen];
            int read = in.read(buffer);
            assertEquals(responseLen, read);
            String response = new String(buffer, 0, read, StandardCharsets.UTF_8);

            assertEquals("Response should match sent message", testMessage, response);
        } finally {
            backendExecutor.shutdownNow();
            proxyExecutor.shutdownNow();
        }
    }

    @Test
    public void testProxyForwardsKafkaRequest() throws Exception {
        int backendPort = 10093;
        int proxyPort = 10092;
        String clientId = "test-client";
        String testMessage = "Kafka Round-trip";

        // 1. Start a Dummy Backend
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
            } catch (Exception e) {}
        });

        assertTrue("Backend failed to start", backendStarted.await(5, TimeUnit.SECONDS));

        // 2. Start Proxy
        ExecutorService proxyExecutor = Executors.newSingleThreadExecutor();
        proxyExecutor.submit(() -> {
            try {
                new KafkaProxy(proxyPort, "localhost", backendPort).run();
            } catch (Exception e) {}
        });

        Thread.sleep(1000);

        // 3. Connect Client to Proxy
        try (Socket socket = new Socket("localhost", proxyPort)) {
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            // Send valid Kafka-like Request
            byte[] msgBytes = testMessage.getBytes(StandardCharsets.UTF_8);
            int payloadLen = 2 + 2 + 4 + 2 + clientId.length() + msgBytes.length;
            java.nio.ByteBuffer b = java.nio.ByteBuffer.allocate(4 + payloadLen);
            b.putInt(payloadLen);
            b.putShort((short) 0); // API Key (Produce)
            b.putShort((short) 0); // API Version
            b.putInt(12345); // Correlation ID
            b.putShort((short) clientId.length());
            b.put(clientId.getBytes());
            b.put(msgBytes);

            out.write(b.array());
            out.flush();

            // Read response
            byte[] lenBuf = new byte[4];
            int readLen = in.read(lenBuf);
            assertEquals(4, readLen);
            int responseLen = java.nio.ByteBuffer.wrap(lenBuf).getInt();
            assertEquals(payloadLen, responseLen);

            byte[] buffer = new byte[responseLen];
            int read = in.read(buffer);
            assertEquals(responseLen, read);

            // Should match what we sent
            java.nio.ByteBuffer resp = java.nio.ByteBuffer.wrap(buffer);
            assertEquals(0, resp.getShort()); // apiKey
            assertEquals(0, resp.getShort()); // apiVersion
            assertEquals(12345, resp.getInt()); // correlationId
            assertEquals(clientId.length(), resp.getShort());
            byte[] clientResp = new byte[clientId.length()];
            resp.get(clientResp);
            assertEquals(clientId, new String(clientResp));
            byte[] msgResp = new byte[testMessage.length()];
            resp.get(msgResp);
            assertEquals(testMessage, new String(msgResp));
        } finally {
            backendExecutor.shutdownNow();
            proxyExecutor.shutdownNow();
        }
    }
}
