package com.mycompany.proxy;

import com.mycompany.proxy.interceptor.AuditInterceptor;
import com.mycompany.proxy.interceptor.KafkaInterceptorChain;
import org.junit.Test;
import static org.junit.Assert.*;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.ByteArrayOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AuditLoggerTest {

    @Test
    public void testAuditLoggerOutput() throws Exception {
        int backendPort = 9097;
        int proxyPort = 9096;

        KafkaInterceptorChain chain = new KafkaInterceptorChain();
        chain.addInterceptor(new AuditInterceptor());

        // Redirect stdout to capture logs
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outContent));

        // 1. Start a Dummy Backend
        CountDownLatch backendStarted = new CountDownLatch(1);
        ExecutorService backendExecutor = Executors.newSingleThreadExecutor();
        backendExecutor.submit(() -> {
            try (ServerSocket serverSocket = new ServerSocket(backendPort)) {
                backendStarted.countDown();
                Socket clientSocket = serverSocket.accept();
                // Just accept and keep open
            } catch (Exception e) {}
        });

        assertTrue("Backend failed to start", backendStarted.await(5, TimeUnit.SECONDS));

        // 2. Start Proxy
        ExecutorService proxyExecutor = Executors.newSingleThreadExecutor();
        proxyExecutor.submit(() -> {
            try {
                new KafkaProxy(proxyPort, "localhost", backendPort, chain).run();
            } catch (Exception e) {}
        });

        Thread.sleep(1000);

        // 3. Connect Client and send a Kafka-like packet
        try (Socket socket = new Socket("localhost", proxyPort)) {
            OutputStream out = socket.getOutputStream();

            // Kafka Request Header: apiKey=0 (Produce), version=1, correlationId=999, clientId="test-client"
            String clientId = "test-client";
            byte[] msgBytes = "message".getBytes();
            int payloadLen = 2 + 2 + 4 + 2 + clientId.length() + msgBytes.length;
            java.nio.ByteBuffer b = java.nio.ByteBuffer.allocate(4 + payloadLen);
            b.putInt(payloadLen);
            b.putShort((short) 0); // API Key (Produce)
            b.putShort((short) 1); // API Version
            b.putInt(999); // Correlation ID
            b.putShort((short) clientId.length());
            b.put(clientId.getBytes());
            b.put(msgBytes);

            out.write(b.array());
            out.flush();

            Thread.sleep(500);
        } finally {
            System.setOut(originalOut);
            backendExecutor.shutdownNow();
            proxyExecutor.shutdownNow();
        }

        String output = outContent.toString();
        // Just print it to debug if it fails again
        originalOut.println("CAPTURED OUTPUT: " + output);
        assertTrue("Should contain [AUDIT] tag", output.contains("[AUDIT]"));
        // API name might vary by Kafka client version, but should contain "PRODUCE" or "produce"
        assertTrue("Should contain Produce API", output.toUpperCase().contains("PRODUCE"));
        assertTrue("Should contain CorrelationId 999", output.contains("CorrelationId=999"));
        assertTrue("Should contain ClientId test-client", output.contains("ClientId=test-client"));
    }
}
