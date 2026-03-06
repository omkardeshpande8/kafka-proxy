package com.example.proxy;

import com.example.proxy.interceptor.FieldMaskingInterceptor;
import com.example.proxy.interceptor.KafkaInterceptorChain;
import org.junit.Test;
import static org.junit.Assert.*;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class FieldMaskingTest {

    @Test
    public void testFieldMasking() throws Exception {
        int backendPort = 15099;
        int proxyPort = 15098;
        String topicName = "mask-topic";
        String originalJson = "{\"user\":\"admin\",\"ssn\":\"12345\"}";
        String expectedJson = "{\"user\":\"admin\",\"ssn\":\"*****\"}";

        FieldMaskingInterceptor masker = new FieldMaskingInterceptor();
        masker.addMaskedField("ssn");

        KafkaInterceptorChain chain = new KafkaInterceptorChain();
        chain.addInterceptor(masker);

        // 1. Start a Dummy Backend
        CountDownLatch backendStarted = new CountDownLatch(1);
        AtomicReference<String> receivedPayload = new AtomicReference<>("");
        ExecutorService backendExecutor = Executors.newSingleThreadExecutor();
        backendExecutor.submit(() -> {
            try (ServerSocket serverSocket = new ServerSocket(backendPort)) {
                backendStarted.countDown();
                Socket clientSocket = serverSocket.accept();
                InputStream in = clientSocket.getInputStream();

                byte[] lenBuf = new byte[4];
                in.read(lenBuf);
                int payloadLen = java.nio.ByteBuffer.wrap(lenBuf).getInt();

                byte[] payload = new byte[payloadLen];
                in.read(payload);

                // Extract just the JSON part (last N bytes for this POC)
                String full = new String(payload, StandardCharsets.UTF_8);
                if (full.contains("{")) {
                    receivedPayload.set(full.substring(full.indexOf("{")));
                }
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

        // 3. Send Produce packet
        try (Socket socket = new Socket("localhost", proxyPort)) {
            OutputStream out = socket.getOutputStream();

            byte[] topicBytes = topicName.getBytes();
            byte[] payloadBytes = originalJson.getBytes();
            // Header + body structure for POC
            int payloadLen = 2+2+4+2 + 10 + 6+topicBytes.length + 8 + payloadBytes.length;
            java.nio.ByteBuffer b = java.nio.ByteBuffer.allocate(4 + payloadLen);
            b.putInt(payloadLen);
            b.putShort((short) 0); // apiKey Produce
            b.putShort((short) 0); // version
            b.putInt(100); // correlationId
            b.putShort((short) 0); // clientid len
            b.putShort((short) 1); // acks
            b.putInt(1000); // timeout
            b.putInt(1); // topicsLen
            b.putShort((short) topicBytes.length);
            b.put(topicBytes);
            b.putInt(1); // partitionsLen
            b.putInt(0); // partitionId
            b.putInt(payloadBytes.length); // messageSetLen
            b.put(payloadBytes);

            out.write(b.array());
            out.flush();

            Thread.sleep(500);
        } finally {
            backendExecutor.shutdownNow();
            proxyExecutor.shutdownNow();
        }

        assertEquals("Payload should be MASKED", expectedJson, receivedPayload.get());
    }
}
