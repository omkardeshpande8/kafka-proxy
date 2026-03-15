package com.mycompany.proxy;

import com.mycompany.proxy.interceptor.JsonValidationInterceptor;
import com.mycompany.proxy.interceptor.KafkaInterceptorChain;
import org.junit.Test;
import static org.junit.Assert.*;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class JsonValidationTest {

    @Test
    public void testJsonValidationBlocksInvalidJson() throws Exception {
        int backendPort = 14099;
        int proxyPort = 14098;
        String invalidJson = "this-is-not-json";

        KafkaInterceptorChain chain = new KafkaInterceptorChain();
        chain.addInterceptor(new JsonValidationInterceptor());

        // 1. Start a Dummy Backend
        CountDownLatch backendStarted = new CountDownLatch(1);
        AtomicBoolean backendReceivedRequest = new AtomicBoolean(false);
        ExecutorService backendExecutor = Executors.newSingleThreadExecutor();
        backendExecutor.submit(() -> {
            try (ServerSocket serverSocket = new ServerSocket(backendPort)) {
                backendStarted.countDown();
                Socket clientSocket = serverSocket.accept();
                if (clientSocket.getInputStream().read() != -1) {
                    backendReceivedRequest.set(true);
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

        // 3. Connect Client and send an invalid JSON Produce packet
        try (Socket socket = new Socket("localhost", proxyPort)) {
            socket.setSoTimeout(1000);
            OutputStream out = socket.getOutputStream();

            // Kafka Produce Request v0 (Body: acks, timeout, topics array)
            // Mocking structure to reach payload extraction in POC
            byte[] topicBytes = "test-topic".getBytes();
            byte[] payloadBytes = invalidJson.getBytes();
            // acks(2), timeout(4), topicsLen(4) -> 10
            // topicNameLen(2), topicName(N), partitionsLen(4) -> 6 + N
            // partitionId(4), messageSetLen(4) -> 8
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

        assertFalse("Backend should NOT have received invalid JSON", backendReceivedRequest.get());
    }
}
