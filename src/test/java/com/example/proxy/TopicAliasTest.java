package com.example.proxy;

import com.example.proxy.interceptor.KafkaInterceptorChain;
import com.example.proxy.interceptor.TopicAliasInterceptor;
import org.junit.Test;
import static org.junit.Assert.*;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TopicAliasTest {

    @Test
    public void testTopicAliasingRemapsTopic() throws Exception {
        int backendPort = 11099;
        int proxyPort = 11098;
        String virtualTopic = "virtual-topic";
        String physicalTopic = "physical-topi"; // Must be same length for in-place

        TopicAliasInterceptor aliaser = new TopicAliasInterceptor();
        aliaser.addAlias(virtualTopic, physicalTopic);

        KafkaInterceptorChain chain = new KafkaInterceptorChain();
        chain.addInterceptor(aliaser);

        // 1. Start a Dummy Backend
        CountDownLatch backendStarted = new CountDownLatch(1);
        AtomicReference<String> receivedTopic = new AtomicReference<>("");
        ExecutorService backendExecutor = Executors.newSingleThreadExecutor();
        backendExecutor.submit(() -> {
            try (ServerSocket serverSocket = new ServerSocket(backendPort)) {
                backendStarted.countDown();
                Socket clientSocket = serverSocket.accept();
                InputStream in = clientSocket.getInputStream();

                // Read framing
                byte[] lenBuf = new byte[4];
                in.read(lenBuf);
                int payloadLen = java.nio.ByteBuffer.wrap(lenBuf).getInt();

                // Read payload
                byte[] payload = new byte[payloadLen];
                in.read(payload);
                java.nio.ByteBuffer b = java.nio.ByteBuffer.wrap(payload);

                // Read RequestHeader to advance position correctly
                org.apache.kafka.common.requests.RequestHeader header = org.apache.kafka.common.requests.RequestHeader.parse(b);

                // Body: acks(2), timeout(4), topicsLen(4)
                b.getShort(); b.getInt(); b.getInt();

                // Topic name
                short nameLen = b.getShort();
                byte[] nameBytes = new byte[nameLen];
                b.get(nameBytes);
                receivedTopic.set(new String(nameBytes));

            } catch (Exception e) {
                e.printStackTrace();
            }
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

        // 3. Connect Client and send a Produce packet for virtual topic
        try (Socket socket = new Socket("localhost", proxyPort)) {
            OutputStream out = socket.getOutputStream();

            // Kafka Produce Request v2:
            // Header: apiKey=0, version=2, correlationId=100, clientIdLen=11, clientId="test-client"
            String clientId = "test-client";
            int payloadLen = 2+2+4+2+clientId.length() + 2+4+4+2+virtualTopic.length();
            java.nio.ByteBuffer b = java.nio.ByteBuffer.allocate(4 + payloadLen);
            b.putInt(payloadLen);
            b.putShort((short) 0); // apiKey (Produce)
            b.putShort((short) 2); // version
            b.putInt(100); // correlationId
            b.putShort((short) clientId.length());
            b.put(clientId.getBytes());
            b.putShort((short) 1); // acks
            b.putInt(1000); // timeout
            b.putInt(1); // topicsLen
            b.putShort((short) virtualTopic.length());
            b.put(virtualTopic.getBytes());

            out.write(b.array());
            out.flush();

            Thread.sleep(500);
        } finally {
            backendExecutor.shutdownNow();
            proxyExecutor.shutdownNow();
        }

        assertEquals("Backend should have received the PHYSICAL topic", physicalTopic, receivedTopic.get());
    }
}
