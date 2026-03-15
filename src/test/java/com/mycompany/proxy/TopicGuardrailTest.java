package com.mycompany.proxy;

import com.mycompany.proxy.interceptor.KafkaInterceptorChain;
import com.mycompany.proxy.interceptor.TopicGuardrailInterceptor;
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

public class TopicGuardrailTest {

    @Test
    public void testTopicGuardrailBlocksTopic() throws Exception {
        int backendPort = 9099;
        int proxyPort = 9098;
        String forbiddenTopic = "secret-topic";

        TopicGuardrailInterceptor guardrail = new TopicGuardrailInterceptor();
        guardrail.blockTopic("secret-.*");

        KafkaInterceptorChain chain = new KafkaInterceptorChain();
        chain.addInterceptor(guardrail);

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

        // 3. Connect Client and send a Produce packet for forbidden topic
        try (Socket socket = new Socket("localhost", proxyPort)) {
            socket.setSoTimeout(1000);
            OutputStream out = socket.getOutputStream();

            // Kafka Produce Request v2:
            // Header: apiKey=0, version=2, correlationId=100, clientIdLen=0
            // Body: acks=1, timeout=1000, topicsLen=1, topicNameLen=12, topicName="secret-topic"
            String topicName = forbiddenTopic;
            int payloadLen = 2+2+4+2 + 2+4+4+2+topicName.length();
            java.nio.ByteBuffer b = java.nio.ByteBuffer.allocate(4 + payloadLen);
            b.putInt(payloadLen);
            b.putShort((short) 0); // apiKey (Produce)
            b.putShort((short) 2); // version
            b.putInt(100); // correlationId
            b.putShort((short) 0); // clientIdLen
            b.putShort((short) 1); // acks
            b.putInt(1000); // timeout
            b.putInt(1); // topicsLen
            b.putShort((short) topicName.length());
            b.put(topicName.getBytes());

            out.write(b.array());
            out.flush();

            Thread.sleep(500);
        } finally {
            backendExecutor.shutdownNow();
            proxyExecutor.shutdownNow();
        }

        assertFalse("Backend should NOT have received the request", backendReceivedRequest.get());
    }
}
