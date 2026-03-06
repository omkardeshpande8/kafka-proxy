package com.example.proxy;

import com.example.proxy.interceptor.KafkaInterceptor;
import com.example.proxy.interceptor.KafkaInterceptorChain;
import com.example.proxy.protocol.KafkaMessage;
import io.netty.channel.ChannelHandlerContext;
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
import java.util.concurrent.atomic.AtomicBoolean;

public class InterceptorTest {

    @Test
    public void testInterceptorCanBlockRequest() throws Exception {
        int backendPort = 9095;
        int proxyPort = 9094;
        String testMessage = "This should be blocked";

        KafkaInterceptorChain chain = new KafkaInterceptorChain();
        AtomicBoolean intercepted = new AtomicBoolean(false);

        chain.addInterceptor(new KafkaInterceptor() {
            @Override
            public boolean onRequest(ChannelHandlerContext ctx, KafkaMessage message) {
                intercepted.set(true);
                return false; // Block
            }

            @Override
            public void onResponse(ChannelHandlerContext ctx, Object response) {}
        });

        // 1. Start a Dummy Backend
        CountDownLatch backendStarted = new CountDownLatch(1);
        ExecutorService backendExecutor = Executors.newSingleThreadExecutor();
        backendExecutor.submit(() -> {
            try (ServerSocket serverSocket = new ServerSocket(backendPort)) {
                backendStarted.countDown();
                Socket clientSocket = serverSocket.accept();
                // Should not receive anything if blocked
            } catch (Exception e) {
                // Ignore
            }
        });

        assertTrue("Backend failed to start", backendStarted.await(5, TimeUnit.SECONDS));

        // 2. Start Proxy
        ExecutorService proxyExecutor = Executors.newSingleThreadExecutor();
        proxyExecutor.submit(() -> {
            try {
                new KafkaProxy(proxyPort, "localhost", backendPort, chain).run();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Thread.sleep(1000);

        // 3. Connect Client to Proxy
        try (Socket socket = new Socket("localhost", proxyPort)) {
            socket.setSoTimeout(2000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            // Kafka-like framing
            // Kafka Request Header v0: apiKey(2), apiVersion(2), correlationId(4), clientIdLen(2), clientId...
            byte[] msgBytes = testMessage.getBytes(StandardCharsets.UTF_8);
            int payloadLen = 2 + 2 + 4 + 2 + 0 + msgBytes.length;
            java.nio.ByteBuffer b = java.nio.ByteBuffer.allocate(4 + payloadLen);
            b.putInt(payloadLen);
            b.putShort((short) 0); // API Key (Produce)
            b.putShort((short) 0); // API Version
            b.putInt(12345); // Correlation ID
            b.putShort((short) 0); // Client ID length (0)
            b.put(msgBytes);

            out.write(b.array());
            out.flush();

            // We expect NO response because it's blocked by interceptor
            byte[] buffer = new byte[1024];
            int read = -1;
            try {
                read = in.read(buffer);
            } catch (java.net.SocketTimeoutException e) {
                // Expected
            }

            assertTrue("Interceptor should have been called", intercepted.get());
            assertTrue("Should not have read data or EOF", read <= 0);
        } finally {
            backendExecutor.shutdownNow();
            proxyExecutor.shutdownNow();
        }
    }
}
