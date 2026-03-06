package com.example.proxy;

import com.example.proxy.interceptor.KafkaInterceptorChain;
import com.example.proxy.interceptor.RateLimitInterceptor;
import org.junit.Test;
import static org.junit.Assert.*;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RateLimitTest {

    @Test
    public void testRateLimitingThrottles() throws Exception {
        int backendPort = 13099;
        int proxyPort = 13098;
        long maxBps = 100;

        KafkaInterceptorChain chain = new KafkaInterceptorChain();
        chain.addInterceptor(new RateLimitInterceptor(maxBps));

        // 1. Start a Dummy Backend
        CountDownLatch backendStarted = new CountDownLatch(1);
        ExecutorService backendExecutor = Executors.newSingleThreadExecutor();
        backendExecutor.submit(() -> {
            try (ServerSocket serverSocket = new ServerSocket(backendPort)) {
                backendStarted.countDown();
                Socket s = serverSocket.accept();
                // Keep open
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

        // 3. Connect Client and send Produce packet larger than maxBps
        long start = System.currentTimeMillis();
        try (Socket socket = new Socket("localhost", proxyPort)) {
            OutputStream out = socket.getOutputStream();

            // Produce header v0: apiKey=0, version=0, correlationId=1, clientIdLen=0
            byte[] msg = new byte[150]; // Larger than maxBps (100)
            java.nio.ByteBuffer b = java.nio.ByteBuffer.allocate(4 + msg.length);
            b.putInt(msg.length);
            b.putShort((short)0); // apiKey Produce
            b.putShort((short)0); // version
            b.putInt(1); // corrId
            b.putShort((short)0); // clientid len
            b.put(new byte[150-10]); // padding

            out.write(b.array());
            out.flush();
            socket.setSoTimeout(1500);
            socket.getInputStream().read();
        } catch (Exception e) {}
        long duration = System.currentTimeMillis() - start;

        assertTrue("Request should have been throttled for at least 1000ms, took " + duration + "ms", duration >= 1000);

        backendExecutor.shutdownNow();
        proxyExecutor.shutdownNow();
    }
}
