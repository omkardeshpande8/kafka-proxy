package com.mycompany.proxy;

import com.mycompany.proxy.interceptor.ChaosInterceptor;
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

public class ChaosTest {

    @Test
    public void testChaosInjectsLatency() throws Exception {
        int backendPort = 12099;
        int proxyPort = 12098;
        int latency = 500;

        KafkaInterceptorChain chain = new KafkaInterceptorChain();
        chain.addInterceptor(new ChaosInterceptor(latency, 0));

        // 1. Start a Dummy Backend
        CountDownLatch backendStarted = new CountDownLatch(1);
        ExecutorService backendExecutor = Executors.newSingleThreadExecutor();
        backendExecutor.submit(() -> {
            try (ServerSocket serverSocket = new ServerSocket(backendPort)) {
                backendStarted.countDown();
                Socket clientSocket = serverSocket.accept();
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

        // 3. Measure request time
        long start = System.currentTimeMillis();
        try (Socket socket = new Socket("localhost", proxyPort)) {
            OutputStream out = socket.getOutputStream();
            byte[] msg = new byte[]{0,0,0,10, 0,0, 0,0, 0,0,0,0, 0,0}; // Fake header
            out.write(msg);
            out.flush();
            socket.setSoTimeout(1000);
            socket.getInputStream().read();
        } catch (Exception e) {}
        long duration = System.currentTimeMillis() - start;

        assertTrue("Request should have been delayed by at least " + latency + "ms, took " + duration + "ms", duration >= (latency - 300));

        backendExecutor.shutdownNow();
        proxyExecutor.shutdownNow();
    }
}
