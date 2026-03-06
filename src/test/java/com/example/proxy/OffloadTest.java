package com.example.proxy;

import com.example.proxy.interceptor.KafkaInterceptorChain;
import com.example.proxy.interceptor.OffloadInterceptor;
import org.junit.Test;
import static org.junit.Assert.*;
import java.io.File;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OffloadTest {

    @Test
    public void testPayloadOffloading() throws Exception {
        int backendPort = 16099;
        int proxyPort = 16098;
        int threshold = 100;

        KafkaInterceptorChain chain = new KafkaInterceptorChain();
        chain.addInterceptor(new OffloadInterceptor(threshold));

        // 1. Start a Dummy Backend
        CountDownLatch backendStarted = new CountDownLatch(1);
        ExecutorService backendExecutor = Executors.newSingleThreadExecutor();
        backendExecutor.submit(() -> {
            try (ServerSocket serverSocket = new ServerSocket(backendPort)) {
                backendStarted.countDown();
                Socket s = serverSocket.accept();
                // Just keep open
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

        // 3. Send Large Produce packet
        try (Socket socket = new Socket("localhost", proxyPort)) {
            OutputStream out = socket.getOutputStream();

            byte[] largePayload = new byte[200];
            java.nio.ByteBuffer b = java.nio.ByteBuffer.allocate(4 + 2 + 2 + 4 + 2 + largePayload.length);
            b.putInt(2+2+4+2 + largePayload.length);
            b.putShort((short) 0); // apiKey Produce
            b.putShort((short) 0);
            b.putInt(1);
            b.putShort((short) 0);
            b.put(largePayload);

            out.write(b.array());
            out.flush();

            Thread.sleep(500);
        } finally {
            backendExecutor.shutdownNow();
            proxyExecutor.shutdownNow();
        }

        // Check if any offload file was created in /tmp
        File tmpDir = new File("/tmp");
        File[] files = tmpDir.listFiles((dir, name) -> name.startsWith("kafka-offload-"));
        assertTrue("Offload file should have been created", files != null && files.length > 0);

        // Cleanup
        for (File f : files) f.delete();
    }
}
