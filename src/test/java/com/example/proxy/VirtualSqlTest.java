package com.example.proxy;

import com.example.proxy.interceptor.KafkaInterceptorChain;
import com.example.proxy.interceptor.VirtualSqlInterceptor;
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

public class VirtualSqlTest {

    @Test
    public void testVirtualSqlFiltersRecord() throws Exception {
        int backendPort = 19099;
        int proxyPort = 19098;
        String filterValue = "allowed";

        KafkaInterceptorChain chain = new KafkaInterceptorChain();
        chain.addInterceptor(new VirtualSqlInterceptor(filterValue));

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

        // 3. Connect Client and send Produce packet that should be FILTERED (DROPPED)
        try (Socket socket = new Socket("localhost", proxyPort)) {
            socket.setSoTimeout(1000);
            OutputStream out = socket.getOutputStream();

            byte[] topicBytes = "test-topic".getBytes();
            byte[] payloadBytes = "this-is-forbidden".getBytes();
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

        assertFalse("Backend should NOT have received filtered record", backendReceivedRequest.get());
    }
}
