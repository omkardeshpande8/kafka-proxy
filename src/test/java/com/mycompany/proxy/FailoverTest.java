package com.mycompany.proxy;

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
import java.util.concurrent.atomic.AtomicBoolean;

public class FailoverTest {

    @Test
    public void testFailoverSwitchesBackend() throws Exception {
        int localPort = 17092;
        int primaryPort = 17093;
        int secondaryPort = 17094;

        KafkaProxy proxy = new KafkaProxy(localPort, "localhost", primaryPort);

        // 1. Start Primary (should fail or we skip it)
        // 2. Start Secondary
        CountDownLatch secondaryStarted = new CountDownLatch(1);
        AtomicBoolean secondaryReceived = new AtomicBoolean(false);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            try (ServerSocket serverSocket = new ServerSocket(secondaryPort)) {
                secondaryStarted.countDown();
                Socket s = serverSocket.accept();
                secondaryReceived.set(true);
            } catch (Exception e) {}
        });

        assertTrue("Secondary failed to start", secondaryStarted.await(5, TimeUnit.SECONDS));

        // 3. Start Proxy
        ExecutorService proxyExecutor = Executors.newSingleThreadExecutor();
        proxyExecutor.submit(() -> {
            try {
                proxy.run();
            } catch (Exception e) {}
        });

        Thread.sleep(1000);

        // 4. Trigger Failover
        proxy.failover("localhost", secondaryPort);

        // 5. Connect and Verify
        try (Socket socket = new Socket("localhost", localPort)) {
            OutputStream out = socket.getOutputStream();
            out.write(new byte[]{0,0,0,1,0}); // Trigger connection to backend
            out.flush();
            Thread.sleep(500);
        }

        assertTrue("Secondary backend should have received the connection", secondaryReceived.get());

        executor.shutdownNow();
        proxyExecutor.shutdownNow();
    }
}
