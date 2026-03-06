package com.example.proxy.interceptor;

import com.example.proxy.protocol.KafkaMessage;
import io.netty.channel.ChannelHandlerContext;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class ChaosInterceptor implements KafkaInterceptor {

    private final int latencyMs;
    private final double errorRate;
    private final Random random = new Random();

    public ChaosInterceptor(int latencyMs, double errorRate) {
        this.latencyMs = latencyMs;
        this.errorRate = errorRate;
    }

    @Override
    public void onRequest(ChannelHandlerContext ctx, KafkaMessage message, KafkaInterceptorChain.Callback callback) {
        if (errorRate > 0 && random.nextDouble() < errorRate) {
            System.out.println("[CHAOS] Simulating connection failure");
            callback.block();
            return;
        }

        if (latencyMs > 0) {
            System.out.println("[CHAOS] Injecting latency: " + latencyMs + "ms");
            ctx.executor().schedule(callback::proceed, latencyMs, TimeUnit.MILLISECONDS);
        } else {
            callback.proceed();
        }
    }
}
