package com.example.proxy.interceptor;

import com.example.proxy.protocol.KafkaMessage;
import io.netty.channel.ChannelHandlerContext;
import org.apache.kafka.common.protocol.ApiKeys;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class RateLimitInterceptor implements KafkaInterceptor {

    private final long maxBytesPerSecond;
    private final AtomicLong bytesThisSecond = new AtomicLong(0);
    private long lastResetTime = System.currentTimeMillis();

    public RateLimitInterceptor(long maxBytesPerSecond) {
        this.maxBytesPerSecond = maxBytesPerSecond;
    }

    @Override
    public void onRequest(ChannelHandlerContext ctx, KafkaMessage message, KafkaInterceptorChain.Callback callback) {
        if (message.apiKey() == ApiKeys.PRODUCE.id) {
            long now = System.currentTimeMillis();
            if (now - lastResetTime > 1000) {
                bytesThisSecond.set(0);
                lastResetTime = now;
            }

            int msgSize = message.payload().readableBytes();
            if (bytesThisSecond.addAndGet(msgSize) > maxBytesPerSecond) {
                System.out.println(String.format("[RATELIMIT] Throttling request of size %d bytes", msgSize));
                ctx.executor().schedule(callback::proceed, 1, TimeUnit.SECONDS);
                return;
            }
        }
        callback.proceed();
    }
}
