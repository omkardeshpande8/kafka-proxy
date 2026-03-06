package com.example.proxy.interceptor;

import com.example.proxy.protocol.KafkaMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.kafka.common.protocol.ApiKeys;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CacheInterceptor implements KafkaInterceptor {

    private final Map<String, ByteBuf> fetchCache = new ConcurrentHashMap<>();

    @Override
    public void onRequest(ChannelHandlerContext ctx, KafkaMessage message, KafkaInterceptorChain.Callback callback) {
        if (message.apiKey() == ApiKeys.FETCH.id) {
            String key = generateCacheKey(message);
            ByteBuf cachedResponse = fetchCache.get(key);
            if (cachedResponse != null) {
                System.out.println("[CACHE] Serving Fetch request from cache for: " + key);
                ctx.writeAndFlush(cachedResponse.retain());
                callback.block();
                return;
            }
        }
        callback.proceed();
    }

    private String generateCacheKey(KafkaMessage message) {
        // Simple key based on topic and partition
        // In a real implementation, we'd extract topic/partition from FetchRequest
        return "topic-partition-offset";
    }

    @Override
    public void onResponse(ChannelHandlerContext ctx, Object response) {
        // Here we would store the response in fetchCache if it's a FetchResponse
    }
}
