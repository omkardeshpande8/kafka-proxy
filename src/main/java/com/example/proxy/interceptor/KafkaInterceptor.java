package com.example.proxy.interceptor;

import com.example.proxy.protocol.KafkaMessage;
import io.netty.channel.ChannelHandlerContext;

public interface KafkaInterceptor {
    /**
     * Called when a request is received from the client.
     */
    void onRequest(ChannelHandlerContext ctx, KafkaMessage message, KafkaInterceptorChain.Callback callback);

    /**
     * Called when a response is received from the broker.
     */
    default void onResponse(ChannelHandlerContext ctx, Object response) {}
}
