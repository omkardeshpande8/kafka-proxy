package com.example.proxy.interceptor;

import com.example.proxy.protocol.KafkaMessage;
import io.netty.channel.ChannelHandlerContext;

public interface KafkaInterceptor {
    /**
     * Called when a request is received from the client.
     * @return true if the request should proceed, false if it should be blocked.
     */
    boolean onRequest(ChannelHandlerContext ctx, KafkaMessage message);

    /**
     * Called when a response is received from the broker.
     */
    void onResponse(ChannelHandlerContext ctx, Object response);
}
