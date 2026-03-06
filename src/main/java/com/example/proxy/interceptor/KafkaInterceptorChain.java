package com.example.proxy.interceptor;

import com.example.proxy.protocol.KafkaMessage;
import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayList;
import java.util.List;

public class KafkaInterceptorChain {
    private final List<KafkaInterceptor> interceptors = new ArrayList<>();

    public void addInterceptor(KafkaInterceptor interceptor) {
        interceptors.add(interceptor);
    }

    public boolean onRequest(ChannelHandlerContext ctx, KafkaMessage message) {
        for (KafkaInterceptor interceptor : interceptors) {
            if (!interceptor.onRequest(ctx, message)) {
                return false;
            }
        }
        return true;
    }

    public void onResponse(ChannelHandlerContext ctx, Object response) {
        for (KafkaInterceptor interceptor : interceptors) {
            interceptor.onResponse(ctx, response);
        }
    }
}
