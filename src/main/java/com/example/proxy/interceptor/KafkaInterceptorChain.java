package com.example.proxy.interceptor;

import com.example.proxy.protocol.KafkaMessage;
import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayList;
import java.util.List;

public class KafkaInterceptorChain {
    public interface Callback {
        void proceed();
        void block();
    }

    private final List<KafkaInterceptor> interceptors = new ArrayList<>();

    public void addInterceptor(KafkaInterceptor interceptor) {
        interceptors.add(interceptor);
    }

    public void onRequest(ChannelHandlerContext ctx, KafkaMessage message, Callback finalCallback) {
        executeInterceptor(0, ctx, message, finalCallback);
    }

    private void executeInterceptor(int index, ChannelHandlerContext ctx, KafkaMessage message, Callback finalCallback) {
        if (index >= interceptors.size()) {
            finalCallback.proceed();
            return;
        }

        interceptors.get(index).onRequest(ctx, message, new Callback() {
            @Override
            public void proceed() {
                executeInterceptor(index + 1, ctx, message, finalCallback);
            }

            @Override
            public void block() {
                finalCallback.block();
            }
        });
    }

    public void onResponse(ChannelHandlerContext ctx, Object response) {
        for (KafkaInterceptor interceptor : interceptors) {
            interceptor.onResponse(ctx, response);
        }
    }
}
