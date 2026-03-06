package com.example.proxy.interceptor;

import com.example.proxy.protocol.KafkaMessage;
import io.netty.channel.ChannelHandlerContext;
import org.apache.kafka.common.protocol.ApiKeys;

public class AuditInterceptor implements KafkaInterceptor {

    @Override
    public void onRequest(ChannelHandlerContext ctx, KafkaMessage message, KafkaInterceptorChain.Callback callback) {
        String apiKeyName = "UNKNOWN";
        try {
            apiKeyName = ApiKeys.forId(message.apiKey()).name;
        } catch (Exception e) {}

        System.out.println(String.format("[AUDIT] Request from %s: API=%s(%d), Version=%d, CorrelationId=%d, ClientId=%s",
                ctx.channel().remoteAddress(),
                apiKeyName,
                message.apiKey(),
                message.apiVersion(),
                message.correlationId(),
                message.clientId()));

        callback.proceed();
    }
}
