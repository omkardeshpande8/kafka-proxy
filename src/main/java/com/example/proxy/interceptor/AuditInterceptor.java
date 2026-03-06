package com.example.proxy.interceptor;

import com.example.proxy.protocol.KafkaMessage;
import io.netty.channel.ChannelHandlerContext;
import org.apache.kafka.common.protocol.ApiKeys;

public class AuditInterceptor implements KafkaInterceptor {

    @Override
    public boolean onRequest(ChannelHandlerContext ctx, KafkaMessage message) {
        String apiKeyName = "UNKNOWN";
        try {
            apiKeyName = ApiKeys.forId(message.apiKey()).name;
        } catch (Exception e) {
            // Ignore
        }

        System.out.println(String.format("[AUDIT] Request from %s: API=%s(%d), Version=%d, CorrelationId=%d, ClientId=%s",
                ctx.channel().remoteAddress(),
                apiKeyName,
                message.apiKey(),
                message.apiVersion(),
                message.correlationId(),
                message.clientId()));

        return true;
    }

    @Override
    public void onResponse(ChannelHandlerContext ctx, Object response) {
        // Log responses if needed
    }
}
