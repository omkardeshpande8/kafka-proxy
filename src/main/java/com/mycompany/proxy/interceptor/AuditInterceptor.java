package com.mycompany.proxy.interceptor;

import com.mycompany.proxy.protocol.KafkaMessage;
import io.netty.channel.ChannelHandlerContext;
import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditInterceptor implements KafkaInterceptor {
    private static final Logger logger = LoggerFactory.getLogger(AuditInterceptor.class);

    @Override
    public void onRequest(ChannelHandlerContext ctx, KafkaMessage message, KafkaInterceptorChain.Callback callback) {
        String apiKeyName = "UNKNOWN";
        try {
            apiKeyName = ApiKeys.forId(message.apiKey()).name;
        } catch (Exception e) {}

        if (logger.isInfoEnabled()) {
            logger.info("[AUDIT] Request from {}: API={}({}), Version={}, CorrelationId={}, ClientId={}",
                    ctx.channel().remoteAddress(),
                    apiKeyName,
                    message.apiKey(),
                    message.apiVersion(),
                    message.correlationId(),
                    message.clientId());
        }

        callback.proceed();
    }
}
